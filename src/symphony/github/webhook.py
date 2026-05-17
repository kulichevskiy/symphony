"""FastAPI receiver for GitHub webhooks."""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Awaitable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal, Protocol, runtime_checkable

import aiosqlite
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from .. import db
from ..webhook import Clock

GITHUB_DELIVERY_HEADER = "x-github-delivery"
GITHUB_EVENT_HEADER = "x-github-event"
GITHUB_SIGNATURE_HEADER = "x-hub-signature-256"

GitHubWebhookEventType = Literal["pull_request", "issue_comment"]


@dataclass(frozen=True)
class GitHubWebhookEvent:
    event_type: GitHubWebhookEventType
    action: str
    repo: str
    delivery_id: str
    pr_number: int | None = None
    merged: bool | None = None
    merged_by: str | None = None
    merged_at: str | None = None
    comment_id: int | None = None
    comment_body: str | None = None
    sender_login: str | None = None


@dataclass(frozen=True)
class GitHubWebhookSettings:
    secret: str = ""
    repo_secrets: Mapping[str, str] = field(default_factory=dict)
    enabled_repos: frozenset[str] | None = None
    dedupe_ttl_secs: int = 600

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "repo_secrets",
            {
                _normalize_repo(repo): secret
                for repo, secret in self.repo_secrets.items()
            },
        )
        if self.enabled_repos is not None:
            object.__setattr__(
                self,
                "enabled_repos",
                frozenset(_normalize_repo(repo) for repo in self.enabled_repos),
            )
        if self.secret or self.enabled_repos is None:
            return
        missing = sorted(
            repo for repo in self.enabled_repos if not self.repo_secrets.get(repo)
        )
        if missing:
            raise ValueError(
                "GITHUB_WEBHOOK_SECRET is empty and enabled repos lack "
                f"webhook_secret: {', '.join(missing)}"
            )

    def all_secrets(self) -> tuple[str, ...]:
        return tuple(s for s in (self.secret, *self.repo_secrets.values()) if s)

    def repo_enabled(self, repo: str | None) -> bool:
        if self.enabled_repos is None:
            return True
        return repo is not None and _normalize_repo(repo) in self.enabled_repos

    def secrets_for_repo(self, repo: str) -> tuple[str, ...]:
        repo_secret = self.repo_secrets.get(_normalize_repo(repo))
        if repo_secret:
            return (repo_secret,)
        return (self.secret,) if self.secret else ()


@runtime_checkable
class GitHubWebhookHandler(Protocol):
    def handle_github_webhook(self, event: GitHubWebhookEvent) -> Awaitable[Any]:
        """Handle a verified, deduped GitHub webhook event."""


def _normalize_repo(repo: str) -> str:
    return repo.casefold()


def verify_github_signature(secret: str, body: bytes, signature: str | None) -> bool:
    if not secret or not signature:
        return False
    digest = _signature_digest(signature)
    if digest is None:
        return False
    expected = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, digest)


def _signature_digest(signature: str) -> str | None:
    value = signature.strip()
    if value.startswith("sha256="):
        value = value[len("sha256=") :]
    if len(value) != 64:
        return None
    return value.casefold()


def _matches_any_secret(
    secrets: Iterable[str],
    body: bytes,
    signature: str | None,
) -> bool:
    return any(verify_github_signature(secret, body, signature) for secret in secrets)


def create_github_webhook_router(
    handler: GitHubWebhookHandler,
    conn: aiosqlite.Connection,
    settings: GitHubWebhookSettings,
    *,
    clock: Clock | None = None,
) -> APIRouter:
    router = APIRouter()
    now_fn = clock if clock is not None else lambda: datetime.now(UTC)

    @router.post("/github/webhook")
    async def github_webhook(request: Request) -> JSONResponse:
        body = await request.body()
        signature = request.headers.get(GITHUB_SIGNATURE_HEADER)
        if not _matches_any_secret(settings.all_secrets(), body, signature):
            raise HTTPException(status_code=401, detail="invalid signature")

        try:
            payload: dict[str, Any] = json.loads(body)
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail="invalid json") from e

        event_name = _event_name(request)
        repo = _payload_repo(payload)
        if repo is None and event_name in {"pull_request", "issue_comment"}:
            raise HTTPException(status_code=400, detail="missing repository.full_name")
        if not settings.repo_enabled(repo):
            return JSONResponse(
                {
                    "status": "ignored",
                    "handled": False,
                    "kind": event_name,
                    "detail": "repo webhook disabled",
                }
            )
        if repo is not None and not _matches_any_secret(
            settings.secrets_for_repo(repo),
            body,
            signature,
        ):
            raise HTTPException(status_code=401, detail="invalid signature")

        delivery_id = request.headers.get(GITHUB_DELIVERY_HEADER)
        if not delivery_id:
            raise HTTPException(status_code=400, detail="missing X-GitHub-Delivery")

        now = now_fn()
        claim_state = await db.webhook_deliveries.begin(
            conn,
            delivery_id,
            received_at=now,
            ttl_secs=settings.dedupe_ttl_secs,
        )
        if claim_state == "duplicate":
            return JSONResponse({"status": "duplicate", "handled": False})
        if claim_state == "pending":
            raise HTTPException(status_code=503, detail="delivery already pending")

        try:
            event = _parse_event(
                event_name,
                payload,
                delivery_id=delivery_id,
            )
            if event is None:
                await db.webhook_deliveries.finish(conn, delivery_id)
                return JSONResponse(
                    {
                        "status": "ignored",
                        "handled": False,
                        "kind": event_name,
                    }
                )
            result = await handler.handle_github_webhook(event)
        except Exception:
            await db.webhook_deliveries.forget(conn, delivery_id)
            raise
        await db.webhook_deliveries.finish(conn, delivery_id)

        return JSONResponse(
            {
                "status": "ok",
                "handled": bool(getattr(result, "handled", True)),
                "kind": event.event_type,
                "action": event.action,
            }
        )

    return router


def _event_name(request: Request) -> str:
    return str(request.headers.get(GITHUB_EVENT_HEADER) or "").casefold()


def _payload_repo(payload: Mapping[str, Any]) -> str | None:
    repo = payload.get("repository")
    if not isinstance(repo, Mapping):
        return None
    full_name = repo.get("full_name")
    if not isinstance(full_name, str) or not full_name:
        return None
    return full_name


def _parse_event(
    event_name: str,
    payload: Mapping[str, Any],
    *,
    delivery_id: str,
) -> GitHubWebhookEvent | None:
    action = str(payload.get("action") or "").casefold()
    if event_name == "pull_request":
        if action not in {"closed", "merged", "reopened"}:
            return None
        return _parse_pull_request_event(payload, action=action, delivery_id=delivery_id)
    if event_name == "issue_comment":
        if action != "created":
            return None
        return _parse_issue_comment_event(payload, action=action, delivery_id=delivery_id)
    return None


def _parse_pull_request_event(
    payload: Mapping[str, Any],
    *,
    action: str,
    delivery_id: str,
) -> GitHubWebhookEvent:
    repo = _required_repo(payload)
    pr = _required_mapping(payload, "pull_request")
    merged = bool(pr.get("merged")) or action == "merged"
    merged_by = _login(pr.get("merged_by")) if merged else None
    merged_at = pr.get("merged_at") if isinstance(pr.get("merged_at"), str) else None
    return GitHubWebhookEvent(
        event_type="pull_request",
        action=action,
        repo=repo,
        delivery_id=delivery_id,
        pr_number=_required_int(pr, "number"),
        merged=merged,
        merged_by=merged_by,
        merged_at=merged_at,
        sender_login=_login(payload.get("sender")),
    )


def _parse_issue_comment_event(
    payload: Mapping[str, Any],
    *,
    action: str,
    delivery_id: str,
) -> GitHubWebhookEvent:
    repo = _required_repo(payload)
    issue = _required_mapping(payload, "issue")
    comment = _required_mapping(payload, "comment")
    pr_number = (
        _required_int(issue, "number") if isinstance(issue.get("pull_request"), Mapping) else None
    )
    body = comment.get("body")
    return GitHubWebhookEvent(
        event_type="issue_comment",
        action=action,
        repo=repo,
        delivery_id=delivery_id,
        pr_number=pr_number,
        comment_id=_required_int(comment, "id"),
        comment_body=body if isinstance(body, str) else None,
        sender_login=_login(payload.get("sender")),
    )


def _required_repo(payload: Mapping[str, Any]) -> str:
    repo = _payload_repo(payload)
    if repo is None:
        raise HTTPException(status_code=400, detail="missing repository.full_name")
    return repo


def _required_mapping(payload: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = payload.get(key)
    if not isinstance(value, Mapping):
        raise HTTPException(status_code=400, detail=f"missing {key}")
    return value


def _required_int(payload: Mapping[str, Any], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise HTTPException(status_code=400, detail=f"missing {key}")
    return value


def _login(value: object) -> str | None:
    if not isinstance(value, Mapping):
        return None
    login = value.get("login")
    return login if isinstance(login, str) else None
