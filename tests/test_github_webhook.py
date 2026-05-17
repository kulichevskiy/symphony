from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest

from symphony import db
from symphony.app import create_app
from symphony.github.webhook import GitHubWebhookEvent, GitHubWebhookSettings

SECRET = "github-webhook-secret"
REPO_SECRET = "repo-specific-secret"
NOW = datetime(2026, 5, 17, 12, 0, tzinfo=UTC)


@dataclass
class _Result:
    handled: bool = True


class _Handler:
    def __init__(self) -> None:
        self.events: list[GitHubWebhookEvent] = []

    async def handle_linear_webhook(self, payload: dict[str, Any]) -> _Result:
        raise AssertionError(f"linear webhook should not be called: {payload!r}")

    async def handle_github_webhook(self, event: GitHubWebhookEvent) -> _Result:
        self.events.append(event)
        return _Result()


def _body(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode()


def _headers(
    body: bytes,
    *,
    event: str = "pull_request",
    delivery: str = "delivery-1",
    secret: str = SECRET,
) -> dict[str, str]:
    digest = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return {
        "X-GitHub-Event": event,
        "X-GitHub-Delivery": delivery,
        "X-Hub-Signature-256": f"sha256={digest}",
        "Content-Type": "application/json",
    }


def _pull_request_payload(*, merged: bool = True, action: str = "closed") -> dict[str, Any]:
    return {
        "action": action,
        "repository": {"full_name": "org/repo"},
        "pull_request": {
            "number": 42,
            "merged": merged,
            "merged_by": {"login": "maintainer"} if merged else None,
            "merged_at": "2026-05-17T11:59:00Z" if merged else None,
        },
        "sender": {"login": "octocat"},
    }


def _issue_comment_payload() -> dict[str, Any]:
    return {
        "action": "created",
        "repository": {"full_name": "org/repo"},
        "issue": {"number": 42, "pull_request": {"url": "https://api.github/pr"}},
        "comment": {"id": 1234, "body": "@codex review"},
        "sender": {"login": "octocat"},
    }


async def _delivery_rows(conn: object) -> list[tuple[str, str]]:
    cur = await conn.execute("SELECT id, status FROM webhook_deliveries ORDER BY id")
    return [(str(row[0]), str(row[1])) for row in await cur.fetchall()]


@pytest.mark.asyncio
async def test_valid_pull_request_webhook_is_verified_persisted_and_emitted(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        handler = _Handler()
        app = create_app(
            handler,
            conn,
            github_webhook_settings=GitHubWebhookSettings(
                secret=SECRET,
                enabled_repos=frozenset({"org/repo"}),
            ),
            ui_enabled=False,
            clock=lambda: NOW,
        )
        body = _body(_pull_request_payload())
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/github/webhook",
                content=body,
                headers=_headers(body),
            )

        assert response.status_code == 200
        assert response.json() == {
            "status": "ok",
            "handled": True,
            "kind": "pull_request",
            "action": "closed",
        }
        assert handler.events == [
            GitHubWebhookEvent(
                event_type="pull_request",
                action="closed",
                repo="org/repo",
                delivery_id="delivery-1",
                pr_number=42,
                merged=True,
                merged_by="maintainer",
                merged_at="2026-05-17T11:59:00Z",
                sender_login="octocat",
            )
        ]
        assert await _delivery_rows(conn) == [("delivery-1", "handled")]
        assert await conn.execute_fetchall("SELECT * FROM issue_prs") == []
        assert await conn.execute_fetchall("SELECT * FROM operator_waits") == []
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_valid_issue_comment_webhook_is_accepted(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        handler = _Handler()
        app = create_app(
            handler,
            conn,
            github_webhook_settings=GitHubWebhookSettings(secret=SECRET),
            ui_enabled=False,
            clock=lambda: NOW,
        )
        body = _body(_issue_comment_payload())
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/github/webhook",
                content=body,
                headers=_headers(body, event="issue_comment"),
            )

        assert response.status_code == 200
        assert handler.events == [
            GitHubWebhookEvent(
                event_type="issue_comment",
                action="created",
                repo="org/repo",
                delivery_id="delivery-1",
                pr_number=42,
                comment_id=1234,
                comment_body="@codex review",
                sender_login="octocat",
            )
        ]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_invalid_github_signature_writes_no_delivery(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        handler = _Handler()
        app = create_app(
            handler,
            conn,
            github_webhook_settings=GitHubWebhookSettings(secret=SECRET),
            ui_enabled=False,
            clock=lambda: NOW,
        )
        body = _body(_pull_request_payload())
        headers = _headers(body)
        headers["X-Hub-Signature-256"] = "sha256=bad"
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/github/webhook",
                content=body,
                headers=headers,
            )

        assert response.status_code == 401
        assert handler.events == []
        assert await _delivery_rows(conn) == []
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_duplicate_github_delivery_is_200_noop(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        handler = _Handler()
        app = create_app(
            handler,
            conn,
            github_webhook_settings=GitHubWebhookSettings(secret=SECRET),
            ui_enabled=False,
            clock=lambda: NOW,
        )
        body = _body(_pull_request_payload())
        headers = _headers(body, delivery="delivery-duplicate")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            first = await client.post("/github/webhook", content=body, headers=headers)
            second = await client.post("/github/webhook", content=body, headers=headers)

        assert first.status_code == 200
        assert second.status_code == 200
        assert second.json() == {"status": "duplicate", "handled": False}
        assert len(handler.events) == 1
        assert await _delivery_rows(conn) == [("delivery-duplicate", "handled")]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_unknown_github_event_type_is_deduped_noop(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        handler = _Handler()
        app = create_app(
            handler,
            conn,
            github_webhook_settings=GitHubWebhookSettings(secret=SECRET),
            ui_enabled=False,
            clock=lambda: NOW,
        )
        body = _body({"action": "created", "repository": {"full_name": "org/repo"}})
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/github/webhook",
                content=body,
                headers=_headers(body, event="push"),
            )

        assert response.status_code == 200
        assert response.json() == {
            "status": "ignored",
            "handled": False,
            "kind": "push",
        }
        assert handler.events == []
        assert await _delivery_rows(conn) == [("delivery-1", "handled")]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_malformed_github_body_returns_400(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        handler = _Handler()
        app = create_app(
            handler,
            conn,
            github_webhook_settings=GitHubWebhookSettings(secret=SECRET),
            ui_enabled=False,
            clock=lambda: NOW,
        )
        body = b"{not-json"
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/github/webhook",
                content=body,
                headers=_headers(body),
            )

        assert response.status_code == 400
        assert handler.events == []
        assert await _delivery_rows(conn) == []
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_repo_webhook_secret_override_is_enforced(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        handler = _Handler()
        app = create_app(
            handler,
            conn,
            github_webhook_settings=GitHubWebhookSettings(
                secret=SECRET,
                repo_secrets={"org/repo": REPO_SECRET},
                enabled_repos=frozenset({"org/repo"}),
            ),
            ui_enabled=False,
            clock=lambda: NOW,
        )
        body = _body(_pull_request_payload())
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            global_secret = await client.post(
                "/github/webhook",
                content=body,
                headers=_headers(body, delivery="delivery-global"),
            )
            repo_secret = await client.post(
                "/github/webhook",
                content=body,
                headers=_headers(
                    body,
                    delivery="delivery-repo",
                    secret=REPO_SECRET,
                ),
            )

        assert global_secret.status_code == 401
        assert repo_secret.status_code == 200
        assert len(handler.events) == 1
        assert await _delivery_rows(conn) == [("delivery-repo", "handled")]
    finally:
        await conn.close()
