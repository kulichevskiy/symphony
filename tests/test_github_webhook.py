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
from click import ClickException

from symphony import db
from symphony.app import create_app
from symphony.cli import _github_webhook_settings, _live_github_webhook_settings
from symphony.config import Config, LinearStates, RepoBinding
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


def _binding(
    *,
    webhook_enabled: bool = True,
    webhook_secret: str | None = None,
) -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        webhook_enabled=webhook_enabled,
        webhook_secret=webhook_secret,
        linear_states=LinearStates(ready="Todo", code_review="Needs Approval"),
    )


def _issue_comment_payload() -> dict[str, Any]:
    return {
        "action": "created",
        "repository": {"full_name": "org/repo"},
        "issue": {"number": 42, "pull_request": {"url": "https://api.github/pr"}},
        "comment": {"id": 1234, "body": "@codex review"},
        "sender": {"login": "octocat"},
    }


def test_github_webhook_settings_require_secret_for_enabled_repos() -> None:
    with pytest.raises(ValueError, match="enabled repos lack webhook_secret: org/web"):
        GitHubWebhookSettings(
            repo_secrets={"org/repo": REPO_SECRET},
            enabled_repos=frozenset({"org/repo", "org/web"}),
        )


def test_cli_rejects_enabled_github_webhook_repo_without_secret() -> None:
    cfg = Config(
        repos=[
            _binding(webhook_secret=REPO_SECRET),
            RepoBinding(
                linear_team_key="WEB",
                github_repo="org/web",
                linear_states=LinearStates(ready="Todo", code_review="Needs Approval"),
            ),
        ]
    )

    with pytest.raises(ClickException, match="enabled repos lack webhook_secret: org/web"):
        _github_webhook_settings(cfg)


@pytest.mark.asyncio
async def test_migrated_repo_secret_reaches_verifier(tmp_path: Path) -> None:
    """Post-cutover (SYM-194): a repo's webhook secret stored in the DB
    repo-secret table is loaded into the view at boot and reaches the verifier
    settings, so verification keeps working without manual re-entry."""
    from symphony.db.config_repo_secrets import load_view, set_secret

    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        # The binding topology is DB-owned; the secret lives in its own table.
        await db.config_bindings.insert(
            conn,
            payload={
                "linear_team_key": "ENG",
                "github_repo": "org/repo",
                "webhook_enabled": True,
                "linear_states": {"ready": "Todo", "code_review": "Needs Approval"},
            },
            key=("ENG", "org/repo", "", "linear", "default"),
        )
        await set_secret(conn, github_repo="org/repo", secret="stored-secret", expected_version=0)
        view = await load_view(conn)
        # The binding no longer carries the secret itself.
        cfg = Config(repos=[_binding(webhook_secret=None)])
        settings = _github_webhook_settings(cfg, view.as_map())
    finally:
        await conn.close()
    assert settings is not None
    assert settings.secrets_for_repo("org/repo") == ("stored-secret",)


def test_cli_skips_github_webhook_settings_when_repos_are_disabled() -> None:
    cfg = Config(repos=[_binding(webhook_enabled=False)])

    assert _github_webhook_settings(cfg) is None


def test_legacy_secret_precedence_last_binding_wins() -> None:
    """A legacy (not-yet-imported) YAML topology with multiple bindings on the
    same `github_repo` disagreeing on `webhook_secret` must resolve the same
    tie-break the pre-SYM-194 dict comprehension did: the *last* binding in
    `cfg.repos` wins, not the first (SYM-194 review)."""
    cfg = Config(
        repos=[
            _binding(webhook_secret="first"),
            RepoBinding(
                linear_team_key="ENG",
                github_repo="org/repo",
                issue_label="second-label",
                webhook_enabled=True,
                webhook_secret="second",
                linear_states=LinearStates(ready="Todo", code_review="Needs Approval"),
            ),
        ]
    )

    settings = _github_webhook_settings(cfg)

    assert settings is not None
    assert settings.secrets_for_repo("org/repo") == ("second",)


def test_db_owned_repo_secret_overrides_legacy_binding_secret() -> None:
    """The DB-owned repo-secret view takes precedence over any legacy
    per-binding secret for a repo it already covers — the per-binding value is
    only a fallback for a repo the view doesn't cover yet (SYM-194)."""
    cfg = Config(repos=[_binding(webhook_secret="legacy")])

    settings = _github_webhook_settings(cfg, {"org/repo": "from-view"})

    assert settings is not None
    assert settings.secrets_for_repo("org/repo") == ("from-view",)


def test_live_github_webhook_settings_disables_repos_instead_of_raising() -> None:
    """A DB reload can hot-add/edit a webhook-enabled repo without a secret
    while `GITHUB_WEBHOOK_SECRET` is empty. The per-request callable passed
    to the router must not let `_github_webhook_settings`'s boot-time
    `ClickException` escape into a live request — it should disable every
    repo instead, matching the router's own `None`-provider fallback
    (SYM-189 review fix)."""
    cfg = Config(
        repos=[
            RepoBinding(
                linear_team_key="WEB",
                github_repo="org/web",
                linear_states=LinearStates(ready="Todo", code_review="Needs Approval"),
            )
        ]
    )

    assert _live_github_webhook_settings(cfg) is None


async def _delivery_rows(conn: object) -> list[tuple[str, str]]:
    cur = await conn.execute("SELECT id, status FROM webhook_deliveries ORDER BY id")
    return [(str(row[0]), str(row[1])) for row in await cur.fetchall()]


@pytest.mark.asyncio
async def test_repo_secret_set_via_crud_hot_swaps_verifier(tmp_path: Path) -> None:
    """Acceptance (SYM-194): a webhook secret set through the config API is
    accepted by the verifier on the next request — no restart. The verifier
    reads its repo secrets from the shared `RepoSecretView` the config write
    path hot-swaps."""
    from symphony.db.config_repo_secrets import RepoSecretView

    db_path = tmp_path / "s.sqlite"
    conn = await db.connect(db_path)
    try:
        view = RepoSecretView()
        handler = _Handler()
        app = create_app(
            handler,
            conn,
            # enabled_repos=None → every repo enabled; repo secrets come live
            # from the shared view the CRUD router writes.
            github_webhook_settings=lambda: GitHubWebhookSettings(
                secret="", repo_secrets=view.as_map(), enabled_repos=None
            ),
            github_handler=handler,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_external_config=Config(linear_api_key="k"),
            ui_repo_secret_view=view,
        )
        body = _body(_pull_request_payload())
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            # Before the secret exists, the signed delivery is rejected.
            pre = await client.post(
                "/github/webhook",
                content=body,
                headers=_headers(body, secret="hot-secret", delivery="d1"),
            )
            assert pre.status_code == 401

            # Operator sets the repo's webhook secret through the config API.
            created = await client.post(
                "/api/config/bindings",
                json={
                    "payload": {
                        "project_key": "ENG",
                        "github_repo": "org/repo",
                        "states": {"ready": "Todo"},
                        "webhook_secret": "hot-secret",
                    }
                },
            )
            assert created.status_code == 201, created.text

            # The very next signed delivery verifies — no restart.
            post = await client.post(
                "/github/webhook",
                content=body,
                headers=_headers(body, secret="hot-secret", delivery="d2"),
            )
            assert post.status_code == 200, post.text
            assert handler.events and handler.events[-1].repo == "org/repo"
    finally:
        await conn.close()


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
async def test_repo_matching_is_case_insensitive(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        handler = _Handler()
        app = create_app(
            handler,
            conn,
            github_webhook_settings=GitHubWebhookSettings(
                repo_secrets={"org/repo": REPO_SECRET},
                enabled_repos=frozenset({"org/repo"}),
            ),
            ui_enabled=False,
            clock=lambda: NOW,
        )
        payload = _pull_request_payload()
        payload["repository"] = {"full_name": "Org/Repo"}
        body = _body(payload)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/github/webhook",
                content=body,
                headers=_headers(body, secret=REPO_SECRET),
            )

        assert response.status_code == 200
        assert handler.events[0].repo == "Org/Repo"
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
