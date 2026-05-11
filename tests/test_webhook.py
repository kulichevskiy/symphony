from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from pydantic import ValidationError

from symphony import db
from symphony.config import Config, LinearStates, RepoBinding
from symphony.linear.client import LinearComment, LinearIssue
from symphony.orchestrator.poll import Orchestrator
from symphony.webhook import WebhookSettings, build_server_config, create_app

SECRET = "whsec_test"
NOW = datetime(2026, 5, 11, 12, 0, tzinfo=UTC)


@dataclass
class _Result:
    kind: str
    handled: bool


class _Handler:
    def __init__(self) -> None:
        self.payloads: list[dict[str, Any]] = []

    async def handle_linear_webhook(self, payload: dict[str, Any]) -> _Result:
        self.payloads.append(payload)
        return _Result(kind=str(payload.get("type", "")), handled=True)


def _body(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode()


def _headers(body: bytes, *, delivery: str = "evt-1") -> dict[str, str]:
    signature = hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()
    return {
        "Linear-Signature": signature,
        "Linear-Delivery": delivery,
        "Content-Type": "application/json",
    }


def _payload() -> dict[str, Any]:
    return {
        "type": "Comment",
        "action": "create",
        "webhookTimestamp": int(NOW.timestamp() * 1000),
        "data": {
            "id": "c1",
            "body": "/stop",
            "createdAt": "2026-05-11T12:00:00+00:00",
            "issueId": "iss-1",
        },
    }


@pytest.mark.asyncio
async def test_valid_webhook_is_hmac_verified_and_handled(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        handler = _Handler()
        app = create_app(
            handler,
            conn,
            WebhookSettings(secret=SECRET),
            clock=lambda: NOW,
        )
        body = _body(_payload())
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/linear/webhook", content=body, headers=_headers(body)
            )

        assert response.status_code == 200
        assert response.json()["handled"] is True
        assert len(handler.payloads) == 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_duplicate_delivery_id_is_200_noop(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        handler = _Handler()
        app = create_app(
            handler,
            conn,
            WebhookSettings(secret=SECRET),
            clock=lambda: NOW,
        )
        body = _body(_payload())
        headers = _headers(body, delivery="evt-duplicate")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            first = await client.post("/linear/webhook", content=body, headers=headers)
            second = await client.post("/linear/webhook", content=body, headers=headers)

        assert first.status_code == 200
        assert second.status_code == 200
        assert second.json() == {"status": "duplicate", "handled": False}
        assert len(handler.payloads) == 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_bad_or_missing_hmac_returns_401_without_parsing_body(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        handler = _Handler()
        app = create_app(
            handler,
            conn,
            WebhookSettings(secret=SECRET),
            clock=lambda: NOW,
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            bad = await client.post(
                "/linear/webhook",
                content=b"{not-json",
                headers={"Linear-Signature": "bad", "Linear-Delivery": "evt-1"},
            )
            missing = await client.post(
                "/linear/webhook",
                content=_body(_payload()),
                headers={"Linear-Delivery": "evt-2"},
            )

        assert bad.status_code == 401
        assert missing.status_code == 401
        assert handler.payloads == []
    finally:
        await conn.close()


def test_webhook_server_config_is_loopback_only() -> None:
    app = create_app(_Handler(), MagicMock(), WebhookSettings(secret=SECRET))

    cfg = build_server_config(app)

    assert cfg.host == "127.0.0.1"
    with pytest.raises(ValueError, match="127\\.0\\.0\\.1"):
        build_server_config(app, host="0.0.0.0")
    with pytest.raises(ValidationError):
        Config.model_validate({"webhook_host": "0.0.0.0"})


def _binding() -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        linear_states=LinearStates(ready="Todo"),
    )


def _issue(uid: str = "iss-1", ident: str = "ENG-1") -> LinearIssue:
    return LinearIssue(
        id=uid,
        identifier=ident,
        title="t",
        description="",
        url="https://linear.app/x",
        state_id="state-todo",
        state_name="Todo",
        state_type="unstarted",
        team_key="ENG",
        labels=[],
    )


def _comment(
    body: str,
    *,
    cid: str = "c1",
    created_at: str = "2026-05-11T12:00:00+00:00",
) -> LinearComment:
    return LinearComment(
        id=cid,
        body=body,
        created_at=created_at,
        author_name="user",
        author_is_me=False,
        external_thread_type=None,
    )


def _make_orch(cfg: Config, linear: AsyncMock, conn: object) -> Orchestrator:
    runner = MagicMock()
    runner.kill = AsyncMock()
    workspace = MagicMock()
    workspace.acquire = AsyncMock(return_value=Path("/dev/null"))
    workspace.release = MagicMock()
    gh = MagicMock()
    push_fn = AsyncMock()
    orch = Orchestrator(
        cfg,
        linear,
        conn,  # type: ignore[arg-type]
        runner=runner,
        gh=gh,
        workspace=workspace,
        push_fn=push_fn,
    )
    orch._states = {  # noqa: SLF001
        "ENG": {
            "Todo": "state-todo",
            "In Progress": "state-progress",
            "Blocked": "state-blocked",
        }
    }
    return orch


async def _seed_active_run(conn: object, *, issue_id: str, run_id: str) -> None:
    await db.issues.upsert(
        conn,  # type: ignore[arg-type]
        id=issue_id,
        identifier="ENG-1",
        title="t",
        team_key="ENG",
    )
    await db.runs.create(
        conn,  # type: ignore[arg-type]
        id=run_id,
        issue_id=issue_id,
        stage="implement",
        status="running",
        pid=None,
        started_at="2026-05-11T11:00:00+00:00",
    )


@pytest.mark.asyncio
async def test_webhook_comment_uses_slash_path_and_poll_does_not_refire(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.comments_since = AsyncMock(return_value=[_comment("/stop")])
        orch = _make_orch(cfg, linear, conn)
        await _seed_active_run(conn, issue_id="iss-1", run_id="run-1")
        orch._active_run_ids.add("run-1")  # noqa: SLF001
        orch._dispatch_run_ids["iss-1"] = "run-1"  # noqa: SLF001

        result = await orch.handle_linear_webhook(_payload())
        duplicate = await orch.handle_linear_webhook(_payload())
        await orch._poll_slash_commands()  # noqa: SLF001

        assert result.handled is True
        assert duplicate.handled is False
        assert orch._runner.kill.await_count == 1  # type: ignore[attr-defined]  # noqa: SLF001
        cursor = await db.comment_cursors.get(conn, "iss-1")
        assert cursor == ("2026-05-11T12:00:00+00:00", ["c1"])
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_webhook_comment_does_not_drop_older_out_of_order_command(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        orch = _make_orch(cfg, linear, conn)
        await _seed_active_run(conn, issue_id="iss-1", run_id="run-1")
        orch._active_run_ids.add("run-1")  # noqa: SLF001
        orch._dispatch_run_ids["iss-1"] = "run-1"  # noqa: SLF001
        await db.comment_cursors.set(
            conn,
            "iss-1",
            "2026-05-11T12:01:00+00:00",
            {"newer-noise"},
        )

        result = await orch.handle_linear_webhook(_payload())

        assert result.handled is True
        assert orch._runner.kill.await_count == 1  # type: ignore[attr-defined]  # noqa: SLF001
        cursor = await db.comment_cursors.get(conn, "iss-1")
        assert cursor == ("2026-05-11T12:01:00+00:00", ["newer-noise"])
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_webhook_issue_event_schedules_ready_issue(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        orch = _make_orch(cfg, linear, conn)

        async def done() -> None:
            return None

        task = asyncio.create_task(done())
        orch._schedule_dispatch = MagicMock(return_value=task)  # type: ignore[method-assign]  # noqa: SLF001

        result = await orch.handle_linear_webhook(
            {
                "type": "Issue",
                "action": "update",
                "webhookTimestamp": int(NOW.timestamp() * 1000),
                "data": {"id": "iss-1"},
            }
        )
        await task

        assert result.handled is True
        orch._schedule_dispatch.assert_called_once()  # type: ignore[attr-defined]  # noqa: SLF001
    finally:
        await conn.close()
