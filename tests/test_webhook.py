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
from symphony.app import build_server_config
from symphony.config import Config, LinearStates, RepoBinding
from symphony.linear.client import LinearComment, LinearError, LinearIssue
from symphony.orchestrator.poll import Orchestrator
from symphony.webhook import WebhookSettings, create_app

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


def _payload(body: str = "$stop") -> dict[str, Any]:
    return {
        "type": "Comment",
        "action": "create",
        "webhookTimestamp": int(NOW.timestamp() * 1000),
        "data": {
            "id": "c1",
            "body": body,
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
            response = await client.post("/linear/webhook", content=body, headers=_headers(body))

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
async def test_pending_delivery_id_is_retryable_without_handling(
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
        assert (
            await db.webhook_deliveries.begin(
                conn,
                "evt-pending",
                received_at=NOW,
                ttl_secs=600,
            )
            == "new"
        )

        body = _body(_payload())
        headers = _headers(body, delivery="evt-pending")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            pending = await client.post("/linear/webhook", content=body, headers=headers)
            await db.webhook_deliveries.finish(conn, "evt-pending")
            duplicate = await client.post("/linear/webhook", content=body, headers=headers)

        assert pending.status_code == 503
        assert duplicate.status_code == 200
        assert duplicate.json() == {"status": "duplicate", "handled": False}
        assert handler.payloads == []
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
        linear_states=LinearStates(ready="Todo", code_review="Needs Approval"),
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
        linear.comments_since = AsyncMock(return_value=[_comment("$stop")])
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
async def test_webhook_comment_uses_scoped_active_run_for_tracker_issue_id(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        default_binding = _binding()
        secondary_binding = _binding()
        secondary_binding.tracker_provider = "linear-alt"
        secondary_binding.tracker_site = "secondary"
        cfg = Config(repos=[default_binding, secondary_binding])
        linear = AsyncMock()
        orch = _make_orch(cfg, linear, conn)
        await db.issues.upsert(
            conn,
            id="iss-1",
            identifier="ENG-0",
            title="default",
            team_key="ENG",
            provider=default_binding.tracker_provider,
            site=default_binding.tracker_site,
        )
        scoped_issue_id = await db.issues.upsert(
            conn,
            id="iss-1",
            identifier="ENG-1",
            title="secondary",
            team_key="ENG",
            provider=secondary_binding.tracker_provider,
            site=secondary_binding.tracker_site,
        )
        await db.runs.create(
            conn,
            id="run-1",
            issue_id=scoped_issue_id,
            stage="implement",
            status="running",
            pid=None,
            started_at="2026-05-11T11:00:00+00:00",
        )
        orch._active_run_ids.add("run-1")  # noqa: SLF001
        orch._dispatch_run_ids[scoped_issue_id] = "run-1"  # noqa: SLF001

        result = await orch.handle_linear_webhook(_payload())

        assert result.handled is True
        orch._runner.kill.assert_awaited_once_with(  # type: ignore[attr-defined]  # noqa: SLF001
            "run-1"
        )
        assert await db.comment_cursors.get(conn, "iss-1") is None
        cursor = await db.comment_cursors.get(conn, scoped_issue_id)
        assert cursor == ("2026-05-11T12:00:00+00:00", ["c1"])
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_webhook_comment_swallows_slash_handler_failure(tmp_path: Path) -> None:
    """SYM-32: when a slash handler raises `SlashHandlerFailure` while
    processing a Linear comment webhook, `_handle_webhook_comment` must
    return a successful `WebhookDispatchResult` (not let the exception bubble
    into `src/symphony/webhook.py`). The rejection has already been posted
    inside the lock and the comment is intentionally NOT marked seen so the
    next poll tick retries — but the webhook delivery dedupe claim must stay
    in place so Linear doesn't retry the webhook and trigger another
    rejection comment per retry attempt."""
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.move_issue = AsyncMock(side_effect=LinearError("upstream 503"))
        linear.post_comment = AsyncMock(return_value="cmt-1")
        orch = _make_orch(cfg, linear, conn)
        await _seed_active_run(conn, issue_id="iss-1", run_id="run-1")
        await db.runs.update_status(
            conn,
            "run-1",
            "failed",
            ended_at="2026-05-10T01:00:00+00:00",
        )
        await db.operator_waits.upsert(
            conn,
            issue_id="iss-1",
            run_id="run-1",
            kind=db.operator_waits.KIND_IMPLEMENT_FAILED,
            linear_team_key="ENG",
            github_repo="org/repo",
            issue_label="",
            created_at="2026-05-10T01:00:00+00:00",
        )

        result = await orch.handle_linear_webhook(_payload("$retry"))

        # No exception leaked; webhook reports handled so the dedupe claim sticks
        # and Linear does not retry the delivery.
        assert result.handled is True
        # Rejection was posted to Linear.
        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("$retry" in body and "ignored" in body for body in bodies), (
            f"expected a command_rejected body, got {bodies!r}"
        )
        # Operator wait survives — the next poll tick retries.
        assert await db.operator_waits.get(conn, "iss-1") is not None
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
async def test_poll_and_webhook_comment_share_post_success_marker(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.comments_since = AsyncMock(return_value=[_comment("$stop")])
        orch = _make_orch(cfg, linear, conn)
        await _seed_active_run(conn, issue_id="iss-1", run_id="run-1")
        orch._active_run_ids.add("run-1")  # noqa: SLF001
        orch._dispatch_run_ids["iss-1"] = "run-1"  # noqa: SLF001
        kill_started = asyncio.Event()
        release_kill = asyncio.Event()

        async def slow_kill(_run_id: str) -> None:
            kill_started.set()
            await release_kill.wait()

        orch._runner.kill.side_effect = slow_kill  # type: ignore[attr-defined]  # noqa: SLF001

        poll_task = asyncio.create_task(orch._poll_slash_commands())  # noqa: SLF001
        await kill_started.wait()
        webhook_task = asyncio.create_task(orch.handle_linear_webhook(_payload()))
        await asyncio.sleep(0)
        release_kill.set()

        await poll_task
        duplicate = await webhook_task

        assert duplicate.handled is False
        assert orch._runner.kill.await_count == 1  # type: ignore[attr-defined]  # noqa: SLF001
        assert await db.comment_events.seen(conn, "c1")
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_poll_marks_comment_only_after_successful_slash_handling(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        linear.comments_since = AsyncMock(return_value=[_comment("$stop")])
        orch = _make_orch(cfg, linear, conn)
        await _seed_active_run(conn, issue_id="iss-1", run_id="run-1")
        orch._active_run_ids.add("run-1")  # noqa: SLF001
        orch._dispatch_run_ids["iss-1"] = "run-1"  # noqa: SLF001

        async def fail_intent(
            _issue_id: str,
            _run_id: str,
            _intent: object,
        ) -> None:
            raise RuntimeError("boom")

        orch._handle_slash_intent = fail_intent  # type: ignore[method-assign]  # noqa: SLF001

        with pytest.raises(RuntimeError, match="boom"):
            await orch._poll_slash_commands()  # noqa: SLF001

        assert not await db.comment_events.seen(conn, "c1")

        orch._handle_slash_intent = AsyncMock()  # type: ignore[method-assign]  # noqa: SLF001
        await orch._poll_slash_commands()  # noqa: SLF001

        assert await db.comment_events.seen(conn, "c1")
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


@pytest.mark.asyncio
async def test_webhook_issue_state_change_triggers_reconcile_hook(
    tmp_path: Path,
) -> None:
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
        spy = MagicMock()
        started = asyncio.Event()
        release = asyncio.Event()

        async def slow_reconcile(*, issue_id: str, action: str) -> int:
            started.set()
            await release.wait()
            return 2

        spy.reconcile_linear_issue_event = AsyncMock(side_effect=slow_reconcile)
        orch._reconciler = spy  # noqa: SLF001

        try:
            result = await asyncio.wait_for(
                orch.handle_linear_webhook(
                    {
                        "type": "Issue",
                        "action": "update",
                        "webhookTimestamp": int(NOW.timestamp() * 1000),
                        "updatedFrom": {"stateId": "old-state"},
                        "data": {"id": "iss-1", "state": {"id": "state-todo"}},
                    }
                ),
                timeout=1,
            )
            await asyncio.wait_for(started.wait(), timeout=1)
            assert result.handled is True
            release.set()
            await orch.drain_reconcile_event_tasks()
        finally:
            release.set()
            await orch.drain_reconcile_event_tasks(cancel=True)
        await task

        spy.reconcile_linear_issue_event.assert_awaited_once_with(
            issue_id="iss-1",
            action="update",
        )
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_webhook_issue_update_without_state_change_skips_reconcile_hook(
    tmp_path: Path,
) -> None:
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
        spy = MagicMock()
        spy.reconcile_linear_issue_event = AsyncMock(return_value=2)
        orch._reconciler = spy  # noqa: SLF001

        result = await orch.handle_linear_webhook(
            {
                "type": "Issue",
                "action": "update",
                "webhookTimestamp": int(NOW.timestamp() * 1000),
                "updatedFrom": {"title": "Old title"},
                "data": {"id": "iss-1", "state": {"id": "state-todo"}},
            }
        )
        await task

        assert result.handled is True
        spy.reconcile_linear_issue_event.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_ready_issue_schedule_claim_is_atomic(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        cfg = Config(repos=[_binding()])
        linear = AsyncMock()
        orch = _make_orch(cfg, linear, conn)
        binding = _binding()
        issue = _issue()
        release = asyncio.Event()

        async def hold_dispatch(_binding: RepoBinding, _issue: LinearIssue) -> None:
            await release.wait()

        orch._dispatch_with_limits = hold_dispatch  # type: ignore[method-assign]  # noqa: SLF001

        first, second = await asyncio.gather(
            orch._schedule_ready_issue(binding, issue),  # noqa: SLF001
            orch._schedule_ready_issue(binding, issue),  # noqa: SLF001
        )
        release.set()
        await orch.drain_dispatch_tasks()

        assert sum(task is not None for task in (first, second)) == 1
    finally:
        await conn.close()
