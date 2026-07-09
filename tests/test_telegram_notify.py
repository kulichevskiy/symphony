"""Telegram attention-needed notifications (SYM-171).

Covers the pure notifier + dedupe DAO, then the three orchestrator hook
points (operator-wait created, run failed, PR merged).
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from symphony import db, notify
from symphony.config import Config, LinearStates, RepoBinding
from symphony.linear.client import LinearIssue
from symphony.orchestrator.poll import Orchestrator
from symphony.orchestrator.poll._base import _PendingDelivery
from symphony.pipeline.acceptance_classifier import AcceptanceVerdict
from symphony.pipeline.cost_guard import UsageDelta

# --- pure notifier ---------------------------------------------------------


def test_build_message_includes_identifier_and_deep_link() -> None:
    msg = notify.build_message(
        event=notify.EVENT_PR_MERGED,
        issue_identifier="ENG-1",
        issue_url="https://linear.app/team/issue/ENG-1",
    )
    assert "ENG-1" in msg
    assert "https://linear.app/team/issue/ENG-1" in msg


def test_build_message_truncates_oversized_detail() -> None:
    msg = notify.build_message(
        event=notify.EVENT_RUN_FAILED,
        issue_identifier="ENG-1",
        issue_url="https://linear.app/team/issue/ENG-1",
        detail="x" * 20_000,
    )
    assert len(msg.encode("utf-8")) <= notify.MESSAGE_LIMIT
    assert "ENG-1" in msg
    assert "https://linear.app/team/issue/ENG-1" in msg


def test_notifier_disabled_when_unset() -> None:
    assert notify.TelegramNotifier(bot_token="", chat_id="").enabled is False
    assert notify.TelegramNotifier(bot_token="t", chat_id="").enabled is False
    assert notify.TelegramNotifier(bot_token="", chat_id="c").enabled is False
    assert notify.TelegramNotifier(bot_token="t", chat_id="c").enabled is True


@pytest.mark.asyncio
async def test_notifier_noop_when_disabled() -> None:
    calls: list[tuple[str, str, str]] = []

    async def fake_send(token: str, chat_id: str, text: str) -> None:
        calls.append((token, chat_id, text))

    notifier = notify.TelegramNotifier(bot_token="", chat_id="", send_fn=fake_send)
    assert await notifier.send("hi") is False
    assert calls == []


@pytest.mark.asyncio
async def test_notifier_sends_when_enabled() -> None:
    calls: list[tuple[str, str, str]] = []

    async def fake_send(token: str, chat_id: str, text: str) -> None:
        calls.append((token, chat_id, text))

    notifier = notify.TelegramNotifier(bot_token="t", chat_id="c", send_fn=fake_send)
    assert await notifier.send("hi") is True
    assert calls == [("t", "c", "hi")]


@pytest.mark.asyncio
async def test_http_send_makes_one_call_to_bot_api() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"ok": True})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        await notify._http_send("TOKEN", "CHAT", "hello", client=client)
    finally:
        await client.aclose()

    assert len(requests) == 1
    assert str(requests[0].url) == "https://api.telegram.org/botTOKEN/sendMessage"
    body = json.loads(requests[0].content)
    assert body["chat_id"] == "CHAT"
    assert body["text"] == "hello"


# --- dedupe DAO ------------------------------------------------------------


@pytest.mark.asyncio
async def test_notifications_claim_dedupes(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        assert await db.notifications.claim(conn, "k1", "2026-07-09T00:00:00+00:00") is True
        assert await db.notifications.claim(conn, "k1", "2026-07-09T00:01:00+00:00") is False
        assert await db.notifications.claim(conn, "k2", "2026-07-09T00:02:00+00:00") is True
    finally:
        await conn.close()


# --- orchestrator helper ---------------------------------------------------


class _FakeNotifier:
    def __init__(self, *, enabled: bool = True) -> None:
        self._enabled = enabled
        self.sent: list[str] = []

    @property
    def enabled(self) -> bool:
        return self._enabled

    async def send(self, text: str) -> bool:
        self.sent.append(text)
        return True


def _binding() -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        linear_states=LinearStates(
            ready="Todo",
            code_review="In Review",
            needs_approval="Needs Input",
            done="Done",
        ),
    )


def _issue() -> LinearIssue:
    return LinearIssue(
        id="iss-1",
        identifier="ENG-1",
        title="Add auth",
        description="Need OAuth.",
        url="https://linear.app/team/issue/ENG-1",
        state_id="state-review",
        state_name="In Review",
        state_type="started",
        team_key="ENG",
    )


def _states() -> dict[str, dict[str, str]]:
    return {
        "ENG": {
            "Todo": "state-todo",
            "In Progress": "state-progress",
            "In Review": "state-review",
            "Needs Input": "state-input",
            "Blocked": "state-bl",
            "Done": "state-done",
        }
    }


def _orch(conn, linear: AsyncMock) -> Orchestrator:  # type: ignore[no-untyped-def]
    orch = Orchestrator(
        Config(repos=[_binding()]),
        linear,
        conn,
        runner=MagicMock(),
        gh=MagicMock(),
        workspace=MagicMock(),
        push_fn=AsyncMock(),
    )
    orch._states = _states()  # noqa: SLF001
    return orch


@pytest.mark.asyncio
async def test_notify_attention_sends_once_then_dedupes(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        orch = _orch(conn, AsyncMock())
        fake = _FakeNotifier()
        orch._notifier = fake  # type: ignore[assignment]  # noqa: SLF001

        await orch._notify_attention(  # noqa: SLF001
            event=notify.EVENT_PR_MERGED,
            issue_identifier="ENG-1",
            issue_url="https://linear.app/team/issue/ENG-1",
            dedupe_key="pr_merged:iss-1:run-1",
        )
        assert len(fake.sent) == 1
        assert "ENG-1" in fake.sent[0]
        assert "https://linear.app/team/issue/ENG-1" in fake.sent[0]

        # Repeated poll → same event key → no second message.
        await orch._notify_attention(  # noqa: SLF001
            event=notify.EVENT_PR_MERGED,
            issue_identifier="ENG-1",
            issue_url="https://linear.app/team/issue/ENG-1",
            dedupe_key="pr_merged:iss-1:run-1",
        )
        assert len(fake.sent) == 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_notify_attention_releases_claim_on_send_failure(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        orch = _orch(conn, AsyncMock())
        failing = _FakeNotifier()
        failing.send = AsyncMock(side_effect=httpx.HTTPStatusError(  # type: ignore[method-assign]
            "400", request=MagicMock(), response=MagicMock()
        ))
        orch._notifier = failing  # type: ignore[assignment]  # noqa: SLF001

        await orch._notify_attention(  # noqa: SLF001
            event=notify.EVENT_PR_MERGED,
            issue_identifier="ENG-1",
            issue_url="https://linear.app/team/issue/ENG-1",
            dedupe_key="pr_merged:iss-1:run-1",
        )

        # The failed send must not leave the event permanently claimed —
        # a later poll has to be able to retry it.
        assert await db.notifications.claim(conn, "pr_merged:iss-1:run-1", "t") is True
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_notify_attention_noop_when_disabled(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        orch = _orch(conn, AsyncMock())
        fake = _FakeNotifier(enabled=False)
        orch._notifier = fake  # type: ignore[assignment]  # noqa: SLF001

        await orch._notify_attention(  # noqa: SLF001
            event=notify.EVENT_PR_MERGED,
            issue_identifier="ENG-1",
            issue_url="https://linear.app/team/issue/ENG-1",
            dedupe_key="pr_merged:iss-1:run-1",
        )
        assert fake.sent == []
        # No dedupe row consumed while disabled.
        assert await db.notifications.claim(conn, "pr_merged:iss-1:run-1", "t") is True
    finally:
        await conn.close()


# --- wiring: the three events ----------------------------------------------


@pytest.mark.asyncio
async def test_merge_needs_approval_notifies_operator_wait(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        issue = _issue()
        await db.issues.upsert(
            conn, id=issue.id, identifier=issue.identifier, title=issue.title, team_key="ENG"
        )
        linear = AsyncMock()
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        orch = _orch(conn, linear)
        orch._notify_attention = AsyncMock()  # type: ignore[method-assign]  # noqa: SLF001

        await orch._mark_merge_needs_approval(  # noqa: SLF001
            binding=_binding(),
            issue=issue,
            pr_url="https://github.com/org/repo/pull/42",
            run_id="merge-run",
            reason="branch protection blocked",
            create_run=True,
        )

        orch._notify_attention.assert_awaited_once()  # type: ignore[attr-defined]  # noqa: SLF001
        kwargs = orch._notify_attention.await_args.kwargs  # type: ignore[attr-defined]  # noqa: SLF001
        assert kwargs["event"] == notify.EVENT_OPERATOR_WAIT
        assert kwargs["issue_identifier"] == "ENG-1"
        assert kwargs["issue_url"] == issue.url
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_mark_merge_done_notifies_pr_merged(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        issue = _issue()
        await db.issues.upsert(
            conn, id=issue.id, identifier=issue.identifier, title=issue.title, team_key="ENG"
        )
        await db.issue_prs.upsert(
            conn,
            issue_id=issue.id,
            github_repo="org/repo",
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            created_at="2026-05-10T00:00:00+00:00",
        )
        await db.runs.create(
            conn,
            id="merge-run",
            issue_id=issue.id,
            stage="merge",
            status="running",
            pid=None,
            started_at="2026-05-10T00:01:00+00:00",
        )
        linear = AsyncMock()
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        orch = _orch(conn, linear)
        orch._workspace.cleanup = AsyncMock(return_value=[])  # type: ignore[attr-defined]  # noqa: SLF001
        orch._notify_attention = AsyncMock()  # type: ignore[method-assign]  # noqa: SLF001

        await orch._mark_merge_done(  # noqa: SLF001
            binding=_binding(),
            issue=issue,
            pr_url="https://github.com/org/repo/pull/42",
            run_id="merge-run",
        )

        orch._notify_attention.assert_awaited_once()  # type: ignore[attr-defined]  # noqa: SLF001
        kwargs = orch._notify_attention.await_args.kwargs  # type: ignore[attr-defined]  # noqa: SLF001
        assert kwargs["event"] == notify.EVENT_PR_MERGED
        assert kwargs["issue_identifier"] == "ENG-1"
        assert kwargs["issue_url"] == issue.url
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_fail_review_run_notifies_only_on_operator_wait(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        issue = _issue()
        await db.issues.upsert(
            conn, id=issue.id, identifier=issue.identifier, title=issue.title, team_key="ENG"
        )
        run = db.runs.Run(
            id="review-run",
            issue_id=issue.id,
            stage="review",
            status="running",
            pid=None,
            started_at="2026-05-10T00:00:00+00:00",
            ended_at=None,
            cost_usd=0.0,
        )
        await db.runs.create(
            conn,
            id=run.id,
            issue_id=run.issue_id,
            stage=run.stage,
            status=run.status,
            pid=run.pid,
            started_at=run.started_at,
        )
        linear = AsyncMock()
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        orch = _orch(conn, linear)
        orch._notify_attention = AsyncMock()  # type: ignore[method-assign]  # noqa: SLF001

        # auto-retry (transient) failure — no operator attention, no message.
        await orch._fail_review_run(  # noqa: SLF001
            run=run,
            binding=_binding(),
            issue=issue,
            error="transient",
            last_log="",
            auto_retry=True,
            operator_wait=False,
        )
        orch._notify_attention.assert_not_awaited()  # type: ignore[attr-defined]  # noqa: SLF001

        # operator-facing failure — fires once.
        await orch._fail_review_run(  # noqa: SLF001
            run=run,
            binding=_binding(),
            issue=issue,
            error="review fix-run failed",
            last_log="",
            auto_retry=False,
            operator_wait=True,
        )
        orch._notify_attention.assert_awaited_once()  # type: ignore[attr-defined]  # noqa: SLF001
        kwargs = orch._notify_attention.await_args.kwargs  # type: ignore[attr-defined]  # noqa: SLF001
        assert kwargs["event"] == notify.EVENT_RUN_FAILED
        assert kwargs["issue_url"] == issue.url
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_implement_blocked_notifies_operator_wait(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        issue = _issue()
        await db.issues.upsert(
            conn, id=issue.id, identifier=issue.identifier, title=issue.title, team_key="ENG"
        )
        await db.runs.create(
            conn,
            id="impl-run",
            issue_id=issue.id,
            stage="implement",
            status="running",
            pid=1234,
            started_at="2026-05-10T00:00:00+00:00",
        )
        linear = AsyncMock()
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        orch = _orch(conn, linear)
        orch._notify_attention = AsyncMock()  # type: ignore[method-assign]  # noqa: SLF001

        await orch._block_implement_run(  # noqa: SLF001
            "impl-run",
            "needs a decision on library X",
            issue=issue,
            rollback_state_id="state-todo",
            binding=_binding(),
        )

        orch._notify_attention.assert_awaited_once()  # type: ignore[attr-defined]  # noqa: SLF001
        kwargs = orch._notify_attention.await_args.kwargs  # type: ignore[attr-defined]  # noqa: SLF001
        assert kwargs["event"] == notify.EVENT_OPERATOR_WAIT
        assert kwargs["issue_identifier"] == "ENG-1"
        assert kwargs["issue_url"] == issue.url
        assert kwargs["dedupe_key"] == "operator_wait:impl-run"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_deliver_failed_notifies_run_failed(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        issue = _issue()
        await db.issues.upsert(
            conn, id=issue.id, identifier=issue.identifier, title=issue.title, team_key="ENG"
        )
        await db.runs.create(
            conn,
            id="deliver-run",
            issue_id=issue.id,
            stage="implement",
            status="running",
            pid=1234,
            started_at="2026-05-10T00:00:00+00:00",
        )
        linear = AsyncMock()
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        orch = _orch(conn, linear)
        orch._notify_attention = AsyncMock()  # type: ignore[method-assign]  # noqa: SLF001

        ctx = _PendingDelivery(
            binding=_binding(),
            issue=issue,
            storage_issue_id=issue.id,
            run_id="deliver-run",
            workspace_path=tmp_path / "ws",
            branch="symphony/eng-1",
            cumulative_usage=UsageDelta(),
            local_review_result=None,
        )

        await orch._park_deliver_failed("push failed: boom", ctx=ctx)  # noqa: SLF001

        orch._notify_attention.assert_awaited_once()  # type: ignore[attr-defined]  # noqa: SLF001
        kwargs = orch._notify_attention.await_args.kwargs  # type: ignore[attr-defined]  # noqa: SLF001
        assert kwargs["event"] == notify.EVENT_RUN_FAILED
        assert kwargs["issue_identifier"] == "ENG-1"
        assert kwargs["issue_url"] == issue.url
        assert kwargs["dedupe_key"] == "run_failed:deliver-run"
        assert kwargs["detail"] == "push failed: boom"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_acceptance_blocked_notifies_operator_wait(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        issue = _issue()
        await db.issues.upsert(
            conn, id=issue.id, identifier=issue.identifier, title=issue.title, team_key="ENG"
        )
        await db.runs.create(
            conn,
            id="accept-run",
            issue_id=issue.id,
            stage="acceptance",
            status="running",
            pid=1234,
            started_at="2026-05-10T00:00:00+00:00",
        )
        linear = AsyncMock()
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        orch = _orch(conn, linear)
        orch._notify_attention = AsyncMock()  # type: ignore[method-assign]  # noqa: SLF001

        verdict = AcceptanceVerdict(
            kind="blocked",
            criteria=[],
            cost=0.0,
            hero_screenshot_url="",
            details="needs a human call on scope",
        )
        await orch._track_acceptance_blocked_wait(  # noqa: SLF001
            binding=_binding(),
            issue=issue,
            pr_number=42,
            run_id="accept-run",
            verdict=verdict,
        )

        orch._notify_attention.assert_awaited_once()  # type: ignore[attr-defined]  # noqa: SLF001
        kwargs = orch._notify_attention.await_args.kwargs  # type: ignore[attr-defined]  # noqa: SLF001
        assert kwargs["event"] == notify.EVENT_OPERATOR_WAIT
        assert kwargs["issue_identifier"] == "ENG-1"
        assert kwargs["issue_url"] == issue.url
        assert kwargs["dedupe_key"] == "operator_wait:accept-run"
        assert kwargs["detail"] == "needs a human call on scope"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_acceptance_rejected_notifies_operator_wait(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        issue = _issue()
        await db.issues.upsert(
            conn, id=issue.id, identifier=issue.identifier, title=issue.title, team_key="ENG"
        )
        await db.runs.create(
            conn,
            id="accept-run",
            issue_id=issue.id,
            stage="acceptance",
            status="running",
            pid=1234,
            started_at="2026-05-10T00:00:00+00:00",
        )
        # Already at the fix-iteration cap, so the rejection opens an operator
        # wait directly instead of dispatching another fix-run.
        await db.acceptance_state.bump_iteration(conn, issue.id)
        linear = AsyncMock()
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        orch = _orch(conn, linear)
        orch._notify_attention = AsyncMock()  # type: ignore[method-assign]  # noqa: SLF001

        verdict = AcceptanceVerdict(
            kind="rejected",
            criteria=[],
            cost=0.0,
            hero_screenshot_url="",
            details="button color does not match spec",
        )
        await orch._finalize_acceptance_verdict(  # noqa: SLF001
            run_id="accept-run",
            binding=_binding(),
            issue=issue,
            pr_number=42,
            pr_url="https://github.com/org/repo/pull/42",
            pr_head_sha="deadbeef",
            verdict=verdict,
        )

        orch._notify_attention.assert_awaited_once()  # type: ignore[attr-defined]  # noqa: SLF001
        kwargs = orch._notify_attention.await_args.kwargs  # type: ignore[attr-defined]  # noqa: SLF001
        assert kwargs["event"] == notify.EVENT_OPERATOR_WAIT
        assert kwargs["issue_identifier"] == "ENG-1"
        assert kwargs["issue_url"] == issue.url
        assert kwargs["dedupe_key"] == "operator_wait:accept-run"
        assert kwargs["detail"] == "button color does not match spec"
    finally:
        await conn.close()
