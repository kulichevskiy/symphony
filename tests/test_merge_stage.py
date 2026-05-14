"""Orchestrator-level Merge stage tests."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from symphony import db
from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.config import Config, LinearStates, RepoBinding
from symphony.github.client import CheckRun, GitHub, GitHubError, PRChecks
from symphony.linear.client import LinearError, LinearIssue
from symphony.orchestrator.poll import Orchestrator, _binding_storage_key


class _FakeRunner:
    def __init__(self, events: list[RunnerEvent]) -> None:
        self.events = events
        self.kill_calls: list[str] = []
        self.captured_spec: RunnerSpec | None = None

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.captured_spec = spec
        return self._aiter()

    async def _aiter(self) -> AsyncIterator[RunnerEvent]:
        for ev in self.events:
            yield ev

    async def kill(self, run_id: str) -> None:
        self.kill_calls.append(run_id)


class _BlockingRunner:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.captured_spec: RunnerSpec | None = None

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.captured_spec = spec
        return self._aiter()

    async def _aiter(self) -> AsyncIterator[RunnerEvent]:
        self.started.set()
        yield RunnerEvent(kind="started", pid=123)
        await self.release.wait()
        yield RunnerEvent(kind="exit", returncode=0)

    async def kill(self, run_id: str) -> None:
        self.release.set()


async def _poll_and_wait(orch: Orchestrator) -> None:
    tasks = await orch._poll_merge_candidates()  # noqa: SLF001
    if tasks:
        await asyncio.gather(*tasks)


def _binding(
    *,
    agent: str = "codex",
    issue_label: str | None = None,
    branch_prefix: str = "symphony",
) -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        agent=agent,  # type: ignore[arg-type]
        issue_label=issue_label,
        branch_prefix=branch_prefix,
        linear_states=LinearStates(ready="Todo"),
    )


def _issue() -> LinearIssue:
    return LinearIssue(
        id="iss-1",
        identifier="ENG-1",
        title="Add auth",
        description="Need OAuth.",
        url="https://linear.app/team/issue/ENG-1",
        state_id="state-progress",
        state_name="In Progress",
        state_type="started",
        team_key="ENG",
        labels=["feature"],
    )


def _ready_issue(issue_id: str = "iss-2", identifier: str = "ENG-2") -> LinearIssue:
    return LinearIssue(
        id=issue_id,
        identifier=identifier,
        title="Fresh task",
        description="Start later.",
        url=f"https://linear.app/team/issue/{identifier}",
        state_id="state-todo",
        state_name="Todo",
        state_type="unstarted",
        team_key="ENG",
        labels=["feature"],
    )


def _states() -> dict[str, str]:
    return {
        "Todo": "state-todo",
        "In Progress": "state-progress",
        "Needs Approval": "state-na",
        "Blocked": "state-bl",
        "Done": "state-done",
    }


async def _seed_review_candidate(
    conn, *, binding_key: str = ""
) -> None:  # type: ignore[no-untyped-def]
    await db.issues.upsert(
        conn,
        id="iss-1",
        identifier="ENG-1",
        title="Add auth",
        team_key="ENG",
    )
    await db.runs.create(
        conn,
        id="implement",
        issue_id="iss-1",
        stage="implement",
        status="completed",
        pid=None,
        started_at="2026-05-10T00:00:00+00:00",
        cost_usd=0.50,
    )
    await db.runs.create(
        conn,
        id="review",
        issue_id="iss-1",
        stage="review",
        status="completed",
        pid=None,
        started_at="2026-05-10T00:01:00+00:00",
    )
    await db.issue_prs.upsert(
        conn,
        issue_id="iss-1",
        github_repo="org/repo",
        binding_key=binding_key,
        pr_number=42,
        pr_url="https://github.com/org/repo/pull/42",
        created_at="2026-05-10T00:01:00+00:00",
    )


def _write_fake_gh(tmp_path: Path) -> tuple[Path, Path]:
    calls = tmp_path / "gh-calls.jsonl"
    merged_flag = tmp_path / "merged.flag"
    shim = tmp_path / "gh"
    pr_view = {
        "number": 42,
        "title": "Add auth",
        "state": "OPEN",
        "url": "https://github.com/org/repo/pull/42",
        "headRefName": "symphony/eng-1",
        "headRefOid": "abc123",
        "mergeable": "MERGEABLE",
        "isDraft": False,
        "mergedAt": None,
    }
    checks = [
        {
            "name": "test",
            "state": "SUCCESS",
            "bucket": "pass",
            "link": None,
        }
    ]
    reviews = [
        {
            "user": {"login": "reviewer"},
            "state": "APPROVED",
            "commit_id": "abc123",
            "submitted_at": "2026-05-10T00:03:00Z",
            "body": "ship it",
        }
    ]
    commit = {"commit": {"committer": {"date": "2026-05-10T00:02:00Z"}}}
    shim.write_text(
        "#!/usr/bin/env python3\n"
        "import json, sys\n"
        "from pathlib import Path\n"
        f"calls = {str(calls)!r}\n"
        f"merged_flag = Path({str(merged_flag)!r})\n"
        f"pr_view = json.loads({json.dumps(json.dumps(pr_view))})\n"
        f"checks = json.loads({json.dumps(json.dumps(checks))})\n"
        f"reviews = json.loads({json.dumps(json.dumps(reviews))})\n"
        f"commit = json.loads({json.dumps(json.dumps(commit))})\n"
        "argv = sys.argv[1:]\n"
        "with open(calls, 'a') as f:\n"
        "    f.write(json.dumps({'argv': argv}) + '\\n')\n"
        "joined = ' '.join(argv)\n"
        "if argv[:2] == ['pr', 'view']:\n"
        "    if merged_flag.exists():\n"
        "        pr_view['state'] = 'MERGED'\n"
        "        pr_view['mergedAt'] = '2026-05-10T00:04:00Z'\n"
        "    sys.stdout.write(json.dumps(pr_view)); sys.exit(0)\n"
        "if argv[:2] == ['pr', 'checks']:\n"
        "    sys.stdout.write(json.dumps(checks)); sys.exit(0)\n"
        "if 'repos/org/repo/pulls/42/comments' in joined:\n"
        "    sys.stdout.write('[]'); sys.exit(0)\n"
        "if 'repos/org/repo/pulls/42/reviews' in joined:\n"
        "    sys.stdout.write(json.dumps(reviews)); sys.exit(0)\n"
        "if 'repos/org/repo/issues/42/comments' in joined:\n"
        "    sys.stdout.write('[]'); sys.exit(0)\n"
        "if 'repos/org/repo/issues/42/reactions' in joined:\n"
        "    sys.stdout.write('[]'); sys.exit(0)\n"
        "if 'repos/org/repo/commits/abc123' in joined:\n"
        "    sys.stdout.write(json.dumps(commit)); sys.exit(0)\n"
        "if argv[:3] == ['pr', 'merge', '42']:\n"
        "    merged_flag.write_text('1')\n"
        "    sys.exit(0)\n"
        "sys.stderr.write('unexpected gh call: ' + joined)\n"
        "sys.exit(1)\n"
    )
    shim.chmod(0o755)
    return shim, calls


def _read_calls(calls: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in calls.read_text().splitlines()]


@pytest.mark.asyncio
async def test_merge_candidate_uses_recorded_binding_key(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        target = _binding(
            agent="codex",
            issue_label="backend",
            branch_prefix="backend",
        )
        await _seed_review_candidate(conn, binding_key=_binding_storage_key(target))
        cfg = Config(
            repos=[
                _binding(
                    agent="claude",
                    issue_label="frontend",
                    branch_prefix="frontend",
                ),
                target,
            ],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            AsyncMock(),
            conn,
            runner=MagicMock(),
            gh=MagicMock(),
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )

        candidate = (await db.issue_prs.list_merge_candidates(conn))[0]

        assert orch._binding_for_pr(candidate) == target  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_candidate_falls_back_when_recorded_binding_key_is_stale(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn, binding_key="old-shape:backend")
        target = _binding(
            agent="codex",
            issue_label="backend",
            branch_prefix="backend",
        )
        cfg = Config(
            repos=[target],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            AsyncMock(),
            conn,
            runner=MagicMock(),
            gh=MagicMock(),
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )

        candidate = (await db.issue_prs.list_merge_candidates(conn))[0]

        assert orch._binding_for_pr(candidate) == target  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_stale_binding_key_fallback_uses_recorded_label(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(
            conn,
            binding_key='["ENG","org/repo","backend","legacy"]',
        )
        target = _binding(
            agent="codex",
            issue_label="backend",
            branch_prefix="backend",
        )
        cfg = Config(
            repos=[
                _binding(
                    agent="claude",
                    issue_label="frontend",
                    branch_prefix="frontend",
                ),
                target,
            ],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            AsyncMock(),
            conn,
            runner=MagicMock(),
            gh=MagicMock(),
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )

        candidate = (await db.issue_prs.list_merge_candidates(conn))[0]

        assert orch._binding_for_pr(candidate) == target  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_ambiguous_binding_fallback_returns_none(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn, binding_key="old-shape:backend")
        cfg = Config(
            repos=[
                _binding(
                    agent="claude",
                    issue_label="frontend",
                    branch_prefix="frontend",
                ),
                _binding(
                    agent="codex",
                    issue_label="backend",
                    branch_prefix="backend",
                ),
            ],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            AsyncMock(),
            conn,
            runner=MagicMock(),
            gh=MagicMock(),
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )

        candidate = (await db.issue_prs.list_merge_candidates(conn))[0]

        assert orch._binding_for_pr(candidate) is None  # noqa: SLF001
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_green_review_and_ci_auto_merges_with_fake_gh(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        shim, calls_log = _write_fake_gh(tmp_path)
        result_line = json.dumps({"type": "result", "total_cost_usd": 0.25})
        runner = _FakeRunner(
            [
                RunnerEvent(kind="started", pid=123),
                RunnerEvent(kind="stdout", line=result_line),
                RunnerEvent(kind="exit", returncode=0),
            ]
        )
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock(return_value=None)
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock(return_value=None)
        linear.post_comment = AsyncMock(return_value="cmt-1")
        push_fn = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="codex")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=GitHub(gh_path=str(shim)),
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)

        assert runner.captured_spec is not None
        assert runner.captured_spec.stage == "merge"
        assert runner.captured_spec.command[0] == "codex"
        push_fn.assert_awaited_once()
        linear.move_issue.assert_awaited_once_with("iss-1", "state-done")
        workspace.cleanup.assert_awaited_once_with(_issue())

        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [r.stage for r in history] == ["implement", "review", "merge"]
        assert history[-1].status == "done"
        assert await db.runs.cost_for_issue(conn, "iss-1") == pytest.approx(0.75)

        comment_body = linear.post_comment.await_args.args[1]
        assert "Merge" in comment_body
        assert "Done" in comment_body
        assert "https://github.com/org/repo/pull/42" in comment_body
        assert "$0.7500" in comment_body

        calls = _read_calls(calls_log)
        merge_call = next(c for c in calls if c["argv"][:3] == ["pr", "merge", "42"])
        assert "--squash" in merge_call["argv"]
        assert "--auto" in merge_call["argv"]
        assert "--repo" in merge_call["argv"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_candidate_skips_when_issue_left_active_state(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        paused = _issue()
        paused.state_name = "Blocked"
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=paused)
        gh = MagicMock()
        gh.pr_view = AsyncMock()
        workspace = MagicMock()
        workspace.acquire = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )

        assert await orch._poll_merge_candidates() == []  # noqa: SLF001

        linear.lookup_issue.assert_awaited_once_with("iss-1")
        gh.pr_view.assert_not_awaited()
        workspace.acquire.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_candidate_skips_when_binding_label_no_longer_matches(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        binding = _binding(agent="claude", issue_label="backend")
        await _seed_review_candidate(conn, binding_key=_binding_storage_key(binding))
        linear = MagicMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        gh = MagicMock()
        gh.pr_view = AsyncMock(return_value={})
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org_srepo" / "eng-1")

        cfg = Config(
            repos=[binding],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )

        assert await orch._poll_merge_candidates() == []  # noqa: SLF001

        linear.lookup_issue.assert_awaited_once_with("iss-1")
        gh.pr_view.assert_not_awaited()
        workspace.acquire.assert_not_awaited()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_queued_merge_revalidates_issue_before_execution(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        paused = _issue()
        paused.state_name = "Blocked"
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(side_effect=[_issue(), paused])
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "mergedAt": None,
            }
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="SUCCESS", bucket="pass")]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(
            return_value=[
                {
                    "user": {"login": "reviewer"},
                    "state": "APPROVED",
                    "commit_id": "abc123",
                    "submitted_at": "2026-05-10T00:03:00Z",
                    "body": "",
                }
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
        gh.pr_merge = AsyncMock()
        workspace = MagicMock()
        workspace.acquire = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )

        await _poll_and_wait(orch)

        assert [call.args[0] for call in linear.lookup_issue.await_args_list] == [
            "iss-1",
            "iss-1",
        ]
        workspace.acquire.assert_not_awaited()
        gh.pr_merge.assert_not_awaited()
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert [run.stage for run in history] == ["implement", "review"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_approved_merge_runs_in_background(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        await db.runs.update_status(conn, "review", "running")
        await db.runs.create(
            conn,
            id="old-submitted-merge",
            issue_id="iss-1",
            stage="merge",
            status="completed",
            pid=None,
            started_at="2026-05-09T00:00:00+00:00",
        )
        runner = _BlockingRunner()
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            side_effect=[
                {"headRefOid": "abc123", "mergeable": "MERGEABLE", "mergedAt": None},
                {
                    "headRefOid": "abc123",
                    "mergeable": "MERGEABLE",
                    "mergedAt": "2026-05-10T00:04:00Z",
                },
            ]
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="SUCCESS", bucket="pass")]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(
            return_value=[
                {
                    "user": {"login": "reviewer"},
                    "state": "APPROVED",
                    "commit_id": "abc123",
                    "submitted_at": "2026-05-10T00:03:00Z",
                    "body": "",
                }
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
        gh.pr_merge = AsyncMock()
        push_fn = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await asyncio.wait_for(
            orch._poll_merge_candidates(),  # noqa: SLF001
            timeout=0.2,
        )

        assert len(tasks) == 1
        await asyncio.wait_for(runner.started.wait(), timeout=1)
        assert runner.captured_spec is not None
        assert runner.captured_spec.stage == "merge"
        assert not tasks[0].done()
        assert await db.runs.has_active(conn, "iss-1") is True
        linear.move_issue.assert_not_awaited()

        assert await orch._poll_merge_candidates() == []  # noqa: SLF001

        runner.release.set()
        await asyncio.gather(*tasks)

        push_fn.assert_awaited_once()
        gh.pr_merge.assert_awaited_once()
        linear.move_issue.assert_awaited_once_with("iss-1", "state-done")
        workspace.cleanup.assert_awaited_once_with(_issue())
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].stage == "merge"
        assert history[-1].status == "done"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_tick_schedules_merge_before_new_implementation_when_capacity_is_full(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        binding = _binding().model_copy(update={"max_concurrent": 1})
        cfg = Config(
            repos=[binding],
            global_max_concurrent=1,
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )

        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.issues_in_state = AsyncMock(return_value=[_ready_issue()])
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        linear.comments_since = AsyncMock(return_value=[])

        workspace_path = tmp_path / "ws" / "org_srepo" / "eng-1"
        workspace_path.mkdir(parents=True)
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=workspace_path)
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()

        gh = MagicMock()
        gh.pr_view = AsyncMock(
            side_effect=[
                {"headRefOid": "abc123", "mergeable": "MERGEABLE", "mergedAt": None},
                {
                    "headRefOid": "abc123",
                    "mergeable": "MERGEABLE",
                    "mergedAt": "2026-05-10T00:05:00Z",
                },
            ]
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="SUCCESS", bucket="pass")]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(
            return_value=[
                {
                    "user": {"login": "reviewer"},
                    "state": "APPROVED",
                    "commit_id": "abc123",
                    "submitted_at": "2026-05-10T00:03:00Z",
                    "body": "ship it",
                }
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
        gh.pr_merge = AsyncMock()
        gh.repo_default_branch = AsyncMock(return_value="main")
        gh.pr_create = AsyncMock()
        gh.pr_comment = AsyncMock()

        runner = _BlockingRunner()
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        tasks = await orch._tick()  # noqa: SLF001
        try:
            await asyncio.wait_for(runner.started.wait(), timeout=1)
            assert runner.captured_spec is not None
            assert runner.captured_spec.stage == "merge"

            cur = await conn.execute("SELECT 1 FROM runs WHERE issue_id = 'iss-2'")
            assert await cur.fetchone() is None
        finally:
            runner.release.set()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_codex_no_issues_issue_comment_advances_merge(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            side_effect=[
                {"headRefOid": "abc123", "mergeable": "MERGEABLE", "mergedAt": None},
                {
                    "headRefOid": "abc123",
                    "mergeable": "MERGEABLE",
                    "mergedAt": "2026-05-10T00:05:00Z",
                },
            ]
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="SUCCESS", bucket="pass")]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(return_value=[])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(
            return_value=[
                {
                    "user": {"login": "chatgpt-codex-connector[bot]"},
                    "body": "Codex Review: Didn't find any major issues. :+1:",
                    "created_at": "2026-05-10T00:04:00Z",
                }
            ]
        )
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
        gh.pr_merge = AsyncMock()
        push_fn = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)

        assert runner.captured_spec is not None
        assert runner.captured_spec.stage == "merge"
        push_fn.assert_awaited_once()
        gh.pr_merge.assert_awaited_once()
        linear.move_issue.assert_awaited_once_with("iss-1", "state-done")
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_auto_merge_submission_waits_until_pr_reports_merged(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            side_effect=[
                {"headRefOid": "abc123", "mergeable": "MERGEABLE", "mergedAt": None},
                {"headRefOid": "abc123", "mergeable": "MERGEABLE", "mergedAt": None},
                {
                    "headRefOid": "abc123",
                    "mergeable": "MERGEABLE",
                    "mergedAt": "2026-05-10T00:04:00Z",
                },
            ]
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="SUCCESS", bucket="pass")]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(
            return_value=[
                {
                    "user": {"login": "reviewer"},
                    "state": "APPROVED",
                    "commit_id": "abc123",
                    "submitted_at": "2026-05-10T00:03:00Z",
                    "body": "",
                }
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
        gh.pr_merge = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        push_fn = AsyncMock()
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)

        gh.pr_merge.assert_awaited_once()
        push_fn.assert_awaited_once()
        linear.move_issue.assert_not_awaited()
        workspace.cleanup.assert_not_awaited()
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].stage == "merge"
        assert history[-1].status == "completed"
        assert (await db.issue_prs.list_merge_candidates(conn))[0].pr_number == 42

        await _poll_and_wait(orch)

        gh.pr_merge.assert_awaited_once()
        linear.move_issue.assert_awaited_once_with("iss-1", "state-done")
        workspace.cleanup.assert_awaited_once_with(_issue())
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].status == "done"
        assert await db.issue_prs.list_merge_candidates(conn) == []
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_cleanup_failure_still_marks_done(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock(side_effect=RuntimeError("cleanup down"))
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            side_effect=[
                {"headRefOid": "abc123", "mergeable": "MERGEABLE", "mergedAt": None},
                {
                    "headRefOid": "abc123",
                    "mergeable": "MERGEABLE",
                    "mergedAt": "2026-05-10T00:04:00Z",
                },
            ]
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="SUCCESS", bucket="pass")]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(
            return_value=[
                {
                    "user": {"login": "reviewer"},
                    "state": "APPROVED",
                    "commit_id": "abc123",
                    "submitted_at": "2026-05-10T00:03:00Z",
                    "body": "",
                }
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
        gh.pr_merge = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)

        linear.move_issue.assert_awaited_once_with("iss-1", "state-done")
        workspace.cleanup.assert_awaited_once_with(_issue())
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].stage == "merge"
        assert history[-1].status == "done"
        assert await db.issue_prs.list_merge_candidates(conn) == []
        comment_body = linear.post_comment.await_args.args[1]
        assert "Done" in comment_body
        assert await db.operator_waits.get(conn, "iss-1") is None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_submitted_merge_regression_moves_to_needs_approval(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        await db.runs.create(
            conn,
            id="merge",
            issue_id="iss-1",
            stage="merge",
            status="completed",
            pid=None,
            started_at="2026-05-10T00:02:00+00:00",
        )
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "state": "OPEN",
                "mergedAt": None,
            }
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="FAILURE", bucket="fail")]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(return_value=[])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=MagicMock(),
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)

        linear.move_issue.assert_awaited_once_with("iss-1", "state-na")
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].stage == "merge"
        assert history[-1].status == "needs_approval"
        assert await db.issue_prs.list_merge_candidates(conn) == []
        comment_body = linear.post_comment.await_args.args[1]
        assert "required CI failed: test" in comment_body
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_agent_enforces_issue_cost_cap(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        result_line = json.dumps(
            {
                "type": "result",
                "total_cost_usd": 0.75,
                "usage": {"input_tokens": 1, "output_tokens": 1},
            }
        )
        runner = _FakeRunner(
            [
                RunnerEvent(kind="started", pid=123),
                RunnerEvent(kind="stdout", line=result_line),
                RunnerEvent(kind="exit", returncode=0),
            ]
        )
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "mergedAt": None,
            }
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[CheckRun(name="test", state="SUCCESS", bucket="pass")]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(
            return_value=[
                {
                    "user": {"login": "reviewer"},
                    "state": "APPROVED",
                    "commit_id": "abc123",
                    "submitted_at": "2026-05-10T00:03:00Z",
                    "body": "",
                }
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
        gh.pr_merge = AsyncMock()
        push_fn = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
            cost_cap_per_issue_usd=1.0,
            cost_warning_pct=75,
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=push_fn,
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)

        assert runner.kill_calls
        push_fn.assert_not_awaited()
        gh.pr_merge.assert_not_awaited()
        linear.move_issue.assert_awaited_once_with("iss-1", "state-na")
        workspace.cleanup.assert_not_awaited()
        bodies = [c.args[1] for c in linear.post_comment.await_args_list]
        assert any("Cost notice" in body for body in bodies)
        assert any("cost cap reached: $1.2500" in body for body in bodies)
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].stage == "merge"
        assert history[-1].status == "needs_approval"
        assert history[-1].cost_usd == pytest.approx(0.75)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_externally_merged_candidate_finishes_before_review_classification(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        workspace = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "state": "MERGED",
                "mergedAt": "2026-05-10T00:04:00Z",
            }
        )
        gh.pr_checks = AsyncMock(return_value=PRChecks())
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(return_value=[])
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="")
        gh.pr_merge = AsyncMock(return_value=None)

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)

        gh.pr_checks.assert_not_awaited()
        gh.pr_review_comments.assert_not_awaited()
        gh.pr_reviews.assert_not_awaited()
        gh.pr_reactions.assert_not_awaited()
        gh.commit_committed_at.assert_not_awaited()
        gh.pr_merge.assert_not_awaited()
        linear.lookup_issue.assert_awaited_once_with("iss-1")
        linear.move_issue.assert_awaited_once_with("iss-1", "state-done")
        workspace.cleanup.assert_awaited_once_with(_issue())
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].stage == "merge"
        assert history[-1].status == "done"
        assert await db.issue_prs.list_merge_candidates(conn) == []
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_externally_merged_candidate_records_done_when_final_comment_fails(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        workspace = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(side_effect=LinearError("comments down"))
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "state": "MERGED",
                "mergedAt": "2026-05-10T00:04:00Z",
            }
        )
        gh.pr_checks = AsyncMock()
        gh.pr_review_comments = AsyncMock()
        gh.pr_reviews = AsyncMock()
        gh.pr_reactions = AsyncMock()
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock()
        gh.pr_merge = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)

        gh.pr_checks.assert_not_awaited()
        gh.pr_review_comments.assert_not_awaited()
        gh.pr_reviews.assert_not_awaited()
        gh.pr_reactions.assert_not_awaited()
        gh.commit_committed_at.assert_not_awaited()
        gh.pr_merge.assert_not_awaited()
        linear.move_issue.assert_awaited_once_with("iss-1", "state-done")
        workspace.cleanup.assert_awaited_once_with(_issue())
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].stage == "merge"
        assert history[-1].status == "done"
        assert await db.issue_prs.list_merge_candidates(conn) == []
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_externally_merged_candidate_closes_run_when_done_move_fails(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        workspace = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock(side_effect=[LinearError("move down"), None])
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "state": "MERGED",
                "mergedAt": "2026-05-10T00:04:00Z",
            }
        )
        gh.pr_checks = AsyncMock()
        gh.pr_review_comments = AsyncMock()
        gh.pr_reviews = AsyncMock()
        gh.pr_reactions = AsyncMock()
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock()
        gh.pr_merge = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)

        gh.pr_checks.assert_not_awaited()
        workspace.cleanup.assert_not_awaited()
        assert linear.move_issue.await_count == 2
        linear.move_issue.assert_any_await("iss-1", "state-done")
        linear.move_issue.assert_any_await("iss-1", "state-na")
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].stage == "merge"
        assert history[-1].status == "needs_approval"
        assert await db.runs.has_active(conn, "iss-1") is False
        assert await db.issue_prs.list_merge_candidates(conn) == []
        comment_body = linear.post_comment.await_args.args[1]
        assert "merge finalization failed: move down" in comment_body
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_closed_candidate_moves_to_needs_approval_before_review_classification(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        workspace = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "MERGEABLE",
                "state": "CLOSED",
                "mergedAt": None,
            }
        )
        gh.pr_checks = AsyncMock()
        gh.pr_review_comments = AsyncMock()
        gh.pr_reviews = AsyncMock()
        gh.pr_reactions = AsyncMock()
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock()
        gh.pr_merge = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=MagicMock(),
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)

        gh.pr_checks.assert_not_awaited()
        gh.pr_review_comments.assert_not_awaited()
        gh.pr_reviews.assert_not_awaited()
        gh.pr_reactions.assert_not_awaited()
        gh.commit_committed_at.assert_not_awaited()
        gh.pr_merge.assert_not_awaited()
        linear.move_issue.assert_awaited_once_with("iss-1", "state-na")
        workspace.cleanup.assert_not_awaited()
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].stage == "merge"
        assert history[-1].status == "needs_approval"
        assert await db.issue_prs.list_merge_candidates(conn) == []
        comment_body = linear.post_comment.await_args.args[1]
        assert "pull request closed before merge" in comment_body
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_failure_moves_issue_to_needs_approval(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={"headRefOid": "abc123", "mergeable": "MERGEABLE"}
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[
                    CheckRun(
                        name="test",
                        state="SUCCESS",
                        bucket="pass",
                    )
                ]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(
            return_value=[
                {
                    "user": {"login": "reviewer"},
                    "state": "APPROVED",
                    "commit_id": "abc123",
                    "submitted_at": "2026-05-10T00:03:00Z",
                    "body": "",
                }
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
        gh.pr_merge = AsyncMock(side_effect=GitHubError("branch protection blocked"))

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )
        orch._states = {"ENG": _states()}  # noqa: SLF001

        await _poll_and_wait(orch)

        linear.move_issue.assert_awaited_once_with("iss-1", "state-na")
        workspace.cleanup.assert_not_awaited()
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].stage == "merge"
        assert history[-1].status == "needs_approval"
        comment_body = linear.post_comment.await_args.args[1]
        assert "branch protection blocked" in comment_body
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_merge_conflict_closes_run_when_state_lookup_fails(
    tmp_path: Path,
) -> None:
    conn = await db.connect(tmp_path / "s.sqlite")
    try:
        await _seed_review_candidate(conn)
        runner = _FakeRunner([RunnerEvent(kind="exit", returncode=0)])
        workspace = MagicMock()
        workspace.acquire = AsyncMock(return_value=tmp_path / "ws" / "org" / "eng-1")
        workspace.release = MagicMock()
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.team_states = AsyncMock(side_effect=LinearError("states down"))
        linear.move_issue = AsyncMock()
        linear.post_comment = AsyncMock(return_value="cmt-1")
        gh = MagicMock()
        gh.pr_view = AsyncMock(
            return_value={
                "headRefOid": "abc123",
                "mergeable": "CONFLICTING",
                "mergedAt": None,
            }
        )
        gh.pr_checks = AsyncMock(
            return_value=PRChecks(
                runs=[
                    CheckRun(
                        name="test",
                        state="SUCCESS",
                        bucket="pass",
                    )
                ]
            )
        )
        gh.pr_review_comments = AsyncMock(return_value=[])
        gh.pr_reviews = AsyncMock(
            return_value=[
                {
                    "user": {"login": "reviewer"},
                    "state": "APPROVED",
                    "commit_id": "abc123",
                    "submitted_at": "2026-05-10T00:03:00Z",
                    "body": "",
                }
            ]
        )
        gh.pr_reactions = AsyncMock(return_value=[])
        gh.pr_issue_comments = AsyncMock(return_value=[])
        gh.commit_committed_at = AsyncMock(return_value="2026-05-10T00:02:00Z")
        gh.pr_merge = AsyncMock()

        cfg = Config(
            repos=[_binding(agent="claude")],
            log_root=tmp_path / "logs",
            workspace_root=tmp_path / "ws",
            db_path=tmp_path / "s.sqlite",
        )
        orch = Orchestrator(
            cfg,
            linear,
            conn,
            runner=runner,
            gh=gh,
            workspace=workspace,
            push_fn=AsyncMock(),
        )

        assert await orch._poll_merge_candidates() == []  # noqa: SLF001

        linear.team_states.assert_awaited_once_with("ENG")
        linear.move_issue.assert_not_awaited()
        gh.pr_merge.assert_not_awaited()
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].stage == "merge"
        assert history[-1].status == "needs_approval"
        assert await db.runs.has_active(conn, "iss-1") is False
        comment_body = linear.post_comment.await_args.args[1]
        assert "merge conflict against base" in comment_body
    finally:
        await conn.close()
