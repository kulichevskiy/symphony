"""Orchestrator-level Merge stage tests."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from symphony import db
from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.config import Config, LinearStates, RepoBinding
from symphony.github.client import CheckRun, GitHub, GitHubError, PRChecks
from symphony.linear.client import LinearIssue
from symphony.orchestrator.poll import Orchestrator


class _FakeRunner:
    def __init__(self, events: list[RunnerEvent]) -> None:
        self.events = events
        self.captured_spec: RunnerSpec | None = None

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        self.captured_spec = spec
        return self._aiter()

    async def _aiter(self) -> AsyncIterator[RunnerEvent]:
        for ev in self.events:
            yield ev

    async def kill(self, run_id: str) -> None:
        pass


def _binding(*, agent: str = "codex") -> RepoBinding:
    return RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        agent=agent,  # type: ignore[arg-type]
        branch_prefix="symphony",
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


def _states() -> dict[str, str]:
    return {
        "Todo": "state-todo",
        "In Progress": "state-progress",
        "Needs Approval": "state-na",
        "Blocked": "state-bl",
        "Done": "state-done",
    }


async def _seed_review_candidate(conn) -> None:  # type: ignore[no-untyped-def]
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
        "merged": False,
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
        "        pr_view['merged'] = True\n"
        "    sys.stdout.write(json.dumps(pr_view)); sys.exit(0)\n"
        "if argv[:2] == ['pr', 'checks']:\n"
        "    sys.stdout.write(json.dumps(checks)); sys.exit(0)\n"
        "if 'repos/org/repo/pulls/42/comments' in joined:\n"
        "    sys.stdout.write('[]'); sys.exit(0)\n"
        "if 'repos/org/repo/pulls/42/reviews' in joined:\n"
        "    sys.stdout.write(json.dumps(reviews)); sys.exit(0)\n"
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
        workspace.cleanup = AsyncMock()
        linear = AsyncMock()
        linear.lookup_issue = AsyncMock(return_value=_issue())
        linear.move_issue = AsyncMock()
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

        await orch._poll_merge_candidates()  # noqa: SLF001

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
                {"headRefOid": "abc123", "mergeable": "MERGEABLE", "merged": False},
                {"headRefOid": "abc123", "mergeable": "MERGEABLE", "merged": False},
                {"headRefOid": "abc123", "mergeable": "MERGEABLE", "merged": True},
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

        await orch._poll_merge_candidates()  # noqa: SLF001

        gh.pr_merge.assert_awaited_once()
        push_fn.assert_awaited_once()
        linear.move_issue.assert_not_awaited()
        workspace.cleanup.assert_not_awaited()
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].stage == "merge"
        assert history[-1].status == "completed"
        assert (await db.issue_prs.list_merge_candidates(conn))[0].pr_number == 42

        await orch._poll_merge_candidates()  # noqa: SLF001

        gh.pr_merge.assert_awaited_once()
        linear.move_issue.assert_awaited_once_with("iss-1", "state-done")
        workspace.cleanup.assert_awaited_once_with(_issue())
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].status == "done"
        assert await db.issue_prs.list_merge_candidates(conn) == []
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

        await orch._poll_merge_candidates()  # noqa: SLF001

        linear.move_issue.assert_awaited_once_with("iss-1", "state-na")
        workspace.cleanup.assert_not_awaited()
        history = await db.runs.history_for_issue(conn, "iss-1")
        assert history[-1].stage == "merge"
        assert history[-1].status == "needs_approval"
        comment_body = linear.post_comment.await_args.args[1]
        assert "branch protection blocked" in comment_body
    finally:
        await conn.close()
