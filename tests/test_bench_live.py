import asyncio
from pathlib import Path

import pytest

from symphony.bench.github import GitHubRepository
from symphony.bench.linear import LinearCampaign, LinearIssueState
from symphony.bench.live import LiveBenchConfig, LiveTrialExecutor
from symphony.bench.models import Trial, TrialExecutionCancelled, TrialExecutionError
from symphony.credentials import RunCredentials


class FakeGitHub:
    async def create_repository(self, *, name: str, source: Path) -> GitHubRepository:
        assert (source / "README.md").exists()
        return GitHubRepository(
            slug=f"kulichevskiy/{name}", url=f"https://github.com/kulichevskiy/{name}"
        )

    async def review_metrics(self, *, repository_slug: str) -> dict[str, int]:
        assert repository_slug.startswith("kulichevskiy/")
        return {"remote_review_comments": 2, "remote_review_p1": 1, "remote_review_p2": 1}


class FakeLinear:
    async def create_campaign(self, **_kwargs: object) -> LinearCampaign:
        return LinearCampaign(
            issue_ids=tuple(f"id-{index}" for index in range(6)),
            issue_identifiers=tuple(f"BENCH-{index}" for index in range(6)),
            issue_urls=tuple(f"https://linear.app/BENCH-{index}" for index in range(6)),
        )

    async def issue_states(self, issue_ids: tuple[str, ...]) -> tuple[LinearIssueState, ...]:
        return tuple(
            LinearIssueState(id=issue_id, identifier=issue_id, name="Done", type="completed")
            for issue_id in issue_ids
        )


class CompletingLinear(FakeLinear):
    def __init__(self) -> None:
        self.polls = 0

    async def issue_states(self, issue_ids: tuple[str, ...]) -> tuple[LinearIssueState, ...]:
        self.polls += 1
        completed = self.polls >= 2
        return tuple(
            LinearIssueState(
                id=issue_id,
                identifier=issue_id,
                name="Done" if completed else "In Progress",
                type="completed" if completed else "started",
            )
            for issue_id in issue_ids
        )


class FakeCommands:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.environments: list[dict[str, str]] = []
        self.daemon_started = asyncio.Event()
        self.daemon_cancelled = False

    async def run(
        self,
        argv: list[str],
        *,
        cwd: Path,
        env: dict[str, str] | None = None,
        stdin: str | None = None,
    ) -> str:
        del cwd, stdin
        self.calls.append(tuple(argv))
        self.environments.append(env or {})
        if argv[:2] == ["uv", "run"] and argv[-1] == "symphony":
            self.daemon_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.daemon_cancelled = True
                raise
        if "snapshot" in argv:
            return (
                '{"active_agent_seconds":120.0,"agent_launches":6,'
                '"effective_tokens":1234.0,"runs_by_status":{"completed":6}}\n'
            )
        if "ls-remote" in argv:
            return "a" * 40 + "\trefs/heads/main\n"
        return ""


class FakeGrader:
    async def grade(self, **_kwargs: object) -> dict[str, int]:
        return {
            "hidden_checks_total": 9,
            "hidden_checks_passed": 8,
            "hidden_checks_failed": 1,
            "hidden_checks_errors": 0,
            "hidden_checks_skipped": 0,
        }


class FakeReviewer:
    async def review(self, **_kwargs: object) -> dict[str, object]:
        return {
            "spec_findings_total": 1,
            "spec_findings_major": 1,
            "standards_findings_total": 0,
        }


class BlockingGrader:
    def __init__(self) -> None:
        self.started = asyncio.Event()

    async def grade(self, **_kwargs: object) -> dict[str, int]:
        self.started.set()
        await asyncio.Event().wait()
        return {}


@pytest.mark.asyncio
async def test_live_trial_provisions_runs_and_returns_traceable_outcome(tmp_path: Path) -> None:
    commands = FakeCommands()
    executor = LiveTrialExecutor(
        config=LiveBenchConfig(
            root=tmp_path / "runs",
            control_db=tmp_path / "control.sqlite",
            github_owner="kulichevskiy",
            linear_team_id="team-id",
            symphony_repository="https://github.com/kulichevskiy/symphony.git",
            encryption_key="key",
            poll_seconds=0,
        ),
        commands=commands,
        credentials=RunCredentials(github_token="gh", linear_token="Bearer lin"),
        github=FakeGitHub(),
        linear=FakeLinear(),
        grader=FakeGrader(),
        reviewer=FakeReviewer(),
    )

    outcome = await executor(
        Trial(experiment_id="EXP-1", candidate="A", repetition=1, revision="abc123")
    )

    assert outcome.repository_url == "https://github.com/kulichevskiy/EXP-1-A1"
    assert len(outcome.issue_urls) == 6
    assert outcome.metrics["effective_tokens"] == 1234.0
    assert outcome.metrics["completed_tickets"] == 6
    assert outcome.metrics["hidden_checks_passed"] == 8
    assert outcome.metrics["remote_review_comments"] == 2
    assert outcome.metrics["spec_findings_major"] == 1
    assert any(call[:2] == ("git", "clone") for call in commands.calls)
    clone_index = next(
        index for index, call in enumerate(commands.calls) if call[:2] == ("git", "clone")
    )
    assert commands.environments[clone_index]["GH_TOKEN"] == "gh"
    seed_call = next(call for call in commands.calls if "seed" in call)
    assert seed_call[seed_call.index("--issue-label") + 1] == "symphony-bench"
    assert seed_call[seed_call.index("--issue-title-prefix") + 1] == "[EXP-1-A1]"
    daemon_calls = [
        call for call in commands.calls if call[:2] == ("uv", "run") and call[-1] == "symphony"
    ]
    assert len(daemon_calls) == 1
    assert not any("--once" in call for call in commands.calls)
    assert commands.daemon_cancelled


@pytest.mark.asyncio
async def test_live_trial_keeps_one_candidate_daemon_across_status_polls(tmp_path: Path) -> None:
    commands = FakeCommands()
    linear = CompletingLinear()
    executor = LiveTrialExecutor(
        config=LiveBenchConfig(
            root=tmp_path / "runs",
            control_db=tmp_path / "control.sqlite",
            github_owner="kulichevskiy",
            linear_team_id="team-id",
            symphony_repository="https://github.com/kulichevskiy/symphony.git",
            encryption_key="key",
            poll_seconds=0,
        ),
        commands=commands,
        credentials=RunCredentials(github_token="gh", linear_token="Bearer lin"),
        github=FakeGitHub(),
        linear=linear,
        grader=FakeGrader(),
        reviewer=FakeReviewer(),
    )

    await executor(Trial(experiment_id="EXP-DAEMON", candidate="A", repetition=1, revision="sha"))

    assert linear.polls == 2
    assert (
        len(
            [
                call
                for call in commands.calls
                if call[:2] == ("uv", "run") and call[-1] == "symphony"
            ]
        )
        == 1
    )
    assert commands.daemon_cancelled


@pytest.mark.asyncio
async def test_live_trial_resolves_moving_ref_to_full_sha(tmp_path: Path) -> None:
    commands = FakeCommands()
    executor = LiveTrialExecutor(
        config=LiveBenchConfig(
            root=tmp_path,
            control_db=tmp_path / "control.sqlite",
            github_owner="kulichevskiy",
            linear_team_id="team-id",
            symphony_repository="https://github.com/kulichevskiy/symphony.git",
            encryption_key="key",
        ),
        commands=commands,
        credentials=RunCredentials(github_token="gh", linear_token="lin"),
        github=FakeGitHub(),
        linear=FakeLinear(),
        grader=FakeGrader(),
        reviewer=FakeReviewer(),
    )

    assert await executor.resolve_revision("main") == "a" * 40
    assert await executor.resolve_revision("B" * 40) == "b" * 40


@pytest.mark.asyncio
async def test_live_trial_wall_cap_covers_grading_and_keeps_receipts(
    tmp_path: Path,
) -> None:
    grader = BlockingGrader()
    executor = LiveTrialExecutor(
        config=LiveBenchConfig(
            root=tmp_path / "runs",
            control_db=tmp_path / "control.sqlite",
            github_owner="kulichevskiy",
            linear_team_id="team-id",
            symphony_repository="https://github.com/kulichevskiy/symphony.git",
            encryption_key="key",
            poll_seconds=0,
            wall_time_cap_seconds=0.01,
        ),
        commands=FakeCommands(),
        credentials=RunCredentials(github_token="gh", linear_token="lin"),
        github=FakeGitHub(),
        linear=FakeLinear(),
        grader=grader,
        reviewer=FakeReviewer(),
    )

    with pytest.raises(TrialExecutionError) as raised:
        await executor(Trial(experiment_id="EXP-CAP", candidate="A", repetition=1, revision="sha"))

    assert str(raised.value) == "bench wall-time safety cap exceeded"
    assert raised.value.outcome.repository_url.endswith("/EXP-CAP-A1")  # type: ignore[union-attr]
    assert raised.value.outcome.metrics["completed_tickets"] == 6
    assert "wall_seconds" in raised.value.outcome.metrics


@pytest.mark.asyncio
async def test_live_trial_external_cancellation_keeps_partial_outcome(tmp_path: Path) -> None:
    grader = BlockingGrader()
    executor = LiveTrialExecutor(
        config=LiveBenchConfig(
            root=tmp_path / "runs",
            control_db=tmp_path / "control.sqlite",
            github_owner="kulichevskiy",
            linear_team_id="team-id",
            symphony_repository="https://github.com/kulichevskiy/symphony.git",
            encryption_key="key",
            poll_seconds=0,
        ),
        commands=FakeCommands(),
        credentials=RunCredentials(github_token="gh", linear_token="lin"),
        github=FakeGitHub(),
        linear=FakeLinear(),
        grader=grader,
        reviewer=FakeReviewer(),
    )
    task = asyncio.create_task(
        executor(Trial(experiment_id="EXP-CANCEL", candidate="A", repetition=1, revision="sha"))
    )
    await grader.started.wait()

    task.cancel()
    with pytest.raises(TrialExecutionCancelled) as raised:
        await task

    assert raised.value.outcome.repository_url.endswith("/EXP-CANCEL-A1")  # type: ignore[union-attr]
    assert raised.value.outcome.metrics["completed_tickets"] == 6
