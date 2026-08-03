import asyncio
from pathlib import Path

import pytest

from symphony.bench.github import GitHubRepository
from symphony.bench.harness import snapshot_harness
from symphony.bench.linear import LinearCampaign, LinearIssueState
from symphony.bench.live import LiveBenchConfig, LiveTrialExecutor, _archive_shared_trials
from symphony.bench.metrics import local_review_metrics
from symphony.bench.models import Trial, TrialExecutionCancelled, TrialExecutionError
from symphony.credentials import RunCredentials


class FakeGitHub:
    def __init__(self) -> None:
        self.archived: list[str] = []
        self.reviewed: list[str] = []

    async def create_repository(self, *, name: str, source: Path) -> GitHubRepository:
        assert (source / "README.md").exists()
        return GitHubRepository(
            slug=f"kulichevskiy/{name}", url=f"https://github.com/kulichevskiy/{name}"
        )

    async def review_metrics(self, *, repository_slug: str, cwd: Path) -> dict[str, int]:
        assert repository_slug.startswith("kulichevskiy/")
        assert cwd.name in {"A1", "B1"}
        self.reviewed.append(repository_slug)
        return {
            "remote_review_rounds": 2,
            "remote_review_comments": 2,
            "remote_review_p1": 1,
            "remote_review_p2": 1,
        }

    async def archive_repository(self, *, repository_slug: str, cwd: Path) -> None:
        del cwd
        self.archived.append(repository_slug)


class FailingArchiveGitHub(FakeGitHub):
    async def archive_repository(self, *, repository_slug: str, cwd: Path) -> None:
        del repository_slug, cwd
        raise RuntimeError("archive denied")


class FailingReviewGitHub(FakeGitHub):
    async def review_metrics(self, *, repository_slug: str, cwd: Path) -> dict[str, int]:
        del repository_slug, cwd
        raise RuntimeError("review metrics unavailable")


class FakeLinear:
    async def create_campaign(self, **_kwargs: object) -> LinearCampaign:
        return LinearCampaign(
            issue_ids=tuple(f"id-{index}" for index in range(2)),
            issue_identifiers=tuple(f"BENCH-{index}" for index in range(2)),
            issue_urls=tuple(f"https://linear.app/BENCH-{index}" for index in range(2)),
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

    async def snapshot(self, _db_path: Path, _log_root: Path) -> dict[str, object]:
        return {
            "active_agent_seconds": 120.0,
            "agent_launches": 6,
            "effective_tokens": 1234.0,
            "runs_by_status": {"completed": 6},
        }


class FinalizingCommands(FakeCommands):
    def __init__(self) -> None:
        super().__init__()
        self.snapshots = 0

    async def snapshot(self, _db_path: Path, _log_root: Path) -> dict[str, object]:
        self.snapshots += 1
        if self.snapshots == 1:
            return {
                "active_agent_seconds": 120.0,
                "agent_launches": 6,
                "effective_tokens": 1234.0,
                "runs_by_status": {"running": 1},
            }
        return {
            "active_agent_seconds": 150.0,
            "agent_launches": 6,
            "effective_tokens": 1500.0,
            "runs_by_status": {"done": 1},
        }


class ProxyRoundCommands(FakeCommands):
    async def snapshot(self, db_path: Path, log_root: Path) -> dict[str, object]:
        metrics = await super().snapshot(db_path, log_root)
        metrics["remote_review_rounds"] = 99
        return metrics


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


class FailingGrader:
    async def grade(self, **_kwargs: object) -> dict[str, int]:
        raise RuntimeError("grading broke")


class CapturingGrader(FakeGrader):
    def __init__(self) -> None:
        self.kwargs: dict[str, object] = {}

    async def grade(self, **kwargs: object) -> dict[str, int]:
        self.kwargs = kwargs
        return await super().grade(**kwargs)


class CapturingReviewer(FakeReviewer):
    def __init__(self) -> None:
        self.kwargs: dict[str, object] = {}

    async def review(self, **kwargs: object) -> dict[str, object]:
        self.kwargs = kwargs
        return await super().review(**kwargs)


def _frozen_roots(
    tmp_path: Path, experiment_id: str, private_bench_controls: Path
) -> tuple[Path, Path]:
    root = tmp_path / "runs"
    private_root = tmp_path / "private"
    snapshot_harness(
        private_root / experiment_id / "_harness", controls_root=private_bench_controls
    )
    return root, private_root


def test_local_review_metrics_count_final_verdicts_and_severity(tmp_path: Path) -> None:
    review = tmp_path / "local_review" / "parent"
    review.mkdir(parents=True)
    (review / "review-0-find.last.txt").write_text(
        "## Findings\n- unverified suspicion\n", encoding="utf-8"
    )
    (review / "review-0-verify.last.txt").write_text(
        "## Findings\n"
        "- [Critical] `a.py:1`: data loss\n"
        "- **[Major]** `b.py:2`: requirement broken\n"
        "- [Minor] `c.py:3`: bounded defect\n"
        "- `d.py:4`: old-format finding\n\n"
        "<<<VERDICT:CHANGES_REQUESTED>>>\n",
        encoding="utf-8",
    )
    (review / "review-1-verify.last.txt").write_text(
        "Tried the edge cases.\n<<<VERDICT:APPROVED>>>\n", encoding="utf-8"
    )
    (review / "review-2-verify.last.txt").write_text("no marker", encoding="utf-8")
    (review / "review-2-verify-attempt-2.last.txt").write_text(
        "Recovered.\n<<<VERDICT:APPROVED>>>\n", encoding="utf-8"
    )
    for name in (
        "review-0-find.out.log",
        "review-0-verify.out.log",
        "review-2-verify-attempt-2.out.log",
        "fix-0.out.log",
    ):
        (review / name).write_text("transcript", encoding="utf-8")

    assert local_review_metrics(tmp_path) == {
        "local_review_agent_launches": 4,
        "local_review_rounds": 3,
        "local_review_unparseable_rounds": 1,
        "local_review_findings": 4,
        "local_review_critical": 1,
        "local_review_major": 1,
        "local_review_minor": 1,
        "local_review_unclassified": 1,
    }


def test_prior_solutions_and_hidden_harness_leave_executor_volume(tmp_path: Path) -> None:
    shared = tmp_path / "shared"
    trial = shared / "EXP-OLD" / "A1"
    (trial / "final").mkdir(parents=True)
    (trial / "final/solution.py").write_text("secret solution")
    (trial / "logs").mkdir()
    (trial / "logs/run.log").write_text("receipt")
    (trial / "candidate.sqlite").write_bytes(b"db")
    (trial / "backend-hidden-junit.xml").write_text("<testsuite />")
    (trial / "frontend-hidden.json").write_text("{}")
    legacy_harness = shared / "EXP-OLD" / "_harness"
    legacy_harness.mkdir()
    (legacy_harness / "hidden_test.py").write_text("hidden")
    private = tmp_path / "private"

    _archive_shared_trials(shared, private)

    assert not (shared / "EXP-OLD").exists()
    assert (private / "EXP-OLD/A1/logs/run.log").read_text() == "receipt"
    assert (private / "EXP-OLD/A1/candidate.sqlite").read_bytes() == b"db"
    assert (private / "EXP-OLD/A1/backend-hidden-junit.xml").exists()
    assert (private / "EXP-OLD/A1/frontend-hidden.json").exists()
    assert not (private / "EXP-OLD/A1/final/solution.py").exists()
    assert (private / "EXP-OLD/_harness/hidden_test.py").read_text() == "hidden"


@pytest.mark.asyncio
async def test_live_trial_uses_the_experiment_harness_snapshot(
    tmp_path: Path, private_bench_controls: Path
) -> None:
    root = tmp_path / "runs"
    private_root = tmp_path / "private"
    snapshot_harness(private_root / "EXP-FROZEN" / "_harness", controls_root=private_bench_controls)
    grader = CapturingGrader()
    reviewer = CapturingReviewer()
    commands = FakeCommands()
    executor = LiveTrialExecutor(
        config=LiveBenchConfig(
            root=root,
            private_root=private_root,
            control_db=tmp_path / "control.sqlite",
            github_owner="kulichevskiy",
            linear_team_id="team-id",
            symphony_repository="https://github.com/kulichevskiy/symphony.git",
            encryption_key="key",
            poll_seconds=0,
        ),
        commands=commands,
        credentials=RunCredentials(github_token="gh", linear_token="lin"),
        github=FakeGitHub(),
        linear=FakeLinear(),
        grader=grader,
        reviewer=reviewer,
        candidate_snapshotter=commands.snapshot,
    )

    await executor(Trial(experiment_id="EXP-FROZEN", candidate="A", repetition=1, revision="sha"))

    assert grader.kwargs["backend_hidden_test"] == (
        private_root / "EXP-FROZEN" / "_harness/backend_hidden_test.py"
    )
    assert grader.kwargs["frontend_hidden_test"] == (
        private_root / "EXP-FROZEN" / "_harness/frontend_hidden_test.tsx"
    )
    assert grader.kwargs["manifest"].backend_total == 9
    assert isinstance(grader.kwargs["checks"], dict)
    assert "SPEC reviewer" in str(reviewer.kwargs["spec_prompt"])
    assert "STANDARDS reviewer" in str(reviewer.kwargs["standards_prompt"])
    assert not (root / "EXP-FROZEN/_harness").exists()


def test_live_trial_requires_snapshot(tmp_path: Path) -> None:
    executor = LiveTrialExecutor(
        config=LiveBenchConfig(
            root=tmp_path,
            private_root=tmp_path / "private",
            control_db=tmp_path / "control.sqlite",
            github_owner="kulichevskiy",
            linear_team_id="team-id",
            symphony_repository="repo",
            encryption_key="key",
        )
    )

    with pytest.raises(RuntimeError, match="snapshot is missing"):
        executor._load_trial_harness(  # noqa: SLF001
            Trial(experiment_id="EXP-MISSING", candidate="A", repetition=1, revision="sha")
        )


@pytest.mark.asyncio
async def test_live_trial_provisions_runs_and_returns_traceable_outcome(
    tmp_path: Path, private_bench_controls: Path
) -> None:
    commands = FakeCommands()
    root, private_root = _frozen_roots(tmp_path, "EXP-1", private_bench_controls)
    github = FakeGitHub()
    executor = LiveTrialExecutor(
        config=LiveBenchConfig(
            root=root,
            private_root=private_root,
            control_db=tmp_path / "control.sqlite",
            github_owner="kulichevskiy",
            linear_team_id="team-id",
            symphony_repository="https://github.com/kulichevskiy/symphony.git",
            encryption_key="key",
            poll_seconds=0,
        ),
        commands=commands,
        credentials=RunCredentials(github_token="gh", linear_token="Bearer lin"),
        github=github,
        linear=FakeLinear(),
        grader=FakeGrader(),
        reviewer=FakeReviewer(),
        candidate_snapshotter=commands.snapshot,
    )

    outcome = await executor(
        Trial(experiment_id="EXP-1", candidate="A", repetition=1, revision="abc123")
    )

    assert outcome.repository_url == "https://github.com/kulichevskiy/EXP-1-A1"
    assert len(outcome.issue_urls) == 2
    assert outcome.metrics["effective_tokens"] == 1234.0
    assert outcome.metrics["completed_tickets"] == 2
    assert outcome.metrics["hidden_checks_passed"] == 8
    assert outcome.metrics["remote_review_comments"] == 2
    assert outcome.metrics["spec_findings_major"] == 1
    assert any(call[:2] == ("git", "clone") for call in commands.calls)
    clone_index = next(
        index for index, call in enumerate(commands.calls) if call[:2] == ("git", "clone")
    )
    assert commands.environments[clone_index]["GH_TOKEN"] == "gh"
    checkout_index = next(
        index for index, call in enumerate(commands.calls) if call[:2] == ("git", "checkout")
    )
    assert commands.environments[checkout_index]["GH_TOKEN"] == "gh"
    seed_call = next(call for call in commands.calls if "seed" in call)
    assert seed_call[seed_call.index("--issue-label") + 1] == "symphony-bench"
    assert seed_call[seed_call.index("--issue-title-prefix") + 1] == "[EXP-1-A1]"
    daemon_calls = [
        call for call in commands.calls if call[:2] == ("uv", "run") and call[-1] == "symphony"
    ]
    assert len(daemon_calls) == 1
    assert not any("--once" in call for call in commands.calls)
    assert not any("snapshot" in call for call in commands.calls)
    assert commands.daemon_cancelled
    assert not (root / "EXP-1/A1").exists()
    assert (private_root / "EXP-1/A1/receipt-manifest.json").exists()
    assert github.archived == ["kulichevskiy/EXP-1-A1"]


@pytest.mark.asyncio
async def test_live_trial_reports_repository_archive_failure(
    tmp_path: Path, private_bench_controls: Path
) -> None:
    commands = FakeCommands()
    root, private_root = _frozen_roots(tmp_path, "EXP-ARCHIVE", private_bench_controls)
    executor = LiveTrialExecutor(
        config=LiveBenchConfig(
            root=root,
            private_root=private_root,
            control_db=tmp_path / "control.sqlite",
            github_owner="kulichevskiy",
            linear_team_id="team-id",
            symphony_repository="https://github.com/kulichevskiy/symphony.git",
            encryption_key="key",
            poll_seconds=0,
            provision_attempts=1,
        ),
        commands=commands,
        credentials=RunCredentials(github_token="gh", linear_token="Bearer lin"),
        github=FailingArchiveGitHub(),
        linear=FakeLinear(),
        grader=FakeGrader(),
        reviewer=FakeReviewer(),
        candidate_snapshotter=commands.snapshot,
    )

    with pytest.raises(TrialExecutionError, match="archive denied") as raised:
        await executor(
            Trial(experiment_id="EXP-ARCHIVE", candidate="A", repetition=1, revision="sha")
        )

    assert raised.value.outcome.repository_url.endswith("/EXP-ARCHIVE-A1")  # type: ignore[union-attr]
    assert raised.value.outcome.metrics["hidden_checks_passed"] == 8


@pytest.mark.asyncio
async def test_failed_trial_receipt_counts_actual_remote_review_rounds(
    tmp_path: Path, private_bench_controls: Path
) -> None:
    commands = FakeCommands()
    root, private_root = _frozen_roots(tmp_path, "EXP-FAIL", private_bench_controls)
    github = FakeGitHub()
    executor = LiveTrialExecutor(
        config=LiveBenchConfig(
            root=root,
            private_root=private_root,
            control_db=tmp_path / "control.sqlite",
            github_owner="kulichevskiy",
            linear_team_id="team-id",
            symphony_repository="https://github.com/kulichevskiy/symphony.git",
            encryption_key="key",
            poll_seconds=0,
        ),
        commands=commands,
        credentials=RunCredentials(github_token="gh", linear_token="Bearer lin"),
        github=github,
        linear=FakeLinear(),
        grader=FailingGrader(),
        reviewer=FakeReviewer(),
        candidate_snapshotter=commands.snapshot,
    )

    with pytest.raises(TrialExecutionError, match="grading broke") as raised:
        await executor(Trial(experiment_id="EXP-FAIL", candidate="A", repetition=1, revision="sha"))

    assert raised.value.outcome.metrics["remote_review_rounds"] == 2
    assert github.reviewed == ["kulichevskiy/EXP-FAIL-A1"]


@pytest.mark.asyncio
async def test_failed_trial_receipt_does_not_present_proxy_as_exact_rounds(
    tmp_path: Path, private_bench_controls: Path
) -> None:
    commands = ProxyRoundCommands()
    root, private_root = _frozen_roots(tmp_path, "EXP-NO-REVIEW", private_bench_controls)
    executor = LiveTrialExecutor(
        config=LiveBenchConfig(
            root=root,
            private_root=private_root,
            control_db=tmp_path / "control.sqlite",
            github_owner="kulichevskiy",
            linear_team_id="team-id",
            symphony_repository="https://github.com/kulichevskiy/symphony.git",
            encryption_key="key",
            poll_seconds=0,
        ),
        commands=commands,
        credentials=RunCredentials(github_token="gh", linear_token="Bearer lin"),
        github=FailingReviewGitHub(),
        linear=FakeLinear(),
        grader=FailingGrader(),
        reviewer=FakeReviewer(),
        candidate_snapshotter=commands.snapshot,
    )

    with pytest.raises(TrialExecutionError, match="grading broke") as raised:
        await executor(
            Trial(experiment_id="EXP-NO-REVIEW", candidate="A", repetition=1, revision="sha")
        )

    assert "remote_review_rounds" not in raised.value.outcome.metrics
    assert raised.value.outcome.metrics["remote_review_metrics_unavailable"] is True


@pytest.mark.asyncio
async def test_live_trial_keeps_one_candidate_daemon_across_status_polls(
    tmp_path: Path, private_bench_controls: Path
) -> None:
    commands = FakeCommands()
    linear = CompletingLinear()
    root, private_root = _frozen_roots(tmp_path, "EXP-DAEMON", private_bench_controls)
    executor = LiveTrialExecutor(
        config=LiveBenchConfig(
            root=root,
            private_root=private_root,
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
        candidate_snapshotter=commands.snapshot,
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
async def test_live_trial_waits_for_final_run_before_capturing_metrics(
    tmp_path: Path, private_bench_controls: Path
) -> None:
    commands = FinalizingCommands()
    root, private_root = _frozen_roots(tmp_path, "EXP-FINAL", private_bench_controls)
    executor = LiveTrialExecutor(
        config=LiveBenchConfig(
            root=root,
            private_root=private_root,
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
        candidate_snapshotter=commands.snapshot,
    )

    outcome = await executor(
        Trial(experiment_id="EXP-FINAL", candidate="A", repetition=1, revision="sha")
    )

    assert commands.snapshots == 2
    assert outcome.metrics["effective_tokens"] == 1500.0
    assert outcome.metrics["active_agent_seconds"] == 150.0


@pytest.mark.asyncio
async def test_live_trial_resolves_moving_ref_to_full_sha(tmp_path: Path) -> None:
    commands = FakeCommands()
    executor = LiveTrialExecutor(
        config=LiveBenchConfig(
            root=tmp_path,
            private_root=tmp_path / "private",
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
    tmp_path: Path, private_bench_controls: Path
) -> None:
    grader = BlockingGrader()
    root, private_root = _frozen_roots(tmp_path, "EXP-CAP", private_bench_controls)
    commands = FakeCommands()
    executor = LiveTrialExecutor(
        config=LiveBenchConfig(
            root=root,
            private_root=private_root,
            control_db=tmp_path / "control.sqlite",
            github_owner="kulichevskiy",
            linear_team_id="team-id",
            symphony_repository="https://github.com/kulichevskiy/symphony.git",
            encryption_key="key",
            poll_seconds=0,
            wall_time_cap_seconds=5,
        ),
        commands=commands,
        credentials=RunCredentials(github_token="gh", linear_token="lin"),
        github=FakeGitHub(),
        linear=FakeLinear(),
        grader=grader,
        reviewer=FakeReviewer(),
        candidate_snapshotter=commands.snapshot,
    )

    with pytest.raises(TrialExecutionError) as raised:
        await executor(Trial(experiment_id="EXP-CAP", candidate="A", repetition=1, revision="sha"))

    assert str(raised.value) == "bench wall-time safety cap exceeded"
    assert raised.value.outcome.repository_url.endswith("/EXP-CAP-A1")  # type: ignore[union-attr]
    assert raised.value.outcome.metrics["completed_tickets"] == 2
    assert "wall_seconds" in raised.value.outcome.metrics


@pytest.mark.asyncio
async def test_live_trial_external_cancellation_keeps_partial_outcome(
    tmp_path: Path, private_bench_controls: Path
) -> None:
    grader = BlockingGrader()
    root, private_root = _frozen_roots(tmp_path, "EXP-CANCEL", private_bench_controls)
    commands = FakeCommands()
    github = FakeGitHub()
    executor = LiveTrialExecutor(
        config=LiveBenchConfig(
            root=root,
            private_root=private_root,
            control_db=tmp_path / "control.sqlite",
            github_owner="kulichevskiy",
            linear_team_id="team-id",
            symphony_repository="https://github.com/kulichevskiy/symphony.git",
            encryption_key="key",
            poll_seconds=0,
        ),
        commands=commands,
        credentials=RunCredentials(github_token="gh", linear_token="lin"),
        github=github,
        linear=FakeLinear(),
        grader=grader,
        reviewer=FakeReviewer(),
        candidate_snapshotter=commands.snapshot,
    )
    task = asyncio.create_task(
        executor(Trial(experiment_id="EXP-CANCEL", candidate="A", repetition=1, revision="sha"))
    )
    await grader.started.wait()

    task.cancel()
    with pytest.raises(TrialExecutionCancelled) as raised:
        await task

    assert raised.value.outcome.repository_url.endswith("/EXP-CANCEL-A1")  # type: ignore[union-attr]
    assert raised.value.outcome.metrics["completed_tickets"] == 2
    assert github.archived == ["kulichevskiy/EXP-CANCEL-A1"]
