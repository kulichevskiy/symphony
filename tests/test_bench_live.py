import asyncio
import json
from pathlib import Path

import pytest

from symphony import db
from symphony.bench.connection_sync import (
    mirror_connections,
    reconcile_connections,
    snapshot_connections,
    sync_connections,
)
from symphony.bench.github import GitHubRepository
from symphony.bench.harness import snapshot_harness
from symphony.bench.linear import LinearCampaign, LinearIssueState
from symphony.bench.live import LiveBenchConfig, LiveTrialExecutor, _archive_shared_trials
from symphony.bench.metrics import local_review_metrics
from symphony.bench.models import (
    Experiment,
    ExperimentCreate,
    Trial,
    TrialExecutionCancelled,
    TrialExecutionError,
)
from symphony.credentials import RunCredentials
from symphony.crypto import CredentialCipher


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
    def __init__(self) -> None:
        self.campaign_calls: list[dict[str, object]] = []
        self.project_updates: list[tuple[str, str]] = []
        self.ensure_calls: list[dict[str, object]] = []

    async def ensure_project(self, **kwargs: object) -> str:
        self.ensure_calls.append(dict(kwargs))
        return "project-id"

    async def create_campaign(self, **_kwargs: object) -> LinearCampaign:
        self.campaign_calls.append(dict(_kwargs))
        return LinearCampaign(
            issue_ids=tuple(f"id-{index}" for index in range(2)),
            issue_identifiers=tuple(f"BENCH-{index}" for index in range(2)),
            issue_urls=tuple(f"https://linear.app/BENCH-{index}" for index in range(2)),
            project_id="project-id",
        )

    async def publish_project_update(
        self,
        *,
        project_id: str,
        health: str,
        body: str,
        event_key: str | None = None,
    ) -> None:
        del event_key
        assert project_id == "project-id"
        self.project_updates.append((health, body))

    async def issue_states(self, issue_ids: tuple[str, ...]) -> tuple[LinearIssueState, ...]:
        return tuple(
            LinearIssueState(id=issue_id, identifier=issue_id, name="Done", type="completed")
            for issue_id in issue_ids
        )


class CompletingLinear(FakeLinear):
    def __init__(self) -> None:
        super().__init__()
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


class NeedsInputLinear(FakeLinear):
    async def issue_states(self, issue_ids: tuple[str, ...]) -> tuple[LinearIssueState, ...]:
        return tuple(
            LinearIssueState(
                id=issue_id,
                identifier=issue_id,
                name="Needs Input" if index == 0 else "Waiting",
                type="started" if index == 0 else "unstarted",
            )
            for index, issue_id in enumerate(issue_ids)
        )


class FailingChronicleLinear(FakeLinear):
    async def publish_project_update(self, **_kwargs: object) -> None:
        raise RuntimeError("Linear temporarily unavailable")


class FirstChronicleFailureLinear(FakeLinear):
    def __init__(self) -> None:
        super().__init__()
        self.attempts = 0

    async def publish_project_update(self, **kwargs: object) -> None:
        self.attempts += 1
        if self.attempts == 1:
            raise RuntimeError("first event unavailable")
        await super().publish_project_update(**kwargs)


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


class UnreconciledRunningCommands(FakeCommands):
    async def snapshot(self, _db_path: Path, _log_root: Path) -> dict[str, object]:
        return {
            "active_agent_seconds": 120.0,
            "agent_launches": 6,
            "effective_tokens": 1234.0,
            "raw_tokens": None,
            "runs_by_status": {"running": 1},
            "token_metrics_unavailable": True,
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
        "- `[Major] e.py:5`: null title is accepted\n"
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
        "local_review_findings": 5,
        "local_review_critical": 1,
        "local_review_major": 2,
        "local_review_minor": 1,
        "local_review_unclassified": 1,
    }


def test_local_review_metrics_count_hybrid_combined_verdict_only(tmp_path: Path) -> None:
    review = tmp_path / "local_review" / "parent"
    review.mkdir(parents=True)
    (review / "review-0-spec.last.txt").write_text(
        "Spec axis.\n<<<VERDICT:CHANGES_REQUESTED>>>\n", encoding="utf-8"
    )
    (review / "review-0-bug.last.txt").write_text(
        "[P2] Race — queue.py:10 — stale state", encoding="utf-8"
    )
    (review / "review-0-hybrid.last.txt").write_text(
        "## Findings\n- [Major] queue.py:10 stale state\n<<<VERDICT:CHANGES_REQUESTED>>>\n",
        encoding="utf-8",
    )
    for name in ("review-0-spec.out.log", "review-0-bug.out.log", "fix-0.out.log"):
        (review / name).write_text("transcript", encoding="utf-8")

    metrics = local_review_metrics(tmp_path)
    assert metrics["local_review_agent_launches"] == 3
    assert metrics["local_review_rounds"] == 1
    assert metrics["local_review_unparseable_rounds"] == 0
    assert metrics["local_review_findings"] == 1
    assert metrics["local_review_major"] == 1


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
    assert grader.kwargs["manifest"].backend_total == 16
    assert grader.kwargs["manifest"].frontend_total == 13
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
    linear = FakeLinear()
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
        linear=linear,
        grader=FakeGrader(),
        reviewer=FakeReviewer(),
        candidate_snapshotter=commands.snapshot,
    )

    outcome = await executor(
        Trial(
            experiment_id="EXP-1",
            candidate="A",
            repetition=1,
            revision="abc123",
            hypothesis="The revised review process will finish the complete sample project.",
            design="Run one isolated copy of the project with version A.",
        )
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
    assert "## Hypothesis" in str(linear.campaign_calls[0]["project_description"])
    assert linear.project_updates == [
        ("onTrack", linear.project_updates[0][1])
    ]
    assert "finished successfully" in linear.project_updates[0][1]
    assert "version a, run 1 finished successfully" in linear.project_updates[0][1].lower()
    assert "hidden checks" in linear.project_updates[0][1].lower()


@pytest.mark.asyncio
async def test_live_trial_reconciles_refreshed_connections_both_ways(
    tmp_path: Path,
) -> None:
    cipher = CredentialCipher("test-encryption-key")
    control_db = tmp_path / "control.sqlite"
    candidate_a = tmp_path / "a.sqlite"
    candidate_b = tmp_path / "b.sqlite"
    control = await db.connect(control_db)
    try:
        await db.oauth_connections.set_connection(
            control,
            provider="linear",
            credential="initial",
            cipher=cipher,
            updated_by="oauth",
        )
    finally:
        await control.close()
    await snapshot_connections(control_db, candidate_a)
    await snapshot_connections(control_db, candidate_b)
    for path, token in ((candidate_a, "token-a"), (candidate_b, "token-b")):
        candidate = await db.connect(path)
        try:
            await db.oauth_connections.set_connection(
                candidate,
                provider="linear",
                credential=token,
                cipher=cipher,
                updated_by="auto-refresh",
            )
        finally:
            await candidate.close()

    executor = LiveTrialExecutor(
        config=LiveBenchConfig(
            root=tmp_path / "runs",
            private_root=tmp_path / "private",
            control_db=control_db,
            github_owner="kulichevskiy",
            linear_team_id="team-id",
            symphony_repository="repo",
            encryption_key="key",
        )
    )

    await executor._sync_trial_connections(candidate_a)  # noqa: SLF001
    await executor._sync_trial_connections(candidate_b)  # noqa: SLF001
    await executor._sync_trial_connections(candidate_a)  # noqa: SLF001

    async def connection(path: Path) -> tuple[str | None, int]:
        conn = await db.connect(path)
        try:
            credential = await db.oauth_connections.get_credential(conn, "linear", cipher)
            status = await db.oauth_connections.get_status(conn, "linear")
            assert status is not None
            return credential, status.generation
        finally:
            await conn.close()

    converged = [
        await connection(control_db),
        await connection(candidate_a),
        await connection(candidate_b),
    ]
    assert len(set(converged)) == 1
    assert converged[0][0] in {"token-a", "token-b"}
    assert converged[0][1] == 2

    control = await db.connect(control_db)
    try:
        await db.oauth_connections.delete(control, "linear")
    finally:
        await control.close()

    candidate = await db.connect(candidate_a)
    try:
        await db.oauth_connections.set_connection(
            candidate,
            provider="linear",
            credential="too-late-1",
            cipher=cipher,
            updated_by="auto-refresh",
        )
        await db.oauth_connections.set_connection(
            candidate,
            provider="linear",
            credential="too-late-2",
            cipher=cipher,
            updated_by="auto-refresh",
        )
    finally:
        await candidate.close()
    await executor._sync_trial_connections(candidate_a)  # noqa: SLF001
    await executor._sync_trial_connections(candidate_b)  # noqa: SLF001

    for path in (control_db, candidate_a, candidate_b):
        candidate = await db.connect(path)
        try:
            assert await db.oauth_connections.get_status(candidate, "linear") is None
            generation = await (
                await candidate.execute(
                    "SELECT generation FROM oauth_credential_generations WHERE provider = ?",
                    ("linear",),
                )
            ).fetchone()
            assert generation is not None
            assert generation["generation"] == 4
        finally:
            await candidate.close()


@pytest.mark.asyncio
async def test_connection_reconciliation_does_not_rollback_a_refresh_between_phases(
    tmp_path: Path,
) -> None:
    cipher = CredentialCipher("test-encryption-key")
    control_db = tmp_path / "control.sqlite"
    candidate_db = tmp_path / "candidate.sqlite"
    control = await db.connect(control_db)
    try:
        await db.oauth_connections.set_connection(
            control,
            provider="linear",
            credential="initial",
            cipher=cipher,
            updated_by="oauth",
        )
    finally:
        await control.close()
    await snapshot_connections(control_db, candidate_db)

    candidate = await db.connect(candidate_db)
    try:
        await db.oauth_connections.set_connection(
            candidate,
            provider="linear",
            credential="first-refresh",
            cipher=cipher,
            updated_by="auto-refresh",
        )
    finally:
        await candidate.close()
    assert await sync_connections(candidate_db, control_db) == 1

    candidate = await db.connect(candidate_db)
    try:
        await db.oauth_connections.set_connection(
            candidate,
            provider="linear",
            credential="late-refresh",
            cipher=cipher,
            updated_by="auto-refresh",
        )
    finally:
        await candidate.close()
    assert await mirror_connections(control_db, candidate_db) == 0

    async def connection(path: Path) -> tuple[str | None, int]:
        conn = await db.connect(path)
        try:
            credential = await db.oauth_connections.get_credential(conn, "linear", cipher)
            status = await db.oauth_connections.get_status(conn, "linear")
            assert status is not None
            return credential, status.generation
        finally:
            await conn.close()

    assert await connection(candidate_db) == ("late-refresh", 3)
    assert await reconcile_connections(candidate_db, control_db) == (1, 0)
    assert await connection(control_db) == ("late-refresh", 3)
    assert await connection(candidate_db) == ("late-refresh", 3)


@pytest.mark.asyncio
async def test_connection_reconciliation_ignores_failed_cas_generation_gaps(
    tmp_path: Path,
) -> None:
    cipher = CredentialCipher("test-encryption-key")
    control_db = tmp_path / "control.sqlite"
    candidate_db = tmp_path / "candidate.sqlite"
    control = await db.connect(control_db)
    try:
        await db.oauth_connections.set_connection(
            control,
            provider="linear",
            credential="initial",
            cipher=cipher,
            updated_by="oauth",
        )
    finally:
        await control.close()
    await snapshot_connections(control_db, candidate_db)

    candidate = await db.connect(candidate_db)
    try:
        await db.oauth_connections.set_connection(
            candidate,
            provider="linear",
            credential="refreshed",
            cipher=cipher,
            updated_by="auto-refresh",
        )
    finally:
        await candidate.close()
    control = await db.connect(control_db)
    try:
        for _ in range(2):
            written = await db.oauth_connections.set_connection(
                control,
                provider="linear",
                credential="rejected",
                cipher=cipher,
                updated_by="auto-refresh",
                expect_connected_generation=999,
            )
            assert not written
    finally:
        await control.close()

    assert await reconcile_connections(candidate_db, control_db) == (1, 1)
    for path in (control_db, candidate_db):
        connection = await db.connect(path)
        try:
            credential = await db.oauth_connections.get_credential(connection, "linear", cipher)
            status = await db.oauth_connections.get_status(connection, "linear")
            sequence = await (
                await connection.execute(
                    "SELECT generation FROM oauth_credential_generations WHERE provider = ?",
                    ("linear",),
                )
            ).fetchone()
            assert credential == "refreshed"
            assert status is not None
            assert status.generation == 2
            assert sequence is not None
            assert sequence["generation"] == 3
        finally:
            await connection.close()


@pytest.mark.asyncio
async def test_live_trial_reports_repository_archive_failure(
    tmp_path: Path, private_bench_controls: Path
) -> None:
    commands = FakeCommands()
    root, private_root = _frozen_roots(tmp_path, "EXP-ARCHIVE", private_bench_controls)
    linear = FakeLinear()
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
        linear=linear,
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
    assert linear.project_updates[-1][0] == "offTrack"
    assert "archive denied" in linear.project_updates[-1][1]


@pytest.mark.asyncio
async def test_live_trial_publishes_worker_restart_from_durable_project_receipt(
    tmp_path: Path,
) -> None:
    private_root = tmp_path / "private"
    linear = FakeLinear()
    executor = LiveTrialExecutor(
        config=LiveBenchConfig(
            root=tmp_path / "runs",
            private_root=private_root,
            control_db=tmp_path / "control.sqlite",
            github_owner="kulichevskiy",
            linear_team_id="team-id",
            symphony_repository="repo",
            encryption_key="key",
        ),
        credentials=RunCredentials(github_token="gh", linear_token="lin"),
        linear=linear,
    )

    await executor.publish_interrupted(
        Trial(
            experiment_id="EXP-RESTART",
            candidate="A",
            repetition=1,
            revision="sha",
            linear_project_id="project-id",
        )
    )

    assert linear.project_updates[-1][0] == "offTrack"
    assert "worker restarted" in linear.project_updates[-1][1]


@pytest.mark.asyncio
async def test_live_trial_starts_project_chronicle_before_any_trial(
    tmp_path: Path, private_bench_controls: Path
) -> None:
    _, private_root = _frozen_roots(tmp_path, "EXP-START", private_bench_controls)
    linear = FakeLinear()
    executor = LiveTrialExecutor(
        config=LiveBenchConfig(
            root=tmp_path / "runs",
            private_root=private_root,
            control_db=tmp_path / "control.sqlite",
            github_owner="kulichevskiy",
            linear_team_id="team-id",
            symphony_repository="repo",
            encryption_key="key",
        ),
        credentials=RunCredentials(github_token="gh", linear_token="lin"),
        linear=linear,
    )
    experiment = Experiment.queued(
        experiment_id="EXP-START",
        request=ExperimentCreate(
            mode="single",
            candidate_a="sha",
            hypothesis="The new review process will finish the whole sample project.",
            design="Run one isolated copy with the new review process.",
            repetitions=1,
        ),
    )

    project_id = await executor.start_experiment(experiment)

    assert project_id == "project-id"
    assert "## Hypothesis" in str(linear.ensure_calls[0]["project_description"])
    assert linear.project_updates[-1][0] == "onTrack"
    assert "Experiment started" in linear.project_updates[-1][1]


@pytest.mark.asyncio
async def test_chronicle_delivery_failure_does_not_fail_trial_and_retries_after_restart(
    tmp_path: Path, private_bench_controls: Path
) -> None:
    commands = FakeCommands()
    root, private_root = _frozen_roots(tmp_path, "EXP-OUTBOX", private_bench_controls)
    config = LiveBenchConfig(
        root=root,
        private_root=private_root,
        control_db=tmp_path / "control.sqlite",
        github_owner="kulichevskiy",
        linear_team_id="team-id",
        symphony_repository="repo",
        encryption_key="key",
        poll_seconds=0,
    )
    executor = LiveTrialExecutor(
        config=config,
        commands=commands,
        credentials=RunCredentials(github_token="gh", linear_token="lin"),
        github=FakeGitHub(),
        linear=FailingChronicleLinear(),
        grader=FakeGrader(),
        reviewer=FakeReviewer(),
        candidate_snapshotter=commands.snapshot,
    )

    outcome = await executor(
        Trial(experiment_id="EXP-OUTBOX", candidate="A", repetition=1, revision="sha")
    )

    assert outcome.metrics["linear_chronicle_error"] == "Linear temporarily unavailable"
    pending = list((private_root / "_chronicle").glob("*.json"))
    assert len(pending) == 1

    recovered = FakeLinear()
    recovery = LiveTrialExecutor(
        config=config,
        credentials=RunCredentials(github_token="gh", linear_token="lin"),
        linear=recovered,
    )
    await recovery.recover_chronicle()

    assert recovered.project_updates[-1][0] == "onTrack"
    assert not pending[0].exists()


@pytest.mark.asyncio
async def test_chronicle_recovery_keeps_terminal_failure_last(tmp_path: Path) -> None:
    private_root = tmp_path / "private"
    pending = private_root / "_chronicle"
    pending.mkdir(parents=True)
    (pending / "EXP-ORDER:failed.json").write_text(
        '{"body":"Failed.","event_key":"EXP-ORDER:failed",'
        '"health":"offTrack","project_id":"project-id"}\n',
        encoding="utf-8",
    )
    (pending / "EXP-ORDER:started.json").write_text(
        '{"body":"Started.","event_key":"EXP-ORDER:started",'
        '"health":"onTrack","project_id":"project-id"}\n',
        encoding="utf-8",
    )
    linear = FakeLinear()
    executor = LiveTrialExecutor(
        config=LiveBenchConfig(
            root=tmp_path / "runs",
            private_root=private_root,
            control_db=tmp_path / "control.sqlite",
            github_owner="kulichevskiy",
            linear_team_id="team-id",
            symphony_repository="repo",
            encryption_key="key",
        ),
        credentials=RunCredentials(github_token="gh", linear_token="lin"),
        linear=linear,
    )

    await executor.recover_chronicle()

    assert [health for health, _body in linear.project_updates] == ["onTrack", "offTrack"]


@pytest.mark.asyncio
async def test_chronicle_recovery_stops_later_events_after_earlier_failure(tmp_path: Path) -> None:
    private_root = tmp_path / "private"
    pending = private_root / "_chronicle"
    pending.mkdir(parents=True)
    for event, health in (("started", "onTrack"), ("failed", "offTrack")):
        (pending / f"EXP-BLOCK:{event}.json").write_text(
            json.dumps(
                {
                    "body": event,
                    "event_key": f"EXP-BLOCK:{event}",
                    "health": health,
                    "project_id": "project-id",
                }
            )
            + "\n",
            encoding="utf-8",
        )
    linear = FirstChronicleFailureLinear()
    executor = LiveTrialExecutor(
        config=LiveBenchConfig(
            root=tmp_path / "runs",
            private_root=private_root,
            control_db=tmp_path / "control.sqlite",
            github_owner="kulichevskiy",
            linear_team_id="team-id",
            symphony_repository="repo",
            encryption_key="key",
        ),
        credentials=RunCredentials(github_token="gh", linear_token="lin"),
        linear=linear,
    )

    await executor.recover_chronicle()

    assert linear.attempts == 1
    assert len(list(pending.glob("*.json"))) == 2


@pytest.mark.asyncio
async def test_terminal_update_waits_for_pending_launch_update(
    tmp_path: Path, private_bench_controls: Path
) -> None:
    _, private_root = _frozen_roots(tmp_path, "EXP-SEQUENCE", private_bench_controls)
    config = LiveBenchConfig(
        root=tmp_path / "runs",
        private_root=private_root,
        control_db=tmp_path / "control.sqlite",
        github_owner="kulichevskiy",
        linear_team_id="team-id",
        symphony_repository="repo",
        encryption_key="key",
    )
    experiment = Experiment.queued(
        experiment_id="EXP-SEQUENCE",
        request=ExperimentCreate(
            mode="single",
            candidate_a="sha",
            hypothesis="The tested version will finish the whole sample project.",
            design="Run one isolated copy of the sample project.",
            repetitions=1,
        ),
    )
    launch = LiveTrialExecutor(
        config=config,
        credentials=RunCredentials(github_token="gh", linear_token="lin"),
        linear=FailingChronicleLinear(),
    )
    project_id = await launch.start_experiment(experiment)
    failed = experiment.model_copy(
        update={"status": "failed", "linear_project_id": project_id}
    )
    delivered = FakeLinear()
    terminal = LiveTrialExecutor(
        config=config,
        credentials=RunCredentials(github_token="gh", linear_token="lin"),
        linear=delivered,
    )

    await terminal.publish_failed_experiment(failed)

    assert [health for health, _body in delivered.project_updates] == ["onTrack", "offTrack"]
    assert not list((private_root / "_chronicle").glob("*.json"))


@pytest.mark.asyncio
async def test_chronicle_outbox_write_failure_does_not_fail_successful_trial(
    tmp_path: Path, private_bench_controls: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    commands = FakeCommands()
    root, private_root = _frozen_roots(tmp_path, "EXP-WRITE", private_bench_controls)

    def fail_write(_path: Path, _payload: dict[str, str]) -> None:
        raise OSError("disk unavailable")

    monkeypatch.setattr("symphony.bench.live._write_chronicle_event", fail_write)
    executor = LiveTrialExecutor(
        config=LiveBenchConfig(
            root=root,
            private_root=private_root,
            control_db=tmp_path / "control.sqlite",
            github_owner="kulichevskiy",
            linear_team_id="team-id",
            symphony_repository="repo",
            encryption_key="key",
            poll_seconds=0,
        ),
        commands=commands,
        credentials=RunCredentials(github_token="gh", linear_token="lin"),
        github=FakeGitHub(),
        linear=FakeLinear(),
        grader=FakeGrader(),
        reviewer=FakeReviewer(),
        candidate_snapshotter=commands.snapshot,
    )

    outcome = await executor(
        Trial(experiment_id="EXP-WRITE", candidate="A", repetition=1, revision="sha")
    )

    assert outcome.metrics["linear_chronicle_error"] == "disk unavailable"


@pytest.mark.asyncio
async def test_chronicle_outbox_cleanup_failure_does_not_fail_successful_trial(
    tmp_path: Path, private_bench_controls: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    commands = FakeCommands()
    root, private_root = _frozen_roots(tmp_path, "EXP-UNLINK", private_bench_controls)
    original_unlink = Path.unlink

    def fail_chronicle_unlink(path: Path, *, missing_ok: bool = False) -> None:
        if path.parent.name == "_chronicle":
            raise OSError("unlink unavailable")
        original_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", fail_chronicle_unlink)
    executor = LiveTrialExecutor(
        config=LiveBenchConfig(
            root=root,
            private_root=private_root,
            control_db=tmp_path / "control.sqlite",
            github_owner="kulichevskiy",
            linear_team_id="team-id",
            symphony_repository="repo",
            encryption_key="key",
            poll_seconds=0,
        ),
        commands=commands,
        credentials=RunCredentials(github_token="gh", linear_token="lin"),
        github=FakeGitHub(),
        linear=FakeLinear(),
        grader=FakeGrader(),
        reviewer=FakeReviewer(),
        candidate_snapshotter=commands.snapshot,
    )

    outcome = await executor(
        Trial(experiment_id="EXP-UNLINK", candidate="A", repetition=1, revision="sha")
    )

    assert outcome.metrics["hidden_checks_passed"] == 8


@pytest.mark.asyncio
async def test_failed_trial_receipt_counts_actual_remote_review_rounds(
    tmp_path: Path, private_bench_controls: Path
) -> None:
    commands = FakeCommands()
    root, private_root = _frozen_roots(tmp_path, "EXP-FAIL", private_bench_controls)
    github = FakeGitHub()
    linear = FakeLinear()
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
        linear=linear,
        grader=FailingGrader(),
        reviewer=FakeReviewer(),
        candidate_snapshotter=commands.snapshot,
    )

    with pytest.raises(TrialExecutionError, match="grading broke") as raised:
        await executor(Trial(experiment_id="EXP-FAIL", candidate="A", repetition=1, revision="sha"))

    assert raised.value.outcome.metrics["remote_review_rounds"] == 2
    assert github.reviewed == ["kulichevskiy/EXP-FAIL-A1"]
    assert linear.project_updates[-1][0] == "offTrack"
    assert "version a, run 1 failed" in linear.project_updates[-1][1].lower()
    assert "grading broke" in linear.project_updates[-1][1]


@pytest.mark.asyncio
async def test_failed_trial_receipt_marks_unreconciled_raw_tokens_unavailable(
    tmp_path: Path, private_bench_controls: Path
) -> None:
    commands = UnreconciledRunningCommands()
    root, private_root = _frozen_roots(tmp_path, "EXP-UNRECONCILED", private_bench_controls)
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
        linear=NeedsInputLinear(),
        grader=FakeGrader(),
        reviewer=FakeReviewer(),
        candidate_snapshotter=commands.snapshot,
    )

    with pytest.raises(TrialExecutionError, match="candidate trial stopped") as raised:
        await executor(
            Trial(
                experiment_id="EXP-UNRECONCILED",
                candidate="A",
                repetition=1,
                revision="sha",
            )
        )

    assert raised.value.outcome.metrics["raw_tokens"] is None
    assert raised.value.outcome.metrics["token_metrics_unavailable"] is True


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
    linear = FakeLinear()
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
        linear=linear,
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
    assert linear.project_updates[-1][0] == "offTrack"
    assert "bench worker stopped" in linear.project_updates[-1][1]
