"""Contract tests: pin the harness fakes against recorded real payloads.

The fakes in `tests/harness/fakes.py` were reverse-engineered from our own
client code, so on their own they only prove the fakes match our *assumptions*
about GitHub/Linear — not reality. These tests break that tautology: each one
feeds BOTH a recorded real payload and the fake's output through the **same real
parsing path** and asserts the resulting domain objects agree. A fake whose
shape or values drift away from the recorded real payload goes red.

The golden fixtures live in `tests/fixtures/contract/` and are refreshed with a
single command — see `scripts/capture-fixtures.sh` and the fixtures README.

Deferred (YAGNI — add only if drift ever bites, see issue SYM-138):
  * an opt-in *live-recording* pytest mode (`--record`) that re-captures the
    goldens against a real PR/issue inside the test run, and
  * a periodic *sandbox-CI drift check* that diffs freshly-captured payloads
    against the committed goldens on a schedule.
Both are intentionally NOT built here: the static committed fixtures + the
one-command regenerator are enough until real drift is observed.
"""

from __future__ import annotations

import json
from dataclasses import astuple
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from symphony.github.client import GitHub
from symphony.github.webhook import _parse_event
from symphony.linear.client import Linear, comment_from_webhook_payload
from symphony.orchestrator.poll import (
    _no_signal_head_check_state,
    _pr_view_has_merge_conflict,
    _pr_view_is_clean_mergeable,
    _pr_view_is_closed,
    _pr_view_is_merged,
)
from symphony.tracker import Comment

from tests.harness.clock import ManualClock
from tests.harness.fakes import FakeGitHub, FakeLinear
from tests.harness.sim import PR_OPEN, Sim, SimComment, SimIssue, SimPR

FIXTURES = Path(__file__).parent / "fixtures" / "contract"


def _load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text())


def _new_sim() -> Sim:
    return Sim(ManualClock())


# --- FakeGitHub ----------------------------------------------------------


def _pr_view_domain(view: dict[str, Any]) -> tuple[Any, ...]:
    """The orchestrator-facing classification of a `pr view` payload.

    This is the real parsing path the poll loop runs on every `pr_view`; a
    payload whose field names/values diverge lands in a different bucket here.
    """
    return (
        _pr_view_is_merged(view),
        _pr_view_is_closed(view),
        _pr_view_has_merge_conflict(view),
        _pr_view_is_clean_mergeable(view),
        _no_signal_head_check_state(view),
    )


async def test_pr_view_fake_matches_recorded_real_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_payload = _load("github_pr_view.json")

    # Real parsing path: `GitHub.pr_view` feeds gh's JSON straight through.
    gh = GitHub()

    async def fake_run_json(*_args: Any, **_kwargs: Any) -> Any:
        return real_payload

    monkeypatch.setattr(gh, "_run_json", fake_run_json)
    real_view = await gh.pr_view(real_payload["number"], include_status_checks=True)

    # Fake configured to represent the *same scenario* the golden records:
    # an open, mergeable PR whose required checks are green.
    assert not _pr_view_is_merged(real_payload)
    assert not _pr_view_is_closed(real_payload)
    assert _pr_view_is_clean_mergeable(real_payload)
    assert _no_signal_head_check_state(real_payload) == "green"

    sim = _new_sim()
    sim.prs[("acme/widgets", real_payload["number"])] = SimPR(
        repo="acme/widgets",
        number=real_payload["number"],
        head=real_payload["headRefName"],
        base=real_payload["baseRefName"],
        title=real_payload["title"],
        url=real_payload["url"],
        state=PR_OPEN,
        head_sha=real_payload["headRefOid"],
        checks_passed=True,
    )
    fake = FakeGitHub(sim)
    fake_view = await fake.pr_view(
        real_payload["number"], repo="acme/widgets", include_status_checks=True
    )

    assert _pr_view_domain(fake_view) == _pr_view_domain(real_view)


async def test_pr_checks_fake_matches_recorded_real_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_payload = _load("github_pr_checks.json")  # one failing required check

    # Real parsing path: `GitHub.pr_checks` parses gh's `--json` array.
    gh = GitHub()

    async def fake_capture(*_args: Any, **_kwargs: Any) -> tuple[str, str, int]:
        # gh pr checks exits 1 when required checks are failing (not just "no checks").
        return (json.dumps(real_payload), "", 1)

    monkeypatch.setattr(gh, "_run_capture", fake_capture)
    real_checks = await gh.pr_checks(1, repo="acme/widgets")

    # Fake configured for the same scenario: checks not passing.
    sim = _new_sim()
    sim.prs[("acme/widgets", 1)] = SimPR(
        repo="acme/widgets", number=1, head="feat/x", checks_passed=False
    )
    fake = FakeGitHub(sim)
    fake_checks = await fake.pr_checks(1, repo="acme/widgets")

    # Domain semantics the merge gate reads must agree...
    assert (real_checks.all_passed, real_checks.any_failed, real_checks.pending) == (
        fake_checks.all_passed,
        fake_checks.any_failed,
        fake_checks.pending,
    )
    # ...as must the per-check fields the orchestrator surfaces (link is
    # metadata only and intentionally not modelled by the fake).
    assert [(r.name, r.state, r.bucket) for r in real_checks.runs] == [
        (r.name, r.state, r.bucket) for r in fake_checks.runs
    ]


async def test_pr_checks_passing_fake_matches_recorded_real_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_payload = _load("github_pr_checks_passing.json")  # one passing required check

    gh = GitHub()

    async def fake_capture(*_args: Any, **_kwargs: Any) -> tuple[str, str, int]:
        return (json.dumps(real_payload), "", 0)

    monkeypatch.setattr(gh, "_run_capture", fake_capture)
    real_checks = await gh.pr_checks(1, repo="acme/widgets")

    # Fake with checks_passed=True models "passes the merge gate" as empty runs
    # (see fakes.py). Gate semantics agree with a real all-green run; individual
    # run names/states are not modelled by the fake.
    sim = _new_sim()
    sim.prs[("acme/widgets", 1)] = SimPR(
        repo="acme/widgets", number=1, head="feat/x", checks_passed=True
    )
    fake = FakeGitHub(sim)
    fake_checks = await fake.pr_checks(1, repo="acme/widgets")

    # Gate semantics must agree: a green PR is unblocked in both cases.
    assert (real_checks.all_passed, real_checks.any_failed, real_checks.pending) == (
        fake_checks.all_passed,
        fake_checks.any_failed,
        fake_checks.pending,
    )
    # The fake collapses all-green to empty runs (identical gate semantics).
    # Individual run names/states are intentionally not modelled — see fakes.py.
    assert fake_checks.runs == []
    assert real_checks.runs  # golden has at least one passing run


async def test_github_pr_webhook_fake_matches_recorded_real_payload() -> None:
    real_payload = _load("github_pr_webhook.json")  # merged pull_request delivery

    # Real parsing path: the webhook receiver's event parser.
    real_event = _parse_event(
        "pull_request", real_payload, delivery_id="recorded-delivery"
    )
    assert real_event is not None

    # Fake path: `Sim.merge_pr` fabricates the GitHubWebhookEvent the
    # orchestrator would receive for the same merge.
    repo = real_payload["repository"]["full_name"]
    number = real_payload["pull_request"]["number"]
    sim = _new_sim()
    sim.prs[(repo, number)] = SimPR(repo=repo, number=number, head="feat/x")
    sim.merge_pr(number, repo=repo)
    fake_event = sim.github_webhooks[-1]

    # Compare the fields the Sim models. `merged_by`/`sender_login` are real
    # GitHub fields the Sim does not fabricate (no operator identity in the
    # sim) — excluded by design, not divergence.
    assert (
        real_event.event_type,
        real_event.action,
        real_event.repo,
        real_event.pr_number,
        real_event.merged,
        bool(real_event.merged_at),
    ) == (
        fake_event.event_type,
        fake_event.action,
        fake_event.repo,
        fake_event.pr_number,
        fake_event.merged,
        bool(fake_event.merged_at),
    )


# --- FakeLinear ----------------------------------------------------------


async def test_issues_in_state_fake_matches_recorded_real_payload() -> None:
    real_data = _load("linear_issues_in_state.json")  # GraphQL `data` body

    # Real parsing path: `LinearTracker.issues_in_state` → `LinearIssue.from_node`.
    linear = Linear("test-key")

    async def fake_query(_gql: str, _vars: dict[str, Any]) -> dict[str, Any]:
        return real_data

    linear._query = fake_query  # type: ignore[method-assign]
    try:
        real_issues = await linear.issues_in_state("SYM", "Ready", "symphony")
    finally:
        await linear.aclose()
    assert len(real_issues) == 1
    real = real_issues[0]

    # Fake path: seed the Sim with hardcoded primitives matching the fixture's
    # expected domain values, then read back via FakeLinear. The equality check
    # below verifies that `from_node` parsed the fixture into exactly those values
    # AND that `_to_issue` reproduces them faithfully. Seeding from `real` would
    # be a tautology — both sides would derive from the same `from_node` call.
    sim = _new_sim()
    sim.issues["8a1f0c2e-1b3d-4e5f-9a7b-0c1d2e3f4a5b"] = SimIssue(
        id="8a1f0c2e-1b3d-4e5f-9a7b-0c1d2e3f4a5b",
        identifier="SYM-42",
        title="Fix flaky merge gate",
        description="The merge gate occasionally races with auto-merge.",
        url="https://linear.app/acme/issue/SYM-42/fix-flaky-merge-gate",
        state_id="state-ready-uuid",
        state_name="Ready",
        state_type="unstarted",
        team_key="SYM",
        labels=["symphony"],
        updated_at="2026-06-20T10:00:00.000Z",
    )
    fake = FakeLinear(sim)
    fake_issues = await fake.issues_in_state("SYM", "Ready", "symphony")

    assert len(fake_issues) == 1
    assert astuple(fake_issues[0]) == astuple(real)


async def test_comments_since_fake_matches_recorded_real_payload() -> None:
    real_data = _load("linear_comments_since.json")  # GraphQL `data` body

    # Real parsing path: `LinearTracker.comments_since` → `LinearComment.from_node`.
    linear = Linear("test-key")

    async def fake_query(_gql: str, _vars: dict[str, Any]) -> dict[str, Any]:
        return real_data

    linear._query = fake_query  # type: ignore[method-assign]
    after = datetime(2000, 1, 1, tzinfo=UTC)
    try:
        real_comments = await linear.comments_since("issue-uuid", after)
    finally:
        await linear.aclose()
    assert len(real_comments) == 1
    real = real_comments[0]

    # Fake path: seed the Sim with hardcoded primitives matching the fixture's
    # expected domain values — seeding from `real` would be a tautology.
    sim = _new_sim()
    sim.comments["issue-uuid"] = [
        SimComment(
            id="c1a2b3c4-d5e6-4f70-8192-a3b4c5d6e7f8",
            issue_id="issue-uuid",
            body="$approve",
            created_at="2026-06-20T15:00:00.000Z",
            author_name="Alex",
            author_is_me=False,  # "$approve" is not a symphony-marked comment
            external_thread_type=None,
        )
    ]
    fake = FakeLinear(sim)
    fake_comments = await fake.comments_since("issue-uuid", after)

    assert len(fake_comments) == 1
    assert astuple(fake_comments[0]) == astuple(real)


async def test_linear_comment_webhook_fake_matches_recorded_real_payload() -> None:
    real_payload = _load("linear_comment_webhook.json")  # real delivery body

    # Real parsing path: `comment_from_webhook_payload`, shared with the
    # orchestrator's `_handle_webhook_comment`.
    real_comment = comment_from_webhook_payload(real_payload)
    assert real_comment is not None

    # Fake path: `Sim.operator_comment` enqueues the webhook payload the
    # harness delivers; run *that* through the same parser.
    sim = _new_sim()
    sim.issues["issue-uuid"] = SimIssue(id="issue-uuid", identifier="SYM-42")
    body = real_payload["data"]["body"]
    sim.operator_comment("issue-uuid", body)
    fake_payload = sim.linear_webhooks[-1]
    fake_comment = comment_from_webhook_payload(fake_payload)
    assert fake_comment is not None

    # Compare the fields the Sim models and the orchestrator acts on. The Sim
    # does not fabricate an `actor` block, so `author_name` (display only —
    # slash routing gates on `author_is_me`/the symphony marker, not the name)
    # is excluded by design.
    def _domain(c: Comment) -> tuple[Any, ...]:
        return (c.body, c.author_is_me, c.external_thread_type)

    assert _domain(fake_comment) == _domain(real_comment)
