"""Hot-apply bindings at the tick boundary (SYM-189).

The daemon and UI API share one process, so the orchestrator re-reads *all*
bindings from the config DB at the start of every poll tick — no restart. A
binding inserted mid-run is scanned on the next tick; a removed one has its
tracker-queue lanes pruned; a binding introducing a tracker context unseen at
boot gets a hot-added registry client. The reload takes the same in-process
config-write lock a config write takes, so it never observes an uncommitted
multi-row save on the shared connection.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from symphony import db
from symphony.config import Config, LinearStates, RepoBinding
from symphony.effective_config import assemble_effective_config
from symphony.linear.client import LinearError
from symphony.orchestrator.poll import Orchestrator, _binding_key
from symphony.tracker import TrackerContext, TrackerRegistry
from tests.harness import Harness

REPO = "org/repo"
READY = "Todo"
DONE = "Done"


def _base(tmp_path: Path) -> Config:
    return Config(
        workspace_root=tmp_path / "workspaces",
        log_root=tmp_path / "logs",
        db_path=tmp_path / "symphony.sqlite",
    )


def _payload(team: str, repo: str = REPO, *, site: str = "default", **extra) -> dict:
    payload: dict = {
        "linear_team_key": team,
        "github_repo": repo,
        "local_review": False,
        "remote_review": False,
        "linear_states": {
            "ready": READY,
            "in_progress": "In Progress",
            "code_review": "Needs Approval",
            "done": DONE,
        },
    }
    if site != "default":
        payload["tracker_site"] = site
    payload.update(extra)
    return payload


async def _insert(
    conn, *, team: str, repo: str = REPO, site: str = "default", priority: int = 0, **extra
) -> None:
    await db.config_bindings.insert(
        conn,
        payload=_payload(team, repo, site=site, **extra),
        key=(team, repo, "", "linear", site),
        priority=priority,
    )


def _seed_team(harness: Harness, team: str) -> None:
    harness.sim.seed_team(
        team,
        {
            READY: "state-todo",
            "In Progress": "state-inprog",
            "Needs Approval": "state-review",
            DONE: "state-done",
        },
        {
            READY: "unstarted",
            "In Progress": "started",
            "Needs Approval": "started",
            DONE: "completed",
        },
    )


async def _queue_scopes(conn) -> set[tuple[str, str]]:
    cur = await conn.execute("SELECT DISTINCT team_key, scope FROM tracker_queue")
    return {(str(r["team_key"]), str(r["scope"])) for r in await cur.fetchall()}


@pytest.mark.asyncio
async def test_binding_inserted_mid_run_is_scanned_next_tick(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "symphony.sqlite")
    await _insert(conn, team="ENG")
    cfg = await assemble_effective_config(conn, _base(tmp_path))
    await conn.close()

    harness = await Harness.create(tmp_path, config=cfg, reload_bindings=True)
    try:
        _seed_team(harness, "OPS")
        await harness.warmup()
        # A second binding lands in the DB *after* boot.
        await _insert(harness.conn, team="OPS", repo="org/ops", priority=1)
        issue = harness.sim.seed_issue(
            identifier="OPS-1", team_key="OPS", state_name=READY, title="new binding"
        )
        scheduled = await harness.step()
        assert len(scheduled) == 1
        # The reload picked up the OPS binding and dispatched its ready issue.
        assert "OPS" in {b.linear_team_key for b in harness.orch.config.repos}
        assert harness.sim.issues[issue.id].state_name != READY
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_binding_removed_mid_run_prunes_its_queue_scope(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "symphony.sqlite")
    await _insert(conn, team="ENG", repo="org/eng", priority=0, max_concurrent=0)
    # `max_concurrent=0` lets the OPS scan mirror its Ready lane into
    # `tracker_queue` (giving the scope a row to prune) without dispatching a
    # full pipeline run.
    await _insert(conn, team="OPS", repo="org/ops", priority=1, max_concurrent=0)
    cfg = await assemble_effective_config(conn, _base(tmp_path))
    await conn.close()

    harness = await Harness.create(tmp_path, config=cfg, reload_bindings=True)
    try:
        _seed_team(harness, "OPS")
        await harness.warmup()
        harness.sim.seed_issue(identifier="ENG-1", team_key="ENG", state_name=READY, title="kept")
        harness.sim.seed_issue(identifier="OPS-1", team_key="OPS", state_name=READY, title="queued")
        await harness.step()
        scopes = await _queue_scopes(harness.conn)
        assert any(team == "OPS" for team, _ in scopes)

        # Remove the OPS binding from the DB (write path lands in slice 3; the
        # test mutates the row directly).
        await harness.conn.execute("DELETE FROM config_bindings WHERE github_repo = 'org/ops'")
        await harness.conn.commit()
        await harness.step()

        assert "OPS" not in {b.linear_team_key for b in harness.orch.config.repos}
        scopes = await _queue_scopes(harness.conn)
        assert not any(team == "OPS" for team, _ in scopes)
        assert any(team == "ENG" for team, _ in scopes)
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_binding_with_unseen_tracker_context_is_hot_added(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "symphony.sqlite")
    await _insert(conn, team="ENG")
    cfg = await assemble_effective_config(conn, _base(tmp_path))
    await conn.close()

    harness = await Harness.create(tmp_path, config=cfg, reload_bindings=True)
    try:
        _seed_team(harness, "OPS")
        await harness.warmup()
        # A binding on a tracker site the process never saw at boot.
        await _insert(harness.conn, team="OPS", repo="org/ops", site="other-site", priority=1)
        issue = harness.sim.seed_issue(
            identifier="OPS-9", team_key="OPS", state_name=READY, title="unseen ctx"
        )
        scheduled = await harness.step()
        # Resolving/scanning this binding requires a registry client for the
        # (linear, other-site) context — one that did not exist at boot.
        assert len(scheduled) == 1
        assert harness.sim.issues[issue.id].state_name != READY
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_reload_never_observes_uncommitted_write(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "symphony.sqlite")
    await _insert(conn, team="ENG")
    cfg = await assemble_effective_config(conn, _base(tmp_path))
    await conn.close()

    harness = await Harness.create(tmp_path, config=cfg, reload_bindings=True)
    try:
        await harness.warmup()
        assert {b.linear_team_key for b in harness.orch.config.repos} == {"ENG"}

        # Model a config write: take the shared lock and start a multi-row
        # transaction on the shared connection without committing it.
        async with harness.orch.config_write_lock:
            await harness.conn.execute(
                """
                INSERT INTO config_bindings (
                    payload, version, enabled, priority, updated_at, updated_by,
                    project_key, github_repo, issue_label, tracker_provider, tracker_site
                ) VALUES (?, 1, 1, 5, '', '', 'OPS', 'org/ops', '', 'linear', 'default')
                """,
                (json.dumps(_payload("OPS", "org/ops")),),
            )
            reload_task = asyncio.create_task(harness.orch._reload_bindings())  # noqa: SLF001
            await asyncio.sleep(0.05)
            # The reload cannot proceed while the writer holds the lock, so it
            # never sees the uncommitted OPS row.
            assert not reload_task.done()
            assert {b.linear_team_key for b in harness.orch.config.repos} == {"ENG"}
            await harness.conn.commit()

        await reload_task
        assert {b.linear_team_key for b in harness.orch.config.repos} == {"ENG", "OPS"}
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_stage_after_edit_uses_new_row(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "symphony.sqlite")
    await _insert(conn, team="ENG")
    cfg = await assemble_effective_config(conn, _base(tmp_path))
    await conn.close()

    harness = await Harness.create(tmp_path, config=cfg, reload_bindings=True)
    try:
        await harness.warmup()
        before = harness.orch.config.repos[0]
        assert before.max_concurrent != 7

        # Edit the binding row's payload (write path is slice 3; mutate directly).
        edited = _payload("ENG")
        edited["max_concurrent"] = 7
        await harness.conn.execute(
            "UPDATE config_bindings SET payload = ? WHERE github_repo = ?",
            (json.dumps(edited), REPO),
        )
        await harness.conn.commit()
        await harness.step()

        after = harness.orch.config.repos[0]
        assert after.max_concurrent == 7
        # The stage started before the edit captured its own binding object; the
        # reload rebuilds a fresh one rather than mutating the captured row.
        assert before.max_concurrent != 7
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_last_binding_removed_mid_run_empties_topology_and_prunes_scope(
    tmp_path: Path,
) -> None:
    """Deleting every `config_bindings` row must collapse the topology to
    empty, not keep scanning the deleted binding (SYM-189 review fix)."""
    conn = await db.connect(tmp_path / "symphony.sqlite")
    await _insert(conn, team="ENG", repo="org/eng", priority=0, max_concurrent=0)
    cfg = await assemble_effective_config(conn, _base(tmp_path))
    await conn.close()

    harness = await Harness.create(tmp_path, config=cfg, reload_bindings=True)
    try:
        await harness.warmup()
        harness.sim.seed_issue(identifier="ENG-1", team_key="ENG", state_name=READY, title="queued")
        await harness.step()
        scopes = await _queue_scopes(harness.conn)
        assert any(team == "ENG" for team, _ in scopes)

        await harness.conn.execute("DELETE FROM config_bindings")
        await harness.conn.commit()
        await harness.step()

        assert harness.orch.config.repos == []
        scopes = await _queue_scopes(harness.conn)
        assert not scopes
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_hot_added_binding_bad_waiting_state_raises_on_first_load(
    tmp_path: Path,
) -> None:
    """A binding whose states are first loaded mid-run (hot-added, never
    covered by boot's `warmup`) must still get the `waiting`-state gate that
    `warmup` runs for boot bindings, instead of silently mis-driving
    auto-unblock (SYM-189 review fix)."""
    conn = await db.connect(tmp_path / "symphony.sqlite")
    await _insert(conn, team="ENG")
    cfg = await assemble_effective_config(conn, _base(tmp_path))
    await conn.close()

    harness = await Harness.create(tmp_path, config=cfg, reload_bindings=True)
    try:
        await harness.warmup()
        _seed_team(harness, "OPS")
        await _insert(
            harness.conn,
            team="OPS",
            repo="org/ops",
            priority=1,
            linear_states={
                "ready": READY,
                "in_progress": "In Progress",
                "code_review": "Needs Approval",
                "done": DONE,
                "waiting": "Blocked",  # never seeded for the OPS team above
            },
        )
        await harness.step()

        ops_binding = next(b for b in harness.orch.config.repos if b.linear_team_key == "OPS")
        with pytest.raises(LinearError, match="Blocked"):
            await harness.orch._states_for_binding(ops_binding)  # noqa: SLF001
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_edited_binding_waiting_state_is_revalidated_on_reload(
    tmp_path: Path,
) -> None:
    """A binding whose states were already warmed (cache hit) must be
    re-validated after an edit declares a `waiting` state absent from its
    Linear workflow — the cache key is unchanged, so a stale entry would
    silently skip `_validate_waiting_state` (SYM-189 review fix)."""
    conn = await db.connect(tmp_path / "symphony.sqlite")
    await _insert(conn, team="ENG")
    cfg = await assemble_effective_config(conn, _base(tmp_path))
    await conn.close()

    harness = await Harness.create(tmp_path, config=cfg, reload_bindings=True)
    try:
        await harness.warmup()
        binding = harness.orch.config.repos[0]
        # Warm the cache the same way a normal scan would.
        await harness.orch._states_for_binding(binding)  # noqa: SLF001

        edited = _payload("ENG")
        edited["linear_states"]["waiting"] = "On Hold"  # never seeded above
        await harness.conn.execute(
            "UPDATE config_bindings SET payload = ? WHERE github_repo = ?",
            (json.dumps(edited), REPO),
        )
        await harness.conn.commit()
        await harness.orch._reload_bindings()  # noqa: SLF001

        edited_binding = harness.orch.config.repos[0]
        with pytest.raises(LinearError, match="On Hold"):
            await harness.orch._states_for_binding(edited_binding)  # noqa: SLF001
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_scan_failure_on_one_binding_does_not_starve_the_rest(
    tmp_path: Path,
) -> None:
    """A hot-add tracker-factory failure leaves one binding's context
    unregistered; the scan loop must skip that lane and still scan bindings
    ordered after it instead of aborting the whole tick (SYM-189 review fix).
    """
    conn = await db.connect(tmp_path / "symphony.sqlite")
    # Priority 5 so this binding sorts *after* the hot-added, broken OPS
    # binding below (priority 0) — the scenario the review flagged is a
    # broken lane starving every binding ordered at/after it.
    await _insert(conn, team="ENG", repo="org/eng", priority=5)
    cfg = await assemble_effective_config(conn, _base(tmp_path))
    await conn.close()

    harness = await Harness.create(tmp_path, config=cfg, reload_bindings=True)
    try:
        await harness.warmup()
        _seed_team(harness, "OPS")
        # A second binding on a tracker site whose hot-add will fail below.
        await _insert(harness.conn, team="OPS", repo="org/ops", site="broken-site", priority=0)
        real_factory = harness.orch._tracker_factory  # noqa: SLF001

        def _flaky_factory(binding):
            if binding.tracker_site == "broken-site":
                raise ValueError("boom")
            return real_factory(binding)

        harness.orch._tracker_factory = _flaky_factory  # noqa: SLF001

        eng_issue = harness.sim.seed_issue(
            identifier="ENG-1", team_key="ENG", state_name=READY, title="kept lane"
        )
        harness.sim.seed_issue(
            identifier="OPS-1", team_key="OPS", state_name=READY, title="dead lane"
        )
        scheduled = await harness.step()

        # The ENG lane (ordered after the broken OPS hot-add) still dispatches.
        assert len(scheduled) == 1
        assert harness.sim.issues[eng_issue.id].state_name != READY
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_hot_add_failure_is_retried_on_the_next_tick(tmp_path: Path) -> None:
    """A transient `_tracker_factory` failure must not latch the binding set:
    with the set unchanged, the next tick retries the hot-add and scans the
    binding once it succeeds (SYM-189 review fix).
    """
    conn = await db.connect(tmp_path / "symphony.sqlite")
    await _insert(conn, team="ENG", repo="org/eng", priority=5)
    cfg = await assemble_effective_config(conn, _base(tmp_path))
    await conn.close()

    harness = await Harness.create(tmp_path, config=cfg, reload_bindings=True)
    try:
        await harness.warmup()
        _seed_team(harness, "OPS")
        await _insert(harness.conn, team="OPS", repo="org/ops", site="flaky-site", priority=0)
        real_factory = harness.orch._tracker_factory  # noqa: SLF001
        should_fail = True

        def _flaky_factory(binding):
            if binding.tracker_site == "flaky-site" and should_fail:
                raise ValueError("boom")
            return real_factory(binding)

        harness.orch._tracker_factory = _flaky_factory  # noqa: SLF001

        harness.sim.seed_issue(identifier="ENG-1", team_key="ENG", state_name=READY, title="eng")
        ops_issue = harness.sim.seed_issue(
            identifier="OPS-1", team_key="OPS", state_name=READY, title="unscanned until retry"
        )

        await harness.step()
        assert harness.sim.issues[ops_issue.id].state_name == READY

        # The binding set is unchanged, but the failed hot-add must not have
        # latched — the factory now succeeds, so this tick retries and scans it.
        should_fail = False
        await harness.step()
        assert harness.sim.issues[ops_issue.id].state_name != READY
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_hot_added_binding_sharing_warm_state_cache_still_validates_waiting_state(
    tmp_path: Path,
) -> None:
    """`_states_for_binding` caches team workflow states by `_state_cache_key`
    (provider, site, team) — coarser than a binding's natural key. A second
    binding on the same team, hot-added after the first already warmed that
    shared cache entry, must still get its own `waiting` state checked instead
    of silently reusing the sibling's cache hit (SYM-189 review fix)."""
    conn = await db.connect(tmp_path / "symphony.sqlite")
    await _insert(conn, team="ENG", repo="org/eng-a")
    cfg = await assemble_effective_config(conn, _base(tmp_path))
    await conn.close()

    harness = await Harness.create(tmp_path, config=cfg, reload_bindings=True)
    try:
        await harness.warmup()

        # A second binding on the SAME team, hot-added mid-run, declaring a
        # waiting state absent from the team's workflow. Adding it changes the
        # per-state_key binding set, so the reload evicts the shared cache
        # entry (a separate, already-fixed gap) — that's not what's under
        # test here.
        await _insert(
            harness.conn,
            team="ENG",
            repo="org/eng-b",
            priority=1,
            linear_states={
                "ready": READY,
                "in_progress": "In Progress",
                "code_review": "Needs Approval",
                "done": DONE,
                # Not one of `LinearStates`' own default role names (so it
                # can't accidentally match ENG's auto-derived sim states) and
                # never explicitly seeded either.
                "waiting": "On Hold",
            },
        )
        await harness.orch._reload_bindings()  # noqa: SLF001

        first_binding = next(b for b in harness.orch.config.repos if b.github_repo == "org/eng-a")
        second_binding = next(b for b in harness.orch.config.repos if b.github_repo == "org/eng-b")
        # Mirrors scan order within one tick: the first binding on this team
        # is scanned (and re-warms the just-evicted shared cache entry)
        # before the second.
        await harness.orch._states_for_binding(first_binding)  # noqa: SLF001

        with pytest.raises(LinearError, match="On Hold"):
            await harness.orch._states_for_binding(second_binding)  # noqa: SLF001
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_shutdown_closes_hot_added_tracker_clients(tmp_path: Path) -> None:
    """A hot-added client (`_hot_add_trackers`, a provider/site unseen at
    boot) has no owner besides the orchestrator's own bookkeeping — unlike
    boot clients, entered through `_configured_tracker_registry`'s
    `AsyncExitStack`. `aclose_hot_added_trackers` must close it or its
    underlying connection leaks for the rest of the process (SYM-189 review
    fix)."""
    conn = await db.connect(tmp_path / "symphony.sqlite")
    await _insert(conn, team="ENG")
    cfg = await assemble_effective_config(conn, _base(tmp_path))
    await conn.close()

    harness = await Harness.create(tmp_path, config=cfg, reload_bindings=True)
    try:
        await harness.warmup()

        class _FakeTracker:
            def __init__(self) -> None:
                self.closed = False

            async def aclose(self) -> None:
                self.closed = True

        built: list[_FakeTracker] = []

        def _factory(binding: RepoBinding) -> _FakeTracker:
            tracker = _FakeTracker()
            built.append(tracker)
            return tracker

        harness.orch._tracker_factory = _factory  # noqa: SLF001

        # A binding on a tracker site the process never saw at boot.
        await _insert(harness.conn, team="OPS", repo="org/ops", site="other-site", priority=1)
        await harness.orch._reload_bindings()  # noqa: SLF001
        await harness.orch._react_to_binding_set()  # noqa: SLF001

        assert len(built) == 1
        assert not built[0].closed

        await harness.orch.aclose_hot_added_trackers()
        assert built[0].closed
        # Idempotent: a second call (e.g. both the `once` and the daemon
        # shutdown path) must not error on an already-closed client.
        await harness.orch.aclose_hot_added_trackers()
    finally:
        await harness.close()


@pytest.mark.asyncio
async def test_jira_binding_states_edit_rebuilds_and_closes_tracker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A Jira binding's declared `states` are baked into its `JiraTracker` at
    construction (`for_binding`); `team_states()` just returns that captured
    mapping, so an edit to `linear_states` that leaves the binding's natural
    key (and so its tracker context) unchanged must still rebuild + register
    a fresh client, and close the stale one — `_react_to_binding_set`'s
    "binding set unchanged" early return must not skip this (SYM-189 review
    fix)."""
    binding = RepoBinding(
        linear_team_key="PROJ",
        github_repo="org/repo",
        provider="jira",
        tracker_site="acme",
        linear_states=LinearStates(ready="To Do", waiting="Blocked"),
    )
    cfg = Config(
        workspace_root=tmp_path / "workspaces",
        log_root=tmp_path / "logs",
        db_path=tmp_path / "symphony.sqlite",
        repos=[binding],
    )
    conn = await db.connect(cfg.db_path)
    try:

        class _FakeJiraTracker:
            def __init__(self, *, states: dict[str, str]) -> None:
                self.states = states
                self.closed = False

            async def aclose(self) -> None:
                self.closed = True

        built: list[_FakeJiraTracker] = []

        def _factory(b: RepoBinding) -> _FakeJiraTracker:
            tracker = _FakeJiraTracker(states=dict(b.linear_states.model_dump()))
            built.append(tracker)
            return tracker

        registry = TrackerRegistry()
        first = _factory(binding)
        registry.register("jira", "acme", first, project_key="PROJ")

        orch = Orchestrator(
            cfg, registry, conn, reload_bindings_from_db=True, tracker_factory=_factory
        )
        # Boot already reacted to this binding set — the bug is the early
        # return skipping a rebuild when the set (correctly) looks unchanged.
        orch._binding_keys = frozenset({_binding_key(binding)})  # noqa: SLF001

        edited = binding.model_copy(
            update={"linear_states": LinearStates(ready="To Do", waiting="On Hold")}
        )
        edited_cfg = cfg.model_copy(update={"repos": [edited]})

        async def _fake_assemble(conn, base, *, boot_gates, is_reload):
            return edited_cfg

        monkeypatch.setattr(
            "symphony.orchestrator.poll._base.assemble_effective_config", _fake_assemble
        )

        await orch._reload_bindings()  # noqa: SLF001
        await orch._react_to_binding_set()  # noqa: SLF001

        assert len(built) == 2
        assert built[1].states["waiting"] == "On Hold"
        assert first.closed
        ctx = TrackerContext(provider="jira", site="acme", project_key="PROJ")
        assert registry.get(ctx) is built[1]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_jira_binding_base_url_edit_rebuilds_tracker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A stable, explicit `tracker_site` keeps a Jira binding's natural key
    (and so its tracker context / `state_key`) unchanged across a `base_url`
    edit — `JiraTracker` bakes `base_url` in at construction, so this
    state-only comparison must still rebuild + register a fresh client
    instead of leaving the pre-edit URL live forever (SYM-189 review fix)."""
    binding = RepoBinding(
        linear_team_key="PROJ",
        github_repo="org/repo",
        provider="jira",
        tracker_site="acme",
        base_url="https://old.atlassian.net",
        linear_states=LinearStates(ready="To Do", waiting="Blocked"),
    )
    cfg = Config(
        workspace_root=tmp_path / "workspaces",
        log_root=tmp_path / "logs",
        db_path=tmp_path / "symphony.sqlite",
        repos=[binding],
    )
    conn = await db.connect(cfg.db_path)
    try:

        class _FakeJiraTracker:
            def __init__(self, *, base_url: str) -> None:
                self.base_url = base_url
                self.closed = False

            async def aclose(self) -> None:
                self.closed = True

        built: list[_FakeJiraTracker] = []

        def _factory(b: RepoBinding) -> _FakeJiraTracker:
            tracker = _FakeJiraTracker(base_url=b.base_url or "")
            built.append(tracker)
            return tracker

        registry = TrackerRegistry()
        first = _factory(binding)
        registry.register("jira", "acme", first, project_key="PROJ")

        orch = Orchestrator(
            cfg, registry, conn, reload_bindings_from_db=True, tracker_factory=_factory
        )
        orch._binding_keys = frozenset({_binding_key(binding)})  # noqa: SLF001

        edited = binding.model_copy(update={"base_url": "https://new.atlassian.net"})
        edited_cfg = cfg.model_copy(update={"repos": [edited]})

        async def _fake_assemble(conn, base, *, boot_gates, is_reload):
            return edited_cfg

        monkeypatch.setattr(
            "symphony.orchestrator.poll._base.assemble_effective_config", _fake_assemble
        )

        await orch._reload_bindings()  # noqa: SLF001
        await orch._react_to_binding_set()  # noqa: SLF001

        assert len(built) == 2
        assert built[1].base_url == "https://new.atlassian.net"
        assert first.closed
        ctx = TrackerContext(provider="jira", site="acme", project_key="PROJ")
        assert registry.get(ctx) is built[1]
    finally:
        await conn.close()


def _linear_binding(*, team: str, repo: str, site: str = "default") -> RepoBinding:
    return RepoBinding(
        linear_team_key=team,
        github_repo=repo,
        tracker_site=site,
        linear_states=LinearStates(
            ready="Todo", in_progress="In Progress", code_review="Needs Approval"
        ),
    )


class _FakeLinearTracker:
    def __init__(self) -> None:
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_hot_added_linear_tracker_aliased_as_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Boot registration aliases the first Linear tracker it sees to
    `(linear, default)`, for callers such as `_external_linear_tracker` that
    resolve the daemon's default Linear client without a full
    `TrackerContext`. A fresh install boots with zero bindings — no alias is
    set at all — and hot-adds its first Linear binding, on a non-default
    site, later; the hot-add path must set that alias too, or those callers
    stay broken forever with no restart able to fix it (SYM-189 review fix).
    """
    cfg = Config(
        workspace_root=tmp_path / "workspaces",
        log_root=tmp_path / "logs",
        db_path=tmp_path / "symphony.sqlite",
        repos=[],
    )
    conn = await db.connect(cfg.db_path)
    try:
        built: list[_FakeLinearTracker] = []

        def _factory(b: RepoBinding) -> _FakeLinearTracker:
            tracker = _FakeLinearTracker()
            built.append(tracker)
            return tracker

        registry = TrackerRegistry()
        orch = Orchestrator(
            cfg, registry, conn, reload_bindings_from_db=True, tracker_factory=_factory
        )
        orch._binding_keys = frozenset()  # noqa: SLF001 — boot reacted to the empty set

        binding = _linear_binding(team="ENG", repo="org/repo", site="acme")
        edited_cfg = cfg.model_copy(update={"repos": [binding]})

        async def _fake_assemble(conn, base, *, boot_gates, is_reload):
            return edited_cfg

        monkeypatch.setattr(
            "symphony.orchestrator.poll._base.assemble_effective_config", _fake_assemble
        )

        await orch._reload_bindings()  # noqa: SLF001
        await orch._react_to_binding_set()  # noqa: SLF001

        assert len(built) == 1
        assert registry.get(TrackerContext(provider="linear", site="acme")) is built[0]
        assert registry.get(TrackerContext()) is built[0]
        assert registry.resolve(TrackerContext()) is built[0]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_removed_binding_closes_and_unregisters_hot_added_tracker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A tracker `_hot_add_trackers` builds for a provider/site never seen at
    boot has no owner besides the orchestrator's own bookkeeping. If the
    binding that introduced it is later deleted, the client must close (and
    its registry entry drop) right away rather than only at process
    shutdown, or the connection leaks for the rest of the run — and a future
    binding reintroducing the same context would otherwise resolve the
    already-closed client instead of hot-adding a fresh one (SYM-189 review
    fix)."""
    cfg = Config(
        workspace_root=tmp_path / "workspaces",
        log_root=tmp_path / "logs",
        db_path=tmp_path / "symphony.sqlite",
        repos=[],
    )
    conn = await db.connect(cfg.db_path)
    try:
        built: list[_FakeLinearTracker] = []

        def _factory(b: RepoBinding) -> _FakeLinearTracker:
            tracker = _FakeLinearTracker()
            built.append(tracker)
            return tracker

        registry = TrackerRegistry()
        orch = Orchestrator(
            cfg, registry, conn, reload_bindings_from_db=True, tracker_factory=_factory
        )
        orch._binding_keys = frozenset()  # noqa: SLF001 — boot reacted to the empty set

        binding = _linear_binding(team="OPS", repo="org/ops", site="acme")
        with_binding_cfg = cfg.model_copy(update={"repos": [binding]})

        async def _fake_assemble_add(conn, base, *, boot_gates, is_reload):
            return with_binding_cfg

        monkeypatch.setattr(
            "symphony.orchestrator.poll._base.assemble_effective_config", _fake_assemble_add
        )
        await orch._reload_bindings()  # noqa: SLF001
        await orch._react_to_binding_set()  # noqa: SLF001

        ctx = TrackerContext(provider="linear", site="acme")
        tracker = registry.get(ctx)
        assert tracker is built[0]
        assert not tracker.closed
        # Hot-adding the only Linear binding also aliases it as the default.
        assert registry.get(TrackerContext()) is tracker

        async def _fake_assemble_remove(conn, base, *, boot_gates, is_reload):
            return cfg

        monkeypatch.setattr(
            "symphony.orchestrator.poll._base.assemble_effective_config", _fake_assemble_remove
        )
        await orch._reload_bindings()  # noqa: SLF001
        await orch._react_to_binding_set()  # noqa: SLF001

        assert tracker.closed
        assert registry.get(ctx) is None
        # The default alias must not keep resolving to the now-closed client.
        assert registry.get(TrackerContext()) is None
    finally:
        await conn.close()
