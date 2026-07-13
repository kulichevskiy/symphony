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
from symphony.config import Config
from symphony.effective_config import assemble_effective_config
from symphony.linear.client import LinearError
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
        harness.sim.seed_issue(
            identifier="ENG-1", team_key="ENG", state_name=READY, title="kept"
        )
        harness.sim.seed_issue(
            identifier="OPS-1", team_key="OPS", state_name=READY, title="queued"
        )
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
            await asyncio.sleep(0)
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
        harness.sim.seed_issue(
            identifier="ENG-1", team_key="ENG", state_name=READY, title="queued"
        )
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

        ops_binding = next(
            b for b in harness.orch.config.repos if b.linear_team_key == "OPS"
        )
        with pytest.raises(LinearError, match="Blocked"):
            await harness.orch._states_for_binding(ops_binding)  # noqa: SLF001
    finally:
        await harness.close()
