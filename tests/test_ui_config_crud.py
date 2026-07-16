"""Binding CRUD over the app harness — real FastAPI app, temp SQLite (SYM-190).

Asserts external behavior at the HTTP seam: CRUD round-trips, duplicate-selector
rejection, optimistic-lock conflicts, field-path validation errors, `updated_by`
stamping, secret redaction, and the options payload.
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx
from fastapi import FastAPI

from symphony import db
from symphony.app import create_app
from symphony.config import Config
from symphony.db import config_bindings
from symphony.ui.config_crud import create_config_crud_router

from .test_auth import JWKS_URI, _jwks, _settings, _token
from .test_webhook import _Handler


def _binding_key_str(rec: dict[str, Any]) -> str:
    return json.dumps(
        [
            rec["project_key"],
            rec["github_repo"],
            rec["issue_label"],
            rec["tracker_provider"],
            rec["tracker_site"],
        ],
        separators=(",", ":"),
    )


def _drain_app(conn: Any, *, scheduled_slots: Any = None) -> FastAPI:
    """A minimal app mounting only the CRUD router, so a test can inject a
    `scheduled_slots` provider (the in-memory drain-guard input the daemon
    supplies in production)."""

    async def _provider() -> Any:
        return conn

    app = FastAPI()
    app.include_router(
        create_config_crud_router(
            _provider,
            config_provider=Config(
                github_webhook_secret="test-global-secret",
                linear_api_key="test-linear-key",
            ),
            scheduled_slots=scheduled_slots,
        )
    )
    return app


def _payload(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "project_key": "ENG",
        "github_repo": "org/repo",
        "states": {"ready": "Todo"},
    }
    base.update(overrides)
    return base


def _app(
    conn: Any,
    db_path: Path,
    *,
    auth: bool = False,
    github_webhook_secret: str = "test-global-secret",
    linear_api_key: str = "test-linear-key",
) -> Any:
    # A global secret/key is set by default so tests unrelated to a specific
    # check don't trip it; the tests for each check override the relevant
    # kwarg to empty to exercise it directly.
    return create_app(
        _Handler(),
        conn,
        ui_enabled=True,
        ui_db_path=db_path,
        ui_external_config=Config(
            github_webhook_secret=github_webhook_secret,
            linear_api_key=linear_api_key,
        ),
        auth0_settings=_settings() if auth else None,
    )


async def _open(tmp_path: Path) -> tuple[Any, Path]:
    db_path = tmp_path / "state.sqlite"
    conn = await db.connect(db_path)
    return conn, db_path


def _client(app: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


@pytest.mark.asyncio
async def test_crud_round_trip(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            created = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(max_concurrent=3), "priority": 2},
            )
            assert created.status_code == 201, created.text
            rec = created.json()
            bid = rec["id"]
            assert rec["version"] == 1
            assert rec["updated_by"] == "local"
            assert rec["priority"] == 2
            assert rec["payload"]["max_concurrent"] == 3

            listed = await client.get("/api/config/bindings")
            assert [b["id"] for b in listed.json()] == [bid]

            got = await client.get(f"/api/config/bindings/{bid}")
            assert got.status_code == 200 and got.json()["id"] == bid

            updated = await client.put(
                f"/api/config/bindings/{bid}",
                json={
                    "payload": _payload(max_concurrent=5),
                    "version": 1,
                },
            )
            assert updated.status_code == 200, updated.text
            assert updated.json()["version"] == 2
            assert updated.json()["enabled"] is True
            assert updated.json()["payload"]["max_concurrent"] == 5

            deleted = await client.delete(f"/api/config/bindings/{bid}?version=2")
            assert deleted.status_code == 204
            assert (await client.get("/api/config/bindings")).json() == []
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_binding_crud_survives_shared_connection_being_closed(
    tmp_path: Path,
) -> None:
    """The CRUD router must write through its own connection (`WriteDbPool`),
    never the daemon's shared `conn` — a `commit()` on a connection the
    orchestrator also writes through would flush whatever unrelated,
    not-yet-committed statements it had pending at that moment (SYM-190).
    Closing `conn` before any CRUD request proves the router never touches
    it: with the old shared-connection wiring this would fail with
    "Cannot operate on a closed database"."""
    conn, db_path = await _open(tmp_path)
    app = _app(conn, db_path)
    await conn.close()
    async with _client(app) as client:
        created = await client.post("/api/config/bindings", json={"payload": _payload()})
        assert created.status_code == 201, created.text
        listed = await client.get("/api/config/bindings")
        assert [b["id"] for b in listed.json()] == [created.json()["id"]]


@pytest.mark.asyncio
async def test_duplicate_selector_rejected_including_unlabeled(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            # Unlabeled catch-all on repo-a.
            first = await client.post(
                "/api/config/bindings", json={"payload": _payload(github_repo="org/a")}
            )
            assert first.status_code == 201
            # Same scope + (empty) label + ready state, different repo → exact
            # duplicate selector, rejected.
            dup = await client.post(
                "/api/config/bindings", json={"payload": _payload(github_repo="org/b")}
            )
            assert dup.status_code == 422
            assert dup.json()["detail"][0]["loc"] == ["issue_label"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_same_label_different_ready_state_accepted(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            a = await client.post(
                "/api/config/bindings",
                json={
                    "payload": _payload(
                        github_repo="org/a", issue_label="bug", states={"ready": "Backlog"}
                    )
                },
            )
            assert a.status_code == 201, a.text
            # Same tracker scope + same label but a different ready lane — a
            # legitimate two-lane setup, accepted.
            b = await client.post(
                "/api/config/bindings",
                json={
                    "payload": _payload(
                        github_repo="org/b", issue_label="bug", states={"ready": "Todo"}
                    )
                },
            )
            assert b.status_code == 201, b.text
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_same_scope_different_ready_state_same_repo_rolls_back(tmp_path: Path) -> None:
    """Same project/repo/label/site with a different ready state passes the
    selector duplicate check (ready state makes it a legitimate two-lane
    setup) but the DB's unique index — keyed on natural-key columns only, no
    ready state — still catches it. The resulting `IntegrityError` must roll
    back the write transaction the failed `INSERT` opened, or it holds the
    write lock until some later, unrelated commit closes it."""
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            first = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(issue_label="bug", states={"ready": "Backlog"})},
            )
            assert first.status_code == 201, first.text
            dup = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(issue_label="bug", states={"ready": "Todo"})},
            )
            assert dup.status_code == 422
            assert dup.json()["detail"][0]["loc"] == ["github_repo"]
            assert not conn.in_transaction

            # The connection must still be writable afterwards.
            follow_up = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(github_repo="org/other")},
            )
            assert follow_up.status_code == 201, follow_up.text
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_enabled_toggle_persists(tmp_path: Path) -> None:
    """The `enabled` toggle is honored now that the binding lifecycle ships
    (SYM-193): a disabled binding is created/updated and reads back disabled."""
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            created_disabled = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(), "enabled": False},
            )
            assert created_disabled.status_code == 201, created_disabled.text
            bid = created_disabled.json()["id"]
            assert created_disabled.json()["enabled"] is False

            # Re-enable via update.
            reenabled = await client.put(
                f"/api/config/bindings/{bid}",
                json={"payload": _payload(), "version": 1, "enabled": True},
            )
            assert reenabled.status_code == 200, reenabled.text
            assert reenabled.json()["enabled"] is True

            # And disable again.
            disabled = await client.put(
                f"/api/config/bindings/{bid}",
                json={"payload": _payload(), "version": 2, "enabled": False},
            )
            assert disabled.status_code == 200, disabled.text
            assert disabled.json()["enabled"] is False
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_stale_version_conflict(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            bid = (await client.post("/api/config/bindings", json={"payload": _payload()})).json()[
                "id"
            ]
            stale = await client.put(
                f"/api/config/bindings/{bid}",
                json={"payload": _payload(max_concurrent=4), "version": 99},
            )
            assert stale.status_code == 409
            assert stale.json()["detail"]["current_version"] == 1
            # DELETE with a stale version conflicts too.
            del_stale = await client.delete(f"/api/config/bindings/{bid}?version=99")
            assert del_stale.status_code == 409
            # Neither conflict left a write transaction open on the shared
            # connection (both roll back before raising).
            assert conn.in_transaction is False

            # A same-connection write right after the conflicts still lands
            # cleanly — proof the dangling transaction isn't blocking it.
            retry = await client.put(
                f"/api/config/bindings/{bid}",
                json={"payload": _payload(max_concurrent=4), "version": 1},
            )
            assert retry.status_code == 200, retry.text
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_field_validation_error_carries_loc(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            # Missing required `states.ready`.
            bad_states = await client.post(
                "/api/config/bindings",
                json={"payload": {"project_key": "ENG", "github_repo": "org/repo", "states": {}}},
            )
            assert bad_states.status_code == 422
            locs = [tuple(err["loc"]) for err in bad_states.json()["detail"]]
            assert ("states", "ready") in locs

            # Bad merge strategy → the exact field.
            bad_merge = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(merge_strategy="fast-forward")},
            )
            assert bad_merge.status_code == 422
            assert any(err["loc"][-1] == "merge_strategy" for err in bad_merge.json()["detail"])
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_legacy_role_field_rejected_with_loc(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.post(
                "/api/config/bindings", json={"payload": _payload(agent="codex")}
            )
            assert resp.status_code == 422
            assert resp.json()["detail"][0]["loc"] == ["agent"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_unknown_field_rejected_with_loc(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.post(
                "/api/config/bindings", json={"payload": _payload(max_concurent=3)}
            )
            assert resp.status_code == 422
            assert resp.json()["detail"][0]["loc"] == ["max_concurent"]

            # Persisted `.env`/YAML-alias spellings are legal, not unknown.
            aliased = await client.post(
                "/api/config/bindings",
                json={
                    "payload": {
                        "linear_team_key": "ENG",
                        "github_repo": "org/repo2",
                        "linear_states": {"ready": "Todo"},
                    }
                },
            )
            assert aliased.status_code == 201
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_roles_matrix_error_shows_roles_path(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.post(
                "/api/config/bindings",
                json={
                    "payload": _payload(
                        roles={"implement": {"agent": "codex", "model": "not-a-model"}}
                    )
                },
            )
            assert resp.status_code == 422
            assert resp.json()["detail"][0]["loc"] == ["roles"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_unknown_env_key_fails_closed(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(env={"MY_TOKEN": "DEFINITELY_NOT_A_REAL_ENV_KEY_XZ"})},
            )
            assert resp.status_code == 422
            assert resp.json()["detail"][0]["loc"] == ["env"]
            assert "DEFINITELY_NOT_A_REAL_ENV_KEY_XZ" in resp.json()["detail"][0]["msg"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_webhook_secret_redacted_on_read(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            created = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(webhook_secret="s3cr3t-should-never-leak")},
            )
            assert created.status_code == 201
            assert "s3cr3t-should-never-leak" not in created.text
            assert created.json()["webhook_secret_set"] is True
            assert "webhook_secret" not in created.json()["payload"]

            listed = await client.get("/api/config/bindings")
            assert "s3cr3t-should-never-leak" not in listed.text
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_webhook_secret_survives_edit_of_redacted_payload(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            created = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(webhook_secret="s3cr3t-should-never-leak")},
            )
            assert created.status_code == 201, created.text
            bid = created.json()["id"]

            got = await client.get(f"/api/config/bindings/{bid}")
            assert got.json()["webhook_secret_set"] is True
            assert "webhook_secret" not in got.json()["payload"]

            # Edit re-sends exactly the redacted GET payload (as the form
            # does) — the stored secret must survive, not be cleared.
            put = await client.put(
                f"/api/config/bindings/{bid}",
                json={
                    "payload": {**got.json()["payload"], "max_concurrent": 7},
                    "enabled": got.json()["enabled"],
                    "priority": got.json()["priority"],
                    "version": got.json()["version"],
                },
            )
            assert put.status_code == 200, put.text
            assert put.json()["webhook_secret_set"] is True
            assert "s3cr3t-should-never-leak" not in put.text
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_repo_secret_metadata_in_response(tmp_path: Path) -> None:
    """The response carries the repo-secret set flag, its own `version`, and
    updated metadata — never the value (SYM-194)."""
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            created = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(webhook_secret="s3cr3t")},
            )
            assert created.status_code == 201, created.text
            rec = created.json()
            assert rec["webhook_secret_set"] is True
            assert rec["webhook_secret_version"] == 1
            assert rec["webhook_secret_updated_by"] == "local"
            assert "s3cr3t" not in created.text
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_repo_secret_version_conflict_across_bindings(tmp_path: Path) -> None:
    """The repo secret is shared across a repo's bindings, so its own `version`
    guards concurrent edits: two tabs editing *different* bindings of the same
    repo conflict on the shared secret (SYM-194 acceptance)."""
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            b1 = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(webhook_secret="a")},
            )
            assert b1.status_code == 201, b1.text
            assert b1.json()["webhook_secret_version"] == 1

            # A second binding on the SAME repo (distinct label — not a
            # duplicate selector) sees the shared secret at version 1.
            b2 = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(issue_label="bug")},
            )
            assert b2.status_code == 201, b2.text
            assert b2.json()["webhook_secret_version"] == 1

            # Tab A replaces the secret from binding 1 → version 2.
            put1 = await client.put(
                f"/api/config/bindings/{b1.json()['id']}",
                json={
                    "payload": _payload(webhook_secret="b"),
                    "version": b1.json()["version"],
                    "webhook_secret_version": 1,
                },
            )
            assert put1.status_code == 200, put1.text
            assert put1.json()["webhook_secret_version"] == 2

            # Tab B still holds version 1 and tries to replace it from binding
            # 2 → conflict on the shared repo secret.
            put2 = await client.put(
                f"/api/config/bindings/{b2.json()['id']}",
                json={
                    "payload": _payload(issue_label="bug", webhook_secret="c"),
                    "version": b2.json()["version"],
                    "webhook_secret_version": 1,
                },
            )
            assert put2.status_code == 409, put2.text
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_repo_secret_version_conflict_on_create(tmp_path: Path) -> None:
    """A create has no prior binding to have loaded a repo-secret version
    from, so `webhook_secret_version` is normally omitted (`None`) on a
    create request. That must not fall back to an unconditional overwrite of
    an already-secreted repo: two concurrent creates racing to set the same
    repo's secret must still serialize under the shared secret's optimistic
    lock, same as two tabs editing existing bindings (SYM-194 review)."""
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            seed = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(webhook_secret="a")},
            )
            assert seed.status_code == 201, seed.text
            assert seed.json()["webhook_secret_version"] == 1

            # Two more bindings on the SAME repo (distinct labels), each
            # racing to replace the shared secret with no explicit
            # `webhook_secret_version` — neither client loaded one, since
            # neither is editing an existing binding.
            results = await asyncio.gather(
                client.post(
                    "/api/config/bindings",
                    json={"payload": _payload(issue_label="one", webhook_secret="b")},
                ),
                client.post(
                    "/api/config/bindings",
                    json={"payload": _payload(issue_label="two", webhook_secret="c")},
                ),
            )
            statuses = sorted(r.status_code for r in results)
            assert statuses == [201, 409], [r.text for r in results]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_webhook_enabled_rename_validates_against_new_repos_secret(
    tmp_path: Path,
) -> None:
    """A rename (`github_repo` change) must validate `webhook_enabled` against
    the *target* repo's secret, not the repo being renamed away from — else a
    rename onto an unsecured repo with no global fallback saves cleanly with
    verification silently disabled (SYM-194 review)."""
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path, github_webhook_secret="")
        async with _client(app) as client:
            created = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(webhook_enabled=True, webhook_secret="s3cr3t")},
            )
            assert created.status_code == 201, created.text

            renamed = await client.put(
                f"/api/config/bindings/{created.json()['id']}",
                json={
                    "payload": _payload(github_repo="org/other", webhook_enabled=True),
                    "version": created.json()["version"],
                },
            )
            assert renamed.status_code == 422, renamed.text
            assert renamed.json()["detail"][0]["loc"] == ["webhook_secret"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_repo_secret_version_conflict_on_update_with_omitted_version(
    tmp_path: Path,
) -> None:
    """Mirrors `test_repo_secret_version_conflict_on_create`: an update that
    omits `webhook_secret_version` must not fall back to an unconditional
    overwrite of the shared repo secret — it has to share create's
    optimistic-lock contract (SYM-194 review)."""
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            b1 = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(webhook_secret="a")},
            )
            assert b1.status_code == 201, b1.text
            b2 = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(issue_label="bug")},
            )
            assert b2.status_code == 201, b2.text

            # Both tabs race to replace the shared secret with no explicit
            # `webhook_secret_version` — before the fix, this drove
            # `set_secret` into its unconditional-overwrite branch and both
            # writes would land.
            results = await asyncio.gather(
                client.put(
                    f"/api/config/bindings/{b1.json()['id']}",
                    json={"payload": _payload(webhook_secret="b"), "version": b1.json()["version"]},
                ),
                client.put(
                    f"/api/config/bindings/{b2.json()['id']}",
                    json={
                        "payload": _payload(issue_label="bug", webhook_secret="c"),
                        "version": b2.json()["version"],
                    },
                ),
            )
            statuses = sorted(r.status_code for r in results)
            assert statuses == [200, 409], [r.text for r in results]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_repo_secret_explicit_clear(tmp_path: Path) -> None:
    """Omit keeps; an explicit clear marker removes the secret (SYM-194)."""
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            created = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(webhook_secret="s3cr3t")},
            )
            bid = created.json()["id"]
            assert created.json()["webhook_secret_set"] is True

            # Ordinary edit that omits the secret → kept.
            got = await client.get(f"/api/config/bindings/{bid}")
            kept = await client.put(
                f"/api/config/bindings/{bid}",
                json={
                    "payload": {**got.json()["payload"], "max_concurrent": 7},
                    "version": got.json()["version"],
                },
            )
            assert kept.status_code == 200, kept.text
            assert kept.json()["webhook_secret_set"] is True

            # Explicit clear marker → removed.
            cleared = await client.put(
                f"/api/config/bindings/{bid}",
                json={
                    "payload": {**kept.json()["payload"]},
                    "version": kept.json()["version"],
                    "webhook_secret_clear": True,
                    "webhook_secret_version": kept.json()["webhook_secret_version"],
                },
            )
            assert cleared.status_code == 200, cleared.text
            assert cleared.json()["webhook_secret_set"] is False
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_mcp_server_secrets_redacted_on_read(tmp_path: Path) -> None:
    """`mcp_servers` has no `resolve_env`-style name indirection (unlike the
    top-level `env` field), so an operator embeds literal credentials
    straight into a server's `env`/`headers` — those must never appear in a
    response."""
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            created = await client.post(
                "/api/config/bindings",
                json={
                    "payload": _payload(
                        mcp_servers={
                            "supabase": {
                                "command": "npx",
                                "env": {"API_KEY": "sk-should-never-leak"},
                            }
                        }
                    )
                },
            )
            assert created.status_code == 201, created.text
            assert "sk-should-never-leak" not in created.text
            assert created.json()["payload"]["mcp_servers"] == {
                "supabase": {"command": "npx", "env": {"API_KEY": True}}
            }

            listed = await client.get("/api/config/bindings")
            assert "sk-should-never-leak" not in listed.text
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_mcp_server_secrets_survive_edit_of_redacted_payload(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            created = await client.post(
                "/api/config/bindings",
                json={
                    "payload": _payload(
                        mcp_servers={
                            "supabase": {
                                "command": "npx",
                                "env": {"API_KEY": "sk-should-never-leak"},
                            }
                        }
                    )
                },
            )
            bid = created.json()["id"]
            got = await client.get(f"/api/config/bindings/{bid}")

            # Edit re-sends exactly the redacted GET payload (as the
            # collapsible raw-JSON section does) plus an unrelated change —
            # the stored secret must survive, not be overwritten with the
            # `true` redaction placeholder.
            put = await client.put(
                f"/api/config/bindings/{bid}",
                json={
                    "payload": {**got.json()["payload"], "max_concurrent": 7},
                    "enabled": got.json()["enabled"],
                    "priority": got.json()["priority"],
                    "version": got.json()["version"],
                },
            )
            assert put.status_code == 200, put.text
            assert "sk-should-never-leak" not in put.text

            stored = await config_bindings.get(conn, bid)
            assert stored is not None
            assert stored.payload["mcp_servers"]["supabase"]["env"] == {
                "API_KEY": "sk-should-never-leak"
            }

            # A real edit (a literal string, not the `true` placeholder)
            # still overrides the stored secret.
            rotated = await client.put(
                f"/api/config/bindings/{bid}",
                json={
                    "payload": {
                        **put.json()["payload"],
                        "mcp_servers": {
                            "supabase": {"command": "npx", "env": {"API_KEY": "sk-rotated"}}
                        },
                    },
                    "enabled": put.json()["enabled"],
                    "priority": put.json()["priority"],
                    "version": put.json()["version"],
                },
            )
            assert rotated.status_code == 200, rotated.text
            stored = await config_bindings.get(conn, bid)
            assert stored is not None
            assert stored.payload["mcp_servers"]["supabase"]["env"] == {"API_KEY": "sk-rotated"}
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_diff_logged_without_secret_values(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    conn, db_path = await _open(tmp_path)
    caplog.set_level(logging.INFO, logger="symphony.ui.config_crud")
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            created = await client.post(
                "/api/config/bindings",
                json={
                    "payload": _payload(
                        webhook_secret="topsecret",
                        max_concurrent=9,
                        mcp_servers={"supabase": {"env": {"API_KEY": "mcp-topsecret"}}},
                    )
                },
            )
            bid = created.json()["id"]
            await client.put(
                f"/api/config/bindings/{bid}",
                json={
                    "payload": _payload(
                        webhook_secret="rotated",
                        max_concurrent=1,
                        mcp_servers={"supabase": {"env": {"API_KEY": "mcp-rotated"}}},
                    ),
                    "version": 1,
                },
            )
    finally:
        await conn.close()
    text = caplog.text
    assert "topsecret" not in text and "rotated" not in text
    assert "mcp-topsecret" not in text and "mcp-rotated" not in text
    assert "webhook_secret" in text  # the flag, not the value
    assert "max_concurrent" in text
    assert "mcp_servers" in text  # which servers changed, not their contents


def test_diff_never_reports_legacy_payload_secret_as_cleared() -> None:
    """A binding row from before the repo-scoped secret table (SYM-190..193)
    can still carry `webhook_secret` in its stored payload if it somehow
    reaches `_diff` before the schema migration strips it. The per-field diff
    must never report that as a spurious "cleared" for a value the operator
    never touched — `_add_secret_flag` (fed from the real secret-table state)
    owns `webhook_secret`'s audit flag, not this generic field diff
    (SYM-194 review)."""
    from symphony.db import config_bindings
    from symphony.ui import config_crud

    old = config_bindings.StoredBinding(
        id=1,
        payload={
            "github_repo": "org/repo",
            "project_key": "ENG",
            "webhook_secret": "legacy-value-should-never-log",
            "max_concurrent": 4,
        },
        version=1,
        enabled=True,
        priority=0,
        updated_at="",
        updated_by="",
        project_key="ENG",
        github_repo="org/repo",
        issue_label="",
        tracker_provider="linear",
        tracker_site="default",
    )
    new_payload = {"github_repo": "org/repo", "project_key": "ENG", "max_concurrent": 5}

    changes = config_crud._diff(old, new_payload, enabled=True, priority=0)

    assert "webhook_secret" not in changes
    assert changes["max_concurrent"] == {"from": 4, "to": 5}


@pytest.mark.asyncio
async def test_same_family_review_returns_nonblocking_warning(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            # Both implement and review on codex → save succeeds but warns.
            resp = await client.post(
                "/api/config/bindings",
                json={
                    "payload": _payload(
                        roles={
                            "implement": {"agent": "codex"},
                            "review_find": {"agent": "codex"},
                            "review_verify": {"agent": "codex"},
                        }
                    )
                },
            )
            assert resp.status_code == 201, resp.text
            assert any("diversity" in w for w in resp.json()["warnings"])
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_webhook_enabled_without_secret_rejected(tmp_path: Path) -> None:
    """`webhook_enabled: true` with no per-binding secret and no global
    `GITHUB_WEBHOOK_SECRET` must fail closed at save time — otherwise the
    daemon's hot-reload path (`cli._live_github_webhook_settings`) swallows
    the misconfiguration and silently disables *every* repo's webhook
    verification, not just this one."""
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path, github_webhook_secret="")
        async with _client(app) as client:
            resp = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(webhook_enabled=True)},
            )
            assert resp.status_code == 422
            assert resp.json()["detail"][0]["loc"] == ["webhook_secret"]

            # A per-binding secret satisfies it.
            with_secret = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(webhook_enabled=True, webhook_secret="s3cr3t")},
            )
            assert with_secret.status_code == 201, with_secret.text
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_linear_binding_without_api_key_rejected(tmp_path: Path) -> None:
    """A fresh DB-owned install boots with zero bindings, so `cli._run`'s
    boot-time `LINEAR_API_KEY` check never fires before the first linear
    binding is created via the UI — this write path must be the fail-closed
    gate instead, or the binding saves cleanly and then silently never
    dispatches (`Orchestrator._reload_bindings` swallows the resulting
    `for_binding` `ValueError` and just logs it)."""
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path, linear_api_key="")
        async with _client(app) as client:
            resp = await client.post("/api/config/bindings", json={"payload": _payload()})
            assert resp.status_code == 422
            assert resp.json()["detail"][0]["loc"] == ["provider"]
            assert "LINEAR_API_KEY" in resp.json()["detail"][0]["msg"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_jira_binding_without_credentials_rejected(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_external_config=Config(github_webhook_secret="test-global-secret"),
        )
        async with _client(app) as client:
            resp = await client.post(
                "/api/config/bindings", json={"payload": _payload(provider="jira")}
            )
            assert resp.status_code == 422
            assert resp.json()["detail"][0]["loc"] == ["provider"]
            msg = resp.json()["detail"][0]["msg"]
            assert "JIRA_BASE_URL" in msg
            assert "JIRA_EMAIL" in msg
            assert "JIRA_API_TOKEN" in msg
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_jira_tracker_site_normalized_from_global_base_url(tmp_path: Path) -> None:
    """A Jira binding with no per-binding `base_url` keys on the global
    `jira_base_url`, matching `assemble_effective_config`'s resolution — not
    the "default" placeholder `RepoBinding.model_validate` alone would leave
    it with."""
    conn, db_path = await _open(tmp_path)
    try:
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_external_config=Config(
                jira_base_url="https://issues.example.com",
                jira_email="jira@example.com",
                jira_api_token="test-jira-token",
                github_webhook_secret="test-global-secret",
            ),
        )
        async with _client(app) as client:
            resp = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(provider="jira")},
            )
            assert resp.status_code == 201, resp.text
            assert resp.json()["tracker_site"] == "https://issues.example.com"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_duplicate_rejected_across_jira_bindings_relying_on_global_base_url(
    tmp_path: Path,
) -> None:
    """The duplicate-selector check must normalize *existing* rows the same
    way it normalizes the candidate: a stored Jira binding with no per-binding
    `base_url` has no `tracker_site` of its own in its payload, and without
    re-deriving it from the global `jira_base_url` it would compare against
    the "default" placeholder instead of the site it actually resolves to —
    letting an exact duplicate slip through."""
    conn, db_path = await _open(tmp_path)
    try:
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_external_config=Config(
                jira_base_url="https://issues.example.com",
                jira_email="jira@example.com",
                jira_api_token="test-jira-token",
                github_webhook_secret="test-global-secret",
            ),
        )
        async with _client(app) as client:
            first = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(provider="jira", github_repo="org/a")},
            )
            assert first.status_code == 201, first.text
            dup = await client.post(
                "/api/config/bindings",
                json={"payload": _payload(provider="jira", github_repo="org/b")},
            )
            assert dup.status_code == 422
            assert dup.json()["detail"][0]["loc"] == ["issue_label"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_options_payload(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    async def _caps(model: str, api_key: str | None = None) -> list[str] | None:
        return None

    monkeypatch.setattr("symphony.ui.config_crud.fetch_claude_effort_capabilities", _caps)
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.get("/api/config/options")
        body = resp.json()
        assert body["agent_families"] == ["claude", "codex"]
        assert body["merge_strategies"] == ["squash", "merge", "rebase"]
        assert set(body["claude_aliases"]) == {"opus", "sonnet", "haiku"}
        assert "gpt-5.1-codex" in body["codex_models"]
    finally:
        await conn.close()


@pytest.mark.asyncio
@respx.mock
async def test_updated_by_uses_auth_email(tmp_path: Path) -> None:
    respx.get(JWKS_URI).mock(return_value=httpx.Response(200, json=_jwks()))
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path, auth=True)
        async with _client(app) as client:
            headers = {"Authorization": f"Bearer {_token()}"}
            created = await client.post(
                "/api/config/bindings", json={"payload": _payload()}, headers=headers
            )
            assert created.status_code == 201, created.text
            assert created.json()["updated_by"] == "alice@example.com"

            # Unauthenticated write is rejected by the shared gate.
            unauth = await client.post("/api/config/bindings", json={"payload": _payload()})
            assert unauth.status_code == 401
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_crud_router_not_mounted_when_yaml_owns_topology(tmp_path: Path) -> None:
    # A legacy YAML topology not yet imported keeps `reload_bindings_from_db`
    # off, so the daemon never applies a write made here — the router must not
    # mount rather than accept writes it will silently ignore.
    conn, db_path = await _open(tmp_path)
    try:
        app = create_app(
            _Handler(),
            conn,
            ui_enabled=True,
            ui_db_path=db_path,
            ui_external_config=Config(github_webhook_secret="test-global-secret"),
            ui_db_owns_topology=False,
        )
        async with _client(app) as client:
            options = await client.get("/api/config/options")
            bindings = await client.get("/api/config/bindings")
            created = await client.post("/api/config/bindings", json={"payload": _payload()})
        assert options.status_code == 404
        assert bindings.status_code == 404
        assert created.status_code == 404
    finally:
        await conn.close()


# --- global roles matrix (SYM-191) -----------------------------------------


@pytest.mark.asyncio
async def test_roles_get_put_round_trip(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            # Fresh DB: no globals row yet → empty matrix at version 0.
            got = await client.get("/api/config/roles")
            assert got.status_code == 200
            assert got.json() == {"roles": {}, "version": 0}

            put = await client.put(
                "/api/config/roles",
                json={"roles": {"implement": {"agent": "codex"}}, "version": 0},
            )
            assert put.status_code == 200, put.text
            assert put.json()["version"] == 1
            assert put.json()["roles"] == {"implement": {"agent": "codex"}}

            reread = await client.get("/api/config/roles")
            assert reread.json() == {
                "roles": {"implement": {"agent": "codex"}},
                "version": 1,
            }
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_roles_put_rejects_bad_effort_with_zero_bindings(tmp_path: Path) -> None:
    """A fresh DB has no bindings for the family/effort loop to walk — the
    global matrix must still be validated on its own (SYM-191 review)."""
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            put = await client.put(
                "/api/config/roles",
                json={
                    "roles": {"implement": {"agent": "claude", "effort": "turbo"}},
                    "version": 0,
                },
            )
            assert put.status_code == 422, put.text
            assert put.json()["detail"][0]["loc"] == ["roles"]

            # Rejected, not persisted — a reread still shows the empty matrix.
            reread = await client.get("/api/config/roles")
            assert reread.json() == {"roles": {}, "version": 0}
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_roles_put_rejects_unknown_role_cell_field(tmp_path: Path) -> None:
    """A typo'd role-cell key (e.g. `effr` for `effort`) is rejected, not
    silently dropped — `RoleConfig`'s default `extra="ignore"` would otherwise
    persist a cell the operator thinks is set but the daemon never sees
    (SYM-191 review)."""
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            put = await client.put(
                "/api/config/roles",
                json={
                    "roles": {"implement": {"agent": "claude", "effr": "high"}},
                    "version": 0,
                },
            )
            assert put.status_code == 422, put.text

            # Rejected, not persisted — a reread still shows the empty matrix.
            reread = await client.get("/api/config/roles")
            assert reread.json() == {"roles": {}, "version": 0}
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_binding_rejects_unknown_role_cell_field(tmp_path: Path) -> None:
    """The same unknown-field guard applies to a per-binding `roles:`
    override — `RepoBinding.roles` shares the same `RoleConfig` cell type."""
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.post(
                "/api/config/bindings",
                json={
                    "payload": _payload(roles={"implement": {"agent": "claude", "effr": "high"}})
                },
            )
            assert resp.status_code == 422, resp.text
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_roles_put_version_conflict(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            first = await client.put(
                "/api/config/roles",
                json={"roles": {"implement": {"agent": "codex"}}, "version": 0},
            )
            assert first.status_code == 200
            # A second write still carrying the stale version 0 → 409.
            stale = await client.put(
                "/api/config/roles",
                json={"roles": {"implement": {"agent": "claude"}}, "version": 0},
            )
            assert stale.status_code == 409
            assert stale.json()["detail"]["current_version"] == 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_roles_put_diversity_warning_non_blocking(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            created = await client.post("/api/config/bindings", json={"payload": _payload()})
            assert created.status_code == 201, created.text
            # Bindings default implement→claude; forcing the reviewer to claude
            # too loses cross-family diversity — a non-blocking warning.
            put = await client.put(
                "/api/config/roles",
                json={"roles": {"review_find": {"agent": "claude"}}, "version": 0},
            )
            assert put.status_code == 200, put.text
            assert any("diversity" in w for w in put.json()["warnings"])
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_effort_with_inherited_model_saves(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """An effort override with no explicit model saves — it is family-checked
    against the resolved role — as long as that role resolves a codex agent,
    which needs no per-model capability check (SYM-191)."""

    async def _caps(model: str, api_key: str | None = None) -> list[str] | None:
        return None

    monkeypatch.setattr("symphony.ui.config_crud.fetch_claude_effort_capabilities", _caps)
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            created = await client.post(
                "/api/config/bindings",
                json={
                    "payload": _payload(roles={"implement": {"agent": "codex", "effort": "high"}})
                },
            )
            assert created.status_code == 201, created.text
            assert created.json()["payload"]["roles"] == {
                "implement": {"agent": "codex", "effort": "high"}
            }
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_claude_effort_with_no_resolved_model_rejected(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """A claude role that resolves `effort` but no `model` (e.g. `implement:
    {agent: claude, effort: xhigh}` — the ordinary default, since claude
    builders pass no `--model`) fails at save: there is no model to check the
    effort against, and the CLI's own default model may not support it, so the
    mismatch would otherwise only surface at dispatch (SYM-191 review)."""

    async def _caps(model: str, api_key: str | None = None) -> list[str] | None:
        return None

    monkeypatch.setattr("symphony.ui.config_crud.fetch_claude_effort_capabilities", _caps)
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.post(
                "/api/config/bindings",
                json={
                    "payload": _payload(roles={"implement": {"agent": "claude", "effort": "xhigh"}})
                },
            )
            assert resp.status_code == 422, resp.text
            assert resp.json()["detail"][0]["loc"] == ["roles"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_options_claude_efforts_per_model(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    async def _caps(model: str, api_key: str | None = None) -> list[str] | None:
        return {"opus": ["low", "high"], "sonnet": ["medium"]}.get(model, ["low"])

    monkeypatch.setattr("symphony.ui.config_crud.fetch_claude_effort_capabilities", _caps)
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.get("/api/config/options")
        body = resp.json()
        assert body["claude_efforts_by_model"]["opus"] == ["low", "high"]
        assert body["claude_efforts_by_model"]["sonnet"] == ["medium"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_options_uses_dotenv_sourced_key(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """`ANTHROPIC_API_KEY` living only in `.env` (not `os.environ`) must still
    drive the options capability fetch, matching the save path's key
    resolution (`_env_key_source`) — otherwise the dropdown falls back to the
    family-wide set while save validates against the narrower per-model set
    (SYM-191 review)."""
    seen_keys: list[str | None] = []

    async def _caps(model: str, api_key: str | None = None) -> list[str] | None:
        seen_keys.append(api_key)
        return ["low", "medium"]

    monkeypatch.setattr("symphony.ui.config_crud.fetch_claude_effort_capabilities", _caps)
    monkeypatch.setattr(
        "symphony.ui.config_crud._env_key_source",
        lambda: {"ANTHROPIC_API_KEY": "dotenv-only-key"},
    )
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.get("/api/config/options")
        assert resp.status_code == 200
        assert seen_keys and all(key == "dotenv-only-key" for key in seen_keys)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_options_fetches_claude_efforts_concurrently(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """Per-alias capability fetches must run concurrently, not sequentially —
    a slow/unreachable Anthropic API should cost one wait, not one per alias
    (SYM-191 review)."""
    import asyncio

    concurrent = 0
    max_concurrent = 0

    async def _caps(model: str, api_key: str | None = None) -> list[str] | None:
        nonlocal concurrent, max_concurrent
        concurrent += 1
        max_concurrent = max(max_concurrent, concurrent)
        await asyncio.sleep(0.05)
        concurrent -= 1
        return ["low"]

    monkeypatch.setattr("symphony.ui.config_crud.fetch_claude_effort_capabilities", _caps)
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.get("/api/config/options")
        assert resp.status_code == 200
        assert max_concurrent > 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_options_claude_efforts_fall_back_without_key(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """No ANTHROPIC_API_KEY → the capability fetch returns None; the endpoint
    falls back to the family-wide effort set rather than an empty list."""

    async def _caps(model: str, api_key: str | None = None) -> list[str] | None:
        return None

    monkeypatch.setattr("symphony.ui.config_crud.fetch_claude_effort_capabilities", _caps)
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.get("/api/config/options")
        body = resp.json()
        assert set(body["claude_efforts_by_model"]["opus"]) == {
            "low",
            "medium",
            "high",
            "xhigh",
            "max",
        }
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_save_rejects_unsupported_claude_effort(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """A claude (model, effort) pair the live capability check rejects fails at
    save with a `roles` field path — not silently at dispatch."""

    async def _caps(model: str, api_key: str | None = None) -> list[str] | None:
        return ["low", "medium"]

    monkeypatch.setattr("symphony.ui.config_crud.fetch_claude_effort_capabilities", _caps)
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.post(
                "/api/config/bindings",
                json={
                    "payload": _payload(
                        roles={"implement": {"agent": "claude", "model": "opus", "effort": "xhigh"}}
                    )
                },
            )
            assert resp.status_code == 422, resp.text
            assert resp.json()["detail"][0]["loc"] == ["roles"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_stale_effort_on_unrelated_binding_does_not_block_save(
    tmp_path: Path,
    monkeypatch,  # type: ignore[no-untyped-def]
) -> None:
    """A now-unsupported claude (model, effort) on binding A (e.g. saved
    fail-open before ANTHROPIC_API_KEY was configured) must not fail a save on
    unrelated binding B: `_reject_unsupported_efforts` is scoped to the
    candidate for a single-binding save."""

    caps: list[str] = ["low", "medium", "high", "xhigh", "max"]

    async def _caps(model: str, api_key: str | None = None) -> list[str] | None:
        return caps

    monkeypatch.setattr("symphony.ui.config_crud.fetch_claude_effort_capabilities", _caps)
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            created_a = await client.post(
                "/api/config/bindings",
                json={
                    "payload": _payload(
                        project_key="ENG-A",
                        github_repo="org/repo-a",
                        roles={
                            "implement": {"agent": "claude", "model": "opus", "effort": "xhigh"}
                        },
                    )
                },
            )
            assert created_a.status_code == 201, created_a.text

            # Capability check now rejects binding A's stored effort — as if
            # the key/model capabilities changed after A was saved.
            caps[:] = ["low", "medium"]

            created_b = await client.post(
                "/api/config/bindings",
                json={
                    "payload": _payload(
                        project_key="ENG-B",
                        github_repo="org/repo-b",
                        roles={"implement": {"agent": "claude", "model": "opus", "effort": "low"}},
                    )
                },
            )
            assert created_b.status_code == 201, created_b.text
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_global_roles_put_not_blocked_by_unrelated_binding_override(
    tmp_path: Path,
    monkeypatch,  # type: ignore[no-untyped-def]
) -> None:
    """A binding pinning its own explicit `(model, effort)` for one role must
    not block a global roles PUT that doesn't touch that role: the pair is
    fully overridden, so it never resolves through the edited global cell
    (SYM-191 review)."""

    caps: list[str] = ["low", "medium", "high", "xhigh", "max"]

    async def _caps(model: str, api_key: str | None = None) -> list[str] | None:
        return caps

    monkeypatch.setattr("symphony.ui.config_crud.fetch_claude_effort_capabilities", _caps)
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            created = await client.post(
                "/api/config/bindings",
                json={
                    "payload": _payload(
                        roles={"implement": {"agent": "claude", "model": "opus", "effort": "xhigh"}}
                    )
                },
            )
            assert created.status_code == 201, created.text

            # Capability check now rejects the binding's stored `implement`
            # pair — as if the key/model capabilities changed after it saved.
            caps[:] = ["low", "medium"]

            # An unrelated global cell (`review_find`) must still save.
            put = await client.put(
                "/api/config/roles",
                json={"roles": {"review_find": {"agent": "claude"}}, "version": 0},
            )
            assert put.status_code == 200, put.text
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_only_inherited_rechecks_a_binding_role_with_unpinned_agent(
    monkeypatch,  # type: ignore[no-untyped-def]
) -> None:
    """`only_inherited=True` (the `put_roles` sweep) must not skip a binding
    role just because its own `model`/`effort` are pinned — that's only safe
    when `agent` is pinned too, since an unpinned `agent` still resolves
    through whichever global cell the write just changed. Exercises
    `_reject_unsupported_efforts` directly (not the full CRUD save path,
    which would also reject a genuine cross-family mismatch structurally
    before this check even runs) to isolate the online-capability skip logic
    itself (SYM-191 review)."""
    from symphony.config import LinearStates, RepoBinding, RoleConfig
    from symphony.ui import config_crud

    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")

    async def _caps(model: str, api_key: str | None = None) -> list[str] | None:
        # "xhigh" is structurally a valid claude effort but unsupported by
        # this (live-reported) model — only the online check catches it.
        return ["low", "medium", "high"]

    monkeypatch.setattr(config_crud, "fetch_claude_effort_capabilities", _caps)

    binding = RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        linear_states=LinearStates(ready="Todo"),
        roles={"implement": RoleConfig(model="opus", effort="xhigh")},
    )
    trial = Config(roles={"implement": RoleConfig(agent="claude")}, repos=[binding])

    with pytest.raises(config_crud.HTTPException) as exc_info:
        await config_crud._reject_unsupported_efforts(
            trial, bindings=[binding], only_inherited=True
        )
    assert exc_info.value.status_code == 422
    assert "xhigh" in str(exc_info.value.detail)


@pytest.mark.asyncio
async def test_reject_unsupported_efforts_honors_binding_env_alias_pin(
    monkeypatch,  # type: ignore[no-untyped-def]
) -> None:
    """A binding pinning `ANTHROPIC_DEFAULT_SONNET_MODEL` through its own
    (unresolved) `env:` mapping runs its claude subprocess against that pin
    (`{**os.environ, **spec.env}`), so the save-time capability recheck must
    resolve `sonnet` against the same pin for that binding — not the
    process-wide default — or it validates the wrong model (SYM-191 review)."""
    from symphony.config import LinearStates, RepoBinding, RoleConfig
    from symphony.ui import config_crud

    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setenv("PINNED_SONNET", "claude-sonnet-4-6")

    seen_models: list[str] = []

    async def _caps(model: str, api_key: str | None = None) -> list[str] | None:
        seen_models.append(model)
        # Only the pinned model supports "high" — the unpinned default
        # ("claude-sonnet-5") doesn't — so validating against the wrong model
        # would flip this test's outcome.
        return ["low", "medium", "high"] if model == "claude-sonnet-4-6" else ["low"]

    monkeypatch.setattr(config_crud, "fetch_claude_effort_capabilities", _caps)

    binding = RepoBinding(
        linear_team_key="ENG",
        github_repo="org/repo",
        linear_states=LinearStates(ready="Todo"),
        env={"ANTHROPIC_DEFAULT_SONNET_MODEL": "PINNED_SONNET"},
        roles={"implement": RoleConfig(agent="claude", model="sonnet", effort="high")},
    )
    trial = Config(repos=[binding])

    await config_crud._reject_unsupported_efforts(trial, bindings=[binding])
    assert seen_models == ["claude-sonnet-4-6"]


@pytest.mark.asyncio
async def test_save_fails_closed_on_capability_lookup_error(
    tmp_path: Path,
    monkeypatch,  # type: ignore[no-untyped-def]
) -> None:
    """A present `ANTHROPIC_API_KEY` that fails the Models-API capability
    lookup (auth error, network error, malformed response) must fail the save
    with a 422, not silently accept the effort as if no key were configured
    at all (SYM-191 review)."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "broken-key")

    async def _caps(model: str, api_key: str | None = None) -> list[str] | None:
        raise ValueError(f"could not reach the Models API to validate claude model {model!r}")

    monkeypatch.setattr("symphony.ui.config_crud.fetch_claude_effort_capabilities", _caps)
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.post(
                "/api/config/bindings",
                json={
                    "payload": _payload(
                        roles={"implement": {"agent": "claude", "model": "opus", "effort": "xhigh"}}
                    )
                },
            )
            assert resp.status_code == 422, resp.text
            assert resp.json()["detail"][0]["loc"] == ["roles"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_roles_put_rejects_when_bindings_change_during_validation(
    tmp_path: Path,
    monkeypatch,  # type: ignore[no-untyped-def]
) -> None:
    """A binding created while a roles PUT's (network-bound) capability check
    is in flight must not be silently missed: the in-lock recheck against a
    fresh binding listing fails the write with a 409 rather than committing
    against a stale binding snapshot (SYM-191 review)."""
    conn, db_path = await _open(tmp_path)
    original_list_all = config_bindings.list_all
    calls = {"n": 0}

    async def _list_all(c: Any) -> list[Any]:
        calls["n"] += 1
        if calls["n"] == 2:
            # Simulate a binding create landing between the pre-lock snapshot
            # and the in-lock recheck.
            await config_bindings.insert(
                c, payload=_payload(), key=("ENG", "org/repo", "", "linear", "default")
            )
        return await original_list_all(c)

    monkeypatch.setattr("symphony.ui.config_crud.config_bindings.list_all", _list_all)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            put = await client.put(
                "/api/config/roles",
                json={"roles": {"implement": {"agent": "codex"}}, "version": 0},
            )
            assert put.status_code == 409, put.text
            assert "bindings changed" in put.json()["detail"]["msg"]

            # Not persisted — a reread still shows the empty matrix.
            reread = await client.get("/api/config/roles")
            assert reread.json() == {"roles": {}, "version": 0}
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_binding_create_rejects_when_globals_change_during_validation(
    tmp_path: Path,
    monkeypatch,  # type: ignore[no-untyped-def]
) -> None:
    """A global roles PUT landing while a binding save's (network-bound)
    capability check is in flight must not be silently missed: the in-lock
    recheck against the current globals version fails the write with a 409
    rather than committing against a stale global-roles snapshot (SYM-191
    review)."""
    conn, db_path = await _open(tmp_path)
    original_get = db.config_globals.get
    calls = {"n": 0}

    async def _get(c: Any) -> Any:
        calls["n"] += 1
        if calls["n"] == 1:
            return None
        # Simulate a global roles PUT landing between the pre-lock validation
        # and the in-lock recheck.
        await db.config_globals.set_globals(c, roles={"implement": {"agent": "codex"}}, version=1)
        return await original_get(c)

    monkeypatch.setattr("symphony.ui.config_crud.config_globals.get", _get)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            resp = await client.post("/api/config/bindings", json={"payload": _payload()})
            assert resp.status_code == 409, resp.text
            assert "global roles matrix changed" in resp.json()["detail"]["msg"]

            # Not persisted — no binding was created.
            listed = await client.get("/api/config/bindings")
            assert listed.json() == []
    finally:
        await conn.close()


async def _create_binding(client: Any, **overrides: Any) -> dict[str, Any]:
    resp = await client.post("/api/config/bindings", json={"payload": _payload(**overrides)})
    assert resp.status_code == 201, resp.text
    return resp.json()


async def _seed_issue(conn: Any, *, ident: str = "ENG-1") -> str:
    return await db.issues.upsert(
        conn,
        id=f"issue-{ident}",
        provider="linear",
        site="default",
        identifier=ident,
        title="t",
        team_key="ENG",
    )


@pytest.mark.asyncio
async def test_drain_guard_blocks_delete_on_running_run(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            rec = await _create_binding(client)
            issue_id = await _seed_issue(conn)
            await db.runs.create(
                conn,
                id="run-1",
                issue_id=issue_id,
                stage="implement",
                status="running",
                pid=None,
                started_at="2026-01-01T00:00:00Z",
                binding_key=_binding_key_str(rec),
            )
            deleted = await client.delete(
                f"/api/config/bindings/{rec['id']}?version={rec['version']}"
            )
            assert deleted.status_code == 409, deleted.text
            assert deleted.json()["detail"]["blockers"]["running_runs"] == ["ENG-1"]

            # The card list flags active work.
            listed = await client.get("/api/config/bindings")
            assert listed.json()[0]["active_work"] is True
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_drain_guard_blocks_delete_on_open_pr(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            rec = await _create_binding(client)
            issue_id = await _seed_issue(conn)
            await db.issue_prs.upsert(
                conn,
                issue_id=issue_id,
                github_repo="org/repo",
                binding_key=_binding_key_str(rec),
                pr_number=7,
                pr_url="https://github.com/org/repo/pull/7",
                created_at="2026-01-01T00:00:00Z",
            )
            deleted = await client.delete(
                f"/api/config/bindings/{rec['id']}?version={rec['version']}"
            )
            assert deleted.status_code == 409, deleted.text
            assert deleted.json()["detail"]["blockers"]["open_prs"] == ["ENG-1"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_drain_guard_blocks_delete_on_legacy_running_run(tmp_path: Path) -> None:
    """A run still live from before the `binding_key` column existed is
    stamped at the migration's `''` default — the drain guard must still
    attribute it (by team + repo) rather than let the delete through
    (SYM-193 review)."""
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            rec = await _create_binding(client)
            issue_id = await _seed_issue(conn)
            await db.runs.create(
                conn,
                id="run-1",
                issue_id=issue_id,
                stage="implement",
                status="running",
                pid=None,
                started_at="2026-01-01T00:00:00Z",
            )
            deleted = await client.delete(
                f"/api/config/bindings/{rec['id']}?version={rec['version']}"
            )
            assert deleted.status_code == 409, deleted.text
            assert deleted.json()["detail"]["blockers"]["running_runs"] == ["ENG-1"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_drain_guard_blocks_delete_on_legacy_open_pr(tmp_path: Path) -> None:
    """An unmerged PR opened before the `binding_key` column existed is
    stamped at the migration's `''` default — the drain guard must still
    attribute it (by team + repo) rather than let the delete through
    (SYM-193 review)."""
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            rec = await _create_binding(client)
            issue_id = await _seed_issue(conn)
            await db.issue_prs.upsert(
                conn,
                issue_id=issue_id,
                github_repo="org/repo",
                pr_number=7,
                pr_url="https://github.com/org/repo/pull/7",
                created_at="2026-01-01T00:00:00Z",
            )
            deleted = await client.delete(
                f"/api/config/bindings/{rec['id']}?version={rec['version']}"
            )
            assert deleted.status_code == 409, deleted.text
            assert deleted.json()["detail"]["blockers"]["open_prs"] == ["ENG-1"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_drain_guard_blocks_delete_on_operator_wait(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            rec = await _create_binding(client)
            issue_id = await _seed_issue(conn)
            # A parked wait references a (completed) run — the wait, not the
            # run, is the blocker here.
            await db.runs.create(
                conn,
                id="run-1",
                issue_id=issue_id,
                stage="implement",
                status="failed",
                pid=None,
                started_at="2026-01-01T00:00:00Z",
            )
            await db.operator_waits.upsert(
                conn,
                issue_id=issue_id,
                run_id="run-1",
                kind=db.operator_waits.KIND_IMPLEMENT_FAILED,
                linear_team_key="ENG",
                github_repo="org/repo",
                issue_label="",
                created_at="2026-01-01T00:00:00Z",
            )
            deleted = await client.delete(
                f"/api/config/bindings/{rec['id']}?version={rec['version']}"
            )
            assert deleted.status_code == 409, deleted.text
            assert deleted.json()["detail"]["blockers"]["operator_waits"] == ["ENG-1"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_drain_guard_blocks_delete_on_scheduled_slot(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        # The daemon reserves an in-memory scheduled slot before any run row
        # exists; the drain guard must see it via the injected provider.
        app = _drain_app(conn, scheduled_slots=lambda key: 1)
        async with _client(app) as client:
            rec = await _create_binding(client)
            deleted = await client.delete(
                f"/api/config/bindings/{rec['id']}?version={rec['version']}"
            )
            assert deleted.status_code == 409, deleted.text
            assert deleted.json()["detail"]["blockers"]["scheduled_slots"] == 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_drain_guard_blocks_rename_and_branch_edit(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            rec = await _create_binding(client)
            issue_id = await _seed_issue(conn)
            await db.runs.create(
                conn,
                id="run-1",
                issue_id=issue_id,
                stage="implement",
                status="running",
                pid=None,
                started_at="2026-01-01T00:00:00Z",
                binding_key=_binding_key_str(rec),
            )
            # A natural-key change (github_repo) is guarded like a delete.
            renamed = await client.put(
                f"/api/config/bindings/{rec['id']}",
                json={
                    "payload": _payload(github_repo="org/other"),
                    "version": rec["version"],
                },
            )
            assert renamed.status_code == 409, renamed.text
            assert renamed.json()["detail"]["blockers"]["running_runs"] == ["ENG-1"]

            # A branch-affecting edit (branch_prefix) is guarded identically.
            rebranch = await client.put(
                f"/api/config/bindings/{rec['id']}",
                json={
                    "payload": _payload(branch_prefix="feature"),
                    "version": rec["version"],
                },
            )
            assert rebranch.status_code == 409, rebranch.text

            # An ordinary edit (max_concurrent) is NOT drain-guarded.
            tweaked = await client.put(
                f"/api/config/bindings/{rec['id']}",
                json={
                    "payload": _payload(max_concurrent=9),
                    "version": rec["version"],
                },
            )
            assert tweaked.status_code == 200, tweaked.text
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_drained_binding_deletes_cleanly(tmp_path: Path) -> None:
    conn, db_path = await _open(tmp_path)
    try:
        app = _app(conn, db_path)
        async with _client(app) as client:
            rec = await _create_binding(client)
            issue_id = await _seed_issue(conn)
            # A *completed* run does not block (only live runs do).
            await db.runs.create(
                conn,
                id="run-done",
                issue_id=issue_id,
                stage="implement",
                status="completed",
                pid=None,
                started_at="2026-01-01T00:00:00Z",
                binding_key=_binding_key_str(rec),
            )
            listed = await client.get("/api/config/bindings")
            assert listed.json()[0]["active_work"] is False
            deleted = await client.delete(
                f"/api/config/bindings/{rec['id']}?version={rec['version']}"
            )
            assert deleted.status_code == 204, deleted.text
            assert (await client.get("/api/config/bindings")).json() == []
    finally:
        await conn.close()
