"""Preflight CLI tests — uses a fake Linear so no network is required."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import httpx
import pytest
from click.testing import CliRunner

from symphony import db
from symphony.agent.claude_models import fetch_claude_effort_capabilities
from symphony.cli import main


class _FakeLinear:
    def __init__(self, viewer_keys: list[str], states: dict[str, dict[str, str]]) -> None:
        self._viewer_keys = viewer_keys
        self._states = states

    async def __aenter__(self) -> _FakeLinear:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        return None

    async def viewer_team_keys(self) -> list[str]:
        return self._viewer_keys

    async def team_states(self, key: str) -> dict[str, str]:
        return self._states.get(key, {})


def _install_fake(monkeypatch, fake: _FakeLinear) -> None:  # type: ignore[no-untyped-def]
    def _factory(_api_key: str) -> _FakeLinear:
        return fake

    def _for_binding(binding, _secrets, *, registry=None):  # type: ignore[no-untyped-def]
        if registry is not None:
            registry.register(binding.tracker_provider, binding.tracker_site, fake)
        return fake

    monkeypatch.setattr("symphony.cli.Linear", _factory)
    monkeypatch.setattr("symphony.cli.for_binding", _for_binding)


def _isolate_codex_home(tmp_path: Path, monkeypatch) -> Path:  # type: ignore[no-untyped-def]
    codex_home = tmp_path / "codex-home"
    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    return codex_home


_STD_STATES = {
    "Todo": "id1",
    "In Progress": "id2",
    "Needs Approval": "id3",
    "Blocked": "id4",
    "Done": "id5",
}

_READY_STATES = {
    "ready": "Todo",
    "in_progress": "In Progress",
    "code_review": "Needs Approval",
    "needs_approval": "Needs Approval",
    "blocked": "Blocked",
    "done": "Done",
}


def _seed_preflight_db(tmp_path: Path, monkeypatch, payloads: list[dict[str, Any]]) -> None:  # type: ignore[no-untyped-def]
    """Seed `config_bindings` into a tmp DB and point the env-driven config at
    it — preflight boots its topology from SQLite now (Config v2 9/9)."""
    monkeypatch.setenv("SYMPHONY_DB_PATH", str(tmp_path / "state.sqlite"))
    monkeypatch.setenv("SYMPHONY_LOG_ROOT", str(tmp_path / "logs"))
    monkeypatch.setenv("SYMPHONY_WORKSPACE_ROOT", str(tmp_path / "workspaces"))

    async def _seed() -> None:
        conn = await db.connect(tmp_path / "state.sqlite")
        try:
            for i, payload in enumerate(payloads):
                project = payload.get("project_key") or payload["linear_team_key"]
                key = (
                    project,
                    payload["github_repo"],
                    payload.get("issue_label") or "",
                    "linear",
                    "default",
                )
                await db.config_bindings.insert(conn, payload=payload, key=key, priority=i)
        finally:
            await conn.close()

    asyncio.run(_seed())


def _ready_binding(ready: str = "Todo", *, waiting: str | None = None) -> dict[str, Any]:
    states = dict(_READY_STATES)
    states["ready"] = ready
    if waiting is not None:
        states["waiting"] = waiting
    return {"linear_team_key": "ENG", "github_repo": "org/api-svc", "linear_states": states}


def _role_effort_binding(effort: str, *, agent: str = "claude", model: str) -> dict[str, Any]:
    role: dict[str, str] = {"model": model, "effort": effort}
    if agent == "codex":
        role["agent"] = "codex"
    return {
        "linear_team_key": "ENG",
        "github_repo": "org/api-svc",
        "roles": {"implement": role},
        "linear_states": dict(_READY_STATES),
    }


def _sonnet_high_binding(
    team: str, repo: str, *, env: dict[str, str] | None = None
) -> dict[str, Any]:
    """A claude binding whose `implement` role pins sonnet + effort high — the
    shape the capability-validation tests exercise. `env:` maps a key name the
    binding resolves from the process/.env (e.g. ANTHROPIC_API_KEY)."""
    payload: dict[str, Any] = {
        "linear_team_key": team,
        "github_repo": repo,
        "roles": {"implement": {"model": "sonnet", "effort": "high"}},
        "linear_states": dict(_READY_STATES),
    }
    if env is not None:
        payload["env"] = env
    return payload


def _review_lane_binding(
    *,
    local_review: bool,
    remote_review: bool,
    code_review: str | None = "In Review",
    local_code_review: str | None = "Local Code Review",
) -> dict[str, Any]:
    states: dict[str, str] = {
        "ready": "Todo",
        "in_progress": "In Progress",
        "needs_approval": "Manual Approval",
        "blocked": "Blocked",
        "done": "Done",
    }
    if code_review is not None:
        states["code_review"] = code_review
    if local_code_review is not None:
        states["local_code_review"] = local_code_review
    return {
        "linear_team_key": "ENG",
        "github_repo": "org/api-svc",
        "local_review": local_review,
        "remote_review": remote_review,
        "linear_states": states,
    }


def _install_mock_transport(monkeypatch, handler) -> None:  # type: ignore[no-untyped-def]
    """Make `fetch_claude_effort_capabilities` route through `handler` so the
    real request/`raise_for_status` path is exercised without the network."""
    import symphony.agent.claude_models as cm

    real_client = cm.httpx.AsyncClient

    def _client(*args, **kwargs):  # type: ignore[no-untyped-def]
        kwargs["transport"] = httpx.MockTransport(handler)
        return real_client(*args, **kwargs)

    monkeypatch.setattr(cm.httpx, "AsyncClient", _client)


async def test_fetch_claude_effort_capabilities_parses_effort_tree(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")

    def _handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["x-api-key"] == "sk-test"
        return httpx.Response(
            200,
            json={"capabilities": {"effort": {"low": {}, "medium": {}, "high": {}}}},
        )

    _install_mock_transport(monkeypatch, _handler)
    assert await fetch_claude_effort_capabilities("sonnet") == [
        "low",
        "medium",
        "high",
    ]


@pytest.mark.parametrize(
    "payload",
    [{}, {"capabilities": {}}, {"capabilities": {"effort": {}}}],
)
async def test_fetch_claude_effort_capabilities_empty_tree_raises_valueerror(
    monkeypatch, payload
) -> None:  # type: ignore[no-untyped-def]
    """An absent/empty effort tree must raise "cannot validate" — NOT return
    `[]`, which the caller would read as "supports zero efforts" and use to
    falsely reject a structurally valid pair."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    _install_mock_transport(monkeypatch, _handler)
    with pytest.raises(ValueError, match="no effort capabilities"):
        await fetch_claude_effort_capabilities("sonnet")


async def test_fetch_claude_effort_capabilities_missing_key_returns_none(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    """No ANTHROPIC_API_KEY → return None (a skip signal), NOT a raise: the
    containerized/OAuth deployment has no API key and must still pass preflight."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    assert await fetch_claude_effort_capabilities("sonnet") is None


@pytest.mark.parametrize(
    ("alias", "expected_model_id"),
    [
        ("opus", "claude-opus-4-8"),
        ("sonnet", "claude-sonnet-5"),
        ("haiku", "claude-haiku-4-5-20251001"),
    ],
)
async def test_fetch_claude_effort_capabilities_resolves_cli_alias(
    monkeypatch, alias, expected_model_id
) -> None:  # type: ignore[no-untyped-def]
    """The Models API has no `opus`/`sonnet`/`haiku` alias — only the `claude`
    CLI does — so the short alias must be mapped to a full model ID before the
    request goes out, or `/v1/models/<alias>` 404s (SYM-191 review)."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    seen_paths = []

    def _handler(request: httpx.Request) -> httpx.Response:
        seen_paths.append(request.url.path)
        return httpx.Response(200, json={"capabilities": {"effort": {"low": {}}}})

    _install_mock_transport(monkeypatch, _handler)
    await fetch_claude_effort_capabilities(alias)
    assert seen_paths == [f"/v1/models/{expected_model_id}"]


async def test_fetch_claude_effort_capabilities_alias_env_override(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    """An org's Claude Code model allowlist can pin `sonnet` to an older ID
    than the one hard-coded here; `SYMPHONY_CLAUDE_ALIAS_SONNET` lets an
    operator tell us the real pinned ID instead of the guess (SYM-191 review)."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setenv("SYMPHONY_CLAUDE_ALIAS_SONNET", "claude-sonnet-4-6")
    seen_paths = []

    def _handler(request: httpx.Request) -> httpx.Response:
        seen_paths.append(request.url.path)
        return httpx.Response(200, json={"capabilities": {"effort": {"low": {}}}})

    _install_mock_transport(monkeypatch, _handler)
    await fetch_claude_effort_capabilities("sonnet")
    assert seen_paths == ["/v1/models/claude-sonnet-4-6"]


async def test_fetch_claude_effort_capabilities_honors_anthropic_default_model_env(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    """Claude Code's own `ANTHROPIC_DEFAULT_SONNET_MODEL` pins what `sonnet`
    resolves to for the CLI itself; when a deployment sets it, that IS the
    real pinned ID — honor it over our hard-coded guess (SYM-191 review)."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setenv("ANTHROPIC_DEFAULT_SONNET_MODEL", "claude-sonnet-4-6")
    seen_paths = []

    def _handler(request: httpx.Request) -> httpx.Response:
        seen_paths.append(request.url.path)
        return httpx.Response(200, json={"capabilities": {"effort": {"low": {}}}})

    _install_mock_transport(monkeypatch, _handler)
    await fetch_claude_effort_capabilities("sonnet")
    assert seen_paths == ["/v1/models/claude-sonnet-4-6"]


async def test_fetch_claude_effort_capabilities_strips_1m_context_suffix(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    """Claude Code's documented 1M-context pinning syntax
    (`ANTHROPIC_DEFAULT_OPUS_MODEL='claude-opus-4-8[1m]'`) is stripped by the
    CLI itself before use; the Models API never recognizes the bracket suffix
    either (`/v1/models/claude-opus-4-8[1m]` 404s), so the pinned value must
    be stripped here too before the capability lookup (SYM-191 review)."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setenv("ANTHROPIC_DEFAULT_OPUS_MODEL", "claude-opus-4-8[1m]")
    seen_paths = []

    def _handler(request: httpx.Request) -> httpx.Response:
        seen_paths.append(request.url.path)
        return httpx.Response(200, json={"capabilities": {"effort": {"low": {}}}})

    _install_mock_transport(monkeypatch, _handler)
    await fetch_claude_effort_capabilities("opus")
    assert seen_paths == ["/v1/models/claude-opus-4-8"]


async def test_fetch_claude_effort_capabilities_symphony_override_wins_over_anthropic_default(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    """`SYMPHONY_CLAUDE_ALIAS_SONNET` is the more explicit, check-specific
    override — it should win when both it and Claude Code's own
    `ANTHROPIC_DEFAULT_SONNET_MODEL` are set (SYM-191 review)."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setenv("ANTHROPIC_DEFAULT_SONNET_MODEL", "claude-sonnet-4-6")
    monkeypatch.setenv("SYMPHONY_CLAUDE_ALIAS_SONNET", "claude-sonnet-explicit")
    seen_paths = []

    def _handler(request: httpx.Request) -> httpx.Response:
        seen_paths.append(request.url.path)
        return httpx.Response(200, json={"capabilities": {"effort": {"low": {}}}})

    _install_mock_transport(monkeypatch, _handler)
    await fetch_claude_effort_capabilities("sonnet")
    assert seen_paths == ["/v1/models/claude-sonnet-explicit"]


async def test_fetch_claude_effort_capabilities_full_model_id_passes_through(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    seen_paths = []

    def _handler(request: httpx.Request) -> httpx.Response:
        seen_paths.append(request.url.path)
        return httpx.Response(200, json={"capabilities": {"effort": {"low": {}}}})

    _install_mock_transport(monkeypatch, _handler)
    await fetch_claude_effort_capabilities("claude-sonnet-4-6")
    assert seen_paths == ["/v1/models/claude-sonnet-4-6"]


async def test_fetch_claude_effort_capabilities_filters_unsupported_levels(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    """A level whose value is `null` or carries `supported: false` must be
    dropped — its mere presence as a key in the tree previously passed the
    caller's plain membership check even though the model rejects it
    (SYM-191 review)."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "capabilities": {
                    "effort": {
                        "low": {},
                        "medium": {"supported": True},
                        "high": {"supported": False},
                        "xhigh": None,
                    }
                }
            },
        )

    _install_mock_transport(monkeypatch, _handler)
    assert await fetch_claude_effort_capabilities("sonnet") == ["low", "medium"]


async def test_fetch_claude_effort_capabilities_ignores_sibling_metadata_keys(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    """The Models API's `effort` tree carries a sibling `supported` boolean
    alongside the per-level entries, not just per-level flags. That key must
    not be returned as if it were an effort level (SYM-191 review)."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "capabilities": {
                    "effort": {
                        "low": {},
                        "medium": {},
                        "high": {},
                        "supported": True,
                    }
                }
            },
        )

    _install_mock_transport(monkeypatch, _handler)
    assert await fetch_claude_effort_capabilities("sonnet") == ["low", "medium", "high"]


async def test_fetch_claude_effort_capabilities_http_error_raises_valueerror(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    """A 401 from an invalid key surfaces as a clean ValueError, not a raw
    httpx.HTTPStatusError that would escape preflight's `except ValueError`."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "bad")

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"error": "auth"})

    _install_mock_transport(monkeypatch, _handler)
    with pytest.raises(ValueError, match="HTTP 401"):
        await fetch_claude_effort_capabilities("sonnet")


def test_preflight_exits_cleanly_when_fetcher_http_errors(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """An httpx error from the fetcher must not escape as a traceback — preflight
    exits 2 with a message via the re-raised ValueError."""
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    _isolate_codex_home(tmp_path, monkeypatch)
    _install_fake(monkeypatch, _FakeLinear(viewer_keys=["ENG"], states={"ENG": _STD_STATES}))

    async def _raise(_model: str, _api_key: str | None = None) -> list[str]:
        raise ValueError("Models API returned HTTP 401 for claude model 'sonnet'")

    monkeypatch.setattr("symphony.cli.fetch_claude_effort_capabilities", _raise)
    _seed_preflight_db(tmp_path, monkeypatch, [_role_effort_binding("high", model="sonnet")])
    result = CliRunner().invoke(main, ["preflight"])
    assert result.exit_code == 2
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert "HTTP 401" in result.output


def _fake_claude_caps(monkeypatch, supported: list[str]) -> None:  # type: ignore[no-untyped-def]
    async def _fetch(_model: str, _api_key: str | None = None) -> list[str]:
        return list(supported)

    monkeypatch.setattr("symphony.cli.fetch_claude_effort_capabilities", _fetch)


def test_preflight_accepts_supported_model_effort_pair(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    _isolate_codex_home(tmp_path, monkeypatch)
    _install_fake(monkeypatch, _FakeLinear(viewer_keys=["ENG"], states={"ENG": _STD_STATES}))
    _fake_claude_caps(monkeypatch, ["low", "medium", "high", "max"])
    _seed_preflight_db(tmp_path, monkeypatch, [_role_effort_binding("high", model="sonnet")])
    result = CliRunner().invoke(main, ["preflight"])
    assert result.exit_code == 0, result.output
    assert "claude model 'sonnet' supports effort 'high'" in result.output


def test_preflight_skips_claude_check_when_api_key_missing(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """Fetcher returns None (no ANTHROPIC_API_KEY) → the claude effort check is
    skipped with a warning and preflight still exits 0. This is the containerized
    OAuth deployment: claude runs via CLI auth, no API key present."""
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    _isolate_codex_home(tmp_path, monkeypatch)
    _install_fake(monkeypatch, _FakeLinear(viewer_keys=["ENG"], states={"ENG": _STD_STATES}))

    async def _no_key(_model: str, _api_key: str | None = None) -> None:
        return None

    monkeypatch.setattr("symphony.cli.fetch_claude_effort_capabilities", _no_key)
    _seed_preflight_db(tmp_path, monkeypatch, [_role_effort_binding("high", model="sonnet")])
    result = CliRunner().invoke(main, ["preflight"])
    assert result.exit_code == 0, result.output
    assert "skipping claude model 'sonnet' effort validation" in result.output
    assert "ANTHROPIC_API_KEY not set" in result.output


def test_preflight_validates_with_binding_supplied_api_key(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """A key supplied only through a binding's `env:` mapping (not the process
    env) must still drive validation — not fall into the no-key skip. Otherwise
    an API-key deployment could pass preflight on an unsupported effort."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    monkeypatch.setenv("MY_ANTHROPIC_KEY", "sk-from-binding")
    _isolate_codex_home(tmp_path, monkeypatch)
    _install_fake(monkeypatch, _FakeLinear(viewer_keys=["ENG"], states={"ENG": _STD_STATES}))

    seen_keys: list[str | None] = []

    async def _fetch(_model: str, api_key: str | None = None) -> list[str]:
        seen_keys.append(api_key)
        return ["low", "medium", "high"]

    monkeypatch.setattr("symphony.cli.fetch_claude_effort_capabilities", _fetch)
    _seed_preflight_db(
        tmp_path,
        monkeypatch,
        [_sonnet_high_binding("ENG", "org/api-svc", env={"ANTHROPIC_API_KEY": "MY_ANTHROPIC_KEY"})],
    )
    result = CliRunner().invoke(main, ["preflight"])
    assert result.exit_code == 0, result.output
    assert "claude model 'sonnet' supports effort 'high'" in result.output
    assert "skipping" not in result.output
    # The binding-resolved secret — not an empty string — reached the fetcher.
    assert seen_keys == ["sk-from-binding"]


def test_preflight_exercises_each_binding_api_key(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """Two claude bindings with different `env:` keys for the same model: each
    distinct key is validated, so a present-but-broken key on one binding fails
    preflight instead of hiding behind the other binding's valid key."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    monkeypatch.setenv("KEY_A", "sk-a")
    monkeypatch.setenv("KEY_B", "sk-b")
    _isolate_codex_home(tmp_path, monkeypatch)
    _install_fake(
        monkeypatch,
        _FakeLinear(viewer_keys=["ENG", "OPS"], states={"ENG": _STD_STATES, "OPS": _STD_STATES}),
    )

    seen_keys: list[str | None] = []

    async def _fetch(_model: str, api_key: str | None = None) -> list[str]:
        seen_keys.append(api_key)
        if api_key == "sk-b":  # binding B's key is expired/invalid
            raise ValueError("Models API returned HTTP 401 for claude model 'sonnet'")
        return ["low", "medium", "high"]

    monkeypatch.setattr("symphony.cli.fetch_claude_effort_capabilities", _fetch)
    _seed_preflight_db(
        tmp_path,
        monkeypatch,
        [
            _sonnet_high_binding("ENG", "org/api-svc", env={"ANTHROPIC_API_KEY": "KEY_A"}),
            _sonnet_high_binding("OPS", "org/ops-svc", env={"ANTHROPIC_API_KEY": "KEY_B"}),
        ],
    )
    result = CliRunner().invoke(main, ["preflight"])
    assert result.exit_code == 2, result.output
    assert "HTTP 401" in result.output
    # Both distinct binding keys were exercised (not just the first).
    assert set(seen_keys) == {"sk-a", "sk-b"}


def test_preflight_honors_binding_specific_claude_alias_pin(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """A binding pinning `ANTHROPIC_DEFAULT_SONNET_MODEL` via its own `env:`
    mapping runs its claude subprocess against that pin
    (`{**os.environ, **spec.env}`), so the capability check must resolve
    `sonnet` against the SAME pin for that binding — not the process-wide
    default — or it validates the wrong model (SYM-191 review). A sibling
    binding with no pin must still resolve against the ordinary default,
    proving each binding's own resolution isn't cached over the other's."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    monkeypatch.setenv("PINNED_SONNET", "claude-sonnet-4-6")
    _isolate_codex_home(tmp_path, monkeypatch)
    _install_fake(
        monkeypatch,
        _FakeLinear(viewer_keys=["ENG", "OPS"], states={"ENG": _STD_STATES, "OPS": _STD_STATES}),
    )

    seen_models: list[str] = []

    async def _fetch(model: str, _api_key: str | None = None) -> list[str]:
        seen_models.append(model)
        # Only the pinned model supports "high" — the unpinned default
        # doesn't — so a binding validated against the wrong model would flip
        # this test's pass/fail outcome for that binding.
        return ["low", "medium", "high"] if model == "claude-sonnet-4-6" else ["low"]

    monkeypatch.setattr("symphony.cli.fetch_claude_effort_capabilities", _fetch)
    _seed_preflight_db(
        tmp_path,
        monkeypatch,
        [
            _sonnet_high_binding(
                "ENG", "org/api-svc", env={"ANTHROPIC_DEFAULT_SONNET_MODEL": "PINNED_SONNET"}
            ),
            _sonnet_high_binding("OPS", "org/ops-svc"),
        ],
    )
    result = CliRunner().invoke(main, ["preflight"])
    # Both bindings' distinct resolved models were exercised — not collapsed
    # into one cache entry keyed on the bare "sonnet" alias.
    assert set(seen_models) == {"claude-sonnet-4-6", "claude-sonnet-5"}
    assert "claude model 'sonnet' supports effort 'high'" in result.output
    assert "effort 'high' not supported by claude model 'sonnet'; supported: low" in result.output
    assert result.exit_code != 0, result.output


def test_preflight_empty_binding_key_override_is_not_masked_by_parent(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    """A binding that sets `env: ANTHROPIC_API_KEY` to an empty value overrides
    the parent key for its subprocess ({**os.environ, **spec.env}), so preflight
    must validate with "" (→ skip), not fall back to the parent's valid key."""
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-parent")
    monkeypatch.setenv("EMPTY_SECRET", "")
    _isolate_codex_home(tmp_path, monkeypatch)
    _install_fake(monkeypatch, _FakeLinear(viewer_keys=["ENG"], states={"ENG": _STD_STATES}))

    seen_keys: list[str | None] = []

    async def _fetch(_model: str, api_key: str | None = None) -> list[str] | None:
        seen_keys.append(api_key)
        return None if not api_key else ["low", "medium", "high"]

    monkeypatch.setattr("symphony.cli.fetch_claude_effort_capabilities", _fetch)
    _seed_preflight_db(
        tmp_path,
        monkeypatch,
        [_sonnet_high_binding("ENG", "org/api-svc", env={"ANTHROPIC_API_KEY": "EMPTY_SECRET"})],
    )
    result = CliRunner().invoke(main, ["preflight"])
    assert result.exit_code == 0, result.output
    assert "skipping claude model 'sonnet' effort validation" in result.output
    # Validated with the binding's empty override, never the parent key.
    assert seen_keys == [""]
    assert "sk-parent" not in seen_keys


def test_preflight_rejects_unsupported_model_effort_pair(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    _isolate_codex_home(tmp_path, monkeypatch)
    _install_fake(monkeypatch, _FakeLinear(viewer_keys=["ENG"], states={"ENG": _STD_STATES}))
    _fake_claude_caps(monkeypatch, ["low", "medium", "high", "max"])
    _seed_preflight_db(tmp_path, monkeypatch, [_role_effort_binding("xhigh", model="sonnet")])
    result = CliRunner().invoke(main, ["preflight"])
    assert result.exit_code != 0
    assert (
        "effort 'xhigh' not supported by claude model 'sonnet'; "
        "supported: low, medium, high, max" in result.output
    )


def test_preflight_checks_codex_pair_via_family_enum(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """Codex (model, effort) pairs are checked against the fixed family enum,
    not the Models API — the claude fetcher is never called."""
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    _isolate_codex_home(tmp_path, monkeypatch)
    _install_fake(monkeypatch, _FakeLinear(viewer_keys=["ENG"], states={"ENG": _STD_STATES}))

    async def _boom(_model: str, _api_key: str | None = None) -> list[str]:
        raise AssertionError("claude Models API must not be queried for codex")

    monkeypatch.setattr("symphony.cli.fetch_claude_effort_capabilities", _boom)
    _seed_preflight_db(
        tmp_path,
        monkeypatch,
        [_role_effort_binding("high", agent="codex", model="gpt-5.1-codex")],
    )
    result = CliRunner().invoke(main, ["preflight"])
    assert result.exit_code == 0, result.output
    assert "codex model 'gpt-5.1-codex' supports effort 'high'" in result.output


def test_preflight_skips_codex_profile_when_bindings_do_not_use_codex(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    codex_home = _isolate_codex_home(tmp_path, monkeypatch)
    fake = _FakeLinear(
        viewer_keys=["ENG"],
        states={
            "ENG": {
                "Todo": "id1",
                "In Progress": "id2",
                "Needs Approval": "id3",
                "Blocked": "id4",
                "Done": "id5",
            }
        },
    )
    _install_fake(monkeypatch, fake)
    _seed_preflight_db(tmp_path, monkeypatch, [_ready_binding("Todo")])
    result = CliRunner().invoke(main, ["preflight"])
    assert result.exit_code == 0, result.output
    assert not (codex_home / "config.toml").exists()
    assert "codex CLI not used by configured repos" in result.output


def test_preflight_allows_jira_binding_without_linear_key(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.delenv("LINEAR_API_KEY", raising=False)
    monkeypatch.setenv("JIRA_BASE_URL", "https://jira.example.test")
    monkeypatch.setenv("JIRA_EMAIL", "bot@example.test")
    monkeypatch.setenv("JIRA_API_TOKEN", "jira-token")
    _isolate_codex_home(tmp_path, monkeypatch)
    _seed_preflight_db(
        tmp_path,
        monkeypatch,
        [
            {
                "provider": "jira",
                "project_key": "SYM",
                "base_url": "https://jira.example.test",
                "github_repo": "org/api-svc",
                "states": {"ready": "To Do", "code_review": "In Review"},
            }
        ],
    )

    result = CliRunner().invoke(main, ["preflight"])

    assert result.exit_code == 0, result.output
    assert "jira projects visible to this key: ['SYM']" in result.output
    assert "SYM → org/api-svc: states ok" in result.output


def test_preflight_notes_codex_bypass_when_binding_uses_codex_agent(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    codex_home = _isolate_codex_home(tmp_path, monkeypatch)
    fake = _FakeLinear(
        viewer_keys=["ENG"],
        states={
            "ENG": {
                "Todo": "id1",
                "In Progress": "id2",
                "Needs Approval": "id3",
                "Blocked": "id4",
                "Done": "id5",
            }
        },
    )
    _install_fake(monkeypatch, fake)
    _seed_preflight_db(
        tmp_path,
        monkeypatch,
        [{**_ready_binding("Todo"), "roles": {"implement": {"agent": "codex"}}}],
    )
    result = CliRunner().invoke(main, ["preflight"])
    assert result.exit_code == 0, result.output
    # codex runs bypass its OS sandbox; no permissions profile is provisioned.
    assert not (codex_home / "config.toml").exists()
    assert "--dangerously-bypass-approvals-and-sandbox" in result.output


def test_preflight_notes_codex_bypass_when_local_reviewer_uses_codex(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    codex_home = _isolate_codex_home(tmp_path, monkeypatch)
    fake = _FakeLinear(
        viewer_keys=["ENG"],
        states={
            "ENG": {
                "Todo": "id1",
                "In Progress": "id2",
                "Local Code Review": "id-local",
                "Needs Approval": "id3",
                "Blocked": "id4",
                "Done": "id5",
            }
        },
    )
    _install_fake(monkeypatch, fake)
    _seed_preflight_db(tmp_path, monkeypatch, [{**_ready_binding("Todo"), "local_review": True}])
    result = CliRunner().invoke(main, ["preflight"])
    assert result.exit_code == 0, result.output
    # codex runs bypass its OS sandbox; no permissions profile is provisioned.
    assert not (codex_home / "config.toml").exists()
    assert "--dangerously-bypass-approvals-and-sandbox" in result.output


def test_preflight_fails_when_ready_not_in_team_states(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """If the binding's `ready` name is not in the team's workflow, fail loudly."""
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    _isolate_codex_home(tmp_path, monkeypatch)
    fake = _FakeLinear(
        viewer_keys=["ENG"],
        states={
            "ENG": {
                "Todo": "id1",
                "In Progress": "id2",
                "Needs Approval": "id3",
                "Blocked": "id4",
                "Done": "id5",
            }
        },
    )
    _install_fake(monkeypatch, fake)
    # Binding asks for a "Backlog" ready state that the team's workflow lacks.
    _seed_preflight_db(tmp_path, monkeypatch, [_ready_binding("Backlog")])
    result = CliRunner().invoke(main, ["preflight"])
    assert result.exit_code != 0
    assert "Backlog" in result.output


def test_preflight_fails_when_waiting_not_in_team_states(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    _isolate_codex_home(tmp_path, monkeypatch)
    fake = _FakeLinear(
        viewer_keys=["ENG"],
        states={
            "ENG": {
                "Todo": "id1",
                "In Progress": "id2",
                "Needs Approval": "id3",
                "Blocked": "id4",
                "Done": "id5",
            }
        },
    )
    _install_fake(monkeypatch, fake)
    _seed_preflight_db(tmp_path, monkeypatch, [_ready_binding("Todo", waiting="Waiting")])
    result = CliRunner().invoke(main, ["preflight"])
    assert result.exit_code != 0
    assert "Waiting" in result.output


def test_preflight_checks_local_code_review_when_local_review_enabled(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    _isolate_codex_home(tmp_path, monkeypatch)
    fake = _FakeLinear(
        viewer_keys=["ENG"],
        states={
            "ENG": {
                "Todo": "id1",
                "In Progress": "id2",
                "In Review": "id3",
                "Manual Approval": "id4",
                "Blocked": "id5",
                "Done": "id6",
            }
        },
    )
    _install_fake(monkeypatch, fake)
    _seed_preflight_db(
        tmp_path,
        monkeypatch,
        [
            _review_lane_binding(
                local_review=True, remote_review=True, local_code_review="Local Review"
            )
        ],
    )

    result = CliRunner().invoke(main, ["preflight"])

    assert result.exit_code != 0
    assert "local_code_review state 'Local Review'" in result.output


def test_preflight_allows_empty_review_lanes_when_both_reviews_disabled(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    _isolate_codex_home(tmp_path, monkeypatch)
    fake = _FakeLinear(
        viewer_keys=["ENG"],
        states={
            "ENG": {
                "Todo": "id1",
                "In Progress": "id2",
                "Manual Approval": "id3",
                "Blocked": "id4",
                "Done": "id5",
            }
        },
    )
    _install_fake(monkeypatch, fake)
    _seed_preflight_db(
        tmp_path,
        monkeypatch,
        [
            _review_lane_binding(
                local_review=False, remote_review=False, code_review="", local_code_review=""
            )
        ],
    )

    result = CliRunner().invoke(main, ["preflight"])

    assert result.exit_code == 0, result.output
    assert "ENG → org/api-svc: states ok" in result.output


def test_preflight_allows_omitted_review_lanes_when_both_reviews_disabled(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    _isolate_codex_home(tmp_path, monkeypatch)
    fake = _FakeLinear(
        viewer_keys=["ENG"],
        states={
            "ENG": {
                "Todo": "id1",
                "In Progress": "id2",
                "Manual Approval": "id3",
                "Blocked": "id4",
                "Done": "id5",
            }
        },
    )
    _install_fake(monkeypatch, fake)
    _seed_preflight_db(
        tmp_path,
        monkeypatch,
        [
            _review_lane_binding(
                local_review=False, remote_review=False, code_review=None, local_code_review=None
            )
        ],
    )

    result = CliRunner().invoke(main, ["preflight"])

    assert result.exit_code == 0, result.output
    assert "ENG → org/api-svc: states ok" in result.output


def test_preflight_allows_local_only_without_code_review_lane(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    _isolate_codex_home(tmp_path, monkeypatch)
    fake = _FakeLinear(
        viewer_keys=["ENG"],
        states={
            "ENG": {
                "Todo": "id1",
                "In Progress": "id2",
                "Local Review": "id3",
                "Manual Approval": "id4",
                "Blocked": "id5",
                "Done": "id6",
            }
        },
    )
    _install_fake(monkeypatch, fake)
    _seed_preflight_db(
        tmp_path,
        monkeypatch,
        [
            _review_lane_binding(
                local_review=True,
                remote_review=False,
                code_review="",
                local_code_review="Local Review",
            )
        ],
    )

    result = CliRunner().invoke(main, ["preflight"])

    assert result.exit_code == 0, result.output
    assert "ENG → org/api-svc: states ok" in result.output


def test_preflight_allows_remote_only_without_local_code_review_lane(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    _isolate_codex_home(tmp_path, monkeypatch)
    fake = _FakeLinear(
        viewer_keys=["ENG"],
        states={
            "ENG": {
                "Todo": "id1",
                "In Progress": "id2",
                "In Review": "id3",
                "Manual Approval": "id4",
                "Blocked": "id5",
                "Done": "id6",
            }
        },
    )
    _install_fake(monkeypatch, fake)
    _seed_preflight_db(
        tmp_path,
        monkeypatch,
        [
            _review_lane_binding(
                local_review=False,
                remote_review=True,
                code_review="In Review",
                local_code_review="",
            )
        ],
    )

    result = CliRunner().invoke(main, ["preflight"])

    assert result.exit_code == 0, result.output
    assert "ENG → org/api-svc: states ok" in result.output


def test_preflight_requires_code_review_when_remote_review_enabled(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("LINEAR_API_KEY", "x")
    _isolate_codex_home(tmp_path, monkeypatch)
    fake = _FakeLinear(
        viewer_keys=["ENG"],
        states={
            "ENG": {
                "Todo": "id1",
                "In Progress": "id2",
                "Manual Approval": "id3",
                "Blocked": "id4",
                "Done": "id5",
            }
        },
    )
    _install_fake(monkeypatch, fake)
    _seed_preflight_db(
        tmp_path,
        monkeypatch,
        [
            _review_lane_binding(
                local_review=False, remote_review=True, code_review="", local_code_review=""
            )
        ],
    )

    result = CliRunner().invoke(main, ["preflight"])

    assert result.exit_code != 0
    assert "code_review state ''" in result.output
