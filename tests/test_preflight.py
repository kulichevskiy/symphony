"""Preflight CLI tests — uses a fake Linear so no network is required."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from click.testing import CliRunner

from symphony.cli import main


class _FakeLinear:
    def __init__(
        self, viewer_keys: list[str], states: dict[str, dict[str, str]]
    ) -> None:
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


def _yaml_with_ready(ready: str = "Todo", *, waiting: str | None = None) -> str:
    waiting_line = f"      waiting: {waiting}\n" if waiting is not None else ""
    return f"""
repos:
  - linear_team_key: ENG
    github_repo: org/api-svc
    agent: claude
    review_strategy: remote
    linear_states:
      ready: {ready}
      in_progress: In Progress
      code_review: Needs Approval
      needs_approval: Needs Approval
      blocked: Blocked
{waiting_line}      done: Done
"""


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
    p = tmp_path / "cfg.yaml"
    p.write_text(_yaml_with_ready("Todo"))
    result = CliRunner().invoke(main, ["preflight", "--config", str(p)])
    assert result.exit_code == 0, result.output
    assert not (codex_home / "config.toml").exists()
    assert "codex permissions profile not required" in result.output


def test_preflight_allows_jira_binding_without_linear_key(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.delenv("LINEAR_API_KEY", raising=False)
    monkeypatch.setenv("JIRA_BASE_URL", "https://jira.example.test")
    monkeypatch.setenv("JIRA_EMAIL", "bot@example.test")
    monkeypatch.setenv("JIRA_API_TOKEN", "jira-token")
    _isolate_codex_home(tmp_path, monkeypatch)
    p = tmp_path / "cfg.yaml"
    p.write_text(
        """
repos:
  - provider: jira
    project_key: SYM
    base_url: https://jira.example.test
    github_repo: org/api-svc
    states:
      ready: To Do
      code_review: In Review
"""
    )

    result = CliRunner().invoke(main, ["preflight", "--config", str(p)])

    assert result.exit_code == 0, result.output
    assert "jira projects visible to this key: ['SYM']" in result.output
    assert "SYM → org/api-svc: states ok" in result.output


def test_preflight_creates_codex_profile_when_binding_uses_codex_agent(
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
    p = tmp_path / "cfg.yaml"
    p.write_text(_yaml_with_ready("Todo").replace("agent: claude", "agent: codex"))
    result = CliRunner().invoke(main, ["preflight", "--config", str(p)])
    assert result.exit_code == 0, result.output
    assert (codex_home / "config.toml").exists()
    assert "symphony-git" in result.output


def test_preflight_creates_codex_profile_when_local_reviewer_uses_codex(
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
    p = tmp_path / "cfg.yaml"
    p.write_text(
        _yaml_with_ready("Todo").replace("review_strategy: remote", "review_strategy: local")
    )
    result = CliRunner().invoke(main, ["preflight", "--config", str(p)])
    assert result.exit_code == 0, result.output
    assert (codex_home / "config.toml").exists()
    assert "symphony-git" in result.output


def test_preflight_fails_when_ready_not_in_team_states(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
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
    p = tmp_path / "cfg.yaml"
    # Binding asks for a "Backlog" ready state that the team's workflow lacks.
    p.write_text(_yaml_with_ready("Backlog"))
    result = CliRunner().invoke(main, ["preflight", "--config", str(p)])
    assert result.exit_code != 0
    assert "Backlog" in result.output


def test_preflight_fails_when_waiting_not_in_team_states(
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
                "Needs Approval": "id3",
                "Blocked": "id4",
                "Done": "id5",
            }
        },
    )
    _install_fake(monkeypatch, fake)
    p = tmp_path / "cfg.yaml"
    p.write_text(_yaml_with_ready("Todo", waiting="Waiting"))
    result = CliRunner().invoke(main, ["preflight", "--config", str(p)])
    assert result.exit_code != 0
    assert "Waiting" in result.output
