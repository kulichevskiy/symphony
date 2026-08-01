from pathlib import Path

import pytest

from symphony.bench import harness as harness_module
from symphony.bench.eventdesk import eventdesk_campaign, harness_version, materialize_eventdesk
from symphony.bench.harness import load_harness, snapshot_harness


def test_eventdesk_campaign_is_six_complete_sequential_afk_tickets() -> None:
    campaign = eventdesk_campaign()

    assert [ticket.key for ticket in campaign.tickets] == [
        "booking",
        "capacity",
        "waitlist",
        "cancellation",
        "return-to",
        "payment-webhook",
    ]
    assert [ticket.blocked_by for ticket in campaign.tickets] == [
        (),
        ("booking",),
        ("capacity",),
        ("waitlist",),
        ("cancellation",),
        ("return-to",),
    ]
    for ticket in campaign.tickets:
        assert "## Context" in ticket.description
        assert "## Requirements" in ticket.description
        assert "## Acceptance criteria" in ticket.description
        assert "## Verification" in ticket.description
        assert "Do not ask questions" in ticket.description


def test_materialize_eventdesk_creates_runnable_full_stack_seed(tmp_path: Path) -> None:
    destination = tmp_path / "eventdesk"

    materialize_eventdesk(destination)

    expected = {
        "README.md",
        "STANDARDS.md",
        "pyproject.toml",
        ".github/workflows/ci.yml",
        "eventdesk/main.py",
        "tests/test_events.py",
        "frontend/package.json",
        "frontend/src/App.tsx",
    }
    assert expected <= {
        str(path.relative_to(destination)) for path in destination.rglob("*") if path.is_file()
    }


def test_harness_version_is_stable_content_hash() -> None:
    version = harness_version()

    assert len(version) == 16
    assert version == harness_version()


def test_harness_version_includes_extensionless_and_dotfiles(tmp_path: Path) -> None:
    (tmp_path / ".gitignore").write_text("*.sqlite\n")
    (tmp_path / "NOTICE").write_text("one\n")
    first = harness_version(tmp_path)

    (tmp_path / ".gitignore").write_text("*.sqlite\n*.log\n")
    second = harness_version(tmp_path)

    assert first != second


def test_harness_snapshot_freezes_workload_and_verifies_checksum(tmp_path: Path) -> None:
    snapshot = tmp_path / "snapshot"
    version = snapshot_harness(snapshot)

    frozen = load_harness(snapshot)

    assert frozen.version == version
    assert len(frozen.campaign.tickets) == 6
    assert frozen.hidden_test.exists()
    assert frozen.regression_commands["frontend_build"] == ["npm", "run", "build"]
    assert "SPEC reviewer" in frozen.spec_prompt
    assert (snapshot / "eventdesk" / ".gitignore").exists()

    (snapshot / "campaign.json").write_text("{}")
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        load_harness(snapshot)


def test_harness_snapshot_rejects_changed_execution_engine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    snapshot = tmp_path / "snapshot"
    snapshot_harness(snapshot)
    original = harness_module.harness_version

    def changed(root: Path | None = None) -> str:
        return "different-engine" if root is None else original(root)

    monkeypatch.setattr(harness_module, "harness_version", changed)

    with pytest.raises(RuntimeError, match="engine changed"):
        load_harness(snapshot)


def test_harness_snapshot_rejects_invalid_regression_command(tmp_path: Path) -> None:
    snapshot = tmp_path / "snapshot"
    snapshot_harness(snapshot)
    (snapshot / "regression_commands.json").write_text(
        '{"backend_tests": "uv run pytest"}\n', encoding="utf-8"
    )
    (snapshot / ".version").write_text(harness_version(snapshot), encoding="utf-8")

    with pytest.raises(RuntimeError, match="invalid harness regression command"):
        load_harness(snapshot)
