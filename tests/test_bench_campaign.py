from pathlib import Path

import pytest

from symphony.bench import harness as harness_module
from symphony.bench.campaign import (
    feedback_inbox_campaign,
    harness_version,
    materialize_feedback_inbox,
    materialize_private_control,
    materialize_support_queue,
    support_queue_campaign,
)
from symphony.bench.harness import load_harness, snapshot_harness


def test_feedback_inbox_campaign_is_two_complete_sequential_afk_tickets() -> None:
    campaign = feedback_inbox_campaign()

    assert campaign.name == "Feedback Inbox V1"
    assert [ticket.key for ticket in campaign.tickets] == ["backend", "frontend"]
    assert [ticket.blocked_by for ticket in campaign.tickets] == [(), ("backend",)]
    for ticket in campaign.tickets:
        assert "## Context" in ticket.description
        assert "## Requirements" in ticket.description
        assert "## Acceptance criteria" in ticket.description
        assert "## Verification" in ticket.description
        assert "Do not ask questions" in ticket.description


def test_support_queue_campaign_is_four_complete_diamond_afk_tickets() -> None:
    campaign = support_queue_campaign()

    assert campaign.name == "Support Queue V1"
    assert [ticket.key for ticket in campaign.tickets] == [
        "core",
        "workflow",
        "frontend",
        "integration",
    ]
    assert [ticket.blocked_by for ticket in campaign.tickets] == [
        (),
        ("core",),
        ("core",),
        ("workflow", "frontend"),
    ]
    for ticket in campaign.tickets:
        assert "## Context" in ticket.description
        assert "## Requirements" in ticket.description
        assert "## Acceptance criteria" in ticket.description
        assert "## Verification" in ticket.description
        assert "Do not ask questions" in ticket.description

    combined = "\n".join(ticket.description for ticket in campaign.tickets)
    assert "optimistic" in combined.lower()
    assert "permissions" in combined.lower()
    assert "URL" in combined
    assert "accessibility" in combined.lower()


def test_materialize_feedback_inbox_creates_runnable_full_stack_seed(tmp_path: Path) -> None:
    destination = tmp_path / "feedback-inbox"

    materialize_feedback_inbox(destination)

    expected = {
        "README.md",
        "STANDARDS.md",
        "pyproject.toml",
        ".github/workflows/ci.yml",
        "feedback_inbox/main.py",
        "tests/test_health.py",
        "frontend/package.json",
        "frontend/src/App.tsx",
    }
    assert expected <= {
        str(path.relative_to(destination)) for path in destination.rglob("*") if path.is_file()
    }
    assert not (destination / ".venv").exists()
    assert not (destination / "frontend/node_modules").exists()
    assert not (destination / "frontend/dist").exists()
    workflow = (destination / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert "uv run mypy feedback_inbox" in workflow
    assert "mypy eventdesk" not in workflow


def test_materialize_support_queue_creates_runnable_full_stack_seed(tmp_path: Path) -> None:
    destination = tmp_path / "support-queue"

    materialize_support_queue(destination)

    expected = {
        "README.md",
        "STANDARDS.md",
        "pyproject.toml",
        ".github/workflows/ci.yml",
        "support_queue/main.py",
        "tests/test_health.py",
        "frontend/package.json",
        "frontend/src/App.tsx",
    }
    assert expected <= {
        str(path.relative_to(destination)) for path in destination.rglob("*") if path.is_file()
    }
    assert not (destination / ".venv").exists()
    assert not (destination / "frontend/node_modules").exists()
    assert not (destination / "frontend/dist").exists()


def test_materialize_private_reference_makes_read_only_source_directories_writable(
    tmp_path: Path,
) -> None:
    source = tmp_path / "controls"
    nested = source / "feedback_inbox"
    nested.mkdir(parents=True)
    (nested / "main.py").write_text("# reference\n", encoding="utf-8")
    nested.chmod(0o555)
    source.chmod(0o555)
    destination = tmp_path / "reference"

    materialize_private_control(source, destination)

    (destination / ".venv").mkdir()
    (destination / "feedback_inbox/generated.py").write_text("# generated\n", encoding="utf-8")


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


def test_harness_version_ignores_tool_caches(tmp_path: Path) -> None:
    (tmp_path / "workload.py").write_text("one\n")
    first = harness_version(tmp_path)
    for cache in (".pytest_cache", ".mypy_cache", ".ruff_cache"):
        path = tmp_path / cache / "state"
        path.parent.mkdir()
        path.write_text("volatile\n")

    assert harness_version(tmp_path) == first


def test_harness_snapshot_freezes_workload_and_verifies_checksum(
    tmp_path: Path, private_bench_controls: Path
) -> None:
    snapshot = tmp_path / "snapshot"
    version = snapshot_harness(snapshot, controls_root=private_bench_controls)

    frozen = load_harness(snapshot)

    assert frozen.version == version
    assert frozen.campaign.name == "Support Queue V1"
    assert len(frozen.campaign.tickets) == 4
    assert frozen.backend_hidden_test.exists()
    assert frozen.frontend_hidden_test.exists()
    assert (frozen.reference_root / "support_queue/main.py").exists()
    assert sorted(frozen.mutation_roots) == ["broken_accessibility", "broken_workflow"]
    assert frozen.hidden_manifest.backend_total == 16
    assert frozen.hidden_manifest.frontend_total == 13
    assert (snapshot / "support_queue/support_queue/main.py").exists()
    assert not (snapshot / "feedback_inbox").exists()
    assert not (snapshot / "eventdesk").exists()
    assert frozen.regression_commands["frontend_build"] == ["npm", "run", "build"]
    assert "SPEC reviewer" in frozen.spec_prompt
    assert (snapshot / "support_queue" / ".gitignore").exists()

    (snapshot / "campaign.json").write_text("{}")
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        load_harness(snapshot)


def test_harness_snapshot_rejects_changed_execution_engine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, private_bench_controls: Path
) -> None:
    snapshot = tmp_path / "snapshot"
    snapshot_harness(snapshot, controls_root=private_bench_controls)
    original = harness_module.harness_version

    def changed(root: Path | None = None) -> str:
        return "different-engine" if root is None else original(root)

    monkeypatch.setattr(harness_module, "harness_version", changed)

    with pytest.raises(RuntimeError, match="engine changed"):
        load_harness(snapshot)


def test_harness_snapshot_rejects_invalid_regression_command(
    tmp_path: Path, private_bench_controls: Path
) -> None:
    snapshot = tmp_path / "snapshot"
    snapshot_harness(snapshot, controls_root=private_bench_controls)
    (snapshot / "regression_commands.json").write_text(
        '{"backend_tests": "uv run pytest"}\n', encoding="utf-8"
    )
    (snapshot / ".version").write_text(harness_version(snapshot), encoding="utf-8")

    with pytest.raises(RuntimeError, match="invalid harness regression command"):
        load_harness(snapshot)


def test_harness_snapshot_requires_every_control_entrypoint(
    tmp_path: Path, private_bench_controls: Path
) -> None:
    missing = (
        private_bench_controls
        / "support_queue_mutations"
        / "broken_accessibility"
        / "frontend"
        / "src"
        / "App.tsx"
    )
    missing.unlink()

    with pytest.raises(RuntimeError, match="private benchmark controls are incomplete"):
        snapshot_harness(tmp_path / "snapshot", controls_root=private_bench_controls)
