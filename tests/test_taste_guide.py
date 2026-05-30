from __future__ import annotations

from pathlib import Path

import pytest

from symphony.pipeline.taste_guide import load_taste_guide


def test_load_taste_guide_concatenates_global_then_binding(tmp_path: Path) -> None:
    repo_root = tmp_path / "symphony"
    repo_root.mkdir()
    binding_guide = repo_root / "docs" / "sample-ux.md"
    binding_guide.parent.mkdir()
    (repo_root / "taste-guide.md").write_text(
        "## Principles\n\nGlobal principles.\n\n"
        "## Hard rules (acceptance must reject if violated)\n\n"
        "- Global hard rule.\n",
        encoding="utf-8",
    )
    binding_guide.write_text(
        "## Hard rules (acceptance must reject if violated)\n\n"
        "- Binding hard rule.\n",
        encoding="utf-8",
    )

    guide = load_taste_guide(
        binding_taste_guide="./docs/sample-ux.md",
        repo_root=repo_root,
    )

    assert "Global principles." in guide
    assert "Binding hard rule." in guide
    assert guide.index("Global principles.") < guide.index("Binding hard rule.")
    assert guide.count("## Hard rules (acceptance must reject if violated)") == 2


def test_load_taste_guide_defaults_to_deploy_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    deploy_root = tmp_path / "deploy"
    deploy_root.mkdir()
    (deploy_root / "taste-guide.md").write_text("deploy global\n", encoding="utf-8")
    binding_guide = deploy_root / "docs" / "sample-ux.md"
    binding_guide.parent.mkdir()
    binding_guide.write_text("binding guide\n", encoding="utf-8")
    monkeypatch.chdir(deploy_root)

    guide = load_taste_guide(binding_taste_guide="./docs/sample-ux.md")

    assert guide == "deploy global\n\nbinding guide"


def test_load_taste_guide_returns_empty_string_when_no_files(tmp_path: Path) -> None:
    assert load_taste_guide(binding_taste_guide=None, repo_root=tmp_path) == ""
    assert (
        load_taste_guide(
            binding_taste_guide="./docs/missing.md",
            repo_root=tmp_path,
        )
        == ""
    )


def test_checked_in_global_taste_guide_has_required_sections() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    text = (repo_root / "taste-guide.md").read_text(encoding="utf-8")

    assert "## Principles" in text
    assert "## Hard rules (acceptance must reject if violated)" in text
    assert "## Known past mistakes" in text
    assert "VIB icon ticket" in text
    assert "inline text where an `<Icon/>`" in text
