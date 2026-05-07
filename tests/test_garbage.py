from __future__ import annotations

import os
from types import SimpleNamespace

from symphony import garbage as garbage_mod
from symphony.garbage import find_gc_candidates


def _cfg(tmp_path):
    return SimpleNamespace(
        repo=SimpleNamespace(path=tmp_path / "repo"),
        paths=SimpleNamespace(worktree_root=tmp_path / "wts"),
    )


def test_find_gc_candidates_uses_nested_activity_mtime(monkeypatch, tmp_path):
    cfg = _cfg(tmp_path)
    cfg.paths.worktree_root.mkdir()
    wt = cfg.paths.worktree_root / "symphony-42"
    nested = wt / "src"
    nested.mkdir(parents=True)
    active_file = nested / "active.txt"
    active_file.write_text("recent\n")
    now = 1_000_000.0
    old = now - (20 * 24 * 60 * 60)
    os.utime(wt, (old, old))
    os.utime(nested, (old, old))
    os.utime(active_file, (now, now))

    monkeypatch.setattr(garbage_mod, "name_with_owner", lambda p: ("owner", "symphony"))

    candidates = find_gc_candidates(
        cfg,
        days=14,
        now=now,
        stuck_issues_fn=lambda: {42},
    )

    assert candidates == []


def test_find_gc_candidates_lists_old_inactive_worktree(monkeypatch, tmp_path):
    cfg = _cfg(tmp_path)
    cfg.paths.worktree_root.mkdir()
    wt = cfg.paths.worktree_root / "symphony-42"
    nested = wt / "src"
    nested.mkdir(parents=True)
    old_file = nested / "old.txt"
    old_file.write_text("old\n")
    now = 1_000_000.0
    old = now - (20 * 24 * 60 * 60)
    for path in (old_file, nested, wt):
        os.utime(path, (old, old))

    monkeypatch.setattr(garbage_mod, "name_with_owner", lambda p: ("owner", "symphony"))

    candidates = find_gc_candidates(
        cfg,
        days=14,
        now=now,
        stuck_issues_fn=lambda: {42},
    )

    assert [candidate.issue_number for candidate in candidates] == [42]
    assert candidates[0].age_days == 20
