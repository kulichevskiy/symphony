from __future__ import annotations

import importlib.util
import os
import shutil
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "deploy"
    / "systemd"
    / "symphonyd-maintenance.py"
)


def _load_maintenance_module():
    spec = importlib.util.spec_from_file_location("symphonyd_maintenance", SCRIPT_PATH)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_maintenance_backs_up_sqlite_rotates_backups_and_prunes_logs(
    tmp_path: Path,
) -> None:
    sqlite3 = shutil.which("sqlite3")
    if sqlite3 is None:
        pytest.skip("sqlite3 CLI is required for maintenance backup test")

    module = _load_maintenance_module()
    now = datetime(2026, 5, 11, 12, 30, tzinfo=UTC)
    db_path = tmp_path / "state.sqlite"
    log_root = tmp_path / "logs"
    log_root.mkdir()

    subprocess.run(
        [
            sqlite3,
            str(db_path),
            "CREATE TABLE items (name TEXT); INSERT INTO items VALUES ('ok');",
        ],
        check=True,
    )

    old_backup = Path(f"{db_path}.bak.20260509T000000Z")
    kept_backup = Path(f"{db_path}.bak.20260510T000000Z")
    old_backup.write_text("old")
    kept_backup.write_text("kept")

    stale_log = log_root / "stale.log"
    fresh_log = log_root / "fresh.log"
    unrelated = log_root / "stale.txt"
    stale_log.write_text("stale")
    fresh_log.write_text("fresh")
    unrelated.write_text("ignore")
    stale_time = (now - timedelta(days=15)).timestamp()
    fresh_time = (now - timedelta(days=13)).timestamp()
    os.utime(stale_log, (stale_time, stale_time))
    os.utime(fresh_log, (fresh_time, fresh_time))

    result = module.run_maintenance(
        db_path=db_path,
        log_root=log_root,
        backup_keep=2,
        log_retention_days=14,
        sqlite3_bin=sqlite3,
        now=now,
    )

    assert result.backup_path == Path(f"{db_path}.bak.20260511T123000Z")
    assert result.backup_path.exists()
    rows = subprocess.run(
        [sqlite3, str(result.backup_path), "SELECT name FROM items;"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert rows.stdout.strip() == "ok"
    assert old_backup in result.removed_backups
    assert not old_backup.exists()
    assert kept_backup.exists()
    assert stale_log in result.removed_logs
    assert not stale_log.exists()
    assert fresh_log.exists()
    assert unrelated.exists()
