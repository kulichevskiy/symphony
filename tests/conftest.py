import json
from pathlib import Path

import pytest


@pytest.fixture
def private_bench_controls(tmp_path: Path) -> Path:
    """Synthetic controls for unit tests; production controls never live in Git."""
    root = tmp_path / "private-controls"
    reference = root / "feedback_inbox_reference" / "feedback_inbox"
    hidden = root / "hidden" / "feedback_inbox"
    reference.mkdir(parents=True)
    hidden.mkdir(parents=True)
    (reference / "main.py").write_text("# synthetic reference\n", encoding="utf-8")
    (hidden / "test_backend_hidden.py").write_text("# synthetic hidden test\n", encoding="utf-8")
    (hidden / "App.bench.test.tsx").write_text("// synthetic hidden test\n", encoding="utf-8")
    (hidden / "manifest.json").write_text(
        json.dumps(
            {
                "backend_total": 9,
                "frontend_total": 7,
                "seed_backend_passed": 1,
                "seed_frontend_passed": 1,
            }
        ),
        encoding="utf-8",
    )
    return root
