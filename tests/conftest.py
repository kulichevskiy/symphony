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

    support_reference = root / "support_queue_reference" / "support_queue"
    support_hidden = root / "hidden" / "support_queue"
    support_reference.mkdir(parents=True)
    support_hidden.mkdir(parents=True)
    (support_reference / "main.py").write_text("# synthetic reference\n", encoding="utf-8")
    for name in ("broken_workflow", "broken_accessibility"):
        mutation = root / "support_queue_mutations" / name / "support_queue"
        mutation.mkdir(parents=True)
        (mutation / "main.py").write_text(f"# synthetic {name}\n", encoding="utf-8")
    (support_hidden / "test_backend_hidden.py").write_text(
        "# synthetic hidden test\n", encoding="utf-8"
    )
    (support_hidden / "App.bench.test.tsx").write_text(
        "// synthetic hidden test\n", encoding="utf-8"
    )
    (support_hidden / "manifest.json").write_text(
        json.dumps(
            {
                "backend_total": 14,
                "frontend_total": 10,
                "seed_backend_passed": 1,
                "seed_frontend_passed": 1,
                "mutations": {
                    "broken_workflow": {"backend_passed": 12, "frontend_passed": 10},
                    "broken_accessibility": {"backend_passed": 14, "frontend_passed": 9},
                },
            }
        ),
        encoding="utf-8",
    )
    return root
