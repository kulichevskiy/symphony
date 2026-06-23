"""In-memory deterministic Linear/GitHub rehearsal rig.

Importable test support (not a `test_*.py`). See `harness.py` for the entry
point: `await Harness.create(tmp_path)`.
"""

from __future__ import annotations

from .clock import ManualClock
from .fakes import FakeGitHub, FakeLinear
from .harness import Harness
from .invariants import assert_consistent
from .sim import Sim, SimComment, SimIssue, SimPR

__all__ = [
    "FakeGitHub",
    "FakeLinear",
    "Harness",
    "ManualClock",
    "Sim",
    "SimComment",
    "SimIssue",
    "SimPR",
    "assert_consistent",
]
