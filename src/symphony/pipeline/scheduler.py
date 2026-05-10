"""Pure dispatch planner — enforces global + per-binding concurrency caps.

The orchestrator gathers the pending issues (oldest first) and the
current active-run counts, then asks `plan_dispatch` what to actually
hand off this tick. Keeping this pure means the cap logic is unit-tested
without spinning up SQLite or running clones.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PendingDispatch:
    issue_id: str
    binding_key: str  # github_repo or any stable per-binding identifier


def plan_dispatch(
    *,
    pending: list[PendingDispatch],
    active_per_binding: dict[str, int],
    active_total: int,
    global_cap: int,
    binding_caps: dict[str, int],
) -> list[PendingDispatch]:
    """Return the subset of `pending` that should be dispatched right now.

    Order is preserved (callers pass oldest-first). A pending entry is
    skipped — not stopped at — if its binding is full but global capacity
    remains; later entries for other bindings can still go through. An
    unknown binding (no entry in `binding_caps`) is treated as cap=0.
    """
    out: list[PendingDispatch] = []
    used_per = dict(active_per_binding)
    used_total = active_total
    for p in pending:
        if used_total >= global_cap:
            break
        cap = binding_caps.get(p.binding_key, 0)
        if used_per.get(p.binding_key, 0) >= cap:
            continue
        out.append(p)
        used_per[p.binding_key] = used_per.get(p.binding_key, 0) + 1
        used_total += 1
    return out
