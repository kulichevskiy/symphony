"""The assembled :class:`Orchestrator` (SYM-151).

Phase 1 of the split is closed here: every behaviour lives on a domain mixin
(or `_OrchestratorBase`), so this module is pure assembly — it mixes the seven
components together and adds nothing of its own. Dedup / simplification of the
god-methods is Phase 2.
"""

from __future__ import annotations

from ._acceptance import _AcceptanceMixin
from ._base import _OrchestratorBase
from ._dispatch import _DispatchMixin
from ._lifecycle import _LifecycleMixin
from ._merge import _MergeMixin
from ._review import _ReviewMixin
from ._slash_commands import _SlashCommandsMixin


# Base-class order is load-bearing: it is the pre-split MRO (git `a4fea57`),
# preserved so that the few handler names defined on more than one mixin resolve
# to the same owner they did before the split (e.g.
# `_handle_active_review_retry_intent` -> `_ReviewMixin`,
# `_handle_merge_needs_approval_slash_intent` -> `_MergeMixin`). Reordering would
# silently change behaviour.
class Orchestrator(
    _LifecycleMixin,
    _ReviewMixin,
    _MergeMixin,
    _SlashCommandsMixin,
    _DispatchMixin,
    _AcceptanceMixin,
    _OrchestratorBase,
):
    """Owns the poll loop. Dedupe is a SQLite query over the `runs` table."""
