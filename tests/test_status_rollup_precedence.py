from collections.abc import Callable
from typing import Any

import pytest

from symphony.github.client import _status_check_nodes
from symphony.orchestrator.poll._helpers import _status_rollup_nodes


@pytest.mark.parametrize("normalize", [_status_check_nodes, _status_rollup_nodes])
def test_status_rollup_prefers_nodes_then_edges_then_contexts(
    normalize: Callable[[object], list[dict[str, Any]]],
) -> None:
    edges = [{"node": {"name": "edges"}}]
    contexts = [{"name": "contexts"}]

    assert normalize({"nodes": [{"name": "nodes"}], "edges": edges, "contexts": contexts}) == [
        {"name": "nodes"}
    ]
    assert normalize({"edges": edges, "contexts": contexts}) == [{"name": "edges"}]
    assert normalize({"contexts": contexts}) == [{"name": "contexts"}]
