"""Unit tests for `config_export` (SYM-195 review fixes).

Covers the reviewer findings from the second pass: MCP credential redaction,
and a valid-and-uncommentable `repos:` shape when every downgrade-mode binding
is disabled.
"""

from __future__ import annotations

import yaml

from symphony.config_export import export_config
from symphony.db.config_bindings import StoredBinding


def _row(*, payload: dict, enabled: bool = True, priority: int = 0, **kw) -> StoredBinding:
    defaults = dict(
        id=1,
        version=1,
        enabled=enabled,
        priority=priority,
        updated_at="",
        updated_by="",
        project_key="ENG",
        github_repo="org/api",
        issue_label="",
        tracker_provider="linear",
        tracker_site="default",
    )
    defaults.update(kw)
    return StoredBinding(payload=payload, **defaults)


def test_export_redacts_mcp_credentials() -> None:
    """A binding's `mcp_servers` `env`/`headers` credentials must never appear
    in the exported YAML — only a per-key `true` marker (SYM-195 review)."""
    row = _row(
        payload={
            "mcp_servers": {
                "supabase": {
                    "command": "npx",
                    "env": {"API_KEY": "literal-secret-value"},
                    "headers": {"Authorization": "Bearer literal-token"},
                }
            }
        }
    )
    for mode in ("restore", "downgrade"):
        text = export_config([row], {}, set(), mode=mode)
        assert "literal-secret-value" not in text
        assert "literal-token" not in text
        doc = yaml.safe_load(text)
        server = doc["repos"][0]["mcp_servers"]["supabase"]
        assert server["env"] == {"API_KEY": True}
        assert server["headers"] == {"Authorization": True}


def test_downgrade_all_disabled_repos_parses_empty_and_is_uncommentable() -> None:
    """When every binding is disabled in downgrade mode, the exported
    `repos:` must still parse as an empty list as-is, and uncommenting a
    single disabled entry must still produce valid YAML (SYM-195 review)."""
    rows = [
        _row(payload={"max_concurrent": 4}, enabled=False, priority=0, github_repo="org/api"),
        _row(payload={"issue_label": "urgent"}, enabled=False, priority=1, github_repo="org/web"),
    ]
    text = export_config(rows, {}, set(), mode="downgrade")
    doc = yaml.safe_load(text)
    assert doc["repos"] == []

    lines = text.splitlines()
    start = lines.index("repos: [")
    end = lines.index("]", start)
    commented = lines[start + 1 : end]
    assert len(commented) == 2
    assert all(ln.startswith("#") for ln in commented)

    # Uncommenting exactly one entry must still parse, with only that binding live.
    uncommented = commented[0].lstrip("#")
    edited = lines[: start + 1] + [uncommented, commented[1]] + lines[end:]
    doc2 = yaml.safe_load("\n".join(edited))
    assert len(doc2["repos"]) == 1
    assert doc2["repos"][0]["max_concurrent"] == 4
