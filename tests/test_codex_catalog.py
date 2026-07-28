from __future__ import annotations

import sys
from pathlib import Path

import pytest

from symphony.agent.codex_catalog import CodexCatalogClient


@pytest.mark.asyncio
async def test_catalog_client_reads_visible_models_and_ordered_efforts(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("OPENAI_API_KEY", "host-openai-key")
    monkeypatch.setenv("CODEX_API_KEY", "host-codex-key")
    server = tmp_path / "fake_app_server.py"
    server.write_text(
        """
import json
import os
import sys
from pathlib import Path

assert Path(os.environ["CODEX_HOME"], "auth.json").read_text() == '{"tokens":{}}'
assert "OPENAI_API_KEY" not in os.environ
assert "CODEX_API_KEY" not in os.environ

initialized = False
for line in sys.stdin:
    request = json.loads(line)
    if request["method"] == "initialize":
        print(json.dumps({"id": request["id"], "result": {}}), flush=True)
    elif request["method"] == "initialized":
        initialized = True
    elif request["method"] == "model/list":
        assert initialized
        print(json.dumps({
            "id": request["id"],
            "result": {
                "data": [
                    {
                        "model": "gpt-future",
                        "supportedReasoningEfforts": [
                            {"reasoningEffort": "low", "description": "Low"},
                            {"reasoningEffort": "ultra", "description": "Ultra"}
                        ]
                    }
                ],
                "nextCursor": None
            }
        }), flush=True)
""",
        encoding="utf-8",
    )
    client = CodexCatalogClient(command=(sys.executable, str(server)))

    catalog = await client.get(credential='{"tokens":{}}', generation=7)

    assert catalog.models == ("gpt-future",)
    assert catalog.efforts_by_model == {"gpt-future": ("low", "ultra")}


@pytest.mark.asyncio
async def test_catalog_client_caches_by_credential_generation_and_keeps_stale_success(
    tmp_path: Path,
) -> None:
    count_path = tmp_path / "count"
    server = tmp_path / "counting_app_server.py"
    server.write_text(
        f"""
import json
import sys
from pathlib import Path

count_path = Path({str(count_path)!r})
count = int(count_path.read_text()) + 1 if count_path.exists() else 1
count_path.write_text(str(count))
for line in sys.stdin:
    request = json.loads(line)
    if request["method"] == "initialized":
        continue
    result = {{}} if request["method"] == "initialize" else {{
        "data": [{{
            "model": "gpt-dynamic",
            "supportedReasoningEfforts": [{{"reasoningEffort": "high"}}]
        }}],
        "nextCursor": None
    }}
    print(json.dumps({{"id": request["id"], "result": result}}), flush=True)
""",
        encoding="utf-8",
    )
    now = [0.0]
    client = CodexCatalogClient(
        command=(sys.executable, str(server)),
        ttl_seconds=10,
        clock=lambda: now[0],
    )

    first = await client.get(credential="{}", generation=1)
    cached = await client.get(credential="{}", generation=1)
    now[0] = 11
    client.command = ("missing-codex-for-test",)
    stale = await client.get(credential="{}", generation=1)

    assert first == cached
    assert count_path.read_text() == "1"
    assert stale.models == ("gpt-dynamic",)
    assert stale.source == "stale"


@pytest.mark.asyncio
async def test_catalog_client_does_not_reuse_stale_catalog_for_new_generation(
    tmp_path: Path,
) -> None:
    server = tmp_path / "app_server.py"
    server.write_text(
        """
import json
import sys

for line in sys.stdin:
    request = json.loads(line)
    if request["method"] == "initialized":
        continue
    result = {} if request["method"] == "initialize" else {
        "data": [{"model": "gpt-account-a", "supportedReasoningEfforts": []}],
        "nextCursor": None,
    }
    print(json.dumps({"id": request["id"], "result": result}), flush=True)
""",
        encoding="utf-8",
    )
    now = [0.0]
    client = CodexCatalogClient(
        command=(sys.executable, str(server)),
        ttl_seconds=10,
        clock=lambda: now[0],
    )
    first = await client.get(credential="account-a", generation=1)
    now[0] = 11
    client.command = ("missing-codex-for-test",)

    fallback = await client.get(credential="account-b", generation=2)

    assert first.models == ("gpt-account-a",)
    assert fallback.source == "static"
    assert "gpt-account-a" not in fallback.models


@pytest.mark.asyncio
async def test_catalog_client_throttles_failed_refreshes(tmp_path: Path) -> None:
    count_path = tmp_path / "attempts"
    server = tmp_path / "failing_app_server.py"
    server.write_text(
        f"""
from pathlib import Path

count_path = Path({str(count_path)!r})
count = int(count_path.read_text()) + 1 if count_path.exists() else 1
count_path.write_text(str(count))
""",
        encoding="utf-8",
    )
    now = [0.0]
    client = CodexCatalogClient(
        command=(sys.executable, str(server)),
        failure_ttl_seconds=10,
        clock=lambda: now[0],
    )

    first = await client.get(credential="{}", generation=1)
    second = await client.get(credential="{}", generation=1)
    now[0] = 11
    third = await client.get(credential="{}", generation=1)

    assert first.source == second.source == third.source == "static"
    assert count_path.read_text() == "2"


@pytest.mark.asyncio
async def test_catalog_client_uses_static_fallback_without_credential() -> None:
    client = CodexCatalogClient(command=("missing-codex-for-test",))

    catalog = await client.get(credential=None, generation=None)

    assert {"gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna"} <= set(catalog.models)
    assert catalog.source == "static"


@pytest.mark.asyncio
async def test_catalog_client_writes_back_rotated_credential(tmp_path: Path) -> None:
    server = tmp_path / "rotating_app_server.py"
    server.write_text(
        """
import json
import os
import sys
from pathlib import Path

auth_path = Path(os.environ["CODEX_HOME"], "auth.json")
for line in sys.stdin:
    request = json.loads(line)
    if request["method"] == "initialized":
        continue
    if request["method"] == "initialize":
        result = {}
    else:
        auth_path.write_text('{"rotated":true}')
        result = {
            "data": [{
                "model": "gpt-rotating",
                "supportedReasoningEfforts": [{"reasoningEffort": "high"}]
            }],
            "nextCursor": None
        }
    print(json.dumps({"id": request["id"], "result": result}), flush=True)
""",
        encoding="utf-8",
    )
    written: list[str] = []

    async def _write_back(credential: str) -> int:
        written.append(credential)
        return 8

    client = CodexCatalogClient(command=(sys.executable, str(server)))

    await client.get(credential='{"rotated":false}', generation=7, write_back=_write_back)
    cached = await client.get(credential='{"rotated":true}', generation=8)

    assert written == ['{"rotated":true}']
    assert cached.models == ("gpt-rotating",)
