"""Claude code-paste login driver + pending-login registry (OAuth in UI 5/7).

The daemon drives the `claude` CLI login as a subprocess: `start` captures the
printed OAuth URL, `submit_code` feeds the pasted authorization code to stdin
and reads back the credentials the CLI wrote. Subprocess interaction is faked
here — these pin the registry (holding the live handle between the two
requests) and the credential-file helpers.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from symphony.claude_login import (
    PendingLoginRegistry,
    claude_expires_at,
    read_claude_credential,
)


class _FakeLogin:
    def __init__(self, url: str, credential: str) -> None:
        self._url = url
        self._credential = credential
        self.closed = False
        self.submitted: str | None = None

    async def start(self) -> str:
        return self._url

    async def submit_code(self, code: str) -> str:
        self.submitted = code
        return self._credential

    async def close(self) -> None:
        self.closed = True


def test_registry_add_then_pop_is_single_use() -> None:
    registry = PendingLoginRegistry(id_factory=iter(["s1", "s2"]).__next__)
    proc = _FakeLogin("https://claude.ai/oauth?x=1", "{}")
    session_id = registry.add(proc)
    assert session_id == "s1"
    assert registry.pop("s1") is proc
    # Single-use: the handle is gone after the first pop.
    assert registry.pop("s1") is None


def test_registry_pop_unknown_returns_none() -> None:
    registry = PendingLoginRegistry()
    assert registry.pop("never-issued") is None


@pytest.mark.asyncio
async def test_registry_discard_closes_process() -> None:
    registry = PendingLoginRegistry(id_factory=iter(["s1"]).__next__)
    proc = _FakeLogin("https://claude.ai/oauth", "{}")
    registry.add(proc)
    await registry.discard("s1")
    assert proc.closed is True
    assert registry.pop("s1") is None


def test_read_claude_credential_missing_returns_none(tmp_path: Path) -> None:
    assert read_claude_credential(tmp_path / "nope.json") is None


def test_read_claude_credential_returns_raw_text(tmp_path: Path) -> None:
    path = tmp_path / ".credentials.json"
    payload = '{"claudeAiOauth": {"accessToken": "tok"}}'
    path.write_text(payload, encoding="utf-8")
    assert read_claude_credential(path) == payload


def test_claude_expires_at_parses_epoch_millis() -> None:
    # 2030-01-01T00:00:00Z == 1893456000000 ms
    raw = json.dumps({"claudeAiOauth": {"expiresAt": 1893456000000}})
    assert claude_expires_at(raw) == "2030-01-01T00:00:00Z"


def test_claude_expires_at_none_when_absent_or_garbage() -> None:
    assert claude_expires_at(json.dumps({"claudeAiOauth": {}})) is None
    assert claude_expires_at("not json") is None
    assert claude_expires_at(json.dumps({"claudeAiOauth": {"expiresAt": "soon"}})) is None
