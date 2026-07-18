"""Codex device-auth login driver + registry peek (OAuth in UI 6/7).

The daemon drives `codex login --device-auth` as a subprocess: `start` captures
the printed verification URL + user code, then `poll` reports pending →
success/failure as the subprocess exits (reading back `~/.codex/auth.json` on
success). Subprocess interaction is faked in the UI test; these pin the shared
`PendingLoginRegistry`'s `peek` (used across repeated polls) and the codex
credential-file helpers.
"""

from __future__ import annotations

import asyncio
import base64
import json
from pathlib import Path

import pytest

from symphony.claude_login import PendingLoginRegistry
from symphony.codex_login import (
    codex_credential_expired,
    codex_expires_at,
    default_codex_credentials_path,
    read_codex_credential,
)


class _FakeCodexLogin:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def _jwt(exp: int) -> str:
    def seg(obj: dict[str, object]) -> str:
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()

    return f"{seg({'alg': 'RS256'})}.{seg({'exp': exp})}.sig"


def _auth_json(exp: int) -> str:
    return json.dumps({"tokens": {"access_token": _jwt(exp), "refresh_token": "r"}})


def test_registry_peek_returns_handle_without_consuming() -> None:
    registry: PendingLoginRegistry[_FakeCodexLogin] = PendingLoginRegistry(
        id_factory=iter(["s1"]).__next__
    )
    proc = _FakeCodexLogin()
    session_id = registry.add(proc)
    # Peek is repeatable (device-auth polls the same session many times)…
    assert registry.peek(session_id) is proc
    assert registry.peek(session_id) is proc
    # …and does not consume it, unlike pop.
    assert registry.pop(session_id) is proc
    assert registry.peek(session_id) is None


def test_registry_peek_unknown_returns_none() -> None:
    registry: PendingLoginRegistry[_FakeCodexLogin] = PendingLoginRegistry()
    assert registry.peek("never-issued") is None


@pytest.mark.asyncio
async def test_registry_peek_expired_returns_none_and_closes() -> None:
    now = [0.0]
    registry: PendingLoginRegistry[_FakeCodexLogin] = PendingLoginRegistry(
        id_factory=iter(["s1"]).__next__, ttl_secs=10.0, clock=lambda: now[0]
    )
    proc = _FakeCodexLogin()
    registry.add(proc)
    now[0] = 10.0
    assert registry.peek("s1") is None
    await asyncio.sleep(0)  # let the fire-and-forget close() task run
    assert proc.closed is True


def test_read_codex_credential_missing_returns_none(tmp_path: Path) -> None:
    assert read_codex_credential(tmp_path / "nope.json") is None


def test_read_codex_credential_returns_raw_text(tmp_path: Path) -> None:
    path = tmp_path / "auth.json"
    payload = '{"tokens": {"access_token": "tok"}}'
    path.write_text(payload, encoding="utf-8")
    assert read_codex_credential(path) == payload


def test_default_credentials_path_honors_codex_home(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("CODEX_HOME", str(tmp_path))
    assert default_codex_credentials_path() == tmp_path / "auth.json"


def test_default_credentials_path_without_codex_home(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CODEX_HOME", raising=False)
    assert default_codex_credentials_path() == Path.home() / ".codex" / "auth.json"


def test_codex_expires_at_parses_jwt_exp() -> None:
    # 2030-01-01T00:00:00Z == 1893456000 (epoch seconds)
    assert codex_expires_at(_auth_json(1893456000)) == "2030-01-01T00:00:00Z"


def test_codex_expires_at_none_when_absent_or_garbage() -> None:
    assert codex_expires_at(json.dumps({"tokens": {}})) is None
    assert codex_expires_at(json.dumps({"tokens": {"access_token": "not-a-jwt"}})) is None
    assert codex_expires_at("not json") is None


def test_codex_credential_expired() -> None:
    assert codex_credential_expired(_auth_json(1000000000)) is True  # 2001 — past
    assert codex_credential_expired(_auth_json(1893456000)) is False  # 2030 — future
    # No parseable expiry → treated as not-expired (don't flip a usable card).
    assert codex_credential_expired(json.dumps({"tokens": {}})) is False
