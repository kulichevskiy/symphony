"""Codex model discovery through the local app-server catalog."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import tempfile
import time
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from ..codex_login import default_codex_credentials_path, pin_file_auth_storage
from .codex_models import STATIC_CODEX_EFFORTS_BY_MODEL

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class CodexCatalog:
    models: tuple[str, ...]
    efforts_by_model: dict[str, tuple[str, ...]]
    source: str = "live"


STATIC_CODEX_CATALOG = CodexCatalog(
    models=tuple(STATIC_CODEX_EFFORTS_BY_MODEL),
    efforts_by_model=dict(STATIC_CODEX_EFFORTS_BY_MODEL),
    source="static",
)


@dataclass
class CodexCatalogClient:
    command: tuple[str, ...] = ("codex", "app-server")
    timeout_seconds: float = 5.0
    ttl_seconds: float = 600.0
    clock: Callable[[], float] = time.monotonic
    _cached_generation: int | None = field(default=None, init=False)
    _cached_at: float = field(default=0.0, init=False)
    _last_success: CodexCatalog | None = field(default=None, init=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)

    async def get(
        self,
        *,
        credential: str | None,
        generation: int | None,
        write_back: Callable[[str], Awaitable[int | None]] | None = None,
    ) -> CodexCatalog:
        """Return the entitled catalog, or a stale/static fallback on failure."""
        if credential is None or generation is None:
            return STATIC_CODEX_CATALOG
        now = self.clock()
        if (
            self._last_success is not None
            and self._cached_generation == generation
            and now - self._cached_at < self.ttl_seconds
        ):
            return self._last_success
        async with self._lock:
            now = self.clock()
            if (
                self._last_success is not None
                and self._cached_generation == generation
                and now - self._cached_at < self.ttl_seconds
            ):
                return self._last_success
            try:
                catalog, refreshed_credential = await self._fetch(credential)
            except (TimeoutError, OSError, ValueError):
                log.warning("Codex model discovery failed; using cached fallback", exc_info=True)
                return (
                    replace(self._last_success, source="stale")
                    if self._last_success is not None
                    else STATIC_CODEX_CATALOG
                )
            cached_generation = generation
            if refreshed_credential != credential and write_back is not None:
                try:
                    written_generation = await write_back(refreshed_credential)
                except Exception:  # noqa: BLE001 — discovery remains best-effort
                    log.warning("Codex catalog credential write-back failed", exc_info=True)
                else:
                    if written_generation is not None:
                        cached_generation = written_generation
            self._last_success = catalog
            self._cached_generation = cached_generation
            self._cached_at = now
            return catalog

    async def _fetch(self, credential: str) -> tuple[CodexCatalog, str]:
        with tempfile.TemporaryDirectory(prefix="symphony-codex-catalog-") as raw_home:
            codex_home = Path(raw_home)
            source_config = default_codex_credentials_path().parent / "config.toml"
            if source_config.is_file():
                shutil.copyfile(source_config, codex_home / "config.toml")
            pin_file_auth_storage(codex_home)
            auth_path = codex_home / "auth.json"
            auth_path.write_text(credential, encoding="utf-8")
            auth_path.chmod(0o600)
            env = dict(os.environ)
            env["CODEX_HOME"] = str(codex_home)
            proc = await asyncio.create_subprocess_exec(
                *self.command,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                env=env,
            )
            try:
                async with asyncio.timeout(self.timeout_seconds):
                    await self._request(
                        proc,
                        1,
                        "initialize",
                        {
                            "clientInfo": {
                                "name": "symphony",
                                "title": "Symphony",
                                "version": "0.0.1",
                            },
                            "capabilities": {},
                        },
                    )
                    await self._notify(proc, "initialized")
                    models: list[str] = []
                    efforts_by_model: dict[str, tuple[str, ...]] = {}
                    cursor: str | None = None
                    request_id = 2
                    while True:
                        result = await self._request(
                            proc,
                            request_id,
                            "model/list",
                            {"cursor": cursor, "includeHidden": False},
                        )
                        request_id += 1
                        data = result.get("data")
                        if not isinstance(data, list):
                            raise ValueError("Codex model/list returned no data")
                        for item in data:
                            parsed = _parse_model(item)
                            if parsed is None:
                                continue
                            model, efforts = parsed
                            if model not in efforts_by_model:
                                models.append(model)
                                efforts_by_model[model] = efforts
                        next_cursor = result.get("nextCursor")
                        if not isinstance(next_cursor, str) or not next_cursor:
                            break
                        cursor = next_cursor
                    if not models:
                        raise ValueError("Codex model/list returned an empty catalog")
                    refreshed = auth_path.read_text(encoding="utf-8")
                    return CodexCatalog(tuple(models), efforts_by_model), refreshed
            finally:
                if proc.stdin is not None:
                    proc.stdin.close()
                if proc.returncode is None:
                    with suppress(ProcessLookupError):
                        proc.terminate()
                    try:
                        await asyncio.wait_for(proc.wait(), timeout=0.5)
                    except TimeoutError:
                        with suppress(ProcessLookupError):
                            proc.kill()
                        await proc.wait()

    @staticmethod
    async def _notify(proc: asyncio.subprocess.Process, method: str) -> None:
        if proc.stdin is None:
            raise ValueError("Codex app-server has no stdio transport")
        proc.stdin.write((json.dumps({"method": method}) + "\n").encode())
        await proc.stdin.drain()

    @staticmethod
    async def _request(
        proc: asyncio.subprocess.Process,
        request_id: int,
        method: str,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        if proc.stdin is None or proc.stdout is None:
            raise ValueError("Codex app-server has no stdio transport")
        message = json.dumps({"id": request_id, "method": method, "params": params})
        proc.stdin.write((message + "\n").encode())
        await proc.stdin.drain()
        while True:
            line = await proc.stdout.readline()
            if not line:
                raise ValueError("Codex app-server exited before responding")
            try:
                response = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(response, dict) or response.get("id") != request_id:
                continue
            if "error" in response:
                raise ValueError(f"Codex app-server request failed: {response['error']!r}")
            result = response.get("result")
            if not isinstance(result, dict):
                raise ValueError("Codex app-server returned an invalid result")
            return result


def _parse_model(value: object) -> tuple[str, tuple[str, ...]] | None:
    if not isinstance(value, dict):
        return None
    model = value.get("model")
    if not isinstance(model, str) or not model:
        return None
    efforts: list[str] = []
    raw_efforts = value.get("supportedReasoningEfforts")
    if isinstance(raw_efforts, list):
        for raw in raw_efforts:
            if not isinstance(raw, dict):
                continue
            effort = raw.get("reasoningEffort", raw.get("effort"))
            if isinstance(effort, str) and effort:
                efforts.append(effort)
    return model, tuple(efforts)


codex_catalog_client = CodexCatalogClient()

__all__ = [
    "CodexCatalog",
    "CodexCatalogClient",
    "STATIC_CODEX_CATALOG",
    "codex_catalog_client",
]
