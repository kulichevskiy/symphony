import asyncio
from pathlib import Path

import httpx
import pytest
import respx
from fastapi.testclient import TestClient

from symphony.bench.executor import RemoteCommands, create_executor_app
from symphony.bench.github import CommandError


class FakeCommands:
    def __init__(self) -> None:
        self.calls: list[tuple[list[str], Path, dict[str, str], str | None]] = []

    async def run(
        self,
        argv: list[str],
        *,
        cwd: Path,
        env: dict[str, str] | None = None,
        stdin: str | None = None,
    ) -> str:
        self.calls.append((argv, cwd, env or {}, stdin))
        return "command output"


class BlockingCommands(FakeCommands):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.cancelled = False

    async def run(
        self,
        argv: list[str],
        *,
        cwd: Path,
        env: dict[str, str] | None = None,
        stdin: str | None = None,
    ) -> str:
        del argv, cwd, env, stdin
        self.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        return "unreachable"


def test_executor_requires_token_and_confines_working_directory(tmp_path: Path) -> None:
    commands = FakeCommands()
    app = create_executor_app(root=tmp_path, api_token="secret", commands=commands)
    with TestClient(app) as client:
        unauthorized = client.post("/commands", json={"argv": ["true"], "cwd": str(tmp_path)})
        assert unauthorized.status_code == 401
        escaped = client.post(
            "/commands",
            headers={"Authorization": "Bearer secret"},
            json={"argv": ["true"], "cwd": str(tmp_path.parent)},
        )
        assert escaped.status_code == 403
        completed = client.post(
            "/commands",
            headers={"Authorization": "Bearer secret"},
            json={
                "argv": ["tool", "arg"],
                "cwd": str(tmp_path),
                "env": {"GH_TOKEN": "token"},
                "stdin": "input",
            },
        )
    assert completed.status_code == 200
    assert completed.json() == {"stdout": "command output"}
    assert commands.calls == [(["tool", "arg"], tmp_path, {"GH_TOKEN": "token"}, "input")]


@pytest.mark.asyncio
@respx.mock
async def test_remote_commands_forwards_without_exposing_token_in_payload(tmp_path: Path) -> None:
    route = respx.post("http://executor:8090/commands").mock(
        return_value=httpx.Response(200, json={"stdout": "ok"})
    )
    commands = RemoteCommands(
        base_url="http://executor:8090", token="executor-secret", timeout_seconds=60
    )

    output = await commands.run(
        ["git", "status"], cwd=tmp_path, env={"GH_TOKEN": "github"}, stdin=None
    )

    assert output == "ok"
    request = route.calls[0].request
    assert request.headers["Authorization"] == "Bearer executor-secret"
    assert b"executor-secret" not in request.content


@pytest.mark.asyncio
@respx.mock
async def test_remote_commands_preserves_executor_error_detail(tmp_path: Path) -> None:
    respx.post("http://executor:8090/commands").mock(
        return_value=httpx.Response(
            422,
            json={"detail": "gh api repos/kulichevskiy/missing exited 1: HTTP 404"},
        )
    )
    commands = RemoteCommands(
        base_url="http://executor:8090", token="executor-secret", timeout_seconds=60
    )

    with pytest.raises(CommandError, match="HTTP 404"):
        await commands.run(["gh", "api", "repos/kulichevskiy/missing"], cwd=tmp_path)


@pytest.mark.asyncio
async def test_executor_cancel_all_cancels_active_command(tmp_path: Path) -> None:
    commands = BlockingCommands()
    app = create_executor_app(root=tmp_path, api_token="secret", commands=commands)
    transport = httpx.ASGITransport(app=app)
    headers = {"Authorization": "Bearer secret"}
    async with httpx.AsyncClient(transport=transport, base_url="http://executor") as client:
        running = asyncio.create_task(
            client.post(
                "/commands",
                headers=headers,
                json={"argv": ["sleep"], "cwd": str(tmp_path)},
            )
        )
        await commands.started.wait()
        cancelled = await client.post("/commands/cancel-all", headers=headers)
        with pytest.raises(asyncio.CancelledError):
            await running

    assert cancelled.json() == {"cancelled": 1}
    assert commands.cancelled is True
