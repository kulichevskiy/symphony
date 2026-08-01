from __future__ import annotations

import asyncio
import contextlib
import secrets
from pathlib import Path
from typing import Annotated

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, status
from pydantic import BaseModel, Field

from .github import CommandError, Commands, SubprocessCommands


class CommandRequest(BaseModel):
    argv: list[str] = Field(min_length=1)
    cwd: str
    env: dict[str, str] = Field(default_factory=dict)
    stdin: str | None = None
    timeout_seconds: float = Field(default=2 * 60 * 60, ge=1, le=8 * 60 * 60)


class CommandResponse(BaseModel):
    stdout: str


def create_executor_app(*, root: Path, api_token: str, commands: Commands | None = None) -> FastAPI:
    if not api_token:
        raise ValueError("executor API token must not be empty")
    root = root.resolve()
    runner = commands or SubprocessCommands()
    app = FastAPI(title="Symphony Bench Executor", docs_url=None, redoc_url=None)
    active: set[asyncio.Task[str]] = set()

    def require_token(authorization: Annotated[str | None, Header()] = None) -> None:
        if authorization is None or not secrets.compare_digest(
            authorization, f"Bearer {api_token}"
        ):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/commands", response_model=CommandResponse, dependencies=[Depends(require_token)])
    async def execute(request: CommandRequest) -> CommandResponse:
        cwd = await asyncio.to_thread(Path(request.cwd).resolve)
        if not cwd.is_relative_to(root):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="working directory is outside executor root",
            )
        try:
            task = asyncio.create_task(
                runner.run(
                    request.argv,
                    cwd=cwd,
                    env=request.env,
                    stdin=request.stdin,
                )
            )
            active.add(task)
            try:
                stdout = await asyncio.wait_for(task, timeout=request.timeout_seconds)
            finally:
                active.discard(task)
        except TimeoutError as exc:
            raise HTTPException(
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                detail="command timed out and was terminated",
            ) from exc
        except CommandError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)
            ) from exc
        return CommandResponse(stdout=stdout)

    @app.post("/commands/cancel-all", dependencies=[Depends(require_token)])
    async def cancel_all() -> dict[str, int]:
        tasks = list(active)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        return {"cancelled": len(tasks)}

    return app


class RemoteCommands:
    def __init__(self, *, base_url: str, token: str, timeout_seconds: float) -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._timeout = timeout_seconds

    async def run(
        self,
        argv: list[str],
        *,
        cwd: Path,
        env: dict[str, str] | None = None,
        stdin: str | None = None,
    ) -> str:
        try:
            async with httpx.AsyncClient(timeout=self._timeout + 30) as client:
                response = await client.post(
                    f"{self._base_url}/commands",
                    headers={"Authorization": f"Bearer {self._token}"},
                    json={
                        "argv": argv,
                        "cwd": str(cwd),
                        "env": env or {},
                        "stdin": stdin,
                        "timeout_seconds": self._timeout,
                    },
                )
                response.raise_for_status()
        except asyncio.CancelledError:
            with contextlib.suppress(CommandError):
                await self.cancel_all()
            raise
        except httpx.HTTPError as exc:
            raise CommandError(f"remote command failed: {exc}") from exc
        body = response.json()
        stdout = body.get("stdout") if isinstance(body, dict) else None
        if not isinstance(stdout, str):
            raise CommandError("remote executor returned invalid output")
        return stdout

    async def cancel_all(self) -> None:
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.post(
                    f"{self._base_url}/commands/cancel-all",
                    headers={"Authorization": f"Bearer {self._token}"},
                )
                response.raise_for_status()
        except httpx.HTTPError as exc:
            raise CommandError(f"remote command cancellation failed: {exc}") from exc
