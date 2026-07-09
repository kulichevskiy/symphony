from __future__ import annotations

from typing import Any

import httpx
import pytest
import respx

from symphony.app import create_app
from symphony.config import Config, RepoBinding, RoleConfig, TrackerStates

from .test_auth import JWKS_URI, _client, _jwks, _settings, _token
from .test_webhook import _Handler

SECRET = "s3cr3t-should-never-leak"


def _config() -> Config:
    return Config(
        global_max_concurrent=7,
        poll_interval_secs=42,
        linear_api_key=SECRET,
        telegram_bot_token=SECRET,
        repos=[
            RepoBinding(
                project_key="SYM",
                github_repo="org/symphony",
                agent="codex",
                max_concurrent=3,
                webhook_secret=SECRET,
                env={"MY_TOKEN": SECRET},
                roles={"review_find": RoleConfig(agent="claude", model="opus", effort="high")},
                states=TrackerStates(ready="Ready"),
            )
        ],
    )


def _app() -> Any:
    return create_app(
        _Handler(),
        object(),  # type: ignore[arg-type]
        ui_enabled=True,
        ui_external_config=_config(),
        auth0_settings=_settings(),
    )


@pytest.mark.asyncio
@respx.mock
async def test_config_without_token_is_401() -> None:
    respx.get(JWKS_URI).mock(return_value=httpx.Response(200, json=_jwks()))
    async with _client(_app()) as client:
        resp = await client.get("/api/config")
    assert resp.status_code == 401


@pytest.mark.asyncio
@respx.mock
async def test_config_returns_bindings_roles_and_caps() -> None:
    respx.get(JWKS_URI).mock(return_value=httpx.Response(200, json=_jwks()))
    async with _client(_app()) as client:
        resp = await client.get("/api/config", headers={"Authorization": f"Bearer {_token()}"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["read_only"] is True
    assert body["global_max_concurrent"] == 7
    assert len(body["bindings"]) == 1
    binding = body["bindings"][0]
    assert binding["project_key"] == "SYM"
    assert binding["github_repo"] == "org/symphony"
    assert binding["max_concurrent"] == 3
    assert binding["roles"]["review_find"] == {
        "agent": "claude",
        "model": "opus",
        "effort": "high",
    }
    # A role with no override resolves from the binding's legacy agent.
    assert binding["roles"]["implement"]["agent"] == "codex"


@pytest.mark.asyncio
@respx.mock
async def test_config_redacts_secrets() -> None:
    respx.get(JWKS_URI).mock(return_value=httpx.Response(200, json=_jwks()))
    async with _client(_app()) as client:
        resp = await client.get("/api/config", headers={"Authorization": f"Bearer {_token()}"})
    assert resp.status_code == 200
    assert SECRET not in resp.text
