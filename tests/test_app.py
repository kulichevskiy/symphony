from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from symphony import db
from symphony.app import create_app
from symphony.webhook import WebhookSettings

from .test_webhook import NOW, SECRET, _body, _Handler, _headers, _payload


def _dist(tmp_path: Path) -> Path:
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "index.html").write_text(
        "<!doctype html><html><body>symphonyd UI v0</body></html>"
    )
    return dist


@pytest.mark.asyncio
async def test_ui_mount_serves_index_and_spa_fallback(tmp_path: Path) -> None:
    app = create_app(
        _Handler(),
        object(),  # type: ignore[arg-type]
        ui_enabled=True,
        ui_dist_dir=_dist(tmp_path),
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        root = await client.get("/ui/")
        nested = await client.get("/ui/some/nested/path")

    assert root.status_code == 200
    assert nested.status_code == 200
    assert "symphonyd UI v0" in root.text
    assert nested.text == root.text


@pytest.mark.asyncio
async def test_ui_disabled_skips_ui_and_api_mounts(tmp_path: Path) -> None:
    app = create_app(
        _Handler(),
        object(),  # type: ignore[arg-type]
        ui_enabled=False,
        ui_dist_dir=_dist(tmp_path),
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        ui = await client.get("/ui/")
        api = await client.get("/api/anything")

    assert ui.status_code == 404
    assert api.status_code == 404
    assert "/api/{path:path}" not in {route.path for route in app.routes}


@pytest.mark.asyncio
async def test_api_namespace_is_reserved_with_placeholder_404(tmp_path: Path) -> None:
    app = create_app(
        _Handler(),
        object(),  # type: ignore[arg-type]
        ui_enabled=True,
        ui_dist_dir=_dist(tmp_path),
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get("/api/issues")

    assert response.status_code == 404
    assert response.json() == {"detail": "Not Found"}
    assert "/api/{path:path}" in {route.path for route in app.routes}


@pytest.mark.asyncio
async def test_shared_app_preserves_linear_webhook_route(tmp_path: Path) -> None:
    conn = await db.connect(tmp_path / "state.sqlite")
    try:
        handler = _Handler()
        app = create_app(
            handler,
            conn,
            WebhookSettings(secret=SECRET),
            ui_enabled=True,
            ui_dist_dir=_dist(tmp_path),
            clock=lambda: NOW,
        )
        body = _body(_payload())

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/linear/webhook",
                content=body,
                headers=_headers(body),
            )
    finally:
        await conn.close()

    assert response.status_code == 200
    assert response.json()["handled"] is True
    assert len(handler.payloads) == 1
