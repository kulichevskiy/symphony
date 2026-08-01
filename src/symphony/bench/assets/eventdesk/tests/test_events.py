from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from eventdesk.main import app


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    monkeypatch.setenv("EVENTDESK_DB_PATH", str(tmp_path / "test.sqlite"))
    with TestClient(app) as test_client:
        yield test_client


def test_create_and_list_events(client: TestClient) -> None:
    created = client.post("/events", json={"name": "  Launch  ", "capacity": 20})

    assert created.status_code == 201
    assert created.json()["name"] == "Launch"
    assert created.json()["capacity"] == 20
    assert client.get("/events").json() == [created.json()]


@pytest.mark.parametrize("payload", [{"name": "", "capacity": 2}, {"name": "x", "capacity": 0}])
def test_invalid_event(payload: dict[str, object], client: TestClient) -> None:
    assert client.post("/events", json=payload).status_code == 422


def test_demo_login_sets_operator_cookie(client: TestClient) -> None:
    response = client.post(
        "/login",
        data={"email": "admin@example.com", "password": "eventdesk", "return_to": "/events"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/"
    assert "eventdesk_session=admin" in response.headers["set-cookie"]
