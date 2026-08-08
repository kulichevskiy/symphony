from fastapi.testclient import TestClient

from support_queue.main import app


def test_health() -> None:
    assert TestClient(app).get("/health").json() == {"status": "ok"}
