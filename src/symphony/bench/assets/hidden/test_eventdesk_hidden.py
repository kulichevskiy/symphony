from __future__ import annotations

import hashlib
import hmac
import importlib
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier
from types import ModuleType
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def application(tmp_path: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    monkeypatch.setenv("EVENTDESK_DB_PATH", str(tmp_path / "eventdesk.sqlite"))
    monkeypatch.setenv("PAYMENT_WEBHOOK_SECRET", "hidden-secret")
    import eventdesk.main

    return importlib.reload(eventdesk.main)


def _event(client: TestClient, *, capacity: int = 2) -> int:
    response = client.post("/events", json={"name": "Hidden summit", "capacity": capacity})
    assert response.status_code == 201, response.text
    return int(response.json()["id"])


def _book(client: TestClient, event_id: int, index: int = 1):
    return client.post(
        f"/events/{event_id}/bookings",
        json={"name": f" Person {index} ", "email": f" PERSON{index}@Example.COM "},
    )


def _signed(client: TestClient, payload: dict[str, object]):
    body = json.dumps(payload, separators=(",", ":")).encode()
    signature = hmac.new(b"hidden-secret", body, hashlib.sha256).hexdigest()
    return client.post(
        "/webhooks/payments",
        content=body,
        headers={
            "content-type": "application/json",
            "X-EventDesk-Signature": signature,
        },
    )


def test_booking_contract_normalizes_and_rejects_duplicates(application: ModuleType) -> None:
    with TestClient(application.app) as client:
        event_id = _event(client)
        created = _book(client, event_id)
        assert created.status_code == 201, created.text
        assert created.json()["name"] == "Person 1"
        assert created.json()["email"] == "person1@example.com"
        assert created.json()["status"] == "confirmed"
        assert created.json()["waitlist_position"] is None
        assert _book(client, event_id).status_code == 409
        listed = client.get(f"/events/{event_id}/bookings")
        assert listed.status_code == 200
        assert [row["id"] for row in listed.json()] == [created.json()["id"]]


def test_booking_validation_and_unknown_event_contract(application: ModuleType) -> None:
    with TestClient(application.app) as client:
        event_id = _event(client)
        assert (
            client.post(
                f"/events/{event_id}/bookings", json={"name": "  ", "email": "a@example.com"}
            ).status_code
            == 422
        )
        assert (
            client.post(
                f"/events/{event_id}/bookings", json={"name": "Ada", "email": "not-email"}
            ).status_code
            == 422
        )
        assert _book(client, 999_999).status_code == 404
        assert client.get("/events/999999/bookings").status_code == 404


def test_waitlist_and_cancellation_preserve_order(application: ModuleType) -> None:
    with TestClient(application.app) as client:
        event_id = _event(client, capacity=1)
        first = _book(client, event_id, 1).json()
        second = _book(client, event_id, 2).json()
        third = _book(client, event_id, 3).json()
        assert (first["status"], first["waitlist_position"]) == ("confirmed", None)
        assert (second["status"], second["waitlist_position"]) == ("waitlisted", 1)
        assert (third["status"], third["waitlist_position"]) == ("waitlisted", 2)

        cancelled = client.post(
            f"/bookings/{first['id']}/cancel", json={"reason": " attendee request "}
        )
        assert cancelled.status_code == 200, cancelled.text
        body = cancelled.json()
        assert body["cancelled_booking"]["status"] == "cancelled"
        assert body["cancelled_booking"]["cancellation_reason"] == "attendee request"
        assert body["promoted_booking"]["id"] == second["id"]
        rows = client.get(f"/events/{event_id}/bookings").json()
        by_id = {row["id"]: row for row in rows}
        assert by_id[second["id"]]["status"] == "confirmed"
        assert by_id[second["id"]]["waitlist_position"] is None
        assert by_id[third["id"]]["waitlist_position"] == 1


def test_waitlisted_cancellation_compacts_without_promotion(application: ModuleType) -> None:
    with TestClient(application.app) as client:
        event_id = _event(client, capacity=1)
        confirmed = _book(client, event_id, 1).json()
        first_waiter = _book(client, event_id, 2).json()
        second_waiter = _book(client, event_id, 3).json()
        response = client.post(
            f"/bookings/{first_waiter['id']}/cancel", json={"reason": "duplicate"}
        )
        assert response.status_code == 200, response.text
        assert response.json()["promoted_booking"] is None
        rows = client.get(f"/events/{event_id}/bookings").json()
        by_id = {row["id"]: row for row in rows}
        assert by_id[confirmed["id"]]["status"] == "confirmed"
        assert by_id[second_waiter["id"]]["waitlist_position"] == 1
        assert (
            client.post(
                f"/bookings/{first_waiter['id']}/cancel", json={"reason": "again"}
            ).status_code
            == 409
        )
        assert client.post("/bookings/999999/cancel", json={"reason": "missing"}).status_code == 404
        assert (
            client.post(f"/bookings/{confirmed['id']}/cancel", json={"reason": "  "}).status_code
            == 422
        )


def test_concurrent_overflow_has_one_seat_and_unique_positions(
    application: ModuleType,
) -> None:
    with TestClient(application.app) as setup:
        event_id = _event(setup, capacity=1)
    barrier = Barrier(7)

    def attempt(index: int) -> dict[str, object]:
        with TestClient(application.app) as client:
            barrier.wait(timeout=10)
            response = _book(client, event_id, index)
            assert response.status_code == 201, response.text
            result: dict[str, object] = response.json()
            return result

    with ThreadPoolExecutor(max_workers=7) as pool:
        rows = list(pool.map(attempt, range(1, 8)))
    confirmed = [row for row in rows if row["status"] == "confirmed"]
    waitlisted = [row for row in rows if row["status"] == "waitlisted"]
    assert len(confirmed) == 1
    assert sorted(row["waitlist_position"] for row in waitlisted) == list(range(1, 7))


def test_concurrent_cancellations_promote_only_released_seats(application: ModuleType) -> None:
    with TestClient(application.app) as setup:
        event_id = _event(setup, capacity=2)
        bookings = [_book(setup, event_id, index).json() for index in range(1, 6)]
    barrier = Barrier(2)

    def cancel(booking_id: int) -> int:
        with TestClient(application.app) as client:
            barrier.wait(timeout=10)
            response = client.post(f"/bookings/{booking_id}/cancel", json={"reason": "concurrent"})
            return response.status_code

    with ThreadPoolExecutor(max_workers=2) as pool:
        statuses = list(pool.map(cancel, [bookings[0]["id"], bookings[1]["id"]]))
    assert statuses == [200, 200]
    with TestClient(application.app) as client:
        rows = client.get(f"/events/{event_id}/bookings").json()
    assert sum(row["status"] == "confirmed" for row in rows) == 2
    assert sum(row["status"] == "cancelled" for row in rows) == 2
    waitlisted = [row for row in rows if row["status"] == "waitlisted"]
    assert [row["waitlist_position"] for row in waitlisted] == [1]


@pytest.mark.parametrize(
    "unsafe",
    [
        "https://evil.test/x",
        "//evil.test/x",
        "/\\evil",
        "/%5Cevil",
        "%2F%2Fevil.test",
        "/line\nfeed",
        "\\evil",
    ],
)
def test_login_rejects_unsafe_return_targets(application: ModuleType, unsafe: str) -> None:
    with TestClient(application.app, follow_redirects=False) as client:
        response = client.post(
            "/login",
            data={
                "email": "admin@example.com",
                "password": "eventdesk",
                "return_to": unsafe,
            },
        )
        assert response.status_code == 303
        assert response.headers["location"] == "/"


def test_login_round_trip_preserves_internal_path_and_query(application: ModuleType) -> None:
    with TestClient(application.app, follow_redirects=False) as client:
        event_id = _event(client)
        client.cookies.clear()
        requested = f"/manage/events/{event_id}?tab=waitlist"
        response = client.get(requested)
        assert response.status_code in {302, 303, 307, 308}
        location = response.headers["location"]
        assert urlsplit(location).path == "/login"
        return_to = parse_qs(urlsplit(location).query)["return_to"][0]
        assert return_to == requested
        logged_in = client.post(
            "/login",
            data={
                "email": "admin@example.com",
                "password": "eventdesk",
                "return_to": return_to,
            },
        )
        assert logged_in.status_code == 303
        assert logged_in.headers["location"] == requested


def test_payment_webhook_is_idempotent_and_success_is_terminal(application: ModuleType) -> None:
    with TestClient(application.app) as client:
        event_id = _event(client)
        booking = _book(client, event_id).json()
        now = datetime.now(UTC)
        success = {
            "id": "evt-success",
            "booking_id": booking["id"],
            "type": "payment.succeeded",
            "occurred_at": now.isoformat(),
        }
        first = _signed(client, success)
        assert first.status_code == 200, first.text
        duplicate = _signed(client, success)
        assert duplicate.status_code == 200
        assert duplicate.json()["duplicate"] is True
        pending = {
            "id": "evt-later-pending",
            "booking_id": booking["id"],
            "type": "payment.pending",
            "occurred_at": (now + timedelta(minutes=1)).isoformat(),
        }
        assert _signed(client, pending).status_code == 200
        current = client.get(f"/events/{event_id}/bookings").json()[0]
        assert current["payment_status"] == "succeeded"

        conflict = {**success, "type": "payment.failed"}
        assert _signed(client, conflict).status_code == 409


def test_payment_webhook_auth_and_equal_time_precedence(application: ModuleType) -> None:
    with TestClient(application.app) as client:
        event_id = _event(client)
        booking = _book(client, event_id).json()
        occurred_at = datetime.now(UTC).isoformat()
        failed = {
            "id": "evt-failed",
            "booking_id": booking["id"],
            "type": "payment.failed",
            "occurred_at": occurred_at,
        }
        raw = json.dumps(failed, separators=(",", ":")).encode()
        assert (
            client.post(
                "/webhooks/payments", content=raw, headers={"content-type": "application/json"}
            ).status_code
            == 401
        )
        assert (
            client.post(
                "/webhooks/payments",
                content=raw,
                headers={"content-type": "application/json", "X-EventDesk-Signature": "0" * 64},
            ).status_code
            == 401
        )
        assert _signed(client, failed).status_code == 200
        succeeded = {
            **failed,
            "id": "evt-succeeded",
            "type": "payment.succeeded",
        }
        assert _signed(client, succeeded).status_code == 200
        older = {
            **failed,
            "id": "evt-older",
            "occurred_at": (datetime.fromisoformat(occurred_at) - timedelta(days=1)).isoformat(),
        }
        assert _signed(client, older).status_code == 200
        current = client.get(f"/events/{event_id}/bookings").json()[0]
        assert current["payment_status"] == "succeeded"


def test_unknown_booking_does_not_consume_webhook_id(application: ModuleType) -> None:
    with TestClient(application.app) as client:
        event_id = _event(client)
        booking = _book(client, event_id).json()
        payload = {
            "id": "evt-reusable",
            "booking_id": 999_999,
            "type": "payment.pending",
            "occurred_at": datetime.now(UTC).isoformat(),
        }
        assert _signed(client, payload).status_code == 404
        payload["booking_id"] = booking["id"]
        assert _signed(client, payload).status_code == 200


def test_booking_state_survives_application_restart(application: ModuleType) -> None:
    with TestClient(application.app) as client:
        event_id = _event(client, capacity=1)
        booking = _book(client, event_id).json()
        payload = {
            "id": "evt-persisted",
            "booking_id": booking["id"],
            "type": "payment.succeeded",
            "occurred_at": datetime.now(UTC).isoformat(),
        }
        assert _signed(client, payload).status_code == 200

    restarted = importlib.reload(application)
    with TestClient(restarted.app) as client:
        rows = client.get(f"/events/{event_id}/bookings")
        assert rows.status_code == 200
        assert rows.json()[0]["id"] == booking["id"]
        assert rows.json()[0]["payment_status"] == "succeeded"
