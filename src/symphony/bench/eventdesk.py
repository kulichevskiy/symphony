from __future__ import annotations

import hashlib
import shutil
from dataclasses import dataclass
from importlib.resources import as_file, files
from pathlib import Path

_AFK = """
## Working agreement

Do not ask questions. This ticket is intentionally complete. Make the smallest production-quality
change that satisfies it, update tests, and preserve all existing behavior. Follow `README.md` and
`STANDARDS.md`. Do not weaken tests or branch protection.
"""


@dataclass(frozen=True)
class CampaignTicket:
    key: str
    title: str
    description: str
    blocked_by: tuple[str, ...] = ()


@dataclass(frozen=True)
class Campaign:
    name: str
    tickets: tuple[CampaignTicket, ...]


def materialize_eventdesk(destination: Path) -> None:
    """Copy the immutable EventDesk seed into a new trial repository."""
    if destination.exists():
        raise FileExistsError(destination)
    source = files("symphony.bench.assets").joinpath("eventdesk")
    with as_file(source) as source_path:
        shutil.copytree(source_path, destination)


def _ticket(key: str, title: str, body: str, *, blocked_by: tuple[str, ...] = ()) -> CampaignTicket:
    return CampaignTicket(
        key=key,
        title=title,
        description=f"{body.strip()}\n{_AFK.strip()}\n",
        blocked_by=blocked_by,
    )


def eventdesk_campaign() -> Campaign:
    """The fixed V1 live-E2E benchmark campaign."""
    return Campaign(
        name="EventDesk V1",
        tickets=(
            _ticket(
                "booking",
                "Add event booking API and UI",
                """
## Context

EventDesk currently creates and lists events. An event has `id`, `name`, `capacity`, and
`created_at`. Add the first attendee workflow without changing those fields.

## Requirements

- Persist bookings in SQLite with a stable unique id, `event_id`, trimmed attendee `name`, trimmed
  lowercase `email`, status `confirmed`, and UTC `created_at`.
- `POST /events/{event_id}/bookings` accepts `{\"name\": str, \"email\": str}` and returns the
  created booking with HTTP 201. Reject blank names and malformed email with 422. Return 404 for an
  unknown event. A given normalized email may book an event only once; return 409 on duplicate.
- `GET /events/{event_id}/bookings` returns bookings oldest first and 404 for an unknown event.
- Add a booking form and visible booking list to the selected event in the existing React UI. Show
  loading and server errors; refresh the list after success.
- Add schema initialization/migration that preserves existing event rows.

## Acceptance criteria

Creating a valid booking through either API or UI persists it and it appears in the list. Invalid,
duplicate, and unknown-event requests use the exact statuses above. Existing event create/list
behavior remains green.

## Verification

Add backend tests for success, normalization, ordering, validation, duplicate, and unknown event.
Add focused frontend tests for form submission, refresh, and an error response. Run every existing
backend/frontend check documented in the repository.
""",
            ),
            _ticket(
                "capacity",
                "Make booking capacity safe under concurrency",
                """
## Context

Confirmed bookings must never exceed an event's capacity, including when requests race. This is a
database correctness rule, not a UI-only check.

## Requirements

- Keep `POST /events/{event_id}/bookings` compatible for available events.
- Serialize the capacity check and confirmed insert in one SQLite transaction. Two processes may
  use separate database connections; a read followed by a later unprotected write is insufficient.
- If capacity is exhausted, return HTTP 409 with JSON detail `event is full` and do not insert.
- A duplicate email remains HTTP 409 and must not consume capacity.
- Configure bounded handling for transient SQLite lock contention; never turn it into an
  over-capacity insert.

## Acceptance criteria

For capacity 1, two simultaneous requests with different emails produce exactly one HTTP 201 and
one HTTP 409, and the database contains exactly one confirmed booking. The invariant also holds for
larger bursts and after process restart.

## Verification

Add a real concurrency test using separate requests/connections and a barrier so both attempts
overlap. Assert response counts and the persisted invariant. Keep all prior API/UI tests green.
""",
                blocked_by=("booking",),
            ),
            _ticket(
                "waitlist",
                "Waitlist attendees when an event is full",
                """
## Context

Full events should retain demand instead of rejecting new attendees. The capacity invariant from
the previous ticket still applies only to confirmed bookings.

## Requirements

- When no confirmed seat is available, `POST /events/{event_id}/bookings` returns HTTP 201 with a
  persisted booking whose status is `waitlisted` and whose `waitlist_position` is a positive
  integer.
- Positions are assigned atomically per event in arrival order and are never duplicated under
  concurrent requests. Confirmed bookings expose `waitlist_position: null`.
- `GET /events/{event_id}/bookings` returns confirmed bookings first (oldest first), then waitlisted
  bookings by ascending position.
- Duplicate normalized email remains forbidden across both statuses with HTTP 409.
- Expose status and waitlist position clearly in the existing UI.

## Acceptance criteria

After a capacity-1 event has one confirmed booking, the next two distinct attendees are persisted at
waitlist positions 1 and 2. Concurrent overflow requests receive unique contiguous positions. A
restart preserves positions and ordering.

## Verification

Add API/database concurrency tests for sequential and simultaneous overflow, ordering, duplicate
email across statuses, and persistence. Add a focused UI assertion for the displayed position.
""",
                blocked_by=("capacity",),
            ),
            _ticket(
                "cancellation",
                "Cancel bookings and promote the first waiter",
                """
## Context

Operators need an auditable cancellation flow. Releasing a confirmed seat must promote exactly the
first waiting attendee in the same transaction.

## Requirements

- `POST /bookings/{booking_id}/cancel` requires JSON `{\"reason\": str}`. Trim the reason; reject a
  blank reason with 422. Return 404 for an unknown booking and 409 if already cancelled.
- Persist status `cancelled`, `cancelled_at` in UTC, and `cancellation_reason`; never delete a
  booking.
- Cancelling a confirmed booking atomically promotes the lowest-position waitlisted booking to
  `confirmed`, clears its position, and compacts remaining waitlist positions to 1..N.
- Cancelling a waitlisted booking only compacts positions. Concurrent cancellations must not promote
  more attendees than released seats.
- The response contains the cancelled booking and `promoted_booking` (object or null).

## Acceptance criteria

With one confirmed attendee and waitlist positions 1 and 2, cancelling the confirmed attendee
promotes former position 1 and shifts former position 2 to position 1. The reason remains queryable.
Every transition is atomic after failures or races.

## Verification

Test confirmed and waitlisted cancellation, blank reason, missing/already-cancelled booking,
position compaction, audit fields, rollback safety, and two overlapping cancellations.
""",
                blocked_by=("waitlist",),
            ),
            _ticket(
                "return-to",
                "Return users to the requested page after login",
                """
## Context

EventDesk has a demo cookie login. Protected pages currently redirect to `/login` and lose the
original destination. The fixed demo credentials are `admin@example.com` / `eventdesk`; the form
fields are `email`, `password`, and hidden `return_to`. Preserve the destination without creating
an open redirect.

## Requirements

- Protect `/manage/events/{event_id}` and redirect anonymous requests to
  `/login?return_to=<urlencoded internal path including query>`.
- The login form carries `return_to` through failed attempts. Successful login redirects there.
- Accept only a path beginning with exactly one `/`. Reject absolute/protocol-relative URLs,
  backslashes, control characters, and paths beginning `//`; fall back to `/`.
- Do not trust a client-only check: validate on the server immediately before redirecting.
- Add a minimal management page showing the event and its bookings. Unknown events return 404 after
  authentication. Preserve existing APIs and UI.

## Acceptance criteria

An anonymous request for `/manage/events/1?tab=waitlist` returns to that exact internal destination
after login. Inputs such as `https://evil.test`, `//evil.test`, and encoded backslash/control
variants return to `/`. Failed login does not discard a valid destination.

## Verification

Add server-level tests for the happy path, query preservation, each unsafe class (including encoded
forms), failed login, unknown event, and a direct forged login submission.
""",
                blocked_by=("cancellation",),
            ),
            _ticket(
                "payment-webhook",
                "Handle payment webhooks idempotently and out of order",
                """
## Context

Payments arrive from an external provider at least once and not necessarily in chronological order.
The webhook must converge without regressing a terminal payment.

## Requirements

- `POST /webhooks/payments` accepts `id`, `booking_id`, `type`, and ISO-8601 `occurred_at`.
  Supported types are `payment.pending`, `payment.succeeded`, and `payment.failed`; reject others
  with 422.
- Authenticate the raw request body with `X-EventDesk-Signature`, lowercase hex HMAC-SHA256 using
  `PAYMENT_WEBHOOK_SECRET`. Missing/invalid signatures return 401 before mutation.
- Store every unique provider event id once. Exact replay returns HTTP 200 with `duplicate: true`
  and causes no second transition. Reuse of an id with different content returns 409.
- Persist payment state on the booking. Apply only an event newer than its recorded `occurred_at`.
  At equal time, `succeeded` outranks `failed`, which outranks `pending`. Once succeeded, older or
  equal lower-priority events cannot regress it. Valid ignored events still return 200.
- Unknown bookings return 404 and do not consume the provider event id.

## Acceptance criteria

Duplicate delivery is harmless; a newer success wins regardless of arrival order; a later pending
cannot regress success; equal-time precedence is deterministic; failed authentication and unknown
bookings leave all tables unchanged. State survives restart.

## Verification

Add tests using signatures computed over exact raw bytes for success, invalid/missing signature,
exact duplicate, conflicting duplicate id, every out-of-order combination, equal timestamps,
unknown booking, and restart persistence. Keep the complete suite green.
""",
                blocked_by=("return-to",),
            ),
        ),
    )


def harness_version(root: Path | None = None) -> str:
    """Stable identity of the campaign, seed application, and hidden grader."""
    package = files("symphony.bench")
    payload = b""
    with as_file(package) as packaged_root:
        seed_root = root or packaged_root
        ignored = {".ruff_cache", "__pycache__", "node_modules", "dist"}
        harness_files = sorted(
            path
            for path in seed_root.rglob("*")
            if path.is_file()
            and not ignored.intersection(path.relative_to(seed_root).parts)
            and path.suffix not in {".pyc", ".pyo"}
            and path.name != ".DS_Store"
            and path.name != ".version"
        )
        for path in harness_files:
            payload += str(path.relative_to(seed_root)).encode() + b"\0" + path.read_bytes()
    return hashlib.sha256(payload).hexdigest()[:16]
