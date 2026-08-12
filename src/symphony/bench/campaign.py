from __future__ import annotations

import hashlib
import os
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


def _materialize_seed(asset: str, destination: Path) -> None:
    if destination.exists():
        raise FileExistsError(destination)
    source = files("symphony.bench.assets").joinpath(asset)
    with as_file(source) as source_path:
        shutil.copytree(
            source_path,
            destination,
            ignore=shutil.ignore_patterns(
                ".venv", "node_modules", "dist", "__pycache__", "*.tsbuildinfo"
            ),
        )


def materialize_feedback_inbox(destination: Path) -> None:
    """Copy the immutable legacy Feedback Inbox seed."""
    _materialize_seed("feedback_inbox", destination)


def materialize_support_queue(destination: Path) -> None:
    """Copy the immutable Support Queue seed into a new trial repository."""
    _materialize_seed("support_queue", destination)


def materialize_private_control(source: Path, destination: Path) -> None:
    """Copy a private grader control into an isolated writable workspace."""
    if destination.exists():
        raise FileExistsError(destination)
    shutil.copytree(
        source,
        destination,
        ignore=shutil.ignore_patterns(
            ".venv",
            "node_modules",
            "dist",
            "__pycache__",
            ".pytest_cache",
            ".mypy_cache",
            ".ruff_cache",
            "*.tsbuildinfo",
        ),
    )
    destination.chmod(destination.stat().st_mode | 0o700)
    for directory in (path for path in destination.rglob("*") if path.is_dir()):
        directory.chmod(directory.stat().st_mode | 0o700)


def _ticket(key: str, title: str, body: str, *, blocked_by: tuple[str, ...] = ()) -> CampaignTicket:
    return CampaignTicket(
        key=key,
        title=title,
        description=f"{body.strip()}\n{_AFK.strip()}\n",
        blocked_by=blocked_by,
    )


def feedback_inbox_campaign() -> Campaign:
    """The fixed two-ticket V1 live-E2E benchmark campaign."""
    return Campaign(
        name="Feedback Inbox V1",
        tickets=(
            _ticket(
                "backend",
                "Build the feedback API",
                """
## Context

The seed has a FastAPI service and SQLite database but no feedback workflow. Build the complete
backend contract that the frontend ticket can consume.

## Requirements

- Persist feedback with stable integer `id`, trimmed `title`, optional `details`, `priority`,
  `status`, and UTC `created_at`/`updated_at`. Data must survive restart.
- `POST /feedback` accepts `title`, optional `details`, and priority `low`, `medium`, or `high`.
  Require 1..120 title characters after trimming, reject details over 2000 characters and unknown
  priorities with 422, create status `open`, and return HTTP 201.
- `GET /feedback` accepts optional `status=open|resolved`. Return matching records newest first,
  with `id` as stable tie-breaker. Reject unknown status values with 422.
- `PATCH /feedback/{id}` accepts only `{"status": "open"|"resolved"}`, returns the updated record,
  refreshes `updated_at`, returns 404 for an unknown id, and 422 for an invalid body.
- Keep the health endpoint and every documented backend check green.

## Acceptance criteria

A valid item can be created, listed, resolved, filtered, reopened, and read after application
restart. Invalid boundaries use the exact statuses above without mutating stored data.

## Verification

Test persistence, trimming and length boundaries, enum validation, deterministic ordering, both
filters, resolve/reopen, unknown ids, and invalid patch bodies. Run every documented backend check.
""",
            ),
            _ticket(
                "frontend",
                "Build the feedback inbox UI",
                """
## Context

The backend feedback contract is complete. Replace the placeholder React screen with the operator
inbox that uses those APIs.

## Requirements

- Provide a create form with title, optional details, and low/medium/high priority. Disable all
  controls while submitting so a double submit creates only one item. Clear after success and
  refresh the currently selected list.
- Provide Open and Resolved views. Load only the selected status and show title, optional details,
  priority, and status for every row.
- Allow open items to be resolved and resolved items to be reopened. Disable the affected action
  while pending and refresh the current view after success.
- Render distinct loading, empty, and error states. Errors must not also claim the list is empty and
  must offer Retry.
- Preserve documented frontend test, build, and accessibility conventions.

## Acceptance criteria

An operator can create feedback, see it in Open, resolve it, see it in Resolved, and reopen it.
Transient load failures recover through Retry. Slow mutations cannot be submitted twice.

## Verification

Test create, filtering, resolve/reopen, loading, empty, failed load plus retry, double-submit
prevention, and refresh after mutations. Run all documented frontend and repository checks.
""",
                blocked_by=("backend",),
            ),
        ),
    )


def support_queue_campaign() -> Campaign:
    """The fixed four-ticket Support Queue benchmark campaign."""
    return Campaign(
        name="Support Queue V1",
        tickets=(
            _ticket(
                "core",
                "Build the support queue core API",
                """
## Context

The seed contains a FastAPI health endpoint, SQLite wiring, and a placeholder React screen. Build
the persistent ticket and comment API that the other three tickets will extend.

## Requirements

- Persist tickets with integer `id`, trimmed `title`, optional `description`, priority
  `low|medium|high|urgent`, status `open`, integer `version`, and UTC timestamps.
- `POST /tickets` validates title 1..120 characters after trimming and description up to 4000
  characters, returns 201, and starts at version 1.
- `GET /tickets` filters by `status` and `priority`, returns newest first with stable id
  tie-breaking, and rejects invalid filter values with 422.
- `GET /tickets/{id}` returns one ticket with its comments or 404.
- `POST /tickets/{id}/comments` persists a trimmed 1..2000 character body and returns 201.
- `PATCH /tickets/{id}` updates validated title or description. `DELETE /tickets/{id}` returns 204
  and also deletes its comments.
- `PATCH` and `DELETE /tickets/{ticket_id}/comments/{comment_id}` update a validated body or return
  204. Unknown ticket/comment pairs return 404.
- Preserve data across application restarts and keep the health endpoint green.

## Acceptance criteria

A caller can create, filter, retrieve, comment on, and reload tickets without losing data. Invalid
input returns the documented status and never mutates storage.

## Verification

Test persistence, boundaries, enum validation, deterministic ordering, filters, complete ticket
and comment CRUD, unknown ids, and invalid bodies. Run every documented backend check.
""",
            ),
            _ticket(
                "workflow",
                "Add support workflow and permissions",
                """
## Context

The core ticket API exists. Add the state machine, assignment rules, permissions, and optimistic
concurrency that operators need while the frontend is being built independently.

## Requirements

- Support status transitions `open -> in_progress -> resolved` and `resolved -> open`; reject every
  other transition with 409.
- Extend the core `PATCH /tickets/{id}` contract to require current `version` and accept `status`,
  `assignee`, and `priority` alongside content fields. A stale version returns 409 without mutation;
  every successful update increments version exactly once.
- Only admins may edit ticket title or description, delete a ticket, assign another actor, or change
  priority. A comment author or admin may edit/delete it; other agents receive 403.
- Read actor identity and role from `X-Actor` and `X-Role: viewer|agent|admin`. Missing headers or a
  viewer may read but cannot mutate. Agents may comment, assign themselves, and transition tickets;
  only admins may assign another actor or change priority.
- Return 403 for permission failures, 404 for unknown tickets, and 422 for malformed requests.
- Apply permission, transition, and version checks inside the same database transaction as update.

## Acceptance criteria

Concurrent stale writes cannot overwrite newer data. Authorized actors can perform only their
documented operations, and invalid transitions or permissions leave the ticket unchanged.

## Verification

Test the complete transition matrix, viewer/agent/admin permissions, self/other assignment, full
ticket/comment CRUD, stale versions, atomic failure, and exact version increments.
""",
                blocked_by=("core",),
            ),
            _ticket(
                "frontend",
                "Build the support queue interface",
                """
## Context

The core API contract is available. Build the operator interface against that stable contract while
workflow and permissions are implemented in parallel.

## Requirements

- Show a Support Queue heading, create form, and ticket list with title, priority, status, assignee,
  version, and comment count.
- Provide status and priority filters. Load only the selected values and render distinct loading,
  empty, and error states; errors offer Retry and never also claim the queue is empty.
- Open a ticket detail view, show comments, and add a comment. Disable all controls for a pending
  form so double submission creates only one record.
- Use accessible labels, headings, status/alert roles, keyboard-operable controls, and meaningful
  action names.
- Preserve documented frontend test, build, and accessibility conventions.

## Acceptance criteria

An operator can create, filter, inspect, and comment on tickets. Slow requests cannot duplicate
mutations, and transient failures recover through Retry.

## Verification

Test create, filters, detail, comments, loading, empty, error/retry, pending-state deduplication,
accessible names, and refresh after mutations.
""",
                blocked_by=("core",),
            ),
            _ticket(
                "integration",
                "Integrate workflow, conflicts, and durable filters",
                """
## Context

The backend workflow and basic frontend now exist. Connect them into one robust end-to-end operator
flow and close the integration gaps that isolated tickets could not detect.

## Requirements

- Allow an agent to claim an unassigned ticket, advance valid statuses, resolve it, and reopen it;
  send `X-Actor`, `X-Role`, and the displayed version on every mutation.
- On a 409 stale-version response, keep the user's context, show an accessible conflict message,
  reload the current ticket, and require an explicit retry. Never silently overwrite.
- Store status and priority filters in URL search parameters. Initialize from valid URL values,
  update history when filters change, and ignore invalid values without crashing.
- Keep the selected detail and list synchronized after comment, assignment, and status mutations.
- Finish responsive and accessibility behavior: visible focus, live status text, named navigation,
  and no unlabeled interactive controls.

## Acceptance criteria

The full create-to-resolution flow works against the real API. Two stale browser views cannot lose
updates, URLs reproduce the same filtered queue, and keyboard/screen-reader users receive state and
conflict feedback.

## Verification

Test the full workflow, headers and versions, stale-conflict recovery, URL initialization/history,
invalid URL values, synchronized list/detail state, keyboard operation, and accessibility roles.
Run every repository check.
""",
                blocked_by=("workflow", "frontend"),
            ),
        ),
    )


def harness_version(root: Path | None = None) -> str:
    """Stable identity of the campaign, seed application, and hidden grader."""
    package = files("symphony.bench")
    with as_file(package) as packaged_root:
        seed_root = root or packaged_root
        ignored = {
            ".mypy_cache",
            ".pytest_cache",
            ".ruff_cache",
            "__pycache__",
            "node_modules",
            "dist",
        }
        if root is None:
            ignored.update(
                {
                    "feedback_inbox_reference",
                    "hidden",
                    "support_queue_reference",
                    "support_queue_mutations",
                }
            )
        harness_files: list[Path] = []
        for current, directories, names in os.walk(seed_root):
            directories[:] = sorted(name for name in directories if name not in ignored)
            current_path = Path(current)
            harness_files.extend(
                current_path / name
                for name in sorted(names)
                if Path(name).suffix not in {".pyc", ".pyo"}
                and name not in {".DS_Store", ".version"}
            )
        digest = hashlib.sha256()
        for path in harness_files:
            digest.update(str(path.relative_to(seed_root)).encode())
            digest.update(b"\0")
            digest.update(path.read_bytes())
    return digest.hexdigest()[:16]
