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


def materialize_feedback_inbox(destination: Path) -> None:
    """Copy the immutable Feedback Inbox seed into a new trial repository."""
    if destination.exists():
        raise FileExistsError(destination)
    source = files("symphony.bench.assets").joinpath("feedback_inbox")
    with as_file(source) as source_path:
        shutil.copytree(
            source_path,
            destination,
            ignore=shutil.ignore_patterns(
                ".venv", "node_modules", "dist", "__pycache__", "*.tsbuildinfo"
            ),
        )


def materialize_feedback_inbox_reference(source: Path, destination: Path) -> None:
    """Copy the private known-good implementation used only by grader preflight."""
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


def harness_version(root: Path | None = None) -> str:
    """Stable identity of the campaign, seed application, and hidden grader."""
    package = files("symphony.bench")
    payload = b""
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
