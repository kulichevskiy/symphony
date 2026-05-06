"""SQLite append-only event log for Symphony runtime state.

The event log intentionally has one stable table. New runtime information is
represented as new ``kind`` strings or payload keys, not schema changes.
"""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


TERMINAL_KINDS = {"merge", "auto-stuck", "run-terminal"}
ACTIVE_KINDS = {
    "dispatch",
    "agent-start",
    "review-fresh",
    "review-verdict",
    "push",
    "pr-open",
}


@dataclass(frozen=True)
class Event:
    id: int
    ts: int
    issue_number: int | None
    run_id: str
    kind: str
    payload: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "ts": self.ts,
            "issue_number": self.issue_number,
            "run_id": self.run_id,
            "kind": self.kind,
            "payload": self.payload,
        }

    def to_json_line(self) -> str:
        return json.dumps(self.as_dict(), sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class ReviewReplay:
    rounds_used: int
    last_reviewed_sha: str
    last_review_verdict: str


@dataclass(frozen=True)
class InFlightStatus:
    issue_number: int
    run_id: str
    round: int
    elapsed_s: int
    latest_sha: str
    last_reviewed_sha: str
    last_review_verdict: str


@dataclass(frozen=True)
class TerminalRun:
    issue_number: int
    run_id: str
    outcome: str
    rounds: int
    total_elapsed_s: int


@dataclass(frozen=True)
class StatusSnapshot:
    in_flight: list[InFlightStatus]
    terminal_runs: list[TerminalRun]


class EventLog:
    def __init__(self, db_path: Path):
        self.db_path = db_path

    @classmethod
    def for_repo(cls, repo_path: Path) -> "EventLog":
        return cls(repo_path / ".symphony" / "events.db")

    def ensure(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY,
                    ts INTEGER NOT NULL,
                    issue_number INTEGER,
                    run_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )

    def emit(
        self,
        kind: str,
        *,
        issue_number: int | None = None,
        run_id: str = "",
        payload: dict[str, Any] | None = None,
        ts: int | None = None,
    ) -> Event:
        self.ensure()
        payload_json = json.dumps(
            payload or {}, sort_keys=True, default=_json_default
        )
        event_ts = int(time.time()) if ts is None else int(ts)
        with self._connect() as con:
            cur = con.execute(
                """
                INSERT INTO events (ts, issue_number, run_id, kind, payload_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (event_ts, issue_number, run_id, kind, payload_json),
            )
            event_id = int(cur.lastrowid)
        return Event(
            id=event_id,
            ts=event_ts,
            issue_number=issue_number,
            run_id=run_id,
            kind=kind,
            payload=json.loads(payload_json),
        )

    def iter_events(
        self,
        *,
        issue_number: int | None = None,
        after_id: int = 0,
        limit: int | None = None,
    ) -> list[Event]:
        self.ensure()
        where = ["id > ?"]
        params: list[Any] = [after_id]
        if issue_number is not None:
            where.append("issue_number = ?")
            params.append(issue_number)
        sql = (
            "SELECT id, ts, issue_number, run_id, kind, payload_json "
            f"FROM events WHERE {' AND '.join(where)} ORDER BY id ASC"
        )
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        with self._connect() as con:
            rows = con.execute(sql, params).fetchall()
        return [self._row_to_event(row) for row in rows]

    def tail_events(
        self,
        *,
        issue_number: int | None = None,
        limit: int = 100,
    ) -> list[Event]:
        self.ensure()
        where: list[str] = []
        params: list[Any] = []
        if issue_number is not None:
            where.append("issue_number = ?")
            params.append(issue_number)
        sql = "SELECT id, ts, issue_number, run_id, kind, payload_json FROM events"
        if where:
            sql += f" WHERE {' AND '.join(where)}"
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        with self._connect() as con:
            rows = con.execute(sql, params).fetchall()
        return [self._row_to_event(row) for row in reversed(rows)]

    def replay_review(self, issue_number: int) -> ReviewReplay:
        rounds_used = 0
        last_reviewed_sha = ""
        last_review_verdict = ""
        for ev in self.iter_events(issue_number=issue_number):
            payload = ev.payload
            if ev.kind == "review-fresh":
                last_reviewed_sha = payload.get("head_sha", last_reviewed_sha)
            elif ev.kind == "review-verdict":
                last_reviewed_sha = payload.get("head_sha", last_reviewed_sha)
                last_review_verdict = payload.get("verdict", last_review_verdict)
            elif ev.kind == "agent-exit" and payload.get("phase") == "review":
                if payload.get("success"):
                    rounds_used = max(rounds_used, int(payload.get("round", 0)))
            elif ev.kind in TERMINAL_KINDS:
                rounds_used = int(payload.get("rounds_used", rounds_used))
        return ReviewReplay(
            rounds_used=rounds_used,
            last_reviewed_sha=last_reviewed_sha,
            last_review_verdict=last_review_verdict,
        )

    def status_snapshot(
        self,
        *,
        now_ts: int | None = None,
        terminal_window_s: int = 24 * 60 * 60,
    ) -> StatusSnapshot:
        now = int(time.time()) if now_ts is None else int(now_ts)
        active: dict[int, dict[str, Any]] = {}
        terminal: list[TerminalRun] = []
        for ev in self.iter_events():
            if ev.issue_number is None:
                continue
            issue = ev.issue_number
            payload = ev.payload
            if ev.kind in ("dispatch", "agent-start"):
                active.setdefault(
                    issue,
                    {
                        "run_id": ev.run_id,
                        "started_ts": ev.ts,
                        "round": 0,
                        "latest_sha": "",
                        "last_reviewed_sha": "",
                        "last_review_verdict": "",
                    },
                )
            if issue not in active and ev.kind not in ACTIVE_KINDS | TERMINAL_KINDS:
                continue
            state = active.setdefault(
                issue,
                {
                    "run_id": ev.run_id,
                    "started_ts": ev.ts,
                    "round": 0,
                    "latest_sha": "",
                    "last_reviewed_sha": "",
                    "last_review_verdict": "",
                },
            )
            if ev.run_id:
                state["run_id"] = ev.run_id
            if "round" in payload:
                state["round"] = max(state["round"], int(payload["round"]))
            if "rounds_used" in payload:
                state["round"] = max(state["round"], int(payload["rounds_used"]))
            sha = payload.get("head_sha") or payload.get("sha")
            if sha:
                state["latest_sha"] = sha
            if ev.kind in ("review-fresh", "review-verdict"):
                if sha:
                    state["last_reviewed_sha"] = sha
                if "verdict" in payload:
                    state["last_review_verdict"] = payload["verdict"]
            if ev.kind in TERMINAL_KINDS:
                total_elapsed = ev.ts - int(state.get("started_ts", ev.ts))
                if ev.ts >= now - terminal_window_s:
                    terminal.append(
                        TerminalRun(
                            issue_number=issue,
                            run_id=state.get("run_id", ev.run_id),
                            outcome=payload.get("outcome", ev.kind),
                            rounds=int(payload.get("rounds_used", state["round"])),
                            total_elapsed_s=max(0, total_elapsed),
                        )
                    )
                active.pop(issue, None)

        in_flight = [
            InFlightStatus(
                issue_number=issue,
                run_id=state.get("run_id", ""),
                round=int(state.get("round", 0)),
                elapsed_s=max(0, now - int(state.get("started_ts", now))),
                latest_sha=state.get("latest_sha", ""),
                last_reviewed_sha=state.get("last_reviewed_sha", ""),
                last_review_verdict=state.get("last_review_verdict", ""),
            )
            for issue, state in sorted(active.items())
        ]
        return StatusSnapshot(in_flight=in_flight, terminal_runs=terminal)

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    @staticmethod
    def _row_to_event(row: sqlite3.Row | tuple[Any, ...]) -> Event:
        event_id, ts, issue_number, run_id, kind, payload_json = row
        return Event(
            id=int(event_id),
            ts=int(ts),
            issue_number=int(issue_number) if issue_number is not None else None,
            run_id=run_id,
            kind=kind,
            payload=json.loads(payload_json),
        )


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
