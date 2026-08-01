from __future__ import annotations

import os
import sqlite3
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from html import escape
from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, Form, HTTPException, status
from pydantic import BaseModel, Field
from starlette.responses import HTMLResponse, RedirectResponse


class EventCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    capacity: int = Field(ge=1, le=100_000)


class Event(EventCreate):
    id: int
    created_at: datetime


def _db_path() -> Path:
    return Path(os.environ.get("EVENTDESK_DB_PATH", "eventdesk.sqlite"))


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def _initialize() -> None:
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                capacity INTEGER NOT NULL CHECK (capacity > 0),
                created_at TEXT NOT NULL
            )
            """
        )


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    _initialize()
    yield


app = FastAPI(title="EventDesk", lifespan=lifespan)


def database() -> Iterator[sqlite3.Connection]:
    with _connect() as conn:
        yield conn


Db = Annotated[sqlite3.Connection, Depends(database)]


@app.get("/login", response_class=HTMLResponse)
def login_page(return_to: str = "") -> str:
    escaped_return_to = escape(return_to, quote=True)
    return f"""
    <!doctype html>
    <title>EventDesk login</title>
    <h1>Operator login</h1>
    <form method="post" action="/login">
      <input name="email" type="email" required>
      <input name="password" type="password" required>
      <input name="return_to" type="hidden" value="{escaped_return_to}">
      <button type="submit">Log in</button>
    </form>
    """


@app.post("/login", response_model=None)
def login(
    email: Annotated[str, Form()],
    password: Annotated[str, Form()],
    return_to: Annotated[str, Form()] = "",
) -> RedirectResponse | HTMLResponse:
    if email != "admin@example.com" or password != "eventdesk":
        return HTMLResponse(login_page(return_to), status_code=status.HTTP_401_UNAUTHORIZED)
    response = RedirectResponse("/", status_code=status.HTTP_303_SEE_OTHER)
    response.set_cookie("eventdesk_session", "admin", httponly=True, samesite="lax")
    return response


@app.post("/events", response_model=Event, status_code=status.HTTP_201_CREATED)
def create_event(request: EventCreate, conn: Db) -> Event:
    name = request.name.strip()
    if not name:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY)
    created_at = datetime.now(UTC)
    cursor = conn.execute(
        "INSERT INTO events (name, capacity, created_at) VALUES (?, ?, ?)",
        (name, request.capacity, created_at.isoformat()),
    )
    conn.commit()
    event_id = cursor.lastrowid
    if event_id is None:  # pragma: no cover - SQLite INSERT always assigns one
        raise RuntimeError("SQLite did not return an event id")
    return Event(id=event_id, name=name, capacity=request.capacity, created_at=created_at)


@app.get("/events", response_model=list[Event])
def list_events(conn: Db) -> list[Event]:
    rows = conn.execute(
        "SELECT id, name, capacity, created_at FROM events ORDER BY created_at, id"
    ).fetchall()
    return [Event.model_validate(dict(row)) for row in rows]
