# EventDesk

Small event-management application used as a realistic engineering project.

## Run backend

```bash
uv run uvicorn eventdesk.main:app --reload
```

The SQLite file defaults to `eventdesk.sqlite`. Set `EVENTDESK_DB_PATH` to isolate it.

## Backend checks

```bash
uv run pytest
uv run ruff check .
uv run mypy eventdesk
```

## Frontend checks

```bash
cd frontend
npm ci
npm test -- --run
npm run build
```

## Product baseline

Users can create events with a positive capacity and list events. The API contract is visible at
`/docs`; the React UI talks to the API at the same origin. Extend this baseline only through the
requirements in the linked BENCH tickets.

The demo operator login is `admin@example.com` / `eventdesk`. It sets an HTTP-only
`eventdesk_session` cookie and currently always redirects to `/`.
