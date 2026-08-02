# Feedback Inbox

Small FastAPI, SQLite, and React application used by the Symphony verification kit.

## Run backend

```bash
uv run uvicorn feedback_inbox.main:app --reload
```

The SQLite file should be selected with `FEEDBACK_INBOX_DB_PATH` once persistence is implemented.

## Backend checks

```bash
uv run pytest
uv run ruff check .
uv run mypy feedback_inbox
```

## Frontend checks

```bash
cd frontend
npm ci
npm test -- --run
npm run build
```

## Product baseline

The seed exposes `GET /health` and a placeholder React screen. Implement only the behavior in the
linked BENCH tickets. Keep `feedback_inbox.main:app` and the exported React `App` as public entry
points so the documented commands remain valid.
