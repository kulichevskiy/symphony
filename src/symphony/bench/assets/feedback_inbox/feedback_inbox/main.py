from __future__ import annotations

from fastapi import FastAPI

app = FastAPI(title="Feedback Inbox")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
