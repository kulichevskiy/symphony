"""Unit tests for the transient-API-error stream classifier.

Covers both providers over captured-shape stream fixtures: claude's
`is_error` / `api_error_status` result (+ synthetic "API Error" assistant
text) and codex's `turn.failed` / `error` event. A transient status
({429,500,502,503,529}) classifies as transient; a deterministic 4xx does not;
a clean no-verdict stream classifies to None (so it is never retried).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from symphony.pipeline.local_review import (
    TRANSIENT_API_STATUSES,
    StreamApiError,
    classify_stream_api_error,
)

_FIXTURES = Path(__file__).parent / "fixtures" / "agent_streams"


def _load(name: str) -> str:
    return (_FIXTURES / name).read_text(encoding="utf-8")


def test_claude_api_error_500_fixture_is_transient() -> None:
    err = classify_stream_api_error(_load("claude_api_error_500.jsonl"))
    assert err is not None
    assert err.status == 500
    assert err.transient is True
    assert err.message.startswith("API Error: 500")


def test_codex_api_error_500_fixture_is_transient() -> None:
    err = classify_stream_api_error(_load("codex_api_error_500.jsonl"))
    assert err is not None
    assert err.status == 500
    assert err.transient is True
    assert "Internal server error" in err.message


def test_codex_model_unsupported_400_is_not_transient() -> None:
    """A deterministic 4xx surfaces the real message but is NOT transient."""
    err = classify_stream_api_error(_load("codex_model_unsupported_400.jsonl"))
    assert err is not None
    assert err.status == 400
    assert err.transient is False
    assert "is not supported" in err.message


def test_claude_clean_stream_is_none() -> None:
    assert classify_stream_api_error(_load("claude_clean.jsonl")) is None


def test_codex_clean_stream_is_none() -> None:
    assert classify_stream_api_error(_load("codex_clean.jsonl")) is None


@pytest.mark.parametrize("status", sorted(TRANSIENT_API_STATUSES))
def test_each_transient_status_classifies_transient(status: int) -> None:
    """Every status in the transient set classifies transient (both shapes)."""
    claude = "\n".join(
        [
            json.dumps(
                {
                    "type": "result",
                    "subtype": "error_during_execution",
                    "is_error": True,
                    "result": f"API Error: {status} overloaded",
                    "api_error_status": status,
                }
            ),
        ]
    )
    err = classify_stream_api_error(claude)
    assert err is not None and err.status == status and err.transient

    inner = json.dumps({"type": "error", "status": status, "error": {"message": "x"}})
    codex = json.dumps({"type": "turn.failed", "error": {"message": inner}})
    err = classify_stream_api_error(codex)
    assert err is not None and err.status == status and err.transient


def test_synthetic_assistant_text_status_parsed_without_result_event() -> None:
    """A synthetic `<synthetic>` assistant carries the status in its text even
    if the terminal result event is absent/truncated."""
    stream = json.dumps(
        {
            "type": "assistant",
            "message": {
                "model": "<synthetic>",
                "content": [{"type": "text", "text": "API Error: 529 overloaded"}],
            },
        }
    )
    err = classify_stream_api_error(stream)
    assert err == StreamApiError(message="API Error: 529 overloaded", status=529)
    assert err.transient is True


def test_normal_assistant_message_is_not_an_error() -> None:
    """A real (non-synthetic) assistant message must not classify as an error."""
    stream = json.dumps(
        {
            "type": "assistant",
            "message": {
                "model": "claude-opus-4-8",
                "content": [{"type": "text", "text": "API Error: 500 in a code block"}],
            },
        }
    )
    assert classify_stream_api_error(stream) is None


def test_synthetic_assistant_without_api_error_pattern_is_not_an_error() -> None:
    """A <synthetic> message whose text is not 'API Error: <status>' must not
    classify as an error — only genuine provider failures carry that prefix."""
    stream = json.dumps(
        {
            "type": "assistant",
            "message": {
                "model": "<synthetic>",
                "content": [{"type": "text", "text": "Something went wrong internally."}],
            },
        }
    )
    assert classify_stream_api_error(stream) is None


def test_codex_raw_api_error_text_status_parsed() -> None:
    """A codex error event whose raw message text is 'API Error: 500 …' (no
    separate status field) must parse status=500 from the text."""
    stream = json.dumps({"type": "turn.failed", "error": {"message": "API Error: 500 overloaded"}})
    err = classify_stream_api_error(stream)
    assert err is not None
    assert err.status == 500
    assert err.transient is True
    assert "API Error: 500" in err.message


def test_codex_raw_error_event_api_error_text_status_parsed() -> None:
    """Same as above but via the top-level 'error' event type."""
    stream = json.dumps({"type": "error", "message": "API Error: 503 service unavailable"})
    err = classify_stream_api_error(stream)
    assert err is not None
    assert err.status == 503
    assert err.transient is True
