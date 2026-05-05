from dataclasses import dataclass, field


@dataclass(frozen=True)
class AgentResult:
    """Outcome of one `claude` subprocess invocation.

    `success` is True only when the process exited 0, a `result` event was
    emitted, and that event's `is_error` is false.
    """

    session_id: str | None
    exit_code: int
    success: bool
    is_error: bool
    duration_ms: int | None
    num_turns: int | None
    total_cost_usd: float | None
    final_text: str | None
    raw_events: list[dict] = field(default_factory=list)
    stderr: str = ""
