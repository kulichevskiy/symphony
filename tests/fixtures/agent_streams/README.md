# Agent terminal-stream fixtures

Captured-shape JSONL streams an agent CLI emits when its turn ends on a
provider API error. Both providers exit 0 with no verdict / completion marker,
so these are the streams the transient-error classifier
(`classify_stream_api_error`) must recognise:

- `claude_api_error_500.jsonl` — claude `--output-format stream-json`: a
  synthetic `model:"<synthetic>"` assistant message reading `API Error: 500 …`
  followed by a terminal `result` with `is_error:true` + `api_error_status:500`.
- `codex_api_error_500.jsonl` — codex `--json`: a `turn.failed` whose
  one-level-nested `error.message` JSON carries `status:500` (transient).
- `codex_model_unsupported_400.jsonl` — codex `turn.failed` with `status:400`
  (a deterministic 4xx — NOT transient; the real message must still surface).
- `claude_clean.jsonl` / `codex_clean.jsonl` — normal no-error streams; the
  classifier must return `None` (so a clean no-verdict run is not retried).

Payloads are sanitized but structurally faithful to the real CLI output.
