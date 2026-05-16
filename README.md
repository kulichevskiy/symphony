# symphonyd — walking skeleton

Python port of Symphony Mac, headless and Linear-native. See `../docs/python-port-research.md` for the full design.

## What works in this skeleton

- Config loading (`pydantic-settings`, env + YAML).
- Linear GraphQL client (`httpx`, hand-rolled queries).
- `Runner` protocol with `LocalRunner` impl (subprocess + stall watchdog).
- Single poll cycle: scan a Linear team for `symphony`-labelled `Todo` issues, dispatch one, comment back.
- CLI entrypoint (`python -m symphony`).

## What's stubbed

- SQLite persistence (in-memory dict for now).
- GitHub `gh` CLI wrapper (the runner currently echoes a fake command).
- Pipeline state machine (single-stage; `Implement → Review → Merge → Done` lands in iteration 4+).
- Inbound slash commands (`/approve`, `/stop`).
- Per-stage prompt templates.
- Red-gate.

## Run

```bash
cd python
uv sync                              # installs deps
export LINEAR_API_KEY="lin_api_..."
uv run python -m symphony --config examples/config.yaml
```

## Structure

See `src/symphony/` — module layout matches `docs/python-port-research.md` §14.

## Tests

```bash
uv run pytest
```

## Dependency-Aware Pickup

By default, Symphony keeps the older behavior and picks up every matching issue
in the configured `ready` state. A binding can opt into dependency-aware pickup
with:

```yaml
linear_states:
  ready: Todo
  waiting: Waiting
```

When `waiting` is configured, Symphony checks Linear issue relations before it
dispatches work. If the ready issue is blocked by any open, unarchived blocker
(`backlog`, `unstarted`, `started`, or `triage`), Symphony moves it to
`Waiting`, posts a Linear comment naming the blockers, and does not create a run.

`Waiting` is separate from `Blocked`: `Waiting` means "dependency not ready",
while `Blocked` remains the agent-error parking lane for cost caps, merge
rejects, and other pipeline failures. Returning dependency-waiting issues to
Ready is manual in this slice.
