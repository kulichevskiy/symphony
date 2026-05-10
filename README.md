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
