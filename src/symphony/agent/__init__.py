"""Agent execution layer.

`runner.py` defines the `Runner` protocol — the seam between "the
orchestrator says what to run" and "execution happens here." `runners/`
holds concrete implementations (LocalRunner today; E2BRunner / DaytonaRunner
in v2 per docs/python-port-research.md §15).
"""
