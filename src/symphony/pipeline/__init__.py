"""Pipeline state-machine + scheduler.

Both modules are pure: input → output, no IO, no clock, no DB. The
orchestrator (in `orchestrator/`) wraps them with the side-effecting
calls (Linear comments, GitHub PR opens, runner spawns).
"""
