"""Pipeline state-machine + scheduler.

The state-machine and scheduler modules are pure: input → output, no IO, no
clock, no DB. The orchestrator (in `orchestrator/`) wraps them with the
side-effecting calls (Linear comments, GitHub PR opens, runner spawns).

`controls` is the one exception: it owns the durable `pipeline_controls`
table directly (see its own docstring), since the actor/state transitions it
records must be committed atomically with the action that produced them.
"""
