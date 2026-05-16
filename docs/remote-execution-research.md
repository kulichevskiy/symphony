# Remote execution research — moving issue implementation off the orchestrator host

Date: 2026-05-14 (iterations 1–15)
Status: design proposal — not yet a decision. PR 1a is the only thing the recommendation actually asks anyone to do; everything else is contingent on §6.2 triggers.
Scope: how (and whether) to move per-run agent execution off the orchestrator host. Does not cover: moving the orchestrator itself, switching LLM providers, or changes to the Linear/GitHub state machine.
Owner: needs one. The migration is small but the trigger-watch in §6.2 is a recurring task with no current home. See §12 for the proposed ownership/cadence.

**One-sentence summary:** Move agent execution to a managed sandbox if/when we're throughput-bound or have an isolation incident; until then, ship a 1-day refactor (PR 1a) that makes the abstraction real and stop.

**Relationship to prior research.** This doc extends `../SymphonyMac/docs/python-port-research.md` §6.3 (managed sandboxes deferred to v2), §15 (`Runner` protocol abstraction), and §16 (sandbox-vs-VPS cost analysis). What's new here: (1) the §6.2 measurable triggers, (2) the verified-against-code mapping of orchestrator vs sandbox surfaces (§2.1, §5.0), (3) the actual Daytona SDK shape forcing the PID-tracking workaround (§5 PR 2), (4) the production-volume cost grounding ($2.77/audit-period), and (5) PR 1a as a standalone refactor independent of the migration. The Rust doc said "design the seam"; this doc says "ship the seam, defer the migration, here's how to know when to stop deferring."

## Contents

- [TL;DR](#tldr) — five-bullet recommendation
- [§1 Why this question matters now](#1-why-this-question-matters-now) — isolation, concurrency, optionality
- [§2 Current local-execution surface](#2-current-local-execution-surface-what-were-moving) — what LocalRunner gives us today, verified against `poll.py:5049`
- [§3 Three architectures](#3-three-architectures) — A (sandbox lift-and-shift), B (managed agents), C (Codex app-server) + end-to-end sequence diagram
- [§4 Pros / cons matrix](#4-pros--cons-matrix) — 16-dimension comparison
- [§5 Concrete migration plan (Architecture A)](#5-concrete-migration-plan-architecture-a) — minimum landable change (PR 1a checklist), PR 1b skeleton, Daytona/E2B providers, escape hatch
- [§6 Decision: should we do it?](#6-decision-should-we-do-it) — grounded in production audit numbers
- [§6.0.5 The cheap alternative: just buy a bigger VPS](#605-the-cheap-alternative-just-buy-a-bigger-vps) — bigger box fixes concurrency, not isolation
- [§6.1 When NOT to do this migration](#61-when-not-to-do-this-migration) — counter-case
- [§6.2 How we know a trigger has fired](#62-how-we-know-a-trigger-has-fired-measurement) — measurable signals
- [§7 Rollout](#7-rollout) — go/no-go bars, global-cap landmine
- [§7.1 Testing strategy](#71-testing-strategy) — `_FakeSandboxProvider` pattern
- [§8 Steady state](#8-steady-state--what-success-looks-like-12-months-out) — what success looks like 12 months out, both branches
- [§9 Auth and secrets](#9-auth-and-secrets--what-lives-where) — threat model
- [§10 Operational sharp edges](#10-operational-sharp-edges) — kill latency, orphan reconcile, stream disconnect, activity-comment lag
- [§11 Where this analysis could be wrong](#11-where-this-analysis-could-be-wrong) — load-bearing assumptions
- [§11.5 The case against PR 1a itself](#115-the-case-against-pr-1a-itself) — the skeptic's read
- [§12 Ownership and cadence](#12-ownership-and-cadence) — who watches the triggers
- [§13 Open questions](#13-open-questions)
- [§14 References](#14-references)

## TL;DR

- **It is worth building the option, but it is not yet worth deploying it.** Land the per-binding `Runner` factory (PR 1a of §5) as a refactor so the abstraction is real. Then *stop* until we hit one of the trigger conditions in §6.1 (concurrency ceiling losing throughput, or a real isolation incident).
- **If/when we do migrate, the architecture is locked in: Architecture A — "Sandbox lift-and-shift."** Keep the orchestrator and the Linear/GitHub state machine local (or on a VPS); move each `codex exec` / `claude --print` invocation into a managed sandbox (Daytona for per-issue persistent workspaces, E2B for ephemeral, Fly Machines as escape hatch). Symphony's `Runner` protocol (`src/symphony/agent/runner.py:51`) already anticipates this exact swap; the `runner: "e2b" | "daytona"` config keys are stubbed but unimplemented.
- **Not recommended: Architecture B — "Native managed agents."** Handing whole issues to Codex Cloud / GitHub Coding Agent / Claude Managed Agents looks like the cleaner move, but it deletes symphonyd's cost guard, stall watchdog, activity-comment stream, review-fix loop, and per-issue workspace caching. That is most of the value the orchestrator currently provides. Use it as a *fallback* venue for a single binding, not a wholesale migration.
- **Before sandbox migration, try the cheap fix.** If the concrete pain is concurrency-only (not isolation), the right first move is to upgrade the orchestrator VPS plan (Hetzner CX22 → CX42 is €12/mo more, gets us to 8 vCPU / 20-ish concurrent runs) and raise `global_max_concurrent`. Watch for 30 days. Sandbox migration is the *second* response to concurrency, never the first. See §6.0.5.
- The migration, when triggered, is three or four shippable PRs (§5): the protocol refactor (PR 1a) ships now and stands alone; the SandboxRunner skeleton (PR 1b), Daytona provider (PR 2), and E2B provider (PR 3) ship only after a §6.2 trigger. PR 4 is an optional managed-agent escape hatch for one untrusted-repo binding, not the migration. Per-binding `runner:` knob already exists in config.
- The single most important load-bearing claim: **everything downstream of the `Runner` protocol survives unchanged.** Cost guard, activity stream, stall handling, review-fix loop, merge-conflict recovery are all driven off the `RunnerEvent` stream (§2.1, verified against `poll.py:5049`). The seams that *do* change are bounded and named: `Runner` (the new SandboxRunner), `Workspace`/`RunnerSpec.workspace_path` → `WorkspaceHandle` (§5.0.1), `_push_fn` (§5.0), `global_max_concurrent` (§7), auth/secrets (§9), and the `symphony-runner` image pipeline (§5 PR 2). That's the entire blast radius.

---

## 1. Why this question matters now

The local LocalRunner (`src/symphony/agent/runners/local.py`) is the only execution venue today. It pins symphonyd's deploy story to "one host, one shell, one set of CLI credentials." Three forcing functions, in descending order of weight:

1. **Per-issue blast radius.** The Rust port research (`SymphonyMac/docs/python-port-research.md` §6.3, §15) flagged that a misbehaving agent can `rm -rf .` inside its own workspace — contained today, but on the same machine as 3 other live runs and the orchestrator process itself. Per-run isolation removes that. We have not had a real incident; this is a "ticking-clock" risk, not a current bleed.
2. **Concurrency ceiling.** Global cap is 4 (`config.local.yaml:7`). The cap exists because the orchestrator host's CPU and RAM are the bottleneck, not because four is the right number for the Linear queue depth. We have not consistently been throughput-bound, but the audit's VIB-1 retry storm did briefly starve other bindings (`production-reliability-audit.md`, "Existing work starved by new work"). **Important**: the cheapest response to this forcing function is a bigger VPS, not a migration — see §6.0.5. Sandbox-migration is the right response only if (a) we've already scaled the VPS and still need more, or (b) we're addressing forcing function #1 (isolation) at the same time.
3. **Optionality.** The `Runner` protocol is already shaped for a sandbox swap (`runner.py:51`, `config.py:69` enum). Every iteration we leave the second implementation unwritten, the abstraction is one step closer to "the kind of thing nobody is sure works anymore." Landing PR 1a alone proves it.

What is *not* a forcing function: **cost.** §6 grounds this in the audit's actual volume — the entire audited dispatch history would cost <$3 of Daytona compute. The VPS is cheaper for very-low utilization than managed sandboxes, but neither figure is a budget event for symphonyd. Stop reaching for the cost argument; this migration is about isolation and abstraction.

## 2. Current local-execution surface (what we're moving)

`LocalRunner.run(spec)` is a thin wrapper around `asyncio.create_subprocess_exec`. The surface area that any remote runner has to preserve:

| Capability | Where it lives today | Why it matters |
|---|---|---|
| Spawn agent CLI in workspace dir | `runners/local.py:38` | The whole pipeline runs `codex` / `claude` |
| Stream stdout/stderr line-by-line as `RunnerEvent` | `runners/local.py:64` | Drives cost guard, activity comments, stall detection |
| Stall watchdog (SIGTERM → SIGKILL on inactivity) | `runners/local.py:78` | Prod-critical: stuck runs cost real $ |
| `kill(run_id)` from another coroutine | `runners/local.py:161` | `$stop` slash-command, shutdown |
| Per-issue persistent workspace clone | `workspace.py:33` | Implement → Review-fix → Merge reuse same dir |
| TTL sweep of stale workspaces | `workspace.py:127` | Keeps the host from filling up |
| `gh` CLI auth on the host | implicit | PR creation, comment posting, repo cloning |
| `codex login` / `claude /login` auth on the host | implicit | The agent CLI itself |
| Activity stream → Linear comments | `agent/activity.py`, `db/activity_comments.py` | User-visible progress |
| Cost cap enforced per issue | `pipeline/cost_guard.py` | Burned-runaway protection |

**The contract any remote runner must keep:** the `Runner` protocol (`agent/runner.py:51`) — `run(spec) → AsyncIterator[RunnerEvent]` and `kill(run_id)`. Plus whatever workspace assumption the new venue makes (per-stage clone, or persistent per-issue workspace).

### 2.1 Why this contract is enough (verified against the code)

This matters because the case for Architecture A — and against Architecture B — depends on it. The orchestrator's audit-driven subsystems are all wired into the *event stream returned by the runner*, not into LocalRunner specifically. From `poll.py:5049`:

```python
async for ev in self._runner.run(spec):
    if ev.kind == "started" and ev.pid is not None:
        await db.runs.update_pid(self._conn, run_id, ev.pid)
    elif ev.kind == "stdout" and ev.line is not None:
        logf.write(ev.line + "\n")
        usage = parse_event_line(ev.line)            # cost guard
        if usage is not None:
            cost_delta = cost_estimator.delta(usage)
            …
            if decision.cap_breached:
                await self._kill_active_runner(run_id)
        await self._record_activity_stdout(…)        # activity stream
    elif ev.kind == "tick":
        await self._record_activity_tick(…)
    elif ev.kind in ("exit", "stall_timeout", "spawn_failed"):
        …
```

Concretely: cost guard, activity comments, stall-timeout, spawn-failed handling, log archival, and PID tracking all flow off `RunnerEvent`. Any runner that yields the same event shapes — including a sandbox runner that pipes a remote `codex exec --json` stdout into `RunnerEvent(kind="stdout", line=…)` — gets all five for free. Nothing in the pipeline state machine, the review classifier, the merge-conflict path, or the cost guard parser is coupled to LocalRunner. The `Runner` protocol holds, and is the architectural seam that makes Architecture A a swap rather than a rewrite.

One scope-confirmation while we're here: `self._runner` is currently a **single process-wide instance** (`poll.py:814`). PR 1a of the migration plan has to convert this to a per-binding lookup so that mixed-runner deployments (`local` for ADJ, `daytona` for VIB) work. That refactor is small and contained.

## 3. Three architectures

The phrase "move codex/claude execution remote" can mean three structurally different things. A and B are the main contenders; C exists as a narrower variant. They are not the same migration and the trade-offs are completely different.

### Architecture A — Sandbox lift-and-shift

Orchestrator stays local/VPS. Every place that today says `asyncio.create_subprocess_exec("codex", "exec", …)` instead says "provision a sandbox, push the workspace into it, run the same command there, stream events back."

```
┌────────────────────────────────┐    ┌─────────────────────────────────┐
│ Orchestrator (laptop / VPS)    │    │ E2B/Daytona/Modal sandbox       │
│                                │    │                                 │
│ Poll → Pipeline → SandboxRunner│───►│ git clone, codex exec --json …  │
│        ▲                       │    │ stream stdout ◄─────────────────┤
│        │                       │◄───┤ push branch                     │
│ Linear / GitHub clients        │    └─────────────────────────────────┘
└────────────────────────────────┘
```

Symphony still owns: Linear poll, state machine, PR creation, review classifier, cost guard, stall watchdog (logically), activity comment cadence, retries.

End-to-end call flow for one Implement run under Architecture A (Daytona variant):

```
   Orchestrator (VPS)              Daytona session              codex CLI           GitHub
   ─────────────────              ───────────────              ─────────           ──────
1. poll Linear → ready
2. Workspace.acquire(issue) ───── git clone if missing ──────────────────────────► repo
3. RunnerSpec built (workspace=SandboxWorkspaceHandle, command=["codex","exec",…])
4. SandboxRunner.run(spec)
       │
       ▼
   provider.start(spec) ──────► create_session, run_async
                                  ▼
                                exec wrapped command  ─────► codex exec --json
                                                                  │
                                                                  ▼ (stdout JSONL, one line per event)
       ◄──── on_stdout(line) ◄── stream logs ◄──────────────── token/usage/turn events
       │
       ▼
   per line:                              (these all happen on the orchestrator,
     parse_event_line → Usage              not in the sandbox)
     cost_guard.evaluate
     activity._record_activity_stdout (→ Linear comment, throttled)
     log file append
       │
       ▼
   on cap breach → runner.kill(run_id)
                       │
                       ▼
                   handle.cancel() ─────► exec "kill -TERM $(cat /tmp/symphony.pid)"
                                            │
                                            ▼
                                          codex stops, exits N
       ◄──── exit code ◄──────────────────────
       │
       ▼
   on exit:
     workspace.push(branch) ──► sandbox.process.exec("git push origin <branch>") ──► repo
     orchestrator: gh.pr_create(…)  ────────────────────────────────────────────────► PR opened
       │
       ▼
   Implement done → state machine flips to Review
```

The orchestrator side of that diagram is **byte-for-byte the same** as today (`poll.py:5049`'s `async for ev in self._runner.run(spec)` loop). The only difference is what's behind `self._runner` and `spec.workspace`.

### Architecture B — Native managed agents

Symphony hands the entire issue (prompt, repo, branch) to a cloud agent that has its own clone-edit-test-PR loop. We wait for the PR URL and then resume our review/merge state machine.

Candidates:
- **Codex Cloud / `@codex` on a GitHub issue.** Assign or tag, get a PR back. ([OpenAI Codex web](https://developers.openai.com/codex/cloud), [Codex GitHub integration](https://developers.openai.com/codex/integrations/github))
- **GitHub Copilot Coding Agent.** Assign Copilot to a GitHub issue, runs on GitHub Actions infrastructure, ships a PR. ([about coding agent](https://docs.github.com/copilot/concepts/agents/coding-agent/about-coding-agent), [Linear integration](https://docs.github.com/copilot/how-tos/use-copilot-agents/cloud-agent))
- **Claude Managed Agents.** Anthropic-hosted REST API with persistent sessions (`POST /v1/sessions`, `managed-agents-2026-04-01` beta header). ([overview](https://platform.claude.com/docs/en/managed-agents/overview), [sessions](https://platform.claude.com/docs/en/managed-agents/sessions))

```
┌────────────────────────────────┐    ┌─────────────────────────────────┐
│ Orchestrator                   │    │ Codex Cloud / Copilot / Claude  │
│                                │    │ Managed Agent                   │
│ Implement: POST prompt ───────►│───►│   (full clone+edit+test+PR)     │
│                                │    │                                 │
│ Wait for PR webhook  ◄─────────│◄───┤ opens PR                        │
│ Review stage continues as before│   └─────────────────────────────────┘
└────────────────────────────────┘
```

This is *not* a runner swap. The "single run yields a stream of events" contract goes away. The new contract is "tell remote agent to fix issue X; later, a PR appears." Most of symphonyd's internal machinery has no role in that model. See §4 for what specifically breaks.

### Architecture C — Codex's own app-server / remote-control protocol

A third option that sits between A and B, worth naming explicitly so we don't pretend A and B are the whole space.

Codex's CLI ships a server mode: `codex app-server --listen ws://IP:PORT` exposes a JSON-RPC 2.0 protocol over WebSocket, and `codex --remote ws://host:port` connects a client to it. The `codex-exec-server` crate is the same idea for the non-interactive `exec` path: it spawns and controls subprocesses on a remote host, streaming events back over WS. ([app-server reference](https://developers.openai.com/codex/app-server), [remote connections](https://developers.openai.com/codex/remote-connections))

```
┌────────────────────────────────┐    ┌─────────────────────────────────┐
│ Orchestrator                   │    │ Tiny remote host (VPS/container)│
│                                │ JSON-RPC over WS                    │
│ Implement: WS RPC ────────────►│───►│ codex app-server                │
│ stdout events ◄────────────────│◄───│   (workspace on disk here)      │
└────────────────────────────────┘    └─────────────────────────────────┘
```

Differences from A:
- ➕ Wire protocol designed for this exact thing by the vendor (auth, framing, cancellation are all defined).
- ➕ No generic-sandbox SDK to learn or pay for; just a Codex binary on a remote box.
- ➖ Codex-only — does not help with the Claude binding.
- ➖ App-server WebSocket transport is currently **experimental and unsupported** per the official docs.
- ➖ You still have to provision the remote box (Fly Machine, ec2-tiny, whatever) and rotate creds there.

**Verdict on C**: not the right primitive for a Symphony-wide migration because half our bindings are Claude. It is, however, a viable backend *inside* Architecture A — i.e. a `CodexAppServerRunner` that talks JSON-RPC to a pool of codex-app-server processes pre-warmed inside Daytona/E2B sandboxes. That's mostly a latency optimization (skip CLI process spawn on every run); revisit only if cold-start latency turns out to dominate.

## 4. Pros / cons matrix

Legend: ✅ same as today, ➕ improves, ➖ regresses, ⚠️ changes character (not strictly worse but needs new code).

| Concern | LocalRunner (today) | Architecture A — Sandbox CLI | Architecture B — Managed agent |
|---|---|---|---|
| Per-run isolation | host workspace dir | ➕ Firecracker microVM per run | ➕ vendor-managed |
| Concurrency ceiling | host CPU/RAM (~4) | ➕ scales horizontally, paid per-second | ➕ scales effectively without limit |
| Cost model | always-on VPS, idle hours wasted | ➕ per-second compute; cheaper at ≥200 runs/day; more expensive at very low utilization | ⚠️ token-billed plus **$0.08/session-hour runtime surcharge on Claude Managed Agents** ([pricing](https://platform.claude.com/docs/en/about-claude/pricing)); Codex Cloud is opaque; Copilot is per-seat |
| Stdout/stderr stream fidelity | ✅ line-by-line | ✅ via SDK callbacks (E2B/Daytona/Modal all expose `on_stdout`) | ➖ vendors expose status events, not raw model tokens; cost guard parser (`agent/process.py`) is dead |
| `Usage` events for cost guard | ✅ from JSONL stream | ✅ same JSONL stream | ➖ Codex Cloud / Copilot: not exposed; Claude Managed: exposed via session usage endpoint |
| Stall watchdog (SIGTERM on inactivity) | ✅ PID-based, ours | ⚠️ via SDK `kill` / sandbox destroy — semantics differ per vendor | ➖ vendor's own timeout; we lose grip |
| `$stop` (`kill(run_id)`) | ✅ kills process group | ✅ SDK cancel + destroy sandbox | ⚠️ vendor cancel endpoint or none — Claude Managed Agents' cancel semantics are not yet documented in public beta; Copilot has no cancel; Codex Cloud cancels by closing the issue |
| Workspace persistence across stages | ✅ on-disk clone reused for Implement → Review-fix → Merge | ⚠️ two paths: Daytona keeps a per-issue persistent workspace; E2B/Modal: re-clone each stage (slower) | ➖ workspace lives inside vendor; we have no Implement → Review-fix shared state |
| Review-fix loop semantics | ✅ same CLI, same workspace, just a new prompt | ✅ same model — fix-run = new sandbox-exec in same persistent workspace, or fresh ephemeral | ➖ have to either push a new prompt as a comment on the PR (Codex Cloud supports `@codex fix`) or open a fresh agent session against the existing PR branch. Either way, less surgical |
| Merge-conflict resolution | ✅ orchestrator runs `git rebase`, agent edits conflict markers | ✅ same flow, just in sandbox | ➖ have to invoke the cloud agent on the conflict prompt; not all vendors expose this |
| `gh` / `git push` auth | host PAT | ⚠️ inject PAT into sandbox per-run (sandbox SDKs all do this; one more secret hop) | ➕ vendor uses their own GitHub App, no PAT to manage |
| `codex` / `claude` CLI auth | host login | ⚠️ inject API key into sandbox; `--api-key` flags exist on both CLIs | ✅ no CLI involved |
| Activity comments on Linear | ✅ derived from stdout JSONL | ✅ same parser, same flow | ➖ have to derive from vendor status events; new parser per vendor |
| Cost cap enforcement (per-issue USD) | ✅ killed mid-run if cap hit | ✅ same mechanism | ⚠️ Claude Managed: pre-flight budget on session; Codex Cloud / Copilot: post-hoc only |
| Cold start | 0 (subprocess fork) | Daytona ~30–90 ms claimed, E2B sub-second, Modal 1–5 s | seconds-to-minutes (vendor provision time) |
| Failure modes you have to learn | OS-level | + sandbox SDK errors, network blips | + vendor outages, queue depth, rate limits, beta-API churn |
| Beta / contract stability | stable (POSIX) | stable (sandbox vendors are GA) | ⚠️ Claude Managed Agents is beta (`managed-agents-2026-04-01`); Codex Cloud GH integration is GA but APIs evolve |
| Operator burden to add | small | medium (one new module + auth wiring) | large (replaces ~6 subsystems with vendor calls; new failure modes) |
| Path back to local if it goes wrong | n/a | ✅ keep LocalRunner as the default, `runner:` knob picks | ⚠️ harder — review/merge state assumes managed flow once issues stop emitting stream events |

**Reading the matrix:** Architecture A keeps 100% of symphonyd's current behavior and adds isolation + horizontal scale. Architecture B trades away the four subsystems we built specifically because the audit said the old version *didn't have them*: cost guard, stall watchdog, activity stream, per-issue review-fix loop. That's why I'm calling B a regression even though the box "use cloud agent" intuitively reads as an upgrade.

## 5. Concrete migration plan (Architecture A)

The skeleton is in place. `runner: "local" | "e2b" | "daytona"` is already a config field (`config.py:69`). The `Runner` protocol already takes everything the new venue needs as `RunnerSpec` data. Four PRs land the migration; each is independently shippable and can be reverted.

### 5.−1 The smallest reversible step

If you read this doc and agreed with the TL;DR, the literal next commit is small. It is *not* the SandboxRunner skeleton, the Daytona provider, or any of the production-touching code. It is the refactor that makes the abstraction real.

**Minimum landable change (one PR, no production behavior delta):**

1. Change `RunnerSpec.workspace_path: Path` to `RunnerSpec.workspace: WorkspaceHandle` (§5.0.1). Add `LocalWorkspaceHandle(path)`. Update `LocalRunner` to read `spec.workspace.path`. Update the ~10 `poll.py` call sites that build the spec.
2. Convert `self._runner: Runner` in `Orchestrator.__init__` (`poll.py:814`) to a `_runner_for(binding) → Runner` lookup that today returns the single `LocalRunner` for every binding (per-binding shape, single impl).
3. Move `_push_fn` from a module-level callable to `WorkspaceHandle.push(branch)`. For `LocalWorkspaceHandle`, dispatch to the existing `_default_push`. The pipeline stops carrying a free function around.

Result: behavior is byte-for-byte identical to today. The abstraction stops being aspirational. A reviewer can read `runner.py` + `workspace.py` + the new factory and see a working two-implementation seam (LocalRunner + a documented `SandboxRunner` not-yet-written). Tests in `tests/test_implement_e2e.py` pass unchanged because `_FakeRunner` doesn't care about the workspace shape.

That single PR is the entire commitment until §6.1 triggers fire. Everything below (PRs 2–4, providers, rollout) is contingent on those triggers.

**Naming convention used below.** "PR 1a" = the refactor described above (this section). "PR 1b" = the SandboxRunner skeleton in the next subsection. The full §5 PR 1 lands both together *if and only if* a §6.1 trigger has fired. If the trigger has not fired, ship PR 1a alone; that is the entire migration's standing investment.

#### PR 1a implementer checklist

A scan-able file-by-file map of what to touch, for the engineer who picks this up:

| File | Change |
|---|---|
| `src/symphony/workspace.py` | Add `WorkspaceHandle` protocol and `LocalWorkspaceHandle(path: Path)` impl. Make `Workspace.acquire(binding, issue)` return `WorkspaceHandle` (today: `Path`). Add `WorkspaceHandle.push(branch, *, force: bool = False) -> Awaitable[None]` (today: free-function `_default_push` / `_default_force_push` in `poll.py:820–823`). `LocalWorkspaceHandle.push` dispatches to the existing `_default_push` / `_default_force_push`. |
| `src/symphony/agent/runner.py` | Change `RunnerSpec.workspace_path: Path` → `RunnerSpec.workspace: WorkspaceHandle`. Update the docstring (currently hedges with "descriptor for sandbox runners"). |
| `src/symphony/agent/runners/local.py` | `runners/local.py:43` reads `spec.workspace_path`; change to `spec.workspace.path` after `isinstance` assert. No other code touches `RunnerSpec`. |
| `src/symphony/agent/runners/__init__.py` (new) | Add `def make_runner(binding: RepoBinding) -> Runner`. For now returns `LocalRunner()` for every venue; the function body is a 3-line `match`/`if`. |
| `src/symphony/orchestrator/poll.py` | `poll.py:814` — replace `self._runner: Runner = runner if runner is not None else LocalRunner()` with a dict `self._runners_by_venue: dict[str, Runner] = {…}` and a `_runner_for(binding) -> Runner` method. All ~6 call sites that today do `await self._runner.kill(run_id)` / `async for ev in self._runner.run(spec)` route through the new method. Push call sites (`poll.py:2052`, `:2308`, `:4197`, `:4401`) and force-push call site (`poll.py:2664`) go through `workspace_handle.push(branch)` / `workspace_handle.push(branch, force=True)`. |
| `src/symphony/orchestrator/poll.py` (continued) | Drop `_push_fn` and `_force_push_fn` from `Orchestrator.__init__` (`poll.py:805–807, 820–823`) — they're now embedded in the workspace handle. The `_default_push` / `_default_force_push` module-level functions stay (called by `LocalWorkspaceHandle.push`), they just stop being parameters. |
| `src/symphony/orchestrator/poll.py` `_kill_active_runner` | `poll.py:1444–1448` today is one line: `await self._runner.kill(run_id)`. Under per-binding runners, `_kill_active_runner(run_id)` has no binding context (one of its callers — `drain_dispatch_tasks(cancel=True)` at `poll.py:1424` — iterates `self._active_run_ids` globally). Cleanest fix: each Runner impl already keeps its own `_active: dict[str, ...]` map; have `_kill_active_runner` iterate `self._runners_by_venue.values()` and call `kill()` on every one (it's a no-op for runners that don't own that run_id). The fan-out is bounded (≤3 runners across all venues), and the cost-cap fast path at `poll.py:5080` still has `binding` in scope and can call directly. |
| `tests/test_implement_e2e.py`, `tests/test_review_stage.py`, `tests/test_merge_stage.py` | `_FakeRunner` and orchestrator construction: tests that pass `push_fn=…` to `Orchestrator(…)` need to either drop the kwarg or pass a `Workspace` fixture that produces handles wrapping the test's push fn. The latter is the cleaner change; one helper in a test conftest. |
| `tests/test_runner_local.py` | Tests that build `RunnerSpec(workspace_path=Path(...))` need to build `RunnerSpec(workspace=LocalWorkspaceHandle(Path(...)))`. ~6 spots. |

What the diff is **not** changing: `LocalRunner` internals (stall watchdog, process group, signal handling), `Workspace` lock semantics, the activity-stream parser (`agent/process.py`), the review classifier, the state machine, the pipeline. Nothing in `docs/` other than this file.

CI signal: `uv run pytest` should pass with zero behavior changes. If it doesn't, PR 1a has scope creep.

### 5.0 What stays on the orchestrator vs what moves to the sandbox

Before code, the load-bearing distinction: **git operations move; GitHub API calls do not.**

The orchestrator calls GitHub two completely different ways today:

| Operation | Surface in code | Needs the workspace? | Verdict |
|---|---|---|---|
| `gh repo clone` (populating the workspace) | `gh.repo_clone` via `Workspace(clone_fn=…)` (`workspace.py:46`, `poll.py:818`) | Yes — creates the workspace | **Moves to sandbox.** |
| `git fetch origin` on workspace acquire | `Workspace.acquire` → `self._git(path, "fetch", "origin")` (`workspace.py:96`) | Yes — refreshes the per-issue clone | **Moves to sandbox.** Becomes `sandbox.process.exec("git", "fetch", …)`. |
| `git switch / switch -c` for branch checkout | `Workspace._ensure_branch` (`workspace.py:188`) | Yes — checks out the symphony-prefixed branch | **Moves to sandbox.** Same shape. |
| `git push` from the workspace | `self._push_fn(workspace_path, branch)` (`poll.py:2052`, `:2308`, `:4197`, `:4401`) | Yes — pushes commits the agent made on disk | **Moves to sandbox.** |
| `git fetch / rebase` for merge-conflict recovery | scattered git shellouts | Yes | **Moves to sandbox.** |
| `gh pr create` | `self._gh.pr_create` (`poll.py:4221`) | No (pure REST call) | **Stays on orchestrator.** |
| `gh pr view / pr_checks / pr_reviews / pr_review_comments / pr_reactions / pr_issue_comments / pr_comment / check_log_tail / commit_committed_at / repo_default_branch` | `self._gh.*` (dozens of call sites) | No (pure REST calls) | **Stays on orchestrator.** |

That split is clean. The orchestrator becomes a Linear+GitHub-API client that never touches `git` and never needs anything cloned locally. The sandbox is the only place where a workspace exists.

Consequences:
- `Workspace.clone_fn` becomes "the sandbox's `git clone` into the sandbox's filesystem", not the orchestrator's.
- `_push_fn` is no longer a free function over `(Path, branch)`. It becomes "ask the workspace to push," which the workspace dispatches to its venue. For LocalRunner that's still `git push` on a host path; for SandboxRunner that's `sandbox.process.exec("git", "push", …)`.
- The orchestrator's GitHub PAT keeps doing all the GitHub-API work. The sandbox needs its **own** scoped credential to do `git clone` and `git push` — see §9 (auth threat model).
- Merge-conflict recovery (`git fetch` + `git rebase`) moves into the sandbox. The conflict-marker prompt the orchestrator builds (`prompt.py:101`) stays the same; only the place it executes against changes.

### 5.0.1 Protocol-level breaking change: `RunnerSpec.workspace_path`

`RunnerSpec` today (`runner.py:25`) types the workspace as `Path`:

```python
workspace_path: (
    Path  # already-cloned dir on disk for LocalRunner; descriptor for sandbox runners
)
```

The comment hand-waves "descriptor for sandbox runners" but the type is `Path`. That works for LocalRunner because `Path("/symphony/workspaces/ENG/eng-1")` is a real filesystem path. For SandboxRunner, the workspace lives inside a sandbox; there *is* no host filesystem path. Forcing a `Path` either means lying with a fake string (which breaks anything calling `.exists()` or `.iterdir()` on the orchestrator side) or sneaking a sandbox handle behind a `Path` subclass (clever, opaque, brittle).

The honest fix — and the load-bearing breaking change PR 1a has to ship — is a `WorkspaceHandle` abstraction:

```python
# src/symphony/workspace.py
from typing import Protocol

class WorkspaceHandle(Protocol):
    """Opaque venue-agnostic reference to one issue's workspace.

    The pipeline never inspects this. Only the runner that produced it
    knows what to do with it. The workspace abstraction (push, clone,
    sweep) calls back through its WorkspaceProvider.
    """
    @property
    def venue(self) -> str: ...  # "local" | "daytona" | "e2b"


class LocalWorkspaceHandle(WorkspaceHandle):
    def __init__(self, path: Path) -> None:
        self.path = path
    @property
    def venue(self) -> str:
        return "local"


class SandboxWorkspaceHandle(WorkspaceHandle):
    def __init__(self, sandbox_id: str, remote_path: str) -> None:
        self.sandbox_id = sandbox_id
        self.remote_path = remote_path
    @property
    def venue(self) -> str:
        return "daytona"  # or whichever provider
```

And `RunnerSpec` changes:

```python
@dataclass
class RunnerSpec:
    run_id: str
    workspace: WorkspaceHandle    # was: workspace_path: Path
    command: list[str]
    env: dict[str, str] = field(default_factory=dict)
    stall_secs: int = 300
    stage: str = ""
```

Migration cost: every call site that today reads `spec.workspace_path` (only LocalRunner — `runners/local.py:43`) becomes `spec.workspace.path` after asserting `isinstance(spec.workspace, LocalWorkspaceHandle)`. Every call site that hands a workspace to a downstream operation (`_push_fn(workspace_path, branch)`, `git rebase` in merge-conflict recovery) takes a `WorkspaceHandle` and dispatches on its venue.

This is one search-and-replace in `poll.py` (~10 call sites) plus the new type. It must land in PR 1a, because PR 1b's SandboxRunner can't be built against a `Path`. Tests in `tests/test_implement_e2e.py` use `_FakeRunner` and never inspect the workspace argument, so they update trivially. Workspace TTL sweep (`workspace.py:127`) only ever sees `LocalWorkspaceHandle` because sandbox TTL is the provider's responsibility, so the sweep code becomes "if local, do mtime check; if remote, skip — provider owns it."

Also for `RunnerSpec.env`: today the orchestrator pre-fills `env` with whatever it wants, and LocalRunner merges it into `os.environ` (`runners/local.py:39`). Under SandboxRunner, the merge target is the *sandbox's* env, not the orchestrator's. The orchestrator must **stop** putting the orchestrator-host's secrets into `spec.env` and instead put only the Codex/Claude API key the agent CLI needs (passed in by the provider from the sandbox's own secret store, not from the orchestrator's). Concretely: `spec.env` should carry **prompt-shaped** values (`SYMPHONY_RUN_ID`, `SYMPHONY_STAGE`, etc.), never secrets. Secrets flow provider-side. This is a quiet behavior change that needs a test.

### PR 1b — Provider-agnostic SandboxRunner scaffolding (post-trigger)

Files:
- `src/symphony/agent/runners/sandbox.py` — new. Implements `Runner`. Constructor takes a `SandboxProvider` callable so we can swap E2B / Daytona / Modal without rewriting the runner itself.
- `src/symphony/agent/runners/__init__.py` — wire the factory: `make_runner(binding.runner)` returns `LocalRunner()` or `SandboxRunner(provider=...)`.
- `src/symphony/orchestrator/poll.py:814` — replace `self._runner = runner if runner is not None else LocalRunner()` with a per-binding lookup (today the runner is process-wide; that needs to become per-binding because a single orchestrator can serve `runner: local` and `runner: daytona` simultaneously). The PoIl uses `binding` already when dispatching, so the lookup is `self._runner_for(binding)`.

Open question for PR 1a: per-binding vs per-run runner. Per-binding is the simpler change and matches §15 of the Rust research. Per-run is overkill until we have a reason.

Concrete skeleton for `sandbox.py` (illustrative — not final API):

```python
# src/symphony/agent/runners/sandbox.py
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import suppress
from typing import Protocol

from ..runner import RunnerEvent, RunnerSpec


class SandboxHandle(Protocol):
    """A live remote execution context for one run.

    Providers (Daytona, E2B, Modal) implement this. The Runner owns
    lifecycle; the Provider owns the wire protocol.
    """

    async def stream(self) -> AsyncIterator[tuple[str, str]]:
        """Yield (kind, line) where kind ∈ {"stdout", "stderr"}."""

    async def cancel(self) -> None: ...
    async def wait(self) -> int:
        """Return the remote process exit code."""


class SandboxProvider(Protocol):
    async def start(self, spec: RunnerSpec) -> SandboxHandle: ...


class SandboxRunner:
    """Implements Runner over a SandboxProvider.

    All the LocalRunner concerns translate one-to-one:
      - subprocess spawn → provider.start()
      - PID liveness     → handle.wait() / heartbeat from stream
      - process group    → handle.cancel() (vendor-defined)
      - stdout/stderr    → handle.stream()
      - stall watchdog   → same activity.set() pattern, fed by stream
    """

    def __init__(self, provider: SandboxProvider) -> None:
        self._provider = provider
        self._active: dict[str, SandboxHandle] = {}
        self._pending_kills: set[str] = set()

    async def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        try:
            handle = await self._provider.start(spec)
        except Exception as e:  # noqa: BLE001 — match LocalRunner contract
            yield RunnerEvent(kind="spawn_failed", error=f"{type(e).__name__}: {e}")
            return

        self._active[spec.run_id] = handle
        if spec.run_id in self._pending_kills:
            self._pending_kills.discard(spec.run_id)
            with suppress(Exception):
                await handle.cancel()

        # No PID on the orchestrator host. Pass through 0 or a vendor id
        # for the runs table.
        yield RunnerEvent(kind="started", pid=0)

        activity = asyncio.Event()
        stalled = asyncio.Event()
        events: asyncio.Queue[RunnerEvent] = asyncio.Queue()

        async def pump() -> None:
            async for kind, line in handle.stream():
                activity.set()
                await events.put(RunnerEvent(kind=kind, line=line))  # type: ignore[arg-type]

        async def watchdog() -> None:
            while True:
                try:
                    await asyncio.wait_for(activity.wait(), timeout=spec.stall_secs)
                except TimeoutError:
                    stalled.set()
                    with suppress(Exception):
                        await handle.cancel()
                    return
                else:
                    activity.clear()

        pump_task = asyncio.create_task(pump())
        watch_task = asyncio.create_task(watchdog())
        wait_task = asyncio.create_task(handle.wait())

        try:
            while True:
                try:
                    ev = await asyncio.wait_for(events.get(), timeout=0.25)
                except TimeoutError:
                    if wait_task.done() and pump_task.done():
                        break
                    yield RunnerEvent(kind="tick")
                    continue
                yield ev

            for t in (pump_task, watch_task):
                if not t.done():
                    t.cancel()
            await asyncio.gather(pump_task, watch_task, return_exceptions=True)

            returncode = await wait_task
            if stalled.is_set():
                yield RunnerEvent(kind="stall_timeout")
            else:
                yield RunnerEvent(kind="exit", returncode=returncode)
        finally:
            self._active.pop(spec.run_id, None)

    async def kill(self, run_id: str) -> None:
        handle = self._active.get(run_id)
        if handle is None:
            self._pending_kills.add(run_id)
            return
        with suppress(Exception):
            await handle.cancel()
```

This is **structurally the same** as `LocalRunner` (`runners/local.py`). The differences are: no PID, no `os.killpg`, vendor-defined cancel semantics, and the stream comes off a Provider object instead of `proc.stdout`. The `RunnerEvent` contract is byte-for-byte unchanged, which is why §2.1's claim (cost guard / activity stream / stall handling all survive untouched) holds.

### PR 2 — Daytona provider for SandboxRunner

Daytona over E2B because it has first-class **persistent workspaces** (sandboxes that survive between commands, holding our git clone). That maps cleanly onto symphonyd's per-issue workspace assumption.

**Daytona SDK realities that shape the implementation** (verified against [`daytona.io/docs/en/python-sdk/async/async-process/`](https://www.daytona.io/docs/en/python-sdk/async/async-process/) on 2026-05-14):

1. `sandbox.process.exec()` is **non-streaming**: returns a complete `ExecuteResponse` after the command finishes. Usable for short utility commands (`git status`, `git push`) but *not* for long-lived agent runs that need live stdout.
2. Streaming requires the **session API**: `create_session`, `execute_session_command(run_async=True)`, then `get_session_command_logs_async(session_id, command_id, on_stdout, on_stderr)`. `OutputHandler` accepts sync or async callbacks.
3. **There is no per-command cancel.** The documented kill primitive is `delete_session()`, which tears down the whole session. To preserve the per-issue workspace across kills, we have to kill the running process *inside* the session (e.g., write the agent PID to a file and `kill <pid>` via another session command), not destroy the session itself.

That changes the provider sketch from iteration 1. Concrete shape:

```python
# src/symphony/agent/runners/sandbox_daytona.py
import asyncio
from collections.abc import AsyncIterator

from daytona import AsyncDaytona, SessionExecuteRequest

from ..runner import RunnerSpec


def _session_id_for(spec: RunnerSpec) -> str:
    # WorkspaceHandle for Daytona carries the sandbox id; the session
    # name is stable per-issue so Implement / Review-fix / Merge reuse it.
    return f"symphony-{spec.run_id}"  # or {issue_id} from the handle


class _DaytonaHandle:
    """One run inside one Daytona session. Implements SandboxHandle (§5 PR 1b)."""

    def __init__(self, sandbox, session_id: str, command_id: str) -> None:
        self._sandbox = sandbox
        self._session_id = session_id
        self._command_id = command_id
        self._queue: asyncio.Queue[tuple[str, str] | None] = asyncio.Queue()
        self._returncode_future: asyncio.Future[int] = asyncio.get_running_loop().create_future()
        self._pump_task = asyncio.create_task(self._pump())

    async def _pump(self) -> None:
        # Daytona's get_session_command_logs_async takes sync or async
        # callbacks; we push into a Queue so SandboxRunner's drain loop
        # can pull via .stream().
        async def on_stdout(line: str) -> None:
            await self._queue.put(("stdout", line))

        async def on_stderr(line: str) -> None:
            await self._queue.put(("stderr", line))

        try:
            await self._sandbox.process.get_session_command_logs_async(
                self._session_id, self._command_id, on_stdout, on_stderr,
            )
        finally:
            # Stream ended → command finished. Fetch exit code.
            info = await self._sandbox.process.get_session_command(
                self._session_id, self._command_id,
            )
            await self._queue.put(None)  # stream sentinel
            if not self._returncode_future.done():
                self._returncode_future.set_result(info.exit_code or 0)

    async def stream(self) -> AsyncIterator[tuple[str, str]]:
        while True:
            item = await self._queue.get()
            if item is None:
                return
            yield item

    async def wait(self) -> int:
        return await self._returncode_future

    async def cancel(self) -> None:
        # No per-command cancel in Daytona. Kill the running process by
        # PID written to /tmp/symphony.pid by the wrapper script that
        # exec'd the agent. Session and workspace stay alive.
        await self._sandbox.process.exec(
            "test -f /tmp/symphony.pid && kill -TERM $(cat /tmp/symphony.pid) || true",
        )
        # Best-effort SIGKILL after a grace period.
        await asyncio.sleep(5)
        await self._sandbox.process.exec(
            "test -f /tmp/symphony.pid && kill -KILL $(cat /tmp/symphony.pid) || true",
        )


class DaytonaProvider:
    """Implements SandboxProvider (§5 PR 1b)."""

    def __init__(self, image: str, *, secret_env: dict[str, str]) -> None:
        self._image = image
        self._secret_env = secret_env  # CODEX_API_KEY / ANTHROPIC_API_KEY / git PAT
        self._daytona = AsyncDaytona()

    async def start(self, spec: RunnerSpec) -> _DaytonaHandle:
        # 1. Acquire-or-create the sandbox for this (repo, issue).
        #    The handle is held in spec.workspace (a SandboxWorkspaceHandle).
        sandbox = await self._acquire_sandbox(spec)

        # 2. Acquire-or-create the session. One session per issue persists
        #    across Implement → Review-fix → Merge.
        session_id = _session_id_for(spec)
        await self._ensure_session(sandbox, session_id)

        # 3. Build the wrapped command: write PID to /tmp/symphony.pid so
        #    cancel() can find it, then exec the agent CLI.
        wrapped = "echo $$ > /tmp/symphony.pid && exec " + _shell_quote(spec.command)

        req = SessionExecuteRequest(
            command=wrapped,
            run_async=True,
            cwd=spec.workspace.remote_path,
            env={**self._secret_env, **spec.env},
        )
        result = await sandbox.process.execute_session_command(session_id, req)
        return _DaytonaHandle(sandbox, session_id, result.cmd_id)

    async def _acquire_sandbox(self, spec):
        # Look up by tag (binding.github_repo + issue_id). Create if missing.
        # Mirrors Workspace.acquire's idempotency contract (workspace.py:89).
        ...

    async def _ensure_session(self, sandbox, session_id: str) -> None:
        # No-op if exists, else create. Daytona returns an error on
        # duplicate create; the wrapper here swallows that and returns.
        ...
```

Daytona-specific decisions to make:
- **CLI binaries**: bake a `symphony-runner` image with `codex` + `claude` + `gh` + `git` preinstalled. Faster cold start than installing on every cold sandbox. Image-pinned by digest in symphonyd config so updates are reviewed. **This image and its CI pipeline don't exist today.** PR 2 scope includes:
    - `deploy/symphony-runner.Dockerfile` — minimal Debian/Alpine base, codex CLI from npm (pinned), claude CLI from Anthropic install script (pinned), `gh` from official apt repo, `git` from apt. ~150 MB stripped.
    - GitHub Action that rebuilds on Dockerfile changes and on pinned-version bumps, pushes to a container registry (GHCR or Daytona's), and updates a digest-pinned entry in `config.local.yaml` via PR.
    - **No secrets baked into the image.** Codex/Claude API keys, GitHub deploy keys, and per-binding scoped tokens all flow at runtime via Daytona's secrets API (see §9). The image's only purpose is to be the deterministic CLI environment.
    - Update cadence: pinned versions roll forward in a per-month maintenance PR, not on every upstream CLI release. Symphony's review classifier and cost-guard parser have implicit assumptions about CLI JSONL shapes; a surprise CLI upgrade can break those. The Dockerfile pin is the version contract.
- **Workspace clone path**: mirror the local layout — `/workspace/{repo_safe}/{issue_id}/`. The SandboxWorkspaceHandle from §5.0.1 carries this as `remote_path`.
- **Credentials**: sandbox env vars sourced from Daytona's [secrets API](https://www.daytona.io/docs/en/python-sdk/). Never bake into the image. Per-binding scope so VIB's GitHub PAT can't sign as ADJ's.
- **TTL**: match `DEFAULT_TTL_SECS = 7 * 24 * 3600` (`workspace.py:25`). Daytona supports `auto_stop_after_minutes`; configure to roughly that. The orchestrator's own TTL sweep (`workspace.py:127`) becomes a no-op for sandbox handles since the provider owns lifetime.
- **PID-tracking trick**: writing PID to `/tmp/symphony.pid` is the workaround for Daytona's lack of per-command cancel. It's ugly but it works and it preserves the persistent-workspace-across-stages property.

References:
- [Daytona Python SDK](https://www.daytona.io/docs/en/python-sdk/), [AsyncProcess](https://www.daytona.io/docs/en/python-sdk/async/async-process/), [process exec (sync)](https://www.daytona.io/docs/en/python-sdk/sync/process/)

### PR 3 — E2B provider for SandboxRunner (ephemeral, opt-in)

E2B is the lighter weight option — sub-second cold start, no per-issue persistence. For bindings where workspace reuse doesn't matter (one-shot Implement, no review-fix expected), this is cheaper and faster.

Same `Runner` interface, different provider. The decision between Daytona and E2B becomes a per-binding `runner:` config value: `daytona` for stateful, `e2b` for ephemeral.

References:
- [E2B pricing](https://e2b.dev/pricing) — $0.0504/vCPU-hr, per-second billing.

### PR 4 (optional, later) — Managed-agent venue as escape hatch

Architecture B is still useful for *one* class of binding: a totally untrusted repo where we don't want Symphony's CLI tokens to ever touch the code. Add `runner: "github_coding_agent"` for that single binding, accept the loss of cost guard / stall watchdog / activity comments, and treat the GitHub Coding Agent as a black box that produces a PR. Symphony's review and merge stages still run, against the PR the cloud agent opened.

This is *not* the default. It's an escape hatch.

## 6. Decision: should we do it?

**Build the option (PR 1a of §5), defer the deployment.** Architecture A is the right shape if we do it, but the production-volume math says we are not buying enough to justify deploying it today. Grounded in the production audit numbers (`docs/production-reliability-audit.md` §Evidence):

| Metric | Audited value |
|---|---|
| Implement runs (completed / failed) | 19 / 436 |
| Review runs (completed / failed) | 24 / 54 |
| Review-fix runs (completed / failed / interrupted) | 112 / 7 / 7 |
| **Total dispatched runs in the audit window** | ~659 |
| Single-issue worst case (VIB-1 retry storm) | 406 implement attempts over 31 h |

If we assume a 5-minute average run at Daytona/E2B's $0.0504/vCPU-hr:

```
659 runs × 5 min × ($0.0504/60) = $2.77 in sandbox-compute for the entire audited period.
```

That is **noise** compared to the LLM token spend on those runs (a single Implement at $1–2 of tokens dominates). So:

- Cost is not the reason to migrate.
- The current VPS (~€4/mo Hetzner CX22 class) is also not expensive enough to move *off*.
- What we are buying with Architecture A is concurrency-without-host-contention and per-run blast-radius isolation. Those are real but they are not dollars.

Concrete reasons the option is worth holding open:
- The 4-concurrent ceiling exists structurally; we have not yet been throughput-bound by it, but the audit found that VIB-1's 406-attempt retry storm did interfere with other bindings' dispatch (`production-reliability-audit.md`, "Existing work starved by new work"). The fix landed in the state machine, but the *fundamental* contention — one host's CPU/RAM serving every binding — remains.
- The audit shows 436 failed implement runs. Some unknown fraction are "stuck on host CPU pressure" or "stale workspace state" — both removed by per-run sandbox isolation. We do not have telemetry to apportion them; that's a missing signal, not evidence either way.
- The `Runner` protocol was designed for this. Landing PR 1a alone makes the abstraction *real* (rather than aspirational), which is most of the long-term value: any future contributor reading `runner.py` sees a working factory with the seam exercised, not a `Protocol` with one impl and an aspirational comment.

**No to Architecture B as the default.** The matrix in §4 makes the regression clear: we delete cost guard, stall watchdog, activity stream, and per-issue workspace caching. We built those because we needed them. The fact that managed agents handle "the whole thing" is not actually a win when "the whole thing" is exactly the part we've already specialized.

**Conditional: revisit Architecture B in 6 months** once Claude Managed Agents leaves beta and exposes per-session usage and cancel APIs at parity with our local enforcement. That's the only managed venue today that even attempts to surface the signals we need.

### 6.0.5 The cheap alternative: just buy a bigger VPS

A skeptical engineer reading §6 will say: "if concurrency is the issue, scale the box, not the architecture." That's a real third option this doc has glossed over, and it deserves a direct answer.

The Hetzner upgrade path (the typical deploy surface — VPS hosting the orchestrator):

| Plan | vCPU | RAM | €/mo | Realistic `global_max_concurrent` |
|---|---|---|---|---|
| CX22 (current) | 2 | 4 GB | ~€4 | 4 |
| CX32 | 4 | 8 GB | ~€8 | 8 |
| CX42 | 8 | 16 GB | ~€16 | 16–20 |
| CX52 | 16 | 32 GB | ~€32 | 32+ |

**Laptop orchestrator caveat.** Some operators run symphonyd locally on a laptop (the user's prompt explicitly allows for this). Modern dev laptops are 8–16 cores already, so forcing function #2 (concurrency ceiling) is weaker for that deploy: the bigger-box "upgrade" is free, and `global_max_concurrent: 16` is achievable today. For laptop operators, only forcing function #1 (isolation) and forcing function #3 (optionality) carry weight; the migration decision collapses to "isolation incident? then migrate" without a concurrency consideration. PR 1a still applies (forcing function #3 is venue-independent).

A CX42 (8 vCPU, €16/mo) gets us to ~20 concurrent agent runs on one host — the same number we'd be aiming for after migrating VIB/ADJ/LP to Daytona (per §8a). At €16/mo, that's *much* cheaper than the Daytona spend that comes with running 20 concurrent sandboxes (€16/mo Hetzner vs ~$25–60/mo sandbox compute at our run volume, plus the engineering cost of building PR 1a/1b/2).

So the §6 recommendation has to honestly address: **why don't we just buy CX42 and call it done?**

The answer is that "buy a bigger box" fixes exactly one of §1's three forcing functions:

| Forcing function | Bigger VPS | Sandbox migration |
|---|---|---|
| **(1) Per-issue blast radius / isolation** | ➖ No effect. One bad agent run still shares a kernel with 19 other runs and the orchestrator. The blast radius gets *worse* in absolute terms (more concurrent runs to be affected by one OOM/disk-full event). | ➕ Fixed by design — Firecracker microVM per run. |
| **(2) Concurrency ceiling** | ➕ Fixed. €16/mo gets us 8×. €32/mo gets us 16×. Trivial. | ➕ Fixed differently — horizontal scale via vendor billing. |
| **(3) Optionality / abstraction realness** | ➖ No effect on the `Runner` Protocol-with-one-impl problem. | ➕ PR 1a alone fixes this; sandbox impl is the proof. |

The honest read: **if concurrency is the only thing biting, buy CX42 first.** It's reversible, it's €12/mo more, and it doesn't require shipping code. Get the symphonyd-host migration to a Hetzner Cloud Server with an `EnvironmentFile=` over to systemd (`docs/production-reliability-audit.md` already alludes to this) and increase `global_max_concurrent` to 8. Run for a quarter. If that resolves the throughput issue, great — the migration was avoided cheaply.

**Sandbox migration is justified when:**

- (a) The concurrency-fix-via-bigger-box leaves isolation untouched and an incident makes that visible (per §6.1 trigger b), OR
- (b) We've already scaled to CX42 / CX52 and the LLM rate limit (§11 assumption #3) is *not* the real bind, so we genuinely want to go past per-host CPU limits.

This means the §6.2 monthly trigger watch should *also* track: how big is the orchestrator host currently? If we're still on CX22 when the concurrency-ceiling trigger fires, the answer is "bump to CX42 first; revisit in 30 days." Sandbox migration is the second move, not the first.

PR 1a is **still worth shipping** even in the bigger-VPS branch: it fixes forcing function #3 (abstraction realness) regardless of where the agents run. The §11.5 case-against is unchanged by this section.

### 6.1 When NOT to do this migration

To be honest about the limits of the recommendation: there is a case for keeping LocalRunner indefinitely.

- **If concurrency stays ≤ 4 and isolation incidents stay at zero.** The audit found no incident where one issue's run damaged another issue's workspace. The 4-concurrent ceiling is real but hasn't bitten us yet. If both of those remain true through the next quarter, the migration is a "we built a thing because we could" project, not a "we built a thing because we needed to" project.
- **If the operator burden of one more vendor is greater than the gain.** The audit's recurring lesson is that *we underestimated state-machine complexity*. Adding a new venue is a new set of failure modes (sandbox provisioning, transient WS disconnects, secret-rotation in the sandbox image, Daytona TTL evicting an in-progress workspace). Those will absorb engineering attention that could otherwise go to closing the open audit items (routing-quality preflight, adaptive backoff).
- **If we decide to deprecate the Claude binding entirely.** Then Architecture C becomes viable for the single remaining (Codex) backend, and we can skip the Daytona/E2B abstraction altogether.

So the actual decision criterion is: **do not migrate until either (a) we hit the concurrency ceiling in a way that's losing us Linear-issue throughput, or (b) we have a real isolation incident.** Until then, build PR 1a (the per-binding runner factory) as a refactor and stop. That keeps the option open without paying for it.

### 6.2 How we know a trigger has fired (measurement)

The two §6.1 conditions are unfalsifiable as written, so they're nearly useless as triggers. Operational thresholds we can actually watch:

**Trigger (a) — concurrency-ceiling-induced throughput loss.** Watch all three, fire when any exceeds threshold for two consecutive weeks:

| Signal | SQLite query / source | Threshold |
|---|---|---|
| % of poll cycles where `_dispatch_capacity` returns 0 for an issue that has a `ready` Linear state | `runs` joined with poll-cycle log timestamps; or instrument `_dispatch_capacity` to emit a metric | > 25% |
| Median time between Linear issue entering Todo and the first symphonyd run starting | `select avg(julianday(first_run.started_at) - julianday(issue.entered_ready_at)) …` (would require an `issue_state_history` table — currently approximate via `runs.created_at`) | > 30 min |
| Per-binding tail of issues waiting > 1 h for dispatch when at least one of the binding's slots was idle for that whole hour | join `runs` with binding capacity; the "idle slot + waiting issue" pattern indicates the *global* cap is the binder | any issue in 7-day window |

If the audit's existing query interface doesn't already cover these, PR 1a's companion is a 30-line `docs/concurrency-watch.md` runbook describing the exact `sqlite3` invocations.

**Trigger (b) — isolation incident.** Stricter — one event is enough. Counts as a trigger:
- An agent process in one binding's run wrote into or read from another binding's workspace dir (file-modified-time evidence in `logs/` cross-referenced against `runs.workspace_path`).
- An agent process in one run exfiltrated a secret meant for the orchestrator (egress logs, or a token used from an unexpected IP).
- A host-level OOM kill that took down the orchestrator process while only a subset of running agents were responsible.
- Disk-full on the orchestrator host caused by one binding's workspace, killing other bindings' runs.

Does *not* count as a trigger: an agent destroying its own workspace (that's contained — workspace TTL sweep deals with the cleanup; no other run is affected).

If neither trigger fires within 6 months of PR 1a landing, hold the migration. Re-evaluate the §1 forcing-function ordering — if isolation and concurrency really aren't hurting us, the abstraction's only standing value is optionality, which is what PR 1a was for.

**Sequencing note (per §6.0.5):** if trigger (a) — concurrency-induced throughput loss — fires while we are still on the small VPS (CX22 or equivalent), the first response is **bump the VPS plan, raise `global_max_concurrent`, watch for 30 days**. Only fire the sandbox-migration sequence if the bigger VPS doesn't resolve the throughput problem. Trigger (b) — isolation incident — goes straight to sandbox migration since CPU doesn't fix isolation.

## 7. Rollout

1. **PR 1a only, no production change.** Land the per-binding runner factory + `WorkspaceHandle` refactor (§5.−1 checklist). Every binding stays `runner: local`. The abstraction now exists; nothing observable changes. Stop here unless a §6.1 trigger fires.
2. **On trigger: land PR 1b + PR 2** (SandboxRunner skeleton + Daytona provider; the WorkspaceHandle and per-binding factory landed already as PR 1a) behind `runner: daytona` on **one binding** — VIB is the right first pick (highest volume, lowest external stakes).

   **Operational gotcha: raise `global_max_concurrent` at the same commit.** Today the cap is 4 (`config.local.yaml:7`), enforced regardless of runner venue (`poll.py:1493`, `:1549`, `:3282`). The whole point of moving VIB to Daytona is that VIB's runs stop consuming host CPU — but they keep consuming a global slot. If we don't raise the cap, the migration removes contention from where it never bit us (host CPU) while preserving contention exactly where it does (dispatch slots). Either:
    - Raise `global_max_concurrent` to N (where N ≥ sum of per-binding `max_concurrent`) at the same commit that flips VIB to Daytona, *or*
    - Add a separate cap for sandbox-runner bindings and exempt them from the global one.

    Pick the first; it's two characters of config. The second is over-engineered for a single-migration scenario.
3. **Two-week observation against an explicit success bar.** Migration is reverted (flip VIB back to `runner: local`) if any of:
    - Daytona cold-start p95 > 10 s on Implement (today: subprocess fork is <100 ms).
    - Activity-comment first-event latency p95 > 3× local baseline.
    - Cost guard fails to fire on an Implement run that should have hit the cap (catastrophic — there's a synthetic test in `test_cost_cap_e2e.py` we can mirror to Daytona).
    - Two or more `stall_timeout` events that turn out to be stream disconnects rather than real stalls (suggests we need PR-2.5: stream reconnect logic).
    - Sandbox-eviction of an in-progress workspace (suggests Daytona TTL is fighting our state machine).
4. **Compare prod-reliability audit numbers** against the local-runner baseline after two weeks: failed implement rate, mean time-to-first-PR, total tokens per merged issue. These come from `state.sqlite` queries identical to the audit's.
5. If steps 3–4 pass: flip ADJ + LP. Decommission the local-runner host's "production" role; keep it as the dev / fallback runner.
6. PR 3 (E2B provider) lands only if a binding actually wants ephemeral. Until then, leave it unimplemented — option value, not committed code.

### 7.0 Rollback plan per PR

Each PR has a defined revert. "Working on the hot path" is not an excuse for not having one.

| PR | Revert mechanics | Residual state to clean up |
|---|---|---|
| PR 1a (`WorkspaceHandle` + per-binding factory) | `git revert <sha>`. The refactor is purely structural; no schema migration, no on-disk format change. | None. SQLite is untouched. `state.sqlite` rows continue to work because `workspace_path: str` is still serialized as a path (the `LocalWorkspaceHandle` wraps the same `Path`). |
| PR 1b (`SandboxRunner` + `FakeSandboxProvider`) | `git revert <sha>`. No bindings reference `runner: daytona` yet, so the new code path is unreachable in prod. | None. Test files added in PR 1b also revert. |
| PR 2 (Daytona provider + image pipeline + first binding flipped) | Two-step revert: (1) flip the binding's `runner: daytona` → `runner: local` in `config.local.yaml` and restart the orchestrator (in-flight Daytona sandboxes finish naturally, then auto-stop reclaims them); (2) `git revert` the provider code. | **Real cleanup**: any in-flight Daytona sessions need `delete_session` either via the orchestrator's drain-on-shutdown path (PR 2 includes this) or manually via Daytona's dashboard. Daytona-side `state.sqlite` rows (sandbox IDs) get orphaned — harmless but should be swept by the §10.2 reconcile on next startup. **Time to revert**: ~10 minutes for orchestrator restart + manual session cleanup if drain didn't fire. |
| PR 3 (E2B provider) | Same shape as PR 2 but no production binding uses it yet — revert is `git revert`, no orchestrator restart needed. | None unless we already added an `runner: e2b` binding (we shouldn't have, per §5 PR 3's "ephemeral, opt-in" framing). |
| PR 4 (managed-agent escape hatch) | Flip the binding's `runner:` back to `local` or `daytona`. The vendor-side state (Codex Cloud PR, Copilot PR) survives independently — those PRs may still be open with our code in them. Decide per-incident whether to close them. | The vendor agent's open PRs need manual triage. This is the strongest argument for not using PR 4 as the default — managed-agent state escapes our control. |

**A revert is not a rollback.** A rollback returns the system to a known-good state; a revert just undoes a git commit. PR 1a's revert is also its rollback (no external state). PR 2's revert is a *partial* rollback — orphan sandboxes and Daytona's billing meter both keep ticking until `delete_session` fires. That's by design; immediate teardown would lose in-flight Implement work. The §10.2 reconcile path is the safety net.

**Acceptance criterion**: PR 2 cannot land until the orchestrator's drain-on-shutdown path explicitly calls `provider.shutdown()` on every active sandbox. Verified by a unit test that mocks the provider and asserts the call.

## 7.1 Testing strategy

The existing test suite already has the pattern we need. `tests/test_implement_e2e.py` defines `_FakeRunner(events)` that produces a synthetic `RunnerEvent` stream; the orchestrator is wired against it instead of `LocalRunner`. That's the right shape because `RunnerEvent` is the contract.

Test layering:

| Layer | What it tests | What it uses | Touches Daytona/E2B? |
|---|---|---|---|
| Unit — orchestrator/pipeline | All existing e2e / state-machine / cost-cap tests | `_FakeRunner` (existing) | No |
| Unit — `SandboxRunner` itself | Stream pumping, kill semantics, stall watchdog, spawn_failed mapping | `FakeSandboxProvider` (new — yields scripted `(kind, line)` tuples) | No |
| Integration — `DaytonaProvider` | Real `sandbox.process.exec` with `codex --help` against a small image | Real Daytona credits (a few cents) | Yes |
| Integration — `E2BProvider` | Same shape as above | Real E2B credits | Yes |
| End-to-end — one binding routed through Daytona | Implement → Review-fix → Merge full loop on a throwaway repo | Real Daytona + real Codex + real GitHub PAT scoped to test repo | Yes (paid: a few dollars per E2E pass) |

The point of the layering: 90%+ of the test surface (everything in the first row) is untouched by this migration. We're adding two new test files (`test_runner_sandbox.py`, `test_provider_daytona.py`) and a paid integration harness that runs nightly, not per-PR.

Concretely, mirror the existing fake pattern:

```python
# tests/test_runner_sandbox.py
class _FakeSandboxHandle:
    def __init__(self, lines: list[tuple[str, str]], returncode: int) -> None:
        self._lines = lines
        self._returncode = returncode
        self._cancelled = False

    async def stream(self):
        for kind, line in self._lines:
            yield kind, line

    async def cancel(self) -> None:
        self._cancelled = True

    async def wait(self) -> int:
        return self._returncode


class _FakeSandboxProvider:
    def __init__(self, handle: _FakeSandboxHandle) -> None:
        self.handle = handle

    async def start(self, spec):
        return self.handle
```

This unblocks unit tests for SandboxRunner's stall-timeout path, its kill ordering, and its spawn-failed propagation without ever talking to a real sandbox.

## 8. Steady state — what success looks like 12 months out

"Build PR 1a, then wait" risks reading as "do nothing." Two scenarios worth picturing so the recommendation has a target shape, not just a guardrail:

### 8a — Triggers fire (we ship the full migration)

Twelve months from now:
- VIB, ADJ, LP run on `runner: daytona` against a digest-pinned `symphony-runner` image. The Hetzner CX22 (or equivalent) still hosts the orchestrator, the Linear webhook receiver, and SQLite — that's all. No agent CLI is installed on the orchestrator host.
- `global_max_concurrent` is in the 16–32 range. The orchestrator's CPU floor is dominated by Linear/GitHub poll loops, not agent runs. Concurrency is bounded by LLM provider rate limits and per-binding budgets, not host hardware.
- Per-binding rotation of GitHub deploy keys and Codex/Claude API keys is a one-PR change to a secrets file; no SSH-into-the-host operation.
- The fallback path back to `runner: local` exists and is exercised in CI (a single-binding e2e test points at LocalRunner). Reverting one binding is a config-line change + restart.
- Architecture B (managed agents) is still not used by default. One binding — probably a public-fork "experimental" binding — might be routed to GitHub Copilot Coding Agent as the §5 PR 4 escape hatch, accepting the loss of cost guard and stream signals for runs where Symphony's own controls aren't needed.

### 8b — Triggers don't fire (we keep PR 1a as standing investment)

Twelve months from now:
- All bindings still run `runner: local`. The `Runner` factory has two registered implementations: `LocalRunner` and a single test impl (`_FakeRunner`). The `WorkspaceHandle` protocol has one concrete impl: `LocalWorkspaceHandle`.
- The doc you are reading is in `docs/remote-execution-research.md`; the §6.1 triggers have been re-checked against the audit numbers at least twice and have not been crossed.
- The abstraction has paid for itself in two specific ways even without a migration:
    - Anyone reading `runner.py` sees a real factory with a real second implementation slot, not a `Protocol` with one impl. Future contributors do not ask "is this abstraction load-bearing?"
    - The `WorkspaceHandle` refactor surfaced the `_push_fn` / `_gh.*` / `git rebase` split (§5.0) cleanly. Even without a sandbox, that split has made adding a self-hosted-VPS-clone-server-with-different-auth (a "renter binding" scenario) tractable rather than a global rewrite.
- The Architecture B revisit is scheduled but has not landed; managed-agents are still beta-quality and still drop the cost-guard signal.

Both scenarios are acceptable. Neither requires apology. The doc's contribution is to make the option *exist* with a clear price tag (PR 1a) and a clear trigger (§6.2).

## 9. Auth and secrets — what lives where

The split from §5.0 dictates the secrets layout. There are five distinct credentials in play, and post-migration they live in three different homes:

| Credential | Today | Under Architecture A (Daytona/E2B) | Rotation surface |
|---|---|---|---|
| Linear API key | env on orchestrator (`LINEAR_API_KEY`, `config.py:105`) | unchanged | orchestrator host / systemd `EnvironmentFile=` |
| Linear webhook secret | env on orchestrator (`LINEAR_WEBHOOK_SECRET`) | unchanged | orchestrator host |
| GitHub PAT for API calls (`gh`) | `~/.config/gh/hosts.yml` on orchestrator | unchanged — orchestrator keeps doing all `gh pr_*` REST calls | orchestrator host |
| GitHub deploy key / PAT for git clone+push | implicit via host `gh` auth | **moves into the sandbox image** (per-binding, scoped to the bound repo) | sandbox secret-manager / Daytona variables |
| Codex / Claude API key | `~/.codex` or `~/.claude` on orchestrator | **moves into the sandbox image** (passed as env var) | sandbox secret-manager |

Threat model deltas:

- **Orchestrator-host compromise**: same as today. Linear key + GitHub PAT exposed. Not improved by migration. Not made worse, either.
- **Sandbox-image compromise**: new failure mode. A malicious change to the bundled `symphony-runner` image (or to the secrets pushed into a running sandbox) leaks the GitHub git-credential and the Codex/Claude API key for the duration of the leak. Mitigation: pin the image digest in config, gate image promotion behind code review, and prefer per-binding scoped credentials over a single org-wide key so blast radius is one binding's repo + that binding's LLM spend.
- **Per-run isolation**: ➕ unlike LocalRunner today, the agent process *cannot* see the orchestrator's Linear key, gh PAT, or other bindings' credentials. The only secret in the sandbox is the one needed for that binding's run. This is genuinely better than the status quo, where every agent process inherits the orchestrator's full env (`runners/local.py:39`).
- **Credential rotation**: a per-binding GitHub deploy key + per-binding LLM API key means rotation is a per-binding operation, not an "everything down" operation. Big win for ops.
- **Egress**: Architecture A puts the sandbox on someone else's network (Daytona's, E2B's). Outbound calls from the agent — including any `curl`, `npm install`, `pip install` it does mid-run — now flow through the vendor. That's another logging surface to think about. Daytona and E2B both expose per-sandbox network logs; treat them like CI logs (sensitive, but not as sensitive as secrets).

The rotation story improves; the supply-chain story regresses (we now depend on the sandbox image and the vendor's sandbox kernel). On balance, isolation is the larger gain.

## 10. Operational sharp edges

These are not "open questions" — they have answers; they're just easy to forget if you read the doc in a hurry.

### 10.1 Cost-cap kill latency

Local: cost cap exceeded → `_kill_active_runner` → `os.killpg(pid, SIGTERM)` → microseconds. The agent stops emitting tokens before the next `parse_event_line` even runs.

Sandbox: cost cap exceeded → `_kill_active_runner` → `runner.kill(run_id)` → `handle.cancel()` → `sandbox.process.exec("kill -TERM …")` over network → seconds. The agent keeps emitting tokens during the round trip.

Back-of-envelope at Sonnet 4.x output pricing (~$15/Mtok) and ~10 output-tokens/sec emission:

```
3 s kill latency × 10 tok/s × $15/Mtok = $0.00045 per kill event.
```

Acceptable. The cost-cap-kill path is rare (only fires when an Implement run is genuinely runaway), and the overshoot is rounding-error money. **No design change needed**, but the operational metric "median time from cap-breach to last stdout line" should appear in the post-migration dashboard as a sanity check — if it climbs into double-digit seconds, the SIGKILL fallback is broken.

### 10.2 Orphaned sandbox reconcile

Daytona's `auto_stop_after_minutes` reclaims idle sandboxes, but only the *sandbox*. If the orchestrator crashes mid-Implement and forgets about a sandbox that's still actively producing tokens, auto-stop won't fire and we pay for compute we'll never read.

Mitigation:
- On startup, the orchestrator reconciles its `runs` table against the provider's "list all sandboxes for this Symphony deployment" endpoint. Any sandbox whose `run_id` is in a `failed`/`interrupted`/`completed` state in SQLite but still running in Daytona gets a `delete_session()` call. Mirrors the existing reconcile pattern in `orchestrator/reconcile.py`.
- Tag every sandbox with a deployment-id label at creation so the reconcile query is scoped (do not stomp another symphonyd deployment's sandboxes in shared dev accounts).

### 10.3 Stream-disconnect resilience

Local pipes don't disconnect mid-run. Remote WebSocket streams do. A 5-second blip between Daytona and the orchestrator should not become a `stall_timeout` (cost-cap would fire kill on a still-running agent).

`SandboxRunner.run()` (§5 PR 1b sketch) needs reconnect logic *before* the watchdog fires. Concretely: if `handle.stream()` raises a transient transport error, retry the stream with bounded backoff (≤3 retries, ≤10 s total). Only after exhausted retries do we yield `stall_timeout`. This is provider-specific — Daytona's session log endpoint supports resuming from a known cursor; E2B's WS reconnects automatically. Document per-provider.

### 10.4 Activity-comment latency drift

Local: subprocess fork to first stdout ≈ 50 ms. Activity-comment "🚀 starting" appears in Linear within ~1 second of dispatch.

Sandbox: provision (cold) ≈ 1–5 s on Daytona/E2B, ≈ 30 ms on a warm session. First stdout from `codex exec` adds another ~500 ms (CLI startup + first event). So the "🚀 starting" comment appears in Linear within ~3–6 s on cold start, ~1–2 s on warm.

This shows up in the user experience: an issue moves from Todo → In Progress almost immediately (Linear state transition is orchestrator-side, no sandbox involved), but the first activity comment lags more than today. Not a regression in correctness; might be a regression in perceived responsiveness. Worth a single-line CHANGELOG entry the day we flip the first binding.

## 11. Where this analysis could be wrong

The recommendation depends on several assumptions that look load-bearing right now but could rot. Documenting them so a future reader knows what to revisit:

1. **CLI JSONL schema stability.** Cost guard (`agent/process.py`) parses `result`, `token_count`, and `turn.completed` events emitted by codex and claude CLIs. If either CLI changes its event schema between versions (Codex CLI is moving fast — see the May 2026 changelog references in §14), the cost-guard parser breaks regardless of venue. This is true today; sandbox-execution doesn't make it worse, but it doesn't make it better either. *The doc's recommendation does not depend on this assumption holding* — it's just the existing fragility.

2. **Daytona session API parity with docs.** §5 PR 2's implementation assumes `create_session`, `execute_session_command(run_async=True)`, `get_session_command_logs_async(on_stdout, on_stderr)`, and `delete_session` exist with the documented signatures. Verified against [daytona.io/docs/en/python-sdk/async/async-process/](https://www.daytona.io/docs/en/python-sdk/async/async-process/) on 2026-05-14. If Daytona changes the API (especially adds a per-command cancel, which would let us drop the PID-tracking trick), revisit §5 PR 2.

3. **The 4-concurrent ceiling really is the production-throughput ceiling.** §1 and §6.1 both assume `global_max_concurrent: 4` is what we'd be bumping into. If the real ceiling is the *LLM provider rate limit* — which neither this doc nor the audit measured — then moving to sandboxes does nothing for throughput and we are buying isolation only. The §6.2 measurement explicitly watches dispatch-capacity-zero events, which would falsify this if rate-limits were the real bind (we'd see capacity available but runs failing on 429s instead).

4. **Per-binding `runner:` truly is the right granularity.** §5 PR 1a assumes per-binding is enough. If we discover a need for per-issue or per-stage runner selection (e.g., "Implement on Daytona, Merge on local because Merge is sub-second and a sandbox round-trip is pure overhead"), the per-binding factory needs another seam. Possible but not designed for today; flag if it comes up.

5. **The Architecture B veto is correct.** §4's matrix says managed agents drop cost guard, stall watchdog, activity stream, and review-fix loop. That's true *as of 2026-05-14*. Claude Managed Agents is in beta and the surface is growing. If/when the beta header (`managed-agents-2026-04-01`) is replaced and the session API gains parity with our local enforcement (specifically: a cancel endpoint and per-event token usage), §3 Architecture B should be re-evaluated. The current veto is venue-state-specific, not architectural.

6. **The audit's run-count is representative of the next quarter.** §6's $2.77-per-audit-period cost calc assumes runs continue at roughly the audited rate. A 5–10× growth (new bindings, larger backlog) does not change the recommendation, but it shifts the urgency calibration — at 10× volume, §6.2's measurement thresholds should be tightened proportionally.

7. **Daytona's sub-100 ms cold start claim is marketing.** §6.2 makes "cold start under load" an §13 open question for exactly this reason. The recommendation does not hinge on the marketing number; the rollout's go/no-go bar (§7 step 3) tests the cold start empirically before committing.

If any of (1)–(7) changes, re-read §6 and §6.2. The TL;DR's recommendation (ship PR 1a, wait on §6.2 triggers) is robust against (1), (3), and (6); it's brittle against (2), (4), (5), and (7) — so check those when revisiting.

## 11.5 The case against PR 1a itself

§11 documents where the doc's *premises* could be wrong. This subsection critiques the *recommendation* with the same scrutiny — the skeptic's read on whether PR 1a should ship at all.

The recommendation is "build PR 1a, then wait." That has a defensible counter-case:

- **It's pure abstraction work that no current user or operator asked for.** No Linear issue says "the `Runner` protocol needs a real second impl." No production incident points at LocalRunner. We're shipping code to make a future migration *easier* — which is exactly the kind of work that historically tends to drift, get reverted, or become a museum piece (see: the v1 codex CLI integration assumptions, the deprecated webhook receiver code paths). If the §6.2 triggers never fire, PR 1a is 1 day of refactor that bought nothing.
- **`WorkspaceHandle` adds an indirection that nobody currently needs.** Today, `Workspace.acquire` returns a `Path`. That's blunt but obviously correct. After PR 1a, it returns a `WorkspaceHandle` whose concrete implementation is `LocalWorkspaceHandle(path=...)`. Every call site that wanted a `Path` now goes through `.path` and an `isinstance` check. That's *more* code reading harder for zero behavior change, which is the standard YAGNI critique of premature interfaces.
- **The refactor itself could introduce a regression.** Search-and-replace across ~10 call sites in `poll.py` is the kind of change that hits "looks right, tests pass, fails in one branch the tests don't cover." The cost-guard kill path (`_kill_active_runner`, `poll.py:5080`), the merge-conflict recovery's `_force_push_fn` call (`poll.py:2664`), and the failed-implement waits all touch the affected surfaces. Zero-production-risk is an aspiration, not a guarantee.
- **The "optionality" argument is weak in isolation.** §1 forcing function #3 says PR 1a is worth shipping because it proves the abstraction. But that's only valuable *if* someone later tries to write the second impl. If the §6.2 triggers don't fire in 12 months and the doc gets sunset (§12), PR 1a's only legacy is the indirection in §5.−1's table.

What the steel-man would say next: ship nothing. Leave `LocalRunner` as the only impl, accept the `Path`-as-descriptor lie in `runner.py:25` as a comment that nobody reads, and revisit only if a trigger genuinely fires. At that point we write PR 1a *plus* the SandboxRunner skeleton in one go, because we're actually going to use it.

**Why the doc still recommends shipping PR 1a anyway:**

1. The "optionality drift" risk is asymmetric. If we ship PR 1a now and the triggers never fire, we lose 1 day. If we don't ship PR 1a and a trigger fires in month 4, we need PR 1a *and* PR 1b *and* PR 2 *under pressure of a real production problem* — exactly when this kind of refactor is hardest to land cleanly.
2. The `Path`-as-descriptor comment in `runner.py:25` is a small but real lie in the codebase. Comments that hedge a type ("Path for local runner; descriptor for sandbox runners") are signals that the type is wrong. PR 1a makes the type honest.
3. The §5.−1 checklist is small enough to review in one sitting. The diff is contained, the tests pass unchanged, and the rollback is one commit. The asymmetry of "we lose 1 day if we're wrong, we save 1 month if we're right" justifies the work.
4. PR 1a's `WorkspaceHandle` refactor independently makes the gh-vs-git split (§5.0) explicit. Even with zero migration appetite, that split makes adding a hypothetical "renter binding" scenario (one Linear team, a totally different repo-clone-server) much more tractable. The abstraction earns its keep on a single non-migration axis.

Net: the case against is real, the case for is slightly stronger. A team that disagreed could reasonably reject PR 1a on YAGNI grounds; the doc would still serve as documentation for *when* to revisit. The recommendation is "ship," but it's a close call, not a slam dunk.

## 12. Ownership and cadence

The recommendation "build PR 1a, then wait" requires someone to actually watch the §6.2 triggers, or the wait turns into "we built the thing and forgot." Proposed shape:

- **Owner**: whoever currently owns symphonyd's production reliability (per the prior `production-reliability-audit.md`, that's the same person who handled the May 2026 audit). One human, not a team rotation.
- **Cadence**:
    - **Monthly**, owner runs the three SQLite queries from §6.2 against `state.sqlite`. Five-minute task. Output goes into a one-line audit log in `docs/production-reliability-audit.md`.
    - **Per-incident**, owner classifies any production anomaly as "isolation incident (§6.1 trigger b)" or not. The default is "not"; promoting requires one of the four specific patterns in §6.2.
    - **Semi-annual (every 6 months)**, owner re-reads this doc's §6, §6.1, §8, and §13 with the previous 6 months of data and decides: do triggers still apply, should the recommendation flip?
- **Escalation**: if a trigger fires, owner files a 2-line decision note in `docs/`. Migration restart (PR 1b → PR 2 → rollout per §7) is then a multi-week project, not an emergency — start in the following sprint.
- **Sunset**: if 12 months pass with no trigger, owner can mark this doc Status: `accepted-no-action-needed` and stop the monthly check. The §6.2 queries are still useful audits even without remote execution on the horizon.

The ownership entry in the doc header at the top should be filled in when this doc is accepted.

## 13. Open questions

Items already addressed elsewhere — kill latency (§10.1), orphan reconcile (§10.2), stream-disconnect (§10.3), activity-comment latency (§10.4), credential rotation (§9), cost-cap pre-flight (folded into §10.1).

Genuinely unresolved:

- **Cold start under load.** Daytona claims sub-100 ms cold start; that's marketing. What does it look like for 4 simultaneous Implement spawns from a cold orchestrator state? E2B sub-second per docs is unverified at our concurrency. Measure before flipping the second binding.
- **Workspace lock semantics across venues.** `Workspace._hold_lock` (`workspace.py:62`) serializes acquire vs sweep on the orchestrator host. Daytona's persistent workspace has its own concurrency model. Two stages of the same issue probably won't run concurrently anyway (state machine prevents it), but verify with an integration test.
- **Should the orchestrator itself move to Fly Machines or similar?** Independent question from this one. The Rust research's recommendation #2 (Fly + LocalRunner) becomes (Fly orchestrator + Daytona/E2B for runs) under Architecture A. Worth a separate pass — the orchestrator-side process is single-machine SQLite-bound, so the answer is probably "no, keep the VPS until the SQLite ceiling bites."
- **Webhook flow under sandbox** (confirmation, not a question): Linear slash commands (`/stop`, `/approve`, `/retry`) come in via `webhook.py` → orchestrator → `runner.kill(run_id)`. The kill path is the only sandbox-side dependency, and §10.1 quantified its overshoot. The webhook flow itself is unchanged.

## 14. References

- [OpenAI Codex web (cloud agent)](https://developers.openai.com/codex/cloud)
- [Codex CLI headless mode](https://developers.openai.com/codex/noninteractive)
- [Codex GitHub integration](https://developers.openai.com/codex/integrations/github)
- [Codex App Server (JSON-RPC over WebSocket)](https://developers.openai.com/codex/app-server)
- [Codex remote connections (`--remote ws://`)](https://developers.openai.com/codex/remote-connections)
- [GitHub Copilot Coding Agent](https://docs.github.com/copilot/concepts/agents/coding-agent/about-coding-agent)
- [Claude Managed Agents overview](https://platform.claude.com/docs/en/managed-agents/overview)
- [Claude Managed Agents sessions API](https://platform.claude.com/docs/en/managed-agents/sessions)
- [Claude API pricing (incl. $0.08/session-hour for Managed Agents)](https://platform.claude.com/docs/en/about-claude/pricing)
- [Claude Agent SDK hosting](https://platform.claude.com/docs/en/agent-sdk/hosting)
- [Daytona Python SDK](https://www.daytona.io/docs/en/python-sdk/), [process exec](https://www.daytona.io/docs/en/python-sdk/sync/process/)
- [E2B pricing](https://e2b.dev/pricing)
- [Daytona vs E2B vs Modal vs Vercel Sandbox 2026 comparison](https://www.startuphub.ai/ai-news/artificial-intelligence/2026/daytona-vs-e2b-vs-modal-vs-vercel-sandbox-2026)
- [AI Sandbox pricing comparison 2026 (Northflank)](https://northflank.com/blog/ai-sandbox-pricing)
- Internal: `../SymphonyMac/docs/python-port-research.md` §6.3, §15, §16
- Internal: `docs/production-reliability-audit.md`
