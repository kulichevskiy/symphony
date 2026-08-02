# EXP-4CF9CF210C8A — partial report

## Outcome

- Status: `failed` after an intentional stop.
- Stop reason: EventDesk is too long for the V1 benchmark. The campaign is being replaced by the two-ticket Feedback Inbox campaign.
- This is not a candidate-quality result and must not be used in an A-A comparison.
- Trial: `S0` / candidate `S`, repetition `0`.
- Started: `2026-08-02T16:09:28.337128+00:00`.
- Stopped: `2026-08-02T20:22:57.403274+00:00`.
- Wall time: `15,207.997 s` (`4 h 13 m 28 s`).

## Versions

- Candidate revision: `432b21a664db9d54fb7831350ea063652410703d`.
- Candidate A system version: `157464f6ed1b6bdd`.
- Candidate B system version: `157464f6ed1b6bdd`.
- Harness version: `ba18f5cba56e3b73`.
- Executor toolchain: `uv=0.12.1;codex=0.146.0;claude-code=2.1.220;node=22`.

## Delivery progress

- Linear Project: `EventDesk V1 · 2026-08-02 · 4CF9CF210C8A`.
- Completed and merged: `BENCH-97`, `BENCH-98`, `BENCH-99`, `BENCH-100`.
- Interrupted: `BENCH-101` during implementation/local review.
- Not started: `BENCH-102`.
- Repository: `kulichevskiy/EXP-4CF9CF210C8A-SMOKE`.
- Merged PRs: `#1`, `#2`, `#3`, `#4`.

## Usage

- Agent launches: `59`.
- Active agent time: `10,293.999 s` (`2 h 51 m 34 s`).
- Raw input tokens at the final pre-stop snapshot: `13,794,782`.
- Raw output tokens at the final pre-stop snapshot: `178,679`.
- Cache-read tokens at the final pre-stop snapshot: `12,532,736`.
- Cache-write tokens: `0`.
- Effective tokens: `15,226,734.6`.
- Recorded cost at the final pre-stop snapshot: `$18.0728`.

## Review

- Local-review agent launches: `34`.
- Local-review rounds: `13`.
- Local-review findings: `20` — critical `1`, major `11`, minor `8`, unclassified `0`.
- Unparseable local-review rounds: `0`.
- Remote-review rounds completed: `4`.
- Remote Codex inline comments: `19` — P0 `0`, P1 `2`, P2 `17`, P3 `0`.
- Remote comments by PR: PR `#1` — `13`; PR `#3` — `3`; PR `#4` — `3`; PR `#2` — `0`.

## Checks and errors

- Completed Symphony run records: `28 completed`, `4 done`.
- Failed run records on intentional shutdown: `2`.
- Hidden grader: not reached; no product score exists.
- Trial error: `bench worker stopped during this trial`.
- The report uses the final control-plane receipt plus the last candidate-database snapshot. The executor removed its trial workspace after returning the cancellation receipt.

## Next step

1. Remove the S0 trial from the orchestration and replace it with a cheap infrastructure preflight.
2. Add mandatory per-trial Markdown reports and chat-notification receipts.
3. Replace EventDesk with Feedback Inbox: one backend ticket, then one dependent frontend ticket.
4. Deploy the exact new SHA and run A1/B1, A2/B2, and A3/B3 in parallel pairs.
