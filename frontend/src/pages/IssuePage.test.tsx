import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import {
  aggregateRunsByStage,
  CmdButton,
  ConfirmBar,
  deriveCockpit,
  FinalLogCard,
  NowCard,
  pickDefaultRun,
  pickLiveRun,
  rawDetailJson,
  StageSpendCard,
  TokensCard,
} from "./IssuePage";
import { applicability, COMMANDS } from "./issueControls";
import type { IssueDetail } from "@/lib/api";

function run(stage: string, tok: Partial<Record<string, number | string | boolean | null>>) {
  return {
    id: stage + Math.random(),
    stage,
    status: "done",
    pid: null,
    started_at: "2026-06-07T10:00:00Z",
    ended_at: null,
    input_tokens: 0,
    output_tokens: 0,
    cache_write_tokens: 0,
    cache_read_tokens: 0,
    termination_kind: "",
    termination_detail: "",
    exit_returncode: null,
    has_log: false,
    ...tok,
  };
}

const stageRuns = [
  run("implement", { input_tokens: 100, output_tokens: 40, cache_write_tokens: 5, cache_read_tokens: 1000 }),
  run("implement", { input_tokens: 50, output_tokens: 10 }),
  run("review", { input_tokens: 20, output_tokens: 0 }),
  run("merge", { output_tokens: 50 }),
];

describe("applicability", () => {
  it("enables only Stop while a run is in progress", () => {
    const { en } = applicability("running");
    expect(en.stop).toBe(true);
    expect(en.approve).toBe(false);
    expect(en.retry).toBe(false);
  });

  it("enables Retry / Retry-acceptance on failure", () => {
    const { en } = applicability("failed");
    expect(en.retry).toBe(true);
    expect(en["retry-acceptance"]).toBe(true);
    expect(en.stop).toBe(false);
  });

  it("enables review actions while awaiting review", () => {
    const { en } = applicability("awaiting_review_trigger");
    expect(en.approve).toBe(true);
    expect(en.reject).toBe(true);
    expect(en["skip-review"]).toBe(true);
  });

  it("disables everything once done", () => {
    const { en } = applicability("done");
    expect(Object.values(en).every((v) => v === false)).toBe(true);
  });
});

describe("CmdButton", () => {
  it("greys out and exposes the reason when not applicable", () => {
    const markup = renderToStaticMarkup(
      <CmdButton
        id="approve"
        enabled={false}
        why="Nothing to approve — run failed"
        applied={false}
        busy={false}
        onClick={() => {}}
      />,
    );
    expect(markup).toContain("disabled");
    expect(markup).toContain("Nothing to approve");
  });

  it("shows the applied state", () => {
    const markup = renderToStaticMarkup(
      <CmdButton
        id="approve"
        enabled
        why=""
        applied
        busy={false}
        onClick={() => {}}
      />,
    );
    expect(markup).toContain("Applied");
    expect(markup).toContain("bg-green-50");
  });

  it("is a full-width, phone-sized tap target on mobile and compact on desktop", () => {
    // One-handed reach: approve/reject fill the row and hit 44px on phones,
    // collapsing to inline, compact buttons from the sm breakpoint up.
    const markup = renderToStaticMarkup(
      <CmdButton
        id="approve"
        enabled
        why=""
        applied={false}
        busy={false}
        onClick={() => {}}
      />,
    );
    expect(markup).toContain("h-11");
    expect(markup).toContain("sm:h-9");
    expect(markup).toContain("w-full");
    expect(markup).toContain("sm:w-auto");
  });
});

describe("ConfirmBar", () => {
  it("makes the destructive confirm button a full-width, phone-sized tap target too", () => {
    // The confirm tap completes a reject/stop flow, so it needs the same
    // one-handed 44px/full-width-on-mobile treatment as CmdButton.
    const markup = renderToStaticMarkup(
      <ConfirmBar c={COMMANDS.reject} onCancel={() => {}} onConfirm={() => {}} />,
    );
    expect(markup).toContain("h-11");
    expect(markup).toContain("sm:h-9");
    expect(markup).toContain("w-full");
    expect(markup).toContain("sm:w-auto");
  });
});

const cockpit = {
  status: "awaiting_review_trigger",
  stage: "review",
  runState: "waiting" as const,
  since: "2026-06-07T15:28:00Z",
  activity: "2026-06-07T16:34:00Z",
  reason: null,
  tokens: {
    input_tokens: 2_100_000,
    output_tokens: 184_000,
    cache_write_tokens: 412_000,
    cache_read_tokens: 7_900_000,
  },
  byModel: [
    {
      provider: "claude",
      model: "claude-opus-4-8",
      input_tokens: 2_000_000,
      output_tokens: 180_000,
      cache_write_tokens: 400_000,
      cache_read_tokens: 7_800_000,
    },
    {
      provider: "codex",
      model: "gpt-5.5",
      input_tokens: 100_000,
      output_tokens: 4_000,
      cache_write_tokens: 12_000,
      cache_read_tokens: 100_000,
    },
  ],
  pr: {
    number: 412,
    repo: "kulichevskiy/adjust_os",
    url: "https://github.com/kulichevskiy/adjust_os/pull/412",
    state: "open",
    mergeable: "mergeable",
    merged: false,
    checks: { passing: 11, failing: 0, pending: 1 },
  },
  waitingOn: "review",
};

describe("TokensCard", () => {
  it("renders four equal stat blocks and no summed total, no dollars", () => {
    const markup = renderToStaticMarkup(<TokensCard c={cockpit} />);
    expect(markup).not.toContain("$");
    expect(markup).not.toContain("cap");
    expect(markup).toContain("Tokens");
    // No summed total (10.6M) anywhere — only the four explicit figures.
    expect(markup).not.toContain("10596000");
    expect(markup).not.toContain("10.6M");
    expect(markup).toContain('title="2100000">2.1M</span>');
    expect(markup).toContain('title="184000">184k</span>');
    expect(markup).toContain('title="412000">412k</span>');
    expect(markup).toContain('title="7900000">7.9M</span>');
  });

  it("labels each token stat with its shared-palette swatch", () => {
    const markup = renderToStaticMarkup(<TokensCard c={cockpit} />);
    // The four stat blocks carry the shared TOKEN_CATS swatches (square chips),
    // so the token rail doubles as the colour key for the breakdown bars below.
    expect(markup).toContain("rounded-sm bg-blue-500");
    expect(markup).toContain("rounded-sm bg-violet-500");
    expect(markup).toContain("rounded-sm bg-cyan-500");
    expect(markup).toContain("rounded-sm bg-slate-300 dark:bg-slate-600");
  });

  it("breaks tokens down by provider and model with a mix-bar", () => {
    const markup = renderToStaticMarkup(<TokensCard c={cockpit} />);
    expect(markup).toContain("by provider / model");
    expect(markup).toContain("claude");
    expect(markup).toContain("claude-opus-4-8");
    expect(markup).toContain("codex");
    expect(markup).toContain("gpt-5.5");
    // Proportional mix-bars, no provider/model summed total rendered.
    expect(markup).toContain("width:");
    expect(markup).not.toContain("10380000");
  });
});

describe("aggregateRunsByStage", () => {
  it("sums runs per stage with exact per-run totals, in pipeline order", () => {
    const { rows, reached, total } = aggregateRunsByStage(stageRuns);
    expect(rows.map((r) => r.key)).toEqual([
      "implement",
      "local_review",
      "review",
      "review_fix",
      "merge",
      "acceptance",
    ]);
    const impl = rows.find((r) => r.key === "implement")!;
    expect(impl.input_tokens).toBe(150);
    expect(impl.output_tokens).toBe(50);
    expect(impl.reached).toBe(true);
    expect(rows.find((r) => r.key === "local_review")!.reached).toBe(false);
    // "Reached" = ≥1 run in the stage; M = canonical seen-stage list.
    expect(reached).toBe(3);
    expect(total).toBe(6);
  });

  it("appends a non-canonical stage after the known pipeline", () => {
    const { rows, reached, total } = aggregateRunsByStage([
      run("implement", { output_tokens: 5 }),
      run("mystery", { output_tokens: 7 }),
    ]);
    expect(rows.map((r) => r.key).at(-1)).toBe("mystery");
    // Non-canonical stages don't inflate the N/M count.
    expect(reached).toBe(1);
    expect(total).toBe(6);
  });
});

describe("StageSpendCard", () => {
  it("shows reached/canonical count, greys unreached, output share, raw cols", () => {
    const markup = renderToStaticMarkup(<StageSpendCard runs={stageRuns} />);
    expect(markup).toContain("Spend by lifecycle stage");
    expect(markup).toContain("3/6 reached");
    expect(markup).toContain("Implement");
    expect(markup).toContain("Local review");
    expect(markup).toContain("Merge");
    // Unreached stages greyed.
    expect(markup).toContain("opacity-40");
    // Bars/share use output tokens — implement & merge each 50% of output.
    expect(markup).toContain("50%");
    // Table shows raw token categories with exact per-run sums.
    expect(markup).toContain("CACHE-WRITE");
    expect(markup).toContain('title="150">150</span>');
  });
});

describe("pickDefaultRun", () => {
  it("surfaces the most-recent failed/interrupted run with a log, even under a newer success", () => {
    const runs = [
      run("merge", { id: "r-merge", status: "done", has_log: true, started_at: "2026-06-07T13:00:00Z" }),
      run("implement", { id: "r-fail", status: "failed", has_log: true, started_at: "2026-06-07T12:00:00Z" }),
      run("implement", { id: "r-old-fail", status: "interrupted", has_log: true, started_at: "2026-06-07T09:00:00Z" }),
    ];
    expect(pickDefaultRun(runs)?.id).toBe("r-fail");
  });

  it("falls back to the most-recent run with a log when none failed", () => {
    const runs = [
      run("implement", { id: "r-old", status: "done", has_log: true, started_at: "2026-06-07T09:00:00Z" }),
      run("merge", { id: "r-new", status: "done", has_log: true, started_at: "2026-06-07T13:00:00Z" }),
    ];
    expect(pickDefaultRun(runs)?.id).toBe("r-new");
  });

  it("skips a run without a log for the older run that has one", () => {
    // A synthetic merge-approval park row (no subprocess) never gets a
    // `{run_id}.log`, so `has_log: false` — it must not shadow the run with
    // real output.
    const runs = [
      run("merge", { id: "r-park", status: "needs_approval", has_log: false, started_at: "2026-06-07T14:00:00Z" }),
      run("implement", { id: "r-impl", status: "completed", has_log: true, started_at: "2026-06-07T12:00:00Z" }),
    ];
    expect(pickDefaultRun(runs)?.id).toBe("r-impl");
  });

  it("returns null for no runs", () => {
    expect(pickDefaultRun([])).toBeNull();
  });

  it("prefers a failed run with a log over a newer failed run without one", () => {
    const runs = [
      run("review", { id: "r-review", status: "failed", has_log: false, started_at: "2026-06-07T14:00:00Z" }),
      run("implement", { id: "r-impl-fail", status: "failed", has_log: true, started_at: "2026-06-07T12:00:00Z" }),
    ];
    expect(pickDefaultRun(runs)?.id).toBe("r-impl-fail");
  });

  it("falls back to the newest run when nothing has a log", () => {
    const runs = [
      run("review", { id: "r-review", status: "needs_approval", has_log: false, started_at: "2026-06-07T14:00:00Z" }),
      run("implement", { id: "r-impl", status: "completed", has_log: false, started_at: "2026-06-07T12:00:00Z" }),
    ];
    expect(pickDefaultRun(runs)?.id).toBe("r-review");
  });

  it("skips a superseded duplicate for the older surviving run", () => {
    // Startup reconcile marks the younger duplicate of a collapsed live run
    // "superseded" — pure bookkeeping that must not shadow the survivor once
    // it later completes.
    const runs = [
      run("implement", { id: "r-dup", status: "superseded", has_log: true, started_at: "2026-06-07T13:00:00Z" }),
      run("implement", { id: "r-survivor", status: "completed", has_log: true, started_at: "2026-06-07T12:00:00Z" }),
    ];
    expect(pickDefaultRun(runs)?.id).toBe("r-survivor");
  });

  it("falls back to a superseded run only when nothing else exists", () => {
    const runs = [run("implement", { id: "r-only", status: "superseded" })];
    expect(pickDefaultRun(runs)?.id).toBe("r-only");
  });

  it("skips an interrupted merge displaced by interrupt_running_merge (termination_kind=superseded)", () => {
    // interrupt_running_merge (and the orphaned-approval cleanup) stamp the
    // displaced row status="interrupted"/termination_kind="superseded" rather
    // than status="superseded" — it must not win over a real failed run just
    // because FAILED_RUN_STATUSES includes "interrupted".
    const runs = [
      run("merge", {
        id: "r-displaced",
        status: "interrupted",
        termination_kind: "superseded",
        has_log: true,
        started_at: "2026-06-07T13:00:00Z",
      }),
      run("implement", { id: "r-fail", status: "failed", has_log: true, started_at: "2026-06-07T12:00:00Z" }),
    ];
    expect(pickDefaultRun(runs)?.id).toBe("r-fail");
  });
});

describe("pickLiveRun", () => {
  it("streams a running run that has a log (any stage) → live feed", () => {
    // Every subprocess stage now tees its log; gating is purely has_log.
    const runs = [
      run("acceptance", { id: "r-acc", status: "running", has_log: true, started_at: "2026-06-07T13:00:00Z" }),
    ];
    const { live, active } = pickLiveRun(runs);
    expect(live?.id).toBe("r-acc");
    expect(active?.id).toBe("r-acc");
  });

  it("prefers the running run that has a log over a newer running run without one", () => {
    // _run_prepush_gates starts a running child before the parent's log is
    // written; the log-less child must not shadow the tailable run.
    const runs = [
      run("verify", { id: "r-verify", status: "running", has_log: false, started_at: "2026-06-07T13:00:00Z" }),
      run("local_review", { id: "r-lr", status: "running", has_log: true, started_at: "2026-06-07T12:00:00Z" }),
    ];
    const { live, active } = pickLiveRun(runs);
    expect(live?.id).toBe("r-lr");
    expect(active?.id).toBe("r-lr");
  });

  it("shows the placeholder for a running run with no log yet (review / pre-tee)", () => {
    // A running `review` (remote codex bot, no local subprocess) never gets a
    // log; the newest running row still labels the in-progress placeholder.
    const runs = [
      run("review", { id: "r-review", status: "running", has_log: false, started_at: "2026-06-07T13:00:00Z" }),
      run("acceptance", { id: "r-acc", status: "running", has_log: false, started_at: "2026-06-07T12:00:00Z" }),
    ];
    const { live, active } = pickLiveRun(runs);
    expect(live).toBeNull();
    expect(active?.id).toBe("r-review");
  });

  it("upgrades placeholder → feed once has_log flips true on a detail refresh", () => {
    // The gate re-checks each render as issue detail polls: a running run with
    // no log yet is placeholder-only, then streams once its log appears.
    const before = [run("local_review", { id: "r-lr", status: "running", has_log: false })];
    expect(pickLiveRun(before).live).toBeNull();
    expect(pickLiveRun(before).active?.id).toBe("r-lr");

    const after = [run("local_review", { id: "r-lr", status: "running", has_log: true })];
    expect(pickLiveRun(after).live?.id).toBe("r-lr");
  });

  it("returns both null when nothing is running", () => {
    const runs = [run("implement", { id: "r-done", status: "completed", has_log: true })];
    const { live, active } = pickLiveRun(runs);
    expect(live).toBeNull();
    expect(active).toBeNull();
  });
});

describe("FinalLogCard", () => {
  it("opens the failed run's final log by default, clearly labelled", () => {
    const runs = [
      run("merge", { id: "r-merge", status: "done", has_log: true, started_at: "2026-06-07T13:00:00Z", ended_at: "2026-06-07T13:01:00Z" }),
      run("implement", { id: "r-fail", status: "failed", has_log: true, started_at: "2026-06-07T12:00:00Z", ended_at: "2026-06-07T12:05:00Z" }),
    ];
    const markup = renderToStaticMarkup(<FinalLogCard runs={runs} />);
    expect(markup).toContain("final log — Implement, failed");
  });

  it("opens the newest run with a log by default, not an unlogged newer park row", () => {
    // A local-review-only park: review(needs_approval, no log, newest),
    // local_review(completed, now tees a log), implement(completed, has log).
    // The unlogged review row must not win — the newest run with a log does.
    const runs = [
      run("review", { id: "r-review", status: "needs_approval", has_log: false, started_at: "2026-06-07T14:00:00Z" }),
      run("local_review", { id: "r-lr", status: "completed", has_log: true, started_at: "2026-06-07T13:00:00Z", ended_at: "2026-06-07T13:01:00Z" }),
      run("implement", { id: "r-impl", status: "completed", has_log: true, started_at: "2026-06-07T12:00:00Z", ended_at: "2026-06-07T12:05:00Z" }),
    ];
    const markup = renderToStaticMarkup(<FinalLogCard runs={runs} />);
    expect(markup).toContain("final log — Local review, completed");
    expect(markup).not.toContain("no per-run log");
  });

  it("lists every run in the picker with stage, status and duration", () => {
    const runs = [
      run("merge", { id: "r-merge", status: "done", has_log: true, started_at: "2026-06-07T13:00:00Z", ended_at: "2026-06-07T13:00:30Z" }),
      run("implement", { id: "r-fail", status: "failed", has_log: true, started_at: "2026-06-07T12:00:00Z", ended_at: "2026-06-07T12:05:00Z" }),
    ];
    const markup = renderToStaticMarkup(<FinalLogCard runs={runs} />);
    expect(markup).toContain("Merge");
    expect(markup).toContain("Implement");
    expect(markup).toContain("done");
    expect(markup).toContain("failed");
    // Duration is rendered per run (merge ran 30s, implement 5m).
    expect(markup).toContain("30s");
    expect(markup).toContain("5m 0s");
  });

  it("explains the empty state for a run without a log instead of a spinner", () => {
    // A pre-tee run (or synthetic row): `has_log: false` → honest empty
    // state, no stuck spinner.
    const runs = [
      run("local_review", { id: "r-lr", status: "failed", has_log: false, started_at: "2026-06-07T12:00:00Z", ended_at: "2026-06-07T12:01:00Z" }),
    ];
    const markup = renderToStaticMarkup(<FinalLogCard runs={runs} />);
    expect(markup).toContain("no per-run log");
    expect(markup).not.toContain("Waiting for output…");
  });

  it("drains the log for any run that has one", () => {
    // A finished local_review created after the tee change has a real
    // `{run_id}.log`, so it drains rather than showing the empty state.
    const runs = [
      run("local_review", {
        id: "r-lr",
        status: "failed",
        has_log: true,
        started_at: "2026-06-07T12:00:00Z",
        ended_at: "2026-06-07T12:05:00Z",
      }),
    ];
    const markup = renderToStaticMarkup(<FinalLogCard runs={runs} />);
    expect(markup).toContain("final log — Local review, failed");
    expect(markup).not.toContain("no per-run log");
  });

  it("renders nothing when the issue has no runs", () => {
    const markup = renderToStaticMarkup(<FinalLogCard runs={[]} />);
    expect(markup).toBe("");
  });
});

function detailWith(
  state: IssueDetail["canonical_status"]["state"],
  runs: IssueDetail["runs"],
): IssueDetail {
  return {
    issue: { id: "i1", identifier: "HQ-96", title: "t", team_key: "HQ" },
    tokens_by_model: [],
    canonical_status: { state, since: null, subtitle: null, stuck_for: null },
    runs,
    issue_prs: [],
    operator_waits: [],
    review_state: null,
  } as unknown as IssueDetail;
}

describe("rawDetailJson", () => {
  const detail = detailWith("running", [
    run("implement", { output_tokens: 5 }),
  ] as IssueDetail["runs"]);

  it("does zero serialization while the section is collapsed", () => {
    // The page re-renders every 5s (detail poll) + 10s (nowMs tick); a
    // collapsed Raw JSON must not pay the stringify cost each time.
    const spy = vi.spyOn(JSON, "stringify");
    const out = rawDetailJson(detail, false);
    expect(out).toBe("");
    expect(spy).not.toHaveBeenCalled();
    spy.mockRestore();
  });

  it("serializes the detail once opened", () => {
    const out = rawDetailJson(detail, true);
    expect(out).toBe(JSON.stringify(detail, null, 2));
    expect(out).toContain("implement");
  });
});

describe("deriveCockpit reason", () => {
  const failedRun = run("implement", {
    status: "failed",
    termination_detail: "implement run exited 0 but did not satisfy the completion contract",
  }) as IssueDetail["runs"][number];
  const doneRun = run("merge", {
    status: "done",
    started_at: "2026-06-07T13:00:00Z",
  }) as IssueDetail["runs"][number];

  it("keeps the old run's error out of a completed issue", () => {
    const c = deriveCockpit(detailWith("done", [doneRun, failedRun]), undefined);
    expect(c.runState).toBe("completed");
    expect(c.reason).toBeNull();
  });

  it("still surfaces the error while the issue is failed", () => {
    const c = deriveCockpit(detailWith("failed", [failedRun]), undefined);
    expect(c.reason).toContain("completion contract");
  });
});

describe("NowCard", () => {
  const nowMs = Date.parse("2026-06-07T17:00:00Z");

  it("inlines the PR link, mergeable badge and check summary once a PR exists", () => {
    const markup = renderToStaticMarkup(<NowCard c={cockpit} nowMs={nowMs} />);
    expect(markup).toContain("#412");
    expect(markup).toContain("mergeable");
    expect(markup).toContain("11✓ 0✕ 1⋯");
    expect(markup).toContain("kulichevskiy/adjust_os");
  });

  it("renders no PR row before a PR exists", () => {
    const markup = renderToStaticMarkup(
      <NowCard c={{ ...cockpit, pr: null }} nowMs={nowMs} />,
    );
    expect(markup).not.toContain("#412");
    expect(markup).not.toContain("gitPr");
  });

  it("shows a failure reason only when one is set — a completed issue stays clean", () => {
    const failed = {
      ...cockpit,
      runState: "failed" as const,
      reason: "implement run exited 0 but did not satisfy the completion contract",
    };
    expect(renderToStaticMarkup(<NowCard c={failed} nowMs={nowMs} />)).toContain(
      "completion contract",
    );
    const done = { ...cockpit, runState: "completed" as const, reason: null };
    expect(renderToStaticMarkup(<NowCard c={done} nowMs={nowMs} />)).not.toContain(
      "completion contract",
    );
  });
});
