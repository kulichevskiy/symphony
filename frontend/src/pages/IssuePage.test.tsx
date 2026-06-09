import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import {
  aggregateRunsByStage,
  CmdButton,
  PrCard,
  StageSpendCard,
  TokensCard,
} from "./IssuePage";
import { applicability } from "./issueControls";

function run(stage: string, tok: Partial<Record<string, number>>) {
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

describe("PrCard", () => {
  it("renders the PR link, mergeable badge and check summary", () => {
    const markup = renderToStaticMarkup(<PrCard pr={cockpit.pr} />);
    expect(markup).toContain("#412");
    expect(markup).toContain("mergeable");
    expect(markup).toContain("11✓ 0✕ 1⋯");
  });

  it("handles the no-PR state", () => {
    const markup = renderToStaticMarkup(<PrCard pr={null} />);
    expect(markup).toContain("No PR opened yet");
  });
});
