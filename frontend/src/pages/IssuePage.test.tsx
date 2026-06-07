import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { CmdButton, PrCard, TokensCard } from "./IssuePage";
import { applicability } from "./issueControls";

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
