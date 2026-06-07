import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";

import type {
  IssueSummary,
  ProviderSpend,
  SpendHeatmap,
  SpendSummary,
  TeamSpend,
} from "@/lib/api";

import {
  HeadlineTotals,
  IssueTable,
  PerProvider,
  PerTeam,
  SectionTotals,
  SpendOverview,
} from "./HomePage";

const NOW_MS = Date.UTC(2026, 4, 17, 12, 0, 0);

function issue(overrides: Partial<IssueSummary> = {}): IssueSummary {
  return {
    id: "iss-1",
    identifier: "VIB-16",
    title: "Stale issue",
    team_key: "VIB",
    input_tokens: 0,
    output_tokens: 0,
    cache_write_tokens: 0,
    cache_read_tokens: 0,
    latest_activity_ts: null,
    latest_activity_age_secs: null,
    canonical_status: { state: "running", since: null, subtitle: null, stuck_for: null },
    ...overrides,
  };
}

function renderTable(issues: IssueSummary[], mode: "active" | "done"): string {
  return renderToStaticMarkup(
    <MemoryRouter>
      <IssueTable issues={issues} mode={mode} nowMs={NOW_MS} onOpen={() => {}} />
    </MemoryRouter>,
  );
}

describe("IssueTable", () => {
  it("renders abbreviated token columns and no dollar column", () => {
    const markup = renderTable(
      [
        issue({
          input_tokens: 1_234_000,
          output_tokens: 340_000,
          cache_write_tokens: 999,
          cache_read_tokens: 1_000,
        }),
      ],
      "active",
    );
    expect(markup).not.toContain(">$</th>");
    expect(markup).not.toContain("$");
    expect(markup).toContain('title="1234000">1.2M</span>');
    expect(markup).toContain(">Last activity</th>");
  });

  it("uses a Completed column and completed_at for the done mode", () => {
    const markup = renderTable(
      [issue({ completed_at: "2026-05-16T10:00:00Z", canonical_status: { state: "done", since: null, subtitle: null, stuck_for: null } })],
      "done",
    );
    expect(markup).toContain(">Completed</th>");
    expect(markup).toContain("done");
  });
});

describe("SectionTotals", () => {
  it("sums the four categories for the visible rows without a total", () => {
    const markup = renderToStaticMarkup(
      <SectionTotals
        issues={[
          issue({
            input_tokens: 1000,
            output_tokens: 2000,
            cache_write_tokens: 3000,
            cache_read_tokens: 4000,
          }),
          issue({ id: "iss-2", input_tokens: 200 }),
        ]}
      />,
    );
    expect(markup).not.toContain("$");
    expect(markup).not.toContain("total");
    expect(markup).toContain('title="1200">1.2k</span>');
    expect(markup).toContain('title="2000">2k</span>');
    expect(markup).toContain('title="3000">3k</span>');
    expect(markup).toContain('title="4000">4k</span>');
    // No summed total (1000+2000+3000+4000+200 = 10200).
    expect(markup).not.toContain("10.2k");
  });
});

describe("PerTeam", () => {
  it("sorts teams by output and shows a mix-bar plus the four figures", () => {
    const teams: TeamSpend[] = [
      { key: "VIB", input_tokens: 4_000_000, output_tokens: 1_000_000, cache_write_tokens: 0, cache_read_tokens: 0, issues: 4 },
      { key: "ADJ", input_tokens: 1_000_000, output_tokens: 8_000_000, cache_write_tokens: 0, cache_read_tokens: 0, issues: 9 },
    ];
    const markup = renderToStaticMarkup(<PerTeam teams={teams} />);
    // ADJ wins on output even though VIB has more input → no summed total decides order.
    expect(markup.indexOf("ADJ")).toBeLessThan(markup.indexOf("VIB"));
    expect(markup).not.toContain("$");
    expect(markup).toContain("9 issues");
    // The four explicit figures (output shown), and a proportional mix-bar.
    expect(markup).toContain('title="8000000">8M</span>');
    expect(markup).toContain("cache-w");
    expect(markup).toContain("cache-r");
    expect(markup).toContain("width:");
  });
});

describe("PerProvider", () => {
  const providers: ProviderSpend[] = [
    {
      provider: "codex",
      input_tokens: 1_000_000,
      output_tokens: 500_000,
      cache_write_tokens: 0,
      cache_read_tokens: 0,
      issues: 2,
      per_model: [
        { model: "gpt-5.5", input_tokens: 1_000_000, output_tokens: 500_000, cache_write_tokens: 0, cache_read_tokens: 0, issues: 2 },
      ],
    },
    {
      provider: "claude",
      input_tokens: 2_000_000,
      output_tokens: 7_000_000,
      cache_write_tokens: 0,
      cache_read_tokens: 0,
      issues: 7,
      per_model: [
        { model: "claude-opus-4-8", input_tokens: 2_000_000, output_tokens: 7_000_000, cache_write_tokens: 0, cache_read_tokens: 0, issues: 7 },
      ],
    },
  ];

  it("sorts providers by output and shows the four figures", () => {
    const markup = renderToStaticMarkup(<PerProvider providers={providers} />);
    expect(markup.indexOf("claude")).toBeLessThan(markup.indexOf("codex"));
    expect(markup).not.toContain("$");
    expect(markup).toContain('title="7000000">7M</span>');
    expect(markup).toContain("7 issues");
    expect(markup).toContain("width:");
  });

  it("keeps model rows collapsed until the provider is expanded", () => {
    const markup = renderToStaticMarkup(<PerProvider providers={providers} />);
    expect(markup).not.toContain("claude-opus-4-8");
    expect(markup).not.toContain("gpt-5.5");
  });
});

describe("SpendOverview", () => {
  const summary: SpendSummary = {
    totals: {
      input_tokens: 1_000,
      output_tokens: 2_000,
      cache_write_tokens: 3_000,
      cache_read_tokens: 4_000,
      issues: 172,
    },
    per_team: [
      { key: "VIB", input_tokens: 4_000_000, output_tokens: 1_000_000, cache_write_tokens: 0, cache_read_tokens: 0, issues: 4 },
    ],
    per_provider: [
      {
        provider: "claude",
        input_tokens: 2_000_000,
        output_tokens: 7_000_000,
        cache_write_tokens: 0,
        cache_read_tokens: 0,
        issues: 7,
        per_model: [
          { model: "claude-opus-4-8", input_tokens: 2_000_000, output_tokens: 7_000_000, cache_write_tokens: 0, cache_read_tokens: 0, issues: 7 },
        ],
      },
    ],
  };
  const heatmap: SpendHeatmap = {
    days: [
      { date: "2026-06-01", input_tokens: 1, output_tokens: 1, cache_write_tokens: 0, cache_read_tokens: 0, issues: 1 },
    ],
    start: "2026-06-01",
    end: "2026-06-01",
  };

  function render(): string {
    return renderToStaticMarkup(
      <SpendOverview
        summary={summary}
        heatmap={heatmap}
        heatProvider="all"
        onChangeHeatProvider={() => {}}
        onPickTeam={() => {}}
      />,
    );
  }

  it("stacks heatmap + totals (row 1), team full-width (row 2), provider (row 3)", () => {
    const markup = render();
    // Row 1: the heatmap and the all-time totals both render.
    expect(markup).toContain("Daily token burn");
    expect(markup).toContain("Tokens · all-time");
    // Top-down order: row-1 totals → row-2 team → row-3 provider.
    expect(markup.indexOf("Tokens · all-time")).toBeLessThan(
      markup.indexOf("Tokens by team"),
    );
    expect(markup.indexOf("Tokens by team")).toBeLessThan(
      markup.indexOf("Tokens by provider / model"),
    );
  });

  it("constrains the provider list width so it does not stretch across the card", () => {
    const markup = render();
    const providerIdx = markup.indexOf("Tokens by provider / model");
    // The row-3 wrapper before the heading carries a max-width constraint.
    expect(markup.slice(0, providerIdx)).toMatch(/max-w-/);
  });
});

describe("HeadlineTotals", () => {
  it("renders four token stat blocks under a context label and no summed total", () => {
    const totals: SpendSummary["totals"] = {
      input_tokens: 1_000,
      output_tokens: 2_000,
      cache_write_tokens: 3_000,
      cache_read_tokens: 4_000,
      issues: 172,
    };
    const markup = renderToStaticMarkup(<HeadlineTotals totals={totals} />);
    expect(markup).toContain("Tokens");
    expect(markup).not.toContain("Total tokens");
    expect(markup).not.toContain("$");
    expect(markup).toContain('title="1000">1k</span>');
    expect(markup).toContain('title="2000">2k</span>');
    expect(markup).toContain('title="3000">3k</span>');
    expect(markup).toContain('title="4000">4k</span>');
    // No summed hero number (1000+2000+3000+4000 = 10000).
    expect(markup).not.toContain("10k");
  });
});
