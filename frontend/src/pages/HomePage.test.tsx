import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";

import type { IssueSummary, SpendHeatmap, SpendSummary } from "@/lib/api";
import { DEFAULT_DATE } from "@/lib/filters";

import {
  BreakdownTable,
  IssueTable,
  MixLegend,
  SectionTotals,
  StatRail,
  TokenOverview,
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

describe("StatRail", () => {
  it("renders four token stat blocks with palette swatches and no summed total", () => {
    const totals: SpendSummary["totals"] = {
      input_tokens: 1_000,
      output_tokens: 2_000,
      cache_write_tokens: 3_000,
      cache_read_tokens: 4_000,
      issues: 172,
    };
    const markup = renderToStaticMarkup(<StatRail totals={totals} />);
    expect(markup).not.toContain("$");
    // Each category carries its shared-palette swatch.
    expect(markup).toContain("bg-blue-500");
    expect(markup).toContain("bg-violet-500");
    expect(markup).toContain("bg-cyan-500");
    expect(markup).toContain("bg-slate-300");
    expect(markup).toContain('title="1000">1k</span>');
    expect(markup).toContain('title="2000">2k</span>');
    expect(markup).toContain('title="3000">3k</span>');
    expect(markup).toContain('title="4000">4k</span>');
    // No summed hero number (1000+2000+3000+4000 = 10000).
    expect(markup).not.toContain("10k");
  });
});

describe("MixLegend", () => {
  it("lists the four token categories", () => {
    const markup = renderToStaticMarkup(<MixLegend />);
    for (const label of ["in", "out", "cache-write", "cache-read"]) {
      expect(markup).toContain(label);
    }
  });
});

describe("BreakdownTable", () => {
  const teamRows = [
    { rowKey: "VIB", teamKey: "VIB", issues: 4, input_tokens: 4_000_000, output_tokens: 1_000_000, cache_write_tokens: 0, cache_read_tokens: 0 },
    { rowKey: "ADJ", teamKey: "ADJ", issues: 9, input_tokens: 1_000_000, output_tokens: 8_000_000, cache_write_tokens: 0, cache_read_tokens: 0 },
  ];

  it("renders team columns and defaults to output descending", () => {
    const markup = renderToStaticMarkup(
      <BreakdownTable rows={teamRows} kind="team" />,
    );
    expect(markup).toContain("<table");
    for (const header of [">Team", ">Issues", ">Mix", ">IN", ">OUT", ">CACHE-WRITE", ">CACHE-READ"]) {
      expect(markup).toContain(header);
    }
    expect(markup).not.toContain("$");
    // ADJ wins on output even though VIB has more input → output decides order.
    expect(markup.indexOf("ADJ")).toBeLessThan(markup.indexOf("VIB"));
    expect(markup).toContain('title="8000000">8M</span>');
  });

  it("magnitude bar: the smaller-total row gets a scaled-down length", () => {
    const rows = [
      { rowKey: "BIG", teamKey: "BIG", issues: 1, input_tokens: 100, output_tokens: 0, cache_write_tokens: 0, cache_read_tokens: 0 },
      { rowKey: "SML", teamKey: "SML", issues: 1, input_tokens: 50, output_tokens: 0, cache_write_tokens: 0, cache_read_tokens: 0 },
    ];
    const markup = renderToStaticMarkup(
      <BreakdownTable rows={rows} kind="team" barMode="magnitude" />,
    );
    // Largest total fills the track; the 50-token row is half as long.
    expect(markup).toContain("width:100%");
    expect(markup).toContain("width:50%");
  });

  it("renders provider/model rows with a provider tag", () => {
    const modelRows = [
      { rowKey: "claude/opus-4.1", provider: "claude", model: "opus-4.1", issues: 7, input_tokens: 2_000_000, output_tokens: 7_000_000, cache_write_tokens: 0, cache_read_tokens: 0 },
    ];
    const markup = renderToStaticMarkup(
      <BreakdownTable rows={modelRows} kind="model" />,
    );
    expect(markup).toContain("Provider / model");
    expect(markup).toContain("opus-4.1");
    expect(markup).toContain("claude");
  });
});

describe("TokenOverview", () => {
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
      {
        provider: "codex",
        input_tokens: 1_000_000,
        output_tokens: 500_000,
        cache_write_tokens: 0,
        cache_read_tokens: 0,
        issues: 2,
        per_model: [
          { model: "gpt-5-codex", input_tokens: 1_000_000, output_tokens: 500_000, cache_write_tokens: 0, cache_read_tokens: 0, issues: 2 },
        ],
      },
    ],
    teams: ["VIB", "ADJ"],
  };
  const heatmap: SpendHeatmap = {
    days: [
      { date: "2026-06-01", input_tokens: 1, output_tokens: 1, cache_write_tokens: 0, cache_read_tokens: 0, issues: 1 },
    ],
    start: "2026-06-01",
    end: "2026-06-01",
  };

  it("renders heatmap + all-time rail + a single Breakdown table with a By team/By model toggle", () => {
    const markup = renderToStaticMarkup(
      <TokenOverview
        summary={summary}
        heatmap={heatmap}
        provider="all"
        date={DEFAULT_DATE}
        window={{ from: null, to: null }}
      />,
    );
    expect(markup).toContain("Daily token burn");
    expect(markup).toContain("Tokens · all-time");
    expect(markup).toContain("Breakdown");
    expect(markup).toContain("By team");
    expect(markup).toContain("By model");
    // Defaults to the team view (VIB row present, no model names yet).
    expect(markup).toContain(">VIB</span>");
    expect(markup).not.toContain("gpt-5-codex");
  });

  it("suffixes the rail eyebrow with the active provider", () => {
    const markup = renderToStaticMarkup(
      <TokenOverview
        summary={summary}
        heatmap={heatmap}
        provider="codex"
        date={DEFAULT_DATE}
        window={{ from: null, to: null }}
      />,
    );
    expect(markup).toContain("· codex");
  });

  it("reflects the active window in the rail header and dims out-of-window cells", () => {
    const markup = renderToStaticMarkup(
      <TokenOverview
        summary={summary}
        heatmap={heatmap}
        provider="all"
        date={{ kind: "preset", preset: "7d" }}
        window={{ from: "2026-06-10", to: "2026-06-17" }}
      />,
    );
    // Header tracks the window, not "all-time".
    expect(markup).toContain("Tokens · last 7 days");
    expect(markup).not.toContain("Tokens · all-time");
    // The single 2026-06-01 cell is outside [06-10, 06-17] → dimmed.
    expect(markup).toContain("opacity-25");
  });
});
