import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";

import type { IssueSummary, SpendSummary, TeamSpend } from "@/lib/api";

import { HeadlineTotals, IssueTable, PerTeam, SectionTotals } from "./HomePage";

const NOW_MS = Date.UTC(2026, 4, 17, 12, 0, 0);

function issue(overrides: Partial<IssueSummary> = {}): IssueSummary {
  return {
    id: "iss-1",
    identifier: "VIB-16",
    title: "Stale issue",
    team_key: "VIB",
    cost_usd: 0,
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
  it("renders a cost column alongside abbreviated tokens", () => {
    const markup = renderTable(
      [
        issue({
          cost_usd: 13.09,
          input_tokens: 1_234_000,
          output_tokens: 340_000,
          cache_write_tokens: 999,
          cache_read_tokens: 1_000,
        }),
      ],
      "active",
    );
    expect(markup).toContain(">$</th>");
    expect(markup).toContain("$13.09");
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
  it("sums cost and tokens for the visible rows", () => {
    const markup = renderToStaticMarkup(
      <SectionTotals
        issues={[
          issue({ cost_usd: 1.5, input_tokens: 1000 }),
          issue({ id: "iss-2", cost_usd: 2.5, input_tokens: 200 }),
        ]}
      />,
    );
    expect(markup).toContain("$4.00");
    expect(markup).toContain('title="1200">1.2k</span>');
  });
});

describe("PerTeam", () => {
  it("sorts teams by spend and shows issue counts", () => {
    const teams: TeamSpend[] = [
      { key: "VIB", cost_usd: 100, total_tokens: 5_000_000, input_tokens: 0, output_tokens: 0, cache_write_tokens: 0, cache_read_tokens: 0, issues: 4 },
      { key: "ADJ", cost_usd: 500, total_tokens: 9_000_000, input_tokens: 0, output_tokens: 0, cache_write_tokens: 0, cache_read_tokens: 0, issues: 9 },
    ];
    const markup = renderToStaticMarkup(<PerTeam teams={teams} />);
    expect(markup.indexOf("ADJ")).toBeLessThan(markup.indexOf("VIB"));
    expect(markup).toContain("$500");
    expect(markup).toContain("9 issues");
  });
});

describe("HeadlineTotals", () => {
  it("renders all-time spend and token total", () => {
    const totals: SpendSummary["totals"] = {
      cost_usd: 8425,
      total_tokens: 1_200_000_000,
      input_tokens: 0,
      output_tokens: 0,
      cache_write_tokens: 0,
      cache_read_tokens: 0,
      issues: 172,
    };
    const markup = renderToStaticMarkup(<HeadlineTotals totals={totals} />);
    expect(markup).toContain("Total spend");
    expect(markup).toContain("$8,425");
    expect(markup).toContain("1.2B");
  });
});
