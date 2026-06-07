import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";

import type {
  IssueSummary,
  ProviderSpend,
  SpendSummary,
  TeamSpend,
} from "@/lib/api";

import {
  HeadlineTotals,
  IssueTable,
  PerProvider,
  PerTeam,
  SectionTotals,
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
  it("sums tokens for the visible rows", () => {
    const markup = renderToStaticMarkup(
      <SectionTotals
        issues={[
          issue({ input_tokens: 1000 }),
          issue({ id: "iss-2", input_tokens: 200 }),
        ]}
      />,
    );
    expect(markup).not.toContain("$");
    expect(markup).toContain('title="1200">1.2k</span>');
  });
});

describe("PerTeam", () => {
  it("sorts teams by total tokens and shows issue counts", () => {
    const teams: TeamSpend[] = [
      { key: "VIB", total_tokens: 5_000_000, input_tokens: 0, output_tokens: 0, cache_write_tokens: 0, cache_read_tokens: 0, issues: 4 },
      { key: "ADJ", total_tokens: 9_000_000, input_tokens: 0, output_tokens: 0, cache_write_tokens: 0, cache_read_tokens: 0, issues: 9 },
    ];
    const markup = renderToStaticMarkup(<PerTeam teams={teams} />);
    expect(markup.indexOf("ADJ")).toBeLessThan(markup.indexOf("VIB"));
    expect(markup).not.toContain("$");
    expect(markup).toContain('title="9000000">9M</span>');
    expect(markup).toContain("9 issues");
  });
});

describe("PerProvider", () => {
  const providers: ProviderSpend[] = [
    {
      provider: "codex",
      total_tokens: 2_000_000,
      input_tokens: 0,
      output_tokens: 0,
      cache_write_tokens: 0,
      cache_read_tokens: 0,
      issues: 2,
      per_model: [
        { model: "gpt-5.5", total_tokens: 2_000_000, input_tokens: 0, output_tokens: 0, cache_write_tokens: 0, cache_read_tokens: 0, issues: 2 },
      ],
    },
    {
      provider: "claude",
      total_tokens: 9_000_000,
      input_tokens: 0,
      output_tokens: 0,
      cache_write_tokens: 0,
      cache_read_tokens: 0,
      issues: 7,
      per_model: [
        { model: "claude-opus-4-8", total_tokens: 9_000_000, input_tokens: 0, output_tokens: 0, cache_write_tokens: 0, cache_read_tokens: 0, issues: 7 },
      ],
    },
  ];

  it("sorts providers by total tokens and shows issue counts", () => {
    const markup = renderToStaticMarkup(<PerProvider providers={providers} />);
    expect(markup.indexOf("claude")).toBeLessThan(markup.indexOf("codex"));
    expect(markup).not.toContain("$");
    expect(markup).toContain('title="9000000">9M</span>');
    expect(markup).toContain("7 issues");
  });

  it("keeps model rows collapsed until the provider is expanded", () => {
    const markup = renderToStaticMarkup(<PerProvider providers={providers} />);
    expect(markup).not.toContain("claude-opus-4-8");
    expect(markup).not.toContain("gpt-5.5");
  });
});

describe("HeadlineTotals", () => {
  it("renders the all-time token total and no dollars", () => {
    const totals: SpendSummary["totals"] = {
      total_tokens: 1_200_000_000,
      input_tokens: 0,
      output_tokens: 0,
      cache_write_tokens: 0,
      cache_read_tokens: 0,
      issues: 172,
    };
    const markup = renderToStaticMarkup(<HeadlineTotals totals={totals} />);
    expect(markup).toContain("Total tokens");
    expect(markup).not.toContain("$");
    expect(markup).toContain("1.2B");
  });
});
