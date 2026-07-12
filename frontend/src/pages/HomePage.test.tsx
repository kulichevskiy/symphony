import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";

import type { IssueSummary, SpendHeatmap, SpendSummary } from "@/lib/api";
import { DEFAULT_DATE, FiltersProvider } from "@/lib/filters";

import {
  BOARD_COLUMNS,
  BreakdownTable,
  groupForBoard,
  HomePage,
  IssueTable,
  KanbanBoard,
  MixLegend,
  PauseToggle,
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

describe("IssueTable affordances", () => {
  it("identifier links to the issue page, Linear opens via a labelled external icon", () => {
    const markup = renderTable([issue()], "active");
    // Internal navigation: identifier is a router link to /issue/:id.
    expect(markup).toContain('href="/issue/iss-1"');
    // External: separate anchor to Linear with an explanatory tooltip.
    expect(markup).toContain('href="https://linear.app/issue/VIB-16"');
    expect(markup).toContain("Open VIB-16 in Linear");
    // Trailing chevron signals that the whole row navigates.
    expect(markup).toContain('"m9 18 6-6-6-6"');
  });

  it("renders the title as plain text — the row itself opens the issue", () => {
    const markup = renderTable([issue({ title: "Some issue title" })], "active");
    expect(markup).toContain("Some issue title");
    expect(markup).not.toContain('>Some issue title</a>');
  });
});

describe("groupForBoard", () => {
  it("buckets active issues into lanes by canonical status and done into Done", () => {
    const lanes = groupForBoard(
      [
        issue({ id: "a", canonical_status: { state: "running", since: null, subtitle: "implement", stuck_for: null } }),
        issue({ id: "b", canonical_status: { state: "pr_open", since: null, subtitle: null, stuck_for: null } }),
        issue({ id: "c", canonical_status: { state: "failed", since: null, subtitle: null, stuck_for: null } }),
        issue({ id: "d", canonical_status: { state: "idle", since: null, subtitle: null, stuck_for: null } }),
        issue({ id: "t", canonical_status: { state: "todo", since: null, subtitle: "Todo", stuck_for: null } }),
        issue({ id: "w", canonical_status: { state: "waiting", since: null, subtitle: "blocked by VIB-1", stuck_for: null } }),
        issue({ id: "m", canonical_status: { state: "awaiting_merge", since: null, subtitle: null, stuck_for: null } }),
      ],
      [issue({ id: "e", canonical_status: { state: "done", since: null, subtitle: null, stuck_for: null } })],
    );
    expect(lanes.get("implement")!.map((i) => i.id)).toEqual(["a"]);
    expect(lanes.get("review")!.map((i) => i.id)).toEqual(["b"]);
    expect(lanes.get("attention")!.map((i) => i.id)).toEqual(["c"]);
    // Tracked-but-stateless (idle) issues ride in the Todo lane.
    expect(lanes.get("todo")!.map((i) => i.id)).toEqual(["d", "t"]);
    expect(lanes.get("waiting")!.map((i) => i.id)).toEqual(["w"]);
    expect(lanes.get("merge")!.map((i) => i.id)).toEqual(["m"]);
    expect(lanes.get("done")!.map((i) => i.id)).toEqual(["e"]);
  });

  it("places running issues by their stage subtitle", () => {
    const running = (id: string, stage: string | null) =>
      issue({ id, canonical_status: { state: "running", since: null, subtitle: stage, stuck_for: null } });
    const lanes = groupForBoard(
      [
        running("imp", "implement"),
        running("lr", "local_review"),
        running("rev", "review"),
        running("rf", "review_fix"),
        running("acc", "acceptance"),
        running("unk", "mystery_stage"),
        running("none", null),
      ],
      [],
    );
    expect(lanes.get("implement")!.map((i) => i.id)).toEqual(
      expect.arrayContaining(["imp", "unk", "none"]),
    );
    expect(lanes.get("local_review")!.map((i) => i.id)).toEqual(["lr"]);
    expect(lanes.get("review")!.map((i) => i.id)).toEqual(
      expect.arrayContaining(["rev", "rf"]),
    );
    expect(lanes.get("merge")!.map((i) => i.id)).toEqual(["acc"]);
  });

  it("never drops an issue: unknown statuses land in Needs attention", () => {
    const lanes = groupForBoard(
      [issue({ id: "x", canonical_status: { state: "mystery" as never, since: null, subtitle: null, stuck_for: null } })],
      [],
    );
    expect(lanes.get("attention")!.map((i) => i.id)).toEqual(["x"]);
  });

  it("orders lanes newest-activity-first (Done by completed_at)", () => {
    const lanes = groupForBoard(
      [
        issue({ id: "old", latest_activity_ts: "2026-05-16T10:00:00Z" }),
        issue({ id: "new", latest_activity_ts: "2026-05-17T10:00:00Z" }),
      ],
      [
        issue({ id: "d-old", completed_at: "2026-05-10T10:00:00Z" }),
        issue({ id: "d-new", completed_at: "2026-05-15T10:00:00Z" }),
      ],
    );
    expect(lanes.get("implement")!.map((i) => i.id)).toEqual(["new", "old"]);
    expect(lanes.get("done")!.map((i) => i.id)).toEqual(["d-new", "d-old"]);
  });

  it("orders the Todo/Waiting queue lanes by identifier (dispatch order)", () => {
    const lanes = groupForBoard(
      [
        issue({ id: "b", identifier: "SYM-180", canonical_status: { state: "todo", since: null, subtitle: null, stuck_for: null } }),
        issue({ id: "a", identifier: "SYM-9", canonical_status: { state: "todo", since: null, subtitle: null, stuck_for: null } }),
      ],
      [],
    );
    expect(lanes.get("todo")!.map((i) => i.identifier)).toEqual(["SYM-9", "SYM-180"]);
  });
});

describe("KanbanBoard", () => {
  function renderBoard(active: IssueSummary[], done: IssueSummary[]): string {
    return renderToStaticMarkup(
      <MemoryRouter>
        <KanbanBoard active={active} done={done} nowMs={NOW_MS} />
      </MemoryRouter>,
    );
  }

  it("renders every lane with its count, cards linking to the issue page", () => {
    const markup = renderBoard(
      [issue({ id: "a", identifier: "VIB-1" })],
      [issue({ id: "b", identifier: "VIB-2", canonical_status: { state: "done", since: null, subtitle: null, stuck_for: null } })],
    );
    for (const col of BOARD_COLUMNS) {
      expect(markup).toContain(col.label);
    }
    expect(markup).toContain('href="/issue/a"');
    expect(markup).toContain('href="/issue/b"');
    expect(markup).toContain("VIB-1");
    expect(markup).toContain("VIB-2");
  });

  it("shows a status badge only in mixed lanes (review / needs attention)", () => {
    const single = renderBoard(
      [issue({ id: "a", canonical_status: { state: "running", since: null, subtitle: null, stuck_for: null } })],
      [],
    );
    // Working lane is single-status: no badge inside the card.
    expect(single).not.toContain(">running<");

    const mixed = renderBoard(
      [issue({ id: "b", canonical_status: { state: "pr_open", since: null, subtitle: null, stuck_for: null } })],
      [],
    );
    expect(mixed).toContain("PR open");
  });

  it("marks empty lanes instead of hiding them", () => {
    const markup = renderBoard([], []);
    expect(markup.match(/empty/g)?.length).toBe(BOARD_COLUMNS.length);
  });

  it("links queue-only (untracked) cards to Linear, not the issue page", () => {
    const markup = renderBoard(
      [
        issue({
          id: "lin-uuid",
          identifier: "VIB-9",
          tracked: false,
          canonical_status: { state: "todo", since: "2026-07-12T08:00:00Z", subtitle: "Todo", stuck_for: null },
        }),
      ],
      [],
    );
    expect(markup).toContain('href="https://linear.app/issue/VIB-9"');
    expect(markup).not.toContain('href="/issue/lin-uuid"');
    expect(markup).toContain("Open VIB-9 in Linear");
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

describe("PauseToggle", () => {
  it("offers a Pause action when dispatch is running", () => {
    const markup = renderToStaticMarkup(
      <PauseToggle paused={false} pending={false} onToggle={() => {}} />,
    );
    expect(markup).toContain("Pause");
    expect(markup).not.toContain("Paused");
    // Not disabled while idle.
    expect(markup).not.toContain('disabled=""');
  });

  it("shows a paused indicator and a Resume action when paused", () => {
    const markup = renderToStaticMarkup(
      <PauseToggle paused={true} pending={false} onToggle={() => {}} />,
    );
    expect(markup).toContain("Paused");
    expect(markup).toContain("Resume");
  });

  it("disables the control while a toggle is in flight", () => {
    const markup = renderToStaticMarkup(
      <PauseToggle paused={false} pending={true} onToggle={() => {}} />,
    );
    expect(markup).toContain('disabled=""');
  });

  it("gives the button a phone-sized tap target that shrinks on desktop", () => {
    // One-handed mobile reach: 44px touch target on phones, compact on ≥sm.
    const markup = renderToStaticMarkup(
      <PauseToggle paused={false} pending={false} onToggle={() => {}} />,
    );
    expect(markup).toContain("h-11");
    expect(markup).toContain("sm:h-9");
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

  it("renders the filtered empty-state copy when there are no rows", () => {
    const markup = renderToStaticMarkup(<BreakdownTable rows={[]} kind="team" />);
    expect(markup).toContain("No teams/models match the current filters");
  });

  it("marks selected rows when selection is enabled", () => {
    const markup = renderToStaticMarkup(
      <BreakdownTable
        rows={teamRows}
        kind="team"
        selectedKeys={new Set(["VIB"])}
        onToggleRow={() => {}}
      />,
    );
    // Selected row carries aria-selected=true and a highlight; the unselected
    // one is aria-selected=false. Rows become click-to-select (cursor-pointer).
    expect(markup).toContain('aria-selected="true"');
    expect(markup).toContain('aria-selected="false"');
    expect(markup).toContain("bg-secondary");
    expect(markup).toContain("cursor-pointer");
  });

  it("renders stage rows in the given pipeline order without re-sorting", () => {
    // Outputs are NOT descending: an output-sort would put merge first.
    const stageRows = [
      { rowKey: "implement", stageKey: "implement", issues: 2, input_tokens: 150, output_tokens: 10, cache_write_tokens: 0, cache_read_tokens: 0 },
      { rowKey: "review", stageKey: "review", issues: 1, input_tokens: 10, output_tokens: 0, cache_write_tokens: 0, cache_read_tokens: 0 },
      { rowKey: "merge", stageKey: "merge", issues: 1, input_tokens: 0, output_tokens: 100, cache_write_tokens: 0, cache_read_tokens: 0 },
    ];
    const markup = renderToStaticMarkup(
      <BreakdownTable rows={stageRows} kind="stage" barMode="magnitude" />,
    );
    expect(markup).toContain(">Stage</th>");
    // Pipeline order preserved (implement → review → merge), not output-sorted.
    expect(markup.indexOf("Implement")).toBeLessThan(markup.indexOf("Review"));
    expect(markup.indexOf("Review")).toBeLessThan(markup.indexOf("Merge"));
    // Non-sortable: numeric headers are plain text, no sort affordance.
    expect(markup).not.toContain('aria-sort');
    // Share column on output: merge 100/110 ≈ 91%, review at 0%.
    expect(markup).toContain(">Share</th>");
    expect(markup).toContain("0%");
    // Stage palette dots + raw OUT values.
    expect(markup).toContain("bg-violet-500"); // review tint
    expect(markup).toContain("bg-emerald-500"); // merge tint
    expect(markup).toContain('title="100">100</span>');
  });
});

// TokenOverview now fetches its trend series with useQuery and reads
// teams/models from useFilters, so it must render under both providers.
function renderOverview(props: Parameters<typeof TokenOverview>[0]): string {
  return renderToStaticMarkup(
    <QueryClientProvider client={new QueryClient()}>
      <MemoryRouter>
        <FiltersProvider>
          <TokenOverview {...props} />
        </FiltersProvider>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

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
    per_stage: [
      { key: "implement", input_tokens: 5_000_000, output_tokens: 6_000_000, cache_write_tokens: 0, cache_read_tokens: 0, issues: 5 },
      { key: "review", input_tokens: 100, output_tokens: 0, cache_write_tokens: 0, cache_read_tokens: 0, issues: 1 },
      { key: "merge", input_tokens: 1_000, output_tokens: 1_500_000, cache_write_tokens: 0, cache_read_tokens: 0, issues: 3 },
    ],
    teams: ["VIB", "ADJ"],
    models: [
      { provider: "claude", model: "claude-opus-4-8" },
      { provider: "codex", model: "gpt-5-codex" },
    ],
  };
  const heatmap: SpendHeatmap = {
    days: [
      { date: "2026-06-01", input_tokens: 1, output_tokens: 1, cache_write_tokens: 0, cache_read_tokens: 0, issues: 1 },
    ],
    start: "2026-06-01",
    end: "2026-06-01",
  };

  it("renders heatmap + all-time rail + a single Breakdown table with a By team/By model toggle", () => {
    const markup = renderOverview({
      summary,
      heatmap,
      provider: "all",
      date: DEFAULT_DATE,
      window: { from: null, to: null },
    });
    expect(markup).toContain("Daily token burn");
    expect(markup).toContain("Tokens · all-time");
    expect(markup).toContain("Breakdown");
    expect(markup).toContain("By team");
    expect(markup).toContain("By model");
    expect(markup).toContain("By stage");
    // Defaults to the team view (VIB row present, no model names yet).
    expect(markup).toContain(">VIB</span>");
    expect(markup).not.toContain("gpt-5-codex");
    // Team Totals shows the stacked totals bar (its segment title is the team
    // label + tokens — unique to LifecycleBar, distinct from the table rows).
    expect(markup).toContain('title="VIB ');
  });

  it("offers the Totals/Trend sub-toggle in every breakdown view, Totals by default", () => {
    const markup = renderOverview({
      summary,
      heatmap,
      provider: "all",
      date: DEFAULT_DATE,
      window: { from: null, to: null },
    });
    // The sub-toggle is present even in the default (team) view now.
    expect(markup).toContain("Totals");
    expect(markup).toContain("Trend");
    // Totals is the default, so the totals table (team row) shows, not a chart.
    expect(markup).toContain(">VIB</span>");
  });

  it("suffixes the rail eyebrow with the active provider", () => {
    const markup = renderOverview({
      summary,
      heatmap,
      provider: "codex",
      date: DEFAULT_DATE,
      window: { from: null, to: null },
    });
    expect(markup).toContain("· codex");
  });

  it("reflects the active window in the rail header and dims out-of-window cells", () => {
    const markup = renderOverview({
      summary,
      heatmap,
      provider: "all",
      date: { kind: "preset", preset: "7d" },
      window: { from: "2026-06-10", to: "2026-06-17" },
    });
    // Header tracks the window, not "all-time".
    expect(markup).toContain("Tokens · last 7 days");
    expect(markup).not.toContain("Tokens · all-time");
    // The single 2026-06-01 cell is outside [06-10, 06-17] → dimmed.
    expect(markup).toContain("opacity-25");
  });
});

describe("HomePage issues section", () => {
  it("defaults to the kanban board with a Board/Table toggle", () => {
    const markup = renderToStaticMarkup(
      <QueryClientProvider client={new QueryClient()}>
        <MemoryRouter>
          <FiltersProvider>
            <HomePage />
          </FiltersProvider>
        </MemoryRouter>
      </QueryClientProvider>,
    );
    expect(markup).toContain("Board");
    expect(markup).toContain("Table");
    for (const col of BOARD_COLUMNS) {
      expect(markup).toContain(col.label);
    }
  });
});
