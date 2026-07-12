import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { Link, useNavigate } from "react-router";

import {
  LifecycleBar,
  MixBar,
  PROVIDER_TINT,
  STAGE_LABEL,
  STAGE_TINT,
  stageRank,
  Tk,
  TOKEN_CATS,
} from "@/components/dashboard/atoms";
import { Heatmap } from "@/components/dashboard/Heatmap";
import {
  StageTrend,
  type SeriesAdapter,
} from "@/components/dashboard/StageTrend";
import { StatusBadge } from "@/components/dashboard/StatusBadge";
import { Card } from "@/components/ui/card";
import { Icon } from "@/components/ui/icon";
import { Segmented } from "@/components/ui/segmented";
import {
  fetchIssues,
  fetchPauseState,
  fetchSpendHeatmap,
  fetchSpendStageSeries,
  fetchSpendSummary,
  setPauseState,
  type IssueSummary,
  type SpendHeatmap,
  type SpendSummary,
  type SpendTotals,
  type TokenSplit,
} from "@/lib/api";
import {
  dateWindowLabel,
  resolveDateWindow,
  type DateFilter,
  type Provider,
  useFilters,
} from "@/lib/filters";
import { cn } from "@/lib/utils";

import { formatRelativeTimestamp, formatUtcTimestamp } from "./activityFreshness";

// Team dot palette (Dashboard v2). Matches the design's team hues.
const TEAM_TINT: Record<string, string> = {
  VIB: "bg-blue-500",
  ADJ: "bg-violet-500",
  LP: "bg-cyan-500",
  SYM: "bg-emerald-500",
  HQ: "bg-amber-500",
};

// How each Breakdown view stacks/labels/colors its trend keys. Stage uses the
// component's own pipeline-ordered default; team/model keep the server's order
// and color by the same team-dot / provider-dot palette as the totals table.
// (model series keys are "provider/model"; tint by the provider prefix.)
const TREND_ADAPTERS: Record<"team" | "model", SeriesAdapter> = {
  team: {
    order: (keys) => keys,
    label: (key) => key,
    tint: (key) => TEAM_TINT[key] ?? "bg-slate-400",
  },
  model: {
    order: (keys) => keys,
    label: (key) => key,
    tint: (key) => PROVIDER_TINT[key.split("/")[0]] ?? "bg-slate-400",
  },
};

function useNowMs(): number {
  const [nowMs, setNowMs] = useState(() => Date.now());
  useEffect(() => {
    const id = window.setInterval(() => setNowMs(Date.now()), 10_000);
    return () => window.clearInterval(id);
  }, []);
  return nowMs;
}

function linearIssueUrl(identifier: string): string {
  return `https://linear.app/issue/${encodeURIComponent(identifier)}`;
}

/** The global dispatch kill-switch control (pure presentation). When paused,
 *  shows a prominent "Paused" pill + a Resume action; otherwise a Pause action.
 *  `pending` disables the button while a toggle round-trips. */
export function PauseToggle({
  paused,
  pending,
  onToggle,
}: {
  paused: boolean;
  pending: boolean;
  onToggle: () => void;
}) {
  return (
    <div className="flex items-center gap-2">
      {paused ? (
        <span className="inline-flex items-center gap-1.5 rounded-md bg-amber-500/15 px-2 py-1 text-xs font-medium text-amber-600 dark:text-amber-400">
          <span className="h-1.5 w-1.5 rounded-full bg-amber-500" />
          Paused — no new runs
        </span>
      ) : null}
      <button
        type="button"
        onClick={onToggle}
        disabled={pending}
        aria-pressed={paused}
        title={
          paused
            ? "Resume dispatch — the daemon starts new runs again"
            : "Pause dispatch — the daemon starts no new runs (in-flight runs continue)"
        }
        className={cn(
          // 44px tap target on phones for one-handed pause/resume; compact ≥sm.
          "inline-flex h-11 items-center gap-1.5 rounded-md px-3 text-sm font-medium transition-colors disabled:pointer-events-none disabled:opacity-50 sm:h-9",
          paused
            ? "bg-primary text-primary-foreground hover:bg-primary/90"
            : "border border-border hover:bg-secondary",
        )}
      >
        <Icon name={paused ? "play" : "pause"} size={14} />
        {paused ? "Resume" : "Pause"}
      </button>
    </div>
  );
}

/** Wires the pause toggle to the daemon: reads `/api/pause` (polled), and posts
 *  the flip. Renders nothing until the first read resolves so we never show a
 *  wrong state. Hidden entirely when the daemon exposes no pause control (503). */
function PauseControl() {
  const queryClient = useQueryClient();
  const pauseQuery = useQuery({
    queryKey: ["pause"],
    queryFn: fetchPauseState,
    refetchInterval: 10_000,
  });
  const mutation = useMutation({
    mutationFn: (next: boolean) => setPauseState(next),
    onSuccess: (state) => queryClient.setQueryData(["pause"], state),
  });
  if (pauseQuery.data === undefined) {
    return null;
  }
  const paused = pauseQuery.data.paused;
  return (
    <PauseToggle
      paused={paused}
      pending={mutation.isPending}
      onToggle={() => mutation.mutate(!paused)}
    />
  );
}

/** All-time token totals as a tidy 2×2 rail of the four categories, each with
 *  its shared palette swatch. No summed grand total, no spend. */
export function StatRail({ totals }: { totals: SpendTotals }) {
  return (
    <div className="grid grid-cols-2 gap-x-6 gap-y-4">
      {TOKEN_CATS.map((c) => (
        <div key={c.key}>
          <div className="flex items-center gap-1.5 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
            <span className={cn("h-2 w-2 rounded-sm", c.swatch)} />
            {c.label}
          </div>
          <div className="mt-1 font-mono text-2xl font-semibold tracking-tight">
            <Tk value={totals[c.key]} />
          </div>
        </div>
      ))}
    </div>
  );
}

/** The shared in / out / cache-write / cache-read legend for the breakdown bars. */
export function MixLegend() {
  return (
    <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
      {TOKEN_CATS.map((c) => (
        <span
          key={c.key}
          className="flex items-center gap-1.5 text-[11px] text-muted-foreground"
        >
          <span className={cn("h-2 w-2 rounded-sm", c.swatch)} />
          {c.label}
        </span>
      ))}
    </div>
  );
}

/** One unified breakdown row — a team, a provider/model, or a stage — carrying
 *  the four token figures plus enough identity to render its name cell. */
type BreakdownRow = TokenSplit & {
  rowKey: string;
  issues: number;
  teamKey?: string;
  provider?: string;
  model?: string;
  stageKey?: string;
};

const NUM_COLS: Array<{ key: keyof TokenSplit; head: string }> = [
  { key: "input_tokens", head: "IN" },
  { key: "output_tokens", head: "OUT" },
  { key: "cache_write_tokens", head: "CACHE-WRITE" },
  { key: "cache_read_tokens", head: "CACHE-READ" },
];

function rowTotal(r: TokenSplit): number {
  return (
    r.input_tokens + r.output_tokens + r.cache_write_tokens + r.cache_read_tokens
  );
}

/** The unified Breakdown table — renders team, provider/model, or stage rows
 *  with the same columns and a magnitude mix bar (length = sum of tokens).
 *  Team/model are click-to-sort on the four numeric columns (descending); the
 *  stage view is non-sortable, kept in the caller's pipeline order, and gains a
 *  "Share" column showing each stage's percentage of total output tokens. */
export function BreakdownTable({
  rows,
  kind,
  barMode = "magnitude",
  selectedKeys,
  onToggleRow,
}: {
  rows: BreakdownRow[];
  kind: "team" | "model" | "stage";
  barMode?: "composition" | "magnitude";
  // When provided, rows are click-to-select (toggle) and the selected set is
  // highlighted; the charts above filter to it. Sorting still works via the
  // column headers (their clicks don't bubble to row selection).
  selectedKeys?: Set<string>;
  onToggleRow?: (key: string) => void;
}) {
  const [sortKey, setSortKey] = useState<keyof TokenSplit>("output_tokens");
  const sortable = kind !== "stage";
  // Stage stays in the caller's pipeline order; team/model sort by the column.
  const sorted = sortable
    ? [...rows].sort((a, b) => b[sortKey] - a[sortKey])
    : rows;
  const maxTotal = rows.length ? Math.max(...rows.map(rowTotal)) : 0;
  const totalOutput = rows.reduce((s, r) => s + r.output_tokens, 0);

  if (rows.length === 0) {
    return (
      <div className="rounded-md border border-border p-6 text-sm text-muted-foreground">
        No teams/models match the current filters
      </div>
    );
  }

  const nameHead =
    kind === "team" ? "Team" : kind === "model" ? "Provider / model" : "Stage";

  return (
    <div className="overflow-x-auto rounded-md border border-border">
      <table className="w-full caption-bottom text-sm">
        <thead>
          <tr className="border-b border-border bg-secondary/40 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
            <th className="px-3 py-1.5 text-left font-medium">{nameHead}</th>
            <th className="px-3 py-1.5 text-right font-medium">Issues</th>
            {kind === "stage" && (
              <th className="px-3 py-1.5 text-right font-medium">Share</th>
            )}
            <th className="w-[180px] px-3 py-1.5 text-left font-medium">Mix</th>
            {NUM_COLS.map((c) => {
              if (!sortable) {
                return (
                  <th
                    key={c.key}
                    className="px-3 py-1.5 text-right font-medium"
                  >
                    {c.head}
                  </th>
                );
              }
              const active = sortKey === c.key;
              return (
                <th
                  key={c.key}
                  aria-sort={active ? "descending" : undefined}
                  className="px-3 py-1.5 text-right font-medium"
                >
                  <button
                    type="button"
                    onClick={() => setSortKey(c.key)}
                    className={cn(
                      "inline-flex items-center gap-1 transition-colors hover:text-foreground",
                      active && "text-foreground",
                    )}
                  >
                    {c.head}
                    {active && (
                      <Icon name="chevronDown" size={11} strokeWidth={2} />
                    )}
                  </button>
                </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {sorted.map((r) => {
            const sharePct =
              totalOutput > 0
                ? Math.round((r.output_tokens / totalOutput) * 100)
                : 0;
            const selected = selectedKeys?.has(r.rowKey) ?? false;
            return (
              <tr
                key={r.rowKey}
                aria-selected={onToggleRow ? selected : undefined}
                title={
                  onToggleRow
                    ? selected
                      ? "Click to unpin — charts show everything again"
                      : "Click to pin this row — charts filter to the selection"
                    : undefined
                }
                onClick={onToggleRow ? () => onToggleRow(r.rowKey) : undefined}
                className={cn(
                  "border-b border-border/70 transition-colors last:border-0 hover:bg-secondary/50",
                  onToggleRow && "cursor-pointer select-none",
                  selected && "bg-secondary",
                )}
              >
                <td className="whitespace-nowrap px-3 py-2.5">
                  {kind === "team" ? (
                    <span className="flex items-center gap-2">
                      <span
                        className={cn(
                          "h-2 w-2 shrink-0 rounded-full",
                          TEAM_TINT[r.teamKey ?? ""] ?? "bg-slate-400",
                        )}
                      />
                      <span className="text-sm font-medium">{r.teamKey}</span>
                    </span>
                  ) : kind === "model" ? (
                    <span className="flex items-center gap-2">
                      <span
                        className={cn(
                          "h-2 w-2 shrink-0 rounded-full",
                          PROVIDER_TINT[r.provider ?? ""] ?? "bg-slate-400",
                        )}
                      />
                      <span className="text-sm font-medium">{r.model}</span>
                      <span className="text-xs text-muted-foreground">
                        {r.provider}
                      </span>
                    </span>
                  ) : (
                    <span className="flex items-center gap-2">
                      <span
                        className={cn(
                          "h-2 w-2 shrink-0 rounded-full",
                          STAGE_TINT[r.stageKey ?? ""] ?? "bg-slate-400",
                        )}
                      />
                      <span className="text-sm font-medium">
                        {STAGE_LABEL[r.stageKey ?? ""] ?? r.stageKey}
                      </span>
                    </span>
                  )}
                </td>
                <td className="whitespace-nowrap px-3 py-2.5 text-right font-mono text-xs tabular-nums text-muted-foreground">
                  {r.issues}
                </td>
                {kind === "stage" && (
                  <td className="whitespace-nowrap px-3 py-2.5 text-right font-mono text-xs tabular-nums text-muted-foreground">
                    {sharePct}%
                  </td>
                )}
                <td className="px-3 py-2.5">
                  <MixBar split={r} mode={barMode} maxTotal={maxTotal} />
                </td>
                {NUM_COLS.map((c) => (
                  <td
                    key={c.key}
                    className="whitespace-nowrap px-3 py-2.5 text-right font-mono text-xs tabular-nums"
                  >
                    <Tk value={r[c.key]} />
                  </td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

/** The token overview card: heatmap + all-time stat rail on top, then a single
 *  unified breakdown table toggled between By team / By model. */
export function TokenOverview({
  summary,
  heatmap,
  provider,
  date,
  window,
}: {
  summary?: SpendSummary;
  heatmap?: SpendHeatmap;
  provider: Provider;
  date: DateFilter;
  window: { from: string | null; to: string | null };
}) {
  const [view, setView] = useState<"team" | "model" | "stage">("team");
  // Totals vs Trend sub-view, available in every breakdown view.
  const [stageMode, setStageMode] = useState<"totals" | "trend">("totals");
  // Tokens vs % share metric for the Trend chart; owned here so its toggle can
  // sit in the shared Breakdown header row.
  const [trendMetric, setTrendMetric] = useState<"tokens" | "share">("tokens");
  // Rows pinned by clicking the table: the charts (totals bar + trend) collapse
  // to just these. Empty = no filter (show all). Reset when the view changes,
  // since a key from one view (e.g. a team) is meaningless in another.
  const [selectedKeys, setSelectedKeys] = useState<Set<string>>(new Set());
  useEffect(() => setSelectedKeys(new Set()), [view]);
  const toggleRow = (key: string) =>
    setSelectedKeys((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });

  // The trend series follows the active view (stage / team / model) and the
  // resolved date window; fetched only while Trend is showing. Filters mirror
  // the summary query so the trend reconciles with the totals table.
  const { teams, models } = useFilters();
  const providerFilter = provider === "all" ? undefined : provider;
  const teamsKey = [...teams].sort().join(",");
  const teamsFilter = teams.length ? teams : undefined;
  const modelsKey = [...models].sort().join(",");
  const modelsFilter = models.length ? models : undefined;
  const seriesQuery = useQuery({
    queryKey: [
      "spend-series",
      view,
      provider,
      teamsKey,
      modelsKey,
      window.from,
      window.to,
    ],
    queryFn: () =>
      fetchSpendStageSeries(
        view,
        providerFilter,
        teamsFilter,
        modelsFilter,
        window.from ?? undefined,
        window.to ?? undefined,
      ),
    enabled: stageMode === "trend",
    refetchInterval: 30_000,
    placeholderData: (prev) => prev,
  });
  const series = seriesQuery.data;

  const teamRows: BreakdownRow[] = (summary?.per_team ?? []).map((t) => ({
    rowKey: t.key,
    teamKey: t.key,
    issues: t.issues,
    input_tokens: t.input_tokens,
    output_tokens: t.output_tokens,
    cache_write_tokens: t.cache_write_tokens,
    cache_read_tokens: t.cache_read_tokens,
  }));
  // per_provider isn't provider-scoped server-side, so narrow to the active
  // provider here — keeps "By model" reconciled with the rail + team rows.
  const modelRows: BreakdownRow[] = (summary?.per_provider ?? [])
    .filter((p) => provider === "all" || p.provider === provider)
    .flatMap((p) =>
      p.per_model.map((m) => ({
        rowKey: `${p.provider}/${m.model}`,
        provider: p.provider,
        model: m.model,
        issues: m.issues,
        input_tokens: m.input_tokens,
        output_tokens: m.output_tokens,
        cache_write_tokens: m.cache_write_tokens,
        cache_read_tokens: m.cache_read_tokens,
      })),
    );
  // per_stage is provider-scoped server-side, so it reconciles with the rail as
  // returned; just reorder into pipeline order (unknown stages appended).
  const sortedStages = [...(summary?.per_stage ?? [])].sort(
    (a, b) => stageRank(a.key) - stageRank(b.key),
  );
  const stageRows: BreakdownRow[] = sortedStages.map((s) => ({
    rowKey: s.key,
    stageKey: s.key,
    issues: s.issues,
    input_tokens: s.input_tokens,
    output_tokens: s.output_tokens,
    cache_write_tokens: s.cache_write_tokens,
    cache_read_tokens: s.cache_read_tokens,
  }));
  const rows =
    view === "team" ? teamRows : view === "model" ? modelRows : stageRows;
  // The totals bar's segments (output-token share) and its palette: stage uses
  // the default pipeline palette, team/model reuse their trend adapter so the
  // bar matches the table's row dots.
  const barAdapter = view === "stage" ? undefined : TREND_ADAPTERS[view];
  const barRows = rows.map((r) => ({
    key: r.rowKey,
    output_tokens: r.output_tokens,
  }));

  // Charts collapse to the pinned rows; empty selection = show everything.
  const hasSelection = selectedKeys.size > 0;
  const chartBarRows = hasSelection
    ? barRows.filter((r) => selectedKeys.has(r.key))
    : barRows;
  const chartSeries =
    series && hasSelection
      ? {
          ...series,
          stages: series.stages.filter((s) => selectedKeys.has(s)),
          buckets: series.buckets.map((b) => ({
            ...b,
            output_tokens: Object.fromEntries(
              Object.entries(b.output_tokens).filter(([k]) =>
                selectedKeys.has(k),
              ),
            ),
          })),
        }
      : series;

  return (
    <Card className="p-5">
      {/* top: heatmap (anchor) + all-time stat rail */}
      <div className="grid gap-6 lg:grid-cols-[minmax(0,1.55fr)_minmax(0,1fr)]">
        <div className="min-w-0">
          <div className="mb-3 flex items-baseline justify-between gap-3">
            <h2 className="text-sm font-semibold">Daily token burn</h2>
            <span className="font-mono text-xs text-muted-foreground">
              last 12 months
            </span>
          </div>
          {heatmap ? (
            <Heatmap
              days={heatmap.days}
              start={heatmap.start}
              end={heatmap.end}
              highlightFrom={window.from}
              highlightTo={window.to}
            />
          ) : (
            <p className="text-sm text-muted-foreground">Loading…</p>
          )}
        </div>
        <div className="lg:border-l lg:border-border lg:pl-6">
          <div className="mb-4 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
            Tokens · {dateWindowLabel(date)}
            {provider !== "all" ? (
              <span className="text-foreground"> · {provider}</span>
            ) : null}
          </div>
          {summary ? (
            <StatRail totals={summary.totals} />
          ) : (
            <p className="text-sm text-muted-foreground">Loading…</p>
          )}
        </div>
      </div>

      {/* breakdown: one table, toggle team / model */}
      <div className="mt-6 border-t border-border pt-5">
        <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
          <div className="flex items-center gap-3">
            <h2 className="text-sm font-semibold">Breakdown</h2>
            <Segmented
              ariaLabel="Breakdown grouping"
              options={[
                { value: "team", label: "By team" },
                { value: "model", label: "By model" },
                { value: "stage", label: "By stage" },
              ]}
              value={view}
              onChange={(v) => setView(v as "team" | "model" | "stage")}
            />
            <Segmented
              ariaLabel="Breakdown view"
              options={[
                { value: "totals", label: "Totals" },
                { value: "trend", label: "Trend" },
              ]}
              value={stageMode}
              onChange={(v) => setStageMode(v as "totals" | "trend")}
            />
            {stageMode === "trend" ? (
              <Segmented
                ariaLabel="Trend metric"
                options={[
                  { value: "tokens", label: "Tokens" },
                  { value: "share", label: "% share" },
                ]}
                value={trendMetric}
                onChange={(v) => setTrendMetric(v as "tokens" | "share")}
              />
            ) : null}
            {hasSelection ? (
              <button
                type="button"
                onClick={() => setSelectedKeys(new Set())}
                className="text-xs text-muted-foreground underline-offset-2 hover:text-foreground hover:underline"
              >
                Clear ({selectedKeys.size})
              </button>
            ) : null}
          </div>
          {stageMode === "trend" ? (
            <span className="font-mono text-[11px] text-muted-foreground">
              output tokens · {series?.bucket === "week" ? "weekly" : "daily"}
            </span>
          ) : (
            <MixLegend />
          )}
        </div>
        {stageMode === "trend" ? (
          chartSeries ? (
            <StageTrend
              series={chartSeries}
              mode={trendMetric}
              adapter={barAdapter}
            />
          ) : (
            <p className="text-sm text-muted-foreground">Loading…</p>
          )
        ) : (
          <LifecycleBar
            rows={chartBarRows}
            label={barAdapter?.label}
            tint={barAdapter?.tint}
          />
        )}
        {/* The breakdown table shows in both modes — under the totals bar, and
            under the trend chart (so the figures stay visible while charting).
            Clicking a row pins it; the charts above collapse to the selection. */}
        <div className="mt-4">
          <BreakdownTable
            rows={rows}
            kind={view}
            barMode="magnitude"
            selectedKeys={selectedKeys}
            onToggleRow={toggleRow}
          />
        </div>
      </div>
    </Card>
  );
}

export function SectionTotals({ issues }: { issues: IssueSummary[] }) {
  const tot = issues.reduce(
    (a, i) => ({
      inp: a.inp + i.input_tokens,
      out: a.out + i.output_tokens,
      cw: a.cw + i.cache_write_tokens,
      cr: a.cr + i.cache_read_tokens,
    }),
    { inp: 0, out: 0, cw: 0, cr: 0 },
  );
  return (
    <div className="flex flex-wrap gap-x-4 gap-y-1 font-mono text-xs text-muted-foreground">
      <span>in <Tk value={tot.inp} /></span>
      <span>out <Tk value={tot.out} /></span>
      <span>cache-write <Tk value={tot.cw} /></span>
      <span>cache-read <Tk value={tot.cr} /></span>
    </div>
  );
}

/** Kanban lanes in pipeline order. `mixed` lanes hold several statuses, so
 *  their cards keep a status badge to disambiguate. Running issues are placed
 *  by their stage (the canonical status subtitle), everything else by state. */
export const BOARD_COLUMNS: Array<{
  key: string;
  label: string;
  dot: string;
  mixed?: boolean;
}> = [
  { key: "todo", label: "Todo", dot: "bg-slate-400" },
  { key: "waiting", label: "Waiting", dot: "bg-slate-500" },
  { key: "implement", label: "Implement", dot: "bg-blue-500" },
  { key: "attention", label: "Needs attention", dot: "bg-red-500", mixed: true },
  { key: "local_review", label: "Local review", dot: "bg-cyan-500" },
  { key: "review", label: "Review", dot: "bg-violet-500", mixed: true },
  { key: "merge", label: "Merge", dot: "bg-emerald-500", mixed: true },
  { key: "done", label: "Done", dot: "bg-green-500" },
];

/** Which lane a live run sits in, by its stage (fix stages ride with their
 *  parent stage; verify is the local pre-PR gate; acceptance gates the
 *  merge). Unknown stages → Implement. */
const RUNNING_STAGE_LANE: Record<string, string> = {
  implement: "implement",
  implement_fix: "implement",
  local_review: "local_review",
  local_review_fix: "local_review",
  verify: "local_review",
  verify_fix: "local_review",
  review: "review",
  review_fix: "review",
  merge: "merge",
  acceptance: "merge",
  acceptance_fix: "merge",
  done: "merge",
};

const STATE_TO_COLUMN: Record<string, string> = {
  todo: "todo",
  idle: "todo",
  waiting: "waiting",
  failed: "attention",
  halted: "attention",
  paused: "attention",
  drift_detected: "attention",
  awaiting_review_trigger: "review",
  pr_open: "review",
  awaiting_merge: "merge",
  done: "done",
};

/** Lane key for one issue: running runs go by stage, other statuses by the
 *  state map; unknown states land in "Needs attention" — never dropped. */
export function boardLane(issue: IssueSummary): string {
  const { state, subtitle } = issue.canonical_status;
  if (state === "running") {
    return RUNNING_STAGE_LANE[subtitle ?? ""] ?? "implement";
  }
  return STATE_TO_COLUMN[state] ?? "attention";
}

/** Bucket issues into board lanes, done-scope issues into Done. Newest
 *  activity first within a lane; the Todo/Waiting queue lanes keep the
 *  tracker's dispatch order (by identifier) instead. */
export function groupForBoard(
  active: IssueSummary[],
  done: IssueSummary[],
): Map<string, IssueSummary[]> {
  const lanes = new Map<string, IssueSummary[]>(BOARD_COLUMNS.map((c) => [c.key, []]));
  for (const i of active) {
    const key = boardLane(i);
    lanes.get(key === "done" ? "done" : key)!.push(i);
  }
  for (const i of done) {
    lanes.get("done")!.push(i);
  }
  for (const [key, issues] of lanes) {
    if (key === "todo" || key === "waiting") {
      issues.sort((a, b) =>
        a.identifier.localeCompare(b.identifier, undefined, { numeric: true }),
      );
      continue;
    }
    issues.sort((a, b) => {
      const ta = (key === "done" ? a.completed_at : a.latest_activity_ts) ?? "";
      const tb = (key === "done" ? b.completed_at : b.latest_activity_ts) ?? "";
      return tb.localeCompare(ta);
    });
  }
  return lanes;
}

function BoardCard({
  issue,
  showBadge,
  nowMs,
}: {
  issue: IssueSummary;
  showBadge: boolean;
  nowMs: number;
}) {
  const ts =
    issue.canonical_status.state === "done"
      ? issue.completed_at ?? issue.latest_activity_ts
      : issue.latest_activity_ts ?? issue.canonical_status.since;
  // Queue-only issues (tracked === false) have no daemon runs and thus no
  // issue page — their card opens the issue in Linear instead.
  const untracked = issue.tracked === false;
  const body = (
    <>
      <div className="flex items-center justify-between gap-2">
        <span className="flex items-center gap-1 font-mono text-xs font-semibold text-primary underline-offset-2 group-hover:underline">
          {issue.identifier}
          {untracked ? (
            <Icon name="external" size={11} className="text-muted-foreground" />
          ) : null}
        </span>
        <span
          className="shrink-0 font-mono text-[11px] text-muted-foreground"
          title={ts ? formatUtcTimestamp(ts) : undefined}
        >
          {ts ? formatRelativeTimestamp(ts, nowMs) : "—"}
        </span>
      </div>
      <p className="mt-1 line-clamp-2 text-xs leading-snug text-foreground">
        {issue.title}
      </p>
      <div className="mt-2 flex items-center justify-between gap-2">
        <span className="flex items-center gap-1.5 text-[11px] text-muted-foreground">
          <span
            className={cn(
              "h-1.5 w-1.5 shrink-0 rounded-full",
              TEAM_TINT[issue.team_key] ?? "bg-slate-400",
            )}
          />
          {issue.team_key}
        </span>
        {showBadge ? (
          <StatusBadge status={issue.canonical_status.state} live />
        ) : null}
      </div>
    </>
  );
  const cardClass =
    "group block rounded-md border border-border bg-background p-2.5 transition-colors hover:border-blue-400 dark:hover:border-blue-600";
  if (untracked) {
    return (
      <a
        href={linearIssueUrl(issue.identifier)}
        target="_blank"
        rel="noreferrer"
        title={`Open ${issue.identifier} in Linear`}
        className={cardClass}
      >
        {body}
      </a>
    );
  }
  return (
    <Link to={`/issue/${encodeURIComponent(issue.id)}`} className={cardClass}>
      {body}
    </Link>
  );
}

/** Max cards rendered in the Done lane before collapsing to a "+N more"
 *  affordance. The done feed is capped server-side (default 50); this keeps the
 *  board readable and nudges deeper browsing into the Table view. */
export const DONE_LANE_CARD_CAP = 30;

/** How many done issues the home feed fetches by default (matches the API's
 *  own default). The kanban shows the newest `DONE_LANE_CARD_CAP`; the rest
 *  are reachable via the "+N more" affordance / Table view's "Load more". */
const DONE_SCOPE_LIMIT = 50;

/** Ceiling for the done scope's `limit` param (matches the API's `le=500`).
 *  "Load more" in the Table view steps toward this instead of unbounded growth. */
const DONE_SCOPE_MAX_LIMIT = 500;

/** The issues kanban: one lane per pipeline step, every tracked issue exactly
 *  once. Cards are links to the issue page (⌘-click works); the identifier
 *  underlines on hover to signal it. The Done lane caps its card list and, when
 *  it overflows, shows a "+N more" button that calls `onShowMore` (the Table
 *  view sees the full fetched set). */
export function KanbanBoard({
  active,
  done,
  nowMs,
  onShowMore,
}: {
  active: IssueSummary[];
  done: IssueSummary[];
  nowMs: number;
  onShowMore?: () => void;
}) {
  const lanes = groupForBoard(active, done);
  return (
    <div className="flex gap-3 overflow-x-auto pb-1">
      {BOARD_COLUMNS.map((col) => {
        const issues = lanes.get(col.key) ?? [];
        const shown =
          col.key === "done" ? issues.slice(0, DONE_LANE_CARD_CAP) : issues;
        const overflow = issues.length - shown.length;
        return (
          <div
            key={col.key}
            className="flex min-w-[190px] flex-1 flex-col rounded-lg border border-border bg-secondary/20"
          >
            <div className="flex items-center gap-2 border-b border-border px-3 py-2">
              <span className={cn("h-2 w-2 shrink-0 rounded-full", col.dot)} />
              <span className="text-xs font-semibold">{col.label}</span>
              <span className="ml-auto font-mono text-[11px] tabular-nums text-muted-foreground">
                {issues.length}
              </span>
            </div>
            <div className="flex flex-col gap-2 p-2">
              {shown.length ? (
                shown.map((i) => (
                  <BoardCard
                    key={i.id}
                    issue={i}
                    showBadge={Boolean(col.mixed)}
                    nowMs={nowMs}
                  />
                ))
              ) : (
                <p className="py-3 text-center text-xs text-muted-foreground/60">
                  empty
                </p>
              )}
              {overflow > 0 ? (
                <button
                  type="button"
                  onClick={onShowMore}
                  className="rounded-md border border-dashed border-border py-2 text-center text-xs font-medium text-muted-foreground transition-colors hover:border-blue-400 hover:text-foreground dark:hover:border-blue-600"
                >
                  +{overflow} more — view in table
                </button>
              ) : null}
            </div>
          </div>
        );
      })}
    </div>
  );
}

export function IssueTable({
  issues,
  mode,
  nowMs,
  onOpen,
}: {
  issues: IssueSummary[];
  mode: "active" | "done";
  nowMs: number;
  onOpen: (id: string) => void;
}) {
  const headers = [
    "Identifier",
    "Status",
    mode === "done" ? "Completed" : "Last activity",
    "in",
    "out",
    "cache-write",
    "cache-read",
    "Title",
    "Team",
    "", // trailing chevron: the whole row opens the issue page
  ];
  return (
    <div className="w-full overflow-x-auto rounded-md border border-border">
      <table className="w-full caption-bottom text-sm">
        <thead>
          <tr className="border-b border-border bg-secondary/30">
            {headers.map((h, i) => (
              <th
                key={h || "nav"}
                className={cn(
                  "h-9 px-3 align-middle text-xs font-medium uppercase tracking-wide text-muted-foreground",
                  i >= 3 && i <= 6 ? "text-right" : "text-left",
                )}
              >
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {issues.map((issue) => {
            const state = issue.canonical_status.state;
            // Queued issues carry their age in canonical_status.since (time
            // they entered the Todo/Waiting lane), not latest_activity_ts.
            const queued = state === "todo" || state === "waiting";
            const ts =
              mode === "done"
                ? issue.completed_at
                : issue.latest_activity_ts ??
                  (queued ? issue.canonical_status.since : null);
            // Queue-only rows have no issue page — the row and identifier
            // both open Linear instead of a 404.
            const untracked = issue.tracked === false;
            return (
              <tr
                key={issue.id}
                className="group cursor-pointer border-b border-border/70 transition-colors last:border-0 hover:bg-secondary/50"
                onClick={() =>
                  untracked
                    ? window.open(linearIssueUrl(issue.identifier), "_blank", "noreferrer")
                    : onOpen(issue.id)
                }
              >
                <td className="whitespace-nowrap px-3 py-2.5 font-medium">
                  <span className="flex items-center gap-1.5">
                    {untracked ? (
                      <a
                        href={linearIssueUrl(issue.identifier)}
                        target="_blank"
                        rel="noreferrer"
                        onClick={(e) => e.stopPropagation()}
                        title={`Open ${issue.identifier} in Linear`}
                        className="text-primary underline-offset-4 hover:underline"
                      >
                        {issue.identifier}
                      </a>
                    ) : (
                      <Link
                        to={`/issue/${encodeURIComponent(issue.id)}`}
                        onClick={(e) => e.stopPropagation()}
                        className="text-primary underline-offset-4 hover:underline"
                      >
                        {issue.identifier}
                      </Link>
                    )}
                    <a
                      href={linearIssueUrl(issue.identifier)}
                      target="_blank"
                      rel="noreferrer"
                      onClick={(e) => e.stopPropagation()}
                      title={`Open ${issue.identifier} in Linear`}
                      className="text-muted-foreground transition-colors hover:text-foreground"
                    >
                      <Icon name="external" size={12} />
                    </a>
                  </span>
                </td>
                <td className="px-3 py-2.5">
                  <StatusBadge status={issue.canonical_status.state} live />
                </td>
                <td
                  className="w-32 whitespace-nowrap px-3 py-2.5 font-mono text-xs text-muted-foreground"
                  title={ts ? formatUtcTimestamp(ts) : undefined}
                >
                  {ts ? formatRelativeTimestamp(ts, nowMs) : "—"}
                </td>
                <td className="whitespace-nowrap px-3 py-2.5 text-right font-mono text-xs">
                  <Tk value={issue.input_tokens} />
                </td>
                <td className="whitespace-nowrap px-3 py-2.5 text-right font-mono text-xs">
                  <Tk value={issue.output_tokens} />
                </td>
                <td className="whitespace-nowrap px-3 py-2.5 text-right font-mono text-xs">
                  <Tk value={issue.cache_write_tokens} />
                </td>
                <td className="whitespace-nowrap px-3 py-2.5 text-right font-mono text-xs">
                  <Tk value={issue.cache_read_tokens} />
                </td>
                <td className="max-w-[26rem] truncate px-3 py-2.5">{issue.title}</td>
                <td className="px-3 py-2.5 text-muted-foreground">{issue.team_key}</td>
                <td className="w-8 px-2 py-2.5 text-right">
                  <Icon
                    name="chevronRight"
                    size={14}
                    className="inline text-muted-foreground/50 transition-colors group-hover:text-foreground"
                  />
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export function HomePage() {
  const navigate = useNavigate();
  const nowMs = useNowMs();
  const [issueView, setIssueView] = useState<"board" | "table">("board");
  const { provider, teams, models, date } = useFilters();
  // Day-granular bounds; the strings are stable across the 10s `nowMs` ticker,
  // so they don't churn the query keys.
  const { from, to } = resolveDateWindow(date, nowMs);
  const dateFrom = from ?? undefined;
  const dateTo = to ?? undefined;

  const providerFilter = provider === "all" ? undefined : provider;
  // Stable cache keys for the selections (order-independent).
  const teamsKey = [...teams].sort().join(",");
  const teamsFilter = teams.length ? teams : undefined;
  const modelsKey = [...models].sort().join(",");
  const modelsFilter = models.length ? models : undefined;

  // How many done issues to request; grows via the Table view's "Load more".
  // Reset to the default whenever the filters change so a bumped limit from a
  // prior filter selection doesn't leak into a new one.
  const [doneLimit, setDoneLimit] = useState(DONE_SCOPE_LIMIT);
  useEffect(() => {
    setDoneLimit(DONE_SCOPE_LIMIT);
  }, [provider, teamsKey, modelsKey, from, to]);

  const summaryQuery = useQuery({
    queryKey: ["spend-summary", provider, teamsKey, modelsKey, from, to],
    queryFn: () =>
      fetchSpendSummary(providerFilter, teamsFilter, modelsFilter, dateFrom, dateTo),
    refetchInterval: 30_000,
    placeholderData: (prev) => prev,
  });
  // The heatmap is the time axis itself — it stays a fixed 12-month canvas and
  // takes no date param; the window only highlights cells within it.
  const heatmapQuery = useQuery({
    queryKey: ["spend-heatmap", provider, teamsKey, modelsKey],
    queryFn: () => fetchSpendHeatmap(371, providerFilter, teamsFilter, modelsFilter),
    refetchInterval: 60_000,
    placeholderData: (prev) => prev,
  });
  // The trend series is fetched inside TokenOverview (it follows the active
  // breakdown view), so HomePage doesn't fetch it here.
  const activeQuery = useQuery({
    queryKey: ["issues", "active", provider, teamsKey, modelsKey, from, to],
    queryFn: () =>
      fetchIssues({
        scope: "active",
        provider: providerFilter,
        teams: teamsFilter,
        from: dateFrom,
        to: dateTo,
        models: modelsFilter,
      }),
    refetchInterval: 10_000,
    placeholderData: (prev) => prev,
  });
  const doneQuery = useQuery({
    queryKey: ["issues", "done", provider, teamsKey, modelsKey, from, to, doneLimit],
    queryFn: () =>
      fetchIssues({
        scope: "done",
        provider: providerFilter,
        teams: teamsFilter,
        from: dateFrom,
        to: dateTo,
        models: modelsFilter,
        limit: doneLimit,
      }),
    refetchInterval: 30_000,
    placeholderData: (prev) => prev,
  });

  const active = activeQuery.data ?? [];
  const done = doneQuery.data ?? [];
  const tracked = summaryQuery.data?.totals.issues ?? 0;
  // A full page implies more done history exists beyond what's fetched.
  const hasMoreDone = done.length === doneLimit && doneLimit < DONE_SCOPE_MAX_LIMIT;

  const openIssue = (id: string) => navigate(`/issue/${encodeURIComponent(id)}`);

  return (
    <main className="mx-auto w-full max-w-[1200px] px-4 py-6 sm:px-6 lg:px-8">
      <div className="mb-5 flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-xl font-semibold tracking-tight">Operator dashboard</h1>
          <p className="mt-0.5 text-sm text-muted-foreground">
            {tracked} issues tracked · {active.length + done.length} shown
          </p>
        </div>
        <PauseControl />
      </div>

      <TokenOverview
        summary={summaryQuery.data}
        heatmap={heatmapQuery.data}
        provider={provider}
        date={date}
        window={{ from, to }}
      />

      <section className="mt-7">
        <div className="mb-2.5 flex flex-wrap items-center justify-between gap-3">
          <div className="flex flex-wrap items-center gap-3">
            <h2 className="flex items-center gap-2 text-base font-semibold">
              Issues{" "}
              <span className="font-mono text-sm font-normal text-muted-foreground">
                · {active.length + done.length}
              </span>
            </h2>
            <Segmented
              ariaLabel="Issues view"
              options={[
                { value: "board", label: "Board" },
                { value: "table", label: "Table" },
              ]}
              value={issueView}
              onChange={(v) => setIssueView(v as "board" | "table")}
            />
          </div>
          <SectionTotals issues={[...active, ...done]} />
        </div>

        {issueView === "board" ? (
          <KanbanBoard
            active={active}
            done={done}
            nowMs={nowMs}
            onShowMore={() => setIssueView("table")}
          />
        ) : (
          <>
            <div className="mb-2.5 flex flex-wrap items-center justify-between gap-3">
              <h3 className="flex items-center gap-2 text-sm font-semibold">
                Active{" "}
                <span className="font-mono text-xs font-normal text-muted-foreground">
                  · {active.length}
                </span>
              </h3>
            </div>
            {active.length ? (
              <IssueTable
                issues={active}
                mode="active"
                nowMs={nowMs}
                onOpen={openIssue}
              />
            ) : (
              <div className="rounded-md border border-border p-6 text-sm text-muted-foreground">
                No active issues match your filters
              </div>
            )}

            <div className="mb-2.5 mt-6 flex flex-wrap items-center justify-between gap-3">
              <h3 className="flex items-center gap-2 text-sm font-semibold">
                Recently done{" "}
                <span className="font-mono text-xs font-normal text-muted-foreground">
                  · {done.length}
                </span>
              </h3>
            </div>
            {done.length ? (
              <>
                <IssueTable issues={done} mode="done" nowMs={nowMs} onOpen={openIssue} />
                {hasMoreDone ? (
                  <button
                    type="button"
                    onClick={() =>
                      setDoneLimit((l) => Math.min(l + DONE_SCOPE_LIMIT, DONE_SCOPE_MAX_LIMIT))
                    }
                    disabled={doneQuery.isFetching}
                    className="mt-2 w-full rounded-md border border-dashed border-border py-2 text-center text-xs font-medium text-muted-foreground transition-colors hover:border-blue-400 hover:text-foreground disabled:opacity-50 dark:hover:border-blue-600"
                  >
                    {doneQuery.isFetching ? "Loading…" : "Load more"}
                  </button>
                ) : null}
              </>
            ) : (
              <div className="rounded-md border border-border p-6 text-sm text-muted-foreground">
                No completed issues match your filters
              </div>
            )}
          </>
        )}
      </section>

      <footer className="mt-10 border-t border-border pt-4 text-xs text-muted-foreground">
        Completed = Linear <span className="font-mono">done</span> lane or all tracked
        PRs merged · completion time shown relative to now
      </footer>
    </main>
  );
}
