import { useQuery } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { Link, useNavigate } from "react-router";

import {
  MixBar,
  PROVIDER_TINT,
  Tk,
  TOKEN_CATS,
} from "@/components/dashboard/atoms";
import { Heatmap } from "@/components/dashboard/Heatmap";
import { StatusBadge } from "@/components/dashboard/StatusBadge";
import { Card } from "@/components/ui/card";
import { Icon } from "@/components/ui/icon";
import { Segmented } from "@/components/ui/segmented";
import {
  fetchIssues,
  fetchSpendHeatmap,
  fetchSpendSummary,
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

/** One unified breakdown row — a team or a provider/model — carrying the four
 *  token figures plus enough identity to render its name cell. */
type BreakdownRow = TokenSplit & {
  rowKey: string;
  issues: number;
  teamKey?: string;
  provider?: string;
  model?: string;
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

/** The unified Breakdown table — renders team rows or provider/model rows with
 *  the same columns, a magnitude mix bar (length = sum of tokens), and click-to-
 *  sort on the four numeric columns (descending). */
export function BreakdownTable({
  rows,
  kind,
  barMode = "magnitude",
}: {
  rows: BreakdownRow[];
  kind: "team" | "model";
  barMode?: "composition" | "magnitude";
}) {
  const [sortKey, setSortKey] = useState<keyof TokenSplit>("output_tokens");
  const sorted = [...rows].sort((a, b) => b[sortKey] - a[sortKey]);
  const maxTotal = rows.length ? Math.max(...rows.map(rowTotal)) : 0;

  if (rows.length === 0) {
    return (
      <div className="rounded-md border border-border p-6 text-sm text-muted-foreground">
        No teams/models match the current filters
      </div>
    );
  }

  return (
    <div className="overflow-x-auto rounded-md border border-border">
      <table className="w-full caption-bottom text-sm">
        <thead>
          <tr className="border-b border-border bg-secondary/40 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
            <th className="px-3 py-1.5 text-left font-medium">
              {kind === "team" ? "Team" : "Provider / model"}
            </th>
            <th className="px-3 py-1.5 text-right font-medium">Issues</th>
            <th className="w-[180px] px-3 py-1.5 text-left font-medium">Mix</th>
            {NUM_COLS.map((c) => {
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
            return (
              <tr
                key={r.rowKey}
                className="border-b border-border/70 transition-colors last:border-0 hover:bg-secondary/50"
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
                  ) : (
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
                  )}
                </td>
                <td className="whitespace-nowrap px-3 py-2.5 text-right font-mono text-xs tabular-nums text-muted-foreground">
                  {r.issues}
                </td>
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
  const [view, setView] = useState<"team" | "model">("team");

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
  const rows = view === "team" ? teamRows : modelRows;

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
              ]}
              value={view}
              onChange={(v) => setView(v as "team" | "model")}
            />
          </div>
          <MixLegend />
        </div>
        <BreakdownTable rows={rows} kind={view} barMode="magnitude" />
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
  ];
  return (
    <div className="w-full overflow-x-auto rounded-md border border-border">
      <table className="w-full caption-bottom text-sm">
        <thead>
          <tr className="border-b border-border bg-secondary/30">
            {headers.map((h, i) => (
              <th
                key={h}
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
            const ts = mode === "done" ? issue.completed_at : issue.latest_activity_ts;
            return (
              <tr
                key={issue.id}
                className="cursor-pointer border-b border-border/70 transition-colors last:border-0 hover:bg-secondary/50"
                onClick={() => onOpen(issue.id)}
              >
                <td className="whitespace-nowrap px-3 py-2.5 font-medium">
                  <a
                    href={linearIssueUrl(issue.identifier)}
                    target="_blank"
                    rel="noreferrer"
                    onClick={(e) => e.stopPropagation()}
                    className="text-primary underline-offset-4 hover:underline"
                  >
                    {issue.identifier}
                  </a>
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
                <td className="max-w-[26rem] px-3 py-2.5">
                  <Link
                    to={`/issue/${encodeURIComponent(issue.id)}`}
                    onClick={(e) => e.stopPropagation()}
                    className="text-foreground underline-offset-4 hover:underline"
                  >
                    {issue.title}
                  </Link>
                </td>
                <td className="px-3 py-2.5 text-muted-foreground">{issue.team_key}</td>
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
    queryKey: ["issues", "done", provider, teamsKey, modelsKey, from, to],
    queryFn: () =>
      fetchIssues({
        scope: "done",
        provider: providerFilter,
        teams: teamsFilter,
        from: dateFrom,
        to: dateTo,
        models: modelsFilter,
      }),
    refetchInterval: 30_000,
    placeholderData: (prev) => prev,
  });

  const active = activeQuery.data ?? [];
  const done = doneQuery.data ?? [];
  const tracked = summaryQuery.data?.totals.issues ?? 0;

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
          <h2 className="flex items-center gap-2 text-base font-semibold">
            Active{" "}
            <span className="font-mono text-sm font-normal text-muted-foreground">
              · {active.length}
            </span>
          </h2>
          <SectionTotals issues={active} />
        </div>
        {active.length ? (
          <IssueTable issues={active} mode="active" nowMs={nowMs} onOpen={openIssue} />
        ) : (
          <div className="rounded-md border border-border p-6 text-sm text-muted-foreground">
            No active issues match your filters
          </div>
        )}
      </section>

      <section className="mt-7">
        <div className="mb-2.5 flex flex-wrap items-center justify-between gap-3">
          <h2 className="flex items-center gap-2 text-base font-semibold">
            Recently done{" "}
            <span className="font-mono text-sm font-normal text-muted-foreground">
              · {done.length}
            </span>
          </h2>
          <SectionTotals issues={done} />
        </div>
        {done.length ? (
          <IssueTable issues={done} mode="done" nowMs={nowMs} onOpen={openIssue} />
        ) : (
          <div className="rounded-md border border-border p-6 text-sm text-muted-foreground">
            No completed issues match your filters
          </div>
        )}
      </section>

      <footer className="mt-10 border-t border-border pt-4 text-xs text-muted-foreground">
        Completed = Linear <span className="font-mono">done</span> lane or all tracked
        PRs merged · completion time shown relative to now
      </footer>
    </main>
  );
}
