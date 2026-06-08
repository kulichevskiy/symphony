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
import { Segmented, type SegmentedOption } from "@/components/ui/segmented";
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
import { type Provider, useProviderFilter } from "@/lib/providerFilter";
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

const DONE_WINDOWS: Array<SegmentedOption & { secs: number }> = [
  { value: "24h", label: "24h", secs: 86_400 },
  { value: "7d", label: "7d", secs: 7 * 86_400 },
  { value: "30d", label: "30d", secs: 30 * 86_400 },
];

function useNowMs(): number {
  const [nowMs, setNowMs] = useState(() => Date.now());
  useEffect(() => {
    const id = window.setInterval(() => setNowMs(Date.now()), 10_000);
    return () => window.clearInterval(id);
  }, []);
  return nowMs;
}

/** Debounce a value so the search box doesn't refetch the overview per keystroke. */
function useDebounced<T>(value: T, ms: number): T {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const id = window.setTimeout(() => setDebounced(value), ms);
    return () => window.clearTimeout(id);
  }, [value, ms]);
  return debounced;
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
  onPick,
}: {
  rows: BreakdownRow[];
  kind: "team" | "model";
  barMode?: "composition" | "magnitude";
  onPick?: (key: string) => void;
}) {
  const [sortKey, setSortKey] = useState<keyof TokenSplit>("output_tokens");
  const sorted = [...rows].sort((a, b) => b[sortKey] - a[sortKey]);
  const maxTotal = rows.length ? Math.max(...rows.map(rowTotal)) : 0;

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
            const clickable = kind === "team" && onPick;
            return (
              <tr
                key={r.rowKey}
                className={cn(
                  "border-b border-border/70 transition-colors last:border-0 hover:bg-secondary/50",
                  clickable && "cursor-pointer",
                )}
                onClick={clickable ? () => onPick(r.teamKey ?? r.rowKey) : undefined}
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
  onPickTeam,
}: {
  summary?: SpendSummary;
  heatmap?: SpendHeatmap;
  provider: Provider;
  onPickTeam: (key: string) => void;
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
            <Heatmap days={heatmap.days} start={heatmap.start} end={heatmap.end} />
          ) : (
            <p className="text-sm text-muted-foreground">Loading…</p>
          )}
        </div>
        <div className="lg:border-l lg:border-border lg:pl-6">
          <div className="mb-4 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
            Tokens · all-time
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
        <BreakdownTable
          rows={rows}
          kind={view}
          barMode="magnitude"
          onPick={view === "team" ? onPickTeam : undefined}
        />
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
  const [query, setQuery] = useState("");
  const [doneWindow, setDoneWindow] = useState("7d");
  const { provider } = useProviderFilter();
  const win = DONE_WINDOWS.find((w) => w.value === doneWindow) ?? DONE_WINDOWS[1];

  const providerFilter = provider === "all" ? undefined : provider;
  // The trimmed search term scopes the overview (totals + breakdown + heatmap)
  // to matching issues — debounced so typing doesn't refetch per keystroke.
  const debouncedQuery = useDebounced(query.trim(), 250);
  const overviewQuery = debouncedQuery || undefined;

  // Unscoped (provider-only) summary: drives the stable "issues tracked" count
  // and the overview when no search is active, so search never shrinks "tracked".
  const baseSummaryQuery = useQuery({
    queryKey: ["spend-summary", provider],
    queryFn: () => fetchSpendSummary(providerFilter),
    refetchInterval: 30_000,
    placeholderData: (prev) => prev,
  });
  // Search-scoped summary: only fetched while a query is active.
  const scopedSummaryQuery = useQuery({
    queryKey: ["spend-summary", provider, overviewQuery],
    queryFn: () => fetchSpendSummary(providerFilter, overviewQuery),
    enabled: overviewQuery != null,
    refetchInterval: 30_000,
    placeholderData: (prev) => prev,
  });
  const summaryQuery = overviewQuery != null ? scopedSummaryQuery : baseSummaryQuery;
  const heatmapQuery = useQuery({
    queryKey: ["spend-heatmap", provider, overviewQuery],
    queryFn: () => fetchSpendHeatmap(371, providerFilter, overviewQuery),
    refetchInterval: 60_000,
    placeholderData: (prev) => prev,
  });
  const activeQuery = useQuery({
    queryKey: ["issues", "active", provider],
    queryFn: () => fetchIssues({ scope: "active", provider: providerFilter }),
    refetchInterval: 10_000,
    placeholderData: (prev) => prev,
  });
  const doneQuery = useQuery({
    queryKey: ["issues", "done", win.secs, provider],
    queryFn: () =>
      fetchIssues({ scope: "done", withinSecs: win.secs, provider: providerFilter }),
    refetchInterval: 30_000,
    placeholderData: (prev) => prev,
  });

  const q = query.trim().toLowerCase();
  const matches = (i: IssueSummary) =>
    !q ||
    i.identifier.toLowerCase().includes(q) ||
    i.title.toLowerCase().includes(q) ||
    i.team_key.toLowerCase().includes(q);

  const active = (activeQuery.data ?? []).filter(matches);
  const done = (doneQuery.data ?? []).filter(matches);
  const tracked = baseSummaryQuery.data?.totals.issues ?? 0;

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
        <div className="relative">
          <Icon
            name="search"
            size={15}
            className="pointer-events-none absolute left-2.5 top-1/2 -translate-y-1/2 text-muted-foreground"
          />
          <input
            type="search"
            aria-label="Search issues"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search identifier or title"
            className="h-9 w-full rounded-md border border-input bg-background pl-8 pr-3 text-sm shadow-sm transition-colors placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring sm:w-72"
          />
        </div>
      </div>

      <TokenOverview
        summary={summaryQuery.data}
        heatmap={heatmapQuery.data}
        provider={provider}
        onPickTeam={(k) => setQuery(k)}
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
          <div className="flex items-center gap-3">
            <SectionTotals issues={done} />
            <Segmented
              ariaLabel="Completed window"
              options={DONE_WINDOWS}
              value={doneWindow}
              onChange={setDoneWindow}
            />
          </div>
        </div>
        {done.length ? (
          <IssueTable issues={done} mode="done" nowMs={nowMs} onOpen={openIssue} />
        ) : (
          <div className="rounded-md border border-border p-6 text-sm text-muted-foreground">
            Nothing completed in the last {doneWindow}
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
