import { useQuery } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { Link, useNavigate } from "react-router";

import { Tk } from "@/components/dashboard/atoms";
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
  type TeamSpend,
} from "@/lib/api";
import { formatCost } from "@/lib/format";
import { cn } from "@/lib/utils";

import { formatRelativeTimestamp, formatUtcTimestamp } from "./activityFreshness";

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

function linearIssueUrl(identifier: string): string {
  return `https://linear.app/issue/${encodeURIComponent(identifier)}`;
}

export function HeadlineTotals({
  totals,
  compact = false,
}: {
  totals: SpendSummary["totals"];
  compact?: boolean;
}) {
  return (
    <div
      className={
        compact ? "flex flex-wrap items-end gap-x-8 gap-y-3" : "flex flex-col gap-1"
      }
    >
      <div>
        <div className="whitespace-nowrap text-xs font-medium uppercase tracking-wide text-muted-foreground">
          Total spend · all-time
        </div>
        <div className="mt-1 font-mono text-3xl font-semibold tracking-tight text-foreground">
          {formatCost(totals.cost_usd)}
        </div>
      </div>
      <div>
        <div className="whitespace-nowrap text-xs font-medium uppercase tracking-wide text-muted-foreground">
          Total tokens
        </div>
        <div className="mt-1 font-mono text-3xl font-semibold tracking-tight text-foreground">
          <Tk value={totals.total_tokens} />
        </div>
      </div>
      <div className="flex basis-full flex-wrap gap-x-4 gap-y-1 font-mono text-xs text-muted-foreground">
        <span>in <Tk value={totals.input_tokens} /></span>
        <span>out <Tk value={totals.output_tokens} /></span>
        <span>cache-write <Tk value={totals.cache_write_tokens} /></span>
        <span>cache-read <Tk value={totals.cache_read_tokens} /></span>
      </div>
    </div>
  );
}

export function PerTeam({
  teams,
  onPick,
}: {
  teams: TeamSpend[];
  onPick?: (key: string) => void;
}) {
  const sorted = [...teams].sort((a, b) => b.cost_usd - a.cost_usd);
  return (
    <div className="divide-y divide-border/70 overflow-hidden rounded-md border border-border">
      <div className="grid grid-cols-[1fr_auto_auto] items-center gap-4 bg-secondary/40 px-3 py-1.5 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
        <span>Team</span>
        <span className="text-right">Spend</span>
        <span className="w-20 text-right">Tokens</span>
      </div>
      {sorted.map((t) => (
        <button
          key={t.key}
          type="button"
          onClick={() => onPick?.(t.key)}
          className="grid w-full grid-cols-[1fr_auto_auto] items-center gap-4 px-3 py-2 text-left transition-colors hover:bg-secondary/60"
        >
          <span className="flex items-center gap-2">
            <span
              className={cn(
                "h-2 w-2 shrink-0 rounded-full",
                TEAM_TINT[t.key] ?? "bg-slate-400",
              )}
            />
            <span className="text-sm font-medium">{t.key}</span>
            <span className="whitespace-nowrap text-xs text-muted-foreground">
              {t.issues} issues
            </span>
          </span>
          <span className="text-right font-mono text-sm tabular-nums">
            {formatCost(t.cost_usd)}
          </span>
          <span className="w-20 text-right font-mono text-xs text-muted-foreground">
            <Tk value={t.total_tokens} />
          </span>
        </button>
      ))}
    </div>
  );
}

function SpendOverview({
  summary,
  heatmap,
  onPickTeam,
}: {
  summary?: SpendSummary;
  heatmap?: SpendHeatmap;
  onPickTeam: (key: string) => void;
}) {
  return (
    <Card className="p-5">
      <div className="grid gap-6 lg:grid-cols-[minmax(0,1.4fr)_minmax(0,1fr)]">
        <div className="min-w-0">
          <div className="mb-3 flex items-baseline justify-between">
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
          {summary ? (
            <>
              <HeadlineTotals totals={summary.totals} compact />
              <div className="mt-5">
                <div className="mb-2.5 flex items-center justify-between">
                  <h2 className="text-sm font-semibold">Spend by team</h2>
                  <span className="text-xs text-muted-foreground">by spend</span>
                </div>
                <PerTeam teams={summary.per_team} onPick={onPickTeam} />
              </div>
            </>
          ) : (
            <p className="text-sm text-muted-foreground">Loading…</p>
          )}
        </div>
      </div>
    </Card>
  );
}

export function SectionTotals({ issues }: { issues: IssueSummary[] }) {
  const tot = issues.reduce(
    (a, i) => ({
      cost: a.cost + i.cost_usd,
      inp: a.inp + i.input_tokens,
      out: a.out + i.output_tokens,
      cw: a.cw + i.cache_write_tokens,
      cr: a.cr + i.cache_read_tokens,
    }),
    { cost: 0, inp: 0, out: 0, cw: 0, cr: 0 },
  );
  return (
    <div className="flex flex-wrap gap-x-4 gap-y-1 font-mono text-xs text-muted-foreground">
      <span className="text-foreground">{formatCost(tot.cost)}</span>
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
    "$",
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
                  i >= 3 && i <= 7 ? "text-right" : "text-left",
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
                <td className="whitespace-nowrap px-3 py-2.5 text-right font-mono text-xs tabular-nums">
                  {formatCost(issue.cost_usd)}
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
  const win = DONE_WINDOWS.find((w) => w.value === doneWindow) ?? DONE_WINDOWS[1];

  const summaryQuery = useQuery({
    queryKey: ["spend-summary"],
    queryFn: fetchSpendSummary,
    refetchInterval: 30_000,
  });
  const heatmapQuery = useQuery({
    queryKey: ["spend-heatmap"],
    queryFn: () => fetchSpendHeatmap(371),
    refetchInterval: 60_000,
  });
  const activeQuery = useQuery({
    queryKey: ["issues", "active"],
    queryFn: () => fetchIssues({ scope: "active" }),
    refetchInterval: 10_000,
    placeholderData: (prev) => prev,
  });
  const doneQuery = useQuery({
    queryKey: ["issues", "done", win.secs],
    queryFn: () => fetchIssues({ scope: "done", withinSecs: win.secs }),
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

      <SpendOverview
        summary={summaryQuery.data}
        heatmap={heatmapQuery.data}
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
