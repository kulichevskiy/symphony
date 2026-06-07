import { useQuery } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { Link, useNavigate } from "react-router";

import { MixBar, Tk, TokenFigures } from "@/components/dashboard/atoms";
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
  type ProviderSpend,
  type SpendHeatmap,
  type SpendSummary,
  type TeamSpend,
} from "@/lib/api";
import { cn } from "@/lib/utils";

import { formatRelativeTimestamp, formatUtcTimestamp } from "./activityFreshness";

const TEAM_TINT: Record<string, string> = {
  VIB: "bg-blue-500",
  ADJ: "bg-violet-500",
  LP: "bg-cyan-500",
  SYM: "bg-emerald-500",
  HQ: "bg-amber-500",
};

const HEATMAP_PROVIDERS: SegmentedOption[] = [
  { value: "all", label: "All" },
  { value: "claude", label: "claude" },
  { value: "codex", label: "codex" },
];

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
  const blocks: { label: string; value: number }[] = [
    { label: "input", value: totals.input_tokens },
    { label: "output", value: totals.output_tokens },
    { label: "cache-write", value: totals.cache_write_tokens },
    { label: "cache-read", value: totals.cache_read_tokens },
  ];
  return (
    <div className="flex flex-col gap-2">
      <div className="whitespace-nowrap text-xs font-medium uppercase tracking-wide text-muted-foreground">
        Tokens · all-time
      </div>
      <div
        className={
          compact
            ? "flex flex-wrap gap-x-8 gap-y-3"
            : "grid grid-cols-2 gap-x-8 gap-y-3 sm:grid-cols-4"
        }
      >
        {blocks.map((b) => (
          <div key={b.label}>
            <div className="whitespace-nowrap text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
              {b.label}
            </div>
            <div className="mt-0.5 font-mono text-xl font-semibold tracking-tight text-foreground">
              <Tk value={b.value} />
            </div>
          </div>
        ))}
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
  const sorted = [...teams].sort((a, b) => b.output_tokens - a.output_tokens);
  return (
    <div className="divide-y divide-border/70 overflow-hidden rounded-md border border-border">
      <div className="bg-secondary/40 px-3 py-1.5 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
        Team
      </div>
      {sorted.map((t) => (
        <button
          key={t.key}
          type="button"
          onClick={() => onPick?.(t.key)}
          className="flex w-full flex-col gap-1.5 px-3 py-2 text-left transition-colors hover:bg-secondary/60"
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
          <MixBar split={t} />
          <TokenFigures split={t} />
        </button>
      ))}
    </div>
  );
}

const PROVIDER_TINT: Record<string, string> = {
  claude: "bg-orange-500",
  codex: "bg-sky-500",
};

export function PerProvider({ providers }: { providers: ProviderSpend[] }) {
  const sorted = [...providers].sort((a, b) => b.output_tokens - a.output_tokens);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const toggle = (provider: string) =>
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(provider)) next.delete(provider);
      else next.add(provider);
      return next;
    });

  return (
    <div className="divide-y divide-border/70 overflow-hidden rounded-md border border-border">
      <div className="bg-secondary/40 px-3 py-1.5 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
        Provider / model
      </div>
      {sorted.map((p) => {
        const open = expanded.has(p.provider);
        const models = [...p.per_model].sort(
          (a, b) => b.output_tokens - a.output_tokens,
        );
        return (
          <div key={p.provider}>
            <button
              type="button"
              onClick={() => toggle(p.provider)}
              aria-expanded={open}
              className="flex w-full flex-col gap-1.5 px-3 py-2 text-left transition-colors hover:bg-secondary/60"
            >
              <span className="flex items-center gap-2">
                <Icon
                  name="chevronRight"
                  size={14}
                  className={cn(
                    "shrink-0 text-muted-foreground transition-transform",
                    open && "rotate-90",
                  )}
                />
                <span
                  className={cn(
                    "h-2 w-2 shrink-0 rounded-full",
                    PROVIDER_TINT[p.provider] ?? "bg-slate-400",
                  )}
                />
                <span className="text-sm font-medium">{p.provider}</span>
                <span className="whitespace-nowrap text-xs text-muted-foreground">
                  {p.issues} issues
                </span>
              </span>
              <MixBar split={p} />
              <TokenFigures split={p} />
            </button>
            {open && (
              <div className="divide-y divide-border/50 bg-secondary/20">
                {models.map((m) => (
                  <div
                    key={m.model}
                    className="flex flex-col gap-1.5 py-1.5 pl-9 pr-3"
                  >
                    <span className="truncate text-xs text-muted-foreground">
                      {m.model}
                    </span>
                    <MixBar split={m} />
                    <TokenFigures split={m} />
                  </div>
                ))}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

export function SpendOverview({
  summary,
  heatmap,
  heatProvider,
  onChangeHeatProvider,
  onPickTeam,
}: {
  summary?: SpendSummary;
  heatmap?: SpendHeatmap;
  heatProvider: string;
  onChangeHeatProvider: (value: string) => void;
  onPickTeam: (key: string) => void;
}) {
  return (
    <Card className="p-5">
      {/* Row 1: heatmap | all-time totals */}
      <div className="grid gap-6 lg:grid-cols-[minmax(0,1.4fr)_minmax(0,1fr)]">
        <div className="min-w-0">
          <div className="mb-3 flex items-baseline justify-between gap-3">
            <h2 className="text-sm font-semibold">Daily token burn</h2>
            <div className="flex items-center gap-3">
              <Segmented
                ariaLabel="Heatmap provider"
                options={HEATMAP_PROVIDERS}
                value={heatProvider}
                onChange={onChangeHeatProvider}
              />
              <span className="font-mono text-xs text-muted-foreground">
                last 12 months
              </span>
            </div>
          </div>
          {heatmap ? (
            <Heatmap days={heatmap.days} start={heatmap.start} end={heatmap.end} />
          ) : (
            <p className="text-sm text-muted-foreground">Loading…</p>
          )}
        </div>
        <div className="lg:border-l lg:border-border lg:pl-6">
          {summary ? (
            <HeadlineTotals totals={summary.totals} compact />
          ) : (
            <p className="text-sm text-muted-foreground">Loading…</p>
          )}
        </div>
      </div>

      {summary && (
        <>
          {/* Row 2: tokens by team, full width */}
          <div className="mt-6">
            <div className="mb-2.5 flex items-center justify-between">
              <h2 className="text-sm font-semibold">Tokens by team</h2>
              <span className="text-xs text-muted-foreground">by output</span>
            </div>
            <PerTeam teams={summary.per_team} onPick={onPickTeam} />
          </div>

          {/* Row 3: tokens by provider / model, width-constrained */}
          {summary.per_provider.length > 0 && (
            <div className="mt-6 max-w-xl">
              <div className="mb-2.5 flex items-center justify-between">
                <h2 className="text-sm font-semibold">
                  Tokens by provider / model
                </h2>
                <span className="text-xs text-muted-foreground">by output</span>
              </div>
              <PerProvider providers={summary.per_provider} />
            </div>
          )}
        </>
      )}
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
  const [heatProvider, setHeatProvider] = useState("all");
  const win = DONE_WINDOWS.find((w) => w.value === doneWindow) ?? DONE_WINDOWS[1];

  const summaryQuery = useQuery({
    queryKey: ["spend-summary"],
    queryFn: fetchSpendSummary,
    refetchInterval: 30_000,
  });
  const heatmapQuery = useQuery({
    queryKey: ["spend-heatmap", heatProvider],
    queryFn: () =>
      fetchSpendHeatmap(371, heatProvider === "all" ? undefined : heatProvider),
    refetchInterval: 60_000,
    placeholderData: (prev) => prev,
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
        heatProvider={heatProvider}
        onChangeHeatProvider={setHeatProvider}
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
