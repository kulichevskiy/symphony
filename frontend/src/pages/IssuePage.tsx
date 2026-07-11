import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useRef, useState, type ReactNode } from "react";
import { Link, useParams } from "react-router";

import {
  type Checks,
  CheckSummary,
  LifecycleBar,
  MixBar,
  PROVIDER_TINT,
  STAGE_LABEL,
  STAGE_TINT,
  STAGES,
  stageRank,
  Tk,
  TOKEN_CATS,
  TokenFigures,
} from "@/components/dashboard/atoms";
import { LiveDot, StatusBadge } from "@/components/dashboard/StatusBadge";
import { IssueTimeline } from "@/components/IssueTimeline";
import { LiveFeed } from "@/components/LiveFeed";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Icon } from "@/components/ui/icon";
import {
  fetchIssueDetail,
  fetchIssueExternal,
  postIssueCommand,
  type IssueDetail,
  type IssueExternalSnapshot,
  type TokenModelUsage,
  type TokenSplit,
} from "@/lib/api";
import {
  effectiveTokens,
  exactInt,
  formatRelative,
  formatTokens,
  formatUtc,
} from "@/lib/format";
import { cn } from "@/lib/utils";

import {
  applicability,
  COMMANDS,
  type CommandId,
  type CommandMeta,
  GROUPS,
  waitLabel,
} from "./issueControls";

type Cockpit = {
  status: string;
  stage: string;
  runState: "running" | "failed" | "waiting" | "completed" | "idle";
  since: string | null;
  activity: string | null;
  reason: string | null;
  tokens: {
    input_tokens: number;
    output_tokens: number;
    cache_write_tokens: number;
    cache_read_tokens: number;
  };
  byModel: TokenModelUsage[];
  pr: {
    number: number;
    repo: string;
    url: string;
    state: string;
    mergeable: string;
    merged: boolean;
    checks: Checks | null;
  } | null;
  waitingOn: string | null;
};

function runStateFor(status: string): Cockpit["runState"] {
  if (status === "running") return "running";
  if (status === "failed" || status === "halted") return "failed";
  if (status === "done") return "completed";
  if (status === "idle") return "idle";
  return "waiting";
}

function deriveCockpit(
  detail: IssueDetail,
  external: IssueExternalSnapshot | undefined,
): Cockpit {
  const status = detail.canonical_status.state;
  const runs = [...detail.runs].sort(
    (a, b) => Date.parse(b.started_at) - Date.parse(a.started_at),
  );
  const latest = runs[0];
  const failed = runs.find((r) => r.status === "failed" && r.termination_detail);
  let reason: string | null = null;
  if (failed) {
    reason = failed.termination_detail;
  } else if (status === "drift_detected" || status === "paused") {
    reason = detail.canonical_status.subtitle;
  }

  const tokens = detail.runs.reduce(
    (a, r) => ({
      input_tokens: a.input_tokens + r.input_tokens,
      output_tokens: a.output_tokens + r.output_tokens,
      cache_write_tokens: a.cache_write_tokens + r.cache_write_tokens,
      cache_read_tokens: a.cache_read_tokens + r.cache_read_tokens,
    }),
    { input_tokens: 0, output_tokens: 0, cache_write_tokens: 0, cache_read_tokens: 0 },
  );

  const gh = external?.github;
  const dbPr = detail.issue_prs[0];
  let pr: Cockpit["pr"] = null;
  if (gh?.pr_number) {
    const cs = gh.check_summary;
    pr = {
      number: gh.pr_number,
      repo: gh.github_repo ?? dbPr?.github_repo ?? "",
      url: gh.url ?? dbPr?.pr_url ?? "",
      state: String(gh.state ?? "open").toLowerCase(),
      mergeable:
        typeof gh.mergeable === "string"
          ? gh.mergeable.toLowerCase()
          : gh.mergeable
            ? "mergeable"
            : "unknown",
      merged: Boolean(gh.merged_at),
      checks: cs
        ? { passing: cs.passing, failing: cs.failing, pending: cs.pending }
        : null,
    };
  } else if (dbPr) {
    pr = {
      number: dbPr.pr_number,
      repo: dbPr.github_repo,
      url: dbPr.pr_url,
      state: dbPr.merged_at ? "merged" : "open",
      mergeable: "unknown",
      merged: Boolean(dbPr.merged_at),
      checks: null,
    };
  }

  return {
    status,
    stage: latest?.stage ?? "—",
    runState: runStateFor(status),
    since: detail.canonical_status.since,
    activity: detail.latest_activity_ts ?? null,
    reason,
    tokens,
    byModel: detail.tokens_by_model ?? [],
    pr,
    waitingOn: detail.operator_waits[0]?.kind ?? null,
  };
}

function CockpitCard({
  title,
  aside,
  children,
  className = "",
}: {
  title: string;
  aside?: ReactNode;
  children: ReactNode;
  className?: string;
}) {
  return (
    <Card className={cn("p-4", className)}>
      <div className="mb-3 flex items-center justify-between gap-2">
        <h3 className="whitespace-nowrap text-xs font-semibold uppercase tracking-wide text-muted-foreground">
          {title}
        </h3>
        {aside}
      </div>
      {children}
    </Card>
  );
}

function NowCard({ c, nowMs }: { c: Cockpit; nowMs: number }) {
  const tone =
    {
      running: "text-blue-600 dark:text-blue-400",
      failed: "text-red-600 dark:text-red-400",
      waiting: "text-amber-600 dark:text-amber-400",
      completed: "text-green-600 dark:text-green-400",
      idle: "text-foreground",
    }[c.runState] ?? "text-foreground";
  return (
    <CockpitCard
      title="What's happening now"
      aside={
        c.status === "running" ? (
          <span className="inline-flex items-center gap-1.5 text-xs font-medium text-blue-600 dark:text-blue-400">
            <LiveDot tone="bg-blue-500" /> live
          </span>
        ) : null
      }
    >
      <div className="flex flex-wrap items-baseline gap-x-2 gap-y-1">
        <span className="text-lg font-semibold capitalize">{c.stage}</span>
        <span className="text-muted-foreground">·</span>
        <span className={cn("text-lg font-semibold capitalize", tone)}>
          {c.runState}
        </span>
      </div>
      <p className="mt-1 text-sm text-muted-foreground">
        since{" "}
        <span className="font-mono" title={formatUtc(c.since)}>
          {formatRelative(c.since, nowMs)}
        </span>
        {" · last activity "}
        <span className="font-mono">{formatRelative(c.activity, nowMs)}</span>
      </p>
      {c.reason ? (
        <div className="mt-3 flex items-start gap-2 rounded-md border border-red-300 bg-red-50 px-3 py-2 text-sm text-red-900 dark:border-red-800 dark:bg-red-950/40 dark:text-red-200">
          <Icon name="alert" size={15} strokeWidth={2} className="mt-0.5 shrink-0" />
          <span className="font-mono text-xs leading-relaxed">{c.reason}</span>
        </div>
      ) : null}
    </CockpitCard>
  );
}

type ProviderGroup = TokenSplit & {
  provider: string;
  models: TokenModelUsage[];
};

function groupByProvider(rows: TokenModelUsage[]): ProviderGroup[] {
  const groups = new Map<string, ProviderGroup>();
  for (const row of rows) {
    const g = groups.get(row.provider) ?? {
      provider: row.provider,
      input_tokens: 0,
      output_tokens: 0,
      cache_write_tokens: 0,
      cache_read_tokens: 0,
      models: [],
    };
    g.input_tokens += row.input_tokens;
    g.output_tokens += row.output_tokens;
    g.cache_write_tokens += row.cache_write_tokens;
    g.cache_read_tokens += row.cache_read_tokens;
    g.models.push(row);
    groups.set(row.provider, g);
  }
  const result = [...groups.values()];
  for (const g of result) {
    g.models.sort((a, b) => b.output_tokens - a.output_tokens);
  }
  result.sort((a, b) => b.output_tokens - a.output_tokens);
  return result;
}

function TokensByModel({ rows }: { rows: TokenModelUsage[] }) {
  if (!rows.length) return null;
  const groups = groupByProvider(rows);
  return (
    <div className="mt-3 border-t border-border pt-3">
      <div className="mb-1.5 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
        by provider / model
      </div>
      <div className="space-y-3">
        {groups.map((g) => (
          <div key={g.provider} className="space-y-1.5">
            <div className="flex items-center gap-1.5 font-mono text-xs">
              <span
                className={cn(
                  "h-2 w-2 shrink-0 rounded-full",
                  PROVIDER_TINT[g.provider] ?? "bg-slate-400",
                )}
              />
              <span className="font-medium text-foreground">{g.provider}</span>
            </div>
            <MixBar split={g} />
            <TokenFigures split={g} />
            {g.models.map((m) => (
              <div key={m.model} className="mt-1 space-y-1 pl-3.5">
                <div className="truncate font-mono text-xs text-muted-foreground">
                  {m.model}
                </div>
                <MixBar split={m} />
                <TokenFigures split={m} />
              </div>
            ))}
          </div>
        ))}
      </div>
    </div>
  );
}

export function TokensCard({ c }: { c: Cockpit }) {
  const eff = effectiveTokens(c.tokens);
  return (
    <CockpitCard
      title="Tokens"
      aside={
        <span
          className="text-xs font-medium text-muted-foreground"
          title={`Effective (weighted) tokens — the per-issue budget unit: ${exactInt(eff)}`}
        >
          eff <span className="font-mono text-foreground">{formatTokens(eff)}</span>
        </span>
      }
    >
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        {TOKEN_CATS.map((s) => (
          <div
            key={s.key}
            className="rounded-md border border-border bg-secondary/20 px-3 py-2"
          >
            <div className="font-mono text-lg font-semibold tracking-tight text-foreground">
              <Tk value={c.tokens[s.key]} />
            </div>
            <div className="mt-0.5 flex items-center gap-1.5 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
              <span className={cn("h-2 w-2 shrink-0 rounded-sm", s.swatch)} />
              {s.label}
            </div>
          </div>
        ))}
      </div>
      <TokensByModel rows={c.byModel} />
    </CockpitCard>
  );
}

/** One lifecycle stage's exact per-run token sums for this issue, plus whether
 *  the issue ever ran it. */
export type StageAggRow = TokenSplit & {
  key: string;
  reached: boolean;
};

/** Aggregate an issue's runs by `stage` into one row per canonical pipeline
 *  stage (in pipeline order), tracking which were reached (≥1 run). Stages the
 *  issue ran that aren't in the canonical list are appended after. `reached`/
 *  `total` describe the canonical seen-stage list only (the "N/M reached" count),
 *  so non-canonical stages never inflate it. Sums are exact per-run totals. */
export function aggregateRunsByStage(runs: IssueDetail["runs"]): {
  rows: StageAggRow[];
  reached: number;
  total: number;
} {
  const sums = new Map<string, TokenSplit>();
  for (const r of runs) {
    const s = sums.get(r.stage) ?? {
      input_tokens: 0,
      output_tokens: 0,
      cache_write_tokens: 0,
      cache_read_tokens: 0,
    };
    s.input_tokens += r.input_tokens;
    s.output_tokens += r.output_tokens;
    s.cache_write_tokens += r.cache_write_tokens;
    s.cache_read_tokens += r.cache_read_tokens;
    sums.set(r.stage, s);
  }
  const extras = [...sums.keys()]
    .filter((k) => !(k in STAGE_TINT))
    .sort((a, b) => stageRank(a) - stageRank(b));
  const keys = [...STAGES.map((s) => s.key), ...extras];
  const rows: StageAggRow[] = keys.map((key) => {
    const s = sums.get(key);
    return {
      key,
      reached: s !== undefined,
      input_tokens: s?.input_tokens ?? 0,
      output_tokens: s?.output_tokens ?? 0,
      cache_write_tokens: s?.cache_write_tokens ?? 0,
      cache_read_tokens: s?.cache_read_tokens ?? 0,
    };
  });
  return {
    rows,
    reached: STAGES.filter((s) => sums.has(s.key)).length,
    total: STAGES.length,
  };
}

const STAGE_NUM_COLS: Array<{ key: keyof TokenSplit; head: string }> = [
  { key: "input_tokens", head: "IN" },
  { key: "output_tokens", head: "OUT" },
  { key: "cache_write_tokens", head: "CACHE-WRITE" },
  { key: "cache_read_tokens", head: "CACHE-READ" },
];

/** "Spend by lifecycle stage" — a stage flow bar + per-stage output-share step
 *  bars + a raw-token table, all derived from this issue's `runs`. Bars and the
 *  share % are output-tokens-everywhere; the table prints the four raw token
 *  categories. Stages the issue never reached are greyed. */
export function StageSpendCard({ runs }: { runs: IssueDetail["runs"] }) {
  const { rows, reached, total } = aggregateRunsByStage(runs);
  const totalOutput = rows.reduce((s, r) => s + r.output_tokens, 0);
  return (
    <CockpitCard
      title="Spend by lifecycle stage"
      aside={
        <span className="font-mono text-[11px] text-muted-foreground">
          {reached}/{total} reached
        </span>
      }
    >
      {/* stage flow bar — each reached stage's share of output tokens */}
      <LifecycleBar rows={rows.filter((r) => r.reached)} className="mb-4" />

      {/* per-stage step bars — output share, greyed where unreached */}
      <div className="space-y-2">
        {rows.map((r) => {
          const pct = totalOutput > 0 ? (r.output_tokens / totalOutput) * 100 : 0;
          return (
            <div
              key={r.key}
              className={cn("flex items-center gap-3", !r.reached && "opacity-40")}
            >
              <span className="flex w-28 shrink-0 items-center gap-1.5 text-xs">
                <span
                  className={cn(
                    "h-2 w-2 shrink-0 rounded-full",
                    r.reached ? STAGE_TINT[r.key] ?? "bg-slate-400" : "bg-slate-400",
                  )}
                />
                <span className="truncate font-medium">
                  {STAGE_LABEL[r.key] ?? r.key}
                </span>
              </span>
              <div className="h-2.5 flex-1 overflow-hidden rounded-full bg-secondary/70">
                <div
                  className={cn("h-full", STAGE_TINT[r.key] ?? "bg-slate-400")}
                  style={{ width: `${pct}%` }}
                />
              </div>
              <span className="w-10 shrink-0 text-right font-mono text-xs tabular-nums text-muted-foreground">
                {Math.round(pct)}%
              </span>
            </div>
          );
        })}
      </div>

      {/* stage table — raw token categories (not output-only) */}
      <div className="mt-4 overflow-x-auto rounded-md border border-border">
        <table className="w-full caption-bottom text-sm">
          <thead>
            <tr className="border-b border-border bg-secondary/40 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
              <th className="px-3 py-1.5 text-left font-medium">Stage</th>
              {STAGE_NUM_COLS.map((c) => (
                <th key={c.key} className="px-3 py-1.5 text-right font-medium">
                  {c.head}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr
                key={r.key}
                className={cn(
                  "border-b border-border/70 last:border-0",
                  !r.reached && "opacity-40",
                )}
              >
                <td className="whitespace-nowrap px-3 py-2">
                  <span className="flex items-center gap-2">
                    <span
                      className={cn(
                        "h-2 w-2 shrink-0 rounded-full",
                        r.reached ? STAGE_TINT[r.key] ?? "bg-slate-400" : "bg-slate-400",
                      )}
                    />
                    <span className="text-sm font-medium">
                      {STAGE_LABEL[r.key] ?? r.key}
                    </span>
                  </span>
                </td>
                {STAGE_NUM_COLS.map((c) => (
                  <td
                    key={c.key}
                    className="whitespace-nowrap px-3 py-2 text-right font-mono text-xs tabular-nums"
                  >
                    <Tk value={r[c.key]} />
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </CockpitCard>
  );
}

const MERGE_TONES: Record<string, string> = {
  mergeable:
    "border-green-300 bg-green-50 text-green-900 dark:border-green-700 dark:bg-green-950/40 dark:text-green-200",
  conflicting:
    "border-red-300 bg-red-50 text-red-900 dark:border-red-700 dark:bg-red-950/40 dark:text-red-200",
  merged:
    "border-violet-300 bg-violet-50 text-violet-900 dark:border-violet-700 dark:bg-violet-950/40 dark:text-violet-200",
  unknown:
    "border-slate-300 bg-slate-50 text-slate-700 dark:border-slate-700 dark:bg-slate-800/60 dark:text-slate-300",
};

export function PrCard({ pr }: { pr: Cockpit["pr"] }) {
  if (!pr) {
    return (
      <CockpitCard title="Pull request">
        <p className="text-sm text-muted-foreground">No PR opened yet</p>
      </CockpitCard>
    );
  }
  const badge = pr.merged ? "merged" : pr.mergeable;
  const tone = MERGE_TONES[badge] ?? MERGE_TONES.unknown;
  return (
    <CockpitCard title="Pull request" aside={<CheckSummary checks={pr.checks} />}>
      <div className="flex flex-wrap items-center gap-2">
        <a
          href={pr.url || `https://github.com/${pr.repo}/pull/${pr.number}`}
          target="_blank"
          rel="noreferrer"
          className="inline-flex items-center gap-1.5 text-sm font-medium text-primary underline-offset-4 hover:underline"
        >
          <Icon name="gitPr" size={15} /> #{pr.number}
          <Icon name="external" size={12} className="text-muted-foreground" />
        </a>
        <Badge className={cn("capitalize", tone)}>{badge}</Badge>
      </div>
      <p className="mt-2 truncate font-mono text-xs text-muted-foreground">{pr.repo}</p>
    </CockpitCard>
  );
}

function WaitCard({ waitingOn }: { waitingOn: string | null }) {
  if (!waitingOn) return null;
  return (
    <Card className="border-amber-300 bg-amber-50 p-4 dark:border-amber-700/70 dark:bg-amber-950/30">
      <div className="flex items-start gap-3">
        <span className="mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-amber-100 text-amber-700 dark:bg-amber-900/60 dark:text-amber-300">
          <Icon name="clock" size={16} strokeWidth={2} />
        </span>
        <div>
          <div className="text-xs font-semibold uppercase tracking-wide text-amber-700 dark:text-amber-400">
            Waiting on operator
          </div>
          <p className="mt-0.5 text-sm font-medium text-amber-950 dark:text-amber-100">
            This issue is waiting for {waitLabel(waitingOn)}.
          </p>
          <p className="mt-0.5 text-xs text-amber-800/80 dark:text-amber-200/70">
            Use the controls below to unblock it.
          </p>
        </div>
      </div>
    </Card>
  );
}

export function CmdButton({
  id,
  enabled,
  why,
  applied,
  busy,
  onClick,
}: {
  id: CommandId;
  enabled: boolean;
  why: string;
  applied: boolean;
  busy: boolean;
  onClick: (id: CommandId) => void;
}) {
  const c = COMMANDS[id];
  let cls = "border border-border bg-background hover:bg-secondary text-foreground";
  if (c.primary) cls = "bg-blue-600 text-white hover:bg-blue-700 border border-blue-600";
  if (c.destructive)
    cls =
      "border border-red-300 bg-background text-red-700 hover:bg-red-50 dark:border-red-800 dark:text-red-300 dark:hover:bg-red-950/40";
  if (applied)
    cls =
      "border border-green-400 bg-green-50 text-green-800 dark:border-green-700 dark:bg-green-950/50 dark:text-green-200";
  return (
    <button
      type="button"
      disabled={!enabled || busy}
      title={enabled ? c.cmd : why}
      onClick={() => onClick(id)}
      className={cn(
        // Full-width, 44px tap target on phones (one-handed reach); inline and
        // compact from the sm breakpoint up.
        "group relative inline-flex h-11 w-full items-center justify-center gap-1.5 rounded-md px-3 text-sm font-medium transition-colors disabled:cursor-not-allowed disabled:opacity-45 sm:h-9 sm:w-auto",
        cls,
      )}
    >
      <Icon
        name={applied ? "check" : c.icon}
        size={15}
        strokeWidth={applied || c.primary ? 2 : 1.5}
      />
      {applied ? "Applied" : c.label}
      {id === "approve" ? <span className="text-xs opacity-70">👍</span> : null}
    </button>
  );
}

export function ConfirmBar({
  c,
  onCancel,
  onConfirm,
}: {
  c: CommandMeta;
  onCancel: () => void;
  onConfirm: () => void;
}) {
  return (
    <div className="flex flex-wrap items-center gap-3 rounded-md border border-red-300 bg-red-50 px-4 py-3 dark:border-red-800 dark:bg-red-950/30">
      <Icon name="alert" size={16} strokeWidth={2} className="text-red-600 dark:text-red-400" />
      <span className="text-sm text-red-900 dark:text-red-200">
        Run <span className="font-mono font-semibold">{c.cmd}</span>? This is
        destructive.
      </span>
      <div className="flex w-full flex-col gap-2 sm:ml-auto sm:w-auto sm:flex-row">
        <Button variant="ghost" onClick={onCancel}>
          Cancel
        </Button>
        <button
          type="button"
          onClick={onConfirm}
          // Full-width, 44px tap target on phones (one-handed reach), matching
          // CmdButton; inline and compact from the sm breakpoint up.
          className="inline-flex h-11 w-full items-center justify-center gap-1.5 rounded-md bg-red-600 px-3 text-sm font-medium text-white transition-colors hover:bg-red-700 sm:h-9 sm:w-auto"
        >
          <Icon name={c.icon} size={14} strokeWidth={2} /> Confirm {c.cmd}
        </button>
      </div>
    </div>
  );
}

function Controls({
  status,
  applied,
  busy,
  onRun,
}: {
  status: string;
  applied: CommandId | null;
  busy: boolean;
  onRun: (id: CommandId) => void;
}) {
  const [confirm, setConfirm] = useState<CommandId | null>(null);
  const { en, why } = applicability(status);

  function handle(id: CommandId) {
    if (COMMANDS[id].destructive) {
      setConfirm(id);
    } else {
      onRun(id);
    }
  }

  if (confirm) {
    return (
      <ConfirmBar
        c={COMMANDS[confirm]}
        onCancel={() => setConfirm(null)}
        onConfirm={() => {
          onRun(confirm);
          setConfirm(null);
        }}
      />
    );
  }

  return (
    <div className="grid gap-3 sm:grid-cols-3">
      {GROUPS.map((g) => (
        <div key={g.key} className="rounded-md border border-border bg-secondary/20 p-3">
          <div className="mb-2 text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
            {g.label}
          </div>
          <div className="flex flex-wrap gap-2">
            {g.cmds.map((id) => (
              <CmdButton
                key={id}
                id={id}
                enabled={en[id]}
                why={why[id]}
                applied={applied === id}
                busy={busy}
                onClick={handle}
              />
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

function ActionLog({ entries }: { entries: Array<{ cmd: string; time: string }> }) {
  if (!entries.length) return null;
  return (
    <div className="mt-3 rounded-md border border-border bg-secondary/20 px-3 py-2">
      <div className="mb-1.5 text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
        Action log
      </div>
      <ul className="space-y-1">
        {entries.map((e, i) => (
          <li key={i} className="flex items-center gap-2 font-mono text-xs">
            <Icon
              name="check"
              size={12}
              strokeWidth={2}
              className="text-green-600 dark:text-green-400"
            />
            <span className="text-foreground">{e.cmd}</span>
            <span className="text-muted-foreground">· applied {e.time}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}

function MiniTable({
  columns,
  rows,
}: {
  columns: string[];
  rows: Array<Record<string, ReactNode>>;
}) {
  if (!rows.length) return <p className="text-sm text-muted-foreground">(none)</p>;
  return (
    <div className="w-full overflow-x-auto rounded-md border border-border">
      <table className="w-full text-left text-xs">
        <thead>
          <tr className="border-b border-border bg-secondary/30">
            {columns.map((c) => (
              <th
                key={c}
                className="px-3 py-1.5 font-medium uppercase tracking-wide text-muted-foreground"
              >
                {c}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i} className="border-b border-border/60 last:border-0">
              {columns.map((c) => (
                <td
                  key={c}
                  className="max-w-[280px] truncate px-3 py-1.5 font-mono text-muted-foreground"
                >
                  {r[c] === null || r[c] === undefined || r[c] === "" ? (
                    <span className="opacity-50">null</span>
                  ) : (
                    r[c]
                  )}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function DebugSection({
  title,
  children,
  defaultOpen,
}: {
  title: string;
  children: ReactNode;
  defaultOpen?: boolean;
}) {
  return (
    <details className="border-t border-border py-3" open={defaultOpen}>
      <summary className="cursor-pointer select-none text-sm font-medium text-muted-foreground hover:text-foreground">
        {title}
      </summary>
      <div className="mt-3">{children}</div>
    </details>
  );
}

function useNowMs(): number {
  const [nowMs, setNowMs] = useState(() => Date.now());
  useEffect(() => {
    const id = window.setInterval(() => setNowMs(Date.now()), 10_000);
    return () => window.clearInterval(id);
  }, []);
  return nowMs;
}

// Stages the LiveFeed does not stream *while running*. These subprocess
// stages now tee `log_root/{run_id}.log` in real time (SYM-177), but wiring
// them into the live pane is a follow-up — so the running placeholder still
// treats them as non-streaming. `review` is a passive monitor (pid=None)
// waiting on the remote `@codex review` bot, with no local subprocess at all.
// The final-log viewer, by contrast, keys purely off `has_log`.
const NON_STREAMING_STAGES = new Set([
  "local_review",
  "local_review_fix",
  "acceptance",
  "verify",
  "review",
]);

type Run = IssueDetail["runs"][number];

// Terminal statuses that mean the run failed or was cut short — the ones an
// operator most wants to inspect (mirrors runs.py TERMINAL_NON_SUCCESS_STATUSES
// plus the legacy "halted").
const FAILED_RUN_STATUSES = new Set(["failed", "interrupted", "needs_approval", "halted"]);

// Mirrors runs.py SUPERSEDED_STATUS: startup reconcile marks a collapsed
// duplicate live run this way — pure bookkeeping that must never shadow the
// surviving run's log.
const SUPERSEDED_STATUS = "superseded";

// `interrupt_running_merge` (and the orphaned-merge-approval cleanup) don't
// use SUPERSEDED_STATUS — they stamp the displaced row `status="interrupted"`
// with `termination_kind="superseded"` instead, since "interrupted" is what
// drives merge/needs_approval re-dispatch. Same bookkeeping-only intent: a
// collapsed duplicate, not something the operator wants surfaced by default.
function isSupersededRun(r: Run): boolean {
  return r.status === SUPERSEDED_STATUS || r.termination_kind === "superseded";
}

function runsByStartDesc(runs: Run[]): Run[] {
  return [...runs].sort((a, b) => Date.parse(b.started_at) - Date.parse(a.started_at));
}

/** The run whose final log opens by default: the most-recent failed/interrupted
 *  run that actually has a per-run log (`has_log`, a stat of
 *  `{log_root}/{run_id}.log` from the API), falling back to the most-recent run
 *  with a log, then to the most recent run overall — a failed `review` run or a
 *  synthetic merge-approval park row (no subprocess, so no log) must not shadow
 *  a run with real output. `superseded` rows are excluded from both fallbacks:
 *  they're a killed duplicate of a surviving run, not something an operator
 *  wants to see by default. Null when the issue has no runs. */
export function pickDefaultRun(runs: Run[]): Run | null {
  if (!runs.length) return null;
  const sorted = runsByStartDesc(runs);
  const eligible = sorted.filter((r) => !isSupersededRun(r));
  return (
    eligible.find((r) => FAILED_RUN_STATUSES.has(r.status) && r.has_log) ??
    eligible.find((r) => r.has_log) ??
    eligible[0] ??
    sorted[0]
  );
}

/** The running run to stream live, and the running run to attribute the
 *  "still running" placeholder to when nothing streams. `_run_prepush_gates`
 *  starts newer running `local_review`/`verify` child rows before the parent
 *  `implement` row is marked completed, so the newest running row by start
 *  time isn't necessarily the tailable one — prefer whichever running run
 *  actually has a per-run log, falling back to the newest running row so the
 *  non-streaming placeholder still has a stage/status to label. Both null
 *  when nothing is running. */
export function pickLiveRun(runs: Run[]): { live: Run | null; active: Run | null } {
  const runningRuns = runsByStartDesc(runs).filter((r) => r.status === "running");
  const live = runningRuns.find((r) => !NON_STREAMING_STAGES.has(r.stage)) ?? null;
  return { live, active: live ?? runningRuns[0] ?? null };
}

function formatDuration(startedAt: string, endedAt: string | null): string {
  if (!endedAt) return "—";
  const secs = Math.round((Date.parse(endedAt) - Date.parse(startedAt)) / 1000);
  if (!Number.isFinite(secs) || secs < 0) return "—";
  if (secs < 60) return `${secs}s`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ${secs % 60}s`;
  return `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m`;
}

const RUN_STATUS_TONE: Record<string, string> = {
  done: "text-green-600 dark:text-green-400",
  completed: "text-green-600 dark:text-green-400",
  failed: "text-red-600 dark:text-red-400",
  interrupted: "text-red-600 dark:text-red-400",
  halted: "text-red-600 dark:text-red-400",
  needs_approval: "text-amber-600 dark:text-amber-400",
  running: "text-blue-600 dark:text-blue-400",
};

function RunPicker({
  runs,
  selectedId,
  onSelect,
}: {
  runs: Run[];
  selectedId: string;
  onSelect: (id: string) => void;
}) {
  return (
    <div className="mb-3 flex flex-col gap-1.5">
      {runs.map((r) => {
        const selected = r.id === selectedId;
        return (
          <button
            key={r.id}
            type="button"
            onClick={() => onSelect(r.id)}
            className={cn(
              "flex w-full flex-wrap items-center gap-x-2 gap-y-0.5 rounded-md border px-3 py-2 text-left text-xs transition-colors",
              selected
                ? "border-blue-400 bg-blue-50 dark:border-blue-700 dark:bg-blue-950/40"
                : "border-border bg-secondary/20 hover:bg-secondary/40",
            )}
          >
            <span className="font-medium capitalize text-foreground">
              {STAGE_LABEL[r.stage] ?? r.stage}
            </span>
            <span className={cn("font-medium", RUN_STATUS_TONE[r.status] ?? "text-muted-foreground")}>
              {r.status}
            </span>
            <span className="ml-auto font-mono text-muted-foreground" title={formatUtc(r.started_at)}>
              {formatUtc(r.started_at)}
            </span>
            <span className="font-mono text-muted-foreground">
              · {formatDuration(r.started_at, r.ended_at)}
            </span>
          </button>
        );
      })}
    </div>
  );
}

function NoTailableLog({ stageLabel, reason }: { stageLabel: string; reason: string }) {
  return (
    <div className="rounded-md border border-border bg-secondary/20 px-3 py-6 text-center text-sm text-muted-foreground">
      <span className="capitalize">{stageLabel}</span> {reason}
    </div>
  );
}

/** Final-log viewer for a non-running issue: a run picker (stage, status,
 *  started, duration) over all the issue's runs, defaulting to the most-recent
 *  failed run, with the selected run's log drained once through the LiveFeed in
 *  non-live mode. Runs with no per-run log (`!has_log`) get an explanatory
 *  empty state instead of a stuck spinner. */
export function FinalLogCard({ runs }: { runs: Run[] }) {
  const sorted = runsByStartDesc(runs);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  if (!sorted.length) return null;

  const fallback = pickDefaultRun(sorted)!;
  const selected = sorted.find((r) => r.id === selectedId) ?? fallback;
  const stageLabel = STAGE_LABEL[selected.stage] ?? selected.stage;

  return (
    <CockpitCard
      title="Final log"
      aside={
        <span className="font-mono text-[11px] text-muted-foreground">finished run</span>
      }
    >
      {sorted.length > 1 ? (
        <RunPicker runs={sorted} selectedId={selected.id} onSelect={setSelectedId} />
      ) : null}
      {!selected.has_log ? (
        <NoTailableLog
          stageLabel={stageLabel}
          reason="recorded no per-run log, so there's nothing to show here."
        />
      ) : (
        <LiveFeed
          key={selected.id}
          runId={selected.id}
          active
          live={false}
          label={`final log — ${stageLabel}, ${selected.status}`}
        />
      )}
    </CockpitCard>
  );
}

export function IssuePage() {
  const { id } = useParams();
  const issueId = id ?? "";
  const nowMs = useNowMs();
  const queryClient = useQueryClient();
  const [applied, setApplied] = useState<CommandId | null>(null);
  const [log, setLog] = useState<Array<{ cmd: string; time: string }>>([]);
  const [error, setError] = useState<string | null>(null);
  const appliedTimer = useRef<number | undefined>(undefined);

  useEffect(() => {
    setApplied(null);
    setLog([]);
    setError(null);
  }, [issueId]);

  const detailQuery = useQuery({
    queryKey: ["issue-detail", issueId],
    queryFn: () => fetchIssueDetail(issueId, { includeExternal: true }),
    enabled: issueId.length > 0,
    refetchInterval: 5000,
  });
  const externalQuery = useQuery({
    queryKey: ["external", issueId],
    queryFn: () => fetchIssueExternal(issueId),
    enabled: issueId.length > 0,
    refetchInterval: 60_000,
  });

  const mutation = useMutation({
    mutationFn: (cmd: CommandId) => postIssueCommand(issueId, COMMANDS[cmd].cmd.slice(1)),
    onSuccess: (_data, cmd) => {
      setError(null);
      setApplied(cmd);
      const time = new Date(nowMs).toISOString().slice(11, 16);
      setLog((l) => [{ cmd: COMMANDS[cmd].cmd, time }, ...l].slice(0, 6));
      window.clearTimeout(appliedTimer.current);
      appliedTimer.current = window.setTimeout(() => setApplied(null), 2200);
      window.setTimeout(() => {
        void queryClient.invalidateQueries({ queryKey: ["issue-detail", issueId] });
        void queryClient.invalidateQueries({ queryKey: ["external", issueId] });
      }, 1500);
    },
    onError: (err: Error) => setError(err.message),
  });

  const detail = detailQuery.data;
  const cockpit = detail ? deriveCockpit(detail, externalQuery.data) : null;
  const { live: liveRun, active: activeRun } = pickLiveRun(detail?.runs ?? []);

  return (
    <main className="mx-auto w-full max-w-[1200px] px-4 py-6 sm:px-6 lg:px-8">
      <div className="mb-5">
        <Link
          to="/"
          className="inline-flex items-center gap-1 text-sm font-medium text-muted-foreground transition-colors hover:text-foreground"
        >
          <Icon name="arrowLeft" size={15} /> Dashboard
        </Link>
        <div className="mt-3 flex flex-wrap items-center gap-3">
          <h1 className="text-2xl font-semibold tracking-tight">
            {detail?.issue.identifier ?? `Issue ${issueId}`}
          </h1>
          {detail ? (
            <StatusBadge status={detail.canonical_status.state} live />
          ) : null}
          {detail ? (
            <a
              href={`https://linear.app/issue/${detail.issue.identifier}`}
              target="_blank"
              rel="noreferrer"
              className="inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground"
            >
              Linear <Icon name="external" size={12} />
            </a>
          ) : null}
        </div>
        {detail ? (
          <>
            <p className="mt-1 max-w-3xl text-sm text-muted-foreground">
              {detail.issue.title}
            </p>
            <p className="mt-0.5 text-sm text-muted-foreground">
              since{" "}
              <span className="font-mono" title={formatUtc(detail.canonical_status.since)}>
                {formatUtc(detail.canonical_status.since)}
              </span>{" "}
              · {detail.issue.team_key}
            </p>
          </>
        ) : null}
      </div>

      {detailQuery.isLoading ? (
        <p className="py-5 text-sm text-muted-foreground">Loading</p>
      ) : null}
      {detailQuery.isError ? (
        <p className="py-5 text-sm text-red-600 dark:text-red-400">
          {(detailQuery.error as Error).message}
        </p>
      ) : null}

      {detail && cockpit ? (
        <>
          <WaitCard waitingOn={cockpit.waitingOn} />

          <div className="mt-4 space-y-4">
            <NowCard c={cockpit} nowMs={nowMs} />
            {liveRun ? (
              <CockpitCard title="Live output">
                <LiveFeed runId={liveRun.id} active />
              </CockpitCard>
            ) : activeRun ? (
              <CockpitCard
                title="Live output"
                aside={
                  <span className="inline-flex items-center gap-1.5 text-xs font-medium text-blue-600 dark:text-blue-400">
                    <LiveDot tone="bg-blue-500" /> running
                  </span>
                }
              >
                <NoTailableLog
                  stageLabel={STAGE_LABEL[activeRun.stage] ?? activeRun.stage}
                  reason="is running now, but this stage doesn't write a per-run log to tail."
                />
              </CockpitCard>
            ) : (
              <FinalLogCard runs={detail.runs} />
            )}
            <div className="grid gap-4 sm:grid-cols-2">
              <TokensCard c={cockpit} />
              <PrCard pr={cockpit.pr} />
            </div>
            <StageSpendCard runs={detail.runs} />
            <CockpitCard
              title="Controls"
              aside={
                <span className="font-mono text-[11px] text-muted-foreground">
                  writes apply instantly
                </span>
              }
            >
              <Controls
                status={cockpit.status}
                applied={applied}
                busy={mutation.isPending}
                onRun={(cmd) => mutation.mutate(cmd)}
              />
              {error ? (
                <p className="mt-2 text-xs text-red-600 dark:text-red-400">{error}</p>
              ) : null}
              <ActionLog entries={log} />
            </CockpitCard>
            <CockpitCard title="Timeline">
              <IssueTimeline issueId={detail.issue.id} />
            </CockpitCard>
          </div>

          <section className="mt-8">
            <details className="rounded-lg border border-border bg-secondary/20 px-4">
              <summary className="flex cursor-pointer select-none items-center gap-2 py-3 text-sm font-semibold">
                <Icon name="chevronRight" size={15} className="chev transition-transform" />
                Advanced / Debug
                <span className="font-normal text-muted-foreground">
                  — raw daemon state
                </span>
              </summary>
              <div className="pb-3">
                <DebugSection title="Runs" defaultOpen>
                  <MiniTable
                    columns={["id", "stage", "status", "started_at", "ended_at", "in", "out", "cache-read"]}
                    rows={detail.runs.map((r) => ({
                      id: r.id,
                      stage: r.stage,
                      status: r.status,
                      started_at: formatUtc(r.started_at),
                      ended_at: r.ended_at ? formatUtc(r.ended_at) : null,
                      in: formatTokens(r.input_tokens),
                      out: formatTokens(r.output_tokens),
                      "cache-read": formatTokens(r.cache_read_tokens),
                    }))}
                  />
                </DebugSection>
                <DebugSection title="PRs">
                  <MiniTable
                    columns={["github_repo", "pr_number", "created_at", "merged_at"]}
                    rows={detail.issue_prs.map((p) => ({
                      github_repo: p.github_repo,
                      pr_number: p.pr_number,
                      created_at: formatUtc(p.created_at),
                      merged_at: p.merged_at ? formatUtc(p.merged_at) : null,
                    }))}
                  />
                </DebugSection>
                <DebugSection title="Operator Waits">
                  <MiniTable
                    columns={["run_id", "kind", "linear_team_key", "github_repo", "created_at"]}
                    rows={detail.operator_waits.map((w) => ({
                      run_id: w.run_id,
                      kind: w.kind,
                      linear_team_key: w.linear_team_key,
                      github_repo: w.github_repo,
                      created_at: formatUtc(w.created_at),
                    }))}
                  />
                </DebugSection>
                <DebugSection title="Review State">
                  <MiniTable
                    columns={["iteration", "pr_number", "ci_fetch_failures", "github_repo"]}
                    rows={
                      detail.review_state
                        ? [
                            {
                              iteration: detail.review_state.iteration,
                              pr_number: detail.review_state.pr_number,
                              ci_fetch_failures: detail.review_state.ci_fetch_failures,
                              github_repo: detail.review_state.github_repo,
                            },
                          ]
                        : []
                    }
                  />
                </DebugSection>
                <DebugSection title="Raw JSON">
                  <pre className="max-h-[420px] overflow-auto rounded-md border border-border bg-background p-3 text-xs leading-relaxed">
                    {JSON.stringify(detail, null, 2)}
                  </pre>
                </DebugSection>
              </div>
            </details>
          </section>
        </>
      ) : null}
    </main>
  );
}
