import { useQuery } from "@tanstack/react-query";
import { useRef, useState, type ReactNode } from "react";
import { Link, useParams } from "react-router";

import { StatusCluster, StatusSinceLine } from "@/components/CanonicalStatus";
import { IssueTimeline } from "@/components/IssueTimeline";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";
import { fetchIssueDetail, fetchIssueExternal, fetchIssueObservations } from "@/lib/api";
import type {
  DriftFlag,
  ExternalObservation,
  ExternalComment,
  GithubPrSnapshot,
  IssueExternalSnapshot,
  LinearSnapshot,
} from "@/lib/api";

type CellValue = string | number | null;

type Column<T extends object> = {
  key: Extract<keyof T, string>;
  label: string;
  render?: (row: T) => ReactNode;
};

function formatCell(value: CellValue) {
  if (value === null || value === "") {
    return <span className="text-muted-foreground">null</span>;
  }
  return String(value);
}

function SectionTable<T extends object>({
  title,
  rows,
  columns,
}: {
  title: string;
  rows: T[];
  columns: Column<T>[];
}) {
  return (
    <section className="border-t py-5">
      <h2 className="mb-3 text-base font-semibold tracking-normal">{title}</h2>
      {rows.length === 0 ? (
        <p className="text-sm text-muted-foreground">(none)</p>
      ) : (
        <Table>
          <TableHeader>
            <TableRow>
              {columns.map((column) => (
                <TableHead key={column.key}>{column.label}</TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((row, index) => (
              <TableRow key={Object.values(row).join(":") || index}>
                {columns.map((column) => (
                  <TableCell key={column.key} className="max-w-[360px] break-words font-mono text-xs">
                    {column.render ? column.render(row) : formatCell(row[column.key] as CellValue)}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
    </section>
  );
}

function formatUtc(ts?: string | null) {
  if (!ts) {
    return "null";
  }
  const date = new Date(ts);
  if (Number.isNaN(date.getTime())) {
    return ts;
  }
  return date.toISOString().replace(".000Z", "Z");
}

function formatRelative(ts?: string | null) {
  if (!ts) {
    return "unknown";
  }
  const date = new Date(ts);
  if (Number.isNaN(date.getTime())) {
    return ts;
  }
  const diffSeconds = Math.round((Date.now() - date.getTime()) / 1000);
  const absSeconds = Math.abs(diffSeconds);
  const units: Array<[number, string]> = [
    [60 * 60 * 24, "d"],
    [60 * 60, "h"],
    [60, "m"],
  ];
  let value = absSeconds;
  let unit = "s";
  for (const [seconds, label] of units) {
    if (absSeconds >= seconds) {
      value = Math.floor(absSeconds / seconds);
      unit = label;
      break;
    }
  }
  if (value < 10 && unit === "s") {
    return "now";
  }
  return diffSeconds < 0 ? `in ${value}${unit}` : `${value}${unit} ago`;
}

function flagsByField(flags: DriftFlag[]) {
  return new Map(flags.map((flag) => [flag.field, flag]));
}

function fieldTitle(flag?: DriftFlag) {
  if (!flag) {
    return undefined;
  }
  return `SQLite: ${flag.sqlite_value ?? "null"}; ${flag.source_name}: ${
    flag.source_value ?? "null"
  }`;
}

function FieldRow({
  label,
  value,
  flag,
}: {
  label: string;
  value: ReactNode;
  flag?: DriftFlag;
}) {
  const isWarning = flag?.severity === "warning";
  return (
    <div
      className={cn(
        "grid min-h-9 grid-cols-[9rem_minmax(0,1fr)] items-center gap-3 border-t px-3 py-2 text-sm first:border-t-0",
        flag && !isWarning ? "bg-red-50 text-red-950" : null,
        isWarning ? "bg-amber-50 text-amber-950" : null,
      )}
      title={fieldTitle(flag)}
    >
      <span className="text-muted-foreground">{label}</span>
      <span className="min-w-0 break-words font-mono text-xs">
        {flag ? <span className="mr-2 font-sans text-sm">⚠</span> : null}
        {value}
      </span>
    </div>
  );
}

function SourceAlert({
  source,
  snapshot,
}: {
  source: "Linear" | "GitHub";
  snapshot: LinearSnapshot | GithubPrSnapshot;
}) {
  if (!snapshot.error) {
    return null;
  }
  const stale = snapshot.stale_fetched_at
    ? ` — showing data from ${formatRelative(snapshot.stale_fetched_at)}`
    : "";
  return (
    <div className="border-b border-red-200 bg-red-50 px-3 py-2 text-sm text-red-900">
      {source} returned {snapshot.error}
      {stale}
    </div>
  );
}

function LinearCard({
  snapshot,
  flags,
}: {
  snapshot: LinearSnapshot;
  flags: Map<string, DriftFlag>;
}) {
  return (
    <section className="overflow-hidden rounded-md border border-border">
      <div className="border-b bg-secondary px-3 py-2 text-sm font-semibold">Linear</div>
      <SourceAlert source="Linear" snapshot={snapshot} />
      <FieldRow label="state" value={snapshot.state ?? "null"} flag={flags.get("linear.state")} />
      <FieldRow label="updatedAt" value={formatUtc(snapshot.updated_at)} />
      <div className="grid min-h-9 grid-cols-[9rem_minmax(0,1fr)] items-center gap-3 border-t px-3 py-2 text-sm">
        <span className="text-muted-foreground">labels</span>
        <div className="flex min-w-0 flex-wrap gap-1">
          {(snapshot.labels ?? []).length > 0 ? (
            (snapshot.labels ?? []).map((label) => (
              <Badge key={label} className="border-gray-300 bg-gray-50 text-gray-700">
                {label}
              </Badge>
            ))
          ) : (
            <span className="font-mono text-xs text-muted-foreground">none</span>
          )}
        </div>
      </div>
    </section>
  );
}

function GithubCard({
  snapshot,
  flags,
}: {
  snapshot: GithubPrSnapshot;
  flags: Map<string, DriftFlag>;
}) {
  const checks = snapshot.check_summary;
  const checksText = checks
    ? `${checks.passing} passing / ${checks.failing} failing / ${checks.pending} pending`
    : "null";
  return (
    <section className="overflow-hidden rounded-md border border-border">
      <div className="border-b bg-secondary px-3 py-2 text-sm font-semibold">
        GitHub PR {snapshot.pr_number ? `#${snapshot.pr_number}` : ""}
      </div>
      <SourceAlert source="GitHub" snapshot={snapshot} />
      <FieldRow label="state" value={snapshot.state ?? "null"} flag={flags.get("github.state")} />
      <FieldRow
        label="mergedAt"
        value={formatUtc(snapshot.merged_at)}
        flag={flags.get("github.merged_at")}
      />
      <FieldRow label="mergedBy" value={snapshot.merged_by ?? "null"} />
      <FieldRow label="mergeable" value={String(snapshot.mergeable ?? "null")} />
      <FieldRow label="mergeState" value={snapshot.merge_state_status ?? "null"} />
      <FieldRow label="checks" value={checksText} flag={flags.get("github.checks")} />
    </section>
  );
}

function CommentList({
  title,
  comments,
}: {
  title: string;
  comments: ExternalComment[];
}) {
  const [expanded, setExpanded] = useState<Set<string | number>>(() => new Set());
  return (
    <section>
      <h3 className="mb-2 text-sm font-semibold tracking-normal">{title}</h3>
      {comments.length === 0 ? (
        <p className="text-sm text-muted-foreground">(none)</p>
      ) : (
        <ul className="space-y-2">
          {comments.map((comment) => {
            const isExpanded = expanded.has(comment.comment_id);
            const needsTrim = comment.body.length > 120;
            const body = needsTrim && !isExpanded ? `${comment.body.slice(0, 120)} […]` : comment.body;
            return (
              <li key={comment.comment_id} className="rounded-md border border-border p-3">
                <div className="mb-1 flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
                  <time className="italic" dateTime={comment.ts} title={formatUtc(comment.ts)}>
                    {formatRelative(comment.ts)}
                  </time>
                  <span className="font-mono">{comment.author || "unknown"}</span>
                  {comment.url ? (
                    <a
                      className="text-primary underline-offset-4 hover:underline"
                      href={comment.url}
                      target="_blank"
                      rel="noreferrer"
                    >
                      anchor
                    </a>
                  ) : null}
                </div>
                <p className="whitespace-pre-wrap break-words text-sm">{body}</p>
                {needsTrim ? (
                  <button
                    type="button"
                    className="mt-1 text-xs font-medium text-primary underline-offset-4 hover:underline"
                    onClick={() =>
                      setExpanded((current) => {
                        const next = new Set(current);
                        if (next.has(comment.comment_id)) {
                          next.delete(comment.comment_id);
                        } else {
                          next.add(comment.comment_id);
                        }
                        return next;
                      })
                    }
                  >
                    {isExpanded ? "collapse" : "expand"}
                  </button>
                ) : null}
              </li>
            );
          })}
        </ul>
      )}
    </section>
  );
}

function ExternalTruthSection({
  snapshot,
  isFetching,
  onRefresh,
}: {
  snapshot?: IssueExternalSnapshot;
  isFetching: boolean;
  onRefresh: () => void;
}) {
  const flags = flagsByField(snapshot?.drift_flags ?? []);
  const driftCount = snapshot?.drift_flags.filter((flag) => flag.severity !== "warning").length ?? 0;
  return (
    <section className="border-t py-5">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
        <div className="flex min-w-0 flex-wrap items-center gap-2">
          <h2 className="text-base font-semibold tracking-normal">
            External truth
            {snapshot ? (
              <span className="ml-2 font-normal text-muted-foreground">
                fetched {formatRelative(snapshot.fetched_at)}
              </span>
            ) : null}
          </h2>
          {snapshot ? (
            <Badge
              className={
                driftCount > 0
                  ? "border-red-300 bg-red-50 text-red-900"
                  : "border-green-300 bg-green-50 text-green-900"
              }
            >
              {driftCount > 0 ? `Drift detected ⚠ (${driftCount})` : "In sync ✓"}
            </Badge>
          ) : null}
        </div>
        <Button type="button" variant="secondary" disabled={isFetching} onClick={onRefresh}>
          Refresh now
        </Button>
      </div>
      {snapshot ? (
        <div className="space-y-5">
          <div className="grid gap-4 md:grid-cols-2">
            <LinearCard snapshot={snapshot.linear} flags={flags} />
            <GithubCard snapshot={snapshot.github} flags={flags} />
          </div>
          <div className="grid gap-4 md:grid-cols-2">
            <CommentList title="Recent Linear comments" comments={snapshot.linear.comments ?? []} />
            <CommentList title="Recent PR review comments" comments={snapshot.github.comments ?? []} />
          </div>
        </div>
      ) : (
        <p className="text-sm text-muted-foreground">Loading external state</p>
      )}
    </section>
  );
}

function prettyPayload(raw: string): string {
  try {
    return JSON.stringify(JSON.parse(raw), null, 2);
  } catch {
    return raw;
  }
}

function ObservationsPanel({
  rows,
  isLoading,
  error,
}: {
  rows: ExternalObservation[];
  isLoading: boolean;
  error: unknown;
}) {
  return (
    <section className="border-t py-5">
      <h2 className="mb-3 text-base font-semibold tracking-normal">Recent observations</h2>
      {isLoading ? <p className="text-sm text-muted-foreground">Loading</p> : null}
      {error ? (
        <p className="text-sm text-red-600">{(error as Error).message}</p>
      ) : null}
      {!isLoading && !error && rows.length === 0 ? (
        <p className="text-sm text-muted-foreground">(none)</p>
      ) : null}
      {rows.length > 0 ? (
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-52">observed_at</TableHead>
              <TableHead className="w-24">source</TableHead>
              <TableHead className="w-44">drift_kind</TableHead>
              <TableHead className="w-36">action_taken</TableHead>
              <TableHead>payload</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((row) => (
              <TableRow key={row.id}>
                <TableCell className="font-mono text-xs">{row.observed_at}</TableCell>
                <TableCell className="font-mono text-xs">{row.source}</TableCell>
                <TableCell className="font-mono text-xs">
                  {row.drift_kind ? (
                    <span className="text-red-700">{row.drift_kind}</span>
                  ) : (
                    <span className="text-muted-foreground">null</span>
                  )}
                </TableCell>
                <TableCell className="font-mono text-xs">{row.action_taken}</TableCell>
                <TableCell className="max-w-[520px] align-top font-mono text-xs">
                  <details>
                    <summary className="cursor-pointer text-primary underline-offset-4 hover:underline">
                      payload
                    </summary>
                    <pre className="mt-2 max-h-60 overflow-auto whitespace-pre-wrap rounded-md bg-secondary p-3 leading-relaxed">
                      {prettyPayload(row.payload_json)}
                    </pre>
                  </details>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      ) : null}
    </section>
  );
}

export function IssuePage() {
  const { id } = useParams();
  const issueId = id ?? "";
  const forceExternalRefresh = useRef(false);
  const { data, error, isLoading, isFetching } = useQuery({
    queryKey: ["issue-detail", issueId],
    queryFn: () => fetchIssueDetail(issueId),
    enabled: issueId.length > 0,
    refetchInterval: 5000,
    refetchOnWindowFocus: true,
    staleTime: 0,
  });
  const externalQuery = useQuery({
    queryKey: ["external", issueId],
    queryFn: () => {
      const refresh = forceExternalRefresh.current;
      forceExternalRefresh.current = false;
      return fetchIssueExternal(issueId, { refresh });
    },
    enabled: issueId.length > 0,
    refetchInterval: 60_000,
    refetchOnWindowFocus: true,
    staleTime: 60_000,
  });
  const observationsQuery = useQuery({
    queryKey: ["issue-observations", issueId],
    queryFn: () => fetchIssueObservations(issueId),
    enabled: issueId.length > 0,
    refetchInterval: 10_000,
    refetchOnWindowFocus: true,
    staleTime: 0,
  });

  return (
    <main className="min-h-screen bg-background text-foreground">
      <header className="border-b px-6 py-4">
        <div className="mx-auto flex w-full max-w-6xl items-center justify-between gap-4">
          <div className="min-w-0">
            <Link to="/" className="text-sm font-medium text-muted-foreground hover:text-foreground">
              Back
            </Link>
            <div className="mt-2 flex flex-wrap items-center gap-3">
              <h1 className="text-2xl font-semibold tracking-normal">
                {data?.issue.identifier ?? `Issue ${issueId}`}
              </h1>
              {data ? <StatusCluster status={data.canonical_status} /> : null}
            </div>
            {data ? (
              <>
                <p className="mt-1 text-sm text-muted-foreground">{data.issue.title}</p>
                <StatusSinceLine status={data.canonical_status} />
              </>
            ) : null}
          </div>
          <div className="text-sm text-muted-foreground">{isFetching ? "Refreshing" : "Live"}</div>
        </div>
      </header>

      <div className="mx-auto w-full max-w-6xl px-6 py-2">
        {isLoading ? <p className="py-5 text-sm text-muted-foreground">Loading</p> : null}
        {error ? (
          <p className="py-5 text-sm text-red-600">{(error as Error).message}</p>
        ) : null}
        {data ? (
          <>
            <ExternalTruthSection
              snapshot={externalQuery.data}
              isFetching={externalQuery.isFetching}
              onRefresh={() => {
                forceExternalRefresh.current = true;
                void externalQuery.refetch();
              }}
            />
            {externalQuery.error ? (
              <p className="border-t py-3 text-sm text-red-600">
                {(externalQuery.error as Error).message}
              </p>
            ) : null}
            <ObservationsPanel
              rows={observationsQuery.data ?? []}
              isLoading={observationsQuery.isLoading}
              error={observationsQuery.error}
            />
            <SectionTable
              title="Issue"
              rows={[data.issue]}
              columns={[
                { key: "id", label: "id" },
                { key: "identifier", label: "identifier" },
                { key: "title", label: "title" },
                { key: "team_key", label: "team_key" },
              ]}
            />
            <IssueTimeline issueId={data.issue.id} />
            <SectionTable
              title="Runs"
              rows={data.runs}
              columns={[
                { key: "id", label: "id" },
                { key: "stage", label: "stage" },
                { key: "status", label: "status" },
                { key: "pid", label: "pid" },
                { key: "started_at", label: "started_at" },
                { key: "ended_at", label: "ended_at" },
                { key: "cost_usd", label: "cost_usd" },
              ]}
            />
            <SectionTable
              title="PRs"
              rows={data.issue_prs}
              columns={[
                { key: "github_repo", label: "github_repo" },
                { key: "binding_key", label: "binding_key" },
                {
                  key: "pr_number",
                  label: "pr_number",
                  render: (row) => (
                    <a
                      className="font-medium text-primary underline-offset-4 hover:underline"
                      href={row.pr_url}
                      target="_blank"
                      rel="noreferrer"
                    >
                      {row.pr_number}
                    </a>
                  ),
                },
                { key: "pr_url", label: "pr_url" },
                { key: "created_at", label: "created_at" },
                { key: "merged_at", label: "merged_at" },
              ]}
            />
            <SectionTable
              title="Operator Waits"
              rows={data.operator_waits}
              columns={[
                { key: "run_id", label: "run_id" },
                { key: "kind", label: "kind" },
                { key: "linear_team_key", label: "linear_team_key" },
                { key: "github_repo", label: "github_repo" },
                { key: "issue_label", label: "issue_label" },
                { key: "created_at", label: "created_at" },
              ]}
            />
            <SectionTable
              title="Review State"
              rows={data.review_state ? [data.review_state] : []}
              columns={[
                { key: "iteration", label: "iteration" },
                { key: "last_trigger_signature", label: "last_trigger_signature" },
                { key: "ci_fetch_failures", label: "ci_fetch_failures" },
                { key: "pr_number", label: "pr_number" },
                { key: "pr_url", label: "pr_url" },
                { key: "github_repo", label: "github_repo" },
                { key: "issue_label", label: "issue_label" },
                { key: "codex_lgtm_comment_id", label: "codex_lgtm_comment_id" },
              ]}
            />
            <SectionTable
              title="Comment Events"
              rows={data.comment_events}
              columns={[
                { key: "comment_id", label: "comment_id" },
                { key: "seen_at", label: "seen_at" },
              ]}
            />
            <SectionTable
              title="Activity Comment Marks"
              rows={data.activity_comment_marks}
              columns={[
                { key: "run_id", label: "run_id" },
                { key: "first_unpublished_at", label: "first_unpublished_at" },
                { key: "last_event_at", label: "last_event_at" },
                { key: "event_count_since_post", label: "event_count_since_post" },
                { key: "last_posted_at", label: "last_posted_at" },
                { key: "last_fingerprint", label: "last_fingerprint" },
              ]}
            />
            <SectionTable
              title="Issue Cost Marks"
              rows={data.issue_cost_marks ? [data.issue_cost_marks] : []}
              columns={[{ key: "warning_posted_at", label: "warning_posted_at" }]}
            />
            <details className="border-t py-5">
              <summary className="cursor-pointer text-base font-semibold tracking-normal">Raw JSON</summary>
              <pre className="mt-3 max-h-[520px] overflow-auto rounded-md bg-secondary p-4 text-xs leading-relaxed">
                {JSON.stringify(data, null, 2)}
              </pre>
            </details>
          </>
        ) : null}
      </div>
    </main>
  );
}
