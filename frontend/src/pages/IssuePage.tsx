import { useQuery } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { Link, useParams } from "react-router";

import { StatusCluster, StatusSinceLine } from "@/components/CanonicalStatus";
import { IssueTimeline } from "@/components/IssueTimeline";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { fetchIssueDetail } from "@/lib/api";

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

export function IssuePage() {
  const { id } = useParams();
  const issueId = id ?? "";
  const { data, error, isLoading, isFetching } = useQuery({
    queryKey: ["issue-detail", issueId],
    queryFn: () => fetchIssueDetail(issueId),
    enabled: issueId.length > 0,
    refetchInterval: 5000,
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
