import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router";

import { StatusCluster } from "@/components/CanonicalStatus";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { fetchIssues } from "@/lib/api";

function linearIssueUrl(identifier: string): string {
  return `https://linear.app/issue/${encodeURIComponent(identifier)}`;
}

export function HomePage() {
  const issuesQuery = useQuery({
    queryKey: ["issues"],
    queryFn: fetchIssues,
    refetchInterval: 10_000,
    refetchOnWindowFocus: true,
    staleTime: 0,
    placeholderData: (previousData) => previousData,
  });

  const issues = issuesQuery.data ?? [];

  return (
    <main className="min-h-screen bg-background px-4 py-5 text-foreground sm:px-6 lg:px-8">
      <div className="mx-auto flex w-full max-w-6xl flex-col gap-4">
        <header className="flex min-h-10 flex-wrap items-center justify-between gap-3 border-b border-border pb-4">
          <div>
            <h1 className="text-xl font-semibold tracking-normal">Issues</h1>
            <p className="mt-1 text-sm text-muted-foreground">{issues.length} tracked</p>
          </div>
          {issuesQuery.isFetching ? (
            <span className="text-sm text-muted-foreground">Refreshing...</span>
          ) : null}
        </header>

        {issuesQuery.isError ? (
          <Alert className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <div>
              <AlertTitle>Could not load issues</AlertTitle>
              <AlertDescription>
                The daemon did not return the issue list.
              </AlertDescription>
            </div>
            <Button type="button" onClick={() => void issuesQuery.refetch()}>
              Retry
            </Button>
          </Alert>
        ) : null}

        {!issuesQuery.isError && issuesQuery.isPending ? (
          <div className="rounded-md border border-border p-6 text-sm text-muted-foreground">
            Loading issues...
          </div>
        ) : null}

        {!issuesQuery.isError && !issuesQuery.isPending && issues.length === 0 ? (
          <div className="rounded-md border border-border p-6 text-sm text-muted-foreground">
            No issues tracked yet
          </div>
        ) : null}

        {!issuesQuery.isError && issues.length > 0 ? (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-36">Identifier</TableHead>
                <TableHead className="w-64">Status</TableHead>
                <TableHead>Title</TableHead>
                <TableHead className="w-28">Team</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {issues.map((issue) => (
                <TableRow key={issue.id}>
                  <TableCell className="font-medium">
                    <a
                      className="text-primary underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                      href={linearIssueUrl(issue.identifier)}
                      target="_blank"
                      rel="noreferrer"
                    >
                      {issue.identifier}
                    </a>
                  </TableCell>
                  <TableCell>
                    <StatusCluster status={issue.canonical_status} />
                  </TableCell>
                  <TableCell className="max-w-[48rem] whitespace-normal">
                    <Link
                      to={`/issue/${encodeURIComponent(issue.id)}`}
                      className="text-foreground underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                    >
                      {issue.title}
                    </Link>
                  </TableCell>
                  <TableCell className="text-muted-foreground">{issue.team_key}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        ) : null}
      </div>
    </main>
  );
}
