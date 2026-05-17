import { useQuery } from "@tanstack/react-query";
import { useEffect, useRef, useState, type ChangeEvent } from "react";
import { Link, useSearchParams } from "react-router";

import { StatusCluster } from "@/components/CanonicalStatus";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select } from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { fetchIssues } from "@/lib/api";
import type { IssueScope } from "@/lib/api";

const ISSUE_SCOPES: IssueScope[] = ["active", "recent", "all"];
const SCOPE_LABELS: Record<IssueScope, string> = {
  active: "Active",
  recent: "Recent",
  all: "All",
};

function parseIssueScope(value: string | null): IssueScope {
  return ISSUE_SCOPES.includes(value as IssueScope) ? (value as IssueScope) : "active";
}

function linearIssueUrl(identifier: string): string {
  return `https://linear.app/issue/${encodeURIComponent(identifier)}`;
}

export function HomePage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const searchParamsString = searchParams.toString();
  const scopeParam = searchParams.get("scope");
  const scope = parseIssueScope(scopeParam);
  const q = searchParams.get("q")?.trim() ?? "";
  const [searchInput, setSearchInput] = useState(q);
  const lastUrlQ = useRef(q);

  useEffect(() => {
    if (scopeParam === scope) {
      return;
    }
    const next = new URLSearchParams(searchParamsString);
    next.set("scope", scope);
    setSearchParams(next, { replace: true });
  }, [scope, scopeParam, searchParamsString, setSearchParams]);

  useEffect(() => {
    if (lastUrlQ.current === q) {
      return;
    }
    lastUrlQ.current = q;
    setSearchInput(q);
  }, [q]);

  useEffect(() => {
    const timeout = window.setTimeout(() => {
      const next = new URLSearchParams(searchParamsString);
      const normalizedQ = searchInput.trim();
      if (normalizedQ) {
        next.set("q", normalizedQ);
      } else {
        next.delete("q");
      }
      next.set("scope", scope);

      if (next.toString() !== searchParamsString) {
        setSearchParams(next, { replace: true });
      }
    }, 200);

    return () => window.clearTimeout(timeout);
  }, [scope, searchInput, searchParamsString, setSearchParams]);

  const issuesQuery = useQuery({
    queryKey: ["issues", { q, scope }],
    queryFn: () => fetchIssues({ q, scope }),
    refetchInterval: 10_000,
    refetchOnWindowFocus: true,
    staleTime: 0,
    placeholderData: (previousData) => previousData,
  });

  const issues = issuesQuery.data ?? [];
  const emptyMessage =
    q.length > 0
      ? "No matching issues"
      : scope === "all"
        ? "No issues tracked yet"
        : "No issues in this scope";

  function handleScopeChange(event: ChangeEvent<HTMLSelectElement>) {
    const next = new URLSearchParams(searchParamsString);
    next.set("scope", parseIssueScope(event.target.value));
    setSearchParams(next);
  }

  return (
    <main className="min-h-screen bg-background px-4 py-5 text-foreground sm:px-6 lg:px-8">
      <div className="mx-auto flex w-full max-w-6xl flex-col gap-4">
        <header className="flex min-h-10 flex-wrap items-center justify-between gap-3 border-b border-border pb-4">
          <div>
            <h1 className="text-xl font-semibold tracking-normal">Issues</h1>
            <p className="mt-1 text-sm text-muted-foreground">{issues.length} visible</p>
          </div>
          <div className="flex w-full flex-col gap-2 sm:w-auto sm:flex-row sm:items-center">
            <Input
              aria-label="Search issues"
              className="sm:w-72"
              type="search"
              value={searchInput}
              placeholder="Search identifier or title"
              onChange={(event) => setSearchInput(event.target.value)}
            />
            <Select
              aria-label="Issue scope"
              className="sm:w-36"
              value={scope}
              onChange={handleScopeChange}
            >
              {ISSUE_SCOPES.map((option) => (
                <option key={option} value={option}>
                  {SCOPE_LABELS[option]}
                </option>
              ))}
            </Select>
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
            {emptyMessage}
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
