export interface IssueSummary {
  id: string;
  identifier: string;
  title: string;
  team_key: string;
}

export async function fetchIssues(): Promise<IssueSummary[]> {
  const response = await fetch("/api/issues", {
    headers: { Accept: "application/json" },
  });

  if (!response.ok) {
    throw new Error(`GET /api/issues failed with ${response.status}`);
  }

  return (await response.json()) as IssueSummary[];
}
