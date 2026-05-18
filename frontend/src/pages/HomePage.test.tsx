import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";

import type { IssueSummary } from "@/lib/api";

import { IssueListRow } from "./HomePage";

const NOW_MS = Date.UTC(2026, 4, 17, 12, 0, 0);

function timestampForAge(ageSecs: number): string {
  return new Date(NOW_MS - ageSecs * 1000).toISOString().replace(".000Z", "Z");
}

function issueWithAge(ageSecs: number | null): IssueSummary {
  return {
    id: `issue-${ageSecs ?? "none"}`,
    identifier: "VIB-16",
    title: "Stale issue",
    team_key: "VIB",
    latest_activity_ts: ageSecs === null ? null : timestampForAge(ageSecs),
    latest_activity_age_secs: ageSecs,
    canonical_status: {
      state: "idle",
      since: null,
      subtitle: null,
      stuck_for: null,
    },
  };
}

function renderRow(issue: IssueSummary): string {
  return renderToStaticMarkup(
    <MemoryRouter>
      <table>
        <tbody>
          <IssueListRow issue={issue} activityNowMs={NOW_MS} />
        </tbody>
      </table>
    </MemoryRouter>,
  );
}

describe("IssueListRow activity freshness", () => {
  it("applies the expected tint for each age bucket", () => {
    const fresh = renderRow(issueWithAge(30 * 60));
    expect(fresh).not.toContain("bg-amber-50/60");
    expect(fresh).not.toContain("bg-orange-50/65");
    expect(fresh).not.toContain("bg-red-50/70");

    expect(renderRow(issueWithAge(2 * 60 * 60))).toContain("bg-amber-50/60");
    expect(renderRow(issueWithAge(12 * 60 * 60))).toContain("bg-orange-50/65");
    expect(renderRow(issueWithAge(25 * 60 * 60))).toContain("bg-red-50/70");
  });

  it("renders absolute activity with a relative title and keeps stuck visible", () => {
    const issue = issueWithAge(25 * 60 * 60);
    issue.canonical_status = {
      state: "pr_open",
      since: issue.latest_activity_ts,
      subtitle: "#44",
      stuck_for: 25 * 60 * 60,
    };

    const markup = renderRow(issue);

    expect(markup).toContain("2026-05-16T11:00:00Z");
    expect(markup).toContain('title="1d ago"');
    expect(markup).toContain("bg-red-50/70");
    expect(markup).toContain("stuck 1d");
  });

  it("renders the no-progress chip next to the PR badge", () => {
    const issue = issueWithAge(5 * 60 * 60);
    issue.canonical_status = {
      state: "pr_open",
      since: "2026-05-17T07:00:00Z",
      subtitle: "#23",
      stuck_for: null,
    };
    issue.warnings = ["no_progress"];

    const markup = renderRow(issue);

    expect(markup).toContain("PR open");
    expect(markup).toContain("no progress 5h");
  });

  it("renders drift-detected status as a warning badge", () => {
    const issue = issueWithAge(90 * 60);
    issue.canonical_status = {
      state: "drift_detected",
      since: "2026-05-17T10:30:00Z",
      subtitle: "1 field(s) disagree",
      stuck_for: 90 * 60,
    };

    const markup = renderRow(issue);

    expect(markup).toContain("drift detected");
    expect(markup).toContain("1 field(s) disagree");
    expect(markup).toContain("stuck 1h");
    expect(markup).toContain("bg-red-100");
  });
});
