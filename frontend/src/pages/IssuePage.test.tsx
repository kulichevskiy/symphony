import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import type { DriftFlag } from "@/lib/api";

import { ExternalTruthSection, GithubCard } from "./IssuePage";

describe("GithubCard", () => {
  it("surfaces partial review-comment fetch failures", () => {
    const markup = renderToStaticMarkup(
      <GithubCard
        snapshot={{
          pr_number: 42,
          state: "OPEN",
          comments: [],
          comments_error: "missing scope",
        }}
        flags={new Map<string, DriftFlag>()}
      />,
    );

    expect(markup).toContain("GitHub review comments unavailable");
    expect(markup).toContain("missing scope");
    expect(markup).toContain("border-amber-200");
  });
});

describe("ExternalTruthSection", () => {
  it("does not report in sync when an external source failed", () => {
    const markup = renderToStaticMarkup(
      <ExternalTruthSection
        snapshot={{
          fetched_at: "2026-05-17T12:00:00Z",
          linear: { error: "Linear returned 500" },
          github: { state: "OPEN", comments: [] },
          drift_flags: [],
        }}
        isFetching={false}
        onRefresh={() => undefined}
      />,
    );

    expect(markup).toContain("Source unavailable");
    expect(markup).toContain("border-amber-300");
    expect(markup).not.toContain("In sync");
  });
});
