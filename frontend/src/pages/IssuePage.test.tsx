import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import type { DriftFlag } from "@/lib/api";

import { ExternalTruthSection, GithubCard, RunsSection, RunStatusCell } from "./IssuePage";

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

describe("RunStatusCell", () => {
  it("shows termination detail for non-success runs", () => {
    const markup = renderToStaticMarkup(
      <RunStatusCell
        run={{
          status: "failed",
          termination_kind: "agent_nonzero_exit",
          termination_detail: "[backfill] return code 2",
          exit_returncode: 2,
        }}
      />,
    );

    expect(markup).toContain("failed");
    expect(markup).toContain("agent_nonzero_exit");
    expect(markup).toContain("[backfill] return code 2");
    expect(markup).toContain("exit_returncode=2");
  });

  it("does not show termination badges for success runs", () => {
    const markup = renderToStaticMarkup(
      <RunStatusCell
        run={{
          status: "completed",
          termination_kind: "should_not_render",
          termination_detail: "success detail should stay hidden",
          exit_returncode: 0,
        }}
      />,
    );

    expect(markup).toContain("completed");
    expect(markup).not.toContain("should_not_render");
    expect(markup).not.toContain("success detail should stay hidden");
  });
});

describe("RunsSection", () => {
  it("renders per-run token usage and issue totals next to cost", () => {
    const markup = renderToStaticMarkup(
      <RunsSection
        runs={[
          {
            id: "run-a",
            stage: "implement",
            status: "completed",
            pid: 123,
            started_at: "2026-05-17T10:00:00Z",
            ended_at: "2026-05-17T10:10:00Z",
            cost_usd: 1.25,
            input_tokens: 100,
            output_tokens: 20,
            cache_write_tokens: 30,
            cache_read_tokens: 40,
            termination_kind: "",
            termination_detail: "",
            exit_returncode: null,
          },
          {
            id: "run-b",
            stage: "review",
            status: "running",
            pid: null,
            started_at: "2026-05-17T10:20:00Z",
            ended_at: null,
            cost_usd: 0.5,
            input_tokens: 0,
            output_tokens: 0,
            cache_write_tokens: 0,
            cache_read_tokens: 0,
            termination_kind: "",
            termination_detail: "",
            exit_returncode: null,
          },
        ]}
      />,
    );

    expect(markup).toContain("total cost $1.75");
    expect(markup).toContain("in 100");
    expect(markup).toContain("out 20");
    expect(markup).toContain("cache-write 30");
    expect(markup).toContain("cache-read 40");
    expect(markup).toContain(">in</th>");
    expect(markup).toContain(">out</th>");
    expect(markup).toContain(">cache-write</th>");
    expect(markup).toContain(">cache-read</th>");
    expect(markup).toContain(">0</td>");
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
