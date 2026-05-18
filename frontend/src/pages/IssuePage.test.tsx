import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import type { DriftFlag } from "@/lib/api";

import { GithubCard } from "./IssuePage";

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
