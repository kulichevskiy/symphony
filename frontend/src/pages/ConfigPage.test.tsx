import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import type { ConfigView } from "@/lib/api";

import { ConfigDetails } from "./ConfigPage";

const config: ConfigView = {
  read_only: true,
  global_max_concurrent: 7,
  poll_interval_secs: 42,
  bindings: [
    {
      provider: "linear",
      project_key: "SYM",
      github_repo: "org/symphony",
      max_concurrent: 3,
      roles: {
        implement: { agent: "codex", model: null, effort: null },
        review_find: { agent: "claude", model: "opus", effort: "high" },
      },
    },
  ],
};

describe("ConfigDetails", () => {
  it("renders bindings, roles and concurrency caps", () => {
    const html = renderToStaticMarkup(<ConfigDetails config={config} />);
    expect(html).toContain("SYM");
    expect(html).toContain("org/symphony");
    expect(html).toContain("global max concurrent · 7");
    expect(html).toContain("max concurrent · 3");
    expect(html).toContain("implement");
    expect(html).toContain("opus");
    expect(html).toContain("high");
  });

  it("shows an empty state when no bindings are configured", () => {
    const html = renderToStaticMarkup(
      <ConfigDetails config={{ ...config, bindings: [] }} />,
    );
    expect(html).toContain("No bindings configured");
  });
});
