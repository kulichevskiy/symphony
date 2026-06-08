import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";

import { FiltersProvider } from "@/lib/filters";

import { FilterBar } from "./FilterBar";

function render(initialEntry: string): string {
  return renderToStaticMarkup(
    <MemoryRouter initialEntries={[initialEntry]}>
      <FiltersProvider>
        <FilterBar />
      </FiltersProvider>
    </MemoryRouter>,
  );
}

describe("FilterBar", () => {
  it("renders the provider segmented control with all three options", () => {
    const markup = render("/");
    expect(markup).toContain('aria-label="Model provider"');
    for (const label of [">All<", ">codex<", ">claude<"]) {
      expect(markup).toContain(label);
    }
  });

  it("marks the URL-selected provider active", () => {
    const markup = render("/?provider=codex");
    // The active segment carries the raised background; the others are muted.
    const codexButton = markup.slice(markup.indexOf(">codex<") - 200, markup.indexOf(">codex<"));
    expect(codexButton).toContain("bg-background");
  });
});
