import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";

import { FiltersProvider } from "@/lib/filters";

import { FilterBar } from "./FilterBar";

function render(initialEntry: string): string {
  const queryClient = new QueryClient();
  return renderToStaticMarkup(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={[initialEntry]}>
        <FiltersProvider>
          <FilterBar />
        </FiltersProvider>
      </MemoryRouter>
    </QueryClientProvider>,
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
