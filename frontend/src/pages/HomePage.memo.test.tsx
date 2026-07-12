// @vitest-environment jsdom
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { useState } from "react";
import { MemoryRouter } from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { SpendHeatmap, SpendSummary } from "@/lib/api";
import { DEFAULT_DATE, FiltersProvider } from "@/lib/filters";

import { TokenOverview } from "./HomePage";

// Count the heavy heatmap's renders as a proxy for TokenOverview re-renders:
// TokenOverview renders <Heatmap/> exactly once per render, so a stable count
// across a parent tick proves the memoized card bailed out.
const heat = vi.hoisted(() => ({ renders: 0 }));
vi.mock("@/components/dashboard/Heatmap", () => ({
  Heatmap: () => {
    heat.renders++;
    return null;
  },
}));

const summary: SpendSummary = {
  totals: { input_tokens: 0, output_tokens: 0, cache_write_tokens: 0, cache_read_tokens: 0, issues: 0 },
  per_team: [],
  per_provider: [],
  per_stage: [],
  teams: [],
  models: [],
};
const heatmap: SpendHeatmap = { days: [], start: "2026-06-01", end: "2026-06-01" };
// Day-granular window is stable across nowMs ticks; reuse one reference (as
// HomePage now does via useMemo) so the prop doesn't churn.
const stableWindow = { from: null, to: null };

describe("TokenOverview memoization", () => {
  afterEach(cleanup);

  it("does not re-render on a nowMs-only tick (props referentially unchanged)", () => {
    const client = new QueryClient();
    function Harness() {
      const [n, setN] = useState(0);
      return (
        <QueryClientProvider client={client}>
          <MemoryRouter>
            <FiltersProvider>
              <button onClick={() => setN(n + 1)}>tick {n}</button>
              <TokenOverview
                summary={summary}
                heatmap={heatmap}
                provider="all"
                date={DEFAULT_DATE}
                window={stableWindow}
              />
            </FiltersProvider>
          </MemoryRouter>
        </QueryClientProvider>
      );
    }
    render(<Harness />);
    const before = heat.renders;
    expect(before).toBeGreaterThan(0);

    // A 10s useNowMs tick re-renders the parent but touches none of
    // TokenOverview's props → React.memo bails → no fresh Heatmap render.
    fireEvent.click(screen.getByText(/tick/));
    expect(heat.renders).toBe(before);
  });
});
