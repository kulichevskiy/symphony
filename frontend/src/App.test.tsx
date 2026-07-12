// @vitest-environment jsdom
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { App } from "./App";

/** Contract-valid stub bodies, keyed by endpoint, so pages that render as
 *  soon as their query resolves (no error boundary) don't crash on `{}`. */
function stubBodyFor(url: string): unknown {
  if (url.includes("/api/config")) {
    return {
      read_only: true,
      global_max_concurrent: 0,
      poll_interval_secs: 0,
      bindings: [],
    };
  }
  if (url.includes("/api/issues")) {
    return [];
  }
  if (url.includes("/api/spend/summary")) {
    return {
      totals: {
        issues: 0,
        input_tokens: 0,
        output_tokens: 0,
        cache_write_tokens: 0,
        cache_read_tokens: 0,
      },
      per_team: [],
      per_provider: [],
      per_stage: [],
      teams: [],
      models: [],
    };
  }
  if (url.includes("/api/spend/heatmap")) {
    return { days: [], start: "2024-01-01", end: "2024-01-01" };
  }
  return {};
}

function stubFetch() {
  vi.stubGlobal(
    "fetch",
    vi.fn((input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : input.toString();
      return Promise.resolve({
        ok: true,
        status: 200,
        json: () => Promise.resolve(stubBodyFor(url)),
      } as Response);
    }),
  );
}

function renderAt(path: string) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[path]}>
        <App />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("App lazy routes", () => {
  beforeEach(() => {
    stubFetch();
  });

  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
  });

  it("deep-links straight into the lazy /config route", async () => {
    renderAt("/config");
    await waitFor(() =>
      expect(screen.getByText("No bindings configured")).toBeTruthy(),
    );
  });

  it("navigates from home to the lazy /config route via the header link", async () => {
    renderAt("/");
    // HomePage is the initial route; the Config header link is always present.
    fireEvent.click(screen.getByRole("link", { name: "Config" }));
    await waitFor(() =>
      expect(screen.getByText("No bindings configured")).toBeTruthy(),
    );
  });
});
