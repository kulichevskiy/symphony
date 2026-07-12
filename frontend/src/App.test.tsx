// @vitest-environment jsdom
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { App } from "./App";

function stubFetch() {
  vi.stubGlobal(
    "fetch",
    vi.fn(() =>
      Promise.resolve({
        ok: true,
        status: 200,
        json: () => Promise.resolve({}),
      } as Response),
    ),
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
      expect(screen.getByText("Configuration")).toBeTruthy(),
    );
  });

  it("navigates from home to the lazy /config route via the header link", async () => {
    renderAt("/");
    // HomePage is the initial route; the Config header link is always present.
    fireEvent.click(screen.getByRole("link", { name: "Config" }));
    await waitFor(() =>
      expect(screen.getByText("Configuration")).toBeTruthy(),
    );
  });
});
