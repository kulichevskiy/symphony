import { render, screen, waitFor } from "@testing-library/react";
import { beforeEach, expect, test, vi } from "vitest";

import { App } from "./App";

beforeEach(() => {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue({ ok: true, json: async () => [] }),
  );
});

test("loads the event list", async () => {
  render(<App />);

  expect(screen.getByRole("heading", { name: "EventDesk" })).toBeInTheDocument();
  await waitFor(() => expect(fetch).toHaveBeenCalledWith("/events"));
});
