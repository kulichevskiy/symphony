// @vitest-environment jsdom
import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { streamRun, type LiveEvent } from "@/lib/live";

import { LiveFeed } from "./LiveFeed";

vi.mock("@/lib/live", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/live")>();
  return { ...actual, streamRun: vi.fn() };
});

const streamRunMock = vi.mocked(streamRun);

describe("LiveFeed", () => {
  it("keeps rendered output when the same run changes from live to finished", async () => {
    streamRunMock
      .mockImplementationOnce(async (_runId, options) => {
        options.onEvent({ kind: "message", text: "Useful agent update" } as LiveEvent);
        options.onCursor?.(42);
        return await new Promise(() => undefined);
      })
      .mockImplementationOnce(async () => await new Promise(() => undefined));

    const view = render(<LiveFeed runId="run-1" active live />);
    expect(await screen.findByText("Useful agent update")).toBeTruthy();

    view.rerender(<LiveFeed runId="run-1" active live={false} />);
    await waitFor(() => expect(streamRunMock).toHaveBeenCalledTimes(2));

    expect(screen.getByText("Useful agent update")).toBeTruthy();
    expect(streamRunMock.mock.calls[1]?.[1].offset).toBe(42);
  });
});
