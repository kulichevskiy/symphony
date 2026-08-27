// @vitest-environment jsdom
import { cleanup, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  fetchRunEvents,
  streamRun,
  type FeedEvent,
  type LiveEvent,
  type RunEventsPage,
} from "@/lib/live";

import { LiveFeed } from "./LiveFeed";

vi.mock("@/lib/live", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/live")>();
  return { ...actual, streamRun: vi.fn(), fetchRunEvents: vi.fn() };
});

const streamRunMock = vi.mocked(streamRun);
const fetchRunEventsMock = vi.mocked(fetchRunEvents);

function message(text: string, extra: Partial<FeedEvent> = {}): FeedEvent {
  return { kind: "message", text, ts: null, ...extra } as FeedEvent;
}

function page(overrides: Partial<RunEventsPage>): RunEventsPage {
  return { events: [], nextBefore: null, offset: 0, tokens: null, ...overrides };
}

/** ISO string for a local wall-clock time, so timestamp assertions hold in
 *  any TZ the suite runs under. */
function localIso(
  y: number,
  m: number,
  d: number,
  h: number,
  min: number,
  s: number,
): string {
  return new Date(y, m - 1, d, h, min, s).toISOString();
}

function neverResolves(): Promise<never> {
  return new Promise(() => undefined);
}

/** jsdom implements neither layout nor scrolling: `scrollHeight`/`offsetTop`
 *  are always 0 and `scrollTop` is a no-op. Stub all three on the shared
 *  prototype so the anchoring effect has real geometry to react to, and hand
 *  back a restore function so the stub doesn't leak into other tests. Row
 *  height and position are derived from the rendered `EventRow`s (each one a
 *  ".py-1" div within the ".divide-y" list), so the fake geometry tracks
 *  history/tail growth the same way a real layout would. */
function stubScrollGeometry(rowHeight: number): () => void {
  const scrollTops = new WeakMap<Element, number>();
  const heightDescriptor = Object.getOwnPropertyDescriptor(HTMLElement.prototype, "scrollHeight");
  const topDescriptor = Object.getOwnPropertyDescriptor(HTMLElement.prototype, "scrollTop");
  const offsetTopDescriptor = Object.getOwnPropertyDescriptor(HTMLElement.prototype, "offsetTop");

  Object.defineProperty(HTMLElement.prototype, "scrollHeight", {
    configurable: true,
    get(this: HTMLElement) {
      return this.querySelectorAll(".py-1").length * rowHeight;
    },
  });
  Object.defineProperty(HTMLElement.prototype, "scrollTop", {
    configurable: true,
    get(this: HTMLElement) {
      return scrollTops.get(this) ?? 0;
    },
    set(this: HTMLElement, value: number) {
      scrollTops.set(this, value);
    },
  });
  Object.defineProperty(HTMLElement.prototype, "offsetTop", {
    configurable: true,
    get(this: HTMLElement) {
      const list = this.closest(".divide-y");
      if (!list) return 0;
      const rowsEl = Array.from(list.querySelectorAll(".py-1"));
      const index = rowsEl.indexOf(this);
      return index < 0 ? 0 : index * rowHeight;
    },
  });

  return () => {
    // jsdom defines `scrollHeight`/`scrollTop` on `Element.prototype`, not
    // `HTMLElement.prototype`, so there is no own descriptor to restore here
    // — the stub must be deleted instead, or it permanently shadows the real
    // `Element.prototype` accessor for every test that runs after this one.
    if (heightDescriptor) {
      Object.defineProperty(HTMLElement.prototype, "scrollHeight", heightDescriptor);
    } else {
      delete (HTMLElement.prototype as unknown as Record<string, unknown>).scrollHeight;
    }
    if (topDescriptor) {
      Object.defineProperty(HTMLElement.prototype, "scrollTop", topDescriptor);
    } else {
      delete (HTMLElement.prototype as unknown as Record<string, unknown>).scrollTop;
    }
    if (offsetTopDescriptor) Object.defineProperty(HTMLElement.prototype, "offsetTop", offsetTopDescriptor);
  };
}

beforeEach(() => {
  streamRunMock.mockReset();
  fetchRunEventsMock.mockReset();
  streamRunMock.mockImplementation(async () => await neverResolves());
});

afterEach(cleanup);

describe("LiveFeed", () => {
  it("opens on the newest page, newest first, and tails from its offset", async () => {
    fetchRunEventsMock.mockResolvedValue(
      page({
        events: [message("newest", { seq: 2 }), message("middle", { seq: 1 }), message("oldest", { seq: 0 })],
        offset: 512,
      }),
    );

    const view = render(<LiveFeed runId="run-1" active live />);
    await screen.findByText("newest");

    const text = view.container.textContent ?? "";
    expect(text.indexOf("newest")).toBeLessThan(text.indexOf("middle"));
    expect(text.indexOf("middle")).toBeLessThan(text.indexOf("oldest"));
    await waitFor(() => expect(streamRunMock).toHaveBeenCalled());
    expect(streamRunMock.mock.calls[0]?.[1].offset).toBe(512);
  });

  it("appends the next older page below the loaded one and hides the action when exhausted", async () => {
    fetchRunEventsMock
      .mockResolvedValueOnce(page({ events: [message("page1", { seq: 100 })], nextBefore: 100 }))
      .mockResolvedValueOnce(page({ events: [message("page2", { seq: 99 })], nextBefore: null }));

    const view = render(<LiveFeed runId="run-1" active live />);
    const more = await waitFor(() =>
      within(view.container).getByRole("button", { name: "Загрузить ещё" }),
    );

    more.click();
    await waitFor(() =>
      expect(within(view.container).getByText("page2")).toBeTruthy(),
    );

    expect(fetchRunEventsMock.mock.calls[1]?.[1]).toMatchObject({ before: 100 });
    const text = view.container.textContent ?? "";
    expect(text).toContain("page1");
    expect(text.indexOf("page1")).toBeLessThan(text.indexOf("page2"));
    expect(within(view.container).queryByRole("button", { name: "Загрузить ещё" })).toBeNull();
  });

  it("keeps the load-more action visible and retries with the same cursor after a failed page", async () => {
    fetchRunEventsMock
      .mockResolvedValueOnce(page({ events: [message("page1", { seq: 100 })], nextBefore: 100 }))
      .mockRejectedValueOnce(new Error("boom"))
      .mockResolvedValueOnce(page({ events: [message("page2", { seq: 99 })], nextBefore: null }));

    const view = render(<LiveFeed runId="run-1" active live />);
    const more = await waitFor(() =>
      within(view.container).getByRole("button", { name: "Загрузить ещё" }),
    );

    more.click();
    await waitFor(() => expect((more as HTMLButtonElement).disabled).toBe(false));
    expect(within(view.container).queryByText("page2")).toBeNull();

    more.click();
    await waitFor(() =>
      expect(within(view.container).getByText("page2")).toBeTruthy(),
    );

    expect(fetchRunEventsMock.mock.calls[1]?.[1]).toMatchObject({ before: 100 });
    expect(fetchRunEventsMock.mock.calls[2]?.[1]).toMatchObject({ before: 100 });
  });

  it("does not offer the load-more action when the first page is the whole history", async () => {
    fetchRunEventsMock.mockResolvedValue(page({ events: [message("only")], nextBefore: null }));

    const view = render(<LiveFeed runId="run-1" active live />);
    await screen.findByText("only");

    expect(within(view.container).queryByRole("button", { name: "Загрузить ещё" })).toBeNull();
  });

  it("inserts live events above the loaded history", async () => {
    fetchRunEventsMock.mockResolvedValue(page({ events: [message("history line")] }));
    const emitters: Array<(event: LiveEvent) => void> = [];
    streamRunMock.mockImplementation(async (_runId, options) => {
      emitters.push(options.onEvent);
      return await neverResolves();
    });

    const view = render(<LiveFeed runId="run-1" active live />);
    await screen.findByText("history line");
    await waitFor(() => expect(emitters.length).toBe(1));

    emitters[0]?.({ kind: "message", text: "fresh line", ts: null } as LiveEvent);
    await waitFor(() =>
      expect(within(view.container).getByText("fresh line")).toBeTruthy(),
    );

    const text = view.container.textContent ?? "";
    expect(text.indexOf("fresh line")).toBeLessThan(text.indexOf("history line"));
  });

  it("keeps the operator's scroll offset on a live prepend, but not on a load-more append", async () => {
    const restore = stubScrollGeometry(20);
    try {
      fetchRunEventsMock
        .mockResolvedValueOnce(page({ events: [message("page1 line", { seq: 100 })], nextBefore: 100 }))
        .mockResolvedValueOnce(page({ events: [message("page2 line", { seq: 99 })], nextBefore: null }));
      const emitters: Array<(event: LiveEvent) => void> = [];
      streamRunMock.mockImplementation(async (_runId, options) => {
        emitters.push(options.onEvent);
        return await neverResolves();
      });

      const view = render(<LiveFeed runId="run-1" active live />);
      await screen.findByText("page1 line");
      await waitFor(() => expect(emitters.length).toBe(1));

      const scrollEl = view.container.querySelector(".overflow-y-auto") as HTMLElement;
      scrollEl.scrollTop = 40;

      emitters[0]?.({ kind: "message", text: "fresh line", ts: null } as LiveEvent);
      await waitFor(() =>
        expect(within(view.container).getByText("fresh line")).toBeTruthy(),
      );
      expect(scrollEl.scrollTop).toBe(60);

      const more = within(view.container).getByRole("button", { name: "Загрузить ещё" });
      more.click();
      await waitFor(() =>
        expect(within(view.container).getByText("page2 line")).toBeTruthy(),
      );
      expect(scrollEl.scrollTop).toBe(60);
    } finally {
      restore();
    }
  });

  it("renders local HH:mm:ss, the date at a day boundary, and — without a timestamp", async () => {
    fetchRunEventsMock.mockResolvedValue(
      page({
        events: [
          message("today event", { ts: localIso(2026, 8, 27, 14, 3, 7) }),
          message("yesterday event", { ts: localIso(2026, 8, 26, 23, 59, 1) }),
          message("legacy event", { ts: null }),
        ],
      }),
    );

    const view = render(<LiveFeed runId="run-1" active live />);
    await screen.findByText("today event");

    expect(within(view.container).getByText("14:03:07")).toBeTruthy();
    expect(within(view.container).getByText("2026-08-26 23:59:01")).toBeTruthy();
    expect(within(view.container).getByText("—")).toBeTruthy();
  });

  it("retries a failed first page instead of stranding the run empty", async () => {
    fetchRunEventsMock
      .mockRejectedValueOnce(new Error("boom"))
      .mockResolvedValueOnce(page({ events: [message("recovered")] }));

    const view = render(<LiveFeed runId="run-1" active live />);
    const retry = await waitFor(() =>
      within(view.container).getByRole("button", { name: /retry/ }),
    );

    retry.click();
    await waitFor(() =>
      expect(within(view.container).getByText("recovered")).toBeTruthy(),
    );
  });

  it("shows the token total the paged-over history accounts for", async () => {
    fetchRunEventsMock.mockResolvedValue(
      page({
        events: [message("done")],
        tokens: {
          kind: "tokens",
          cumulative: true,
          input_tokens: 1200,
          output_tokens: 340,
          cache_write_tokens: 0,
          cache_read_tokens: 0,
          cost_usd: 0.1,
        },
      }),
    );

    const view = render(<LiveFeed runId="run-1" active live />);
    await screen.findByText("done");

    await waitFor(() =>
      expect(view.container.textContent ?? "").toContain("in 1.2k"),
    );
    expect(view.container.textContent ?? "").toContain("out 340");
  });

  it("keeps rendered output when the same run changes from live to finished", async () => {
    fetchRunEventsMock.mockResolvedValue(page({ events: [] }));
    streamRunMock
      .mockImplementationOnce(async (_runId, options) => {
        options.onEvent({ kind: "message", text: "Useful agent update" } as LiveEvent);
        options.onCursor?.(42);
        return await neverResolves();
      })
      .mockImplementationOnce(async () => await neverResolves());

    const view = render(<LiveFeed runId="run-1" active live />);
    expect(await screen.findByText("Useful agent update")).toBeTruthy();

    view.rerender(<LiveFeed runId="run-1" active live={false} />);
    await waitFor(() => expect(streamRunMock).toHaveBeenCalledTimes(2));

    expect(screen.getByText("Useful agent update")).toBeTruthy();
    expect(fetchRunEventsMock).toHaveBeenCalledTimes(1);
    expect(streamRunMock.mock.calls[1]?.[1].offset).toBe(42);
  });
});
