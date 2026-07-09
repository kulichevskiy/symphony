import { afterEach, describe, expect, it, vi } from "vitest";

import { registerTokenProvider } from "./auth";
import { foldTokenTick, streamRun, type LiveEvent, type TokenTick } from "./live";

afterEach(() => {
  registerTokenProvider(null);
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

function ndjsonResponse(lines: string[]): Response {
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      const encoder = new TextEncoder();
      for (const line of lines) {
        controller.enqueue(encoder.encode(`${line}\n`));
      }
      controller.close();
    },
  });
  return new Response(body, {
    status: 200,
    headers: { "Content-Type": "application/x-ndjson" },
  });
}

describe("streamRun", () => {
  it("parses NDJSON events and forwards non-control frames", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        ndjsonResponse([
          JSON.stringify({ kind: "message", text: "hi" }),
          JSON.stringify({ kind: "tool_call", tool: "Bash", detail: "ls" }),
          JSON.stringify({ kind: "cursor", offset: 42 }),
          JSON.stringify({ kind: "end" }),
        ]),
      ),
    );

    const events: LiveEvent[] = [];
    const result = await streamRun("run-1", { onEvent: (e) => events.push(e) });

    expect(events).toEqual([
      { kind: "message", text: "hi" },
      { kind: "tool_call", tool: "Bash", detail: "ls" },
    ]);
    expect(result).toEqual({ offset: 42, ended: true });
  });

  it("sends the bearer token and resume offset (no token in URL)", async () => {
    registerTokenProvider({
      getAccessTokenSilently: async () => "opaque",
      getIdTokenClaims: async () => ({ __raw: "id-jwt" }) as never,
      loginWithRedirect: async () => {},
    });
    const fetchMock = vi.fn(async () => ndjsonResponse([JSON.stringify({ kind: "end" })]));
    vi.stubGlobal("fetch", fetchMock);

    await streamRun("run-9", { offset: 7, onEvent: () => {} });

    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe("/api/runs/run-9/stream?offset=7");
    expect(url).not.toContain("id-jwt");
    expect((init.headers as Record<string, string>).Authorization).toBe("Bearer id-jwt");
  });

  it("returns the last cursor and ended=false when the connection drops", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        ndjsonResponse([
          JSON.stringify({ kind: "message", text: "partial" }),
          JSON.stringify({ kind: "cursor", offset: 10 }),
        ]),
      ),
    );

    const result = await streamRun("run-2", { onEvent: () => {} });
    expect(result).toEqual({ offset: 10, ended: false });
  });

  it("reports the last cursor instead of throwing when the read rejects (real drop)", async () => {
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        const encoder = new TextEncoder();
        controller.enqueue(encoder.encode(`${JSON.stringify({ kind: "cursor", offset: 10 })}\n`));
        // Deferred so the enqueued chunk is actually read before the stream
        // errors — an immediate error() discards unread queued chunks too.
        setTimeout(() => controller.error(new TypeError("network error")), 0);
      },
    });
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(body, {
            status: 200,
            headers: { "Content-Type": "application/x-ndjson" },
          }),
      ),
    );

    const cursors: number[] = [];
    const result = await streamRun("run-3", {
      onEvent: () => {},
      onCursor: (offset) => cursors.push(offset),
    });

    expect(cursors).toEqual([10]);
    expect(result).toEqual({ offset: 10, ended: false });
  });
});

function tick(overrides: Partial<TokenTick>): TokenTick {
  return {
    kind: "tokens",
    cumulative: false,
    input_tokens: 0,
    output_tokens: 0,
    cache_write_tokens: 0,
    cache_read_tokens: 0,
    cost_usd: 0,
    ...overrides,
  };
}

describe("foldTokenTick", () => {
  it("sums per-turn delta ticks into a running total", () => {
    let total = foldTokenTick(null, tick({ input_tokens: 5, output_tokens: 10 }));
    total = foldTokenTick(total, tick({ input_tokens: 7, output_tokens: 15 }));

    expect(total).toMatchObject({ input_tokens: 12, output_tokens: 25 });
  });

  it("replaces (not adds) on a repeated cumulative tick, matching the true total", () => {
    // Mirrors codex `token_count`, which re-emits the whole-run running
    // total on every tick (see the backend's own
    // test_codex_token_count_is_cumulative_within_process): out=10 then
    // out=40, true total 40 — a naive sum would wrongly report 50.
    let total = foldTokenTick(null, tick({ cumulative: true, output_tokens: 10 }));
    total = foldTokenTick(total, tick({ cumulative: true, output_tokens: 40 }));

    expect(total?.output_tokens).toBe(40);
  });

  it("keeps input and output on the same accounting basis", () => {
    // Per-turn deltas accumulate live...
    let total = foldTokenTick(null, tick({ input_tokens: 10, output_tokens: 2 }));
    total = foldTokenTick(total, tick({ input_tokens: 10, output_tokens: 3 }));
    expect(total).toMatchObject({ input_tokens: 20, output_tokens: 5 });

    // ...then the terminal cumulative tick (claude `result`) replaces both
    // fields together rather than mixing "latest" input with "summed" output.
    total = foldTokenTick(total, tick({ cumulative: true, input_tokens: 22, output_tokens: 6 }));
    expect(total).toMatchObject({ input_tokens: 22, output_tokens: 6 });
  });
});
