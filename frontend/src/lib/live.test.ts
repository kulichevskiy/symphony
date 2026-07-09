import { afterEach, describe, expect, it, vi } from "vitest";

import { registerTokenProvider } from "./auth";
import { streamRun, type LiveEvent } from "./live";

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
