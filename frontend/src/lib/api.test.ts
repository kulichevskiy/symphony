import { afterEach, describe, expect, it, vi } from "vitest";

import { ApiError, fetchMeta, fetchPauseState, setPauseState } from "./api";
import { registerTokenProvider } from "./auth";

afterEach(() => {
  registerTokenProvider(null);
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("fetchJson", () => {
  it("attaches the ID token as an Authorization: Bearer header", async () => {
    registerTokenProvider({
      getAccessTokenSilently: async () => "opaque",
      getIdTokenClaims: async () => ({ __raw: "id-jwt" }) as never,
      loginWithRedirect: async () => {},
    });
    const fetchMock = vi.fn(
      (_input: RequestInfo | URL, _init?: RequestInit) =>
        Promise.resolve(new Response("{}", { status: 200 })),
    );
    vi.stubGlobal("fetch", fetchMock);

    await fetchMeta();

    const init = fetchMock.mock.calls[0][1] as RequestInit;
    expect((init.headers as Record<string, string>).Authorization).toBe("Bearer id-jwt");
  });

  it("throws an ApiError carrying the HTTP status on failure", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("forbidden", { status: 403 })),
    );

    await expect(fetchMeta()).rejects.toBeInstanceOf(ApiError);
    await expect(fetchMeta()).rejects.toMatchObject({ status: 403 });
  });
});

describe("pause", () => {
  it("reads the daemon pause state", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response(JSON.stringify({ paused: true }), { status: 200 })),
    );

    await expect(fetchPauseState()).resolves.toEqual({ paused: true });
  });

  it("posts the requested pause state and returns the daemon's", async () => {
    const fetchMock = vi.fn(
      (_input: RequestInfo | URL, _init?: RequestInit) =>
        Promise.resolve(new Response(JSON.stringify({ paused: true }), { status: 200 })),
    );
    vi.stubGlobal("fetch", fetchMock);

    await expect(setPauseState(true)).resolves.toEqual({ paused: true });
    const url = fetchMock.mock.calls[0][0] as string;
    const init = fetchMock.mock.calls[0][1] as RequestInit;
    expect(url).toBe("/api/pause");
    expect(init.method).toBe("POST");
    expect(JSON.parse(init.body as string)).toEqual({ paused: true });
  });
});
