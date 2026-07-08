import { afterEach, describe, expect, it, vi } from "vitest";

import { ApiError, fetchMeta } from "./api";
import { registerTokenProvider } from "./auth";

afterEach(() => {
  registerTokenProvider(null);
  vi.restoreAllMocks();
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
