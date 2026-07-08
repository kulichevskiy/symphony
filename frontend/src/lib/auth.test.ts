import { afterEach, describe, expect, it } from "vitest";

import type { TokenProvider } from "./auth";
import { authHeaders, registerTokenProvider } from "./auth";

function provider(overrides: Partial<TokenProvider> = {}): TokenProvider {
  return {
    getAccessTokenSilently: async () => "opaque-access-token",
    getIdTokenClaims: async () => ({ __raw: "id.jwt.token" }) as never,
    loginWithRedirect: async () => {},
    ...overrides,
  };
}

afterEach(() => registerTokenProvider(null));

describe("authHeaders", () => {
  it("sends no header when Auth0 is disabled (no provider registered)", async () => {
    expect(await authHeaders()).toEqual({});
  });

  it("attaches the raw ID token as a Bearer header", async () => {
    registerTokenProvider(provider());
    expect(await authHeaders()).toEqual({ Authorization: "Bearer id.jwt.token" });
  });

  it("refreshes the session before reading the ID token", async () => {
    const calls: string[] = [];
    registerTokenProvider(
      provider({
        getAccessTokenSilently: async () => {
          calls.push("refresh");
          return "a";
        },
        getIdTokenClaims: async () => {
          calls.push("claims");
          return { __raw: "t" } as never;
        },
      }),
    );
    await authHeaders();
    expect(calls).toEqual(["refresh", "claims"]);
  });

  it("forces an uncached refresh when the cached ID token is already expired", async () => {
    const cacheModes: Array<string | undefined> = [];
    let claimsCall = 0;
    registerTokenProvider(
      provider({
        getAccessTokenSilently: async (opts) => {
          cacheModes.push(opts?.cacheMode);
          return "a";
        },
        getIdTokenClaims: async () => {
          claimsCall += 1;
          // First read: expired. Second (post-forced-refresh) read: fresh.
          const exp = claimsCall === 1 ? Date.now() / 1000 - 60 : Date.now() / 1000 + 3600;
          return { __raw: `token-${claimsCall}`, exp } as never;
        },
      }),
    );
    expect(await authHeaders()).toEqual({ Authorization: "Bearer token-2" });
    expect(cacheModes).toEqual([undefined, "off"]);
  });

  it("redirects to login and sends no header when renewal fails", async () => {
    let loggedIn = false;
    registerTokenProvider(
      provider({
        getAccessTokenSilently: async () => {
          throw new Error("login_required");
        },
        loginWithRedirect: async () => {
          loggedIn = true;
        },
      }),
    );
    expect(await authHeaders()).toEqual({});
    expect(loggedIn).toBe(true);
  });
});
