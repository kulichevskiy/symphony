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
