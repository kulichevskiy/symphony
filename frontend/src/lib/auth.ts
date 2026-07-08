import type { IdToken } from "@auth0/auth0-react";

/**
 * The slice of the `@auth0/auth0-react` client that `authHeaders()` needs.
 * `fetchJson` is a plain module function, not a component, so it can't call
 * `useAuth0()`; a component mounted inside `<Auth0Provider>` registers this
 * bridge instead (see `AuthBridge` in `lib/auth0`). Until it does — Auth0
 * disabled, or before mount — `authHeaders()` sends no bearer.
 */
export interface TokenProvider {
  getAccessTokenSilently: (opts?: { cacheMode?: "on" | "off" | "cache-only" }) => Promise<string>;
  getIdTokenClaims: () => Promise<IdToken | undefined>;
  loginWithRedirect: () => Promise<void>;
}

let provider: TokenProvider | null = null;

export function registerTokenProvider(next: TokenProvider | null): void {
  provider = next;
}

function isExpired(claims: IdToken | undefined): boolean {
  return typeof claims?.exp === "number" && claims.exp * 1000 <= Date.now();
}

/**
 * `Authorization` header for `/api/*` fetches; empty when Auth0 is disabled or
 * not yet wired. Calls `getAccessTokenSilently()` first so the SDK rotates the
 * refresh token and refreshes the cache when it's near expiry, then sends the
 * raw ID token — a JWT the backend gate can validate against its email
 * allowlist (the access token would be opaque). The SDK's cache is keyed off
 * the access token's own expiry, not the ID token's, so a dashboard left open
 * past the ID token's expiry (while the cached access token is still valid)
 * can otherwise be served a stale, already-expired ID token straight from
 * cache; if that happens, force one uncached round-trip before giving up.
 * Redirects to login if the session can't be renewed silently.
 */
export async function authHeaders(): Promise<Record<string, string>> {
  if (provider === null) {
    return {};
  }
  try {
    await provider.getAccessTokenSilently();
    let claims = await provider.getIdTokenClaims();
    if (isExpired(claims)) {
      await provider.getAccessTokenSilently({ cacheMode: "off" });
      claims = await provider.getIdTokenClaims();
    }
    const raw = claims?.__raw;
    return raw ? { Authorization: `Bearer ${raw}` } : {};
  } catch {
    await provider.loginWithRedirect();
    return {};
  }
}
