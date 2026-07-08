import { Auth0Client } from "@auth0/auth0-spa-js";

type AuthConfig = { enabled: false } | { enabled: true; domain: string; client_id: string };

/** Refresh the ID token this many seconds before it actually expires. */
const REFRESH_SKEW_SECS = 60;

let client: Auth0Client | null = null;
let idToken: string | null = null;
let idTokenExpiresAt = 0;

async function fetchAuthConfig(): Promise<AuthConfig> {
  const response = await fetch("/api/auth-config", {
    headers: { Accept: "application/json" },
  });
  if (!response.ok) {
    return { enabled: false };
  }
  return (await response.json()) as AuthConfig;
}

/**
 * Reads `/api/auth-config` (unauthenticated) and, if the backend has Auth0
 * enabled, runs the SPA login flow before the app renders — the `/api/*`
 * gate rejects every call otherwise. Redirects away and never resolves when
 * the browser isn't authenticated yet.
 */
export async function initAuth(): Promise<void> {
  const config = await fetchAuthConfig();
  if (!config.enabled) {
    return;
  }

  client = new Auth0Client({
    domain: config.domain,
    clientId: config.client_id,
    authorizationParams: { redirect_uri: `${window.location.origin}/ui/` },
    cacheLocation: "localstorage",
  });

  const params = new URLSearchParams(window.location.search);
  if (params.has("error") && params.has("state")) {
    // Auth0 redirected back with a failed login/consent (no `code`). Let
    // handleRedirectCallback surface the error instead of silently falling
    // through to loginWithRedirect and looping forever.
    await client.handleRedirectCallback();
  } else if (params.has("code") && params.has("state")) {
    const result = await client.handleRedirectCallback<{ targetUrl?: string }>();
    window.history.replaceState({}, "", result.appState?.targetUrl ?? window.location.pathname);
  }

  if (!(await client.isAuthenticated())) {
    const targetUrl = `${window.location.pathname}${window.location.search}`;
    await client.loginWithRedirect({ appState: { targetUrl } });
    return new Promise<void>(() => {});
  }

  await refreshIdToken();
}

async function refreshIdToken(): Promise<void> {
  const claims = await client?.getIdTokenClaims();
  idToken = claims?.__raw ?? null;
  idTokenExpiresAt = claims?.exp ?? 0;
}

/**
 * `Authorization` header for `/api/*` fetches; empty once auth is disabled or
 * unset. Refreshes the cached ID token first if it's expired or about to
 * expire, so a dashboard left open past the token's `exp` keeps working.
 */
export async function authHeaders(): Promise<Record<string, string>> {
  if (client !== null && Date.now() / 1000 >= idTokenExpiresAt - REFRESH_SKEW_SECS) {
    await client.getTokenSilently();
    await refreshIdToken();
  }
  return idToken ? { Authorization: `Bearer ${idToken}` } : {};
}
