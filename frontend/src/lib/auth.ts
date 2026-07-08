import { Auth0Client } from "@auth0/auth0-spa-js";

type AuthConfig = { enabled: false } | { enabled: true; domain: string; client_id: string };

/** Refresh the ID token this many seconds before it actually expires. */
const REFRESH_SKEW_SECS = 60;

let client: Auth0Client | null = null;
let idToken: string | null = null;
let idTokenExpiresAt = 0;

async function fetchAuthConfig(): Promise<AuthConfig> {
  try {
    const response = await fetch("/api/auth-config", {
      headers: { Accept: "application/json" },
    });
    if (!response.ok) {
      return { enabled: false };
    }
    return (await response.json()) as AuthConfig;
  } catch {
    // Network error (e.g. the daemon/proxy isn't up yet) — treat as
    // disabled so the UI renders instead of hanging forever. Auth0-gated
    // API calls will still 401 until the page is reloaded.
    return { enabled: false };
  }
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
    return redirectToLogin();
  }

  await refreshIdToken();
}

/** Sends the browser to Auth0 login, preserving the current route, and never resolves. */
async function redirectToLogin(): Promise<never> {
  const targetUrl = `${window.location.pathname}${window.location.search}`;
  await client?.loginWithRedirect({ appState: { targetUrl } });
  return new Promise<never>(() => {});
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
 * If silent renewal can't complete without an interactive login (Auth0
 * session expired, third-party cookies blocked, ...), redirect to login
 * instead of proceeding with a stale/missing token.
 */
export async function authHeaders(): Promise<Record<string, string>> {
  if (client !== null && Date.now() / 1000 >= idTokenExpiresAt - REFRESH_SKEW_SECS) {
    try {
      // cacheMode: "off" bypasses the access-token cache, which can outlive
      // the ID token and otherwise satisfy this call without contacting
      // Auth0 — leaving refreshIdToken() to reload the same stale ID token.
      await client.getTokenSilently({ cacheMode: "off" });
      await refreshIdToken();
    } catch {
      return redirectToLogin();
    }
  }
  return idToken ? { Authorization: `Bearer ${idToken}` } : {};
}
