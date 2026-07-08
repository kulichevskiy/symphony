import { Auth0Client } from "@auth0/auth0-spa-js";

type AuthConfig = { enabled: false } | { enabled: true; domain: string; client_id: string };

let idToken: string | null = null;

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

  const client = new Auth0Client({
    domain: config.domain,
    clientId: config.client_id,
    authorizationParams: { redirect_uri: `${window.location.origin}/ui/` },
    cacheLocation: "localstorage",
  });

  const params = new URLSearchParams(window.location.search);
  if (params.has("code") && params.has("state")) {
    await client.handleRedirectCallback();
    window.history.replaceState({}, "", window.location.pathname);
  }

  if (!(await client.isAuthenticated())) {
    await client.loginWithRedirect();
    return new Promise<void>(() => {});
  }

  const claims = await client.getIdTokenClaims();
  idToken = claims?.__raw ?? null;
}

/** `Authorization` header for `/api/*` fetches; empty once auth is disabled or unset. */
export function authHeaders(): Record<string, string> {
  return idToken ? { Authorization: `Bearer ${idToken}` } : {};
}
