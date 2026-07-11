import { Auth0Provider, useAuth0 } from "@auth0/auth0-react";
import { useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";

import { Button } from "@/components/ui/button";
import { ApiError, fetchAuthConfig, fetchMeta } from "@/lib/api";
import { registerTokenProvider, type TokenProvider } from "@/lib/auth";

// Build-time fallback only — used when the runtime `/api/auth-config` call
// below fails outright. The daemon's own env vars are the source of truth,
// since a static bundle can be served by a daemon whose AUTH0_DOMAIN /
// AUTH0_CLIENT_ID differ from (or weren't set at) build time.
const BUILD_DOMAIN = import.meta.env.VITE_AUTH0_DOMAIN;
const BUILD_CLIENT_ID = import.meta.env.VITE_AUTH0_CLIENT_ID;

/** Whether the mounted app is running behind the Auth0 gate. `App` reads this
 *  to decide whether to show the logout control; it's only ever read once
 *  `AuthProvider` has resolved (its children, and everything under them,
 *  don't mount until then), so it's always current by the time anything
 *  reads it despite not being reactive state itself. */
export let authEnabled = false;

const RETURN_TO = () => `${window.location.origin}/ui/`;

interface ResolvedAuthConfig {
  domain: string;
  clientId: string;
}

/** The running daemon's `/api/auth-config` is authoritative for whether/how
 *  to run the Auth0 flow. Only falls back to the build-time `VITE_AUTH0_*`
 *  vars when that call fails outright (e.g. network hiccup), so a reachable
 *  daemon's runtime config always wins over whatever was baked into this
 *  bundle at build time. */
async function resolveAuthConfig(): Promise<ResolvedAuthConfig | null> {
  try {
    const config = await fetchAuthConfig();
    if (config.enabled && config.domain && config.client_id) {
      return { domain: config.domain, clientId: config.client_id };
    }
    return null;
  } catch {
    return BUILD_DOMAIN && BUILD_CLIENT_ID
      ? { domain: BUILD_DOMAIN, clientId: BUILD_CLIENT_ID }
      : null;
  }
}

/**
 * Resolves the Auth0 config from the running daemon, then wraps the app in
 * `<Auth0Provider>` (Authorization Code + PKCE, Google, no `audience` so the
 * ID token stays a validatable JWT) and gates it: tokens are cached in
 * localStorage with refresh-token rotation so a reload keeps the session,
 * unauthenticated users are sent to Auth0 login, and
 * a logged-in-but-not-allowlisted user (backend 403) gets an access-denied
 * screen. A no-op passthrough when Auth0 is disabled.
 */
export function AuthProvider({ children }: { children: ReactNode }) {
  const [config, setConfig] = useState<ResolvedAuthConfig | null | "pending">("pending");

  useEffect(() => {
    let cancelled = false;
    void resolveAuthConfig().then((resolved) => {
      if (!cancelled) {
        setConfig(resolved);
      }
    });
    return () => {
      cancelled = true;
    };
  }, []);

  if (config === "pending") {
    return <AuthNotice title="Loading…" />;
  }

  authEnabled = config !== null;

  if (config === null) {
    return <>{children}</>;
  }

  return (
    <Auth0Provider
      domain={config.domain}
      clientId={config.clientId}
      authorizationParams={{
        redirect_uri: RETURN_TO(),
        // Force the Google social connection — the app's only login method.
        connection: "google-oauth2",
      }}
      // Persist the session in localStorage so a page reload restores it
      // instead of bouncing through the Auth0 redirect (and, under Safari ITP,
      // a full re-login). Trade-off: the rotating refresh token now lives in
      // localStorage, an accepted XSS surface for this SPA setup.
      cacheLocation="localstorage"
      useRefreshTokens
      onRedirectCallback={(appState) => {
        window.history.replaceState({}, "", appState?.returnTo ?? "/ui/");
      }}
    >
      <AuthBridge />
      <AuthGate>{children}</AuthGate>
    </Auth0Provider>
  );
}

/**
 * Publishes the auth0-react client to `authHeaders()`, which runs outside
 * React. Registers during render, not in an effect: `AllowlistGate`'s
 * `useQuery` can kick off its fetch as soon as it renders (React Query
 * doesn't wait for effects to commit), so registering the bridge only after
 * commit would leave that first request without a bearer token.
 */
function AuthBridge() {
  const { getAccessTokenSilently, getIdTokenClaims, loginWithRedirect } = useAuth0();
  const tokenProvider = useMemo<TokenProvider>(
    () => ({
      getAccessTokenSilently: (opts) => getAccessTokenSilently(opts),
      getIdTokenClaims: () => getIdTokenClaims(),
      loginWithRedirect: (opts) => loginWithRedirect(opts),
    }),
    [getAccessTokenSilently, getIdTokenClaims, loginWithRedirect],
  );
  registerTokenProvider(tokenProvider);
  useEffect(() => () => registerTokenProvider(null), []);
  return null;
}

export function AuthGate({ children }: { children: ReactNode }) {
  const { isLoading, isAuthenticated, error, loginWithRedirect } = useAuth0();
  // StrictMode double-invokes effects in dev; without this guard an
  // unauthenticated user's first render would start two login redirects.
  const redirectStarted = useRef(false);

  useEffect(() => {
    if (!isLoading && !isAuthenticated && !error && !redirectStarted.current) {
      redirectStarted.current = true;
      void loginWithRedirect({
        appState: { returnTo: `${window.location.pathname}${window.location.search}` },
      });
    }
  }, [isLoading, isAuthenticated, error, loginWithRedirect]);

  if (error) {
    return <AuthNotice title="Sign-in failed" detail={error.message} />;
  }
  if (isLoading || !isAuthenticated) {
    return <AuthNotice title="Signing in…" />;
  }
  return <AllowlistGate>{children}</AllowlistGate>;
}

/** Probes the gated API once logged in: a 403 means the email isn't allowlisted. */
function AllowlistGate({ children }: { children: ReactNode }) {
  const { logout } = useAuth0();
  const { error, isLoading } = useQuery({
    queryKey: ["auth-probe"],
    queryFn: fetchMeta,
    retry: false,
    staleTime: Infinity,
  });

  if (isLoading) {
    return <AuthNotice title="Loading…" />;
  }
  if (error instanceof ApiError && error.status === 403) {
    return (
      <AccessDenied
        onSignOut={() => void logout({ logoutParams: { returnTo: RETURN_TO() } })}
      />
    );
  }
  if (error) {
    return (
      <AuthNotice
        title="Sign-in check failed"
        detail={error instanceof ApiError ? `Server returned ${error.status}.` : String(error)}
      />
    );
  }
  return <>{children}</>;
}

/** Logout control for the app header; renders nothing when Auth0 is disabled. */
export function LogoutButton() {
  const { logout } = useAuth0();
  return (
    <Button
      variant="ghost"
      className="h-8 px-2 text-xs text-muted-foreground"
      onClick={() => void logout({ logoutParams: { returnTo: RETURN_TO() } })}
    >
      Sign out
    </Button>
  );
}

export function AccessDenied({ onSignOut }: { onSignOut: () => void }) {
  return (
    <div className="flex min-h-screen flex-col items-center justify-center gap-4 bg-background px-4 text-center text-foreground">
      <h1 className="text-lg font-semibold">Access denied</h1>
      <p className="max-w-sm text-sm text-muted-foreground">
        Your account isn&apos;t on the allowlist for this dashboard. Sign in with an
        authorized account to continue.
      </p>
      <Button onClick={onSignOut}>Sign out</Button>
    </div>
  );
}

function AuthNotice({ title, detail }: { title: string; detail?: string }) {
  return (
    <div className="flex min-h-screen flex-col items-center justify-center gap-2 bg-background px-4 text-center text-foreground">
      <p className="text-sm font-medium">{title}</p>
      {detail ? <p className="max-w-sm text-xs text-muted-foreground">{detail}</p> : null}
    </div>
  );
}
