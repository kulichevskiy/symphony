import { Auth0Provider, useAuth0 } from "@auth0/auth0-react";
import { useQuery } from "@tanstack/react-query";
import { useEffect, type ReactNode } from "react";

import { Button } from "@/components/ui/button";
import { ApiError, fetchMeta } from "@/lib/api";
import { registerTokenProvider } from "@/lib/auth";

const DOMAIN = import.meta.env.VITE_AUTH0_DOMAIN;
const CLIENT_ID = import.meta.env.VITE_AUTH0_CLIENT_ID;

/** Auth0 is only enforced when both env vars are set (e.g. production). Local
 *  loopback dev leaves them unset and the app renders without a login gate. */
export const authEnabled = Boolean(DOMAIN && CLIENT_ID);

const RETURN_TO = () => `${window.location.origin}/ui/`;

/**
 * Wraps the app in `<Auth0Provider>` (Authorization Code + PKCE, Google, no
 * `audience` so the ID token stays a validatable JWT) and gates it: tokens live
 * in memory with refresh-token rotation, unauthenticated users are sent to
 * Auth0 login, and a logged-in-but-not-allowlisted user (backend 403) gets an
 * access-denied screen. A no-op passthrough when Auth0 is disabled.
 */
export function AuthProvider({ children }: { children: ReactNode }) {
  if (!authEnabled) {
    return <>{children}</>;
  }
  return (
    <Auth0Provider
      domain={DOMAIN as string}
      clientId={CLIENT_ID as string}
      authorizationParams={{
        redirect_uri: RETURN_TO(),
        // Force the Google social connection — the app's only login method.
        connection: "google-oauth2",
      }}
      // Keep tokens in memory (default cache) and rotate refresh tokens; never
      // touch localStorage.
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

/** Publishes the auth0-react client to `authHeaders()`, which runs outside React. */
function AuthBridge() {
  const { getAccessTokenSilently, getIdTokenClaims, loginWithRedirect } = useAuth0();
  useEffect(() => {
    registerTokenProvider({
      getAccessTokenSilently: () => getAccessTokenSilently(),
      getIdTokenClaims,
      loginWithRedirect: () => loginWithRedirect(),
    });
    return () => registerTokenProvider(null);
  }, [getAccessTokenSilently, getIdTokenClaims, loginWithRedirect]);
  return null;
}

function AuthGate({ children }: { children: ReactNode }) {
  const { isLoading, isAuthenticated, error, loginWithRedirect } = useAuth0();

  useEffect(() => {
    if (!isLoading && !isAuthenticated && !error) {
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
