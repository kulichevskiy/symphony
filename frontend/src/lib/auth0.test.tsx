// @vitest-environment jsdom
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, waitFor } from "@testing-library/react";
import { StrictMode, type ReactNode } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const loginWithRedirect = vi.fn();

// Mutable per-test auth0 state; the mocked `useAuth0` returns this object.
let authState: {
  isLoading: boolean;
  isAuthenticated: boolean;
  error: unknown;
};

vi.mock("@auth0/auth0-react", () => ({
  useAuth0: () => ({
    ...authState,
    loginWithRedirect,
    logout: vi.fn(),
    getAccessTokenSilently: vi.fn(),
    getIdTokenClaims: vi.fn(),
  }),
  Auth0Provider: ({ children }: { children: ReactNode }) => <>{children}</>,
}));

class ApiError extends Error {
  constructor(public status: number) {
    super(`status ${status}`);
  }
}

const fetchMeta = vi.fn();

vi.mock("@/lib/api", () => ({
  ApiError,
  fetchMeta: (...args: unknown[]) => fetchMeta(...args),
  fetchAuthConfig: vi.fn(),
}));

// Imported after the mocks are registered.
const { AccessDenied, AuthGate } = await import("./auth0");

function renderGate() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <StrictMode>
      <QueryClientProvider client={client}>
        <AuthGate>
          <div data-testid="dashboard">dashboard</div>
        </AuthGate>
      </QueryClientProvider>
    </StrictMode>,
  );
}

describe("AccessDenied", () => {
  it("shows an access-denied message with a sign-out action", () => {
    const markup = renderToStaticMarkup(<AccessDenied onSignOut={() => {}} />);
    expect(markup).toContain("Access denied");
    expect(markup).toContain("allowlist");
    expect(markup).toContain("Sign out");
  });
});

describe("AuthGate", () => {
  beforeEach(() => {
    loginWithRedirect.mockReset();
    fetchMeta.mockReset();
  });
  afterEach(() => cleanup());

  it("renders children from a cached session without redirecting to Auth0", async () => {
    authState = { isLoading: false, isAuthenticated: true, error: null };
    fetchMeta.mockResolvedValue({});

    const { findByTestId } = renderGate();

    expect(await findByTestId("dashboard")).toBeTruthy();
    expect(loginWithRedirect).not.toHaveBeenCalled();
  });

  it("redirects to Auth0 login exactly once when there is no session", async () => {
    authState = { isLoading: false, isAuthenticated: false, error: null };

    renderGate();

    await waitFor(() => expect(loginWithRedirect).toHaveBeenCalledTimes(1));
    expect(loginWithRedirect).toHaveBeenCalledWith(
      expect.objectContaining({ appState: expect.objectContaining({ returnTo: expect.any(String) }) }),
    );
  });
});
