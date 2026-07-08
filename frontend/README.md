# symphony frontend

Minimal Vite + React 19 shell for the daemon UI. Production builds are served by
FastAPI from `frontend/dist/` at `/ui/`.

## Development

```bash
cd frontend
pnpm install
pnpm dev
```

Vite listens on `http://127.0.0.1:5173` and proxies `/api/*` to the daemon on
`http://127.0.0.1:8787`.

## Production build

```bash
cd frontend
pnpm install
pnpm build
```

The build writes `frontend/dist/`. Start the daemon with UI enabled and open:

```text
http://localhost:8787/ui/
```

## Auth0 login

The daemon's `/api/auth-config` response is authoritative for whether the app
gates behind Auth0 — it's only reachable, and only reports enabled, when the
daemon itself has its Auth0 env set:

```text
AUTH0_DOMAIN=your-tenant.eu.auth0.com
AUTH0_CLIENT_ID=your-spa-client-id
AUTH0_ALLOWED_EMAILS=alice@example.com,bob@example.com
```

The Vite vars below are a build-time fallback only, used when that runtime
call fails outright (e.g. the daemon is unreachable during local dev against a
static build). Set them too if you want the gate to still work in that case:

```text
VITE_AUTH0_DOMAIN=your-tenant.eu.auth0.com
VITE_AUTH0_CLIENT_ID=your-spa-client-id
```

Leave both sides unset for local loopback dev — the login gate is skipped. The
ID token is sent as `Authorization: Bearer` on every `/api/*` request; the
backend gate ([SYM-165](https://linear.app/alexchevsky/issue/SYM-165))
validates it and enforces the email allowlist (403 → access-denied screen).
