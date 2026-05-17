# symphonyd frontend

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
