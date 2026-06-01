#!/usr/bin/env bash
# Build the frontend (watch mode), run symphony, and expose the webhook
# receiver through a cloudflared quick tunnel — all in one terminal.
# - vite rebuilds frontend/dist/ on source change
# - symphony serves /ui from that dist directory
# - cloudflared exposes 127.0.0.1:<webhook_port> on a *.trycloudflare.com URL
# Refresh the browser to pick up frontend edits. Ctrl-C stops everything.
#
# The quick-tunnel URL is random per run and printed below — paste it into
# the Linear webhook config. Set SYMPHONY_TUNNEL=0 to skip the tunnel.

set -euo pipefail

cd "$(dirname "$0")/.."

CONFIG="${SYMPHONY_CONFIG:-config.local.yaml}"

if [[ ! -f "$CONFIG" ]]; then
  echo "config not found: $CONFIG (override with SYMPHONY_CONFIG=)" >&2
  exit 2
fi

# Port the webhook receiver binds to — pulled from config, overridable.
# `|| true` so a config that omits webhook_port (relying on the daemon's
# built-in default) doesn't trip `set -euo pipefail` via grep's non-zero exit.
WEBHOOK_PORT="${SYMPHONY_WEBHOOK_PORT:-$(grep -E '^[[:space:]]*webhook_port:' "$CONFIG" | head -1 | awk '{print $2}' || true)}"
WEBHOOK_PORT="${WEBHOOK_PORT:-8787}"

START_TUNNEL="${SYMPHONY_TUNNEL:-1}"

VITE_PID=""
TUNNEL_PID=""
cleanup() {
  for pid in "$TUNNEL_PID" "$VITE_PID"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
}
trap cleanup EXIT INT TERM

if [[ ! -d frontend/node_modules ]]; then
  echo "› installing frontend deps"
  (cd frontend && pnpm install --frozen-lockfile)
fi

echo "› building frontend (watch mode)"
(cd frontend && pnpm exec vite build --watch) &
VITE_PID=$!

DIST="frontend/dist/index.html"
echo "› waiting for first build to land"
for _ in $(seq 1 240); do
  if [[ -f "$DIST" ]]; then break; fi
  if ! kill -0 "$VITE_PID" 2>/dev/null; then
    echo "vite exited before producing $DIST" >&2
    exit 1
  fi
  sleep 0.5
done
if [[ ! -f "$DIST" ]]; then
  echo "frontend never produced $DIST" >&2
  exit 1
fi

if [[ "$START_TUNNEL" != "0" ]]; then
  if command -v cloudflared >/dev/null 2>&1; then
    mkdir -p logs
    TUNNEL_LOG="logs/cloudflared.log"
    : > "$TUNNEL_LOG"
    echo "› starting cloudflared tunnel → http://127.0.0.1:$WEBHOOK_PORT"
    cloudflared tunnel --url "http://127.0.0.1:$WEBHOOK_PORT" >"$TUNNEL_LOG" 2>&1 &
    TUNNEL_PID=$!

    # The quick-tunnel URL lands in the log a few seconds after start.
    TUNNEL_URL=""
    for _ in $(seq 1 40); do
      TUNNEL_URL="$(grep -oE 'https://[a-z0-9-]+\.trycloudflare\.com' "$TUNNEL_LOG" | head -1 || true)"
      if [[ -n "$TUNNEL_URL" ]]; then break; fi
      if ! kill -0 "$TUNNEL_PID" 2>/dev/null; then
        echo "cloudflared exited before producing a URL — see $TUNNEL_LOG" >&2
        break
      fi
      sleep 0.5
    done
    if [[ -n "$TUNNEL_URL" ]]; then
      # The receiver registers POST /linear/webhook (no prefix), so the bare
      # tunnel origin would 404 — paste the full path into Linear.
      echo
      echo "  ┌─ webhook tunnel ────────────────────────────────────────"
      echo "  │  $TUNNEL_URL/linear/webhook"
      echo "  │  → paste into the Linear webhook URL (logs: $TUNNEL_LOG)"
      echo "  └─────────────────────────────────────────────────────────"
      echo
    else
      echo "› tunnel URL not found yet — tail $TUNNEL_LOG to grab it" >&2
    fi
  else
    echo "› cloudflared not installed — skipping tunnel (set SYMPHONY_TUNNEL=0 to silence)" >&2
  fi
fi

echo "› starting symphony (config=$CONFIG)"
uv run python -m symphony --config "$CONFIG"
