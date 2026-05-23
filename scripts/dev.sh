#!/usr/bin/env bash
# Build the frontend (watch mode) and run symphony, both in parallel.
# - vite rebuilds frontend/dist/ on source change
# - symphony serves /ui from that dist directory
# Refresh the browser to pick up frontend edits. Ctrl-C stops both.

set -euo pipefail

cd "$(dirname "$0")/.."

CONFIG="${SYMPHONY_CONFIG:-config.local.yaml}"

if [[ ! -f "$CONFIG" ]]; then
  echo "config not found: $CONFIG (override with SYMPHONY_CONFIG=)" >&2
  exit 2
fi

VITE_PID=""
cleanup() {
  if [[ -n "$VITE_PID" ]] && kill -0 "$VITE_PID" 2>/dev/null; then
    kill "$VITE_PID" 2>/dev/null || true
    wait "$VITE_PID" 2>/dev/null || true
  fi
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

echo "› starting symphony (config=$CONFIG)"
uv run python -m symphony --config "$CONFIG"
