#!/usr/bin/env bash
# Log the codex CLI into the symphony Docker stack's auth volume using the
# DEFAULT browser OAuth flow (for orgs where `codex login --device-auth` is
# disabled by admin policy).
#
# Why this dance: codex's login server binds 127.0.0.1:1455 INSIDE the
# container, and Docker port-publishing targets the container's eth0 — so a
# plain `-p 1455:1455` gets "empty reply" and the browser redirect to
# http://localhost:1455/auth/callback lands nowhere. A tiny node TCP
# forwarder (0.0.0.0:14550 -> 127.0.0.1:1455) inside the same container
# bridges published traffic to codex's loopback listener.
#
# Plain `docker run` (not `docker compose run`) is required: the compose
# service joins caddy's network namespace (`network_mode: service:caddy`),
# and Docker refuses port publishing on a container that joins another's
# namespace.
#
# Usage:  ./scripts/codex-login-docker.sh
# Then open the printed auth.openai.com URL (your browser usually opens
# automatically) and complete login; the token lands in the codex_auth
# volume. Verify:
#   docker compose run --rm --entrypoint codex symphony login status
#
# Remote VPS: run the same script through an SSH tunnel so the browser's
# localhost:1455 redirect lands on the VPS:
#   ssh -L 1455:localhost:1455 <vps>   # then run this script on the VPS
set -euo pipefail

VOLUME="${CODEX_AUTH_VOLUME:-symphony_codex_auth}"
IMAGE="${SYMPHONY_IMAGE:-symphony:local}"

exec docker run --rm -it \
  -p 127.0.0.1:1455:14550 \
  -v "${VOLUME}:/home/symphony/.codex" \
  --entrypoint sh "${IMAGE}" -c '
node -e "const net=require(\"net\");net.createServer(s=>{const c=net.connect(1455,\"127.0.0.1\");s.pipe(c);c.pipe(s);s.on(\"error\",()=>{});c.on(\"error\",()=>{})}).listen(14550,\"0.0.0.0\")" &
exec codex login'
