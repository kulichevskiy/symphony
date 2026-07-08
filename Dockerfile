# syntax=docker/dockerfile:1

# --- Stage 1: build the web dashboard (frontend/dist) ---------------------
# The daemon only serves /ui when frontend/dist exists, so bake it in.
FROM node:22-bookworm-slim AS frontend
WORKDIR /build/frontend
RUN corepack enable
COPY frontend/package.json frontend/pnpm-lock.yaml ./
RUN --mount=type=cache,target=/root/.local/share/pnpm/store pnpm install --frozen-lockfile
COPY frontend/ ./
RUN pnpm build

# --- Stage 2: runtime image with the full agent toolchain -----------------
FROM python:3.12-slim-bookworm

# uv, from the official static binary image.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# System deps: git + node/npm (for the coding-agent CLIs) + gh (GitHub CLI).
RUN apt-get update && apt-get install -y --no-install-recommends \
        git curl ca-certificates gnupg \
    && curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && mkdir -p -m 755 /etc/apt/keyrings \
    && curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
        -o /etc/apt/keyrings/githubcli-archive-keyring.gpg \
    && chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
        > /etc/apt/sources.list.d/github-cli.list \
    && apt-get update && apt-get install -y --no-install-recommends nodejs gh \
    && rm -rf /var/lib/apt/lists/*

# The two coding-agent CLIs, on PATH globally.
RUN npm install -g @anthropic-ai/claude-code @openai/codex

# Fail the build now if any tool is missing from PATH, rather than at runtime.
RUN command -v claude && command -v codex && command -v gh \
    && command -v git && command -v uv && command -v node

# Non-root runtime user; ~ is /home/symphony so config's ~/.claude etc. resolve.
RUN useradd --create-home --shell /bin/bash symphony
WORKDIR /app

# Dependencies first for layer caching.
COPY pyproject.toml uv.lock README.md ./
COPY src/ ./src/
RUN uv sync --frozen --no-dev

# Prebuilt dashboard so the daemon can mount /ui.
COPY --from=frontend /build/frontend/dist ./frontend/dist

# Data dirs (mount points for named volumes) owned by the runtime user.
# Auth dirs are created here too: an empty named volume mounted over a path
# that doesn't exist in the image gets created by Docker as root:root, which
# breaks the non-root `symphony` user's one-time CLI logins.
RUN mkdir -p /data/workspaces /data/logs /data/db \
        /home/symphony/.claude /home/symphony/.codex /home/symphony/.config/gh \
    && chown -R symphony:symphony /app /data /home/symphony

USER symphony
ENV HOME=/home/symphony
ENV PATH="/app/.venv/bin:$PATH"

ENTRYPOINT ["uv", "run", "--frozen", "--no-sync", "--no-dev", "symphony"]
CMD ["--config", "/app/config.local.yaml"]
