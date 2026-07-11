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
        git curl ca-certificates gnupg util-linux \
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

# Corepack shims (pnpm/yarn) so agents + verify_cmd can run repos pinned to
# pnpm (e.g. `verify_cmd: pnpm build && pnpm test`); node ships corepack.
RUN corepack enable

# Fail the build now if any tool is missing from PATH, rather than at runtime.
RUN command -v claude && command -v codex && command -v gh \
    && command -v git && command -v uv && command -v node

# Non-root runtime user; ~ is /home/symphony so config's ~/.claude etc. resolve.
RUN useradd --create-home --shell /bin/bash symphony
WORKDIR /app

# Dependencies first for layer caching.
COPY pyproject.toml uv.lock README.md ./
COPY src/ ./src/
COPY prompts/ ./prompts/
COPY taste-guide.md ./
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

# Headless commits (agent runs, `git rebase --continue` in the merge path)
# need a global git identity — there's no interactive `git config` step in
# this container. /home/symphony isn't a mounted volume, so this survives
# restarts.
RUN git config --global user.name "Symphony" \
    && git config --global user.email "symphony@localhost"

# The orchestrator delivers via plain `git push`/`git fetch`, not `gh`. Wire
# gh in as the HTTPS credential helper (what `gh auth setup-git` does) so those
# raw git ops authenticate off the mounted gh_auth volume. Baked at build time,
# resolves the token from ~/.config/gh at runtime.
RUN git config --global credential."https://github.com".helper "!gh auth git-credential" \
    && git config --global credential."https://gist.github.com".helper "!gh auth git-credential"

# Start as root so the entrypoint can hand `symphony` an EFFECTIVE CAP_NET_ADMIN.
# The agent's bwrap sandbox creates an isolated network namespace and brings up
# loopback (RTM_NEWADDR), which needs that capability. `cap_add: NET_ADMIN` in
# compose only puts it in the container's *bounding* set; a non-root process
# gets no effective caps across exec without file/ambient caps (Codex P1 on
# #305). So drop to `symphony` via setpriv, raising NET_ADMIN into the *ambient*
# set — ambient caps survive the uid switch AND are inherited by child processes
# (the agent's bwrap), giving it the effective cap it needs. The main daemon
# still runs as the non-root `symphony` user; only the setpriv shim is root.
USER root
ENTRYPOINT ["setpriv", "--reuid", "symphony", "--regid", "symphony", \
            "--init-groups", "--inh-caps", "+net_admin", "--ambient-caps", "+net_admin", \
            "--", "uv", "run", "--frozen", "--no-sync", "--no-dev", "symphony"]
CMD ["--config", "/app/config.local.yaml"]
