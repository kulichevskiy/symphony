"""CLI entrypoint.

```
uv run python -m symphony --config examples/config.yaml
uv run symphony preflight                    # validate auth + states
uv run symphony --config ... --once          # one poll cycle then exit (smoke test)
```
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import signal
import sys
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast, get_args

import aiosqlite
import click

from . import db
from .agent.claude_models import _resolve_alias_model_id, fetch_claude_effort_capabilities
from .agent.codex_models import SUPPORTED_CODEX_EFFORTS
from .app import build_server_config, create_app
from .auth import Auth0Settings
from .config import Config, RepoBinding, RoleName, Secrets
from .effective_config import ConfigBootError, assemble_effective_config
from .github.webhook import GitHubWebhookSettings
from .linear.client import Linear, LinearError, LinearIssue
from .orchestrator.poll import Orchestrator
from .orchestrator.reconcile import reconcile
from .tokens import effective_tokens
from .tracker import (
    DEFAULT_PROVIDER,
    DEFAULT_SITE,
    IssueTracker,
    TrackerContext,
    TrackerRegistry,
    context_for_binding,
    for_binding,
)
from .ui.external import GitHubExternalClient
from .webhook import WebhookSettings

if TYPE_CHECKING:
    from .db.token_backfill import CodexModels

_ANSI_RESET = "\x1b[0m"
_ANSI_DIM = "\x1b[2m"
_ANSI_YELLOW = "\x1b[33m"
_ANSI_RED = "\x1b[31m"
_ANSI_BOLD_RED = "\x1b[1;31m"

# Matches `HTTP/1.1 500 ...` and similar inside log messages from httpx etc.
_HTTP_STATUS_RE = re.compile(r"HTTP/\d(?:\.\d)?\s+(\d{3})\b")


class _ColorFormatter(logging.Formatter):
    """Format log records with ANSI color based on level + HTTP status in message.

    Why HTTP status sniffing: httpx logs every response at INFO, so a 5xx
    from Linear would otherwise look identical to a 200 in the stream.
    """

    def format(self, record: logging.LogRecord) -> str:
        text = super().format(record)
        color = self._color_for(record, text)
        if color is None:
            return text
        return f"{color}{text}{_ANSI_RESET}"

    @staticmethod
    def _color_for(record: logging.LogRecord, text: str) -> str | None:
        if record.levelno >= logging.ERROR:
            return _ANSI_BOLD_RED
        if record.levelno >= logging.WARNING:
            return _ANSI_YELLOW
        match = _HTTP_STATUS_RE.search(text)
        if match:
            status = int(match.group(1))
            if status >= 500:
                return _ANSI_BOLD_RED
            if status >= 400:
                return _ANSI_YELLOW
        if record.levelno <= logging.DEBUG:
            return _ANSI_DIM
        return None


def _setup_logging() -> None:
    handler = logging.StreamHandler()
    fmt = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"
    # NO_COLOR (https://no-color.org) wins; FORCE_COLOR overrides a non-TTY
    # stream (handy when piping through tee or viewing via journalctl with
    # SYSTEMD_COLORS=1) so colors aren't silently lost.
    if os.environ.get("NO_COLOR") is not None:
        use_color = False
    elif os.environ.get("FORCE_COLOR") not in (None, "", "0"):
        use_color = True
    else:
        use_color = handler.stream.isatty()
    handler.setFormatter(_ColorFormatter(fmt) if use_color else logging.Formatter(fmt))
    # force=True so we win over any earlier handler an import may have attached
    # to the root logger (otherwise basicConfig is a silent no-op).
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)


def _resolve_binding(cfg: Config, issue: LinearIssue) -> RepoBinding | None:
    """Pick the binding the poll loop would have used for `issue`.

    The poll loop iterates `cfg.repos` in order and the first binding whose
    `(team_key, label)` filter matches an issue claims it (subsequent
    bindings see it as already-active and skip). Mirror that: walk
    `cfg.repos` in order and return the first binding for the issue's team
    that either has no `issue_label` (catch-all) or whose label is on the
    issue. Selecting by team key alone, as the earlier `next(...)` did,
    silently routed manual dispatches to the wrong repo when one team was
    fanned out to multiple repos via labels.
    """
    team_bindings = [b for b in cfg.repos if b.linear_team_key == issue.team_key]
    if not team_bindings:
        configured = sorted({b.linear_team_key for b in cfg.repos})
        click.echo(
            f"no binding configured for team key {issue.team_key!r}; "
            f"configured teams: {configured}",
            err=True,
        )
        return None

    issue_labels = set(issue.labels)
    for binding in team_bindings:
        if binding.issue_label is None or binding.issue_label in issue_labels:
            return binding

    expected = sorted({b.issue_label for b in team_bindings if b.issue_label})
    click.echo(
        f"no binding matches {issue.identifier}: issue labels {sorted(issue_labels)} "
        f"do not include any of {expected} and no catch-all binding is configured.",
        err=True,
    )
    return None


def _auth0_settings(cfg: Config) -> Auth0Settings | None:
    fields = {
        "AUTH0_DOMAIN": cfg.auth0_domain,
        "AUTH0_CLIENT_ID": cfg.auth0_client_id,
        "AUTH0_ALLOWED_EMAILS": cfg.auth0_allowed_emails,
    }
    set_fields = [name for name, value in fields.items() if value]
    if not set_fields:
        return None
    if len(set_fields) < len(fields):
        missing = sorted(set(fields) - set(set_fields))
        raise click.ClickException(
            f"partial Auth0 config: {', '.join(missing)} not set. "
            "Set all of AUTH0_DOMAIN, AUTH0_CLIENT_ID, AUTH0_ALLOWED_EMAILS to enable "
            "the /api/* gate, or none to disable it."
        )
    try:
        return Auth0Settings.from_env(
            domain=cfg.auth0_domain,
            client_id=cfg.auth0_client_id,
            allowed_emails=cfg.auth0_allowed_emails,
        )
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc


def _github_webhook_settings(
    cfg: Config, repo_secrets: Mapping[str, str] | None = None
) -> GitHubWebhookSettings | None:
    enabled_bindings = [binding for binding in cfg.repos if binding.webhook_enabled]
    # Repo webhook secrets come from the DB-owned repo-secret view (SYM-194),
    # hot-swapped by the config write path. A legacy YAML topology (not yet
    # imported) still carries the secret on the binding itself, so merge those
    # in as a fallback for any repo the view doesn't cover — back-compat for
    # the pre-cutover daemon. Build the legacy fallback first and let the view
    # overwrite it per-repo: iterating `cfg.repos` in order and overwriting on
    # each match (rather than keeping only the first) preserves the pre-SYM-194
    # dict-comprehension's tie-break — the *last* binding wins — for a legacy
    # topology with multiple bindings on one repo that disagree on the secret
    # (SYM-194 review).
    merged: dict[str, str] = {}
    for binding in cfg.repos:
        if binding.webhook_secret:
            merged[binding.github_repo] = binding.webhook_secret
    merged.update(repo_secrets or {})
    if not enabled_bindings and not cfg.github_webhook_secret and not merged:
        return None
    try:
        return GitHubWebhookSettings(
            secret=cfg.github_webhook_secret,
            repo_secrets=merged,
            enabled_repos=frozenset(binding.github_repo for binding in enabled_bindings),
            dedupe_ttl_secs=cfg.webhook_dedupe_ttl_secs,
        )
    except ValueError as e:
        raise click.ClickException(str(e)) from e


def _live_github_webhook_settings(
    cfg: Config, repo_secrets: Mapping[str, str] | None = None
) -> GitHubWebhookSettings | None:
    """Like `_github_webhook_settings`, but for the per-request callable a
    DB-owned topology passes to the webhook router (SYM-189): a hot-reloaded
    edit that adds/enables a repo without a secret while
    `GITHUB_WEBHOOK_SECRET` is empty must not raise `click.ClickException`
    into a live request — that exception type is only meaningful at CLI
    boot. The router already treats a `None` settings provider as "disable
    every repo"; do that here too instead of crashing the request."""
    try:
        return _github_webhook_settings(cfg, repo_secrets)
    except click.ClickException as e:
        logging.getLogger(__name__).warning(
            "live github webhook settings invalid, disabling all repos until fixed: %s", e
        )
        return None


# Review-lane roles only spawn a subprocess when the binding's local review
# is enabled; the builder roles (implement/fix/accept) always can.
_REVIEW_LANE_ROLES: tuple[str, ...] = ("review_find", "review_verify")


def _binding_can_run_codex_cli(
    binding: RepoBinding, global_roles: Mapping[RoleName, Any] | None
) -> bool:
    """True when any role this binding can actually spawn resolves to codex.

    Resolves through the roles matrix (`binding.resolved_role`) — the same
    lookup every dispatch path uses — rather than the legacy `binding.agent`
    field alone, so a binding that selects codex only via `roles:` (e.g. a
    global `roles.implement.agent: codex`) still gets the `symphony-git`
    permissions profile provisioned. Review-lane roles are skipped when
    local review is off: their resolved agent defaults to the implementer's
    opposite family and would otherwise flag codex for bindings that never
    spawn it.
    """
    for name in get_args(RoleName):
        if name in _REVIEW_LANE_ROLES and not binding.resolved_local_review():
            continue
        if binding.resolved_role(name, global_roles).agent == "codex":
            return True
    return False


def _config_can_run_codex_cli(cfg: Config) -> bool:
    return any(_binding_can_run_codex_cli(binding, cfg.roles) for binding in cfg.repos)


def _config_has_linear_bindings(cfg: Config) -> bool:
    return any(binding.provider == "linear" for binding in cfg.repos)


def _tracker_context_for_binding(binding: RepoBinding) -> TrackerContext:
    return context_for_binding(binding)


async def _db_owns_topology(conn: aiosqlite.Connection, *, yaml_has_repos_topology: bool) -> bool:
    """Whether the DB is the topology source of truth: bindings exist now, the
    importer has migrated it before (`config_globals.migrated_at` set), or
    there is no YAML `repos:` topology to fall back to (a true fresh install).

    The marker matters on its own because a migrated DB can have zero
    *current* bindings — e.g. every binding was deleted via the UI — and
    leftover YAML `repos:` must stay ignored (not resolved/validated) in that
    case too, same as when bindings are still present. A true fresh install
    (no bindings, never migrated, and no YAML `repos:` either) must also
    count as DB-owned — otherwise `_run`'s tick-boundary reload (SYM-189)
    stays disabled forever and a binding added via the UI/importer after boot
    is never picked up without a restart. Only a not-yet-migrated legacy YAML
    deployment (bindings absent, `repos:` still present in the YAML) stays
    YAML-owned until the importer runs.
    """
    if await db.config_bindings.count(conn) > 0:
        return True
    globals_row = await db.config_globals.get(conn)
    if globals_row and globals_row.migrated_at:
        return True
    return not yaml_has_repos_topology


@asynccontextmanager
async def _configured_tracker_registry(
    cfg: Config,
) -> AsyncIterator[tuple[TrackerRegistry, Linear | None]]:
    secrets = Secrets()
    registry = TrackerRegistry()
    external_linear: Linear | None = None
    async with AsyncExitStack() as stack:
        for binding in cfg.repos:
            tracker = for_binding(binding, secrets, registry=registry)
            await stack.enter_async_context(cast(Any, tracker))
            if binding.provider == "linear" and external_linear is None:
                external_linear = cast(Linear, tracker)
                registry.register(DEFAULT_PROVIDER, DEFAULT_SITE, tracker)
        yield registry, external_linear


def _external_linear_tracker(trackers: TrackerRegistry) -> Linear | None:
    """Resolve the daemon's current default Linear tracker, live — a fresh
    DB-owned install can boot with no Linear binding and hot-add one later
    (SYM-189), so `create_app`'s external-snapshot service must re-resolve
    this on every call instead of a boot-time snapshot that stays `None`
    forever."""
    try:
        return cast(Linear, trackers.resolve(TrackerContext()))
    except KeyError:
        return None


@click.group(invoke_without_command=True)
@click.option(
    "--config",
    "config_path",
    type=click.Path(path_type=Path, exists=True),
    default=None,
    help="Path to config YAML.",
)
@click.option("--once", is_flag=True, help="Run one poll tick and exit.")
@click.pass_context
def main(ctx: click.Context, config_path: Path | None, once: bool) -> None:
    """symphony — headless Symphony port."""
    _setup_logging()
    if ctx.invoked_subcommand is None:
        if config_path is None:
            click.echo("--config is required when no subcommand is given", err=True)
            sys.exit(2)
        asyncio.run(_run(config_path, once=once))


def _enforce_require_auth0(cfg: Config) -> None:
    """Fail closed BEFORE any state mutation when a publicly-routed deploy
    (SYMPHONY_REQUIRE_AUTH0=1, set by docker-compose.coolify.yml) lacks Auth0
    config. The startup `reconcile` posts tracker comments and flips run rows,
    so the create_app-level guard alone would let a misconfigured public deploy
    mutate Linear/local state once per restart before dying.
    """
    if (
        cfg.ui.enabled
        and os.environ.get("SYMPHONY_REQUIRE_AUTH0", "").strip() not in ("", "0", "false")
        and _auth0_settings(cfg) is None
    ):
        click.echo(
            "SYMPHONY_REQUIRE_AUTH0 is set but AUTH0_DOMAIN/AUTH0_CLIENT_ID/"
            "AUTH0_ALLOWED_EMAILS are not configured — refusing to start a "
            "public deployment with an unauthenticated UI/API. Set all three "
            "in .env, or unset SYMPHONY_REQUIRE_AUTH0 for a local-only stack.",
            err=True,
        )
        sys.exit(2)


async def _run(config_path: Path, *, once: bool) -> None:
    conn = await db.connect(Config.peek_db_path(config_path))
    try:
        # A migrated deployment's leftover YAML `repos:` is ignored — don't
        # pay for (or risk boot-crashing on) validating/resolving it.
        yaml_has_repos_topology = Config.peek_repos_topology(config_path)
        db_owns_topology = await _db_owns_topology(
            conn, yaml_has_repos_topology=yaml_has_repos_topology
        )
        base = Config.load(config_path, resolve_repos=not db_owns_topology)
        try:
            cfg = await assemble_effective_config(
                conn, base, yaml_has_repos_topology=yaml_has_repos_topology
            )
        except ConfigBootError as e:
            click.echo(str(e), err=True)
            sys.exit(2)
        if _config_has_linear_bindings(cfg) and not cfg.linear_api_key:
            click.echo("LINEAR_API_KEY env var is empty; aborting", err=True)
            sys.exit(2)
        _enforce_require_auth0(cfg)
        async with _configured_tracker_registry(cfg) as (trackers, _):
            # When the DB owns topology, hot-apply binding edits at each tick
            # boundary (SYM-189). A reload introducing a provider/site the
            # process never saw at boot builds a real client from `Secrets`.
            reload_secrets = Secrets()

            def _hot_add_tracker(binding: RepoBinding) -> IssueTracker:
                return for_binding(binding, reload_secrets)

            # Live view of per-repo webhook secrets, built from the DB and
            # hot-swapped by the config write path so a secret set/replaced/
            # cleared through the UI reaches the verifier without a restart
            # (SYM-194). Loaded before the orchestrator so it can also be
            # refreshed on every binding reload tick — otherwise a secret row
            # written by `config-import` directly into the DB (not through the
            # CRUD hot-swap path) would need a restart to verify against on an
            # already-running DB-owned daemon (SYM-194 review fix).
            repo_secret_view = await db.config_repo_secrets.load_view(conn)
            orch = Orchestrator(
                cfg,
                trackers,
                conn,
                reload_bindings_from_db=db_owns_topology,
                tracker_factory=_hot_add_tracker if db_owns_topology else None,
                repo_secret_view=repo_secret_view if db_owns_topology else None,
            )
            await reconcile(conn, trackers, bindings=cfg.repos)
            if once:
                await orch.warmup()
                await orch._tick()  # pylint: disable=protected-access
                await orch.drain_dispatch_tasks()
                await orch.aclose_hot_added_trackers()
                return
            webhook_server: object | None = None
            webhook_task: asyncio.Task[None] | None = None
            github_webhook_settings = _github_webhook_settings(cfg, repo_secret_view.as_map())
            # A DB-owned topology can hot-enable a repo's webhook (or add a
            # binding needing one) at any later tick — the boot-time booleans
            # above can't see that yet, so a headless (`ui.enabled=False`)
            # deployment with nothing webhook-enabled at boot must still
            # start the HTTP surface, or a hot-enabled repo would need a
            # restart to get a live endpoint (SYM-189).
            if (
                cfg.linear_webhook_secret
                or github_webhook_settings
                or cfg.ui.enabled
                or db_owns_topology
            ):
                import uvicorn

                webhook_settings = (
                    WebhookSettings(
                        secret=cfg.linear_webhook_secret,
                        dedupe_ttl_secs=cfg.webhook_dedupe_ttl_secs,
                        timestamp_tolerance_secs=(cfg.webhook_timestamp_tolerance_secs),
                    )
                    if cfg.linear_webhook_secret
                    else None
                )
                app = create_app(
                    orch,
                    conn,
                    webhook_settings,
                    # A DB-owned topology hot-applies binding edits onto
                    # `orch.config` (SYM-189) — resolve the webhook settings
                    # from it on every request instead of baking in this
                    # boot-time snapshot, so an edited/added repo's
                    # enabled/secret state doesn't need a restart. The
                    # callable is always passed (even when nothing is
                    # webhook-enabled at boot) so a binding hot-added later
                    # doesn't need one either — the router itself no-ops
                    # (ignores every repo) when the resolved settings are
                    # `None`.
                    lambda: _live_github_webhook_settings(orch.config, repo_secret_view.as_map()),
                    ui_enabled=cfg.ui.enabled,
                    ui_db_path=cfg.db_path,
                    ui_log_root=cfg.log_root,
                    ui_status_thresholds=cfg.ui.status_stuck_thresholds.to_timedeltas(),
                    ui_external_config=lambda: orch.config,
                    ui_external_linear=lambda: _external_linear_tracker(trackers),
                    ui_external_github=cast(GitHubExternalClient, orch._gh),
                    ui_pr_no_progress_threshold=(
                        cfg.ui.status_stuck_thresholds.pr_no_progress_threshold()
                    ),
                    ui_command_sink=orch,
                    ui_pause_controller=orch,
                    ui_config_write_lock=orch.config_write_lock,
                    ui_repo_secret_view=repo_secret_view,
                    ui_db_owns_topology=db_owns_topology,
                    ui_webhook_public_url=os.environ.get("SYMPHONY_WEBHOOK_PUBLIC_URL"),
                    auth0_settings=_auth0_settings(cfg) if cfg.ui.enabled else None,
                )
                server = uvicorn.Server(
                    build_server_config(
                        app,
                        host=cfg.webhook_host,
                        port=cfg.webhook_port,
                    )
                )
                webhook_server = server
                webhook_task = asyncio.create_task(server.serve())
                logging.getLogger(__name__).info(
                    "http surface listening on %s:%d",
                    cfg.webhook_host,
                    cfg.webhook_port,
                )
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, lambda: asyncio.create_task(orch.shutdown()))
            try:
                await orch.run()
            finally:
                if webhook_server is not None and webhook_task is not None:
                    webhook_server.should_exit = True  # type: ignore[attr-defined]
                    await webhook_task
                await orch.aclose_hot_added_trackers()
    finally:
        await conn.close()


@main.command("config-import")
@click.option(
    "--config",
    "config_path",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    required=True,
    help="YAML topology to import (repos + roles).",
)
@click.option(
    "--replace",
    is_flag=True,
    help="Overwrite existing bindings (restore path). Without it, a second import is refused.",
)
@click.option(
    "--issue-bindings",
    "issue_bindings_path",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    default=None,
    help=(
        "JSON file mapping issue identifier → binding natural key "
        "([project_key, github_repo, issue_label, provider, site]) to attribute "
        "in-flight work the importer can't disambiguate automatically."
    ),
)
def config_import_cmd(config_path: Path, replace: bool, issue_bindings_path: Path | None) -> None:
    """One-off: import YAML repo bindings + roles matrix into the config DB."""
    import json

    _setup_logging()
    issue_bindings = (
        json.loads(issue_bindings_path.read_text()) if issue_bindings_path is not None else None
    )
    asyncio.run(_config_import(config_path, replace=replace, issue_bindings=issue_bindings))


async def _config_import(
    config_path: Path, *, replace: bool, issue_bindings: dict[str, Any] | None = None
) -> None:
    from datetime import UTC, datetime

    from .config_import import ConfigImportError, import_config

    # The importer re-validates the YAML itself (`Config.model_validate`,
    # no secrets); don't require a fully-resolvable `.env` just to import.
    db_path = Config.peek_db_path(config_path)
    conn = await db.connect(db_path)
    try:
        now = datetime.now(UTC).isoformat()
        result = await import_config(
            config_path, conn, replace=replace, now=now, issue_bindings=issue_bindings
        )
    except ConfigImportError as e:
        click.echo(str(e), err=True)
        sys.exit(2)
    finally:
        await conn.close()
    verb = "replaced" if result.replaced else "imported"
    click.echo(f"{verb} {result.bindings} binding(s) into {db_path}")
    if result.runs_backfilled or result.prs_backfilled:
        click.echo(
            f"backfilled binding keys onto {result.runs_backfilled} run(s) "
            f"and {result.prs_backfilled} open PR(s)"
        )


@main.command()
@click.option(
    "--config",
    "config_path",
    type=click.Path(path_type=Path, exists=True),
    required=True,
)
def preflight(config_path: Path) -> None:
    """Validate issue tracker auth and confirm configured states exist."""
    _setup_logging()
    asyncio.run(_preflight(config_path))


async def _preflight_configured_bindings(cfg: Config, trackers: TrackerRegistry) -> bool:
    visible_by_ctx: dict[TrackerContext, list[str]] = {}
    ok = True
    for binding in cfg.repos:
        ctx = _tracker_context_for_binding(binding)
        tracker = trackers.resolve(ctx)
        visible = visible_by_ctx.get(ctx)
        if visible is None:
            try:
                visible = await tracker.viewer_team_keys()
            except LinearError as e:
                click.echo(f"{binding.provider} auth failed: {e}", err=True)
                sys.exit(1)
            visible_by_ctx[ctx] = visible
            visible_label = (
                "linear teams" if binding.provider == "linear" else f"{binding.provider} projects"
            )
            click.echo(f"{visible_label} visible to this key: {visible}")
        if binding.project_key not in visible:
            click.echo(f"  ✗ {binding.project_key}: not visible — will be skipped at runtime")
            ok = False
            continue
        states = await tracker.team_states(binding.project_key)
        ready = binding.states.ready
        if not ready or ready not in states:
            click.echo(
                f"  ✗ {binding.project_key}: ready state "
                f"{ready!r} not in project workflow; "
                f"available: {sorted(states.keys())}"
            )
            ok = False
            continue
        required_states: list[tuple[str, str | None]] = [
            ("in_progress", binding.states.in_progress),
            ("needs_approval", binding.states.needs_approval),
            ("blocked", binding.states.blocked),
            ("waiting", binding.states.waiting),
            ("done", binding.states.done),
        ]
        if binding.resolved_local_review():
            required_states.append(("local_code_review", binding.states.local_code_review))
        if binding.resolved_remote_review():
            required_states.append(("code_review", binding.states.code_review))
        missing = [
            (role, name)
            for role, name in required_states
            if name is not None and (not name or name not in states)
        ]
        if missing:
            labels = [f"{role} state {name!r}" for role, name in missing]
            click.echo(
                f"  ✗ {binding.project_key}: missing states {labels}; "
                f"available: {sorted(states.keys())}"
            )
            ok = False
        else:
            click.echo(f"  ✓ {binding.project_key} → {binding.github_repo}: states ok")
    return ok


async def _preflight_validate_capabilities(cfg: Config) -> bool:
    """Validate each resolved `(model, effort)` pair against the live source.

    claude pairs are checked against the Models API `capabilities.effort`
    tree; codex pairs against the fixed family enum. This is the *online*
    check — preflight only. Daemon boot stays structural (`Config.load`'s
    family-enum check) and never queries the network.
    """
    # Each binding runs claude with the key the runner injects: its own `env:`
    # ANTHROPIC_API_KEY (resolved from `.env` by Config.load) takes precedence
    # over the process env, matching `{**os.environ, **spec.env}` in the runner.
    env_key = os.environ.get("ANTHROPIC_API_KEY", "")
    codex_pairs: set[tuple[str, str]] = set()
    # (key, model, effort, resolved_model); key "" means no API key is available
    # for that binding (claude runs via CLI auth). Keying by the resolved key
    # exercises every distinct binding key, so a present-but-broken one fails
    # rather than hiding behind another binding's valid key. `resolved_model` is
    # the alias resolved against *this binding's* env (see below) — it, not the
    # bare `model` alias, is what actually gets queried and cached.
    claude_checks: set[tuple[str, str, str, str]] = set()
    for binding in cfg.repos:
        # Presence, not truthiness: the runner merges `{**os.environ, **spec.env}`,
        # so a binding that sets ANTHROPIC_API_KEY — even to "" (typo/empty secret)
        # — overrides the process key for its subprocess. Inherit the process key
        # only when the binding omits it entirely; an empty override means the
        # binding really runs with no key (→ skipped below), not the parent's.
        if "ANTHROPIC_API_KEY" in binding.env:
            binding_key = binding.env["ANTHROPIC_API_KEY"]
        else:
            binding_key = env_key
        # Same merge the runner uses for the actual subprocess: a binding
        # pinning `ANTHROPIC_DEFAULT_SONNET_MODEL` etc. via its own `env:` runs
        # against that pin, not the process-wide var, so the alias must resolve
        # the same way here or this validates the wrong model (SYM-191 review).
        binding_env = {**os.environ, **binding.env}
        for name in get_args(RoleName):
            role = binding.resolved_role(name, cfg.roles)
            if role.effort is None or role.model is None:
                continue
            if role.agent == "codex":
                codex_pairs.add((role.model, role.effort))
            else:
                resolved_model = _resolve_alias_model_id(role.model, binding_env)
                claude_checks.add((binding_key, role.model, role.effort, resolved_model))

    ok = True
    for model, effort in sorted(codex_pairs):
        ok = _report_effort_support("codex", model, effort, sorted(SUPPORTED_CODEX_EFFORTS)) and ok

    # One Models API call per distinct (key, resolved_model): every binding key
    # AND every distinct alias pin is exercised at least once.
    caps_cache: dict[tuple[str, str], list[str] | None] = {}
    warned_models: set[str] = set()
    for key, model, effort, resolved_model in sorted(claude_checks):
        cache_key = (key, resolved_model)
        if cache_key not in caps_cache:
            caps_cache[cache_key] = await fetch_claude_effort_capabilities(resolved_model, key)
            if caps_cache[cache_key] is None and model not in warned_models:
                # No key for this binding — claude runs via CLI auth, so skip the
                # online effort check with a warning rather than hard-failing an
                # otherwise-valid deployment (the pair is still validated
                # structurally at Config.load). Warn once per model.
                warned_models.add(model)
                click.echo(
                    f"  ⚠ skipping claude model {model!r} effort validation: "
                    "ANTHROPIC_API_KEY not set (daemon uses CLI auth; not "
                    "required to run)",
                    err=True,
                )
        supported = caps_cache[cache_key]
        if supported is None:
            continue
        ok = _report_effort_support("claude", model, effort, supported) and ok
    return ok


def _report_effort_support(agent: str, model: str, effort: str, supported: list[str]) -> bool:
    """Echo a ✓/✗ line for one `(agent, model, effort)` pair; return True if ok."""
    if effort in supported:
        click.echo(f"  ✓ {agent} model {model!r} supports effort {effort!r}")
        return True
    click.echo(
        f"effort {effort!r} not supported by {agent} model {model!r}; "
        f"supported: {', '.join(supported)}",
        err=True,
    )
    return False


async def _preflight(config_path: Path) -> None:
    conn = await db.connect(Config.peek_db_path(config_path))
    try:
        yaml_has_repos_topology = Config.peek_repos_topology(config_path)
        db_owns_topology = await _db_owns_topology(
            conn, yaml_has_repos_topology=yaml_has_repos_topology
        )
        base = Config.load(config_path, resolve_repos=not db_owns_topology)
        cfg = await assemble_effective_config(
            conn,
            base,
            boot_gates=False,
            yaml_has_repos_topology=yaml_has_repos_topology,
        )
    except ConfigBootError as e:
        click.echo(str(e), err=True)
        sys.exit(2)
    finally:
        await conn.close()
    if _config_has_linear_bindings(cfg) and not cfg.linear_api_key:
        click.echo("LINEAR_API_KEY is empty", err=True)
        sys.exit(2)
    if cfg.ui.enabled:
        # Raises ClickException on a partial AUTH0_* env — surface that here
        # rather than at daemon startup.
        if _auth0_settings(cfg) is not None:
            click.echo("Auth0 config: ok, /api/* gate enabled")
        else:
            click.echo("Auth0 config: unset, /api/* is unauthenticated")
    if _config_can_run_codex_cli(cfg):
        click.echo(
            "codex runs use --dangerously-bypass-approvals-and-sandbox "
            "(container is the sandbox); no permissions profile needed"
        )
    else:
        click.echo("codex CLI not used by configured repos")
    try:
        # Structural binding checks first: they emit their findings before the
        # online capability check, so an ANTHROPIC_API_KEY gap can't mask them.
        async with _configured_tracker_registry(cfg) as (trackers, _):
            ok = await _preflight_configured_bindings(cfg, trackers)
        caps_ok = await _preflight_validate_capabilities(cfg)
    except ValueError as e:
        click.echo(str(e), err=True)
        sys.exit(2)
    sys.exit(0 if (ok and caps_ok) else 1)


@main.group()
def runs() -> None:
    """Inspect runs in the SQLite store. No orchestrator process required."""


@runs.command("ls")
@click.option(
    "--db",
    "db_path",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    required=True,
    help="Path to the symphony SQLite file.",
)
@click.option(
    "--limit",
    type=click.IntRange(min=1),
    default=50,
    help="Max rows to show.",
)
def runs_ls(db_path: Path, limit: int) -> None:
    """List active + recent runs."""
    asyncio.run(_runs_ls(db_path, limit))


async def _runs_ls(db_path: Path, limit: int) -> None:
    conn = await db.connect(db_path)
    try:
        rows = await db.runs.list_recent(conn, limit=limit)
    finally:
        await conn.close()
    if not rows:
        click.echo("(no runs)")
        return
    click.echo("id\tissue\tstage\tstatus\ttermination_kind\teff_tokens\tstarted_at")
    for r in rows:
        run = r.run
        termination_kind = (
            run.termination_kind if run.status in db.runs.TERMINAL_NON_SUCCESS_STATUSES else ""
        )
        eff = effective_tokens(
            run.input_tokens,
            run.output_tokens,
            run.cache_write_tokens,
            run.cache_read_tokens,
        )
        click.echo(
            f"{run.id}\t{r.identifier}\t{run.stage}\t{run.status}\t"
            f"{termination_kind}\t{eff:,.0f}\t{run.started_at}"
        )


@runs.command("show")
@click.argument("run_id")
@click.option(
    "--db",
    "db_path",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    required=True,
)
def runs_show(run_id: str, db_path: Path) -> None:
    """Show full detail for a single run."""
    asyncio.run(_runs_show(run_id, db_path))


async def _runs_show(run_id: str, db_path: Path) -> None:
    conn = await db.connect(db_path)
    try:
        rwi = await db.runs.get_with_issue(conn, run_id)
        if rwi is None:
            click.echo(f"run not found: {run_id}", err=True)
            sys.exit(1)
        cursor = await db.comment_cursors.get(conn, rwi.run.issue_id)
        history = await db.runs.history_for_issue(conn, rwi.run.issue_id)
    finally:
        await conn.close()
    run = rwi.run
    click.echo(f"id:             {run.id}")
    click.echo(f"issue:          {rwi.identifier} ({run.issue_id})")
    click.echo(f"stage:          {run.stage}")
    click.echo(f"status:         {run.status}")
    click.echo(f"pid:            {run.pid if run.pid is not None else '-'}")
    click.echo(f"started_at:     {run.started_at}")
    click.echo(f"ended_at:       {run.ended_at or '-'}")
    click.echo(f"input_tokens:   {run.input_tokens}")
    click.echo(f"output_tokens:  {run.output_tokens}")
    click.echo(f"cache_write:    {run.cache_write_tokens}")
    click.echo(f"cache_read:     {run.cache_read_tokens}")
    eff = effective_tokens(
        run.input_tokens,
        run.output_tokens,
        run.cache_write_tokens,
        run.cache_read_tokens,
    )
    click.echo(f"effective_tokens: {eff:,.0f}")
    click.echo(
        f"cost_usd:       {run.cost_usd}  (notional list-price estimate, not the actual bill)"
    )
    if run.status in db.runs.TERMINAL_NON_SUCCESS_STATUSES:
        click.echo(f"termination_kind:   {run.termination_kind or '-'}")
        returncode = run.exit_returncode if run.exit_returncode is not None else "-"
        click.echo(f"exit_returncode:    {returncode}")
        if run.termination_detail:
            click.echo("termination_detail:")
            for line in run.termination_detail.splitlines():
                click.echo(f"  {line}")
        else:
            click.echo("termination_detail: -")
    click.echo(f"comment cursor: {cursor or '-'}")
    click.echo("stage history:")
    for h in history:
        marker = "*" if h.id == run.id else " "
        click.echo(f"  {marker} {h.started_at}  {h.stage:<10}  {h.status:<11}  {h.id}")


@runs.command("local-review-trace")
@click.argument("issue_identifier")
@click.option(
    "--db",
    "db_path",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    required=True,
    help="Path to the symphony SQLite file.",
)
def runs_local_review_trace(issue_identifier: str, db_path: Path) -> None:
    """List local-review phases for a single issue.

    Postmortem tool. Accepts the Linear identifier (`ENG-123`) and
    prints each `stage='local_review'` row's status, effective
    tokens, and duration — most recent first. Run-row IDs are
    surfaced so the operator can `symphony runs show <id>` for full
    detail.
    """
    asyncio.run(_runs_local_review_trace(issue_identifier, db_path))


async def _runs_local_review_trace(issue_identifier: str, db_path: Path) -> None:
    conn = await db.connect(db_path)
    try:
        # Resolve identifier ("ENG-123") → issue_id.
        cur = await conn.execute(
            "SELECT id FROM issues WHERE identifier = ?",
            (issue_identifier,),
        )
        row = await cur.fetchone()
        if row is None:
            click.echo(
                f"no issue found with identifier {issue_identifier!r}",
                err=True,
            )
            sys.exit(1)
        issue_id = row[0]
        history = await db.runs.history_for_issue(conn, issue_id)
    finally:
        await conn.close()
    local_rows = [h for h in history if h.stage == "local_review"]
    if not local_rows:
        click.echo(f"no local-review runs recorded for {issue_identifier}")
        return
    # `history_for_issue` returns chronologically; reverse for "newest first".
    click.echo(f"local-review runs for {issue_identifier} ({len(local_rows)} total):")
    click.echo("started_at                       status        eff_tokens  duration  id")
    for h in reversed(local_rows):
        duration = _duration_secs(h.started_at, h.ended_at)
        duration_str = f"{duration:7.1f}s" if duration is not None else "      —"
        eff = effective_tokens(
            h.input_tokens,
            h.output_tokens,
            h.cache_write_tokens,
            h.cache_read_tokens,
        )
        click.echo(f"{h.started_at:<32} {h.status:<13} {eff:<10,.0f} {duration_str}  {h.id}")


def _duration_secs(started_at: str | None, ended_at: str | None) -> float | None:
    if not started_at or not ended_at:
        return None
    from datetime import datetime

    try:
        st = datetime.fromisoformat(started_at)
        en = datetime.fromisoformat(ended_at)
    except ValueError:
        return None
    return (en - st).total_seconds()


@runs.command("local-review-stats")
@click.option(
    "--db",
    "db_path",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    required=True,
    help="Path to the symphony SQLite file.",
)
def runs_local_review_stats(db_path: Path) -> None:
    """Aggregate local-review telemetry: approval rate, cost, duration.

    Answers "is local-review actually saving time?" without writing SQL.
    """
    asyncio.run(_runs_local_review_stats(db_path))


async def _runs_local_review_stats(db_path: Path) -> None:
    conn = await db.connect(db_path)
    try:
        stats = await db.runs.local_review_stats(conn)
    finally:
        await conn.close()
    finished = stats.completed_count + stats.failed_count
    click.echo(f"completed (APPROVED):    {stats.completed_count}")
    click.echo(f"failed (other):          {stats.failed_count}")
    click.echo(f"running (in-flight):     {stats.running_count}")
    click.echo(f"approval rate:           {stats.approval_rate:.1%}")
    eff = effective_tokens(
        stats.total_input_tokens,
        stats.total_output_tokens,
        stats.total_cache_write_tokens,
        stats.total_cache_read_tokens,
    )
    click.echo(f"total effective tokens:  {eff:,.0f}")
    click.echo("cost (notional list-price estimate, not the actual bill):")
    click.echo(f"  total cost:            ${stats.total_cost_usd:.4f}")
    click.echo(f"  avg cost per session:  ${stats.avg_cost_usd:.4f}")
    click.echo(f"avg duration per session: {stats.avg_duration_secs:.1f}s")
    if finished == 0:
        click.echo("(no finished local-review sessions yet)")


@runs.command("backfill-tokens")
@click.option(
    "--db",
    "db_path",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    required=True,
    help="Path to the symphony SQLite file.",
)
@click.option(
    "--log-root",
    type=click.Path(path_type=Path, exists=True, file_okay=False),
    required=True,
    help="Directory containing historical stdout logs.",
)
def runs_backfill_tokens(db_path: Path, log_root: Path) -> None:
    """Backfill per-run token columns from historical stdout logs."""
    from .db.token_backfill import run_backfill

    try:
        result = run_backfill(db_path=db_path, log_root=log_root)
    except (FileNotFoundError, NotADirectoryError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"updated: {result.updated}")
    click.echo(f"skipped: {result.skipped}")


@runs.command("backfill-model-usage")
@click.option(
    "--db",
    "db_path",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    required=True,
    help="Path to the symphony SQLite file.",
)
@click.option(
    "--log-root",
    type=click.Path(path_type=Path, exists=True, file_okay=False),
    required=True,
    help="Directory containing historical stdout logs.",
)
@click.option(
    "--config",
    "config_path",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    required=False,
    help="YAML system-knobs file, used as the legacy topology fallback for "
    "any team not yet migrated to the config DB. Per-team roles resolve "
    "from `--db`'s config tables when present there; without either "
    "source, Codex usage is attributed to `unknown`.",
)
def runs_backfill_model_usage(db_path: Path, log_root: Path, config_path: Path | None) -> None:
    """Backfill per-(provider, model) token attribution from historical logs."""
    from .db.token_backfill import run_model_usage_backfill

    try:
        codex_models_by_team = asyncio.run(_backfill_model_usage_codex_models(db_path, config_path))
    except ConfigBootError as e:
        raise click.ClickException(str(e)) from e

    try:
        result = run_model_usage_backfill(
            db_path=db_path,
            log_root=log_root,
            codex_models_by_team=codex_models_by_team or None,
        )
    except (FileNotFoundError, NotADirectoryError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"updated: {result.updated}")
    click.echo(f"skipped: {result.skipped}")


async def _backfill_model_usage_codex_models(
    db_path: Path, config_path: Path | None
) -> dict[str, CodexModels]:
    """Per-team Codex models for `backfill-model-usage`, from the same DB-backed
    effective config the daemon dispatches through (SYM-192 review).

    Reading a static `--config` YAML here would miss (or stale-attribute) any
    team whose roles now live only in the config DB, since a matrix edit made
    through the UI never touches the YAML file.
    """
    from .db.token_backfill import CodexModels

    conn = await db.connect(db_path)
    try:
        yaml_has_repos_topology = (
            Config.peek_repos_topology(config_path) if config_path is not None else False
        )
        db_owns_topology = await _db_owns_topology(
            conn, yaml_has_repos_topology=yaml_has_repos_topology
        )
        base = (
            Config.load(config_path, resolve_repos=not db_owns_topology)
            if config_path is not None
            else Config()
        )
        cfg = await assemble_effective_config(
            conn,
            base,
            boot_gates=False,
            yaml_has_repos_topology=yaml_has_repos_topology,
        )
    finally:
        await conn.close()

    codex_models_by_team: dict[str, CodexModels] = {}
    for binding in cfg.repos:
        codex_models_by_team.setdefault(
            binding.linear_team_key,
            CodexModels(
                implementer=binding.resolved_role("implement", cfg.roles).codex_model_arg(),
                fix=binding.resolved_role("fix", cfg.roles).codex_model_arg(),
                accept=binding.resolved_role("accept", cfg.roles).codex_model_arg(),
                reviewer=binding.resolved_role("review_find", cfg.roles).codex_model_arg(),
                verifier=binding.resolved_role("review_verify", cfg.roles).codex_model_arg(),
            ),
        )
    return codex_models_by_team


@main.command("local-review-dry-run")
@click.option(
    "--workspace",
    "workspace_path",
    type=click.Path(path_type=Path, exists=True, file_okay=False),
    required=True,
    help="Path to a checked-out git worktree to review.",
)
@click.option(
    "--base",
    "base_branch",
    default="main",
    help="Base branch to diff against (the reviewer reads "
    "`origin/<base>...HEAD`, with a silent fallback to `<base>...HEAD`).",
)
@click.option(
    "--reviewer",
    "reviewer_agent",
    type=click.Choice(["claude", "codex"]),
    default="codex",
    help="Reviewer agent CLI.",
)
@click.option(
    "--reviewer-model",
    "reviewer_codex_model",
    default=None,
    help="Codex model to use when --reviewer=codex. Omit to use the CLI's account default.",
)
@click.option(
    "--title",
    "issue_title",
    default="(dry-run; no issue title)",
    help="Issue title threaded into the prompt.",
)
@click.option(
    "--body",
    "issue_body",
    default="",
    help="Issue description threaded into the prompt.",
)
@click.option(
    "--label",
    "labels",
    multiple=True,
    help="Repeatable. Issue label(s) threaded into the prompt.",
)
@click.option(
    "--stall-secs",
    type=click.IntRange(min=5),
    default=300,
    help="Per-process stall timeout passed to the runner.",
)
def local_review_dry_run(
    workspace_path: Path,
    base_branch: str,
    reviewer_agent: str,
    reviewer_codex_model: str | None,
    issue_title: str,
    issue_body: str,
    labels: tuple[str, ...],
    stall_secs: int,
) -> None:
    """Run the local reviewer on a workspace without touching Linear/GitHub.

    Use this before enabling `local_review: true` on a binding in
    production: point at a real branch in a real workspace, supply the
    issue context, and eyeball the verdict + findings the reviewer
    produces. Nothing is written to SQLite, no Linear comments are
    posted, no `runs` row is created.
    """
    _setup_logging()
    asyncio.run(
        _local_review_dry_run(
            workspace_path=workspace_path,
            base_branch=base_branch,
            reviewer_agent=reviewer_agent,
            reviewer_codex_model=reviewer_codex_model,
            issue_title=issue_title,
            issue_body=issue_body,
            labels=list(labels),
            stall_secs=stall_secs,
        )
    )


async def _local_review_dry_run(
    *,
    workspace_path: Path,
    base_branch: str,
    reviewer_agent: str,
    reviewer_codex_model: str | None,
    issue_title: str,
    issue_body: str,
    labels: list[str],
    stall_secs: int,
) -> None:
    from .agent.codex_models import DEFAULT_CODEX_MODEL
    from .agent.runner import RunnerSpec
    from .pipeline.local_review import (
        build_local_review_command,
        local_review_prompt,
        parse_local_review_output,
    )
    from .pipeline.local_review_io import collect_runner_output

    prompt = local_review_prompt(
        issue_title=issue_title,
        issue_body=issue_body,
        labels=labels,
        base_branch=base_branch,
    )
    last_msg = workspace_path / ".symphony-dry-run-last.txt"
    if last_msg.exists():
        try:
            last_msg.unlink()
        except OSError:
            pass
    command = build_local_review_command(
        agent=reviewer_agent,  # type: ignore[arg-type]
        prompt=prompt,
        base_branch=base_branch,
        codex_model=reviewer_codex_model or DEFAULT_CODEX_MODEL,
        last_message_path=str(last_msg) if reviewer_agent == "codex" else None,
    )
    # Honour `--reviewer-model None` for codex by stripping --model so
    # the CLI uses the operator's account default — matches the iter-5
    # smoke harness convention.
    if reviewer_codex_model is None and reviewer_agent == "codex":
        if "--model" in command:
            idx = command.index("--model")
            del command[idx : idx + 2]
    spec = RunnerSpec(
        run_id="dry-run",
        workspace_path=workspace_path,
        command=command,
        stall_secs=stall_secs,
        stage="local_review",
    )
    runner = _DRY_RUN_RUNNER_FACTORY()
    click.echo(f"running {reviewer_agent} reviewer against {workspace_path}…")
    out = await collect_runner_output(runner, spec)
    last_text = (
        last_msg.read_text(encoding="utf-8", errors="replace") if last_msg.exists() else None
    )
    verdict = parse_local_review_output(
        agent=reviewer_agent,  # type: ignore[arg-type]
        stdout=out.stdout,
        head_sha="dry-run",  # signature is meaningless without persistence
        last_message_file=last_text,
    )
    click.echo("─" * 60)
    click.echo(f"verdict: {verdict.kind.value}")
    click.echo(f"terminal: {out.terminal_kind} (rc={out.returncode})")
    if verdict.findings:
        click.echo("findings:")
        click.echo(verdict.findings)
    elif verdict.raw_message:
        click.echo("raw message:")
        click.echo(verdict.raw_message)
    if last_msg.exists():
        try:
            last_msg.unlink()
        except OSError:
            pass


# Indirection so tests can inject a fake runner without monkeypatching
# the local-CLI imports. Production calls `LocalRunner()`.
def _default_dry_run_runner() -> Runner:  # noqa: F821 — forward ref
    from .agent.runners.local import LocalRunner

    return LocalRunner()


from .agent.runner import Runner  # noqa: E402 — placed here to match factory

_DRY_RUN_RUNNER_FACTORY: Callable[[], Runner] = _default_dry_run_runner


@main.command()
@click.argument("linear_id")
@click.option(
    "--config",
    "config_path",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    required=True,
)
def dispatch(linear_id: str, config_path: Path) -> None:
    """Hand-launch a run for a Linear issue, regardless of its state."""
    _setup_logging()
    asyncio.run(_dispatch(linear_id, config_path))


async def _dispatch(linear_id: str, config_path: Path) -> None:
    conn = await db.connect(Config.peek_db_path(config_path))
    try:
        yaml_has_repos_topology = Config.peek_repos_topology(config_path)
        db_owns_topology = await _db_owns_topology(
            conn, yaml_has_repos_topology=yaml_has_repos_topology
        )
        base = Config.load(config_path, resolve_repos=not db_owns_topology)
        if not base.linear_api_key:
            click.echo("LINEAR_API_KEY is empty", err=True)
            sys.exit(2)
        try:
            cfg = await assemble_effective_config(
                conn,
                base,
                boot_gates=False,
                yaml_has_repos_topology=yaml_has_repos_topology,
            )
        except ConfigBootError as e:
            click.echo(str(e), err=True)
            sys.exit(2)
        async with Linear(cfg.linear_api_key) as linear:
            try:
                issue = await linear.lookup_issue(linear_id)
            except LinearError as e:
                click.echo(f"linear lookup failed: {e}", err=True)
                sys.exit(1)
            binding = _resolve_binding(cfg, issue)
            if binding is None:
                sys.exit(1)
            orch = Orchestrator(cfg, linear, conn)
            run_id = await orch._dispatch_one(binding, issue)  # noqa: SLF001
            rwi = await db.runs.get_with_issue(conn, run_id) if run_id is not None else None
    finally:
        await conn.close()
    if run_id is None:
        click.echo(
            f"{issue.identifier} already has a running run; refusing to "
            f"start a duplicate. Inspect with `symphony runs ls`.",
            err=True,
        )
        sys.exit(1)
    if rwi is not None and rwi.run.status == "failed":
        click.echo(
            f"dispatch failed for {issue.identifier}; run {run_id} marked failed",
            err=True,
        )
        sys.exit(1)
    click.echo(f"dispatched {issue.identifier} → {binding.github_repo}")


if __name__ == "__main__":
    main()  # pragma: no cover
