"""TOML configuration loader with typed dataclasses and `$VAR` env indirection."""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, fields, is_dataclass
from pathlib import Path
from typing import Any, get_origin, get_type_hints


class ConfigError(Exception):
    """Raised when symphony.toml is missing, malformed, or has unexpected keys."""


@dataclass(frozen=True)
class RepoConfig:
    path: Path
    default_branch: str


@dataclass(frozen=True)
class GithubConfig:
    label: str


@dataclass(frozen=True)
class GitConfig:
    author_name: str
    author_email: str


@dataclass(frozen=True)
class OrchestratorConfig:
    poll_interval_s: int
    max_concurrent: int
    review_round_cap: int
    codex_renudge_after_min: int
    codex_giveup_after_min: int


@dataclass(frozen=True)
class AgentConfig:
    model: str
    max_turns: int


@dataclass(frozen=True)
class PathsConfig:
    worktree_root: Path
    prompts_dir: Path


@dataclass(frozen=True)
class Config:
    repo: RepoConfig
    github: GithubConfig
    git: GitConfig
    orchestrator: OrchestratorConfig
    agent: AgentConfig
    paths: PathsConfig


def _resolve_env(value: str) -> str:
    if not value.startswith("$"):
        return value
    name = value[1:]
    resolved = os.environ.get(name)
    if resolved is None:
        raise ConfigError(f"Environment variable {name!r} referenced in config is not set")
    return resolved


def _coerce(field_name: str, field_type: Any, raw: Any) -> Any:
    origin = get_origin(field_type)
    if origin is not None:
        # We don't use Optional/Union in this config; reject unexpected generics.
        raise ConfigError(f"Unsupported field type for {field_name!r}: {field_type!r}")

    if field_type is Path:
        if not isinstance(raw, str):
            raise ConfigError(f"Field {field_name!r} must be a string path, got {type(raw).__name__}")
        return Path(_resolve_env(raw))
    if field_type is str:
        if not isinstance(raw, str):
            raise ConfigError(f"Field {field_name!r} must be a string, got {type(raw).__name__}")
        return _resolve_env(raw)
    if field_type is int:
        if isinstance(raw, bool) or not isinstance(raw, int):
            raise ConfigError(f"Field {field_name!r} must be an int, got {type(raw).__name__}")
        return raw
    if field_type is bool:
        if not isinstance(raw, bool):
            raise ConfigError(f"Field {field_name!r} must be a bool, got {type(raw).__name__}")
        return raw

    raise ConfigError(f"Unsupported field type for {field_name!r}: {field_type!r}")


def _build_dataclass(cls: type, raw: Any, *, path: str) -> Any:
    if not isinstance(raw, dict):
        raise ConfigError(f"Section {path!r} must be a table")

    expected = {f.name: f.type for f in fields(cls)}
    extra = set(raw) - set(expected)
    if extra:
        raise ConfigError(f"Unknown keys in {path!r}: {sorted(extra)}")
    missing = set(expected) - set(raw)
    if missing:
        raise ConfigError(f"Missing keys in {path!r}: {sorted(missing)}")

    kwargs: dict[str, Any] = {}
    for name, ftype in expected.items():
        value = raw[name]
        if is_dataclass(ftype):
            kwargs[name] = _build_dataclass(ftype, value, path=f"{path}.{name}")
        else:
            kwargs[name] = _coerce(f"{path}.{name}", ftype, value)
    return cls(**kwargs)


def _resolve_dataclass_types(cls: type) -> None:
    """Resolve string annotations on a dataclass once, in-place.

    `from __future__ import annotations` defers type evaluation, so
    `field.type` is a string. We resolve it via `cls.__annotations__` so
    `_build_dataclass` can compare against actual types.
    """
    if getattr(cls, "_symphony_resolved", False):
        return
    hints = get_type_hints(cls)
    for f in fields(cls):
        f.type = hints[f.name]
    cls._symphony_resolved = True  # type: ignore[attr-defined]


def load_config(path: Path) -> Config:
    """Load `symphony.toml` from disk and return a typed :class:`Config`.

    - String values starting with ``$`` are resolved from the environment.
      An unset variable raises :class:`ConfigError`.
    - Unknown top-level or nested keys raise :class:`ConfigError`.
    - Relative paths under ``[paths]`` are resolved against the config file's
      directory so callers can pass ``cfg.paths.prompts_dir`` directly.
    """
    for cls in (
        RepoConfig,
        GithubConfig,
        GitConfig,
        OrchestratorConfig,
        AgentConfig,
        PathsConfig,
        Config,
    ):
        _resolve_dataclass_types(cls)

    if not path.is_file():
        raise ConfigError(f"Config file not found: {path}")

    with path.open("rb") as fh:
        try:
            raw = tomllib.load(fh)
        except tomllib.TOMLDecodeError as e:
            raise ConfigError(f"Invalid TOML in {path}: {e}") from e

    cfg = _build_dataclass(Config, raw, path="config")

    base = path.parent.resolve()
    paths = cfg.paths
    resolved_paths = PathsConfig(
        worktree_root=_resolve_relative(paths.worktree_root, base),
        prompts_dir=_resolve_relative(paths.prompts_dir, base),
    )
    return Config(
        repo=cfg.repo,
        github=cfg.github,
        git=cfg.git,
        orchestrator=cfg.orchestrator,
        agent=cfg.agent,
        paths=resolved_paths,
    )


def _resolve_relative(p: Path, base: Path) -> Path:
    if p.is_absolute():
        return p
    return (base / p).resolve()
