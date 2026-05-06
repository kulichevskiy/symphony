from pathlib import Path

import pytest

from symphony.config import (
    AgentConfig,
    Config,
    ConfigError,
    GitConfig,
    GithubConfig,
    OrchestratorConfig,
    PathsConfig,
    RepoConfig,
    load_config,
)


SAMPLE_TOML = """
[repo]
path = "/tmp/some-project"
default_branch = "main"

[github]
label = "auto"

[git]
author_name = "Symphony"
author_email = "alexey.kulichevskiy+symphony@adjust.com"

[orchestrator]
poll_interval_s = 60
max_concurrent = 3
review_round_cap = 10
codex_renudge_after_min = 10
codex_giveup_after_min = 30

[agent]
model = "claude-opus-4-7"
max_turns = 50

[paths]
worktree_root = "/tmp/symphony-worktrees"
prompts_dir = "./prompts"
"""


def write_config(tmp_path: Path, body: str = SAMPLE_TOML) -> Path:
    p = tmp_path / "symphony.toml"
    p.write_text(body)
    return p


def test_load_config_returns_typed_dataclass(tmp_path):
    cfg = load_config(write_config(tmp_path))
    assert isinstance(cfg, Config)
    assert isinstance(cfg.repo, RepoConfig)
    assert isinstance(cfg.github, GithubConfig)
    assert isinstance(cfg.git, GitConfig)
    assert isinstance(cfg.orchestrator, OrchestratorConfig)
    assert isinstance(cfg.agent, AgentConfig)
    assert isinstance(cfg.paths, PathsConfig)


def test_load_config_field_values(tmp_path):
    cfg = load_config(write_config(tmp_path))
    assert cfg.repo.path == Path("/tmp/some-project")
    assert cfg.repo.default_branch == "main"
    assert cfg.github.label == "auto"
    assert cfg.git.author_name == "Symphony"
    assert cfg.git.author_email == "alexey.kulichevskiy+symphony@adjust.com"
    assert cfg.orchestrator.poll_interval_s == 60
    assert cfg.orchestrator.max_concurrent == 3
    assert cfg.orchestrator.review_round_cap == 10
    assert cfg.orchestrator.codex_renudge_after_min == 10
    assert cfg.orchestrator.codex_giveup_after_min == 30
    assert cfg.agent.model == "claude-opus-4-7"
    assert cfg.agent.max_turns == 50
    assert cfg.paths.worktree_root == Path("/tmp/symphony-worktrees")
    assert cfg.paths.prompts_dir == (tmp_path / "prompts").resolve()


def test_load_config_resolves_prompts_dir_relative_to_config_file(tmp_path):
    cfg = load_config(write_config(tmp_path))
    # prompts_dir as written ("./prompts") should resolve relative to the
    # config file's parent so callers can use cfg.paths.prompts_dir directly.
    assert cfg.paths.prompts_dir == (tmp_path / "prompts").resolve()


def test_load_config_resolves_relative_repo_path_to_config_file(tmp_path):
    """Regression: a relative `[repo].path` must resolve against the config
    file's directory so service/cron launches from another CWD still target
    the right repo (gh/git operations all key off this path).
    """
    body = SAMPLE_TOML.replace(
        'path = "/tmp/some-project"', 'path = "./some-project"'
    )
    p = write_config(tmp_path, body)
    cfg = load_config(p)
    assert cfg.repo.path == (tmp_path / "some-project").resolve()


def test_load_config_keeps_absolute_repo_path(tmp_path):
    cfg = load_config(write_config(tmp_path))
    assert cfg.repo.path == Path("/tmp/some-project")


def test_load_config_missing_section_raises(tmp_path):
    body = SAMPLE_TOML.replace("[github]\nlabel = \"auto\"\n", "")
    p = write_config(tmp_path, body)
    with pytest.raises(ConfigError):
        load_config(p)


def test_load_config_unknown_top_level_key_raises(tmp_path):
    body = SAMPLE_TOML + '\n[mystery]\nfoo = "bar"\n'
    p = write_config(tmp_path, body)
    with pytest.raises(ConfigError):
        load_config(p)


def test_load_config_unknown_nested_key_raises(tmp_path):
    body = SAMPLE_TOML.replace(
        "[agent]\nmodel = \"claude-opus-4-7\"\nmax_turns = 50",
        "[agent]\nmodel = \"claude-opus-4-7\"\nmax_turns = 50\nrogue = true",
    )
    p = write_config(tmp_path, body)
    with pytest.raises(ConfigError):
        load_config(p)


def test_load_config_env_var_indirection(tmp_path, monkeypatch):
    monkeypatch.setenv("SYMPHONY_BOT_EMAIL", "bot@example.com")
    body = SAMPLE_TOML.replace(
        'author_email = "alexey.kulichevskiy+symphony@adjust.com"',
        'author_email = "$SYMPHONY_BOT_EMAIL"',
    )
    p = write_config(tmp_path, body)
    cfg = load_config(p)
    assert cfg.git.author_email == "bot@example.com"


def test_load_config_unset_env_var_raises(tmp_path, monkeypatch):
    monkeypatch.delenv("SYMPHONY_BOT_EMAIL_MISSING", raising=False)
    body = SAMPLE_TOML.replace(
        'author_email = "alexey.kulichevskiy+symphony@adjust.com"',
        'author_email = "$SYMPHONY_BOT_EMAIL_MISSING"',
    )
    p = write_config(tmp_path, body)
    with pytest.raises(ConfigError):
        load_config(p)


def test_load_config_file_not_found(tmp_path):
    with pytest.raises(ConfigError):
        load_config(tmp_path / "nope.toml")
