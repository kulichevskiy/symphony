import json
from pathlib import Path

import pytest

from symphony.bench.github import CommandError, GitHubSandbox


class RecordingCommands:
    def __init__(self, responses: dict[str, str] | None = None) -> None:
        self.calls: list[tuple[tuple[str, ...], Path, dict[str, str], str | None]] = []
        self.responses = responses or {}

    async def run(
        self,
        argv: list[str],
        *,
        cwd: Path,
        env: dict[str, str] | None = None,
        stdin: str | None = None,
    ) -> str:
        self.calls.append((tuple(argv), cwd, env or {}, stdin))
        if len(argv) == 3 and argv[:2] == ["gh", "api"] and argv[2].count("/") == 2:
            raise CommandError("HTTP 404")
        return self.responses.get(argv[-1], "")


class FreePrivateRepositoryCommands(RecordingCommands):
    async def run(
        self,
        argv: list[str],
        *,
        cwd: Path,
        env: dict[str, str] | None = None,
        stdin: str | None = None,
    ) -> str:
        result = await super().run(argv, cwd=cwd, env=env, stdin=stdin)
        if any("branches/main/protection" in arg for arg in argv):
            raise CommandError(
                "Upgrade to GitHub Pro or make this repository public to enable this "
                "feature. (HTTP 403)"
            )
        return result


@pytest.mark.asyncio
async def test_github_sandbox_creates_private_protected_repo(tmp_path: Path) -> None:
    commands = RecordingCommands()
    sandbox = GitHubSandbox(owner="kulichevskiy", token="github-token", commands=commands)

    result = await sandbox.create_repository(name="EXP-1-A1", source=tmp_path)

    assert result.slug == "kulichevskiy/EXP-1-A1"
    assert result.url == "https://github.com/kulichevskiy/EXP-1-A1"
    assert [call[0] for call in commands.calls[:5]] == [
        ("git", "init", "--initial-branch=main"),
        ("git", "add", "."),
        ("git", "commit", "-m", "Seed Feedback Inbox benchmark"),
        ("gh", "api", "repos/kulichevskiy/EXP-1-A1"),
        (
            "gh",
            "repo",
            "create",
            "kulichevskiy/EXP-1-A1",
            "--private",
        ),
    ]
    assert commands.calls[6][0] == (
        "git",
        "remote",
        "add",
        "origin",
        "https://github.com/kulichevskiy/EXP-1-A1.git",
    )
    assert commands.calls[7][0] == ("git", "push", "--set-upstream", "origin", "main")
    assert commands.calls[8][0] == (
        "gh",
        "repo",
        "edit",
        "kulichevskiy/EXP-1-A1",
        "--enable-auto-merge",
    )
    protection = commands.calls[9]
    assert protection[0] == (
        "gh",
        "api",
        "--method",
        "PUT",
        "repos/kulichevskiy/EXP-1-A1/branches/main/protection",
        "--input",
        "-",
    )
    assert protection[2] == {"GH_TOKEN": "github-token"}
    assert '"backend"' in (protection[3] or "")
    assert '"frontend"' in (protection[3] or "")


@pytest.mark.asyncio
async def test_github_sandbox_allows_plan_limited_private_repo(tmp_path: Path) -> None:
    commands = FreePrivateRepositoryCommands()
    sandbox = GitHubSandbox(owner="kulichevskiy", token="github-token", commands=commands)

    result = await sandbox.create_repository(name="EXP-1-A1", source=tmp_path)

    assert result.slug == "kulichevskiy/EXP-1-A1"
    assert any(any("branches/main/protection" in arg for arg in call[0]) for call in commands.calls)


@pytest.mark.asyncio
async def test_github_sandbox_archives_repository(tmp_path: Path) -> None:
    commands = RecordingCommands()
    sandbox = GitHubSandbox(owner="kulichevskiy", token="github-token", commands=commands)

    await sandbox.archive_repository(repository_slug="kulichevskiy/EXP-1-A1", cwd=tmp_path)

    assert commands.calls == [
        (
            (
                "gh",
                "api",
                "--method",
                "PATCH",
                "repos/kulichevskiy/EXP-1-A1",
                "--input",
                "-",
            ),
            tmp_path,
            {"GH_TOKEN": "github-token"},
            '{"archived":true}',
        )
    ]


@pytest.mark.asyncio
async def test_review_metrics_count_codex_comments_by_priority(tmp_path: Path) -> None:
    commands = RecordingCommands(
        responses={
            "repos/kulichevskiy/trial/pulls/comments?per_page=100": json.dumps(
                [
                    [
                        {
                            "user": {"login": "chatgpt-codex-connector[bot]"},
                            "body": (
                                "**<sub><sub>![P1 Badge](https://img.shields.io/badge/"
                                "P1-orange?style=flat)</sub></sub> Race allows overbooking"
                            ),
                        },
                        {
                            "user": {"login": "chatgpt-codex-connector[bot]"},
                            "body": "Malformed inline finding without a priority",
                        },
                        {
                            "user": {"login": "chatgpt-codex-connector[bot]"},
                            "body": None,
                        },
                        {"user": {"login": "human"}, "body": "[P0] Ignore me"},
                    ],
                    [
                        {
                            "user": {"login": "chatgpt-codex-connector[bot]"},
                            "body": (
                                "**<sub><sub>![P2 Badge](https://img.shields.io/badge/"
                                "P2-yellow?style=flat)</sub></sub> Missing validation"
                            ),
                        }
                    ],
                ]
            ),
            "repos/kulichevskiy/trial/issues/comments?per_page=100": json.dumps(
                [
                    [
                        {
                            "user": {"login": "codex[bot]"},
                            "body": "Review found no major issues",
                        },
                        {
                            "user": {"login": "codex[bot]"},
                            "body": "[P2] Replay mutates state",
                        },
                        {
                            "user": {"login": "reviewer"},
                            "body": "@codex review",
                        },
                        {
                            "user": {"login": "reviewer"},
                            "body": "  @Codex   review  ",
                        },
                        {
                            "user": {"login": "reviewer"},
                            "body": "Please run @codex review when ready",
                        },
                    ]
                ]
            ),
        }
    )
    sandbox = GitHubSandbox(owner="kulichevskiy", token="token", commands=commands)

    metrics = await sandbox.review_metrics(repository_slug="kulichevskiy/trial", cwd=tmp_path)

    assert all(call[1] == tmp_path for call in commands.calls)

    assert metrics == {
        "remote_review_rounds": 2,
        "remote_review_comments": 5,
        "remote_review_unclassified": 1,
        "remote_review_unparseable": 1,
        "remote_review_p0": 0,
        "remote_review_p1": 1,
        "remote_review_p2": 2,
        "remote_review_p3": 0,
    }
