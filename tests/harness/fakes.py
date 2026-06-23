"""Object-level fakes implementing the orchestrator-facing interfaces.

`FakeLinear` implements the `IssueTracker` protocol; `FakeGitHub` implements the
subset of the `GitHub` client the orchestrator and reconciler actually call.
Both read and write the *same* `Sim` so there is a single canonical reality.

Wire-level correctness (GraphQL shapes, `gh` argv) stays in the real clients'
unit tests — this rig is about pipeline timing, not transport.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
from collections.abc import AsyncIterator, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from symphony.agent.runner import RunnerEvent, RunnerSpec
from symphony.github.client import CheckRun, GitHubError, PRChecks
from symphony.linear.client import LinearError
from symphony.tracker import Blocker, Comment, Issue

from .sim import PR_CLOSED, PR_MERGED, Sim, SimComment, SimPR


def _to_issue(issue: object) -> Issue:
    return Issue(
        id=issue.id,  # type: ignore[attr-defined]
        identifier=issue.identifier,  # type: ignore[attr-defined]
        title=issue.title,  # type: ignore[attr-defined]
        description=issue.description,  # type: ignore[attr-defined]
        url=issue.url,  # type: ignore[attr-defined]
        state_id=issue.state_id,  # type: ignore[attr-defined]
        state_name=issue.state_name,  # type: ignore[attr-defined]
        state_type=issue.state_type,  # type: ignore[attr-defined]
        team_key=issue.team_key,  # type: ignore[attr-defined]
        labels=list(issue.labels),  # type: ignore[attr-defined]
        blocked_by=list(issue.blocked_by),  # type: ignore[attr-defined]
        updated_at=issue.updated_at,  # type: ignore[attr-defined]
    )


def _to_comment(comment: SimComment) -> Comment:
    return Comment(
        id=comment.id,
        body=comment.body,
        created_at=comment.created_at,
        author_name=comment.author_name,
        author_is_me=comment.author_is_me,
        external_thread_type=comment.external_thread_type,
    )


class FakeRunner:
    """Deterministic `Runner` that never spawns real subprocesses.

    Each `run()` call pops one event sequence from `_queue`; if the queue is
    empty it yields a default sequence that satisfies the implement completion
    gate (SYMPHONY_DONE marker + HEAD advance) for implement-stage runs and a
    bare success exit for all other stages.
    """

    def __init__(self) -> None:
        self._queue: list[list[RunnerEvent]] = []

    def enqueue(self, events: list[RunnerEvent]) -> None:
        """Pre-program the event sequence for the next `run()` call."""
        self._queue.append(events)

    def run(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        if self._queue:
            return self._aiter(self._queue.pop(0))
        return self._default_aiter(spec)

    async def _aiter(self, events: list[RunnerEvent]) -> AsyncIterator[RunnerEvent]:
        for ev in events:
            yield ev

    async def _default_aiter(self, spec: RunnerSpec) -> AsyncIterator[RunnerEvent]:
        # Merge-stage runs must not commit: the PR head must stay at the
        # already-reviewed SHA so _merge_approved_pr sees a consistent headRefOid.
        if spec.stage != "merge":
            env = {
                **os.environ,
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_AUTHOR_NAME": "sim",
                "GIT_AUTHOR_EMAIL": "sim@test",
                "GIT_COMMITTER_NAME": "sim",
                "GIT_COMMITTER_EMAIL": "sim@test",
            }
            proc = await asyncio.create_subprocess_exec(
                "git", "-C", str(spec.workspace_path),
                "commit", "--allow-empty", "-m", f"fake: {spec.stage or 'run'}",
                env=env,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await proc.wait()
        # Emit claude stream-json result event so _read_run_final_message
        # extracts "SYMPHONY_DONE" via _claude_last_result_text.
        yield RunnerEvent(kind="stdout", line='{"type":"result","result":"SYMPHONY_DONE"}')
        yield RunnerEvent(kind="exit", returncode=0)

    async def kill(self, run_id: str) -> None:
        pass


# Re-export so tests can import Runner from this module without needing
# the runner module directly.
__all__ = ["FakeGitHub", "FakeLinear", "FakeRunner"]


class FakeLinear:
    """Backs the `IssueTracker` protocol with a single `Sim`."""

    def __init__(self, sim: Sim) -> None:
        self._sim = sim

    async def viewer_team_keys(self) -> list[str]:
        return list(self._sim.viewer_teams)

    async def team_states(self, team_key: str) -> dict[str, str]:
        return dict(self._sim.states.get(team_key, {}))

    async def lookup_issue(self, identifier_or_uuid: str) -> Issue:
        issue = self._sim.issues.get(identifier_or_uuid)
        if issue is None:
            for candidate in self._sim.issues.values():
                if candidate.identifier == identifier_or_uuid:
                    issue = candidate
                    break
        if issue is None:
            raise LinearError(f"issue not found: {identifier_or_uuid}")
        return _to_issue(issue)

    async def issues_in_state(
        self, team_key: str, state_name: str, label: str | None = None
    ) -> Sequence[Issue]:
        return [
            _to_issue(issue)
            for issue in self._sim.issues.values()
            if issue.team_key == team_key
            and issue.state_name == state_name
            and (label is None or label in issue.labels)
        ]

    async def comments_since(
        self, issue_uuid: str, after: datetime
    ) -> Sequence[Comment]:
        out: list[Comment] = []
        for comment in self._sim.comments.get(issue_uuid, []):
            if datetime.fromisoformat(comment.created_at) >= after:
                out.append(_to_comment(comment))
        return out

    async def post_comment(self, issue_uuid: str, body: str) -> str:
        comment = SimComment(
            id=self._sim.next_comment_id(),
            issue_id=issue_uuid,
            body=body,
            created_at=self._sim.now_iso(),
            author_name="symphony",
            author_is_me=True,
        )
        self._sim.comments.setdefault(issue_uuid, []).append(comment)
        return comment.id

    async def move_issue(self, issue_id_or_identifier: str, state_id: str) -> None:
        issue = self._sim.issues.get(issue_id_or_identifier)
        if issue is None:
            for candidate in self._sim.issues.values():
                if candidate.identifier == issue_id_or_identifier:
                    issue = candidate
                    break
        if issue is None:
            raise LinearError(f"no such issue: {issue_id_or_identifier}")
        issue.state_id = state_id
        name = self._sim.state_name_for_id(issue.team_key, state_id)
        if name is not None:
            issue.state_name = name
        stype = self._sim.state_type_for_id(issue.team_key, state_id)
        if stype is not None:
            issue.state_type = stype

    async def upload_issue_attachment(
        self, *, issue_uuid: str, path: Path, title: str
    ) -> str:
        return f"https://sim.invalid/attachments/{issue_uuid}/{title}"


class FakeGitHub:
    """Backs the orchestrator-facing subset of `GitHub` with a single `Sim`."""

    def __init__(self, sim: Sim) -> None:
        self._sim = sim
        # (repo, number) → list of review dicts
        self._reviews: dict[tuple[str, int], list[dict[str, Any]]] = {}
        # (repo, number) → list of issue comment dicts
        self._pr_comments: dict[tuple[str, int], list[dict[str, Any]]] = {}
        # sha → ISO timestamp recorded at PR creation time
        self._commit_timestamps: dict[str, str] = {}

    def add_pr_review(
        self,
        pr: int | str,
        *,
        repo: str,
        state: str = "APPROVED",
        author: str = "reviewer",
    ) -> None:
        """Seed a review signal so `pr_reviews()` returns it."""
        number = int(pr)
        sim_pr = self._sim.prs.get((repo, number))
        commit_id = sim_pr.head_sha if sim_pr is not None else ""
        self._reviews.setdefault((repo, number), []).append(
            {"state": state, "user": {"login": author}, "commit_id": commit_id}
        )

    def _pr(self, pr: int | str, repo: str | None) -> SimPR | None:
        number = int(pr)
        if repo is not None:
            return self._sim.prs.get((repo, number))
        for (_repo, num), sim_pr in self._sim.prs.items():
            if num == number:
                return sim_pr
        return None

    async def repo_clone(self, repo: str, dest: Path) -> None:
        """Create a minimal real git repo so workspace git operations succeed."""
        env = {**os.environ, "GIT_CONFIG_NOSYSTEM": "1"}
        git_env = {**env, "GIT_AUTHOR_NAME": "sim", "GIT_AUTHOR_EMAIL": "sim@test",
                   "GIT_COMMITTER_NAME": "sim", "GIT_COMMITTER_EMAIL": "sim@test"}
        # Create a bare origin so `git push -u origin <branch>` succeeds.
        origin = dest.parent / (dest.name + "-origin.git")
        origin.mkdir(parents=True, exist_ok=True)
        init_bare = await asyncio.create_subprocess_exec(
            "git", "init", "--bare", str(origin),
            env=env, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        await init_bare.wait()
        dest.mkdir(parents=True, exist_ok=True)
        init = await asyncio.create_subprocess_exec(
            "git", "init", "-b", "main", str(dest),
            env=env, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        await init.wait()
        commit = await asyncio.create_subprocess_exec(
            "git", "-C", str(dest), "commit", "--allow-empty", "-m", "init",
            env=git_env, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        await commit.wait()
        remote = await asyncio.create_subprocess_exec(
            "git", "-C", str(dest), "remote", "add", "origin", str(origin),
            env=env, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        await remote.wait()
        push = await asyncio.create_subprocess_exec(
            "git", "-C", str(dest), "push", "-u", "origin", "main",
            env=env, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        await push.wait()

    async def repo_default_branch(self, repo: str) -> str:
        return "main"

    async def open_pr_for_head(
        self, *, head: str, repo: str | None = None
    ) -> dict[str, Any] | None:
        for (pr_repo, _num), pr in self._sim.prs.items():
            if pr.head == head and (repo is None or pr_repo == repo):
                if pr.state != PR_MERGED and pr.state != PR_CLOSED:
                    return {"number": pr.number, "url": pr.url}
        return None

    async def pr_for_head(self, *, head: str, repo: str | None = None) -> str | None:
        result = await self.open_pr_for_head(head=head, repo=repo)
        return None if result is None else str(result["url"])

    async def ensure_pr(
        self,
        *,
        title: str,
        body: str,
        head: str,
        base: str | None = None,
        repo: str | None = None,
        linear_url: str | None = None,
        draft: bool = False,
    ) -> str:
        for (pr_repo, _num), pr in self._sim.prs.items():
            if pr.head == head and (repo is None or pr_repo == repo):
                if pr.state != PR_MERGED and pr.state != PR_CLOSED:
                    return pr.url
        number = self._sim.next_pr_number()
        key_repo = repo or ""
        url = f"https://github.invalid/{key_repo}/pull/{number}"
        issue_id = ""
        if linear_url:
            for issue in self._sim.issues.values():
                if issue.url == linear_url:
                    issue_id = issue.id
                    break
        # Prefer the SHA already pushed to the fake origin; fall back to a
        # deterministic fabrication only when no push has been recorded.
        head_sha = (
            self._sim.branch_head_shas.get(head)
            or hashlib.sha1(f"{key_repo}:{head}:{number}".encode()).hexdigest()
        )
        self._commit_timestamps.setdefault(head_sha, self._sim.now_iso())
        self._sim.prs[(key_repo, number)] = SimPR(
            repo=key_repo,
            number=number,
            head=head,
            base=base or "main",
            title=title,
            url=url,
            issue_id=issue_id,
            head_sha=head_sha,
        )
        return url

    async def pr_view(
        self,
        pr: int | str,
        *,
        repo: str | None = None,
        include_status_checks: bool = False,
    ) -> dict[str, Any]:
        sim_pr = self._pr(pr, repo)
        if sim_pr is None:
            raise GitHubError(f"no such PR: {pr}")
        # Simulate GitHub auto-merge: when auto-merge is queued and checks pass,
        # GitHub merges the PR in the background before the next poll.
        # Closed PRs are never auto-merged (they were closed before checks passed).
        if (
            sim_pr.auto_merge_enabled
            and sim_pr.checks_passed
            and not sim_pr.merged
            and sim_pr.state != PR_CLOSED
        ):
            sim_pr.state = PR_MERGED
            sim_pr.merged_at = self._sim.now_iso()
        checks_ok = sim_pr.checks_passed
        merge_state = "CLEAN" if checks_ok else "BLOCKED"
        view: dict[str, Any] = {
            "number": sim_pr.number,
            "title": sim_pr.title,
            "state": "MERGED" if sim_pr.merged else sim_pr.state.upper(),
            "url": sim_pr.url,
            "headRefName": sim_pr.head,
            "headRefOid": sim_pr.head_sha,
            "baseRefName": sim_pr.base,
            "mergeable": "MERGEABLE",
            "mergeStateStatus": merge_state,
            "isDraft": False,
            "mergedAt": sim_pr.merged_at,
        }
        if include_status_checks:
            if checks_ok:
                check_node = {
                    "__typename": "CheckRun",
                    "name": "ci",
                    "status": "COMPLETED",
                    "conclusion": "SUCCESS",
                }
            else:
                check_node = {
                    "__typename": "CheckRun",
                    "name": "ci",
                    "status": "COMPLETED",
                    "conclusion": "FAILURE",
                }
            view["statusCheckRollup"] = {"nodes": [check_node]}
        return view

    async def pr_comment(
        self, pr: int | str, body: str, *, repo: str | None = None
    ) -> None:
        sim_pr = self._pr(pr, repo)
        if sim_pr is None:
            raise GitHubError(f"no such PR: {pr}")
        key = (sim_pr.repo, sim_pr.number)
        self._pr_comments.setdefault(key, []).append(
            {"body": body, "user": {"login": "symphony"}, "created_at": self._sim.now_iso()}
        )

    async def pr_diff(self, pr: int | str, *, repo: str | None = None) -> str:
        return ""

    async def pr_checks(self, pr: int | str, *, repo: str | None = None) -> PRChecks:
        sim_pr = self._pr(pr, repo)
        if sim_pr is None:
            raise GitHubError(f"no such PR: {pr}")
        if sim_pr.checks_passed:
            return PRChecks(runs=[])
        return PRChecks(
            runs=[CheckRun(name="ci", state="FAILURE", bucket="fail")]
        )

    async def pr_review_comments(
        self, pr: int | str, *, repo: str
    ) -> list[dict[str, Any]]:
        return []

    async def pr_reviews(self, pr: int | str, *, repo: str) -> list[dict[str, Any]]:
        return list(self._reviews.get((repo, int(pr)), []))

    async def pr_issue_comments(
        self, pr: int | str, *, repo: str
    ) -> list[dict[str, Any]]:
        return list(self._pr_comments.get((repo, int(pr)), []))

    async def pr_reactions(self, pr: int | str, *, repo: str) -> list[dict[str, Any]]:
        return []

    async def commit_committed_at(self, repo: str, sha: str) -> str:
        if sha not in self._commit_timestamps:
            self._commit_timestamps[sha] = self._sim.now_iso()
        return self._commit_timestamps[sha]

    async def check_log_tail(self, check: object, **kwargs: object) -> str:
        return ""

    async def run_failed_log_tail(
        self, run_id: int | str, **kwargs: object
    ) -> str:
        return ""

    async def pr_merge(
        self,
        pr: int | str,
        *,
        strategy: str,
        auto: bool = False,
        repo: str | None = None,
    ) -> None:
        sim_pr = self._pr(pr, repo)
        if sim_pr is None:
            raise GitHubError(f"no such PR: {pr}")
        if sim_pr.state == PR_CLOSED:
            raise GitHubError(f"PR {pr} is already closed")
        if auto and not sim_pr.checks_passed:
            # Queue auto-merge; GitHub merges later when checks go green.
            sim_pr.auto_merge_enabled = True
        elif not sim_pr.checks_passed:
            raise GitHubError(f"PR {pr} cannot be merged: required status checks have not passed")
        else:
            sim_pr.state = PR_MERGED
            sim_pr.merged_at = self._sim.now_iso()
