"""Object-level fakes implementing the orchestrator-facing interfaces.

`FakeLinear` implements the `IssueTracker` protocol; `FakeGitHub` implements the
subset of the `GitHub` client the orchestrator and reconciler actually call.
Both read and write the *same* `Sim` so there is a single canonical reality.

Wire-level correctness (GraphQL shapes, `gh` argv) stays in the real clients'
unit tests — this rig is about pipeline timing, not transport.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from symphony.github.client import CheckRun, PRChecks
from symphony.tracker import Comment, Issue

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
            raise KeyError(f"no such issue: {identifier_or_uuid}")
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
            if datetime.fromisoformat(comment.created_at) > after:
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
            raise KeyError(f"no such issue: {issue_id_or_identifier}")
        issue.state_id = state_id
        name = self._sim.state_name_for_id(issue.team_key, state_id)
        if name is not None:
            issue.state_name = name

    async def upload_issue_attachment(
        self, *, issue_uuid: str, path: Path, title: str
    ) -> str:
        return f"https://sim.invalid/attachments/{issue_uuid}/{title}"


class FakeGitHub:
    """Backs the orchestrator-facing subset of `GitHub` with a single `Sim`."""

    def __init__(self, sim: Sim) -> None:
        self._sim = sim

    def _pr(self, pr: int | str, repo: str | None) -> SimPR | None:
        number = int(pr)
        if repo is not None:
            return self._sim.prs.get((repo, number))
        for (_repo, num), sim_pr in self._sim.prs.items():
            if num == number:
                return sim_pr
        return None

    async def repo_clone(self, repo: str, dest: Path) -> None:
        return None

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
                return pr.url
        number = self._sim.next_pr_number()
        key_repo = repo or ""
        url = f"https://github.invalid/{key_repo}/pull/{number}"
        self._sim.prs[(key_repo, number)] = SimPR(
            repo=key_repo,
            number=number,
            head=head,
            base=base or "main",
            title=title,
            url=url,
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
            raise KeyError(f"no such PR: {pr}")
        return {
            "number": sim_pr.number,
            "title": sim_pr.title,
            "state": "MERGED" if sim_pr.merged else sim_pr.state.upper(),
            "url": sim_pr.url,
            "headRefName": sim_pr.head,
            "headRefOid": sim_pr.head_sha,
            "baseRefName": sim_pr.base,
            "mergeable": "MERGEABLE",
            "mergeStateStatus": "CLEAN",
            "isDraft": False,
            "mergedAt": sim_pr.url if sim_pr.merged else None,
        }

    async def pr_comment(
        self, pr: int | str, body: str, *, repo: str | None = None
    ) -> None:
        return None

    async def pr_diff(self, pr: int | str, *, repo: str | None = None) -> str:
        return ""

    async def pr_checks(self, pr: int | str, *, repo: str | None = None) -> PRChecks:
        sim_pr = self._pr(pr, repo)
        if sim_pr is None or sim_pr.checks_passed:
            return PRChecks(runs=[])
        return PRChecks(
            runs=[CheckRun(name="ci", state="FAILURE", bucket="fail")]
        )

    async def pr_review_comments(
        self, pr: int | str, *, repo: str
    ) -> list[dict[str, Any]]:
        return []

    async def pr_reviews(self, pr: int | str, *, repo: str) -> list[dict[str, Any]]:
        return []

    async def pr_issue_comments(
        self, pr: int | str, *, repo: str
    ) -> list[dict[str, Any]]:
        return []

    async def pr_reactions(self, pr: int | str, *, repo: str) -> list[dict[str, Any]]:
        return []

    async def commit_committed_at(self, repo: str, sha: str) -> str:
        return self._sim.now_iso()

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
            raise KeyError(f"no such PR: {pr}")
        sim_pr.state = PR_MERGED
