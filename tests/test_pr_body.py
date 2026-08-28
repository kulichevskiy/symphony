"""PR body rendering from the tracker issue snapshot (SYM-243).

`build_pr_body` is the single place that turns the issue snapshot the
implement stage already holds into the body of a newly created PR. No
tracker read, no provider-specific branching: the description is passed
through as Markdown, Linear rich-link tags become ordinary links, and the
body always ends with the tracker link footer.
"""

from __future__ import annotations

from symphony.orchestrator.poll import PR_BODY_MAX_CHARS, build_pr_body
from symphony.tracker import Issue


def _issue(description: str) -> Issue:
    return Issue(
        id="iss-1",
        identifier="ENG-1",
        title="Add authentication",
        description=description,
        url="https://linear.app/team/issue/ENG-1",
        state_id="state-todo",
        state_name="Todo",
        state_type="unstarted",
        team_key="ENG",
    )


FOOTER = "---\n\nRelates to https://linear.app/team/issue/ENG-1"


def test_non_empty_description_is_preserved_above_the_footer() -> None:
    description = "## Goal\n\nShip OAuth.\n\n- [ ] login\n- [ ] logout"
    body = build_pr_body(_issue(description))
    assert body == f"{description}\n\n{FOOTER}"


def test_plain_markdown_passes_through_unchanged() -> None:
    """Jira (and any plain-Markdown) description: only the footer is added."""
    description = "h2. nope, actually *markdown* with `code` and https://x.invalid/y"
    assert build_pr_body(_issue(description)) == f"{description}\n\n{FOOTER}"


def test_linear_rich_link_tags_become_markdown_links() -> None:
    description = (
        'Blocked by <issue id="uuid-1" identifier="ENG-9" '
        'title="Earlier work" url="https://linear.app/team/issue/ENG-9">ENG-9</issue> '
        'and <pull-request id="uuid-2" title="Fix it" '
        'url="https://github.com/org/repo/pull/7"/>.'
    )
    body = build_pr_body(_issue(description))
    assert "Blocked by [ENG-9](https://linear.app/team/issue/ENG-9)" in body
    assert "[Fix it](https://github.com/org/repo/pull/7)." in body
    assert "<issue" not in body and "<pull-request" not in body
    assert body.endswith(FOOTER)


def test_trusted_github_markdown_is_left_alone() -> None:
    description = "cc @octocat, see org/repo#12. Fixes #3."
    assert description in build_pr_body(_issue(description))


def test_empty_description_renders_only_the_tracker_link() -> None:
    assert build_pr_body(_issue("")) == "Relates to https://linear.app/team/issue/ENG-1"
    assert build_pr_body(_issue("   \n\n ")) == "Relates to https://linear.app/team/issue/ENG-1"


def test_oversized_description_is_truncated_at_a_line_boundary() -> None:
    line = "x" * 99
    description = "\n".join([line] * 2000)
    body = build_pr_body(_issue(description))
    assert len(body) <= PR_BODY_MAX_CHARS
    assert body.endswith(FOOTER)
    assert "Description truncated; see the source ticket" in body
    head, _, _ = body.partition("\n\nDescription truncated")
    assert head  # some description survived
    assert all(chunk == line for chunk in head.split("\n"))  # cut on a line boundary


def test_oversized_single_line_description_still_fits() -> None:
    body = build_pr_body(_issue("y" * (PR_BODY_MAX_CHARS * 2)))
    assert len(body) <= PR_BODY_MAX_CHARS
    assert body.endswith(FOOTER)
    assert "Description truncated; see the source ticket" in body
