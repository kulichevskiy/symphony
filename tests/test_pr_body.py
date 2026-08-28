"""PR body rendering from the tracker issue snapshot (SYM-243).

`build_pr_body` is the single place that turns the issue snapshot the
implement stage already holds into the body of a newly created PR. No
tracker read, no provider-specific branching: the description is passed
through as Markdown, Linear rich-link tags become ordinary links, and the
body always ends with the tracker link footer.
"""

from __future__ import annotations

import time

from symphony.orchestrator.poll import PR_BODY_MAX_BYTES, PR_BODY_MAX_CHARS, build_pr_body
from symphony.orchestrator.poll._helpers import _linear_rich_links_to_markdown
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


def test_tag_shaped_text_without_url_passes_through_verbatim() -> None:
    description = "The only rewrite converts Linear `<issue …>` and `<pull-request …>` tags."
    assert build_pr_body(_issue(description)) == f"{description}\n\n{FOOTER}"


def test_tag_shaped_text_without_url_survives_inside_a_fenced_block() -> None:
    description = "```xml\n<issue>\n  <id>1</id>\n</issue>\n```"
    assert build_pr_body(_issue(description)) == f"{description}\n\n{FOOTER}"


def test_angle_bracket_inside_a_quoted_attribute_does_not_end_the_tag() -> None:
    description = '<issue id="u1" title="Fix A > B" url="https://l/9">ENG-9</issue>'
    body = build_pr_body(_issue(description))
    assert "[ENG-9](https://l/9)" in body
    assert "url=" not in body


def test_apostrophe_inside_a_double_quoted_title_is_not_truncated() -> None:
    description = '<pull-request id="u2" title="Bob\'s fix" url="https://g/7"/>'
    body = build_pr_body(_issue(description))
    assert "[Bob's fix](https://g/7)" in body


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


def test_oversized_cjk_description_is_truncated_within_the_byte_budget() -> None:
    """A description well under PR_BODY_MAX_CHARS can still blow the argv byte
    budget: each CJK char is ~3 UTF-8 bytes, so ~44k chars ≈ 131 KB."""
    line = "汉" * 99
    description = "\n".join([line] * 500)  # ~50k chars, ~150 KB
    body = build_pr_body(_issue(description))
    assert len(body.encode("utf-8")) <= PR_BODY_MAX_BYTES
    assert body.endswith(FOOTER)
    assert "Description truncated; see the source ticket" in body


def test_oversized_single_line_cjk_description_still_fits_byte_budget() -> None:
    body = build_pr_body(_issue("汉" * 60_000))
    assert len(body.encode("utf-8")) <= PR_BODY_MAX_BYTES
    assert body.endswith(FOOTER)
    assert "Description truncated; see the source ticket" in body


def test_malformed_tag_shaped_input_does_not_backtrack_catastrophically() -> None:
    """A failing tag-shaped input with many ambiguous quote pairs must not
    trigger exponential backtracking (previously ~7x slower per two quote
    pairs added, ~20 minutes at n=22)."""
    description = "<issue " + ' "a"' * 20 + " <"
    start = time.monotonic()
    result = _linear_rich_links_to_markdown(description)
    elapsed = time.monotonic() - start
    assert elapsed < 1.0
    assert result == description


def test_rich_link_text_containing_an_angle_bracket_does_not_leak_a_close_tag() -> None:
    description = '<issue url="https://l/9" identifier="ENG-9">see <https://x.invalid></issue>'
    body = build_pr_body(_issue(description))
    assert "</issue>" not in body
    assert "[see <https://x.invalid>](https://l/9)" in body


def test_self_closing_tag_does_not_swallow_a_later_same_name_tag() -> None:
    """A self-closing `<issue … />` must not reach forward and consume a
    later, unrelated paired `<issue …>…</issue>` of the same name."""
    description = '<issue url="https://l/1"/> and <issue url="https://l/2">ENG-2</issue>'
    result = _linear_rich_links_to_markdown(description)
    assert result == "[https://l/1](https://l/1) and [ENG-2](https://l/2)"


def test_long_whitespace_run_in_a_tag_opener_does_not_backtrack_catastrophically() -> None:
    """A tag-shaped opener followed by a long contiguous whitespace run must
    not be rescanned once per attrs split (previously ~140s at 60k spaces)."""
    description = "<issue " + " " * 60_000 + "text"
    start = time.monotonic()
    result = _linear_rich_links_to_markdown(description)
    elapsed = time.monotonic() - start
    assert elapsed < 5.0
    assert result == description


def test_truncation_preserves_crlf_line_endings() -> None:
    """`splitlines()` treats `\\r\\n` as a line break; joining the kept lines
    back with `\\n` would then drop every `\\r`. The kept head must stay a
    verbatim prefix of the original description."""
    line = "x" * 99
    description = "\r\n".join([line] * 2000)
    body = build_pr_body(_issue(description))
    head, _, _ = body.partition("\n\nDescription truncated")
    assert head
    assert description.startswith(head)


def test_truncation_preserves_unicode_line_separators() -> None:
    """`splitlines()` also breaks on U+2028/U+2029 and friends; joining with
    `\\n` would silently convert them. The kept head must stay a verbatim
    prefix of the original description."""
    line = "x" * 99
    description = " ".join([line] * 2000)
    body = build_pr_body(_issue(description))
    head, _, _ = body.partition("\n\nDescription truncated")
    assert head
    assert description.startswith(head)


def test_truncation_closes_an_unterminated_code_fence() -> None:
    """Cutting a description inside an open ``` fence must close it before
    the truncation notice/footer, or they render inside the code block."""
    line = "y" * 99
    description = "```\n" + "\n".join([line] * 2000)
    body = build_pr_body(_issue(description))
    assert len(body) <= PR_BODY_MAX_CHARS
    assert len(body.encode("utf-8")) <= PR_BODY_MAX_BYTES
    assert body.endswith(FOOTER)
    assert "```\n\nDescription truncated" in body
    assert body.count("```") % 2 == 0


def test_truncation_closes_an_unterminated_tilde_fence() -> None:
    line = "y" * 99
    description = "~~~\n" + "\n".join([line] * 2000)
    body = build_pr_body(_issue(description))
    assert len(body) <= PR_BODY_MAX_CHARS
    assert body.endswith(FOOTER)
    head, _, _ = body.partition("\n\nDescription truncated")
    assert head.endswith("\n~~~")


def test_truncation_closes_an_unterminated_indented_fence() -> None:
    line = "y" * 99
    description = "  ```\n" + "\n".join([line] * 2000)
    body = build_pr_body(_issue(description))
    assert len(body) <= PR_BODY_MAX_CHARS
    assert body.endswith(FOOTER)
    head, _, _ = body.partition("\n\nDescription truncated")
    assert head.endswith("\n```")


def test_truncation_closes_a_four_backtick_fence_without_a_stray_three_backtick_line() -> None:
    """A ``` line inside a description that opened with 4 backticks is just
    content (not a closer) — the truncation must still close with 4+."""
    filler_lines = ["```", "y" * 99] * 1000
    description = "\n".join(["````", *filler_lines])
    body = build_pr_body(_issue(description))
    assert len(body) <= PR_BODY_MAX_CHARS
    assert body.endswith(FOOTER)
    head, _, _ = body.partition("\n\nDescription truncated")
    assert head.endswith("\n````")


def test_indented_code_block_at_the_start_keeps_its_indentation() -> None:
    description = "    def f():\n        return 1"
    assert build_pr_body(_issue(description)) == f"{description}\n\n{FOOTER}"


def test_rich_link_label_with_brackets_is_escaped() -> None:
    description = '<issue url="https://l/9">weird [label] text</issue>'
    result = _linear_rich_links_to_markdown(description)
    assert result == "[weird \\[label\\] text](https://l/9)"


def test_rich_link_tag_with_a_url_inside_inline_code_is_left_alone() -> None:
    description = 'See `<issue url="https://l/9">ENG-9</issue>` for context.'
    result = _linear_rich_links_to_markdown(description)
    assert result == description


def test_rich_link_tag_with_a_url_inside_a_fenced_block_is_left_alone() -> None:
    description = '```\n<issue url="https://l/9">ENG-9</issue>\n```'
    result = _linear_rich_links_to_markdown(description)
    assert result == description


def test_rich_link_tag_inside_an_indented_code_block_is_left_alone() -> None:
    description = '    <issue url="https://l/9">ENG-9</issue>'
    result = _linear_rich_links_to_markdown(description)
    assert result == description


def test_rich_link_tag_inside_a_fence_nested_in_a_block_quote_is_left_alone() -> None:
    description = '> ```\n> <issue url="https://l/9">ENG-9</issue>\n> ```'
    result = _linear_rich_links_to_markdown(description)
    assert result == description


def test_indented_code_block_does_not_interrupt_a_paragraph() -> None:
    """A 4-space-indented line right after prose (no blank line separator) is
    a lazy paragraph continuation per CommonMark, not code — so a real rich
    link written that way still gets rewritten."""
    description = 'Blocked by\n    <issue url="https://l/9">ENG-9</issue>'
    result = _linear_rich_links_to_markdown(description)
    assert result == "Blocked by\n    [ENG-9](https://l/9)"


def test_unclosed_url_less_tag_does_not_swallow_a_following_real_rich_link() -> None:
    """A url-less, tag-shaped `<issue …>` in prose must match on its own and
    not stretch its `text` group forward to a later same-name close tag that
    belongs to a genuine rich link."""
    description = 'Convert `<issue …>` tags. Blocked by <issue url="https://l/9">ENG-9</issue>.'
    result = _linear_rich_links_to_markdown(description)
    assert result == "Convert `<issue …>` tags. Blocked by [ENG-9](https://l/9)."


def test_unterminated_quoted_fence_followed_by_prose_rich_link_is_rewritten() -> None:
    """An unterminated block-quoted fence must not swallow the first
    unquoted line that follows it — that line is prose, not code, so a
    Linear rich link on it must still be rewritten."""
    description = '> ```\n> still quoted\n<issue url="https://l/9">ENG-9</issue>'
    result = _linear_rich_links_to_markdown(description)
    assert result == "> ```\n> still quoted\n[ENG-9](https://l/9)"


def test_long_backtick_run_in_prose_does_not_backtrack_catastrophically() -> None:
    """A long backtick run with no matching close must not be rescanned once
    per possible delimiter length (previously quadratic in the run length)."""
    description = "x" + "`" * 20_000 + "y"
    start = time.monotonic()
    result = _linear_rich_links_to_markdown(description)
    elapsed = time.monotonic() - start
    assert elapsed < 1.0
    assert result == description
