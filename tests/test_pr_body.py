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
    """The closer reuses the opener's own indent (rather than a bare ```)
    so a fence nested under a list item stays inside that list item instead
    of dedenting out of it and opening a stray new root-level fence."""
    line = "y" * 99
    description = "  ```\n" + "\n".join([line] * 2000)
    body = build_pr_body(_issue(description))
    assert len(body) <= PR_BODY_MAX_CHARS
    assert body.endswith(FOOTER)
    head, _, _ = body.partition("\n\nDescription truncated")
    assert head.endswith("\n  ```")


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


def test_normal_size_body_closes_an_unterminated_code_fence() -> None:
    """A short description that itself never closes a ``` fence must not
    swallow the `---`/tracker-link footer inside that code block."""
    description = "```\nsome code"
    body = build_pr_body(_issue(description))
    assert body == f"{description}\n```\n\n{FOOTER}"
    assert body.count("```") % 2 == 0


def test_truncation_closes_an_html_comment_left_open_by_the_cut() -> None:
    """The full description properly closes its `<!-- -->` comment, but the
    truncation cut lands between the opener and the closer — the retained
    prefix must get its own `-->` so the notice/footer aren't swallowed."""
    line = "y" * 99
    description = "<!--\n" + "\n".join([line] * 2000) + "\n-->"
    body = build_pr_body(_issue(description))
    assert len(body) <= PR_BODY_MAX_CHARS
    assert body.endswith(FOOTER)
    head, _, _ = body.partition("\n\nDescription truncated")
    assert head.endswith("\n-->")
    assert head.count("<!--") == head.count("-->")


def test_backtick_delimited_tag_with_a_longer_closing_run_is_rewritten() -> None:
    """A single opening backtick followed only by a longer (double) backtick
    run has no CommonMark code span — the tag is in prose, not code, and must
    still be converted to a Markdown link."""
    description = '`<issue url="https://l/9">ENG-9</issue>``'
    result = _linear_rich_links_to_markdown(description)
    assert result == "`[ENG-9](https://l/9)``"


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


def test_rich_link_tag_inside_a_pre_block_is_left_alone() -> None:
    description = '<pre>\n<issue url="https://l/9">ENG-9</issue>\n</pre>'
    result = _linear_rich_links_to_markdown(description)
    assert result == description


def test_rich_link_tag_inside_a_type6_raw_html_block_is_left_alone() -> None:
    """A `<div>...</div>` wrapper is a CommonMark raw HTML block (type 6):
    GitHub renders its contents as literal HTML rather than reprocessing
    them as Markdown, so a rich-link tag inside it must not be rewritten —
    the resulting `[label](url)` text would render as-is, not as a link."""
    description = '<div>\n<issue url="https://l/9">ENG-9</issue>\n</div>'
    result = _linear_rich_links_to_markdown(description)
    assert result == description


def test_rich_link_tag_inside_a_type6_block_ends_at_a_blank_line() -> None:
    description = (
        '<table>\n<issue url="https://l/9">ENG-9</issue>\n</table>'
        '\n\n<issue url="https://l/10">ENG-10</issue>'
    )
    result = _linear_rich_links_to_markdown(description)
    assert '<issue url="https://l/9">ENG-9</issue>' in result
    assert "[ENG-10](https://l/10)" in result


def test_fence_shaped_text_inside_a_type6_raw_html_block_is_not_treated_as_dangling() -> None:
    """A ``` line inside a `<div>...</div>` wrapper (CommonMark raw HTML
    block type 6) is inert HTML content, not a real code fence — the
    wrapper swallows it, so no closing ``` should be synthesized."""
    description = "<div>\n```\n</div>"
    body = build_pr_body(_issue(description))
    assert body == f"{description}\n\n{FOOTER}"


def test_rich_link_tag_inside_a_type7_raw_html_block_is_left_alone() -> None:
    """A custom-tag wrapper alone on its own lines, like `<x-widget>...
    </x-widget>`, is a CommonMark raw HTML block (type 7): GitHub renders it
    as literal HTML, so a rich-link tag nested inside it must not be
    rewritten."""
    description = '<x-widget>\n<issue url="https://l/9">ENG-9</issue>\n</x-widget>'
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


def test_normal_size_body_closes_an_unterminated_html_comment() -> None:
    """A short description that itself never closes a `<!--` comment must not
    swallow the `---`/tracker-link footer inside that comment."""
    description = "some text <!-- unterminated"
    body = build_pr_body(_issue(description))
    assert body == f"{description}\n-->\n\n{FOOTER}"


def test_comment_opener_inside_inline_code_is_not_treated_as_dangling() -> None:
    """A literal `` `<!--` `` example inside inline code is inert text, not a
    real (unterminated) HTML comment opener."""
    description = "See `<!--` for the syntax."
    body = build_pr_body(_issue(description))
    assert body == f"{description}\n\n{FOOTER}"


def test_comment_opener_inside_an_indented_code_block_is_not_treated_as_dangling() -> None:
    description = "    <!-- literal example"
    body = build_pr_body(_issue(description))
    assert body == f"{description}\n\n{FOOTER}"


def test_normal_size_body_closes_an_unterminated_pre_block() -> None:
    """A short description that opens a `<pre>` raw HTML block but never
    closes it must not swallow the `---`/tracker-link footer inside it."""
    description = "<pre>\nsome code"
    body = build_pr_body(_issue(description))
    assert body == f"{description}\n</pre>\n\n{FOOTER}"


def test_truncation_closes_an_unterminated_pre_block() -> None:
    """An oversized description that opens a `<pre>` block and only closes it
    after the truncation cutoff must get its own `</pre>` in the retained
    prefix, or the truncation notice/footer render inside the raw HTML block."""
    line = "y" * 99
    description = "<pre>\n" + "\n".join([line] * 2000) + "\n</pre>"
    body = build_pr_body(_issue(description))
    assert len(body) <= PR_BODY_MAX_CHARS
    assert body.endswith(FOOTER)
    head, _, _ = body.partition("\n\nDescription truncated")
    assert head.endswith("\n</pre>")


def test_fence_marker_inside_a_closed_html_comment_is_not_treated_as_dangling() -> None:
    """A fence-shaped line fully inside an already-closed `<!-- -->` comment
    is inert raw text, not a real (unterminated) code fence."""
    description = "<!--\n```\n-->"
    body = build_pr_body(_issue(description))
    assert body == f"{description}\n\n{FOOTER}"
    assert body.count("```") == 1


def test_backslash_escaped_rich_link_tag_is_left_alone() -> None:
    """A `\\<issue …>` example is intentionally escaped literal Markdown, not
    a real Linear rich link, and must not be rewritten."""
    description = '\\<issue url="https://l/9">ENG-9</issue>'
    result = _linear_rich_links_to_markdown(description)
    assert result == description


def test_double_backslash_escaped_rich_link_tag_is_rewritten() -> None:
    """`\\\\<issue …>` is an escaped backslash followed by an *unescaped*
    `<issue …>` per CommonMark, so the tag is still eligible for rewriting."""
    description = '\\\\<issue url="https://l/9">ENG-9</issue>'
    result = _linear_rich_links_to_markdown(description)
    assert result == "\\\\[ENG-9](https://l/9)"


def test_quoted_fence_continues_when_a_later_line_drops_the_optional_space() -> None:
    """A quoted fence opened with `> ``` ` still continues on a later line
    quoted with bare `>` (no trailing space) per CommonMark — a rich link
    tag on that line stays inside the code block and is not rewritten."""
    description = '> ```\n><issue url="https://l/9">ENG-9</issue>\n> ```'
    result = _linear_rich_links_to_markdown(description)
    assert result == description


def test_long_backtick_run_in_prose_does_not_backtrack_catastrophically() -> None:
    """A long backtick run with no matching close must not be rescanned once
    per possible delimiter length (previously quadratic in the run length)."""
    description = "x" + "`" * 20_000 + "y"
    start = time.monotonic()
    result = _linear_rich_links_to_markdown(description)
    elapsed = time.monotonic() - start
    assert elapsed < 1.0
    assert result == description


def test_rich_link_tag_inside_a_code_span_with_an_interior_double_backtick_is_left_alone() -> None:
    """A valid single-backtick code span may contain a `` `` `` run before its
    eventual single-backtick closer — that interior run must not be mistaken
    for (and rejected as) a mismatched closer, which would otherwise abandon
    the match and leave a real code span undetected."""
    description = 'See `text ``x`` <issue url="https://l/9">ENG-9</issue>` done.'
    result = _linear_rich_links_to_markdown(description)
    assert result == description


def test_short_fence_nested_under_a_list_item_is_closed_with_matching_indent() -> None:
    """A fence nested under a list item must be closed with the same indent
    it opened with — a bare, unindented closer would fall outside the list
    item per CommonMark and reopen as a stray new root-level fence that
    swallows everything after it (including the footer)."""
    description = "- item\n  ```\n  code"
    body = build_pr_body(_issue(description))
    assert body == f"{description}\n  ```\n\n{FOOTER}"
    assert body.count("```") % 2 == 0


def test_normal_size_body_closes_an_unterminated_processing_instruction() -> None:
    """A short description that opens a `<?...?>` processing instruction but
    never closes it must not swallow the `---`/tracker-link footer inside it."""
    description = '<?xml version="1.0"'
    body = build_pr_body(_issue(description))
    assert body == f"{description}\n?>\n\n{FOOTER}"


def test_normal_size_body_closes_an_unterminated_declaration() -> None:
    description = "<!DOCTYPE html"
    body = build_pr_body(_issue(description))
    assert body == f"{description}\n>\n\n{FOOTER}"


def test_normal_size_body_closes_an_unterminated_cdata_section() -> None:
    description = "<![CDATA[unterminated"
    body = build_pr_body(_issue(description))
    assert body == f"{description}\n]]>\n\n{FOOTER}"


def test_fence_marker_inside_a_closed_processing_instruction_is_not_treated_as_dangling() -> None:
    """A fence-shaped line fully inside an already-closed `<?...?>` block is
    inert raw text, not a real (unterminated) code fence."""
    description = "<?xml\n```\n?>"
    body = build_pr_body(_issue(description))
    assert body == f"{description}\n\n{FOOTER}"
    assert body.count("```") == 1


def test_backslash_escaped_html_comment_opener_is_not_treated_as_dangling() -> None:
    """A `\\<!-- example` is an escaped literal `<`, not a real (unterminated)
    HTML comment opener, per CommonMark backslash-escape rules."""
    description = "some text \\<!-- example"
    body = build_pr_body(_issue(description))
    assert body == f"{description}\n\n{FOOTER}"


def test_double_backslash_before_html_comment_opener_still_dangles() -> None:
    """`\\\\<!--` is an escaped backslash followed by a genuine (unescaped)
    comment opener, so the dangling `-->` must still be appended."""
    description = "some text \\\\<!-- example"
    body = build_pr_body(_issue(description))
    assert body == f"{description}\n-->\n\n{FOOTER}"


def test_backslash_escaped_backtick_does_not_start_a_code_span() -> None:
    """A `` \\` `` is an escaped literal backtick, not a code-span delimiter,
    so the rich-link tag between it and a later (real, unpaired) backtick is
    still in prose and must be rewritten."""
    description = '\\`<issue url="https://l/9">ENG-9</issue>`'
    result = _linear_rich_links_to_markdown(description)
    assert result == "\\`[ENG-9](https://l/9)`"


def test_double_backslash_before_backtick_is_a_real_code_span_delimiter() -> None:
    """`\\\\`` is an escaped backslash followed by a genuine (unescaped)
    backtick delimiter, so the enclosed tag stays inert code."""
    description = '\\\\`<issue url="https://l/9">ENG-9</issue>`'
    result = _linear_rich_links_to_markdown(description)
    assert result == description


def test_truncation_shortens_a_single_oversized_line_that_leaves_markup_open() -> None:
    """When the retained head is a single line already at the budget ceiling
    (e.g. an oversized one-line `<pre>...</pre>` description with no line
    breaks), dropping it outright to make room for the `</pre>` closer would
    discard the whole description. The line must be shortened instead."""
    description = "<pre>" + "z" * (PR_BODY_MAX_CHARS * 2)
    body = build_pr_body(_issue(description))
    assert len(body) <= PR_BODY_MAX_CHARS
    assert len(body.encode("utf-8")) <= PR_BODY_MAX_BYTES
    assert body.endswith(FOOTER)
    head, _, _ = body.partition("\n\nDescription truncated")
    assert head.startswith("<pre>")
    assert head.endswith("</pre>")
    assert len(head) > len("<pre></pre>")


def test_oversized_single_line_of_pure_backticks_is_dropped_rather_than_overflowing() -> None:
    """A single line consisting entirely of backticks long enough to fill the
    whole budget on its own has a fence closer just as long as the opener —
    no prefix of it can be kept alongside that closer, so the line is
    dropped instead of producing a body that overruns the size budget."""
    description = "`" * (PR_BODY_MAX_CHARS * 2)
    body = build_pr_body(_issue(description))
    assert len(body) <= PR_BODY_MAX_CHARS
    assert len(body.encode("utf-8")) <= PR_BODY_MAX_BYTES
    assert body.endswith(FOOTER)
    assert "Description truncated; see the source ticket" in body


def test_rich_link_url_with_an_unbalanced_close_paren_is_wrapped_in_angle_brackets() -> None:
    """A rich-link URL with an unmatched `)` (e.g. a slug ending `fix)-bug`)
    would otherwise terminate the Markdown link destination early and spill
    the remainder of the URL into visible text."""
    description = '<issue url="https://l/9/fix)-bug" identifier="ENG-9">ENG-9</issue>'
    result = _linear_rich_links_to_markdown(description)
    assert result == "[ENG-9](<https://l/9/fix)-bug>)"
