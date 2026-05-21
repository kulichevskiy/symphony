"""Slash-command parser tests.

Mirrors the rules in `docs/python-port-research.md` §13.2 and the prior
research §35 (Strategy 1 — slash-command-only).
"""

from __future__ import annotations

from symphony.linear.client import LinearComment
from symphony.linear.slash import SlashKind, parse


def _c(
    body: str,
    *,
    is_me: bool = False,
    external_thread_type: str | None = None,
    cid: str = "1",
) -> LinearComment:
    return LinearComment(
        id=cid,
        body=body,
        created_at="2026-05-10T00:00:00Z",
        author_name="user",
        author_is_me=is_me,
        external_thread_type=external_thread_type,
    )


def test_parses_known_commands() -> None:
    intents = parse(
        [
            _c("$approve"),
            _c("$REJECT"),
            _c("$retry now"),
            _c("$stop"),
            _c("$retry-acceptance"),
            _c("$skip-acceptance"),
            _c("$skip-review"),
            _c("$skip-local-review"),
            _c("$skip-acceptance"),
            _c("$retry-acceptance"),
        ]
    )
    assert [i.kind for i in intents] == [
        SlashKind.APPROVE,
        SlashKind.REJECT,
        SlashKind.RETRY,
        SlashKind.STOP,
        SlashKind.RETRY_ACCEPTANCE,
        SlashKind.SKIP_ACCEPTANCE,
        SlashKind.SKIP_REVIEW,
        SlashKind.SKIP_LOCAL_REVIEW,
        SlashKind.SKIP_ACCEPTANCE,
        SlashKind.RETRY_ACCEPTANCE,
    ]


def test_skip_local_review_does_not_collide_with_skip_review() -> None:
    """`$skip-review` and `$skip-local-review` are distinct: the parser
    must pick the longer literal first or `$skip-local-review` would
    silently map to `SKIP_REVIEW`."""
    intents = parse([_c("$skip-local-review", cid="a")])
    assert len(intents) == 1
    assert intents[0].kind == SlashKind.SKIP_LOCAL_REVIEW

    intents = parse([_c("$skip-review", cid="b")])
    assert len(intents) == 1
    assert intents[0].kind == SlashKind.SKIP_REVIEW


def test_parses_markdown_wrapped_commands_and_approved_alias() -> None:
    intents = parse(
        [
            _c("`$approve`"),
            _c("```\n$reject\n```"),
            _c("```bash\n$stop\n```"),
            _c("$approved"),
        ]
    )

    assert [i.kind for i in intents] == [
        SlashKind.APPROVE,
        SlashKind.REJECT,
        SlashKind.STOP,
        SlashKind.APPROVE,
    ]


def test_ignores_self_authored() -> None:
    assert parse([_c("$approve", is_me=True)]) == []


def test_ignores_mirrored_from_github() -> None:
    # The GitHub-side review poll handles these; double-firing is the bug
    # we explicitly avoid (see `linear-integration-research.md` §31).
    assert parse([_c("$approve", external_thread_type="githubPullRequest")]) == []


def test_ignores_free_form() -> None:
    assert parse([_c("looks good — please ship it")]) == []
    assert parse([_c("please `$approve`")]) == []


def test_ignores_unknown_slash() -> None:
    assert parse([_c("/yolo")]) == []


def test_treats_thumbs_up_as_approve() -> None:
    comments = [
        _c("👍", cid="c-1"),
        _c(":+1:", cid="c-2"),
        _c(":+1", cid="c-3"),
        _c("👍🏽", cid="c-4"),
        _c("👍️", cid="c-5"),
    ]

    intents = parse(comments)

    assert [i.kind for i in intents] == [
        SlashKind.APPROVE,
        SlashKind.APPROVE,
        SlashKind.APPROVE,
        SlashKind.APPROVE,
        SlashKind.APPROVE,
    ]
