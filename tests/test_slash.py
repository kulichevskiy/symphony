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
            _c("$skip-review"),
        ]
    )
    assert [i.kind for i in intents] == [
        SlashKind.APPROVE,
        SlashKind.REJECT,
        SlashKind.RETRY,
        SlashKind.STOP,
        SlashKind.SKIP_REVIEW,
    ]


def test_ignores_self_authored() -> None:
    assert parse([_c("$approve", is_me=True)]) == []


def test_ignores_mirrored_from_github() -> None:
    # The GitHub-side review poll handles these; double-firing is the bug
    # we explicitly avoid (see `linear-integration-research.md` §31).
    assert parse([_c("$approve", external_thread_type="githubPullRequest")]) == []


def test_ignores_free_form() -> None:
    assert parse([_c("looks good — please ship it")]) == []


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
