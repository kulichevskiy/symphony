from __future__ import annotations

import pytest

from symphony.linear.blockers import OPEN_BLOCKER_TYPES, is_blocked, open_blocker_ids
from symphony.linear.client import Blocker, LinearIssue


def _issue(blockers: list[Blocker]) -> LinearIssue:
    return LinearIssue(
        id="iss-1",
        identifier="ENG-1",
        title="Blocked work",
        description="",
        url="https://linear.app/x",
        state_id="state-todo",
        state_name="Todo",
        state_type="unstarted",
        team_key="ENG",
        blocked_by=blockers,
    )


def _blocker(
    identifier: str,
    state_type: str,
    *,
    archived: bool = False,
) -> Blocker:
    return Blocker(
        id=f"id-{identifier}",
        identifier=identifier,
        state_type=state_type,
        archived=archived,
    )


@pytest.mark.parametrize("state_type", sorted(OPEN_BLOCKER_TYPES))
def test_is_blocked_true_for_open_unarchived_blockers(state_type: str) -> None:
    assert is_blocked(_issue([_blocker("ENG-2", state_type)])) is True


@pytest.mark.parametrize("state_type", ["completed", "canceled"])
def test_is_blocked_false_for_closed_blockers(state_type: str) -> None:
    assert is_blocked(_issue([_blocker("ENG-2", state_type)])) is False


def test_is_blocked_false_for_archived_blocker() -> None:
    assert is_blocked(_issue([_blocker("ENG-2", "started", archived=True)])) is False


def test_is_blocked_false_for_empty_blockers() -> None:
    assert is_blocked(_issue([])) is False


def test_open_blocker_ids_returns_only_open_blockers_in_input_order() -> None:
    issue = _issue(
        [
            _blocker("ENG-2", "completed"),
            _blocker("ENG-3", "started"),
            _blocker("ENG-4", "triage"),
            _blocker("ENG-5", "unstarted", archived=True),
        ]
    )

    assert open_blocker_ids(issue) == ["ENG-3", "ENG-4"]
