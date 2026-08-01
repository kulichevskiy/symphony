from pathlib import Path

from symphony.bench.eventdesk import eventdesk_campaign, harness_version, materialize_eventdesk


def test_eventdesk_campaign_is_six_complete_sequential_afk_tickets() -> None:
    campaign = eventdesk_campaign()

    assert [ticket.key for ticket in campaign.tickets] == [
        "booking",
        "capacity",
        "waitlist",
        "cancellation",
        "return-to",
        "payment-webhook",
    ]
    assert [ticket.blocked_by for ticket in campaign.tickets] == [
        (),
        ("booking",),
        ("capacity",),
        ("waitlist",),
        ("cancellation",),
        ("return-to",),
    ]
    for ticket in campaign.tickets:
        assert "## Context" in ticket.description
        assert "## Requirements" in ticket.description
        assert "## Acceptance criteria" in ticket.description
        assert "## Verification" in ticket.description
        assert "Do not ask questions" in ticket.description


def test_materialize_eventdesk_creates_runnable_full_stack_seed(tmp_path: Path) -> None:
    destination = tmp_path / "eventdesk"

    materialize_eventdesk(destination)

    expected = {
        "README.md",
        "STANDARDS.md",
        "pyproject.toml",
        ".github/workflows/ci.yml",
        "eventdesk/main.py",
        "tests/test_events.py",
        "frontend/package.json",
        "frontend/src/App.tsx",
    }
    assert expected <= {
        str(path.relative_to(destination)) for path in destination.rglob("*") if path.is_file()
    }


def test_harness_version_is_stable_content_hash() -> None:
    version = harness_version()

    assert len(version) == 16
    assert version == harness_version()
