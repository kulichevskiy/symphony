from __future__ import annotations

import json
import shutil
from dataclasses import asdict, dataclass
from importlib.resources import as_file, files
from pathlib import Path

from .eventdesk import (
    Campaign,
    CampaignTicket,
    eventdesk_campaign,
    harness_version,
    materialize_eventdesk,
)
from .grader import regression_commands
from .reviewer import final_review_prompts


@dataclass(frozen=True)
class FrozenHarness:
    root: Path
    version: str
    campaign: Campaign
    hidden_test: Path
    regression_commands: dict[str, list[str]]
    spec_prompt: str
    standards_prompt: str


def snapshot_harness(destination: Path) -> str:
    """Persist every workload artifact before an experiment becomes queue-visible."""
    destination.mkdir(parents=True, exist_ok=False)
    materialize_eventdesk(destination / "eventdesk")
    hidden = files("symphony.bench.assets").joinpath("hidden/test_eventdesk_hidden.py")
    with as_file(hidden) as hidden_path:
        shutil.copyfile(hidden_path, destination / "hidden_test.py")
    campaign = eventdesk_campaign_payload()
    (destination / "campaign.json").write_text(
        json.dumps(campaign, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (destination / "regression_commands.json").write_text(
        json.dumps(regression_commands(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    spec_prompt, standards_prompt = final_review_prompts()
    (destination / "spec_prompt.txt").write_text(spec_prompt, encoding="utf-8")
    (destination / "standards_prompt.txt").write_text(standards_prompt, encoding="utf-8")
    (destination / ".engine-version").write_text(harness_version(), encoding="utf-8")
    version = harness_version(destination)
    (destination / ".version").write_text(version, encoding="utf-8")
    return version


def load_harness(snapshot: Path) -> FrozenHarness:
    try:
        expected = (snapshot / ".version").read_text(encoding="utf-8").strip()
        engine = (snapshot / ".engine-version").read_text(encoding="utf-8").strip()
        campaign_payload = json.loads((snapshot / "campaign.json").read_text(encoding="utf-8"))
        commands_payload = json.loads(
            (snapshot / "regression_commands.json").read_text(encoding="utf-8")
        )
        spec_prompt = (snapshot / "spec_prompt.txt").read_text(encoding="utf-8")
        standards_prompt = (snapshot / "standards_prompt.txt").read_text(encoding="utf-8")
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"invalid harness snapshot at {snapshot}: {exc}") from exc
    actual = harness_version(snapshot)
    if actual != expected:
        raise RuntimeError(f"harness snapshot checksum mismatch: expected {expected}, got {actual}")
    current_engine = harness_version()
    if engine != current_engine:
        raise RuntimeError(
            "queued experiment harness engine changed; resubmit on the deployed bench version"
        )
    if not isinstance(campaign_payload, dict) or not isinstance(commands_payload, dict):
        raise RuntimeError("invalid harness snapshot payload")
    commands: dict[str, list[str]] = {}
    for name, argv in commands_payload.items():
        if (
            not isinstance(name, str)
            or not name
            or not isinstance(argv, list)
            or not argv
            or not all(isinstance(part, str) and part for part in argv)
        ):
            raise RuntimeError("invalid harness regression command")
        commands[name] = list(argv)
    return FrozenHarness(
        root=snapshot,
        version=expected,
        campaign=campaign_from_payload(campaign_payload),
        hidden_test=snapshot / "hidden_test.py",
        regression_commands=commands,
        spec_prompt=spec_prompt,
        standards_prompt=standards_prompt,
    )


def eventdesk_campaign_payload() -> dict[str, object]:
    return asdict(eventdesk_campaign())


def campaign_from_payload(payload: dict[str, object]) -> Campaign:
    raw_tickets = payload.get("tickets")
    if not isinstance(raw_tickets, list):
        raise RuntimeError("invalid harness campaign tickets")
    tickets: list[CampaignTicket] = []
    for raw in raw_tickets:
        if not isinstance(raw, dict):
            raise RuntimeError("invalid harness campaign ticket")
        blocked_by = raw.get("blocked_by", [])
        if not isinstance(blocked_by, list):
            raise RuntimeError("invalid harness campaign dependency")
        tickets.append(
            CampaignTicket(
                key=str(raw["key"]),
                title=str(raw["title"]),
                description=str(raw["description"]),
                blocked_by=tuple(str(value) for value in blocked_by),
            )
        )
    return Campaign(name=str(payload.get("name", "")), tickets=tuple(tickets))
