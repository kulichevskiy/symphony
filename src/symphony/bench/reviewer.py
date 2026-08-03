from __future__ import annotations

import json
import re
import shutil
import tempfile
from pathlib import Path
from typing import Literal

from pydantic import BaseModel

from .. import db
from ..codex_login import codex_expires_at, pin_file_auth_storage, read_codex_credential
from ..credentials import CredentialResolver, CredentialWriteBack
from ..crypto import CredentialCipher
from .campaign import support_queue_campaign
from .github import Commands


def cleanup_stale_reviewer_credentials(root: Path) -> int:
    """Remove plaintext Codex homes left by an interrupted final review."""
    removed = 0
    if not root.exists():
        return removed
    for path in root.glob("EXP-*/*/bench-codex-*"):
        if not path.is_dir():
            continue
        shutil.rmtree(path, ignore_errors=True)
        removed += 1
    return removed


class ReviewFinding(BaseModel):
    severity: Literal["Critical", "Major", "Minor"]
    title: str
    evidence: str
    explanation: str


class ReviewResult(BaseModel):
    findings: list[ReviewFinding]


def parse_review(text: str) -> ReviewResult:
    candidate = text.strip()
    fenced = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", candidate, re.DOTALL | re.I)
    if fenced is not None:
        candidate = fenced.group(1)
    try:
        payload = json.loads(candidate)
        return ReviewResult.model_validate(payload)
    except (json.JSONDecodeError, ValueError) as exc:
        raise RuntimeError("final reviewer returned an invalid JSON contract") from exc


def review_metrics(*, spec: ReviewResult, standards: ReviewResult) -> dict[str, object]:
    metrics: dict[str, object] = {}
    for name, result in (("spec", spec), ("standards", standards)):
        serialized = [finding.model_dump() for finding in result.findings]
        metrics[f"{name}_findings"] = serialized
        metrics[f"{name}_findings_total"] = len(serialized)
        for severity in ("critical", "major", "minor"):
            metrics[f"{name}_findings_{severity}"] = sum(
                finding.severity.lower() == severity for finding in result.findings
            )
    return metrics


class CodexFinalReviewer:
    """Two independent read-only product reviews after hidden grading."""

    def __init__(
        self,
        *,
        commands: Commands,
        control_db: Path,
        encryption_key: str,
        model: str = "gpt-5.6-sol",
    ) -> None:
        self._commands = commands
        self._control_db = control_db
        self._cipher = CredentialCipher(encryption_key)
        self._model = model

    async def review(
        self,
        *,
        checkout: Path,
        spec_prompt: str | None = None,
        standards_prompt: str | None = None,
    ) -> dict[str, object]:
        conn = await db.connect(self._control_db)
        try:
            resolver = CredentialResolver(conn, self._cipher)
            credential = await resolver.resolve("codex")
            if not credential:
                raise RuntimeError("bench Codex connection is missing; reconnect and retry")
            write_back = CredentialWriteBack(conn, self._cipher)
            codex_home = Path(tempfile.mkdtemp(prefix="bench-codex-", dir=checkout.parent))
            try:
                auth = codex_home / "auth.json"
                auth.write_text(credential, encoding="utf-8")
                auth.chmod(0o600)
                pin_file_auth_storage(codex_home)
                spec = await self._run_one(
                    checkout=checkout,
                    codex_home=codex_home,
                    name="spec",
                    prompt=spec_prompt or _spec_prompt(),
                )
                standards = await self._run_one(
                    checkout=checkout,
                    codex_home=codex_home,
                    name="standards",
                    prompt=standards_prompt or _standards_prompt(),
                )
                refreshed = read_codex_credential(auth)
                if refreshed and refreshed != credential:
                    await write_back.write_back(
                        "codex",
                        refreshed,
                        expires_at=codex_expires_at(refreshed),
                        expected_prior=credential,
                    )
            finally:
                shutil.rmtree(codex_home, ignore_errors=True)
        finally:
            await conn.close()
        return review_metrics(spec=spec, standards=standards)

    async def _run_one(
        self,
        *,
        checkout: Path,
        codex_home: Path,
        name: str,
        prompt: str,
    ) -> ReviewResult:
        output = checkout.parent / f"final-{name}-review.json"
        await self._commands.run(
            [
                "codex",
                "exec",
                "--sandbox",
                "read-only",
                "--model",
                self._model,
                "-o",
                str(output),
                prompt,
            ],
            cwd=checkout,
            env={"CODEX_HOME": str(codex_home)},
        )
        try:
            text = output.read_text(encoding="utf-8")
        except OSError as exc:
            raise RuntimeError(f"final {name} reviewer wrote no result") from exc
        return parse_review(text)


_OUTPUT_CONTRACT = """
Return ONLY JSON with this exact shape:
{"findings":[{"severity":"Critical|Major|Minor","title":"short title",
"evidence":"path:line","explanation":"why this is a real problem"}]}
Use Critical only for data loss, security compromise, or a fundamentally unusable required flow;
Major for a materially broken requirement or invariant; Minor for a contained quality defect.
Do not include praise, summaries, markdown, or speculative findings. Use {"findings":[]} when clean.
"""


def _spec_prompt() -> str:
    tickets = "\n\n".join(
        f"# {ticket.title}\n{ticket.description}" for ticket in support_queue_campaign().tickets
    )
    return f"""You are the independent final SPEC reviewer for Support Queue.
Read the entire repository and its tests. Compare the merged implementation against every
requirement below. Do not modify files or run network operations. Report only concrete mismatches
that remain in the final main branch, with exact file evidence.

{tickets}

{_OUTPUT_CONTRACT}
"""


def _standards_prompt() -> str:
    return f"""You are the independent final STANDARDS reviewer for Support Queue.
Read the entire repository, especially STANDARDS.md, README.md, CI configuration, production code,
and tests. Do not judge product requirements here; judge only documented repository standards,
correctness hazards, maintainability, security, and test quality. Do not modify files or use the
network. Report only concrete defects with exact file evidence.

{_OUTPUT_CONTRACT}
"""


def final_review_prompts() -> tuple[str, str]:
    return _spec_prompt(), _standards_prompt()
