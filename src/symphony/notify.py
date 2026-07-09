"""Telegram push for attention-needed events (SYM-171).

One outbound Bot-API call per event, carrying the issue identifier and a
deep link back to the tracker page. Bot token + chat id come from the env
(`.env`); with either unset the notifier is a clean no-op.

Dedupe (so repeated polls don't re-fire) lives in `db.notifications`; this
module is just message formatting + the HTTP send.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import httpx

from .linear.templates import truncate_body

EVENT_OPERATOR_WAIT = "operator_wait"
EVENT_RUN_FAILED = "run_failed"
EVENT_PR_MERGED = "pr_merged"

# Telegram's Bot API `sendMessage` rejects text over 4096 characters with a
# 400; cap the built message (truncating `detail`, the field that can carry
# unbounded subprocess/git stderr) so we always stay under it.
MESSAGE_LIMIT = 4096

# Leading emoji per event so the phone push carries a glanceable signal
# (mirrors the Linear-template convention).
_HEADLINES = {
    EVENT_OPERATOR_WAIT: "🔔 Approval needed",
    EVENT_RUN_FAILED: "❌ Run failed",
    EVENT_PR_MERGED: "✅ PR merged",
}

SendFn = Callable[[str, str, str], Awaitable[None]]


def build_message(*, event: str, issue_identifier: str, issue_url: str, detail: str = "") -> str:
    """A short body: headline + identifier, optional detail, then the deep link.

    `detail` is truncated to whatever budget is left after the headline and
    the deep link, so the joined message never exceeds `MESSAGE_LIMIT`.
    """
    headline = _HEADLINES.get(event, "🔔 Attention needed")
    head = f"{headline}: {issue_identifier}"
    parts = [head]
    fixed_len = len(head.encode("utf-8"))
    if issue_url:
        fixed_len += len(f"\n{issue_url}".encode())
    if detail:
        detail_budget = MESSAGE_LIMIT - fixed_len - len(b"\n")
        detail = truncate_body(detail, limit=max(detail_budget, 0))
        if detail:
            parts.append(detail)
    if issue_url:
        parts.append(issue_url)
    return "\n".join(parts)


class TelegramSendError(RuntimeError):
    """The Bot API rejected `sendMessage` (non-2xx response).

    Carries only the status code and reason phrase. `httpx.HTTPStatusError.__str__`
    embeds the full request URL, which contains the bot token, so it must never
    be logged verbatim — this is what callers should log instead.
    """


async def _http_send(
    bot_token: str,
    chat_id: str,
    text: str,
    *,
    client: httpx.AsyncClient | None = None,
) -> None:
    """One POST to the Telegram Bot API `sendMessage`."""
    owns = client is None
    client = client or httpx.AsyncClient(timeout=10.0)
    try:
        resp = await client.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "disable_web_page_preview": False},
        )
        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError:
            raise TelegramSendError(
                f"Telegram sendMessage failed: {resp.status_code} {resp.reason_phrase}"
            ) from None
    finally:
        if owns:
            await client.aclose()


class TelegramNotifier:
    """Sends a message iff both token and chat id are configured."""

    def __init__(self, bot_token: str = "", chat_id: str = "", *, send_fn: SendFn | None = None):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self._send_fn = send_fn or _http_send

    @property
    def enabled(self) -> bool:
        return bool(self.bot_token and self.chat_id)

    async def send(self, text: str) -> bool:
        """Returns True if a message was sent, False if the notifier is off."""
        if not self.enabled:
            return False
        await self._send_fn(self.bot_token, self.chat_id, text)
        return True


__all__ = [
    "EVENT_OPERATOR_WAIT",
    "EVENT_PR_MERGED",
    "EVENT_RUN_FAILED",
    "TelegramNotifier",
    "TelegramSendError",
    "build_message",
]
