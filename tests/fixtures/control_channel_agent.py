"""Stub agent for the runner's control-channel tests.

Stands in for `claude --input-format stream-json` on the wire, and only on the
wire: it reads its prompt as a message on stdin, optionally asks the host for a
token through a `control_request`, reports what it got back, and then — like the
real CLI — keeps waiting for the next message instead of exiting. Only EOF on
stdin ends it. That last part is the point: a runner that forgets to close stdin
leaves this process hanging exactly the way it would leave a real one.

Flags:
  --ask-token   emit one `control_request` and block until it is answered.
                Repeatable: one 401 recovery produced three requests in ~1.2s
                in the SYM-232 spike, so a run has to survive a burst of them.
  --linger      after a refusal, sleep past any sane test timeout, standing in
                for the real CLI waiting out its own retry window
  --deaf        ignore SIGTERM, so only SIGKILL can end this process
  --malformed   emit a frame whose `type` is a list, not a string
"""

from __future__ import annotations

import json
import signal
import sys
import time

LINGER_SECS = 30


def emit(event: dict[str, object]) -> None:
    sys.stdout.write(json.dumps(event) + "\n")
    sys.stdout.flush()


def prompt_text(line: str) -> str:
    return str(json.loads(line)["message"]["content"][0]["text"])


def ask_for_token(argv: list[str], nth: int) -> bool:
    """Ask once. False once the answer means there is no point asking again."""
    emit(
        {
            "type": "control_request",
            "request_id": f"req-{nth}",
            "request": {"subtype": "oauth_token_refresh"},
        }
    )
    reply = sys.stdin.readline()
    if not reply:
        emit({"type": "assistant", "text": "token:eof"})
        return False
    response = json.loads(reply)["response"]
    if response.get("subtype") == "success":
        emit({"type": "assistant", "text": "token:" + response["response"]["accessToken"]})
        return True
    emit({"type": "assistant", "text": "token:refused"})
    if "--linger" in argv:
        time.sleep(LINGER_SECS)
    return False


def main() -> int:
    argv = sys.argv[1:]
    if "--deaf" in argv:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    if "--malformed" in argv:
        emit({"type": ["control_request"], "text": "not a string type"})
    line = sys.stdin.readline()
    if not line:
        emit({"type": "assistant", "text": "prompt:none"})
        emit({"type": "result", "result": "SYMPHONY_DONE"})
        return 0
    emit({"type": "assistant", "text": "prompt:" + prompt_text(line)})
    for nth in range(1, argv.count("--ask-token") + 1):
        if not ask_for_token(argv, nth):
            break
    emit({"type": "result", "result": "SYMPHONY_DONE"})
    for _ in sys.stdin:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
