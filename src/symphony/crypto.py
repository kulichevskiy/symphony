"""App-level encryption for stored OAuth credentials (OAuth in UI 1/7).

Credentials for the four onboarding providers (GitHub, Linear, Claude, Codex)
are stored encrypted-at-rest in `oauth_connections`. A single deployment secret
(`SYMPHONY_ENCRYPTION_KEY`) keys a Fernet cipher: encrypt-on-write,
decrypt-on-read. The raw secret can be any string — it's stretched to a valid
32-byte Fernet key via SHA-256 — so an operator isn't forced to hand-generate a
base64 key.

A missing key (never configured) or a rotated/corrupt key (ciphertext no longer
decrypts) is surfaced as an explicit, catchable error the API layer renders as
"must re-authorize", never a raw traceback.
"""

from __future__ import annotations

import base64
import hashlib
import os

from cryptography.fernet import Fernet, InvalidToken

_ENV_VAR = "SYMPHONY_ENCRYPTION_KEY"


class CredentialKeyMissingError(Exception):
    """No encryption key is configured, so credentials can't be encrypted or
    decrypted. The operator must set `SYMPHONY_ENCRYPTION_KEY` and re-authorize
    each provider."""

    def __init__(self) -> None:
        super().__init__(
            f"credential encryption key ({_ENV_VAR}) is not configured — "
            "set it and re-authorize each provider"
        )


class CredentialDecryptError(Exception):
    """Stored ciphertext could not be decrypted with the current key (key
    rotated, or data corrupt). The connection must be treated as lost — the
    operator must re-authorize the provider."""

    def __init__(self) -> None:
        super().__init__(
            "stored credential could not be decrypted with the current "
            "encryption key — re-authorize the provider"
        )


def _derive_key(secret: str) -> bytes:
    """Stretch any deployment-secret string into a valid urlsafe-base64 Fernet
    key (32 bytes) via SHA-256."""
    return base64.urlsafe_b64encode(hashlib.sha256(secret.encode("utf-8")).digest())


class CredentialCipher:
    """Fernet-backed encrypt/decrypt for credential payloads. An empty key
    leaves the cipher unavailable — every operation then raises
    `CredentialKeyMissingError` rather than a raw traceback."""

    def __init__(self, key: str) -> None:
        self._fernet = Fernet(_derive_key(key)) if key else None

    @classmethod
    def from_env(cls) -> CredentialCipher:
        return cls(os.environ.get(_ENV_VAR, "").strip())

    @property
    def available(self) -> bool:
        return self._fernet is not None

    def encrypt(self, plaintext: str) -> bytes:
        if self._fernet is None:
            raise CredentialKeyMissingError()
        return self._fernet.encrypt(plaintext.encode("utf-8"))

    def decrypt(self, token: bytes) -> str:
        if self._fernet is None:
            raise CredentialKeyMissingError()
        try:
            return self._fernet.decrypt(token).decode("utf-8")
        except InvalidToken as exc:
            raise CredentialDecryptError() from exc
