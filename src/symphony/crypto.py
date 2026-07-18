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
import logging
import os
import secrets
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken

log = logging.getLogger(__name__)

_ENV_VAR = "SYMPHONY_ENCRYPTION_KEY"
# Auto-provisioned key file, living next to the DB in the data volume so it
# survives redeploys with the data it protects (Config v2 2/9).
KEY_FILE_NAME = ".encryption_key"


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


class EncryptionKeyLostError(Exception):
    """Encrypted credential rows exist but the effective key cannot decrypt
    them — the key was lost or rotated. Raised at boot so the failure is a
    loud, instructive crash instead of silent OAuth 503s at runtime."""

    def __init__(self, providers: list[str]) -> None:
        super().__init__(
            "the effective encryption key cannot decrypt the stored credentials "
            f"for: {', '.join(providers)}. Restore the original {_ENV_VAR} (or the "
            f"{KEY_FILE_NAME} file in the data volume), or delete the stored "
            "connections and re-authorize each provider in the UI."
        )
        self.providers = providers


def resolve_encryption_key(explicit_key: str, data_dir: Path) -> str:
    """The deployment's effective encryption key (Config v2 2/9).

    An explicit key (env/.env `SYMPHONY_ENCRYPTION_KEY`) always wins and never
    touches the filesystem. Otherwise the key file in `data_dir` is used,
    generated on first boot (0600) so a fresh install needs no manual key
    provisioning. The key value itself is never logged."""
    explicit = explicit_key.strip()
    if explicit:
        return explicit
    key_path = data_dir / KEY_FILE_NAME
    try:
        existing = key_path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        existing = ""
    if existing:
        return existing
    key = secrets.token_hex(32)
    data_dir.mkdir(parents=True, exist_ok=True)
    # Atomic create-exclusive via a private temp file + hard link: two
    # overlapping first boots (deploy recreate on a shared volume) must agree
    # on one key — the loser reads the winner's file instead of silently
    # overwriting it with a different secret.
    tmp_path = data_dir / f".encryption_key.tmp-{secrets.token_hex(4)}"
    fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(key + "\n")
    try:
        os.link(tmp_path, key_path)
    except FileExistsError:
        raced = key_path.read_text(encoding="utf-8").strip()
        if raced:
            os.unlink(tmp_path)
            return raced
        # Existing but empty (a truncated earlier write): replace atomically.
        os.replace(tmp_path, key_path)
    else:
        os.unlink(tmp_path)
    log.info(
        "generated a new credential encryption key at %s (fingerprint %s)",
        key_path,
        key_fingerprint(key),
    )
    return key


def key_fingerprint(key: str) -> str:
    """Short non-reversible identifier for the effective key — safe to log and
    to show in the UI so an operator can tell which key an instance runs."""
    if not key:
        return ""
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:12]


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
