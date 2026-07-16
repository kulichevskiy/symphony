"""App-level credential encryption helper (OAuth in UI 1/7)."""

from __future__ import annotations

import pytest

from symphony.crypto import (
    CredentialCipher,
    CredentialDecryptError,
    CredentialKeyMissingError,
)


def test_encrypt_decrypt_round_trip() -> None:
    cipher = CredentialCipher("deployment-secret")
    token = cipher.encrypt("gho_secret_token")
    # Stored form is ciphertext, not the plaintext.
    assert token != b"gho_secret_token"
    assert b"gho_secret_token" not in token
    assert cipher.decrypt(token) == "gho_secret_token"


def test_same_secret_different_processes_decrypt() -> None:
    """A key derived from the same deployment secret round-trips across cipher
    instances (a fresh process must decrypt what an earlier one wrote)."""
    token = CredentialCipher("s").encrypt("payload")
    assert CredentialCipher("s").decrypt(token) == "payload"


def test_missing_key_is_clean_error_not_traceback() -> None:
    cipher = CredentialCipher("")
    assert cipher.available is False
    with pytest.raises(CredentialKeyMissingError):
        cipher.encrypt("x")
    with pytest.raises(CredentialKeyMissingError):
        cipher.decrypt(b"anything")


def test_rotated_key_surfaces_reauthorize_error() -> None:
    token = CredentialCipher("old-key").encrypt("payload")
    with pytest.raises(CredentialDecryptError):
        CredentialCipher("new-key").decrypt(token)


def test_from_env_reads_deployment_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYMPHONY_ENCRYPTION_KEY", "env-secret")
    cipher = CredentialCipher.from_env()
    assert cipher.available is True
    assert cipher.decrypt(cipher.encrypt("v")) == "v"

    monkeypatch.delenv("SYMPHONY_ENCRYPTION_KEY", raising=False)
    assert CredentialCipher.from_env().available is False
