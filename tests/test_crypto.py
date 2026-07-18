"""App-level credential encryption helper (OAuth in UI 1/7)."""

from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from symphony.crypto import (
    KEY_FILE_NAME,
    CredentialCipher,
    CredentialDecryptError,
    CredentialKeyMissingError,
    key_fingerprint,
    resolve_encryption_key,
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


def test_resolve_key_explicit_wins_and_writes_nothing(tmp_path: Path) -> None:
    assert resolve_encryption_key("explicit-secret", tmp_path) == "explicit-secret"
    assert not (tmp_path / KEY_FILE_NAME).exists()


def test_resolve_key_generates_once_with_0600(tmp_path: Path) -> None:
    key = resolve_encryption_key("", tmp_path)
    key_path = tmp_path / KEY_FILE_NAME
    assert key and key_path.exists()
    assert stat.S_IMODE(os.stat(key_path).st_mode) == 0o600
    # Reused verbatim on the next boot — not regenerated.
    assert resolve_encryption_key("", tmp_path) == key


def test_resolve_key_reuses_existing_file(tmp_path: Path) -> None:
    (tmp_path / KEY_FILE_NAME).write_text("preexisting-key\n", encoding="utf-8")
    assert resolve_encryption_key("", tmp_path) == "preexisting-key"


def test_resolve_key_regenerates_over_empty_file(tmp_path: Path) -> None:
    (tmp_path / KEY_FILE_NAME).write_text("", encoding="utf-8")
    key = resolve_encryption_key("", tmp_path)
    assert key
    assert (tmp_path / KEY_FILE_NAME).read_text(encoding="utf-8").strip() == key


def test_key_fingerprint_stable_and_never_the_key() -> None:
    fp = key_fingerprint("deployment-secret")
    assert fp == key_fingerprint("deployment-secret")
    assert len(fp) == 12
    assert "deployment-secret" not in fp
    assert key_fingerprint("") == ""
