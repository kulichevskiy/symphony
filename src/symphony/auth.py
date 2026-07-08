"""Auth0 ID-token gate for the ``/api/*`` surface.

Validates the Auth0 ID token (a JWT presented as ``Authorization: Bearer``)
against the tenant JWKS — RS256 signature, ``iss``, and ``aud == client_id`` —
then enforces an email allowlist against the ``email`` claim. The ``aud``/``iss``
checks are what make the ID-token path safe for a single-user tool: a token
minted for any other app or tenant fails verification.

Webhook routes verify their own HMAC and stay outside this gate.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import httpx
import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer


@dataclass(frozen=True)
class Auth0Settings:
    """Auth0 tenant config for validating ID tokens. All fields from ``.env``."""

    domain: str
    client_id: str
    allowed_emails: frozenset[str]

    @classmethod
    def from_env(cls, *, domain: str, client_id: str, allowed_emails: str) -> Auth0Settings:
        """Build settings from raw env strings, normalizing the comma-separated allowlist."""
        emails = frozenset(
            item.strip().casefold() for item in allowed_emails.split(",") if item.strip()
        )
        return cls(domain=domain.strip(), client_id=client_id.strip(), allowed_emails=emails)

    @property
    def issuer(self) -> str:
        return f"https://{self.domain}/"

    @property
    def jwks_uri(self) -> str:
        return f"https://{self.domain}/.well-known/jwks.json"


class Auth0Verifier:
    """Fetches the tenant JWKS (cached) and verifies ID tokens against it."""

    def __init__(self, settings: Auth0Settings, *, client: httpx.AsyncClient | None = None) -> None:
        self._settings = settings
        self._client = client
        self._keys: dict[str, dict[str, Any]] | None = None

    async def _signing_keys(self) -> dict[str, dict[str, Any]]:
        if self._keys is None:
            if self._client is not None:
                resp = await self._client.get(self._settings.jwks_uri)
            else:
                async with httpx.AsyncClient() as client:
                    resp = await client.get(self._settings.jwks_uri)
            resp.raise_for_status()
            data = resp.json()
            self._keys = {key["kid"]: key for key in data.get("keys", []) if "kid" in key}
        return self._keys

    async def verify(self, token: str) -> dict[str, Any]:
        """Return validated claims, or raise ``HTTPException(401)`` on any failure."""
        try:
            header = jwt.get_unverified_header(token)
        except jwt.InvalidTokenError as exc:
            raise HTTPException(status_code=401, detail="malformed token") from exc
        kid = header.get("kid")
        jwk = (await self._signing_keys()).get(kid) if isinstance(kid, str) else None
        if jwk is None:
            raise HTTPException(status_code=401, detail="unknown signing key")
        public_key = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(jwk))
        try:
            claims: dict[str, Any] = jwt.decode(
                token,
                public_key,  # type: ignore[arg-type]
                algorithms=["RS256"],
                audience=self._settings.client_id,
                issuer=self._settings.issuer,
            )
        except jwt.InvalidTokenError as exc:
            raise HTTPException(status_code=401, detail="invalid token") from exc
        return claims


def create_auth_dependency(
    settings: Auth0Settings, *, client: httpx.AsyncClient | None = None
) -> Callable[..., Awaitable[dict[str, Any]]]:
    """Build the FastAPI dependency enforcing token validity + email allowlist."""
    verifier = Auth0Verifier(settings, client=client)
    bearer = HTTPBearer(auto_error=False)

    async def require_auth(
        credentials: HTTPAuthorizationCredentials | None = Depends(bearer),  # noqa: B008
    ) -> dict[str, Any]:
        if credentials is None or not credentials.credentials:
            raise HTTPException(status_code=401, detail="missing bearer token")
        claims = await verifier.verify(credentials.credentials)
        email = claims.get("email")
        if not isinstance(email, str) or email.strip().casefold() not in settings.allowed_emails:
            raise HTTPException(status_code=403, detail="email not allowlisted")
        return claims

    return require_auth
