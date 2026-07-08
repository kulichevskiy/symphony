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
from fastapi import APIRouter, Depends, HTTPException
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

    async def _fetch_signing_keys(self) -> dict[str, dict[str, Any]]:
        if self._client is not None:
            resp = await self._client.get(self._settings.jwks_uri)
        else:
            async with httpx.AsyncClient() as client:
                resp = await client.get(self._settings.jwks_uri)
        resp.raise_for_status()
        data = resp.json()
        return {key["kid"]: key for key in data.get("keys", []) if "kid" in key}

    async def _signing_keys(self) -> dict[str, dict[str, Any]]:
        if self._keys is None:
            self._keys = await self._fetch_signing_keys()
        return self._keys

    async def verify(self, token: str) -> dict[str, Any]:
        """Return validated claims, or raise ``HTTPException(401)`` on any failure."""
        try:
            header = jwt.get_unverified_header(token)
        except jwt.InvalidTokenError as exc:
            raise HTTPException(status_code=401, detail="malformed token") from exc
        kid = header.get("kid")
        if not isinstance(kid, str):
            raise HTTPException(status_code=401, detail="unknown signing key")
        keys = await self._signing_keys()
        if kid not in keys:
            # Auth0 rotates signing keys without notice; refetch once before
            # rejecting so a token signed with a newly-rotated key still verifies.
            self._keys = keys = await self._fetch_signing_keys()
        jwk = keys.get(kid)
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
        if claims.get("email_verified") is not True:
            raise HTTPException(status_code=403, detail="email not verified")
        email = claims.get("email")
        if not isinstance(email, str) or email.strip().casefold() not in settings.allowed_emails:
            raise HTTPException(status_code=403, detail="email not allowlisted")
        return claims

    return require_auth


def create_auth_config_router(settings: Auth0Settings | None) -> APIRouter:
    """Unauthenticated endpoint the SPA reads at startup to decide whether to
    run the Auth0 login flow before calling the gated ``/api/*`` routes.

    Only the public SPA client_id/domain are exposed here — never the email
    allowlist.
    """
    router = APIRouter()

    @router.get("/api/auth-config")
    async def auth_config() -> dict[str, Any]:
        if settings is None:
            return {"enabled": False}
        return {"enabled": True, "domain": settings.domain, "client_id": settings.client_id}

    return router
