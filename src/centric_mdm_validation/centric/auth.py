from __future__ import annotations

import os
from dataclasses import replace
from pathlib import Path
from typing import Any

import httpx

from .models import AuthSettings


class AuthError(RuntimeError):
    pass


RequestParams = dict[str, Any] | list[tuple[str, Any]]


def _normalize_base_url(value: str) -> str:
    return value.rstrip("/")


def _read_env_file(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}

    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            raise AuthError(f"Invalid .env line {line_number} in {path}: expected KEY=value.")
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            raise AuthError(f"Invalid .env line {line_number} in {path}: empty key.")
        values[key] = _strip_env_quotes(value.strip())
    return values


def _strip_env_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _env_value(key: str, env_file_values: dict[str, str]) -> str | None:
    value = os.environ.get(key)
    if value is None:
        value = env_file_values.get(key)
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def resolve_credentials(
    settings: AuthSettings,
    env_file: Path | None = None,
) -> tuple[str, str | None, str | None, str | None]:
    env_values = _read_env_file(env_file or settings.env_file)
    base_url = _env_value("CENTRIC_BASE_URL", env_values)
    username = _env_value("CENTRIC_USERNAME", env_values)
    password = _env_value("CENTRIC_PASSWORD", env_values)
    token = _env_value("CENTRIC_TOKEN", env_values)

    if not base_url:
        raise AuthError("Missing CENTRIC_BASE_URL in environment or env file.")
    return _normalize_base_url(base_url), username, password, token


def _extract_token(token_value: str) -> str:
    token = token_value.strip()
    if "=" in token:
        token = token.split("=", 1)[1].strip()
    if not token:
        raise AuthError("Received empty token from session endpoint.")
    return token


class AuthContext:
    def __init__(
        self,
        *,
        base_url: str,
        username: str | None,
        password: str | None,
        timeout: float,
        initial_token: str | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        self.base_url = _normalize_base_url(base_url)
        self.username = username
        self.password = password
        self.timeout = timeout
        self.client = client or httpx.Client(timeout=timeout)
        self._owns_client = client is None
        self.token = initial_token

    def close(self) -> None:
        if self._owns_client:
            self.client.close()

    def __enter__(self) -> AuthContext:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def ensure_token(self) -> str:
        if self.token:
            return self.token
        if not self.username or not self.password:
            raise AuthError("Token missing and CENTRIC_USERNAME/CENTRIC_PASSWORD unavailable.")
        self.token = self.get_token()
        return self.token

    def get_token(self) -> str:
        url = f"{self.base_url}/api/v2/session"
        response = self.client.post(
            url,
            headers={"Content-Type": "application/json"},
            json={"username": self.username, "password": self.password},
        )
        if response.status_code >= 400:
            raise AuthError(
                f"Session auth failed with status {response.status_code}: {response.text}"
            )
        payload = response.json()
        if not isinstance(payload, dict) or "token" not in payload:
            raise AuthError("Session auth response missing token field.")
        return _extract_token(str(payload["token"]))

    def refresh_token(self) -> str:
        self.token = None
        return self.ensure_token()

    def request(
        self,
        method: str,
        url: str,
        *,
        params: RequestParams | None = None,
        json_body: Any = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        token = self.ensure_token()
        merged_headers: dict[str, str] = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        if headers:
            merged_headers.update(headers)

        response = self.client.request(
            method,
            url,
            params=params,
            json=json_body,
            headers=merged_headers,
        )

        if response.status_code == 401:
            token = self.refresh_token()
            merged_headers["Authorization"] = f"Bearer {token}"
            response = self.client.request(
                method,
                url,
                params=params,
                json=json_body,
                headers=merged_headers,
            )

        return response


def init_auth_context(
    settings: AuthSettings,
    *,
    timeout: float | None = None,
    env_file: Path | None = None,
    client: httpx.Client | None = None,
) -> AuthContext:
    merged = replace(settings)
    if timeout is not None:
        merged.timeout = timeout
    if env_file is not None:
        merged.env_file = env_file

    base_url, username, password, token = resolve_credentials(merged, env_file=merged.env_file)
    return AuthContext(
        base_url=base_url,
        username=username,
        password=password,
        timeout=merged.timeout,
        initial_token=token,
        client=client,
    )
