from pathlib import Path

import httpx

from centric_mdm_validation.centric.auth import AuthContext, init_auth_context, resolve_credentials
from centric_mdm_validation.centric.models import AuthSettings


def test_resolve_credentials_reads_dotenv_without_config_file(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "CENTRIC_BASE_URL=https://file.example.com",
                "CENTRIC_USERNAME=file_user",
                "CENTRIC_PASSWORD=file_pass",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.delenv("CENTRIC_BASE_URL", raising=False)
    monkeypatch.delenv("CENTRIC_USERNAME", raising=False)
    monkeypatch.delenv("CENTRIC_PASSWORD", raising=False)

    base_url, username, password = resolve_credentials(AuthSettings(env_file=env_file))

    assert base_url == "https://file.example.com"
    assert username == "file_user"
    assert password == "file_pass"


def test_environment_overrides_dotenv(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "CENTRIC_BASE_URL=https://file.example.com\nCENTRIC_USERNAME=file_user\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("CENTRIC_BASE_URL", "https://env.example.com")
    monkeypatch.setenv("CENTRIC_USERNAME", "env_user")
    monkeypatch.setenv("CENTRIC_PASSWORD", "env_pass")

    base_url, username, password = resolve_credentials(AuthSettings(env_file=env_file))

    assert base_url == "https://env.example.com"
    assert username == "env_user"
    assert password == "env_pass"


def test_auth_context_keeps_token_in_memory_and_refreshes_on_401(tmp_path: Path) -> None:
    session_calls = 0
    seen_auth_headers: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal session_calls
        if request.url.path == "/api/v2/session":
            session_calls += 1
            token = "first_token" if session_calls == 1 else "fresh_token"
            return httpx.Response(200, json={"token": f"token={token}"})

        auth = request.headers.get("Authorization", "")
        seen_auth_headers.append(auth)
        if auth == "Bearer first_token":
            return httpx.Response(401, json={"error": "expired"})
        return httpx.Response(200, json={"ok": True})

    client = httpx.Client(transport=httpx.MockTransport(handler), timeout=5.0)
    ctx = AuthContext(
        base_url="https://centric.example.com",
        username="user",
        password="pass",
        timeout=5.0,
        client=client,
    )

    response = ctx.request("GET", "https://centric.example.com/api/v2/items")

    assert response.status_code == 200
    assert session_calls == 2
    assert ctx.token == "fresh_token"
    assert seen_auth_headers == ["Bearer first_token", "Bearer fresh_token"]
    assert not list(tmp_path.iterdir())


def test_init_auth_context_accepts_env_file(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "CENTRIC_BASE_URL=https://file.example.com",
                "CENTRIC_USERNAME=file_user",
                "CENTRIC_PASSWORD=file_pass",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("CENTRIC_BASE_URL", raising=False)
    monkeypatch.delenv("CENTRIC_USERNAME", raising=False)
    monkeypatch.delenv("CENTRIC_PASSWORD", raising=False)

    ctx = init_auth_context(AuthSettings(), env_file=env_file)
    try:
        assert ctx.base_url == "https://file.example.com"
        assert ctx.username == "file_user"
        assert ctx.password == "file_pass"
        assert ctx.token is None
    finally:
        ctx.close()
