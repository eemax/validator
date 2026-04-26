from pathlib import Path

import httpx
import pytest

from centric_mdm_validation.centric.auth import AuthContext
from centric_mdm_validation.centric.fetcher import FetchError, run_endpoint
from centric_mdm_validation.centric.models import CountSpec, EndpointSpec, FetcherConfig


@pytest.fixture
def no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("centric_mdm_validation.centric.fetcher._sleep_backoff", lambda *_: None)


def _make_auth_ctx(tmp_path: Path, handler, *, token: str = "token") -> AuthContext:
    client = httpx.Client(transport=httpx.MockTransport(handler), timeout=5.0)
    return AuthContext(
        base_url="https://centric.example.com",
        username="user",
        password="pass",
        timeout=5.0,
        initial_token=token,
        client=client,
    )


def _make_fetcher_cfg(tmp_path: Path, **overrides) -> FetcherConfig:
    cfg = FetcherConfig(
        output_dir=tmp_path / "output",
        checkpoint_dir=tmp_path / "checkpoints",
    )
    for key, value in overrides.items():
        setattr(cfg, key, value)
    return cfg


def test_pagination_uses_skip_limit_and_stops_on_short_page(tmp_path: Path, no_sleep: None) -> None:
    seen_skips: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v2/items":
            skip = int(request.url.params["skip"])
            limit = int(request.url.params["limit"])
            seen_skips.append(skip)
            if skip == 0:
                return httpx.Response(200, json=[{"id": i} for i in range(limit)])
            if skip == limit:
                return httpx.Response(200, json=[{"id": limit + i} for i in range(20)])
            return httpx.Response(200, json=[])
        return httpx.Response(404)

    endpoint = EndpointSpec(name="items", api_version="v2", path="items", limit=50)
    ctx = _make_auth_ctx(tmp_path, handler)
    cfg = _make_fetcher_cfg(tmp_path)

    result = run_endpoint(endpoint, ctx, cfg, resume=False)

    assert seen_skips == [0, 50]
    assert result.pages_fetched == 2
    assert result.items_fetched == 70
    assert result.next_skip == 100
    output_lines = (cfg.output_dir / "items.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(output_lines) == 70


def test_count_preflight_and_id_validation_pass(tmp_path: Path, no_sleep: None) -> None:
    seen_skips: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v2/items/count":
            return httpx.Response(200, json={"total": 50})
        if request.url.path == "/api/v2/items":
            skip = int(request.url.params["skip"])
            limit = int(request.url.params["limit"])
            seen_skips.append(skip)
            return httpx.Response(200, json=[{"id": i + skip} for i in range(limit)])
        return httpx.Response(404)

    endpoint = EndpointSpec(
        name="items",
        api_version="v2",
        path="items",
        limit=50,
        count_spec=CountSpec(api_version="v2", path="items/count", result_path="$.total"),
    )
    ctx = _make_auth_ctx(tmp_path, handler)
    cfg = _make_fetcher_cfg(tmp_path)

    result = run_endpoint(endpoint, ctx, cfg, resume=False)

    assert seen_skips == [0]
    assert result.expected_count == 50
    assert result.items_fetched == 50
    assert result.count_validation_status == "passed"
    assert result.id_validation_status == "passed"
    assert result.id_validation_checked_items == 50
    assert result.id_validation_unique_ids == 50


def test_duplicate_ids_fail_integrity_when_count_matches(tmp_path: Path, no_sleep: None) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v2/items/count":
            return httpx.Response(200, json={"total": 2})
        if request.url.path == "/api/v2/items":
            return httpx.Response(200, json=[{"id": "A-1"}, {"id": "A-1"}])
        return httpx.Response(404)

    endpoint = EndpointSpec(
        name="items",
        api_version="v2",
        path="items",
        count_spec=CountSpec(api_version="v2", path="items/count", result_path="$.total"),
    )
    ctx = _make_auth_ctx(tmp_path, handler)
    cfg = _make_fetcher_cfg(tmp_path)

    with pytest.raises(FetchError, match="duplicate ids=1"):
        run_endpoint(endpoint, ctx, cfg, resume=False)
