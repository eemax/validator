from pathlib import Path

import pytest

from centric_mdm_validation.centric.config import (
    CONFIG_DIR_ENV_VAR,
    ConfigError,
    load_fetcher_settings,
    resolve_fetch_params_path,
    resolve_private_config_path,
)
from centric_mdm_validation.centric.schema import load_endpoint_schemas


def _write_config(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


def test_fetcher_config_excludes_base_url_and_auth(tmp_path: Path) -> None:
    config = _write_config(
        tmp_path / "fetcher.yml",
        """
base_url: https://centric.example.com
endpoints:
  - name: styles
    api_version: v2
    path: styles
""",
    )

    with pytest.raises(ConfigError, match="CENTRIC_BASE_URL"):
        load_fetcher_settings(config)


def test_fetcher_config_rejects_auth_block(tmp_path: Path) -> None:
    config = _write_config(
        tmp_path / "fetcher.yml",
        """
auth:
  username: user
endpoints:
  - name: styles
    api_version: v2
    path: styles
""",
    )

    with pytest.raises(ConfigError, match="environment variables"):
        load_fetcher_settings(config)


def test_load_fetcher_settings_keeps_endpoint_runtime_config(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv(CONFIG_DIR_ENV_VAR, raising=False)
    config = _write_config(
        tmp_path / "fetcher.yml",
        """
timeout: 12
retry_max_attempts: 2
retry_base_seconds: 1
retry_max_seconds: 2
jitter_ratio: 0.1
output_dir: data/raw
checkpoint_dir: data/checkpoints
endpoints:
  - name: styles
    api_version: v2
    path: styles
    query_params:
      active: true
    skip_param: skip
    limit_param: limit
    limit: 25
    item_path: $.items
    count_spec:
      api_version: v2
      path: styles/count
      result_path: $.total
""",
    )

    fetcher_cfg, auth_settings, endpoints = load_fetcher_settings(config)

    assert fetcher_cfg.base_url == ""
    assert fetcher_cfg.timeout == 12
    assert fetcher_cfg.output_dir == Path("data/raw")
    assert fetcher_cfg.checkpoint_dir == Path("data/checkpoints")
    assert auth_settings.env_file == Path(".local/local.env")
    assert len(endpoints) == 1
    assert endpoints[0].name == "styles"
    assert endpoints[0].item_path == "$.items"
    assert endpoints[0].count_spec is not None


def test_default_env_file_comes_from_config_dir(tmp_path: Path, monkeypatch) -> None:
    config_dir = tmp_path / "centric-config"
    monkeypatch.setenv(CONFIG_DIR_ENV_VAR, str(config_dir))
    config = _write_config(
        tmp_path / "fetcher.yml",
        """
endpoints:
  - name: styles
    api_version: v2
    path: styles
""",
    )

    _, auth_settings, _ = load_fetcher_settings(config)

    assert auth_settings.env_file == config_dir / "local.env"


def test_simple_env_file_name_is_resolved_from_config_dir(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config_dir = tmp_path / "centric-config"
    monkeypatch.setenv(CONFIG_DIR_ENV_VAR, str(config_dir))
    config = _write_config(
        tmp_path / "fetcher.yml",
        """
env_file: private.env
endpoints:
  - name: styles
    api_version: v2
    path: styles
""",
    )

    _, auth_settings, _ = load_fetcher_settings(config)

    assert auth_settings.env_file == config_dir / "private.env"


def test_load_fetcher_settings_applies_private_params_overlay(tmp_path: Path) -> None:
    config = _write_config(
        tmp_path / "fetcher.yml",
        """
endpoints:
  - name: styles
    api_version: v2
    path: styles
    query_params:
      active: true
    count_spec:
      api_version: v2
      path: count/Style
      query_params:
        active: true
      result_path: $.count
""",
    )
    params = _write_config(
        tmp_path / "fetch-params.yml",
        """
endpoints:
  styles:
    query_params:
      custom_status: approved
    count_query_params:
      custom_status: approved
""",
    )

    _, _, endpoints = load_fetcher_settings(config, params_path=params)

    assert endpoints[0].query_params == {"active": True, "custom_status": "approved"}
    assert endpoints[0].count_spec is not None
    assert endpoints[0].count_spec.query_params == {
        "active": True,
        "custom_status": "approved",
    }


def test_repo_config_fetches_product_sizes_as_sizes(monkeypatch) -> None:
    monkeypatch.delenv(CONFIG_DIR_ENV_VAR, raising=False)

    _, _, endpoints = load_fetcher_settings(Path("config/fetcher.yml"))
    endpoint_by_name = {endpoint.name: endpoint for endpoint in endpoints}
    sizes = endpoint_by_name["sizes"]

    assert sizes.path == "product_sizes"
    assert sizes.count_spec is not None
    assert sizes.count_spec.path == "count/ProductSize"


def test_repo_endpoint_schema_includes_sizes() -> None:
    schemas = load_endpoint_schemas(Path("config/endpoint-schema.yml"))

    assert schemas["sizes"].primary_key == "id"
    assert schemas["sizes"].delete_field == "active"
    assert schemas["sizes"].full_snapshot_mode == "upsert_only"


def test_private_config_path_prefers_explicit_then_config_dir(
    tmp_path: Path,
    monkeypatch,
) -> None:
    explicit = tmp_path / "explicit.yml"
    config_dir = tmp_path / "centric-config"
    monkeypatch.setenv(CONFIG_DIR_ENV_VAR, str(config_dir))

    assert resolve_private_config_path("fetch-params.yml", explicit) == explicit
    assert resolve_private_config_path("fetch-params.yml") == config_dir / "fetch-params.yml"


def test_fetch_params_path_uses_config_dir_then_local_when_present(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    config_dir = tmp_path / "centric-config"
    config_dir.mkdir()
    params = _write_config(config_dir / "fetch-params.yml", "endpoints: {}\n")
    monkeypatch.setenv(CONFIG_DIR_ENV_VAR, str(config_dir))

    assert resolve_fetch_params_path() == params

    monkeypatch.delenv(CONFIG_DIR_ENV_VAR)
    local_params = tmp_path / ".local" / "fetch-params.yml"
    local_params.parent.mkdir()
    _write_config(local_params, "endpoints: {}\n")

    assert resolve_fetch_params_path() == Path(".local/fetch-params.yml")
