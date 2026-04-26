from pathlib import Path

import pytest

from centric_mdm_validation.centric.config import ConfigError, load_fetcher_settings


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


def test_load_fetcher_settings_keeps_endpoint_runtime_config(tmp_path: Path) -> None:
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
env_file: config/local.env
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
    assert auth_settings.env_file == Path("config/local.env")
    assert len(endpoints) == 1
    assert endpoints[0].name == "styles"
    assert endpoints[0].item_path == "$.items"
    assert endpoints[0].count_spec is not None
