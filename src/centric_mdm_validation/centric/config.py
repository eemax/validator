from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from .models import AuthSettings, CountSpec, EndpointSpec, FetcherConfig


class ConfigError(ValueError):
    pass


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        import yaml  # type: ignore
    except Exception as exc:
        raise ConfigError("YAML config requested but PyYAML is not installed.") from exc

    with path.open("r", encoding="utf-8") as fh:
        payload = yaml.safe_load(fh)
    if not isinstance(payload, dict):
        raise ConfigError("Config file root must be an object.")
    return payload


def _load_payload(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ConfigError(f"Config file not found: {path}")

    suffix = path.suffix.lower()
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
    elif suffix in {".yaml", ".yml"}:
        payload = _load_yaml(path)
    else:
        raise ConfigError("Config file must be JSON or YAML.")

    if not isinstance(payload, dict):
        raise ConfigError("Config file root must be an object.")
    return payload


def _as_dict(value: Any, *, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ConfigError(f"{field_name} must be an object.")
    return value


def _as_list(value: Any, *, field_name: str) -> list[Any]:
    if not isinstance(value, list):
        raise ConfigError(f"{field_name} must be an array.")
    return value


def _as_version(value: Any, *, field_name: str) -> str:
    if not isinstance(value, str) or value not in {"v2", "v3"}:
        raise ConfigError(f"{field_name} must be 'v2' or 'v3'.")
    return value


def _as_path(value: Any, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{field_name} must be a non-empty string.")
    return value.strip().strip("/")


def _as_json_path(value: Any, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{field_name} must be a non-empty JSON path string.")
    if not value.startswith("$"):
        raise ConfigError(f"{field_name} must start with '$'.")
    return value


def _as_positive_int(value: Any, *, field_name: str, default: int) -> int:
    if value is None:
        return default
    if not isinstance(value, int) or value <= 0:
        raise ConfigError(f"{field_name} must be a positive integer.")
    return value


def _build_count_spec(raw: dict[str, Any]) -> CountSpec:
    api_version = _as_version(raw.get("api_version"), field_name="count_spec.api_version")
    path = _as_path(raw.get("path"), field_name="count_spec.path")
    query_params = _as_dict(raw.get("query_params"), field_name="count_spec.query_params")
    result_path = _as_json_path(
        raw.get("result_path", "$.total"),
        field_name="count_spec.result_path",
    )
    return CountSpec(
        api_version=api_version,
        path=path,
        query_params=query_params,
        result_path=result_path,
    )


def _build_endpoint_spec(raw: dict[str, Any]) -> EndpointSpec:
    name = raw.get("name")
    if not isinstance(name, str) or not name.strip():
        raise ConfigError("endpoint.name must be a non-empty string.")

    api_version = _as_version(raw.get("api_version"), field_name=f"endpoint[{name}].api_version")
    path = _as_path(raw.get("path"), field_name=f"endpoint[{name}].path")
    query_params = _as_dict(raw.get("query_params"), field_name=f"endpoint[{name}].query_params")

    skip_param = raw.get("skip_param", "skip")
    limit_param = raw.get("limit_param", "limit")
    if not isinstance(skip_param, str) or not skip_param.strip():
        raise ConfigError(f"endpoint[{name}].skip_param must be a non-empty string.")
    if not isinstance(limit_param, str) or not limit_param.strip():
        raise ConfigError(f"endpoint[{name}].limit_param must be a non-empty string.")

    limit = _as_positive_int(raw.get("limit", 50), field_name=f"endpoint[{name}].limit", default=50)
    item_path = _as_json_path(raw.get("item_path", "$"), field_name=f"endpoint[{name}].item_path")

    count_spec_raw = raw.get("count_spec")
    if count_spec_raw is not None and not isinstance(count_spec_raw, dict):
        raise ConfigError(f"endpoint[{name}].count_spec must be an object.")
    count_spec = _build_count_spec(count_spec_raw) if isinstance(count_spec_raw, dict) else None

    return EndpointSpec(
        name=name.strip(),
        api_version=api_version,
        path=path,
        query_params=query_params,
        skip_param=skip_param.strip(),
        limit_param=limit_param.strip(),
        limit=limit,
        item_path=item_path,
        count_spec=count_spec,
    )


def _build_fetcher_config(raw: dict[str, Any]) -> FetcherConfig:
    if "base_url" in raw:
        raise ConfigError("base_url belongs in CENTRIC_BASE_URL or .env, not fetcher config.")
    if "auth" in raw:
        raise ConfigError("auth settings belong in CENTRIC_* environment variables or .env.")

    timeout = raw.get("timeout", 30.0)
    retry_max_attempts = raw.get("retry_max_attempts", 5)
    retry_base_seconds = raw.get("retry_base_seconds", 0.5)
    retry_max_seconds = raw.get("retry_max_seconds", 8.0)
    jitter_ratio = raw.get("jitter_ratio", 0.2)

    if not isinstance(timeout, (int, float)) or timeout <= 0:
        raise ConfigError("timeout must be a positive number.")
    if not isinstance(retry_max_attempts, int) or retry_max_attempts <= 0:
        raise ConfigError("retry_max_attempts must be a positive integer.")
    if not isinstance(retry_base_seconds, (int, float)) or retry_base_seconds <= 0:
        raise ConfigError("retry_base_seconds must be a positive number.")
    if not isinstance(retry_max_seconds, (int, float)) or retry_max_seconds <= 0:
        raise ConfigError("retry_max_seconds must be a positive number.")
    if not isinstance(jitter_ratio, (int, float)) or jitter_ratio < 0:
        raise ConfigError("jitter_ratio must be a non-negative number.")
    if float(retry_max_seconds) < float(retry_base_seconds):
        raise ConfigError("retry_max_seconds must be greater than or equal to retry_base_seconds.")
    if float(jitter_ratio) > 1.0:
        raise ConfigError("jitter_ratio must be less than or equal to 1.0.")

    output_dir = Path(raw.get("output_dir", "data/output"))
    checkpoint_dir = Path(raw.get("checkpoint_dir", "data/checkpoints"))

    return FetcherConfig(
        timeout=float(timeout),
        retry_max_attempts=retry_max_attempts,
        retry_base_seconds=float(retry_base_seconds),
        retry_max_seconds=float(retry_max_seconds),
        jitter_ratio=float(jitter_ratio),
        output_dir=output_dir,
        checkpoint_dir=checkpoint_dir,
    )


def _build_auth_settings(raw: dict[str, Any], fetcher_cfg: FetcherConfig) -> AuthSettings:
    env_file = raw.get("env_file", ".env")
    if not isinstance(env_file, str) or not env_file.strip():
        raise ConfigError("env_file must be a non-empty string when provided.")
    return AuthSettings(timeout=fetcher_cfg.timeout, env_file=Path(env_file))


def _ensure_unique_names(specs: Iterable[EndpointSpec]) -> None:
    seen: set[str] = set()
    for spec in specs:
        if spec.name in seen:
            raise ConfigError(f"Duplicate endpoint name: {spec.name}")
        seen.add(spec.name)


def load_fetcher_settings(
    path: str | Path,
) -> tuple[FetcherConfig, AuthSettings, list[EndpointSpec]]:
    config_path = Path(path)
    payload = _load_payload(config_path)

    fetcher_cfg = _build_fetcher_config(payload)
    auth_settings = _build_auth_settings(payload, fetcher_cfg)

    endpoints_raw = _as_list(payload.get("endpoints"), field_name="endpoints")
    endpoints = []
    for endpoint_raw in endpoints_raw:
        if not isinstance(endpoint_raw, dict):
            raise ConfigError("Each endpoint entry must be an object.")
        endpoints.append(_build_endpoint_spec(endpoint_raw))

    if not endpoints:
        raise ConfigError("Config must contain at least one endpoint.")

    _ensure_unique_names(endpoints)
    return fetcher_cfg, auth_settings, endpoints
