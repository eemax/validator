from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

DEFAULT_ENDPOINT_SCHEMA_PATH = Path("config/endpoint-schema.yml")


@dataclass(frozen=True)
class EndpointSchema:
    name: str
    primary_key: str = "id"
    modified_at_fields: tuple[str, ...] = ("_modified_at",)
    delete_field: str | None = "active"
    delete_when: Any = False
    full_snapshot_mode: str = "upsert_only"


DEFAULT_ENDPOINT_SCHEMAS: dict[str, EndpointSchema] = {
    name: EndpointSchema(name=name)
    for name in (
        "styles",
        "colorways",
        "collections",
        "category1s",
        "category2s",
        "sizes",
        "seasons",
        "materials",
        "boms",
        "bomrows",
        "supplierquotes",
        "suppliers",
        "factories",
    )
}


def load_endpoint_schemas(path: Path | None = None) -> dict[str, EndpointSchema]:
    resolved_path = path or DEFAULT_ENDPOINT_SCHEMA_PATH
    if not resolved_path.is_file():
        return dict(DEFAULT_ENDPOINT_SCHEMAS)

    payload = yaml.safe_load(resolved_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Endpoint schema root must be an object: {resolved_path}")

    endpoints = payload.get("endpoints", payload)
    if not isinstance(endpoints, dict):
        raise ValueError(f"Endpoint schema 'endpoints' must be an object: {resolved_path}")

    schemas = dict(DEFAULT_ENDPOINT_SCHEMAS)
    for endpoint_name, config in endpoints.items():
        if config is None:
            config = {}
        if not isinstance(config, dict):
            raise ValueError(
                f"Endpoint schema for {endpoint_name!r} must be an object: {resolved_path}"
            )
        name = str(endpoint_name)
        default = schemas.get(name, EndpointSchema(name=name))
        schemas[name] = EndpointSchema(
            name=name,
            primary_key=str(config.get("primary_key", default.primary_key)),
            modified_at_fields=_string_tuple(
                config.get("modified_at_fields", config.get("modified_at_field")),
                default=default.modified_at_fields,
            ),
            delete_field=_optional_string(config.get("delete_field", default.delete_field)),
            delete_when=config.get("delete_when", default.delete_when),
            full_snapshot_mode=str(
                config.get("full_snapshot_mode", default.full_snapshot_mode)
            ),
        )
    return schemas


def _string_tuple(value: Any, *, default: tuple[str, ...]) -> tuple[str, ...]:
    if value is None:
        return default
    if isinstance(value, str):
        return (value,)
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return tuple(value)
    raise ValueError("Endpoint schema field lists must be strings or arrays of strings.")


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)
