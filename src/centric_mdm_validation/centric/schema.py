from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

DEFAULT_ENDPOINT_SCHEMA_PATH = Path("config/endpoint-schema.yml")


@dataclass(frozen=True)
class DeleteCondition:
    field: str
    equals: Any


@dataclass(frozen=True)
class EndpointSchema:
    name: str
    primary_key: str = "id"
    modified_at_fields: tuple[str, ...] = ("_modified_at",)
    delete_when_any: tuple[DeleteCondition, ...] = (DeleteCondition(field="active", equals=False),)
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
        "bom_section_definitions",
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
        if "delete_field" in config or "delete_when" in config:
            raise ValueError(
                "Endpoint schema delete_field/delete_when are no longer supported; "
                "use delete_when_any instead."
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
            delete_when_any=_delete_condition_tuple(
                config.get("delete_when_any", default.delete_when_any)
            ),
            full_snapshot_mode=str(config.get("full_snapshot_mode", default.full_snapshot_mode)),
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


def _delete_condition_tuple(value: Any) -> tuple[DeleteCondition, ...]:
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)):
        raise ValueError("Endpoint schema delete_when_any must be an array of objects.")

    conditions: list[DeleteCondition] = []
    for item in value:
        if not isinstance(item, dict):
            raise ValueError("Endpoint schema delete_when_any entries must be objects.")
        field = item.get("field")
        if not isinstance(field, str) or not field.strip():
            raise ValueError("Endpoint schema delete_when_any entries require a field.")
        if "equals" not in item:
            raise ValueError("Endpoint schema delete_when_any entries require equals.")
        conditions.append(DeleteCondition(field=field, equals=item["equals"]))
    return tuple(conditions)
