from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from centric_mdm_validation.centric.config import resolve_optional_private_config_path
from centric_mdm_validation.io import read_json_records, write_jsonl
from centric_mdm_validation.models import CentricProductPayload, CentricVariant

ENDPOINT_FILES = {
    "styles": "styles.jsonl",
    "colorways": "colorways.jsonl",
    "seasons": "seasons.jsonl",
    "materials": "materials.jsonl",
}
FIELD_MAPPING_CONFIG_PATH = Path("field-mapping.yml")


@dataclass(frozen=True)
class ProjectionMapping:
    style_global_id_fields: tuple[str, ...] = ()
    style_name_fields: tuple[str, ...] = ("node_name",)
    style_brand_code_fields: tuple[str, ...] = ("brand_code",)
    style_brand_name_fields: tuple[str, ...] = ()
    style_product_type_fields: tuple[str, ...] = ("product_type",)
    style_lifecycle_fields: tuple[str, ...] = ()
    season_brand_code_fields: tuple[str, ...] = ()
    season_brand_name_fields: tuple[str, ...] = ()
    season_code_fields: tuple[str, ...] = ("code",)
    material_name_fields: tuple[str, ...] = ("node_name",)
    material_composition_fields: tuple[str, ...] = (
        "composition",
        "technical_composition",
        "description",
    )
    colorway_global_id_fields: tuple[str, ...] = ("sys_id",)
    colorway_sku_fields: tuple[str, ...] = ("code", "node_name")
    attribute_fields: dict[str, tuple[str, ...]] = field(default_factory=dict)


def resolve_projection_mapping_path(path: Path | None = None) -> Path | None:
    if path is not None:
        return path

    return resolve_optional_private_config_path(FIELD_MAPPING_CONFIG_PATH)


def load_projection_mapping(path: Path | None = None) -> ProjectionMapping:
    resolved_path = resolve_projection_mapping_path(path)
    if resolved_path is None:
        return ProjectionMapping()

    if not resolved_path.is_file():
        raise ValueError(f"Projection mapping file not found: {resolved_path}")

    data = yaml.safe_load(resolved_path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Projection mapping root must be an object: {resolved_path}")

    style = _as_dict(data.get("style"))
    season = _as_dict(data.get("season"))
    material = _as_dict(data.get("material"))
    colorway = _as_dict(data.get("colorway"))

    return ProjectionMapping(
        style_global_id_fields=_field_tuple(style.get("global_style_id_fields")),
        style_name_fields=_field_tuple(style.get("style_name_fields"), default=("node_name",)),
        style_brand_code_fields=_field_tuple(
            style.get("brand_code_fields"),
            default=("brand_code",),
        ),
        style_brand_name_fields=_field_tuple(style.get("brand_name_fields")),
        style_product_type_fields=_field_tuple(
            style.get("product_type_code_fields"),
            default=("product_type",),
        ),
        style_lifecycle_fields=_field_tuple(style.get("lifecycle_status_fields")),
        season_brand_code_fields=_field_tuple(season.get("brand_code_fields")),
        season_brand_name_fields=_field_tuple(season.get("brand_name_fields")),
        season_code_fields=_field_tuple(season.get("season_code_fields"), default=("code",)),
        material_name_fields=_field_tuple(material.get("name_fields"), default=("node_name",)),
        material_composition_fields=_field_tuple(
            material.get("composition_fields"),
            default=("composition", "technical_composition", "description"),
        ),
        colorway_global_id_fields=_field_tuple(
            colorway.get("global_variant_id_fields"),
            default=("sys_id",),
        ),
        colorway_sku_fields=_field_tuple(colorway.get("sku_fields"), default=("code", "node_name")),
        attribute_fields={
            str(name): _field_tuple(fields)
            for name, fields in _as_dict(style.get("attribute_fields")).items()
        },
    )


def load_endpoint_records(input_dir: Path) -> dict[str, list[dict[str, Any]]]:
    records: dict[str, list[dict[str, Any]]] = {}
    for endpoint, filename in ENDPOINT_FILES.items():
        path = input_dir / filename
        records[endpoint] = read_json_records(path) if path.is_file() else []
    return records


def write_projected_products(
    input_dir: Path,
    output_path: Path,
    mapping_path: Path | None = None,
) -> list[CentricProductPayload]:
    payloads = project_products(
        load_endpoint_records(input_dir),
        mapping=load_projection_mapping(mapping_path),
    )
    write_jsonl(
        output_path,
        (payload.model_dump(mode="json", exclude_none=True) for payload in payloads),
    )
    return payloads


def project_products(
    records_by_endpoint: Mapping[str, Iterable[dict[str, Any]]],
    *,
    mapping: ProjectionMapping | None = None,
) -> list[CentricProductPayload]:
    mapping = mapping or ProjectionMapping()
    styles = list(records_by_endpoint.get("styles", []))
    colorways = list(records_by_endpoint.get("colorways", []))
    seasons_by_id = _index_by_id(records_by_endpoint.get("seasons", []))
    materials_by_id = _index_by_id(records_by_endpoint.get("materials", []))

    colorways_by_style: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for colorway in colorways:
        style_id = _clean_ref(colorway.get("style"))
        if style_id:
            colorways_by_style[style_id].append(colorway)

    return [
        project_style(
            style,
            colorways=colorways_by_style.get(str(style.get("id")), []),
            seasons_by_id=seasons_by_id,
            materials_by_id=materials_by_id,
            mapping=mapping,
        )
        for style in styles
        if _clean_ref(style.get("id"))
    ]


def project_style(
    style: dict[str, Any],
    *,
    colorways: Iterable[dict[str, Any]] = (),
    seasons_by_id: Mapping[str, dict[str, Any]] | None = None,
    materials_by_id: Mapping[str, dict[str, Any]] | None = None,
    mapping: ProjectionMapping | None = None,
) -> CentricProductPayload:
    mapping = mapping or ProjectionMapping()
    seasons_by_id = seasons_by_id or {}
    materials_by_id = materials_by_id or {}

    season = _season_for_style(style, seasons_by_id)
    material_ids = _style_material_ids(style)
    material_records = [
        materials_by_id[item_id] for item_id in material_ids if item_id in materials_by_id
    ]
    brand_code = _brand_code(style, season, mapping)

    return CentricProductPayload(
        centric_style_id=str(style["id"]),
        brand_code=brand_code,
        brand_name=_brand_name(style, season, mapping) or brand_code,
        global_style_id=_first_field(style, mapping.style_global_id_fields),
        style_name=_first_field(style, mapping.style_name_fields),
        product_type_code=_clean_ref(_first_field(style, mapping.style_product_type_fields)),
        lifecycle_status=_first_field(style, mapping.style_lifecycle_fields)
        or ("active" if style.get("active") is True else None),
        season=_first_field(season, mapping.season_code_fields)
        if season
        else _first_text(style.get("original_season"), style.get("parent_season")),
        attributes=_style_attributes(
            style,
            season=season,
            materials=material_records,
            material_ids=material_ids,
            mapping=mapping,
        ),
        variants=[
            _variant_from_colorway(colorway, mapping)
            for colorway in sorted(colorways, key=lambda item: str(item.get("id", "")))
        ],
    )


def _index_by_id(records: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        str(record["id"]): record
        for record in records
        if isinstance(record, dict) and _clean_ref(record.get("id"))
    }


def _season_for_style(
    style: dict[str, Any],
    seasons_by_id: Mapping[str, dict[str, Any]],
) -> dict[str, Any] | None:
    for key in ("parent_season", "original_season"):
        season_id = _clean_ref(style.get(key))
        if season_id and season_id in seasons_by_id:
            return seasons_by_id[season_id]
    return None


def _brand_code(
    style: dict[str, Any],
    season: dict[str, Any] | None,
    mapping: ProjectionMapping,
) -> str | None:
    return _first_field(style, mapping.style_brand_code_fields) or _first_field(
        season,
        mapping.season_brand_code_fields,
    )


def _brand_name(
    style: dict[str, Any],
    season: dict[str, Any] | None,
    mapping: ProjectionMapping,
) -> str | None:
    return _first_field(style, mapping.style_brand_name_fields) or _first_field(
        season,
        mapping.season_brand_name_fields,
    )


def _style_attributes(
    style: dict[str, Any],
    *,
    season: dict[str, Any] | None,
    materials: list[dict[str, Any]],
    material_ids: list[str],
    mapping: ProjectionMapping,
) -> dict[str, Any]:
    attributes: dict[str, Any] = {
        "CENTRIC_STYLE_CODE": _clean_text(style.get("code")),
        "RAW_PRODUCT_TYPE_ID": _clean_ref(_first_field(style, mapping.style_product_type_fields)),
        "MAIN_MATERIAL_IDS": material_ids,
        "MAIN_MATERIAL_NAMES": _join_text(
            _first_field(material, mapping.material_name_fields) for material in materials
        ),
        "MATERIAL_COMPOSITION": _join_text(
            _first_field(material, mapping.material_composition_fields)
            for material in materials
        ),
    }
    if season is not None:
        attributes["SEASON_CODE"] = _first_field(season, mapping.season_code_fields)

    for attribute_name, fields in mapping.attribute_fields.items():
        attributes[attribute_name] = _first_field(style, fields)

    return {key: value for key, value in attributes.items() if value not in (None, "", [])}


def _style_material_ids(style: dict[str, Any]) -> list[str]:
    ids: list[str] = []
    for key in ("bom_main_materials", "bom_materials"):
        value = style.get(key)
        if isinstance(value, dict):
            ids.extend(_clean_ref(item) for item in value.values())
        elif isinstance(value, list):
            ids.extend(_clean_ref(item) for item in value)
    return sorted({item for item in ids if item})


def _variant_from_colorway(
    colorway: dict[str, Any],
    mapping: ProjectionMapping,
) -> CentricVariant:
    return CentricVariant(
        centric_variant_id=_clean_ref(colorway.get("id")),
        global_variant_id=_first_field(colorway, mapping.colorway_global_id_fields),
        sku=_first_field(colorway, mapping.colorway_sku_fields),
        color_name=_first_text(colorway.get("node_name"), colorway.get("code")),
        external_ids={"centric_style_id": _clean_ref(colorway.get("style")) or ""},
    )


def _as_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("Projection mapping sections must be objects.")
    return value


def _field_tuple(value: Any, *, default: tuple[str, ...] = ()) -> tuple[str, ...]:
    if value is None:
        return default
    if isinstance(value, str):
        return (value,)
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return tuple(value)
    raise ValueError("Projection mapping field lists must be strings or arrays of strings.")


def _first_field(record: Mapping[str, Any] | None, fields: Iterable[str]) -> str | None:
    if record is None:
        return None
    return _first_text(*(record.get(field) for field in fields))


def _clean_ref(value: Any) -> str | None:
    text = _clean_text(value)
    if text is None or text == "centric:":
        return None
    return text


def _clean_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _first_text(*values: Any) -> str | None:
    for value in values:
        text = _clean_text(value)
        if text:
            return text
    return None


def _join_text(values: Iterable[str | None]) -> str | None:
    seen: set[str] = set()
    cleaned: list[str] = []
    for value in values:
        if value and value not in seen:
            cleaned.append(value)
            seen.add(value)
    return "; ".join(cleaned) if cleaned else None
