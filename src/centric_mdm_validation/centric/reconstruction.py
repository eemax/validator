from __future__ import annotations

import importlib.util
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel

from centric_mdm_validation.centric.config import resolve_optional_private_config_path

RECONSTRUCTION_CONFIG_PATH = Path("reconstruction.py")
DEFAULT_PROJECTION_TARGET = "check"


@dataclass(frozen=True)
class ReconstructionSourceRef:
    endpoint: str
    record_id: str
    relation_type: str | None = None


@dataclass(frozen=True)
class ReconstructionWarning:
    code: str
    message: str
    severity: str = "warning"
    source_endpoint: str | None = None
    source_record_id: str | None = None


@dataclass(frozen=True)
class ReconstructedProduct:
    product_id: str
    style_id: str | None = None
    brand_code: str | None = None
    season: str | None = None
    product_type_code: str | None = None
    graph: dict[str, Any] = field(default_factory=dict)
    source_refs: tuple[ReconstructionSourceRef, ...] = ()
    warnings: tuple[ReconstructionWarning, ...] = ()


@dataclass(frozen=True)
class ReconstructionRuntimeInfo:
    path: Path | None
    master_strategy: str
    projection_strategy: str


class MasterReconstructionFunction(Protocol):
    def __call__(
        self,
        records_by_endpoint: Mapping[str, Iterable[dict[str, Any]]],
    ) -> Iterable[ReconstructedProduct | Mapping[str, Any]]: ...


class ProjectionFunction(Protocol):
    def __call__(
        self,
        target: str,
        reconstructed_products: Iterable[ReconstructedProduct],
    ) -> Iterable[BaseModel | Mapping[str, Any]]: ...


def reconstruct_master_products_from_records(
    records_by_endpoint: Mapping[str, Iterable[dict[str, Any]]],
    *,
    reconstruction_path: Path | None = None,
) -> list[ReconstructedProduct]:
    """Build compact style reconstruction state."""

    module = load_private_reconstruction_module(reconstruction_path)
    if module is not None:
        reconstruction = getattr(module, "reconstruct_master_products", None)
        if callable(reconstruction):
            return [
                _coerce_reconstructed_product(product)
                for product in reconstruction(records_by_endpoint)
            ]
        raise ValueError(
            "Private reconstruction module must define "
            "reconstruct_master_products(records_by_endpoint)."
        )

    return _placeholder_master_products(records_by_endpoint)


def inspect_reconstruction_runtime(
    *,
    target: str = DEFAULT_PROJECTION_TARGET,
    reconstruction_path: Path | None = None,
) -> ReconstructionRuntimeInfo:
    """Describe which reconstruction/projection implementation will be used."""

    path = resolve_reconstruction_path(reconstruction_path)
    if path is None:
        return ReconstructionRuntimeInfo(
            path=None,
            master_strategy="public style-only placeholder",
            projection_strategy=_default_projection_strategy(target),
        )

    module = load_private_reconstruction_module(path)
    has_master_hook = callable(getattr(module, "reconstruct_master_products", None))
    has_projection_hook = callable(getattr(module, "project_reconstructed_products", None))
    projection_strategy = (
        _default_projection_strategy(target)
        if target == DEFAULT_PROJECTION_TARGET
        else (
            "private project_reconstructed_products hook"
            if has_projection_hook
            else _default_projection_strategy(target)
        )
    )

    return ReconstructionRuntimeInfo(
        path=path,
        master_strategy=(
            "private reconstruction hook"
            if has_master_hook
            else "missing private reconstruct_master_products hook"
        ),
        projection_strategy=projection_strategy,
    )


def project_master_products(
    reconstructed_products: Iterable[ReconstructedProduct],
    *,
    target: str = DEFAULT_PROJECTION_TARGET,
    reconstruction_path: Path | None = None,
) -> list[BaseModel | dict[str, Any]]:
    """Project reconstruction output into a check or target-specific payload contract."""

    products = list(reconstructed_products)
    if target == DEFAULT_PROJECTION_TARGET:
        return [_project_reconstruction_check(product) for product in products]

    module = load_private_reconstruction_module(reconstruction_path)
    if module is not None:
        projection = getattr(module, "project_reconstructed_products", None)
        if callable(projection):
            return [
                _coerce_projected_payload(payload)
                for payload in projection(target, products)
            ]

    raise ValueError(
        f"Private projection required for target {target!r}. Define "
        "project_reconstructed_products(target, reconstructed_products) "
        f"in {RECONSTRUCTION_CONFIG_PATH}."
    )


def load_private_reconstruction_module(path: Path | None = None) -> Any | None:
    resolved_path = resolve_reconstruction_path(path)
    if resolved_path is None:
        return None

    spec = importlib.util.spec_from_file_location("centric_private_reconstruction", resolved_path)
    if spec is None or spec.loader is None:
        raise ValueError(f"Could not load reconstruction module: {resolved_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def resolve_reconstruction_path(path: Path | None = None) -> Path | None:
    if path is not None:
        return path

    return resolve_optional_private_config_path(RECONSTRUCTION_CONFIG_PATH)


def _placeholder_master_products(
    records_by_endpoint: Mapping[str, Iterable[dict[str, Any]]],
) -> list[ReconstructedProduct]:
    products: list[ReconstructedProduct] = []
    for style in records_by_endpoint.get("styles", []):
        if not isinstance(style, dict):
            continue
        style_id = _optional_string(style.get("id"))
        if style_id is None:
            continue
        products.append(
            ReconstructedProduct(
                product_id=style_id,
                style_id=style_id,
                brand_code=_optional_string(style.get("brand_code")),
                product_type_code=_optional_string(style.get("product_type")),
                graph={
                    "style_id": style_id,
                    "relationship_ids": {},
                    "relationships": {},
                    "applicability": {},
                    "unresolved_refs": [],
                    "placeholder": True,
                },
                source_refs=(
                    ReconstructionSourceRef(
                        endpoint="styles",
                        record_id=style_id,
                        relation_type="style",
                    ),
                ),
            )
        )
    return products


def _default_projection_strategy(target: str) -> str:
    if target == DEFAULT_PROJECTION_TARGET:
        return "public compact reconstruction check"
    return "private projection required"


def _project_reconstruction_check(product: ReconstructedProduct) -> dict[str, Any]:
    graph = product.graph if isinstance(product.graph, Mapping) else {}
    relationships = _mapping_dict(graph.get("relationships"))
    relationship_ids = _relationship_ids(relationships)
    applicability = {
        key: value
        for key, value in relationships.items()
        if key.endswith("_applicability")
    }
    resolved_records = _resolved_record_counts(product.source_refs)
    warnings = [_warning_record(warning) for warning in product.warnings]
    unresolved_refs = _mapping_list(graph.get("unresolved_refs"))
    return {
        "style_id": product.style_id or product.product_id,
        "relationship_ids": relationship_ids,
        "counts": {
            "relationship_ids": _value_counts(relationship_ids),
            "resolved_records": resolved_records,
            "unresolved_refs": len(unresolved_refs),
            "warnings": len(warnings),
        },
        "applicability": applicability,
        "unresolved_refs": unresolved_refs,
        "warnings": warnings,
    }


def _relationship_ids(relationships: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in relationships.items()
        if not key.endswith("_applicability")
    }


def _value_counts(values: Mapping[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for key, value in values.items():
        if isinstance(value, list | dict):
            counts[key] = len(value)
        elif value in (None, ""):
            counts[key] = 0
        else:
            counts[key] = 1
    return counts


def _resolved_record_counts(
    source_refs: Iterable[ReconstructionSourceRef],
) -> dict[str, int]:
    counts: dict[str, set[str]] = {}
    for source_ref in source_refs:
        if source_ref.relation_type is None or source_ref.relation_type == "style":
            continue
        bucket = _relation_bucket(source_ref.relation_type)
        counts.setdefault(bucket, set()).add(source_ref.record_id)
    return {bucket: len(record_ids) for bucket, record_ids in sorted(counts.items())}


def _relation_bucket(relation_type: str) -> str:
    buckets = {
        "season": "seasons",
        "colorway": "colorways",
        "size": "sizes",
        "bom": "boms",
        "bom_row": "bom_rows",
        "material": "materials",
        "supplier_quote": "supplier_quotes",
        "factory": "factories",
        "supplier": "suppliers",
    }
    return buckets.get(relation_type, relation_type)


def _warning_record(warning: ReconstructionWarning) -> dict[str, Any]:
    return {
        "code": warning.code,
        "message": warning.message,
        "severity": warning.severity,
        "source_endpoint": warning.source_endpoint,
        "source_record_id": warning.source_record_id,
    }


def _coerce_reconstructed_product(
    product: ReconstructedProduct | Mapping[str, Any],
) -> ReconstructedProduct:
    if isinstance(product, ReconstructedProduct):
        return product
    if not isinstance(product, Mapping):
        raise TypeError("Master reconstruction products must be mappings or ReconstructedProduct.")
    graph = product.get("graph")
    product_id = product.get("product_id") or product.get("style_id")
    if not isinstance(product_id, str) or not product_id:
        raise ValueError("Master reconstruction products must include product_id or style_id.")
    return ReconstructedProduct(
        product_id=product_id,
        style_id=_optional_string(product.get("style_id")),
        brand_code=_optional_string(product.get("brand_code")),
        season=_optional_string(product.get("season")),
        product_type_code=_optional_string(product.get("product_type_code")),
        graph=dict(graph) if isinstance(graph, Mapping) else {},
        source_refs=tuple(
            _coerce_source_ref(source_ref)
            for source_ref in _mapping_list(product.get("source_refs"))
        ),
        warnings=tuple(
            _coerce_warning(warning)
            for warning in _mapping_list(product.get("warnings"))
        ),
    )


def _coerce_source_ref(value: Mapping[str, Any]) -> ReconstructionSourceRef:
    endpoint = _optional_string(value.get("endpoint"))
    record_id = _optional_string(value.get("record_id"))
    if endpoint is None or record_id is None:
        raise ValueError("Source refs must include endpoint and record_id.")
    return ReconstructionSourceRef(
        endpoint=endpoint,
        record_id=record_id,
        relation_type=_optional_string(value.get("relation_type")),
    )


def _coerce_warning(value: Mapping[str, Any]) -> ReconstructionWarning:
    code = _optional_string(value.get("code")) or "RECONSTRUCTION_WARNING"
    message = _optional_string(value.get("message")) or ""
    return ReconstructionWarning(
        code=code,
        message=message,
        severity=_optional_string(value.get("severity")) or "warning",
        source_endpoint=_optional_string(value.get("source_endpoint")),
        source_record_id=_optional_string(value.get("source_record_id")),
    )


def _mapping_list(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _mapping_dict(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    return dict(value)


def _coerce_projected_payload(payload: BaseModel | Mapping[str, Any]) -> BaseModel | dict[str, Any]:
    if isinstance(payload, BaseModel):
        return payload
    if isinstance(payload, Mapping):
        return dict(payload)
    raise TypeError("Projected payloads must be Pydantic models or mappings.")


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
