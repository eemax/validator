from __future__ import annotations

import importlib.util
import inspect
import sys
from collections.abc import Iterable, Mapping
from contextlib import suppress
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
        *,
        records_by_endpoint: Mapping[str, Iterable[dict[str, Any]]] | None = None,
    ) -> Iterable[BaseModel | Mapping[str, Any]]: ...


class TargetReconstructionFunction(Protocol):
    def __call__(
        self,
        target: str,
        records_by_endpoint: Mapping[str, Iterable[dict[str, Any]]],
    ) -> Iterable[BaseModel | Mapping[str, Any]]: ...


class AffectedStyleFunction(Protocol):
    def __call__(
        self,
        target: str,
        changed_records: Mapping[str, Iterable[str]],
        *,
        records_by_endpoint: Mapping[str, Iterable[dict[str, Any]]],
    ) -> Iterable[str]: ...


class ValidationFunction(Protocol):
    def __call__(
        self,
        target: str,
        payloads: Iterable[BaseModel | Mapping[str, Any]],
        *,
        rules: Path | None = None,
    ) -> BaseModel | Mapping[str, Any]: ...


class ReportFunction(Protocol):
    def __call__(
        self,
        target: str,
        validation_result: BaseModel | Mapping[str, Any],
        output_dir: Path,
    ) -> None: ...


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
    has_target_hook = callable(getattr(module, "reconstruct_target_records", None))
    has_projection_hook = callable(getattr(module, "project_reconstructed_products", None))
    projection_strategy = (
        _default_projection_strategy(target)
        if target == DEFAULT_PROJECTION_TARGET
        else (
            "private reconstruct_target_records hook"
            if has_target_hook
            else (
                "private project_reconstructed_products hook"
                if has_projection_hook
                else _default_projection_strategy(target)
            )
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
    records_by_endpoint: Mapping[str, Iterable[dict[str, Any]]] | None = None,
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
            try:
                payloads = projection(
                    target,
                    products,
                    records_by_endpoint=records_by_endpoint,
                )
            except TypeError as exc:
                if "records_by_endpoint" not in str(exc):
                    raise
                payloads = projection(target, products)
            return [_coerce_projected_payload(payload) for payload in payloads]

    raise ValueError(
        f"Private projection required for target {target!r}. Define "
        "project_reconstructed_products(target, reconstructed_products) "
        f"in {RECONSTRUCTION_CONFIG_PATH}."
    )


def reconstruct_target_records(
    target: str,
    records_by_endpoint: Mapping[str, Iterable[dict[str, Any]]],
    *,
    reconstruction_path: Path | None = None,
    progress: Any | None = None,
    style_ids: Iterable[str] | None = None,
) -> list[BaseModel | dict[str, Any]]:
    """Build target-specific reconstruction records directly from endpoint state."""

    module = load_private_reconstruction_module(reconstruction_path)
    reconstruction = getattr(module, "reconstruct_target_records", None) if module else None
    if callable(reconstruction):
        kwargs: dict[str, Any] = {}
        if style_ids is not None:
            if not _supports_keyword(reconstruction, "style_ids"):
                raise ValueError(
                    "Scoped reconstruction requires private "
                    "reconstruct_target_records(..., style_ids=...) support."
                )
            kwargs["style_ids"] = set(style_ids)
        payloads = _call_with_optional_progress(
            reconstruction,
            target,
            records_by_endpoint,
            progress=progress,
            **kwargs,
        )
        return [_coerce_projected_payload(payload) for payload in payloads]

    raise ValueError(
        f"Private reconstruction required for target {target!r}. Define "
        "reconstruct_target_records(target, records_by_endpoint) "
        f"in {RECONSTRUCTION_CONFIG_PATH}."
    )


def resolve_affected_style_ids(
    target: str,
    changed_records: Mapping[str, Iterable[str]],
    records_by_endpoint: Mapping[str, Iterable[dict[str, Any]]],
    *,
    reconstruction_path: Path | None = None,
) -> set[str]:
    """Resolve changed endpoint records to affected target style IDs."""

    module = load_private_reconstruction_module(reconstruction_path)
    resolver = getattr(module, "resolve_affected_style_ids", None) if module else None
    if not callable(resolver):
        raise ValueError(
            f"Private affected-style resolver required for target {target!r}. Define "
            "resolve_affected_style_ids(target, changed_records, *, records_by_endpoint) "
            f"in {RECONSTRUCTION_CONFIG_PATH}."
        )
    style_ids = resolver(
        target,
        changed_records,
        records_by_endpoint=records_by_endpoint,
    )
    return {str(style_id) for style_id in style_ids if str(style_id or "").strip()}


def validate_projected_products(
    target: str,
    payloads: Iterable[BaseModel | Mapping[str, Any]],
    *,
    rules: Path | None = None,
    reconstruction_path: Path | None = None,
    progress: Any | None = None,
) -> BaseModel | Mapping[str, Any]:
    """Validate target payloads with a private validation hook."""

    module = load_private_reconstruction_module(reconstruction_path)
    validation = getattr(module, "validate_projected_products", None) if module else None
    if callable(validation):
        return _call_with_optional_progress(
            validation,
            target,
            payloads,
            rules=rules,
            progress=progress,
        )

    raise ValueError(
        f"Private validation required for target {target!r}. Define "
        "validate_projected_products(target, payloads, *, rules=None) "
        f"in {RECONSTRUCTION_CONFIG_PATH}."
    )


def report_validation_results(
    target: str,
    validation_result: BaseModel | Mapping[str, Any],
    output_dir: Path,
    *,
    reconstruction_path: Path | None = None,
    template: str = "default",
    progress: Any | None = None,
) -> None:
    """Write target reports with a private report hook."""

    module = load_private_reconstruction_module(reconstruction_path)
    report = getattr(module, "report_validation_results", None) if module else None
    if callable(report):
        kwargs: dict[str, Any] = {}
        if _supports_keyword(report, "template"):
            kwargs["template"] = template
        elif template != "default":
            raise ValueError(
                f"Private reporting template {template!r} requires "
                "report_validation_results(..., template=...) support."
            )
        _call_with_optional_progress(
            report,
            target,
            validation_result,
            output_dir,
            progress=progress,
            **kwargs,
        )
        return

    raise ValueError(
        f"Private reporting required for target {target!r}. Define "
        "report_validation_results(target, validation_result, output_dir) "
        f"in {RECONSTRUCTION_CONFIG_PATH}."
    )


def has_private_validation_hook(*, reconstruction_path: Path | None = None) -> bool:
    module = load_private_reconstruction_module(reconstruction_path)
    return callable(getattr(module, "validate_projected_products", None)) if module else False


def has_private_report_hook(*, reconstruction_path: Path | None = None) -> bool:
    module = load_private_reconstruction_module(reconstruction_path)
    return callable(getattr(module, "report_validation_results", None)) if module else False


def load_private_reconstruction_module(path: Path | None = None) -> Any | None:
    resolved_path = resolve_reconstruction_path(path)
    if resolved_path is None:
        return None

    spec = importlib.util.spec_from_file_location("centric_private_reconstruction", resolved_path)
    if spec is None or spec.loader is None:
        raise ValueError(f"Could not load reconstruction module: {resolved_path}")

    module = importlib.util.module_from_spec(spec)
    module_dir = str(resolved_path.parent)
    sys.path.insert(0, module_dir)
    try:
        spec.loader.exec_module(module)
    finally:
        with suppress(ValueError):
            sys.path.remove(module_dir)
    return module


def resolve_reconstruction_path(path: Path | None = None) -> Path | None:
    if path is not None:
        return path

    return resolve_optional_private_config_path(RECONSTRUCTION_CONFIG_PATH)


def _call_with_optional_progress(function, *args, progress: Any | None = None, **kwargs):
    if progress is not None and _supports_keyword(function, "progress"):
        kwargs["progress"] = progress
    return function(*args, **kwargs)


def _supports_keyword(function, name: str) -> bool:
    signature = inspect.signature(function)
    return name in signature.parameters or any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    )


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
        key: value for key, value in relationships.items() if key.endswith("_applicability")
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
        key: value for key, value in relationships.items() if not key.endswith("_applicability")
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
            _coerce_warning(warning) for warning in _mapping_list(product.get("warnings"))
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
