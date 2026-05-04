from __future__ import annotations

import importlib.util
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel

from centric_mdm_validation.centric.config import resolve_optional_private_config_path
from centric_mdm_validation.centric.mapper import ProjectionMapping, project_products
from centric_mdm_validation.models import CentricProductPayload

RECONSTRUCTION_CONFIG_PATH = Path("reconstruction.py")
DEFAULT_PROJECTION_TARGET = "dpp"


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


class ReconstructionFunction(Protocol):
    def __call__(
        self,
        records_by_endpoint: Mapping[str, Iterable[dict[str, Any]]],
        *,
        mapping: ProjectionMapping | None = None,
    ) -> list[CentricProductPayload]: ...


class MasterReconstructionFunction(Protocol):
    def __call__(
        self,
        records_by_endpoint: Mapping[str, Iterable[dict[str, Any]]],
        *,
        mapping: ProjectionMapping | None = None,
    ) -> Iterable[ReconstructedProduct | Mapping[str, Any]]: ...


class ProjectionFunction(Protocol):
    def __call__(
        self,
        target: str,
        reconstructed_products: Iterable[ReconstructedProduct],
        *,
        mapping: ProjectionMapping | None = None,
    ) -> Iterable[BaseModel | Mapping[str, Any]]: ...


def reconstruct_master_products_from_records(
    records_by_endpoint: Mapping[str, Iterable[dict[str, Any]]],
    *,
    mapping: ProjectionMapping | None = None,
    reconstruction_path: Path | None = None,
) -> list[ReconstructedProduct]:
    """Build the target-agnostic master reconstruction graph."""

    module = load_private_reconstruction_module(reconstruction_path)
    if module is not None:
        reconstruction = getattr(module, "reconstruct_master_products", None)
        if callable(reconstruction):
            return [
                _coerce_reconstructed_product(product)
                for product in reconstruction(records_by_endpoint, mapping=mapping)
            ]

        legacy_reconstruction = getattr(module, "reconstruct_projected_products", None)
        if callable(legacy_reconstruction):
            return [
                _master_product_from_dpp_payload(payload)
                for payload in legacy_reconstruction(records_by_endpoint, mapping=mapping)
            ]

    return [
        _master_product_from_dpp_payload(payload)
        for payload in project_products(records_by_endpoint, mapping=mapping)
    ]


def project_master_products(
    reconstructed_products: Iterable[ReconstructedProduct],
    *,
    target: str = DEFAULT_PROJECTION_TARGET,
    mapping: ProjectionMapping | None = None,
    reconstruction_path: Path | None = None,
) -> list[BaseModel | dict[str, Any]]:
    """Project master reconstruction output into a target-specific payload contract."""

    products = list(reconstructed_products)
    module = load_private_reconstruction_module(reconstruction_path)
    if module is not None:
        projection = getattr(module, "project_reconstructed_products", None)
        if callable(projection):
            return [
                _coerce_projected_payload(payload)
                for payload in projection(target, products, mapping=mapping)
            ]

    if target != DEFAULT_PROJECTION_TARGET:
        raise ValueError(
            f"No projection configured for target {target!r}. Define "
            "project_reconstructed_products(target, reconstructed_products, *, mapping=None) "
            f"in {RECONSTRUCTION_CONFIG_PATH}."
        )

    return [
        CentricProductPayload.model_validate(product.graph)
        for product in products
        if product.graph
    ]


def reconstruct_products_from_records(
    records_by_endpoint: Mapping[str, Iterable[dict[str, Any]]],
    *,
    mapping: ProjectionMapping | None = None,
    reconstruction_path: Path | None = None,
) -> list[CentricProductPayload]:
    """Project endpoint snapshots, using private reconstruction logic when configured."""

    products = reconstruct_master_products_from_records(
        records_by_endpoint,
        mapping=mapping,
        reconstruction_path=reconstruction_path,
    )
    payloads = project_master_products(
        products,
        target=DEFAULT_PROJECTION_TARGET,
        mapping=mapping,
        reconstruction_path=reconstruction_path,
    )
    return [CentricProductPayload.model_validate(_payload_to_dict(payload)) for payload in payloads]


def load_private_reconstruction(path: Path | None = None) -> ReconstructionFunction | None:
    module = load_private_reconstruction_module(path)
    if module is None:
        return None

    function = getattr(module, "reconstruct_projected_products", None)
    if not callable(function):
        raise ValueError(
            "Private reconstruction module must define reconstruct_projected_products("
            "records_by_endpoint, *, mapping=None)."
        )
    return function


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


def _master_product_from_dpp_payload(
    payload: CentricProductPayload | Mapping[str, Any],
) -> ReconstructedProduct:
    product = CentricProductPayload.model_validate(_payload_to_dict(payload))
    graph = product.model_dump(mode="json", exclude_none=True)
    return ReconstructedProduct(
        product_id=product.centric_style_id,
        style_id=product.centric_style_id,
        brand_code=product.brand_code,
        season=product.season,
        product_type_code=product.product_type_code,
        graph=graph,
        source_refs=(
            ReconstructionSourceRef(
                endpoint="styles",
                record_id=product.centric_style_id,
                relation_type="style",
            ),
        ),
    )


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


def _coerce_projected_payload(payload: BaseModel | Mapping[str, Any]) -> BaseModel | dict[str, Any]:
    if isinstance(payload, BaseModel):
        return payload
    if isinstance(payload, Mapping):
        return dict(payload)
    raise TypeError("Projected payloads must be Pydantic models or mappings.")


def _payload_to_dict(payload: BaseModel | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(payload, BaseModel):
        return payload.model_dump(mode="json", exclude_none=True)
    return dict(payload)


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
