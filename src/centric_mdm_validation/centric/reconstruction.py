from __future__ import annotations

import importlib.util
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, Protocol

from centric_mdm_validation.centric.config import resolve_optional_private_config_path
from centric_mdm_validation.centric.mapper import ProjectionMapping, project_products
from centric_mdm_validation.models import CentricProductPayload

RECONSTRUCTION_CONFIG_PATH = Path("reconstruction.py")


class ReconstructionFunction(Protocol):
    def __call__(
        self,
        records_by_endpoint: Mapping[str, Iterable[dict[str, Any]]],
        *,
        mapping: ProjectionMapping | None = None,
    ) -> list[CentricProductPayload]: ...


def reconstruct_products_from_records(
    records_by_endpoint: Mapping[str, Iterable[dict[str, Any]]],
    *,
    mapping: ProjectionMapping | None = None,
    reconstruction_path: Path | None = None,
) -> list[CentricProductPayload]:
    """Project endpoint snapshots, using private reconstruction logic when configured."""

    reconstruction = load_private_reconstruction(reconstruction_path)
    if reconstruction is None:
        return project_products(records_by_endpoint, mapping=mapping)
    return reconstruction(records_by_endpoint, mapping=mapping)


def load_private_reconstruction(path: Path | None = None) -> ReconstructionFunction | None:
    resolved_path = resolve_reconstruction_path(path)
    if resolved_path is None:
        return None

    spec = importlib.util.spec_from_file_location("centric_private_reconstruction", resolved_path)
    if spec is None or spec.loader is None:
        raise ValueError(f"Could not load reconstruction module: {resolved_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    function = getattr(module, "reconstruct_projected_products", None)
    if not callable(function):
        raise ValueError(
            "Private reconstruction module must define reconstruct_projected_products("
            "records_by_endpoint, *, mapping=None)."
        )
    return function


def resolve_reconstruction_path(path: Path | None = None) -> Path | None:
    if path is not None:
        return path

    return resolve_optional_private_config_path(RECONSTRUCTION_CONFIG_PATH)
