from __future__ import annotations

from dataclasses import replace
from typing import Any, Literal

from .models import EndpointSpec

SortPolicy = Literal["preserve", "if_missing", "force"]


def strip_modified_at_filters(query_params: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in query_params.items()
        if str(key) != "_modified_at" and not str(key).startswith("_modified_at=")
    }


def apply_data_sort(
    spec: EndpointSpec,
    *,
    sort_value: str,
    policy: SortPolicy,
) -> EndpointSpec:
    if policy == "preserve":
        return spec

    if policy == "if_missing" and "sort" in spec.query_params:
        return spec

    query_params = dict(spec.query_params)
    query_params["sort"] = sort_value
    return replace(spec, query_params=query_params)


def build_delta_endpoint_spec(
    spec: EndpointSpec,
    delta_floor: str | None,
    *,
    force_sort: bool = False,
) -> EndpointSpec:
    should_mutate = force_sort or delta_floor is not None
    if not should_mutate:
        return spec

    query_params = strip_modified_at_filters(spec.query_params)
    if delta_floor is not None:
        query_params["_modified_at=ge"] = delta_floor

    next_count_spec = None
    if spec.count_spec is not None:
        count_query_params = strip_modified_at_filters(spec.count_spec.query_params)
        if delta_floor is not None:
            count_query_params["_modified_at=ge"] = delta_floor
        next_count_spec = replace(spec.count_spec, query_params=count_query_params)

    mutated = replace(spec, query_params=query_params, count_spec=next_count_spec)
    if force_sort or delta_floor is not None:
        return apply_data_sort(mutated, sort_value="_modified_at", policy="force")
    return mutated
