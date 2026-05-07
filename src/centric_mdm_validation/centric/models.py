from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal


@dataclass
class CountSpec:
    api_version: str
    path: str
    query_params: dict[str, Any] = field(default_factory=dict)
    result_path: str = "$.total"


@dataclass
class EndpointSpec:
    name: str
    api_version: str
    path: str
    query_params: dict[str, Any] = field(default_factory=dict)
    skip_param: str = "skip"
    limit_param: str = "limit"
    limit: int = 50
    item_path: str = "$"
    count_spec: CountSpec | None = None


@dataclass
class FetcherConfig:
    base_url: str = ""
    timeout: float = 30.0
    retry_max_attempts: int = 5
    retry_base_seconds: float = 0.5
    retry_max_seconds: float = 8.0
    jitter_ratio: float = 0.2
    output_dir: Path = Path("data/output")
    checkpoint_dir: Path = Path("data/checkpoints")


@dataclass
class AuthSettings:
    timeout: float = 30.0
    env_file: Path = Path(".env")


@dataclass
class FetchRunResult:
    endpoint: str
    pages_fetched: int
    items_fetched: int
    expected_count: int | None
    retries_used: int
    start_skip: int
    next_skip: int
    duration_seconds: float
    output_file: Path
    checkpoint_file: Path
    warnings: list[str] = field(default_factory=list)
    already_completed: bool = False
    effective_delta_floor: str | None = None
    did_catch_up: bool = False
    count_validation_status: str = "skipped"
    count_validation_reason: str | None = None
    id_validation_status: str = "not_run"
    id_validation_checked_items: int = 0
    id_validation_unique_ids: int = 0
    id_validation_reason: str | None = None


@dataclass
class FetchProgressEvent:
    kind: Literal["endpoint_start", "page_fetched", "warning", "endpoint_finish"]
    endpoint: str
    page_index: int | None = None
    page_items: int | None = None
    pages_fetched: int | None = None
    items_fetched: int | None = None
    skip: int | None = None
    next_skip: int | None = None
    start_skip: int | None = None
    limit: int | None = None
    expected_count: int | None = None
    expected_pages: int | None = None
    percent_complete: float | None = None
    page_duration_seconds: float | None = None
    rolling_avg_seconds: float | None = None
    estimated_remaining_seconds: float | None = None
    retries_used: int | None = None
    elapsed_seconds: float | None = None
    warnings_count: int | None = None
    message: str | None = None
