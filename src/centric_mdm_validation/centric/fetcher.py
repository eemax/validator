from __future__ import annotations

import json
import random
import re
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from .auth import AuthContext, AuthError
from .delta import build_delta_endpoint_spec
from .models import EndpointSpec, FetcherConfig, FetchProgressEvent, FetchRunResult


class FetchError(RuntimeError):
    pass


_TRANSIENT_STATUSES = {429}
_PATH_RE = re.compile(r"(?:\.([A-Za-z_][A-Za-z0-9_]*))|(?:\[(\d+)\])")
_OPERATOR_SUFFIXES = {"!", "ge", "gt", "le", "lt"}

RequestParams = dict[str, Any] | list[tuple[str, Any]]
ApiLogEvent = dict[str, Any]
ApiLogCallback = Callable[[ApiLogEvent], None] | None


@dataclass
class _Page:
    skip: int
    items: list[dict]


@dataclass
class _CheckpointState:
    exists: bool
    valid: bool
    next_skip: int = 0
    fetched_count: int = 0
    delta_floor: str | None = None
    completed: bool | None = None
    restart_from_zero: bool = False
    window_start_line: int | None = None
    output_file: Path | None = None
    invalid_reason: str | None = None


def _safe_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name)


def _parse_path(path: str) -> list[tuple[str, Any]]:
    if path == "$":
        return []
    if not path.startswith("$"):
        raise FetchError(f"Invalid JSON path: {path}")

    tokens: list[tuple[str, Any]] = []
    index = 1
    while index < len(path):
        match = _PATH_RE.match(path, index)
        if not match:
            raise FetchError(f"Unsupported JSON path segment near '{path[index:]}'")
        key, idx = match.groups()
        if key is not None:
            tokens.append(("key", key))
        elif idx is not None:
            tokens.append(("idx", int(idx)))
        index = match.end()
    return tokens


def extract_json_path(payload: Any, path: str) -> Any:
    current = payload
    for kind, value in _parse_path(path):
        if kind == "key":
            if not isinstance(current, dict) or value not in current:
                raise FetchError(f"JSON path not found: {path}")
            current = current[value]
        else:
            if not isinstance(current, list) or value >= len(current):
                raise FetchError(f"JSON path not found: {path}")
            current = current[value]
    return current


def _is_transient_status(status_code: int) -> bool:
    return status_code in _TRANSIENT_STATUSES or 500 <= status_code <= 599


def _is_transient_exception(exc: Exception) -> bool:
    return isinstance(exc, (httpx.TimeoutException, httpx.TransportError))


def _sleep_backoff(fetcher_cfg: FetcherConfig, attempt: int) -> None:
    base = fetcher_cfg.retry_base_seconds * (2 ** (attempt - 1))
    capped = min(base, fetcher_cfg.retry_max_seconds)
    jitter = fetcher_cfg.jitter_ratio
    low = max(0.0, capped * (1.0 - jitter))
    high = capped * (1.0 + jitter)
    time.sleep(random.uniform(low, high))


def _build_endpoint_url(base_url: str, api_version: str, path: str) -> str:
    normalized = path.strip().strip("/")
    return f"{base_url}/api/{api_version}/{normalized}"


def _compile_query_params(query_params: dict[str, Any]) -> list[tuple[str, Any]]:
    params: list[tuple[str, Any]] = []
    has_decoded = False
    for raw_key, value in query_params.items():
        key = str(raw_key)
        field, sep, suffix = key.rpartition("=")
        if key == "decoded" or (sep and field == "decoded" and suffix == ""):
            has_decoded = True
        if sep and field and suffix in _OPERATOR_SUFFIXES:
            params.append((field, f"{suffix}{value}"))
        elif sep and field and suffix == "":
            # Treat trailing "=" as plain equality (e.g. "active=" -> "active").
            params.append((field, value))
        else:
            params.append((key, value))
    if not has_decoded:
        params.append(("decoded", True))
    return params


def _with_pagination_params(
    params: list[tuple[str, Any]],
    *,
    skip_param: str,
    skip: int,
    limit_param: str,
    limit: int,
) -> list[tuple[str, Any]]:
    base_params = [(key, value) for key, value in params if key not in {skip_param, limit_param}]
    base_params.append((skip_param, skip))
    base_params.append((limit_param, limit))
    return base_params


def _emit_api_log(api_log_callback: ApiLogCallback, event: ApiLogEvent) -> None:
    if api_log_callback is not None:
        api_log_callback(event)


def _format_request_url(url: str, params: RequestParams | None) -> str:
    if not params:
        return url
    return str(httpx.URL(url).copy_merge_params(params))


def _request_json_with_retry(
    auth_ctx: AuthContext,
    *,
    method: str,
    url: str,
    params: RequestParams | None,
    fetcher_cfg: FetcherConfig,
    retries_used_ref: list[int],
    progress_callback: Callable[[FetchProgressEvent], None] | None = None,
    endpoint_name: str | None = None,
    request_kind: str = "request",
    api_log_callback: ApiLogCallback = None,
) -> Any:
    last_error: Exception | None = None
    for attempt in range(1, fetcher_cfg.retry_max_attempts + 1):
        request_url = _format_request_url(url, params)
        _emit_api_log(
            api_log_callback,
            {
                "level": "http",
                "event": "http_request",
                "endpoint": endpoint_name,
                "request_kind": request_kind,
                "method": method.upper(),
                "url": request_url,
                "attempt": attempt,
                "max_attempts": fetcher_cfg.retry_max_attempts,
            },
        )
        try:
            response = auth_ctx.request(method, url, params=params)
        except Exception as exc:  # pragma: no cover - explicit branch tested indirectly
            if _is_transient_exception(exc):
                last_error = exc
                if attempt < fetcher_cfg.retry_max_attempts:
                    retries_used_ref[0] += 1
                    _emit_api_log(
                        api_log_callback,
                        {
                            "level": "debug",
                            "event": "retry_scheduled",
                            "endpoint": endpoint_name,
                            "request_kind": request_kind,
                            "reason": "transient_transport_error",
                            "error": str(exc),
                            "attempt": attempt,
                            "next_attempt": attempt + 1,
                            "max_attempts": fetcher_cfg.retry_max_attempts,
                        },
                    )
                    _emit_progress(
                        progress_callback,
                        FetchProgressEvent(
                            kind="warning",
                            endpoint=endpoint_name or "unknown",
                            retries_used=retries_used_ref[0],
                            message=(
                                f"{request_kind} transient transport error on attempt "
                                f"{attempt}/{fetcher_cfg.retry_max_attempts}: {exc}. "
                                "Action: retrying."
                            ),
                        ),
                    )
                    _sleep_backoff(fetcher_cfg, attempt)
                    continue
                _emit_api_log(
                    api_log_callback,
                    {
                        "level": "summary",
                        "event": "request_failed",
                        "endpoint": endpoint_name,
                        "request_kind": request_kind,
                        "reason": "transport_error_after_retries",
                        "error": str(exc),
                        "attempt": attempt,
                        "max_attempts": fetcher_cfg.retry_max_attempts,
                    },
                )
                raise FetchError(
                    f"{request_kind} failed after retries due to transport error: {exc}. "
                    "Action: exit endpoint."
                ) from exc
            raise FetchError(str(exc)) from exc

        _emit_api_log(
            api_log_callback,
            {
                "level": "http",
                "event": "http_response",
                "endpoint": endpoint_name,
                "request_kind": request_kind,
                "method": method.upper(),
                "url": request_url,
                "attempt": attempt,
                "max_attempts": fetcher_cfg.retry_max_attempts,
                "status_code": response.status_code,
                "reason_phrase": response.reason_phrase or "",
            },
        )

        if response.status_code >= 400:
            if _is_transient_status(response.status_code):
                if attempt < fetcher_cfg.retry_max_attempts:
                    retries_used_ref[0] += 1
                    _emit_api_log(
                        api_log_callback,
                        {
                            "level": "debug",
                            "event": "retry_scheduled",
                            "endpoint": endpoint_name,
                            "request_kind": request_kind,
                            "reason": "transient_http_status",
                            "status_code": response.status_code,
                            "attempt": attempt,
                            "next_attempt": attempt + 1,
                            "max_attempts": fetcher_cfg.retry_max_attempts,
                        },
                    )
                    _emit_progress(
                        progress_callback,
                        FetchProgressEvent(
                            kind="warning",
                            endpoint=endpoint_name or "unknown",
                            retries_used=retries_used_ref[0],
                            message=(
                                f"{request_kind} got transient HTTP {response.status_code} "
                                "on attempt "
                                f"{attempt}/{fetcher_cfg.retry_max_attempts}. Action: retrying."
                            ),
                        ),
                    )
                    _sleep_backoff(fetcher_cfg, attempt)
                    continue
                _emit_api_log(
                    api_log_callback,
                    {
                        "level": "summary",
                        "event": "request_failed",
                        "endpoint": endpoint_name,
                        "request_kind": request_kind,
                        "reason": "transient_http_status_after_retries",
                        "status_code": response.status_code,
                        "attempt": attempt,
                        "max_attempts": fetcher_cfg.retry_max_attempts,
                    },
                )
                raise FetchError(
                    f"{request_kind} failed after retries (HTTP {response.status_code}; "
                    f"{_summarize_response_body(response.text)}). Action: exit endpoint."
                )
            _emit_api_log(
                api_log_callback,
                {
                    "level": "summary",
                    "event": "request_failed",
                    "endpoint": endpoint_name,
                    "request_kind": request_kind,
                    "reason": "non_retryable_http_status",
                    "status_code": response.status_code,
                    "attempt": attempt,
                    "max_attempts": fetcher_cfg.retry_max_attempts,
                },
            )
            raise FetchError(
                f"{request_kind} failed with non-retryable HTTP {response.status_code} "
                f"({_summarize_response_body(response.text)}). Action: exit endpoint."
            )

        try:
            return response.json()
        except Exception as exc:
            raise FetchError(f"Failed to parse JSON response: {exc}") from exc

    if last_error:
        raise FetchError(f"Request failed: {last_error}")
    raise FetchError("Request failed unexpectedly.")


def _emit_progress(
    progress_callback: Callable[[FetchProgressEvent], None] | None,
    event: FetchProgressEvent,
) -> None:
    if progress_callback is not None:
        progress_callback(event)


def _summarize_response_body(response_text: str) -> str:
    text = response_text.strip()
    if not text:
        return "empty response body"

    try:
        payload = json.loads(text)
    except Exception:
        flattened = " ".join(text.split())
        preview = flattened[:180]
        if len(flattened) > 180:
            preview += "..."
        return f"non-JSON body preview={preview!r}"

    if isinstance(payload, dict):
        for key in ("error", "message", "detail", "details"):
            value = payload.get(key)
            if isinstance(value, (str, int, float, bool)):
                return f"json {key}={value!r}"
        return f"json object keys={list(payload.keys())[:5]}"
    if isinstance(payload, list):
        return f"json array items={len(payload)}"
    return f"json {type(payload).__name__}"


def _extract_items(payload: Any, item_path: str) -> list[dict]:
    raw = extract_json_path(payload, item_path)
    if not isinstance(raw, list):
        raise FetchError(f"item_path '{item_path}' did not resolve to an array.")

    items: list[dict] = []
    for idx, item in enumerate(raw):
        if not isinstance(item, dict):
            raise FetchError(f"Item at index {idx} is not an object.")
        items.append(item)
    return items


def _read_checkpoint(path: Path) -> _CheckpointState:
    if not path.is_file():
        return _CheckpointState(exists=False, valid=True)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return _CheckpointState(
            exists=True,
            valid=False,
            invalid_reason=f"file is not valid JSON ({exc})",
        )
    if not isinstance(payload, dict):
        return _CheckpointState(
            exists=True,
            valid=False,
            invalid_reason="checkpoint root must be an object",
        )

    next_skip = payload.get("next_skip", 0)
    fetched_count = payload.get("fetched_count", 0)
    checkpoint_delta_floor = payload.get("delta_floor")
    completed = payload.get("completed")
    restart_from_zero = payload.get("restart_from_zero", False)
    window_start_line = payload.get("window_start_line")
    output_file = payload.get("output_file")

    if not isinstance(next_skip, int) or next_skip < 0:
        return _CheckpointState(
            exists=True,
            valid=False,
            invalid_reason="next_skip must be a non-negative integer",
        )
    if not isinstance(fetched_count, int) or fetched_count < 0:
        return _CheckpointState(
            exists=True,
            valid=False,
            invalid_reason="fetched_count must be a non-negative integer",
        )

    normalized_delta_floor: str | None = None
    if checkpoint_delta_floor is not None:
        if not isinstance(checkpoint_delta_floor, str):
            return _CheckpointState(
                exists=True,
                valid=False,
                invalid_reason="delta_floor must be a string when present",
            )
        stripped = checkpoint_delta_floor.strip()
        normalized_delta_floor = stripped or None

    normalized_completed: bool | None
    if completed is None:
        normalized_completed = None
    elif isinstance(completed, bool):
        normalized_completed = completed
    else:
        return _CheckpointState(
            exists=True,
            valid=False,
            invalid_reason="completed must be true/false when present",
        )

    if not isinstance(restart_from_zero, bool):
        return _CheckpointState(
            exists=True,
            valid=False,
            invalid_reason="restart_from_zero must be true/false when present",
        )

    normalized_window_start_line: int | None
    if window_start_line is None:
        normalized_window_start_line = None
    elif isinstance(window_start_line, int) and window_start_line >= 0:
        normalized_window_start_line = window_start_line
    else:
        return _CheckpointState(
            exists=True,
            valid=False,
            invalid_reason="window_start_line must be a non-negative integer when present",
        )

    normalized_output_file: Path | None = None
    if output_file is not None:
        if not isinstance(output_file, str) or not output_file.strip():
            return _CheckpointState(
                exists=True,
                valid=False,
                invalid_reason="output_file must be a non-empty string when present",
            )
        normalized_output_file = Path(output_file.strip())

    return _CheckpointState(
        exists=True,
        valid=True,
        next_skip=next_skip,
        fetched_count=fetched_count,
        delta_floor=normalized_delta_floor,
        completed=normalized_completed,
        restart_from_zero=restart_from_zero,
        window_start_line=normalized_window_start_line,
        output_file=normalized_output_file,
    )


def _write_checkpoint(
    path: Path,
    endpoint: str,
    next_skip: int,
    fetched_count: int,
    *,
    delta_floor: str | None = None,
    completed: bool = False,
    restart_from_zero: bool = False,
    window_start_line: int | None = None,
    output_file: Path | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "endpoint": endpoint,
        "next_skip": next_skip,
        "fetched_count": fetched_count,
        "updated_at": datetime.now(UTC).isoformat(),
        "completed": completed,
        "restart_from_zero": restart_from_zero,
    }
    if delta_floor is not None:
        payload["delta_floor"] = delta_floor
    if window_start_line is not None:
        payload["window_start_line"] = window_start_line
    if output_file is not None:
        payload["output_file"] = str(output_file)
    temp_path = path.parent / f".{path.name}.tmp"
    temp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    temp_path.replace(path)


def _count_file_lines(path: Path) -> int:
    if not path.is_file():
        return 0
    count = 0
    with path.open("r", encoding="utf-8") as fh:
        for _ in fh:
            count += 1
    return count


def _track_item_id(
    item: Any,
    *,
    seen_ids: set[Any],
    duplicate_id_set: set[Any],
    duplicate_ids: list[Any],
) -> str | None:
    if not isinstance(item, dict):
        return f"line value is non-object type {type(item).__name__}"
    if "id" not in item:
        return "missing field 'id'"

    item_id = item["id"]
    if item_id is None:
        return "field 'id' is null"
    try:
        hash(item_id)
    except TypeError:
        return f"field 'id' is unhashable type {type(item_id).__name__}"

    if item_id in seen_ids:
        if item_id not in duplicate_id_set:
            duplicate_id_set.add(item_id)
            duplicate_ids.append(item_id)
        return None

    seen_ids.add(item_id)
    return None


def _seed_resume_window_id_state(
    output_path: Path,
    *,
    endpoint_name: str,
    window_start_line: int,
    fetched_count: int,
    seen_ids: set[Any],
    duplicate_id_set: set[Any],
    duplicate_ids: list[Any],
) -> tuple[int, int, str | None]:
    if fetched_count <= 0:
        return 0, 0, None
    if not output_path.is_file():
        raise FetchError(
            f"Output file missing for resume validation on '{endpoint_name}': {output_path}. "
            "Action: restart endpoint window."
        )

    start = window_start_line
    end = window_start_line + fetched_count
    loaded = 0
    invalid_id_count = 0
    first_invalid_detail: str | None = None
    with output_path.open("r", encoding="utf-8") as fh:
        for line_index, line in enumerate(fh):
            if line_index < start:
                continue
            if line_index >= end:
                break
            loaded += 1
            text = line.strip()
            if not text:
                invalid_id_count += 1
                if first_invalid_detail is None:
                    first_invalid_detail = f"line {line_index + 1} is empty"
                continue
            try:
                item = json.loads(text)
            except Exception as exc:
                raise FetchError(
                    "Invalid JSONL while validating resume checkpoint window "
                    f"for '{endpoint_name}' "
                    f"at line {line_index + 1}: {exc}. Action: restart endpoint window."
                ) from exc

            invalid_detail = _track_item_id(
                item,
                seen_ids=seen_ids,
                duplicate_id_set=duplicate_id_set,
                duplicate_ids=duplicate_ids,
            )
            if invalid_detail is not None:
                invalid_id_count += 1
                if first_invalid_detail is None:
                    first_invalid_detail = f"line {line_index + 1}: {invalid_detail}"

    if loaded != fetched_count:
        raise FetchError(
            f"Resume checkpoint for '{endpoint_name}' expects {fetched_count} records "
            "in output window "
            f"starting at line {window_start_line + 1}, but found {loaded}. "
            "Action: restart endpoint window."
        )
    return loaded, invalid_id_count, first_invalid_detail


def _iter_pages(
    spec: EndpointSpec,
    auth_ctx: AuthContext,
    fetcher_cfg: FetcherConfig,
    *,
    start_skip: int,
    expected_total: int | None,
    retries_used_ref: list[int],
    progress_callback: Callable[[FetchProgressEvent], None] | None = None,
    api_log_callback: ApiLogCallback = None,
) -> Iterator[_Page]:
    skip = start_skip
    fetched = 0
    url = _build_endpoint_url(
        fetcher_cfg.base_url or auth_ctx.base_url,
        spec.api_version,
        spec.path,
    )
    base_params = _compile_query_params(spec.query_params)

    if expected_total == 0:
        return

    while True:
        params = _with_pagination_params(
            base_params,
            skip_param=spec.skip_param,
            skip=skip,
            limit_param=spec.limit_param,
            limit=spec.limit,
        )

        payload = _request_json_with_retry(
            auth_ctx,
            method="GET",
            url=url,
            params=params,
            fetcher_cfg=fetcher_cfg,
            retries_used_ref=retries_used_ref,
            progress_callback=progress_callback,
            endpoint_name=spec.name,
            request_kind="data fetch",
            api_log_callback=api_log_callback,
        )
        items = _extract_items(payload, spec.item_path)
        page = _Page(skip=skip, items=items)
        yield page

        fetched += len(items)
        if expected_total is not None and fetched >= expected_total:
            break
        if not items or len(items) < spec.limit:
            break
        skip += spec.limit


def get_expected_count(
    spec: EndpointSpec,
    auth_ctx: AuthContext,
    fetcher_cfg: FetcherConfig,
    retries_used_ref: list[int] | None = None,
    progress_callback: Callable[[FetchProgressEvent], None] | None = None,
    api_log_callback: ApiLogCallback = None,
) -> int | None:
    if spec.count_spec is None:
        return None

    retries_ref = retries_used_ref if retries_used_ref is not None else [0]
    count_url = _build_endpoint_url(
        fetcher_cfg.base_url or auth_ctx.base_url,
        spec.count_spec.api_version,
        spec.count_spec.path,
    )
    payload = _request_json_with_retry(
        auth_ctx,
        method="GET",
        url=count_url,
        params=_compile_query_params(spec.count_spec.query_params),
        fetcher_cfg=fetcher_cfg,
        retries_used_ref=retries_ref,
        progress_callback=progress_callback,
        endpoint_name=spec.name,
        request_kind="count preflight",
        api_log_callback=api_log_callback,
    )
    result = extract_json_path(payload, spec.count_spec.result_path)
    if isinstance(result, bool) or not isinstance(result, (int, float)):
        raise FetchError(f"Count path '{spec.count_spec.result_path}' did not resolve to a number.")
    if result < 0:
        raise FetchError("Count result cannot be negative.")
    return int(result)


def iter_endpoint_items(
    spec: EndpointSpec,
    auth_ctx: AuthContext,
    fetcher_cfg: FetcherConfig,
    start_skip: int = 0,
) -> Iterator[dict]:
    retries_used_ref = [0]
    for page in _iter_pages(
        spec,
        auth_ctx,
        fetcher_cfg,
        start_skip=start_skip,
        expected_total=None,
        retries_used_ref=retries_used_ref,
        progress_callback=None,
    ):
        yield from page.items


def run_endpoint(
    spec: EndpointSpec,
    auth_ctx: AuthContext,
    fetcher_cfg: FetcherConfig,
    resume: bool = False,
    append_output: bool = False,
    output_file_suffix: str = "",
    delta_floor: str | None = None,
    progress_callback: Callable[[FetchProgressEvent], None] | None = None,
    api_log_callback: ApiLogCallback = None,
    resume_completed_hint: bool | None = None,
) -> FetchRunResult:
    started = time.time()
    retries_used_ref = [0]
    warnings: list[str] = []

    safe_name = _safe_name(spec.name)
    output_path = fetcher_cfg.output_dir / f"{safe_name}{output_file_suffix}.jsonl"
    checkpoint_path = fetcher_cfg.checkpoint_dir / f"{safe_name}.json"

    checkpoint_state = _read_checkpoint(checkpoint_path)
    if resume and checkpoint_state.exists and not checkpoint_state.valid:
        _emit_api_log(
            api_log_callback,
            {
                "level": "summary",
                "event": "checkpoint_invalid",
                "endpoint": spec.name,
                "checkpoint_file": str(checkpoint_path),
                "reason": checkpoint_state.invalid_reason,
            },
        )
        raise FetchError(
            f"Invalid checkpoint for '{spec.name}' at '{checkpoint_path}': "
            f"{checkpoint_state.invalid_reason}. Action: repair/delete checkpoint "
            "or run without --resume."
        )

    checkpoint_skip = checkpoint_state.next_skip if checkpoint_state.valid else 0
    checkpoint_count = checkpoint_state.fetched_count if checkpoint_state.valid else 0
    checkpoint_delta_floor = checkpoint_state.delta_floor if checkpoint_state.valid else None
    checkpoint_completed = checkpoint_state.completed if checkpoint_state.valid else None
    checkpoint_restart_from_zero = (
        checkpoint_state.restart_from_zero if checkpoint_state.valid else False
    )
    checkpoint_window_start_line = (
        checkpoint_state.window_start_line if checkpoint_state.valid else None
    )
    checkpoint_output_file = checkpoint_state.output_file if checkpoint_state.valid else None
    if resume and checkpoint_output_file is not None and checkpoint_completed is not True:
        output_path = checkpoint_output_file
    effective_delta_floor = delta_floor
    start_skip = checkpoint_skip if resume else 0
    items_fetched = checkpoint_count if (resume and checkpoint_skip > 0) else 0
    checkpoint_warning: str | None = None
    force_output_rewrite = False
    checkpoint_completed_resolved = checkpoint_completed
    if checkpoint_completed_resolved is None and resume_completed_hint is not None:
        checkpoint_completed_resolved = resume_completed_hint

    if resume and checkpoint_restart_from_zero:
        force_output_rewrite = True
        start_skip = 0
        items_fetched = 0
        if checkpoint_delta_floor is not None:
            effective_delta_floor = checkpoint_delta_floor

    # Legacy checkpoints might not include window_start_line.
    # Restart safely to avoid false integrity success.
    if resume and start_skip > 0 and checkpoint_window_start_line is None:
        force_output_rewrite = True
        start_skip = 0
        items_fetched = 0
        if checkpoint_delta_floor is not None:
            effective_delta_floor = checkpoint_delta_floor

    _emit_api_log(
        api_log_callback,
        {
            "level": "debug",
            "event": "checkpoint_state_loaded",
            "endpoint": spec.name,
            "checkpoint_file": str(checkpoint_path),
            "checkpoint_exists": checkpoint_state.exists,
            "checkpoint_valid": checkpoint_state.valid,
            "checkpoint_next_skip": checkpoint_skip,
            "checkpoint_fetched_count": checkpoint_count,
            "checkpoint_delta_floor": checkpoint_delta_floor,
            "checkpoint_completed": checkpoint_completed_resolved,
            "checkpoint_restart_from_zero": checkpoint_restart_from_zero,
            "checkpoint_window_start_line": checkpoint_window_start_line,
            "resume": resume,
            "delta_floor": delta_floor,
            "force_output_rewrite": force_output_rewrite,
        },
    )

    if resume and checkpoint_completed_resolved is True:
        effective_delta_floor = checkpoint_delta_floor or delta_floor
        message = (
            f"Fetch already completed for '{spec.name}'"
            + (
                " ("
                f"checkpoint delta_floor={checkpoint_delta_floor}, "
                f"current delta_floor={delta_floor}"
                ")"
                if checkpoint_delta_floor is not None or delta_floor is not None
                else ""
            )
            + ". Run without --resume to start a new window."
        )
        _emit_progress(
            progress_callback,
            FetchProgressEvent(
                kind="endpoint_start",
                endpoint=spec.name,
                start_skip=start_skip,
                limit=spec.limit,
                expected_count=None,
                retries_used=retries_used_ref[0],
                elapsed_seconds=time.time() - started,
            ),
        )
        _emit_progress(
            progress_callback,
            FetchProgressEvent(
                kind="warning",
                endpoint=spec.name,
                message=message,
                retries_used=retries_used_ref[0],
                elapsed_seconds=time.time() - started,
            ),
        )
        duration = time.time() - started
        _emit_progress(
            progress_callback,
            FetchProgressEvent(
                kind="endpoint_finish",
                endpoint=spec.name,
                pages_fetched=0,
                items_fetched=0,
                expected_count=None,
                retries_used=retries_used_ref[0],
                warnings_count=1,
                elapsed_seconds=duration,
            ),
        )
        _emit_api_log(
            api_log_callback,
            {
                "level": "debug",
                "event": "endpoint_resume_noop",
                "endpoint": spec.name,
                "start_skip": start_skip,
                "effective_delta_floor": effective_delta_floor,
                "message": message,
            },
        )
        return FetchRunResult(
            endpoint=spec.name,
            pages_fetched=0,
            items_fetched=0,
            expected_count=None,
            retries_used=retries_used_ref[0],
            start_skip=start_skip,
            next_skip=start_skip,
            duration_seconds=duration,
            output_file=output_path,
            checkpoint_file=checkpoint_path,
            warnings=[message],
            already_completed=True,
            effective_delta_floor=effective_delta_floor,
            did_catch_up=False,
            count_validation_status="skipped",
            count_validation_reason="resume_already_completed",
            id_validation_status="not_run",
            id_validation_checked_items=0,
            id_validation_unique_ids=0,
            id_validation_reason="resume_already_completed",
        )

    if (
        resume
        and start_skip > 0
        and delta_floor is not None
        and checkpoint_delta_floor is not None
        and checkpoint_delta_floor != delta_floor
    ):
        checkpoint_warning = (
            f"Checkpoint delta floor mismatch for '{spec.name}': "
            f"checkpoint={checkpoint_delta_floor} "
            f"current={delta_floor}. Resume continues with checkpoint delta floor."
        )
        warnings.append(checkpoint_warning)
        _emit_api_log(
            api_log_callback,
            {
                "level": "debug",
                "event": "delta_floor_mismatch",
                "endpoint": spec.name,
                "checkpoint_delta_floor": checkpoint_delta_floor,
                "current_delta_floor": delta_floor,
            },
        )
        effective_delta_floor = checkpoint_delta_floor

    spec = build_delta_endpoint_spec(spec, effective_delta_floor)
    _emit_api_log(
        api_log_callback,
        {
            "level": "debug",
            "event": "runtime_query_prepared",
            "endpoint": spec.name,
            "has_count_spec": spec.count_spec is not None,
            "effective_delta_floor": effective_delta_floor,
            "data_sort": spec.query_params.get("sort"),
        },
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    mode = "w" if force_output_rewrite else ("a" if (start_skip > 0 or append_output) else "w")
    window_start_line = 0
    if mode == "a":
        if resume and start_skip > 0:
            # For resumed windows, continue using the original invocation anchor.
            window_start_line = checkpoint_window_start_line or 0
        else:
            window_start_line = _count_file_lines(output_path)

    if resume:
        _write_checkpoint(
            checkpoint_path,
            spec.name,
            start_skip,
            items_fetched,
            delta_floor=effective_delta_floor,
            completed=False,
            restart_from_zero=False,
            window_start_line=window_start_line,
            output_file=output_path,
        )
        _emit_api_log(
            api_log_callback,
            {
                "level": "debug",
                "event": "checkpoint_written",
                "endpoint": spec.name,
                "checkpoint_file": str(checkpoint_path),
                "next_skip": start_skip,
                "fetched_count": items_fetched,
                "completed": False,
                "delta_floor": effective_delta_floor,
                "restart_from_zero": False,
                "window_start_line": window_start_line,
            },
        )

    expected_count: int | None = None
    preflight_warning: str | None = None
    try:
        expected_count = get_expected_count(
            spec,
            auth_ctx,
            fetcher_cfg,
            retries_used_ref=retries_used_ref,
            progress_callback=progress_callback,
            api_log_callback=api_log_callback,
        )
    except (FetchError, AuthError) as exc:
        preflight_warning = f"Count preflight unavailable for '{spec.name}': {exc}"
        warnings.append(preflight_warning)
        _emit_api_log(
            api_log_callback,
            {
                "level": "debug",
                "event": "count_preflight_unavailable",
                "endpoint": spec.name,
                "error": str(exc),
            },
        )

    _emit_progress(
        progress_callback,
        FetchProgressEvent(
            kind="endpoint_start",
            endpoint=spec.name,
            start_skip=start_skip,
            limit=spec.limit,
            expected_count=expected_count,
            retries_used=retries_used_ref[0],
            elapsed_seconds=time.time() - started,
        ),
    )

    if preflight_warning:
        _emit_progress(
            progress_callback,
            FetchProgressEvent(
                kind="warning",
                endpoint=spec.name,
                message=preflight_warning,
                retries_used=retries_used_ref[0],
                elapsed_seconds=time.time() - started,
            ),
        )
    if checkpoint_warning:
        _emit_progress(
            progress_callback,
            FetchProgressEvent(
                kind="warning",
                endpoint=spec.name,
                message=checkpoint_warning,
                retries_used=retries_used_ref[0],
                elapsed_seconds=time.time() - started,
            ),
        )

    pages_fetched = 0
    next_skip = start_skip

    def _fail_integrity(message: str) -> None:
        _emit_progress(
            progress_callback,
            FetchProgressEvent(
                kind="warning",
                endpoint=spec.name,
                message=message,
                pages_fetched=pages_fetched,
                items_fetched=items_fetched,
                expected_count=expected_count,
                retries_used=retries_used_ref[0],
                elapsed_seconds=time.time() - started,
            ),
        )
        _write_checkpoint(
            checkpoint_path,
            spec.name,
            0,
            0,
            delta_floor=effective_delta_floor,
            completed=False,
            restart_from_zero=True,
            window_start_line=0,
            output_file=output_path,
        )
        _emit_api_log(
            api_log_callback,
            {
                "level": "summary",
                "event": "integrity_failure_checkpoint_marked",
                "endpoint": spec.name,
                "checkpoint_file": str(checkpoint_path),
                "next_skip": 0,
                "fetched_count": 0,
                "completed": False,
                "restart_from_zero": True,
                "window_start_line": 0,
                "delta_floor": effective_delta_floor,
            },
        )
        raise FetchError(message)

    should_validate_ids = expected_count is not None
    tracked_id_items = 0
    seen_ids: set[Any] = set()
    duplicate_ids: list[Any] = []
    duplicate_id_set: set[Any] = set()
    invalid_id_count = 0
    first_invalid_id_detail: str | None = None
    id_validation_status = "not_run"
    id_validation_checked_items = 0
    id_validation_unique_ids = 0
    id_validation_reason: str | None = None
    count_validation_status = "skipped"
    count_validation_reason: str | None = None

    if should_validate_ids and resume and start_skip > 0:
        try:
            (
                seeded_items,
                seeded_invalid_count,
                seeded_first_invalid_detail,
            ) = _seed_resume_window_id_state(
                output_path,
                endpoint_name=spec.name,
                window_start_line=window_start_line,
                fetched_count=items_fetched,
                seen_ids=seen_ids,
                duplicate_id_set=duplicate_id_set,
                duplicate_ids=duplicate_ids,
            )
        except FetchError as exc:
            _emit_api_log(
                api_log_callback,
                {
                    "level": "summary",
                    "event": "id_validation_seed_failed",
                    "endpoint": spec.name,
                    "error": str(exc),
                },
            )
            _fail_integrity(
                f"Post-fetch ID validation failed for '{spec.name}': "
                "unable to validate resumed window state "
                f"({exc}). Action: exit endpoint."
            )

        tracked_id_items += seeded_items
        invalid_id_count += seeded_invalid_count
        if first_invalid_id_detail is None and seeded_first_invalid_detail is not None:
            first_invalid_id_detail = seeded_first_invalid_detail
        _emit_api_log(
            api_log_callback,
            {
                "level": "debug",
                "event": "id_validation_seeded",
                "endpoint": spec.name,
                "seeded_items": seeded_items,
                "window_start_line": window_start_line,
                "window_item_count": items_fetched,
            },
        )

    with output_path.open(mode, encoding="utf-8") as out_fh:
        for page in _iter_pages(
            spec,
            auth_ctx,
            fetcher_cfg,
            start_skip=start_skip,
            expected_total=expected_count,
            retries_used_ref=retries_used_ref,
            progress_callback=progress_callback,
            api_log_callback=api_log_callback,
        ):
            pages_fetched += 1
            for item in page.items:
                out_fh.write(json.dumps(item, separators=(",", ":")) + "\n")
                if should_validate_ids:
                    tracked_id_items += 1
                    invalid_detail = _track_item_id(
                        item,
                        seen_ids=seen_ids,
                        duplicate_id_set=duplicate_id_set,
                        duplicate_ids=duplicate_ids,
                    )
                    if invalid_detail is not None:
                        invalid_id_count += 1
                        if first_invalid_id_detail is None:
                            first_invalid_id_detail = invalid_detail
            items_fetched += len(page.items)
            next_skip = page.skip + spec.limit
            _write_checkpoint(
                checkpoint_path,
                spec.name,
                next_skip,
                items_fetched,
                delta_floor=effective_delta_floor,
                completed=False,
                restart_from_zero=False,
                window_start_line=window_start_line,
                output_file=output_path,
            )
            _emit_api_log(
                api_log_callback,
                {
                    "level": "debug",
                    "event": "checkpoint_written",
                    "endpoint": spec.name,
                    "checkpoint_file": str(checkpoint_path),
                    "next_skip": next_skip,
                    "fetched_count": items_fetched,
                    "completed": False,
                    "delta_floor": effective_delta_floor,
                    "restart_from_zero": False,
                    "window_start_line": window_start_line,
                },
            )
            percent_complete: float | None = None
            if expected_count is not None and expected_count > 0:
                percent_complete = min(100.0, (items_fetched / expected_count) * 100.0)
            _emit_progress(
                progress_callback,
                FetchProgressEvent(
                    kind="page_fetched",
                    endpoint=spec.name,
                    page_index=pages_fetched,
                    page_items=len(page.items),
                    pages_fetched=pages_fetched,
                    items_fetched=items_fetched,
                    skip=page.skip,
                    next_skip=next_skip,
                    expected_count=expected_count,
                    percent_complete=percent_complete,
                    retries_used=retries_used_ref[0],
                    elapsed_seconds=time.time() - started,
                ),
            )

    mismatch = expected_count is not None and items_fetched != expected_count
    if mismatch:
        mismatch_error = (
            f"Fetched {items_fetched} items for '{spec.name}' "
            f"but count preflight expected {expected_count}."
        )
        _emit_api_log(
            api_log_callback,
            {
                "level": "summary",
                "event": "count_mismatch_failed",
                "endpoint": spec.name,
                "expected_count": expected_count,
                "items_fetched": items_fetched,
            },
        )
        _fail_integrity(
            f"{mismatch_error} Post-fetch integrity requires expected count to match actual count. "
            "Action: exit endpoint."
        )

    if expected_count is None:
        count_validation_status = "skipped"
        count_validation_reason = "expected_count_unavailable"
        id_validation_status = "skipped"
        id_validation_checked_items = 0
        id_validation_unique_ids = 0
        id_validation_reason = "expected_count_unavailable"
        _emit_api_log(
            api_log_callback,
            {
                "level": "summary",
                "event": "id_validation_skipped",
                "endpoint": spec.name,
                "reason": "expected_count_unavailable",
                "checked_items": 0,
                "unique_ids": 0,
            },
        )
    else:
        count_validation_status = "passed"
        count_validation_reason = None
        if invalid_id_count > 0 or duplicate_ids:
            detail_parts: list[str] = []
            if invalid_id_count > 0:
                first_issue = first_invalid_id_detail or "invalid field 'id'"
                detail_parts.append(
                    f"invalid id values={invalid_id_count} (first issue: {first_issue})"
                )
            if duplicate_ids:
                duplicate_preview = [repr(value) for value in duplicate_ids[:5]]
                detail_parts.append(
                    f"duplicate ids={len(duplicate_ids)} (sample: {', '.join(duplicate_preview)})"
                )
            validation_error = (
                f"Post-fetch ID validation failed for '{spec.name}': "
                + "; ".join(detail_parts)
                + ". Duplicate IDs indicate unstable pagination. Action: exit endpoint."
            )
            _emit_api_log(
                api_log_callback,
                {
                    "level": "summary",
                    "event": "id_validation_failed",
                    "endpoint": spec.name,
                    "invalid_id_count": invalid_id_count,
                    "duplicate_id_count": len(duplicate_ids),
                    "duplicate_ids_sample": [repr(value) for value in duplicate_ids[:5]],
                },
            )
            _fail_integrity(validation_error)

        id_validation_status = "passed"
        id_validation_checked_items = tracked_id_items
        id_validation_unique_ids = len(seen_ids)
        id_validation_reason = None
        _emit_api_log(
            api_log_callback,
            {
                "level": "summary",
                "event": "id_validation_passed",
                "endpoint": spec.name,
                "checked_items": id_validation_checked_items,
                "unique_ids": id_validation_unique_ids,
            },
        )

    duration = time.time() - started
    _write_checkpoint(
        checkpoint_path,
        spec.name,
        next_skip,
        items_fetched,
        delta_floor=effective_delta_floor,
        completed=True,
        restart_from_zero=False,
        window_start_line=window_start_line,
        output_file=output_path,
    )
    _emit_api_log(
        api_log_callback,
        {
            "level": "debug",
            "event": "checkpoint_written",
            "endpoint": spec.name,
            "checkpoint_file": str(checkpoint_path),
            "next_skip": next_skip,
            "fetched_count": items_fetched,
            "completed": True,
            "delta_floor": effective_delta_floor,
            "restart_from_zero": False,
            "window_start_line": window_start_line,
        },
    )
    _emit_progress(
        progress_callback,
        FetchProgressEvent(
            kind="endpoint_finish",
            endpoint=spec.name,
            pages_fetched=pages_fetched,
            items_fetched=items_fetched,
            expected_count=expected_count,
            retries_used=retries_used_ref[0],
            warnings_count=len(warnings),
            elapsed_seconds=duration,
        ),
    )
    return FetchRunResult(
        endpoint=spec.name,
        pages_fetched=pages_fetched,
        items_fetched=items_fetched,
        expected_count=expected_count,
        retries_used=retries_used_ref[0],
        start_skip=start_skip,
        next_skip=next_skip,
        duration_seconds=duration,
        output_file=output_path,
        checkpoint_file=checkpoint_path,
        warnings=warnings,
        already_completed=False,
        effective_delta_floor=effective_delta_floor,
        did_catch_up=resume,
        count_validation_status=count_validation_status,
        count_validation_reason=count_validation_reason,
        id_validation_status=id_validation_status,
        id_validation_checked_items=id_validation_checked_items,
        id_validation_unique_ids=id_validation_unique_ids,
        id_validation_reason=id_validation_reason,
    )
