from __future__ import annotations

import argparse
import calendar
import json
import re
import sys
import time
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, TextIO
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .auth import AuthError, init_auth_context
from .config import (
    ConfigError,
    load_fetcher_settings,
    resolve_fetch_params_path,
    resolve_private_config_path,
)
from .delta import apply_data_sort, build_delta_endpoint_spec, strip_modified_at_filters
from .fetcher import FetchError, run_endpoint
from .models import EndpointSpec, FetchProgressEvent, FetchRunResult

_DELTA_STATE_VERSION = 1
_DEFAULT_DELTA_STATE_CONFIG_PATH = Path("delta_fetcher.yml")
_DEFAULT_FETCHER_CONFIG_PATH = Path("config/fetcher.yml")
_DEFAULT_DELTA_LOG_PATH = Path("data/logs/delta.log")
_DEFAULT_FETCH_LOG_PATH = Path("fetcher.log")
_DEFAULT_DELTA_OVERLAP_MINUTES = 60
_DEFAULT_DELTA_OVERLAP_DAYS = 0
_MAX_DELTA_OVERLAP_DAYS = 1000
_MIN_MONTHS_BACK = 1
_MAX_MONTHS_BACK = 120
_MIN_DAYS_BACK = 1
_MAX_DAYS_BACK = 3650
_RUN_INTERRUPTED_MESSAGE = "interrupted by user (Ctrl+C)."
_SAFE_NAME_PATTERN = re.compile(r"[^A-Za-z0-9_.-]+")
_LOG_LEVEL_RANKS: dict[str, int] = {
    "off": 0,
    "summary": 1,
    "http": 2,
    "debug": 3,
}
_SENSITIVE_QUERY_KEYS = {"token", "password", "api_key", "authorization"}
LogLevel = Literal["off", "summary", "http", "debug"]
LogFormat = Literal["text", "jsonl"]
LogEvent = dict[str, Any]
LogCallback = Callable[[LogEvent], None]


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _utc_iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _delta_run_id(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H%M%SZ")


def _window_run_id(value: datetime, unit: str, amount: int) -> str:
    return f"{_delta_run_id(value)}-{unit}{amount}"


def _parse_utc_iso(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _normalize_delta_overlap_minutes(value: Any) -> int:
    if isinstance(value, bool):
        return _DEFAULT_DELTA_OVERLAP_MINUTES
    if isinstance(value, int) and value >= 0:
        return value
    return _DEFAULT_DELTA_OVERLAP_MINUTES


def _normalize_delta_overlap_days(value: Any) -> int:
    if isinstance(value, bool):
        return _DEFAULT_DELTA_OVERLAP_DAYS
    if isinstance(value, int) and 0 <= value <= _MAX_DELTA_OVERLAP_DAYS:
        return value
    return _DEFAULT_DELTA_OVERLAP_DAYS


def _resolve_delta_overlaps(delta_state: dict[str, Any]) -> tuple[int, int]:
    overlap_minutes = _normalize_delta_overlap_minutes(delta_state.get("overlap_minutes"))
    overlap_days = _normalize_delta_overlap_days(delta_state.get("overlap_days"))
    delta_state["overlap_minutes"] = overlap_minutes
    delta_state["overlap_days"] = overlap_days
    return overlap_minutes, overlap_days


def _load_delta_state(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {
            "version": _DELTA_STATE_VERSION,
            "updated_at": None,
            "overlap_minutes": _DEFAULT_DELTA_OVERLAP_MINUTES,
            "overlap_days": _DEFAULT_DELTA_OVERLAP_DAYS,
            "endpoints": {},
        }
    try:
        import yaml  # type: ignore
    except Exception as exc:  # pragma: no cover - mirrored in config tests
        raise ConfigError("Delta mode requires PyYAML to read/write delta state.") from exc

    with path.open("r", encoding="utf-8") as fh:
        payload = yaml.safe_load(fh)
    if payload is None:
        payload = {}
    if not isinstance(payload, dict):
        raise ConfigError(f"Delta state root must be an object: {path}")

    endpoints = payload.get("endpoints", {})
    if not isinstance(endpoints, dict):
        raise ConfigError(f"Delta state 'endpoints' must be an object: {path}")

    return {
        "version": _DELTA_STATE_VERSION,
        "updated_at": payload.get("updated_at"),
        "overlap_minutes": _normalize_delta_overlap_minutes(payload.get("overlap_minutes")),
        "overlap_days": _normalize_delta_overlap_days(payload.get("overlap_days")),
        "endpoints": endpoints,
    }


def _write_delta_state(path: Path, state: dict[str, Any]) -> None:
    try:
        import yaml  # type: ignore
    except Exception as exc:  # pragma: no cover - mirrored in config tests
        raise ConfigError("Delta mode requires PyYAML to read/write delta state.") from exc

    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.parent / f".{path.name}.tmp"
    temp_path.write_text(yaml.safe_dump(state, sort_keys=False), encoding="utf-8")
    temp_path.replace(path)


def _append_delta_log(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, separators=(",", ":")) + "\n")


def _redact_url_query(url: str) -> str:
    split_url = urlsplit(url)
    if not split_url.query:
        return url

    params = parse_qsl(split_url.query, keep_blank_values=True)
    changed = False
    redacted: list[tuple[str, str]] = []
    for key, value in params:
        if key.lower() in _SENSITIVE_QUERY_KEYS:
            redacted.append((key, "***"))
            changed = True
        else:
            redacted.append((key, value))
    if not changed:
        return url

    return urlunsplit(
        (
            split_url.scheme,
            split_url.netloc,
            split_url.path,
            urlencode(redacted, doseq=True),
            split_url.fragment,
        )
    )


def _render_log_line(record: LogEvent, *, log_format: LogFormat) -> str:
    if log_format == "jsonl":
        return json.dumps(record, separators=(",", ":"))

    pieces = [
        str(record.get("timestamp", "")),
        str(record.get("level", "summary")).upper(),
        str(record.get("event", "event")),
    ]
    for key in sorted(key for key in record if key not in {"timestamp", "level", "event"}):
        pieces.append(f"{key}={json.dumps(record[key], separators=(',', ':'), ensure_ascii=True)}")
    return " ".join(pieces)


def _build_log_callback(
    log_file: TextIO,
    *,
    log_level: LogLevel,
    log_format: LogFormat,
) -> LogCallback:
    selected_rank = _LOG_LEVEL_RANKS[log_level]

    def _log(event: LogEvent) -> None:
        event_level = str(event.get("level", "summary")).lower()
        event_rank = _LOG_LEVEL_RANKS.get(event_level, _LOG_LEVEL_RANKS["debug"])
        if event_rank > selected_rank:
            return

        record: LogEvent = {"timestamp": _utc_iso(_utc_now()), **event}
        url = record.get("url")
        if isinstance(url, str):
            record["url"] = _redact_url_query(url)

        log_file.write(_render_log_line(record, log_format=log_format) + "\n")
        log_file.flush()

    return _log


def _safe_checkpoint_name(endpoint_name: str) -> str:
    return _SAFE_NAME_PATTERN.sub("_", endpoint_name)


def _read_checkpoint_completed_state(path: Path) -> bool | None:
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if "completed" not in payload:
        return None
    completed = payload.get("completed")
    return completed if isinstance(completed, bool) else None


def _infer_resume_completed_hint(delta_state: dict[str, Any], endpoint_name: str) -> bool | None:
    endpoints = delta_state.get("endpoints", {})
    if not isinstance(endpoints, dict):
        return None
    endpoint_state = endpoints.get(endpoint_name, {})
    if not isinstance(endpoint_state, dict):
        return None

    status = endpoint_state.get("last_attempted_status")
    error = endpoint_state.get("last_attempted_error")
    error_is_empty = error is None or (isinstance(error, str) and not error.strip())
    if status == "OK" and error_is_empty:
        return True
    return None


def _derive_delta_floor(
    delta_state: dict[str, Any], endpoint_name: str, overlap_minutes: int, overlap_days: int
) -> str | None:
    endpoints = delta_state.get("endpoints", {})
    if not isinstance(endpoints, dict):
        return None
    endpoint_state = endpoints.get(endpoint_name, {})
    if not isinstance(endpoint_state, dict):
        return None
    successful_start = _parse_utc_iso(endpoint_state.get("last_successful_fetch_start"))
    if successful_start is None:
        return None
    floor = successful_start - timedelta(minutes=overlap_minutes, days=overlap_days)
    return _utc_iso(floor)


def _subtract_calendar_months(value: datetime, months: int) -> datetime:
    total_month_index = (value.year * 12 + (value.month - 1)) - months
    year = total_month_index // 12
    month = (total_month_index % 12) + 1
    last_day = calendar.monthrange(year, month)[1]
    day = min(value.day, last_day)
    return value.replace(year=year, month=month, day=day)


def _resolve_fetch_window(args: argparse.Namespace) -> tuple[str | None, int | None]:
    if args.days is not None and args.months is not None:
        raise ConfigError("Use either --days or --months, not both.")
    if args.days is not None:
        return "days", args.days
    if args.months is not None:
        return "months", args.months
    return None, None


def _resolve_runtime_fetch_params(args: argparse.Namespace) -> Path | None:
    if args.no_params and args.params is not None:
        raise ConfigError("Use either --params or --no-params, not both.")
    if args.no_params:
        return None
    return resolve_fetch_params_path(args.params)


def _window_modified_since(value: datetime, unit: str, amount: int) -> str:
    if unit == "days":
        return _utc_iso(value - timedelta(days=amount))
    if unit == "months":
        return _utc_iso(_subtract_calendar_months(value, amount))
    raise ValueError(f"Unsupported fetch window unit: {unit}")


def _apply_modified_since_filter(spec: EndpointSpec, modified_since: str) -> EndpointSpec:
    query_params = strip_modified_at_filters(spec.query_params)
    query_params["_modified_at=ge"] = modified_since

    next_count_spec = None
    if spec.count_spec is not None:
        count_query_params = strip_modified_at_filters(spec.count_spec.query_params)
        count_query_params["_modified_at=ge"] = modified_since
        next_count_spec = replace(spec.count_spec, query_params=count_query_params)

    return replace(spec, query_params=query_params, count_spec=next_count_spec)


def _parse_months_back(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--months must be an integer.") from exc
    if parsed < _MIN_MONTHS_BACK or parsed > _MAX_MONTHS_BACK:
        raise argparse.ArgumentTypeError(
            f"--months must be between {_MIN_MONTHS_BACK} and {_MAX_MONTHS_BACK}."
        )
    return parsed


def _parse_days_back(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--days must be an integer.") from exc
    if parsed < _MIN_DAYS_BACK or parsed > _MAX_DAYS_BACK:
        raise argparse.ArgumentTypeError(
            f"--days must be between {_MIN_DAYS_BACK} and {_MAX_DAYS_BACK}."
        )
    return parsed


def _classify_delta_status(result: FetchRunResult | None, error: str | None) -> str:
    if error is not None:
        return "FAILED"
    if result is None:
        return "FAILED"
    if result.already_completed:
        return "OK"
    if result.warnings:
        return "PARTIAL"
    return "OK"


def _update_delta_state_for_endpoint(
    delta_state: dict[str, Any],
    *,
    endpoint_name: str,
    status: str,
    attempt_start: str,
    attempt_end: str,
    error: str | None,
) -> None:
    endpoints = delta_state.setdefault("endpoints", {})
    if not isinstance(endpoints, dict):
        endpoints = {}
        delta_state["endpoints"] = endpoints

    existing = endpoints.get(endpoint_name, {})
    if not isinstance(existing, dict):
        existing = {}

    existing["last_attempted_fetch_start"] = attempt_start
    existing["last_attempted_fetch_end"] = attempt_end
    existing["last_attempted_status"] = status
    existing["last_attempted_error"] = error

    if status in {"OK", "PARTIAL"}:
        existing["last_successful_fetch_start"] = attempt_start
        existing["last_successful_fetch_end"] = attempt_end

    endpoints[endpoint_name] = existing
    delta_state["version"] = _DELTA_STATE_VERSION
    delta_state["updated_at"] = attempt_end


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="centric-mdm fetch")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run fetch jobs for one or more endpoints")
    run_parser.add_argument(
        "--config",
        default=str(_DEFAULT_FETCHER_CONFIG_PATH),
        help=f"Path to JSON/YAML fetcher config. Defaults to {_DEFAULT_FETCHER_CONFIG_PATH}.",
    )
    run_parser.add_argument(
        "--params",
        default=None,
        help=(
            "Optional private fetch params YAML. Defaults to "
            "CENTRIC_CONFIG_DIR/fetch-params.yml or .local/fetch-params.yml when present."
        ),
    )
    run_parser.add_argument(
        "--no-params",
        action="store_true",
        help="Do not load auto-discovered private fetch params.",
    )
    run_parser.add_argument(
        "--endpoint",
        action="append",
        default=[],
        help="Endpoint name to run (repeatable). Defaults to all endpoints.",
    )
    run_parser.add_argument(
        "--output-dir",
        default=None,
        help="Override output_dir from config.",
    )
    run_parser.add_argument(
        "--checkpoint-dir",
        default=None,
        help="Override checkpoint_dir from config.",
    )
    run_parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from endpoint checkpoint if present.",
    )
    run_parser.add_argument(
        "--delta",
        action="store_true",
        help="Run in delta mode using per-endpoint _modified_at floor and delta state tracking.",
    )
    run_parser.add_argument(
        "--delta-state-file",
        default=None,
        help=(
            "Delta state YAML path. Defaults to CENTRIC_CONFIG_DIR/delta_fetcher.yml "
            "or .local/delta_fetcher.yml."
        ),
    )
    run_parser.add_argument(
        "--delta-dry-run",
        action="store_true",
        help="Compute and print delta floors/injected filters without fetching data.",
    )
    run_parser.add_argument(
        "--caffeinate",
        action="store_true",
        help="macOS only: prevent idle sleep while fetch is running.",
    )
    run_parser.add_argument(
        "--days",
        type=_parse_days_back,
        default=None,
        metavar=f"{_MIN_DAYS_BACK}-{_MAX_DAYS_BACK}",
        help=(
            f"Non-delta mode only: fetch records modified in the last N days "
            f"({_MIN_DAYS_BACK}-{_MAX_DAYS_BACK}). Cannot be combined with --months."
        ),
    )
    run_parser.add_argument(
        "--months",
        type=_parse_months_back,
        default=None,
        metavar=f"{_MIN_MONTHS_BACK}-{_MAX_MONTHS_BACK}",
        help=(
            f"Non-delta mode only: fetch records modified in the last N calendar months "
            f"({_MIN_MONTHS_BACK}-{_MAX_MONTHS_BACK}). Cannot be combined with --days."
        ),
    )
    run_parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress live progress and the human final summary.",
    )
    run_parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSONL endpoint result records instead of the human summary.",
    )
    run_parser.add_argument(
        "--log-level",
        choices=["off", "summary", "http", "debug"],
        default="off",
        help="Logging level for run logs (off, summary, http, debug).",
    )
    run_parser.add_argument(
        "--log-format",
        choices=["text", "jsonl"],
        default="text",
        help="Run log output format.",
    )
    run_parser.add_argument(
        "--log-file",
        default=None,
        help=f"Run log file path (default: {_DEFAULT_FETCH_LOG_PATH} when --log-level is not off).",
    )

    run_parser.add_argument(
        "--env-file",
        default=None,
        help=(
            "Optional env file path for CENTRIC_BASE_URL, CENTRIC_USERNAME, "
            "and CENTRIC_PASSWORD."
        ),
    )
    run_parser.add_argument("--timeout", type=float, default=None)

    return parser


def _select_endpoints(all_specs: list[EndpointSpec], names: list[str]) -> list[EndpointSpec]:
    if not names:
        return all_specs
    wanted = set(names)
    selected = [spec for spec in all_specs if spec.name in wanted]
    missing = sorted(wanted - {spec.name for spec in selected})
    if missing:
        raise ConfigError(f"Unknown endpoint names: {', '.join(missing)}")
    return selected


def _format_seconds(value: float | None) -> str:
    seconds = value if value is not None else 0.0
    return f"{seconds:.2f}s"


def _format_fetch_duration(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value < 1:
        return f"{value * 1000:.0f}ms"
    if value < 60:
        return f"{value:.1f}s"
    minutes, seconds = divmod(int(round(value)), 60)
    if minutes < 60:
        return f"{minutes}m {seconds:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m"


def _write_progress_line(event: FetchProgressEvent) -> None:
    if event.kind == "endpoint_start":
        expected = event.expected_count if event.expected_count is not None else "unknown"
        print(
            f"[{event.endpoint}] start: skip={event.start_skip} limit={event.limit} "
            f"expected={expected} retries={event.retries_used} "
            f"elapsed={_format_seconds(event.elapsed_seconds)}",
            file=sys.stderr,
        )
        return

    if event.kind == "page_fetched":
        page_label = str(event.page_index)
        if event.expected_pages is not None:
            page_label = f"{page_label}/{event.expected_pages}"
        line = (
            f"[{event.endpoint}] page {page_label}: page_items={event.page_items} "
            f"total_items={event.items_fetched} skip={event.skip} next_skip={event.next_skip} "
            f"elapsed={_format_seconds(event.elapsed_seconds)}"
        )
        if event.percent_complete is not None:
            line += f" progress={event.percent_complete:.1f}%"
        if event.rolling_avg_seconds is not None:
            line += f" avg_page={_format_fetch_duration(event.rolling_avg_seconds)}"
        if event.estimated_remaining_seconds is not None:
            line += f" eta={_format_fetch_duration(event.estimated_remaining_seconds)}"
        print(line, file=sys.stderr)
        return

    if event.kind == "warning":
        print(f"[{event.endpoint}] warning: {event.message}", file=sys.stderr)
        return

    if event.kind == "endpoint_finish":
        print(
            f"[{event.endpoint}] finish: pages={event.pages_fetched} items={event.items_fetched} "
            f"retries={event.retries_used} warnings={event.warnings_count} "
            f"elapsed={_format_seconds(event.elapsed_seconds)}",
            file=sys.stderr,
        )


def _fetch_result_record(result: FetchRunResult) -> dict[str, Any]:
    return {
        "endpoint": result.endpoint,
        "status": "ok",
        "pages_fetched": result.pages_fetched,
        "items_fetched": result.items_fetched,
        "expected_count": result.expected_count,
        "count_validation": {
            "status": result.count_validation_status,
            "reason": result.count_validation_reason,
        },
        "retries_used": result.retries_used,
        "start_skip": result.start_skip,
        "next_skip": result.next_skip,
        "duration_seconds": round(result.duration_seconds, 3),
        "output_file": str(result.output_file),
        "checkpoint_file": str(result.checkpoint_file),
        "id_validation": {
            "status": result.id_validation_status,
            "checked_items": result.id_validation_checked_items,
            "unique_ids": result.id_validation_unique_ids,
            "reason": result.id_validation_reason,
        },
        "warnings": result.warnings,
    }


def _fetch_failure_record(endpoint: str, message: str) -> dict[str, Any]:
    return {
        "endpoint": endpoint,
        "status": "failed",
        "error": message,
    }


def _print_json_run_records(
    results: list[FetchRunResult],
    failures: list[tuple[str, str]],
) -> None:
    for result in results:
        print(json.dumps(_fetch_result_record(result)))
    for endpoint, message in failures:
        print(json.dumps(_fetch_failure_record(endpoint, message)))


def _plural(value: int, singular: str, plural: str | None = None) -> str:
    word = singular if value == 1 else (plural or f"{singular}s")
    return f"{value} {word}"


def _human_fetch_mode_label(
    args: argparse.Namespace,
    non_delta_window_unit: str | None,
) -> str:
    if args.delta_dry_run:
        return "delta dry-run"
    if args.delta:
        return "delta"
    if non_delta_window_unit == "days":
        return f"days window ({_plural(args.days, 'day')})"
    if non_delta_window_unit == "months":
        return f"months window ({_plural(args.months, 'month')})"
    return "standard"


def _common_output_dir(results: list[FetchRunResult]) -> Path | None:
    if not results:
        return None
    output_dirs = {result.output_file.parent for result in results}
    if len(output_dirs) == 1:
        return next(iter(output_dirs))
    return None


def _display_output_file(result: FetchRunResult, output_dir: Path | None) -> str:
    if output_dir is not None:
        try:
            return str(result.output_file.relative_to(output_dir))
        except ValueError:
            pass
    return str(result.output_file)


def _validation_status_counts(results: list[FetchRunResult], field_name: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for result in results:
        status = str(getattr(result, field_name))
        counts[status] = counts.get(status, 0) + 1
    return counts


def _format_status_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "none"
    ordered_keys = ["passed", "skipped", "failed", "not_run"]
    parts = [
        f"{counts[key]} {key.replace('_', ' ')}"
        for key in ordered_keys
        if counts.get(key, 0) > 0
    ]
    parts.extend(
        f"{count} {key.replace('_', ' ')}"
        for key, count in sorted(counts.items())
        if key not in ordered_keys and count > 0
    )
    return ", ".join(parts)


def _print_human_run_summary(
    *,
    args: argparse.Namespace,
    selected_count: int,
    results: list[FetchRunResult],
    failures: list[tuple[str, str]],
    duration_seconds: float,
    run_id: str | None,
    run_output_dir: Path | None,
    non_delta_window_unit: str | None,
) -> None:
    output_dir = run_output_dir or _common_output_dir(results)
    total_items = sum(result.items_fetched for result in results)
    total_pages = sum(result.pages_fetched for result in results)
    total_retries = sum(result.retries_used for result in results)
    total_warnings = sum(len(result.warnings) for result in results)
    title = "Fetch Complete" if not failures else "Fetch Finished With Failures"

    print(title)
    print()
    print(f"Mode: {_human_fetch_mode_label(args, non_delta_window_unit)}")
    if run_id is not None:
        print(f"Run:  {run_id}")
    if output_dir is not None:
        print(f"Raw:  {output_dir}")
    print()
    print("Summary")
    print(f"Endpoints: {len(results)} ok, {len(failures)} failed, {selected_count} total")
    print(f"Records:   {total_items} fetched")
    print(f"Pages:     {total_pages} fetched")
    print(f"Time:      {_format_fetch_duration(duration_seconds)}")
    print(f"Retries:   {total_retries}")
    if total_warnings:
        print(f"Warnings:  {total_warnings}")

    if results:
        endpoint_width = max(len("Endpoint"), *(len(result.endpoint) for result in results))
        file_width = max(
            len("File"),
            *(
                len(_display_output_file(result, output_dir))
                for result in results
            ),
        )
        header = (
            f"{'Endpoint':<{endpoint_width}}  {'Records':>7}  {'Expected':>8}  "
            f"{'Pages':>5}  {'Time':>7}  {'File':<{file_width}}"
        )
        print()
        print(header)
        print("-" * len(header))
        for result in results:
            expected = (
                str(result.expected_count)
                if result.expected_count is not None
                else "unknown"
            )
            print(
                f"{result.endpoint:<{endpoint_width}}  "
                f"{result.items_fetched:>7}  "
                f"{expected:>8}  "
                f"{result.pages_fetched:>5}  "
                f"{_format_fetch_duration(result.duration_seconds):>7}  "
                f"{_display_output_file(result, output_dir):<{file_width}}"
            )

    print()
    print("Validation")
    print(
        "Count checks: "
        f"{_format_status_counts(_validation_status_counts(results, 'count_validation_status'))}"
    )
    print(
        "ID checks:    "
        f"{_format_status_counts(_validation_status_counts(results, 'id_validation_status'))}"
    )

    warning_rows = [
        (result.endpoint, warning)
        for result in results
        for warning in result.warnings
    ]
    if warning_rows:
        print()
        print("Warnings")
        for endpoint, warning in warning_rows:
            print(f"- {endpoint}: {warning}")

    if failures:
        print()
        print("Failures")
        for endpoint, message in failures:
            print(f"- {endpoint}: {message}")


def _build_delta_run_summary_record(
    *,
    run_started_at: datetime,
    run_finished_at: datetime,
    selected_specs: list[EndpointSpec],
    results: list[FetchRunResult],
    failures: list[tuple[str, str]],
    endpoint_records: list[dict[str, Any]],
) -> dict[str, Any]:
    status_counts: dict[str, int] = {"OK": 0, "PARTIAL": 0, "FAILED": 0}
    for record in endpoint_records:
        status = record.get("status")
        if isinstance(status, str):
            status_counts[status] = status_counts.get(status, 0) + 1

    run_status = "OK"
    if status_counts.get("FAILED", 0) > 0:
        run_status = (
            "FAILED" if status_counts.get("FAILED", 0) == len(endpoint_records) else "PARTIAL"
        )
    elif status_counts.get("PARTIAL", 0) > 0:
        run_status = "PARTIAL"

    endpoint_results = []
    for record in endpoint_records:
        count_validation = record.get("count_validation")
        if not isinstance(count_validation, dict):
            count_validation = {"status": None, "reason": None}
        endpoint_results.append(
            {
                "endpoint": record.get("endpoint"),
                "status": record.get("status"),
                "already_completed": bool(record.get("already_completed")),
                "did_catch_up": bool(record.get("did_catch_up")),
                "delta_floor": record.get("delta_floor"),
                "attempt_start": record.get("attempt_start"),
                "attempt_end": record.get("attempt_end"),
                "duration_seconds": record.get("duration_seconds"),
                "items_fetched": record.get("items_fetched"),
                "pages_fetched": record.get("pages_fetched"),
                "retries_used": record.get("retries_used"),
                "count_validation": count_validation,
                "id_validation_status": record.get("id_validation_status"),
                "id_validation_checked_items": record.get("id_validation_checked_items"),
                "id_validation_unique_ids": record.get("id_validation_unique_ids"),
                "id_validation_reason": record.get("id_validation_reason"),
                "error": record.get("error"),
            }
        )

    return {
        "run_at": _utc_iso(run_finished_at),
        "mode": "delta",
        "record_type": "run_summary",
        "status": run_status,
        "run_started_at": _utc_iso(run_started_at),
        "run_finished_at": _utc_iso(run_finished_at),
        "duration_seconds": round((run_finished_at - run_started_at).total_seconds(), 3),
        "endpoints_total": len(selected_specs),
        "endpoints_succeeded": len(results),
        "endpoints_failed": len(failures),
        "endpoints_by_status": status_counts,
        "endpoints_already_completed": sum(
            1 for record in endpoint_records if record.get("already_completed")
        ),
        "endpoints_caught_up": sum(1 for record in endpoint_records if record.get("did_catch_up")),
        "selected_endpoints": [spec.name for spec in selected_specs],
        "total_items": sum(result.items_fetched for result in results),
        "total_pages": sum(result.pages_fetched for result in results),
        "total_retries": sum(result.retries_used for result in results),
        "failures": [{"endpoint": endpoint, "error": message} for endpoint, message in failures],
        "endpoint_results": endpoint_results,
    }


def _build_endpoint_manifest_record(
    *,
    result: FetchRunResult,
    status: str,
    mode: str,
    run_output_dir: Path,
    delta_floor: str | None = None,
    modified_since: str | None = None,
    attempt_start: str | None = None,
    attempt_end: str | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    output_file = result.output_file
    try:
        file_name = str(output_file.relative_to(run_output_dir))
    except ValueError:
        file_name = output_file.name
    return {
        "endpoint": result.endpoint,
        "file": file_name,
        "mode": mode,
        "status": status,
        "is_delta": mode == "delta",
        "delta_floor": result.effective_delta_floor or delta_floor,
        "modified_since": modified_since,
        "attempt_start": attempt_start,
        "attempt_end": attempt_end,
        "items_fetched": result.items_fetched,
        "pages_fetched": result.pages_fetched,
        "expected_count": result.expected_count,
        "retries_used": result.retries_used,
        "count_validation": {
            "status": result.count_validation_status,
            "reason": result.count_validation_reason,
        },
        "id_validation": {
            "status": result.id_validation_status,
            "checked_items": result.id_validation_checked_items,
            "unique_ids": result.id_validation_unique_ids,
            "reason": result.id_validation_reason,
        },
        "already_completed": result.already_completed,
        "did_catch_up": result.did_catch_up,
        "warnings": result.warnings,
        "error": error,
    }


def _write_run_manifest(
    *,
    output_dir: Path,
    run_id: str,
    mode: str,
    run_started_at: datetime,
    run_finished_at: datetime,
    selected_specs: list[EndpointSpec],
    results: list[FetchRunResult],
    failures: list[tuple[str, str]],
    endpoint_records: list[dict[str, Any]],
    modified_since: str | None,
) -> Path:
    endpoint_record_by_name = {
        str(record.get("endpoint")): record
        for record in endpoint_records
        if isinstance(record.get("endpoint"), str)
    }
    endpoints: dict[str, Any] = {}
    for result in results:
        record = endpoint_record_by_name.get(result.endpoint, {})
        endpoints[result.endpoint] = _build_endpoint_manifest_record(
            result=result,
            status=str(record.get("status", "OK")),
            mode=mode,
            run_output_dir=output_dir,
            delta_floor=record.get("delta_floor") if isinstance(record, dict) else None,
            modified_since=modified_since,
            attempt_start=record.get("attempt_start") if isinstance(record, dict) else None,
            attempt_end=record.get("attempt_end") if isinstance(record, dict) else None,
            error=record.get("error") if isinstance(record, dict) else None,
        )

    run_status = "OK"
    if failures:
        run_status = "FAILED" if len(failures) == len(selected_specs) else "PARTIAL"

    manifest = {
        "run_id": run_id,
        "mode": mode,
        "status": run_status,
        "started_at": _utc_iso(run_started_at),
        "finished_at": _utc_iso(run_finished_at),
        "duration_seconds": round((run_finished_at - run_started_at).total_seconds(), 3),
        "output_dir": str(output_dir),
        "selected_endpoints": [spec.name for spec in selected_specs],
        "endpoints_total": len(selected_specs),
        "endpoints_succeeded": len(results),
        "endpoints_failed": len(failures),
        "total_items": sum(result.items_fetched for result in results),
        "modified_since": modified_since,
        "failures": [{"endpoint": endpoint, "error": message} for endpoint, message in failures],
        "endpoints": endpoints,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "manifest.json"
    temp_path = output_dir / ".manifest.json.tmp"
    temp_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(manifest_path)
    return manifest_path


def _build_delta_endpoint_log_record(
    *,
    endpoint_name: str,
    status: str,
    delta_floor: str | None,
    attempt_start: str,
    attempt_end: str,
    duration_seconds: float,
    result: FetchRunResult | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    if result is not None:
        return {
            "run_at": attempt_end,
            "mode": "delta",
            "endpoint": endpoint_name,
            "status": status,
            "delta_floor": (
                result.effective_delta_floor
                if result.effective_delta_floor is not None
                else delta_floor
            ),
            "attempt_start": attempt_start,
            "attempt_end": attempt_end,
            "duration_seconds": duration_seconds,
            "expected_count": result.expected_count,
            "items_fetched": result.items_fetched,
            "pages_fetched": result.pages_fetched,
            "retries_used": result.retries_used,
            "count_validation": {
                "status": result.count_validation_status,
                "reason": result.count_validation_reason,
            },
            "warnings_count": len(result.warnings),
            "already_completed": result.already_completed,
            "did_catch_up": result.did_catch_up,
            "id_validation_status": result.id_validation_status,
            "id_validation_checked_items": result.id_validation_checked_items,
            "id_validation_unique_ids": result.id_validation_unique_ids,
            "id_validation_reason": result.id_validation_reason,
            "error": None,
            "output_file": str(result.output_file),
            "checkpoint_file": str(result.checkpoint_file),
        }

    return {
        "run_at": attempt_end,
        "mode": "delta",
        "endpoint": endpoint_name,
        "status": status,
        "delta_floor": delta_floor,
        "attempt_start": attempt_start,
        "attempt_end": attempt_end,
        "duration_seconds": duration_seconds,
        "expected_count": None,
        "items_fetched": None,
        "pages_fetched": None,
        "retries_used": None,
        "count_validation": None,
        "warnings_count": None,
        "id_validation_status": None,
        "id_validation_checked_items": None,
        "id_validation_unique_ids": None,
        "id_validation_reason": None,
        "error": error,
        "output_file": None,
        "checkpoint_file": None,
    }


def _prepare_runtime_spec(
    spec: EndpointSpec,
    *,
    delta_enabled: bool,
    delta_floor: str | None,
    non_delta_modified_since: str | None,
) -> EndpointSpec:
    if delta_enabled:
        return build_delta_endpoint_spec(spec, delta_floor, force_sort=True)

    runtime_spec = apply_data_sort(spec, sort_value="_modified_at", policy="if_missing")
    if non_delta_modified_since is not None:
        runtime_spec = _apply_modified_since_filter(runtime_spec, non_delta_modified_since)
    return runtime_spec


def _resolve_resume_completed_hint(
    *,
    resume: bool,
    delta_state: dict[str, Any] | None,
    checkpoint_dir: Path,
    endpoint_name: str,
) -> bool | None:
    if not resume or delta_state is None:
        return None

    checkpoint_path = checkpoint_dir / f"{_safe_checkpoint_name(endpoint_name)}.json"
    checkpoint_completed_state = _read_checkpoint_completed_state(checkpoint_path)
    if checkpoint_completed_state is not None:
        return None
    return _infer_resume_completed_hint(delta_state, endpoint_name)


def _build_run_kwargs(
    *,
    resume: bool,
    quiet: bool,
    log_callback: LogCallback | None,
    delta_enabled: bool,
    delta_floor: str | None,
    delta_state: dict[str, Any] | None,
    checkpoint_dir: Path,
    endpoint_name: str,
) -> dict[str, Any]:
    progress_callback = None if quiet else _write_progress_line
    run_kwargs: dict[str, Any] = {
        "resume": resume,
        "progress_callback": progress_callback,
    }

    if log_callback is not None:
        run_kwargs["api_log_callback"] = log_callback

    if delta_enabled:
        run_kwargs["append_output"] = True
        run_kwargs["output_file_suffix"] = ".delta"
        run_kwargs["delta_floor"] = delta_floor
        resume_completed_hint = _resolve_resume_completed_hint(
            resume=resume,
            delta_state=delta_state,
            checkpoint_dir=checkpoint_dir,
            endpoint_name=endpoint_name,
        )
        if resume_completed_hint is not None:
            run_kwargs["resume_completed_hint"] = resume_completed_hint

    return run_kwargs


def _record_delta_endpoint_attempt(
    *,
    delta_state: dict[str, Any],
    delta_state_file: Path,
    delta_log_file: Path,
    delta_endpoint_records: list[dict[str, Any]],
    endpoint_name: str,
    status: str,
    delta_floor: str | None,
    attempt_start: str,
    attempt_start_dt: datetime,
    update_delta_state: bool,
    result: FetchRunResult | None = None,
    error: str | None = None,
) -> None:
    attempt_end_dt = _utc_now()
    attempt_end = _utc_iso(attempt_end_dt)
    if update_delta_state:
        _update_delta_state_for_endpoint(
            delta_state,
            endpoint_name=endpoint_name,
            status=status,
            attempt_start=attempt_start,
            attempt_end=attempt_end,
            error=error,
        )
        _write_delta_state(delta_state_file, delta_state)

    endpoint_log_record = _build_delta_endpoint_log_record(
        endpoint_name=endpoint_name,
        status=status,
        delta_floor=delta_floor,
        attempt_start=attempt_start,
        attempt_end=attempt_end,
        duration_seconds=round((attempt_end_dt - attempt_start_dt).total_seconds(), 3),
        result=result,
        error=error,
    )
    _append_delta_log(delta_log_file, endpoint_log_record)
    delta_endpoint_records.append(endpoint_log_record)


def _record_endpoint_failure(
    *,
    endpoint_name: str,
    message: str,
    stderr_label: str,
    attempt_start: str,
    attempt_start_dt: datetime,
    delta_floor: str | None,
    failures: list[tuple[str, str]],
    log_callback: LogCallback | None,
    delta_state: dict[str, Any] | None,
    delta_state_file: Path,
    delta_log_file: Path,
    delta_endpoint_records: list[dict[str, Any]],
) -> None:
    failures.append((endpoint_name, message))
    print(f"[{endpoint_name}] {stderr_label}: {message}", file=sys.stderr)
    if log_callback is not None:
        log_callback(
            {
                "level": "summary",
                "event": "endpoint_failed",
                "endpoint": endpoint_name,
                "error": message,
                "duration_seconds": round((_utc_now() - attempt_start_dt).total_seconds(), 3),
            }
        )

    if delta_state is not None:
        status = _classify_delta_status(None, error=message)
        _record_delta_endpoint_attempt(
            delta_state=delta_state,
            delta_state_file=delta_state_file,
            delta_log_file=delta_log_file,
            delta_endpoint_records=delta_endpoint_records,
            endpoint_name=endpoint_name,
            status=status,
            delta_floor=delta_floor,
            attempt_start=attempt_start,
            attempt_start_dt=attempt_start_dt,
            update_delta_state=True,
            error=message,
        )


def _finalize_run(
    *,
    args: argparse.Namespace,
    run_started: float,
    run_started_dt: datetime,
    selected_specs: list[EndpointSpec],
    results: list[FetchRunResult],
    failures: list[tuple[str, str]],
    run_interrupted: bool,
    delta_state: dict[str, Any] | None,
    delta_log_file: Path,
    delta_endpoint_records: list[dict[str, Any]],
    run_output_dir: Path | None,
    run_id: str | None,
    non_delta_window_unit: str | None,
    non_delta_modified_since: str | None,
    log_callback: LogCallback | None,
) -> int:
    run_finished_dt = _utc_now()
    if args.delta and delta_state is not None and not args.delta_dry_run:
        _append_delta_log(
            delta_log_file,
            _build_delta_run_summary_record(
                run_started_at=run_started_dt,
                run_finished_at=run_finished_dt,
                selected_specs=selected_specs,
                results=results,
                failures=failures,
                endpoint_records=delta_endpoint_records,
            ),
        )

    if run_output_dir is not None and run_id is not None:
        _write_run_manifest(
            output_dir=run_output_dir,
            run_id=run_id,
            mode="delta" if args.delta else (non_delta_window_unit or "standard"),
            run_started_at=run_started_dt,
            run_finished_at=run_finished_dt,
            selected_specs=selected_specs,
            results=results,
            failures=failures,
            endpoint_records=delta_endpoint_records,
            modified_since=non_delta_modified_since,
        )

    run_duration_seconds = time.time() - run_started
    if args.json and not args.delta_dry_run:
        _print_json_run_records(results, failures)
    elif not args.quiet and not args.delta_dry_run:
        _print_human_run_summary(
            args=args,
            selected_count=len(selected_specs),
            results=results,
            failures=failures,
            duration_seconds=run_duration_seconds,
            run_id=run_id,
            run_output_dir=run_output_dir,
            non_delta_window_unit=non_delta_window_unit,
        )

    exit_code = 130 if run_interrupted else (1 if failures else 0)

    if log_callback is not None:
        log_callback(
            {
                "level": "summary",
                "event": "run_finish",
                "mode": (
                    "delta_dry_run"
                    if args.delta_dry_run
                    else ("delta" if args.delta else (non_delta_window_unit or "standard"))
                ),
                "duration_seconds": round(run_duration_seconds, 3),
                "endpoints_total": len(selected_specs),
                "endpoints_succeeded": len(results),
                "endpoints_failed": len(failures),
                "exit_code": exit_code,
            }
        )

    return exit_code


def _run(args: argparse.Namespace) -> int:
    run_started = time.time()
    run_started_dt = _utc_now()
    if args.delta_dry_run:
        args.delta = True
    non_delta_window_unit, non_delta_window_amount = _resolve_fetch_window(args)
    if non_delta_window_unit is not None and args.delta:
        raise ConfigError(
            f"--{non_delta_window_unit} is only supported in non-delta mode "
            "(without --delta/--delta-dry-run)."
        )

    fetcher_cfg, auth_settings, endpoint_specs = load_fetcher_settings(
        args.config,
        params_path=_resolve_runtime_fetch_params(args),
    )

    if args.output_dir:
        fetcher_cfg.output_dir = Path(args.output_dir)
    if args.checkpoint_dir:
        fetcher_cfg.checkpoint_dir = Path(args.checkpoint_dir)
    if args.delta and not args.delta_dry_run:
        run_id = _delta_run_id(run_started_dt)
        fetcher_cfg.output_dir = fetcher_cfg.output_dir / "runs" / run_id
    elif non_delta_window_unit is not None and non_delta_window_amount is not None:
        run_id = _window_run_id(run_started_dt, non_delta_window_unit, non_delta_window_amount)
        fetcher_cfg.output_dir = fetcher_cfg.output_dir / "runs" / run_id
    else:
        run_id = None
    run_output_dir = fetcher_cfg.output_dir if run_id is not None else None

    selected_specs = _select_endpoints(endpoint_specs, args.endpoint)

    failures: list[tuple[str, str]] = []
    results: list[FetchRunResult] = []
    run_interrupted = False
    delta_endpoint_records: list[dict[str, Any]] = []
    delta_state_file = resolve_private_config_path(
        _DEFAULT_DELTA_STATE_CONFIG_PATH,
        args.delta_state_file,
    )
    delta_log_file = _DEFAULT_DELTA_LOG_PATH
    delta_state = _load_delta_state(delta_state_file) if args.delta else None
    delta_overlap_minutes = _DEFAULT_DELTA_OVERLAP_MINUTES
    delta_overlap_days = _DEFAULT_DELTA_OVERLAP_DAYS
    if args.delta and delta_state is not None:
        delta_overlap_minutes, delta_overlap_days = _resolve_delta_overlaps(delta_state)
    non_delta_modified_since = (
        _window_modified_since(run_started_dt, non_delta_window_unit, non_delta_window_amount)
        if non_delta_window_unit is not None and non_delta_window_amount is not None
        else None
    )

    fetch_log_file: TextIO | None = None
    log_callback: LogCallback | None = None
    if args.log_level != "off":
        log_path = Path(args.log_file) if args.log_file else _DEFAULT_FETCH_LOG_PATH
        log_path.parent.mkdir(parents=True, exist_ok=True)
        fetch_log_file = log_path.open("a", encoding="utf-8")
        log_callback = _build_log_callback(
            fetch_log_file,
            log_level=args.log_level,
            log_format=args.log_format,
        )

    try:
        if log_callback is not None:
            log_callback(
                {
                    "level": "summary",
                    "event": "run_start",
                    "mode": (
                        "delta_dry_run"
                        if args.delta_dry_run
                        else ("delta" if args.delta else (non_delta_window_unit or "standard"))
                    ),
                    "selected_endpoints": [spec.name for spec in selected_specs],
                    "resume": args.resume,
                }
            )
        with init_auth_context(
            auth_settings,
            timeout=args.timeout,
            env_file=Path(args.env_file) if args.env_file else None,
        ) as auth_ctx:
            fetcher_cfg.base_url = auth_ctx.base_url
            fetcher_cfg.timeout = auth_ctx.timeout

            for spec in selected_specs:
                attempt_start_dt = _utc_now()
                attempt_start = _utc_iso(attempt_start_dt)
                delta_floor = (
                    _derive_delta_floor(
                        delta_state,
                        spec.name,
                        delta_overlap_minutes,
                        delta_overlap_days,
                    )
                    if args.delta and delta_state
                    else None
                )
                runtime_spec = _prepare_runtime_spec(
                    spec,
                    delta_enabled=args.delta,
                    delta_floor=delta_floor,
                    non_delta_modified_since=non_delta_modified_since,
                )
                if log_callback is not None:
                    log_callback(
                        {
                            "level": "debug",
                            "event": "endpoint_runtime_prepared",
                            "endpoint": spec.name,
                            "delta_floor": delta_floor,
                            "non_delta_modified_since": (
                                non_delta_modified_since if not args.delta else None
                            ),
                            "sort": runtime_spec.query_params.get("sort"),
                            "delta_overlap_days": delta_overlap_days if args.delta else None,
                            "delta_overlap_minutes": delta_overlap_minutes if args.delta else None,
                            "mode": (
                                "delta"
                                if args.delta
                                else (non_delta_window_unit or "standard")
                            ),
                        }
                    )

                if args.delta_dry_run:
                    data_modified = runtime_spec.query_params.get("_modified_at=ge")
                    count_modified = (
                        runtime_spec.count_spec.query_params.get("_modified_at=ge")
                        if runtime_spec.count_spec is not None
                        else None
                    )
                    if not args.quiet:
                        print(
                            f"[{spec.name}] delta dry-run: floor={delta_floor or 'none'} "
                            f"overlap_days={delta_overlap_days} "
                            f"overlap_minutes={delta_overlap_minutes} "
                            f"data_modified_at={data_modified or 'none'} "
                            f"count_modified_at={count_modified or 'none'}",
                            file=sys.stderr,
                        )
                    print(
                        json.dumps(
                            {
                                "endpoint": spec.name,
                                "status": "delta_dry_run",
                                "overlap_days": delta_overlap_days,
                                "overlap_minutes": delta_overlap_minutes,
                                "delta_floor": delta_floor,
                                "data_modified_at": data_modified,
                                "count_modified_at": count_modified,
                            }
                        )
                    )
                    if log_callback is not None:
                        log_callback(
                            {
                                "level": "summary",
                                "event": "endpoint_dry_run",
                                "endpoint": spec.name,
                                "delta_floor": delta_floor,
                                "data_modified_at": data_modified,
                                "count_modified_at": count_modified,
                                "overlap_days": delta_overlap_days,
                                "overlap_minutes": delta_overlap_minutes,
                            }
                        )
                    continue

                try:
                    run_kwargs = _build_run_kwargs(
                        resume=args.resume,
                        quiet=args.quiet,
                        log_callback=log_callback,
                        delta_enabled=args.delta,
                        delta_floor=delta_floor,
                        delta_state=delta_state if args.delta else None,
                        checkpoint_dir=fetcher_cfg.checkpoint_dir,
                        endpoint_name=spec.name,
                    )
                    if log_callback is not None:
                        log_callback(
                            {
                                "level": "summary",
                                "event": "endpoint_start",
                                "endpoint": spec.name,
                                "delta_floor": delta_floor,
                                "resume": args.resume,
                                "mode": (
                                    "delta"
                                    if args.delta
                                    else (non_delta_window_unit or "standard")
                                ),
                            }
                        )
                    result = run_endpoint(runtime_spec, auth_ctx, fetcher_cfg, **run_kwargs)
                    results.append(result)
                    if log_callback is not None:
                        log_callback(
                            {
                                "level": "summary",
                                "event": "endpoint_finish",
                                "endpoint": spec.name,
                                "pages_fetched": result.pages_fetched,
                                "items_fetched": result.items_fetched,
                                "retries_used": result.retries_used,
                                "duration_seconds": round(result.duration_seconds, 3),
                                "already_completed": result.already_completed,
                                "did_catch_up": result.did_catch_up,
                                "count_validation_status": result.count_validation_status,
                                "count_validation_reason": result.count_validation_reason,
                                "id_validation_status": result.id_validation_status,
                                "id_validation_checked_items": result.id_validation_checked_items,
                                "id_validation_unique_ids": result.id_validation_unique_ids,
                                "id_validation_reason": result.id_validation_reason,
                            }
                        )

                    if args.delta and delta_state is not None:
                        status = _classify_delta_status(result, error=None)
                        should_update_delta_state = not (args.resume and result.already_completed)
                        _record_delta_endpoint_attempt(
                            delta_state=delta_state,
                            delta_state_file=delta_state_file,
                            delta_log_file=delta_log_file,
                            delta_endpoint_records=delta_endpoint_records,
                            endpoint_name=spec.name,
                            status=status,
                            delta_floor=delta_floor,
                            attempt_start=attempt_start,
                            attempt_start_dt=attempt_start_dt,
                            update_delta_state=should_update_delta_state,
                            result=result,
                        )
                    elif non_delta_window_unit is not None:
                        attempt_end_dt = _utc_now()
                        delta_endpoint_records.append(
                            {
                                "endpoint": spec.name,
                                "status": "OK",
                                "attempt_start": attempt_start,
                                "attempt_end": _utc_iso(attempt_end_dt),
                                "duration_seconds": round(
                                    (attempt_end_dt - attempt_start_dt).total_seconds(),
                                    3,
                                ),
                                "items_fetched": result.items_fetched,
                                "pages_fetched": result.pages_fetched,
                                "retries_used": result.retries_used,
                                "count_validation": {
                                    "status": result.count_validation_status,
                                    "reason": result.count_validation_reason,
                                },
                                "id_validation_status": result.id_validation_status,
                                "id_validation_checked_items": result.id_validation_checked_items,
                                "id_validation_unique_ids": result.id_validation_unique_ids,
                                "id_validation_reason": result.id_validation_reason,
                                "error": None,
                                "output_file": str(result.output_file),
                                "checkpoint_file": str(result.checkpoint_file),
                            }
                        )
                except (FetchError, AuthError) as exc:
                    _record_endpoint_failure(
                        endpoint_name=spec.name,
                        message=str(exc),
                        stderr_label="error",
                        attempt_start=attempt_start,
                        attempt_start_dt=attempt_start_dt,
                        delta_floor=delta_floor,
                        failures=failures,
                        log_callback=log_callback,
                        delta_state=delta_state if args.delta else None,
                        delta_state_file=delta_state_file,
                        delta_log_file=delta_log_file,
                        delta_endpoint_records=delta_endpoint_records,
                    )
                    if not args.delta:
                        delta_endpoint_records.append(
                            {
                                "endpoint": spec.name,
                                "status": "FAILED",
                                "attempt_start": attempt_start,
                                "attempt_end": _utc_iso(_utc_now()),
                                "error": str(exc),
                            }
                        )
                except KeyboardInterrupt:
                    run_interrupted = True
                    _record_endpoint_failure(
                        endpoint_name=spec.name,
                        message=_RUN_INTERRUPTED_MESSAGE,
                        stderr_label="interrupted",
                        attempt_start=attempt_start,
                        attempt_start_dt=attempt_start_dt,
                        delta_floor=delta_floor,
                        failures=failures,
                        log_callback=log_callback,
                        delta_state=delta_state if args.delta else None,
                        delta_state_file=delta_state_file,
                        delta_log_file=delta_log_file,
                        delta_endpoint_records=delta_endpoint_records,
                    )
                    if not args.delta:
                        delta_endpoint_records.append(
                            {
                                "endpoint": spec.name,
                                "status": "FAILED",
                                "attempt_start": attempt_start,
                                "attempt_end": _utc_iso(_utc_now()),
                                "error": _RUN_INTERRUPTED_MESSAGE,
                            }
                        )
                    break

        return _finalize_run(
            args=args,
            run_started=run_started,
            run_started_dt=run_started_dt,
            selected_specs=selected_specs,
            results=results,
            failures=failures,
            run_interrupted=run_interrupted,
            delta_state=delta_state,
            delta_log_file=delta_log_file,
            delta_endpoint_records=delta_endpoint_records,
            run_output_dir=run_output_dir,
            run_id=run_id,
            non_delta_window_unit=non_delta_window_unit,
            non_delta_modified_since=non_delta_modified_since,
            log_callback=log_callback,
        )
    finally:
        if fetch_log_file is not None:
            fetch_log_file.close()


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "run":
            return _run(args)
    except KeyboardInterrupt:
        print(f"Run {_RUN_INTERRUPTED_MESSAGE}", file=sys.stderr)
        return 130
    except (ConfigError, AuthError, FetchError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
