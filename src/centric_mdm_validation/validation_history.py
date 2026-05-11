from __future__ import annotations

import calendar
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb


@dataclass(frozen=True)
class ValidationHistoryRun:
    run_id: str
    target: str
    product_change_count: int
    issue_change_count: int


@dataclass(frozen=True)
class ValidationIndexRow:
    product_id: str
    ready: bool | None
    status: str
    issue_hash: str
    issue_codes: tuple[str, ...]
    issue_severities: dict[str, str]
    display_name: str | None = None
    brand: str | None = None
    season: str | None = None
    season_year: int | None = None
    group_key: str | None = None
    score: float | None = None
    issue_count: int = 0
    failure_count: int = 0
    hard_warning_count: int = 0
    soft_warning_count: int = 0
    updated_source_at: datetime | None = None


_DURATION_PATTERN = re.compile(r"^(?P<count>[1-9][0-9]*)(?P<unit>[hdmy])$")
_ABSOLUTE_FORMATS = (
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%dT%H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
)


def record_validation_history(
    db_path: Path,
    *,
    target: str,
    run: Any,
    input_path: Path | None,
    latest_result_path: Path | None,
    scoped_product_ids: set[str] | None = None,
) -> ValidationHistoryRun:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    created_at = _utc_now()
    current_index = _build_validation_index(run)
    with duckdb.connect(str(db_path)) as conn:
        ensure_validation_history_tables(conn)
        run_id = _allocate_run_id(conn, target, created_at)
        previous_index = _load_current_index(conn, target)
        if scoped_product_ids is not None:
            scoped_product_ids = {str(product_id) for product_id in scoped_product_ids}
            current_index = {
                product_id: row
                for product_id, row in current_index.items()
                if product_id in scoped_product_ids
            }
            previous_for_diff = {
                product_id: row
                for product_id, row in previous_index.items()
                if product_id in scoped_product_ids
            }
            merged_index = {
                product_id: row
                for product_id, row in previous_index.items()
                if product_id not in scoped_product_ids
            }
            merged_index.update(current_index)
            total = len(merged_index)
            ready = sum(1 for row in merged_index.values() if row.ready is True)
            readiness_percent = round((ready / total) * 100, 2) if total else 0.0
        else:
            previous_for_diff = previous_index
            merged_index = current_index
            total, ready = _validation_counts(run)
            readiness_percent = _readiness_percent(run)
        product_events, issue_events = _diff_validation_indexes(
            run_id=run_id,
            target=target,
            changed_at=created_at,
            previous_index=previous_for_diff,
            current_index=current_index,
        )
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute(
                """
                INSERT INTO validation_runs (
                    run_id, target, created_at, input_path, input_sha256,
                    latest_result_path, latest_result_sha256, rule_set_version,
                    total_records, ready_records, readiness_percent
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    run_id,
                    target,
                    created_at,
                    str(input_path) if input_path is not None else None,
                    _file_sha256(input_path),
                    str(latest_result_path) if latest_result_path is not None else None,
                    _file_sha256(latest_result_path),
                    _result_value(run, "rule_set_version", default=None),
                    total,
                    ready,
                    readiness_percent,
                ],
            )
            if product_events:
                conn.executemany(
                    """
                    INSERT INTO validation_change_events (
                        run_id, target, changed_at, product_id, change_type,
                        previous_ready, current_ready, previous_status, current_status,
                        previous_issue_hash, current_issue_hash,
                        previous_issue_codes_json, current_issue_codes_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    product_events,
                )
            if issue_events:
                conn.executemany(
                    """
                    INSERT INTO validation_issue_change_events (
                        run_id, target, changed_at, product_id, issue_code, change_type, severity
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    issue_events,
                )
            if scoped_product_ids is None:
                conn.execute(
                    "DELETE FROM validation_result_index_current WHERE target = ?",
                    [target],
                )
            elif scoped_product_ids:
                conn.executemany(
                    """
                    DELETE FROM validation_result_index_current
                    WHERE target = ? AND product_id = ?
                    """,
                    [[target, product_id] for product_id in sorted(scoped_product_ids)],
                )
            if current_index:
                conn.executemany(
                    """
                    INSERT INTO validation_result_index_current (
                        target, product_id, ready, status, issue_hash, issue_codes_json,
                        issue_severities_json, display_name, brand, season, season_year,
                        group_key, score, issue_count, failure_count, hard_warning_count,
                        soft_warning_count, updated_source_at, updated_at, run_id
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        [
                            target,
                            row.product_id,
                            row.ready,
                            row.status,
                            row.issue_hash,
                            json.dumps(list(row.issue_codes), sort_keys=True),
                            json.dumps(row.issue_severities, sort_keys=True),
                            row.display_name,
                            row.brand,
                            row.season,
                            row.season_year,
                            row.group_key,
                            row.score,
                            row.issue_count,
                            row.failure_count,
                            row.hard_warning_count,
                            row.soft_warning_count,
                            row.updated_source_at,
                            created_at,
                            run_id,
                        ]
                        for row in current_index.values()
                    ],
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    return ValidationHistoryRun(
        run_id=run_id,
        target=target,
        product_change_count=len(product_events),
        issue_change_count=len(issue_events),
    )


def ensure_validation_history_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS validation_runs (
            run_id VARCHAR PRIMARY KEY,
            target VARCHAR NOT NULL,
            created_at TIMESTAMP NOT NULL,
            input_path VARCHAR,
            input_sha256 VARCHAR,
            latest_result_path VARCHAR,
            latest_result_sha256 VARCHAR,
            rule_set_version VARCHAR,
            total_records BIGINT,
            ready_records BIGINT,
            readiness_percent DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS validation_result_index_current (
            target VARCHAR NOT NULL,
            product_id VARCHAR NOT NULL,
            ready BOOLEAN,
            status VARCHAR,
            issue_hash VARCHAR,
            issue_codes_json VARCHAR,
            issue_severities_json VARCHAR,
            display_name VARCHAR,
            brand VARCHAR,
            season VARCHAR,
            season_year INTEGER,
            group_key VARCHAR,
            score DOUBLE,
            issue_count BIGINT,
            failure_count BIGINT,
            hard_warning_count BIGINT,
            soft_warning_count BIGINT,
            updated_source_at TIMESTAMP,
            updated_at TIMESTAMP,
            run_id VARCHAR,
            PRIMARY KEY (target, product_id)
        )
        """
    )
    _ensure_column(conn, "validation_result_index_current", "display_name", "VARCHAR")
    _ensure_column(conn, "validation_result_index_current", "brand", "VARCHAR")
    _ensure_column(conn, "validation_result_index_current", "season", "VARCHAR")
    _ensure_column(conn, "validation_result_index_current", "season_year", "INTEGER")
    _ensure_column(conn, "validation_result_index_current", "group_key", "VARCHAR")
    _ensure_column(conn, "validation_result_index_current", "score", "DOUBLE")
    _ensure_column(conn, "validation_result_index_current", "issue_count", "BIGINT")
    _ensure_column(conn, "validation_result_index_current", "failure_count", "BIGINT")
    _ensure_column(conn, "validation_result_index_current", "hard_warning_count", "BIGINT")
    _ensure_column(conn, "validation_result_index_current", "soft_warning_count", "BIGINT")
    _ensure_column(conn, "validation_result_index_current", "updated_source_at", "TIMESTAMP")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS validation_change_events (
            run_id VARCHAR NOT NULL,
            target VARCHAR NOT NULL,
            changed_at TIMESTAMP NOT NULL,
            product_id VARCHAR NOT NULL,
            change_type VARCHAR NOT NULL,
            previous_ready BOOLEAN,
            current_ready BOOLEAN,
            previous_status VARCHAR,
            current_status VARCHAR,
            previous_issue_hash VARCHAR,
            current_issue_hash VARCHAR,
            previous_issue_codes_json VARCHAR,
            current_issue_codes_json VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS validation_issue_change_events (
            run_id VARCHAR NOT NULL,
            target VARCHAR NOT NULL,
            changed_at TIMESTAMP NOT NULL,
            product_id VARCHAR NOT NULL,
            issue_code VARCHAR NOT NULL,
            change_type VARCHAR NOT NULL,
            severity VARCHAR
        )
        """
    )


def list_validation_runs(
    db_path: Path,
    *,
    target: str | None = None,
    since: datetime | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    if not db_path.is_file():
        return []
    clauses, params = _history_filters(target=target, since=since, time_column="created_at")
    query = f"""
        SELECT run_id, target, created_at, total_records, ready_records,
               readiness_percent, product_changes, issue_changes
        FROM validation_runs
        LEFT JOIN (
            SELECT run_id, COUNT(*) AS product_changes
            FROM validation_change_events
            GROUP BY run_id
        ) product_counts USING (run_id)
        LEFT JOIN (
            SELECT run_id, COUNT(*) AS issue_changes
            FROM validation_issue_change_events
            GROUP BY run_id
        ) issue_counts USING (run_id)
        {clauses}
        ORDER BY created_at DESC
        LIMIT ?
    """
    with duckdb.connect(str(db_path), read_only=True) as conn:
        if not _has_table(conn, "validation_runs"):
            return []
        rows = conn.execute(query, [*params, limit]).fetchall()
    return [
        {
            "run_id": row[0],
            "target": row[1],
            "created_at": row[2],
            "total_records": row[3] or 0,
            "ready_records": row[4] or 0,
            "readiness_percent": row[5] or 0.0,
            "product_changes": row[6] or 0,
            "issue_changes": row[7] or 0,
        }
        for row in rows
    ]


def list_validation_changes(
    db_path: Path,
    *,
    target: str | None = None,
    since: datetime | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    if not db_path.is_file():
        return []
    clauses, params = _history_filters(target=target, since=since, time_column="changed_at")
    query = f"""
        SELECT run_id, target, changed_at, product_id, change_type,
               previous_status, current_status,
               previous_issue_codes_json, current_issue_codes_json
        FROM validation_change_events
        {clauses}
        ORDER BY changed_at DESC, product_id
        LIMIT ?
    """
    with duckdb.connect(str(db_path), read_only=True) as conn:
        if not _has_table(conn, "validation_change_events"):
            return []
        rows = conn.execute(query, [*params, limit]).fetchall()
    return [
        {
            "run_id": row[0],
            "target": row[1],
            "changed_at": row[2],
            "product_id": row[3],
            "change_type": row[4],
            "previous_status": row[5],
            "current_status": row[6],
            "previous_issue_codes": _json_list(row[7]),
            "current_issue_codes": _json_list(row[8]),
        }
        for row in rows
    ]


def list_validation_issue_counts(
    db_path: Path,
    *,
    target: str | None = None,
    since: datetime | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    if not db_path.is_file():
        return []
    clauses, params = _history_filters(target=target, since=since, time_column="changed_at")
    query = f"""
        SELECT target, issue_code, change_type, severity, COUNT(*) AS count
        FROM validation_issue_change_events
        {clauses}
        GROUP BY target, issue_code, change_type, severity
        ORDER BY count DESC, issue_code, change_type
        LIMIT ?
    """
    with duckdb.connect(str(db_path), read_only=True) as conn:
        if not _has_table(conn, "validation_issue_change_events"):
            return []
        rows = conn.execute(query, [*params, limit]).fetchall()
    return [
        {
            "target": row[0],
            "issue_code": row[1],
            "change_type": row[2],
            "severity": row[3],
            "count": row[4],
        }
        for row in rows
    ]


def validation_index_counts(db_path: Path, *, target: str) -> tuple[int, int] | None:
    if not db_path.is_file():
        return None
    with duckdb.connect(str(db_path), read_only=True) as conn:
        if not _has_table(conn, "validation_result_index_current"):
            return None
        row = conn.execute(
            """
            SELECT COUNT(*) AS total_records,
                   COUNT(*) FILTER (WHERE ready IS TRUE) AS ready_records
            FROM validation_result_index_current
            WHERE target = ?
            """,
            [target],
        ).fetchone()
    if row is None or row[0] == 0:
        return None
    return int(row[0] or 0), int(row[1] or 0)


def parse_history_since(value: str | None, *, now: datetime | None = None) -> datetime | None:
    if value is None or not value.strip():
        return None
    value = value.strip()
    current = now or datetime.now().astimezone()
    duration_match = _DURATION_PATTERN.match(value)
    if duration_match:
        count = int(duration_match.group("count"))
        unit = duration_match.group("unit")
        if unit == "h":
            return _to_utc_naive(current - timedelta(hours=count))
        if unit == "d":
            return _to_utc_naive(current - timedelta(days=count))
        if unit == "m":
            return _to_utc_naive(_subtract_months(current, count))
        if unit == "y":
            return _to_utc_naive(_subtract_years(current, count))

    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        parsed = None
    if parsed is None:
        for date_format in _ABSOLUTE_FORMATS:
            try:
                parsed = datetime.strptime(value, date_format)
                break
            except ValueError:
                continue
    if parsed is None:
        raise ValueError(
            "Use an absolute date/time like 2026-05-08 or 2026-05-08T14:30, "
            "or a relative duration like 10h, 2d, 3m, or 1y."
        )
    if parsed.tzinfo is None:
        parsed = parsed.astimezone()
    return _to_utc_naive(parsed)


def _diff_validation_indexes(
    *,
    run_id: str,
    target: str,
    changed_at: datetime,
    previous_index: dict[str, ValidationIndexRow],
    current_index: dict[str, ValidationIndexRow],
) -> tuple[list[list[Any]], list[list[Any]]]:
    product_events: list[list[Any]] = []
    issue_events: list[list[Any]] = []
    all_product_ids = sorted(set(previous_index) | set(current_index))
    for product_id in all_product_ids:
        previous = previous_index.get(product_id)
        current = current_index.get(product_id)
        if previous is None and current is not None:
            change_type = "added"
        elif previous is not None and current is None:
            change_type = "removed"
        elif previous is not None and current is not None and _index_row_changed(previous, current):
            change_type = "changed"
        else:
            continue

        previous_codes = previous.issue_codes if previous is not None else ()
        current_codes = current.issue_codes if current is not None else ()
        product_events.append(
            [
                run_id,
                target,
                changed_at,
                product_id,
                change_type,
                previous.ready if previous is not None else None,
                current.ready if current is not None else None,
                previous.status if previous is not None else None,
                current.status if current is not None else None,
                previous.issue_hash if previous is not None else None,
                current.issue_hash if current is not None else None,
                json.dumps(list(previous_codes), sort_keys=True),
                json.dumps(list(current_codes), sort_keys=True),
            ]
        )
        added_codes = sorted(set(current_codes) - set(previous_codes))
        resolved_codes = sorted(set(previous_codes) - set(current_codes))
        for code in added_codes:
            issue_events.append(
                [
                    run_id,
                    target,
                    changed_at,
                    product_id,
                    code,
                    "added",
                    (current.issue_severities.get(code) if current else None),
                ]
            )
        for code in resolved_codes:
            issue_events.append(
                [
                    run_id,
                    target,
                    changed_at,
                    product_id,
                    code,
                    "resolved",
                    (previous.issue_severities.get(code) if previous else None),
                ]
            )
    return product_events, issue_events


def _build_validation_index(run: Any) -> dict[str, ValidationIndexRow]:
    index: dict[str, ValidationIndexRow] = {}
    for result in _result_value(run, "results", default=[]) or []:
        product_id = _product_id(result)
        if not product_id:
            continue
        issues = list(_result_value(result, "issues", default=[]) or [])
        issue_codes = tuple(sorted({_issue_code(issue) for issue in issues if _issue_code(issue)}))
        issue_severities = {
            code: _issue_severity_for_code(code, issues) for code in issue_codes if code
        }
        ready = _ready_value(result)
        status = str(_result_value(result, "status", default="") or "")
        if not status:
            status = "ready" if ready is True else "failed" if ready is False else "unknown"
        issue_hash = _issue_hash(issues)
        brand = _dashboard_string(result, "brand", "brand_code", "brand_name")
        season = _dashboard_string(result, "season", "season_code")
        index[product_id] = ValidationIndexRow(
            product_id=product_id,
            ready=ready,
            status=status,
            issue_hash=issue_hash,
            issue_codes=issue_codes,
            issue_severities=issue_severities,
            display_name=_dashboard_string(result, "display_name", "style_name", "name"),
            brand=brand,
            season=season,
            season_year=_season_year(season),
            group_key=_dashboard_group_key(result, brand=brand, season=season),
            score=_float_value(result, "score"),
            issue_count=_issue_count(result, issues),
            failure_count=_failure_count(result, issues),
            hard_warning_count=_warning_count(result, issues, warning_level="hard"),
            soft_warning_count=_warning_count(result, issues, warning_level="soft"),
            updated_source_at=_updated_source_at(result),
        )
    return index


def _load_current_index(
    conn: duckdb.DuckDBPyConnection,
    target: str,
) -> dict[str, ValidationIndexRow]:
    if not _has_table(conn, "validation_result_index_current"):
        return {}
    rows = conn.execute(
        """
        SELECT product_id, ready, status, issue_hash, issue_codes_json, issue_severities_json
        FROM validation_result_index_current
        WHERE target = ?
        """,
        [target],
    ).fetchall()
    return {
        row[0]: ValidationIndexRow(
            product_id=row[0],
            ready=row[1],
            status=row[2] or "unknown",
            issue_hash=row[3] or "",
            issue_codes=tuple(_json_list(row[4])),
            issue_severities=_json_dict(row[5]),
        )
        for row in rows
    }


def _allocate_run_id(
    conn: duckdb.DuckDBPyConnection,
    target: str,
    created_at: datetime,
) -> str:
    base = f"{created_at:%Y-%m-%dT%H%M%SZ}-{_target_slug(target)}"
    for index in range(100):
        suffix = "" if index == 0 else f"-{index + 1}"
        run_id = f"{base}{suffix}"
        exists = conn.execute(
            "SELECT COUNT(*) FROM validation_runs WHERE run_id = ?",
            [run_id],
        ).fetchone()[0]
        if not exists:
            return run_id
    raise RuntimeError(f"Could not allocate validation history run id for target {target!r}.")


def _history_filters(
    *,
    target: str | None,
    since: datetime | None,
    time_column: str,
) -> tuple[str, list[Any]]:
    filters = []
    params: list[Any] = []
    if target:
        filters.append("target = ?")
        params.append(target)
    if since is not None:
        filters.append(f"{time_column} >= ?")
        params.append(since)
    if not filters:
        return "", []
    return "WHERE " + " AND ".join(filters), params


def _has_table(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and row[0])


def _ensure_column(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    column_name: str,
    column_type: str,
) -> None:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_name = ?
          AND column_name = ?
        """,
        [table_name, column_name],
    ).fetchone()
    if row and row[0]:
        return
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")


def _index_row_changed(previous: ValidationIndexRow, current: ValidationIndexRow) -> bool:
    return (
        previous.ready != current.ready
        or previous.status != current.status
        or previous.issue_hash != current.issue_hash
        or previous.issue_codes != current.issue_codes
    )


def _issue_hash(issues: list[Any]) -> str:
    identities = sorted(
        (_issue_identity(issue) for issue in issues),
        key=_canonical_json,
    )
    payload = _canonical_json(identities)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _issue_identity(issue: Any) -> dict[str, Any]:
    return {
        "blocking": _result_value(issue, "blocking", default=None),
        "code": _issue_code(issue),
        "rule_id": _result_value(issue, "rule_id", default=None),
        "severity": _issue_severity(issue),
        "source_field": _result_value(issue, "source_field", default=None),
        "source_path": _result_value(issue, "source_path", default=None),
        "source_record_id": _result_value(issue, "source_record_id", default=None),
    }


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _issue_code(issue: Any) -> str:
    fallback = _result_value(issue, "issue_code", default="")
    return str(
        _result_value(
            issue,
            "code",
            default=_result_value(issue, "report_code", default=fallback),
        )
        or ""
    )


def _issue_severity_for_code(code: str, issues: list[Any]) -> str:
    for issue in issues:
        if _issue_code(issue) == code:
            return _issue_severity(issue)
    return ""


def _issue_severity(issue: Any) -> str:
    value = _result_value(issue, "severity", default="")
    return str(getattr(value, "value", value) or "")


def _product_id(result: Any) -> str:
    for key in ("centric_style_id", "style_id", "product_id", "id"):
        value = _result_value(result, key, default=None)
        if value:
            return str(value)
    return ""


def _ready_value(result: Any) -> bool | None:
    value = _result_value(result, "ready", default=None)
    return value if isinstance(value, bool) else None


def _validation_counts(run: Any) -> tuple[int, int]:
    total = _result_value(run, "total_products", default=0)
    ready = _result_value(run, "ready_products", default=0)
    return int(total or 0), int(ready or 0)


def _readiness_percent(run: Any) -> float:
    return float(_result_value(run, "readiness_percent", default=0.0) or 0.0)


def _result_value(value: Any, key: str, *, default: Any) -> Any:
    if isinstance(value, dict):
        return value.get(key, default)
    return getattr(value, key, default)


def _dashboard_string(result: Any, *keys: str) -> str | None:
    for key in keys:
        value = _result_value(result, key, default=None)
        if value is None:
            continue
        text = str(getattr(value, "value", value) or "").strip()
        if text:
            return text
    return None


def _dashboard_group_key(result: Any, *, brand: str | None, season: str | None) -> str | None:
    explicit = _dashboard_string(result, "group_key")
    if explicit:
        return explicit
    if brand or season:
        return f"{brand or 'UNKNOWN'}|{season or 'UNKNOWN'}"
    return None


def _season_year(season: str | None) -> int | None:
    if not season:
        return None
    digits = "".join(character for character in str(season) if character.isdigit())
    if len(digits) < 2:
        return None
    return int(digits[-2:])


def _float_value(result: Any, key: str) -> float | None:
    value = _result_value(result, key, default=None)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_value(result: Any, key: str) -> int | None:
    value = _result_value(result, key, default=None)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _issue_count(result: Any, issues: list[Any]) -> int:
    explicit = _int_value(result, "issue_count")
    return explicit if explicit is not None else len(issues)


def _failure_count(result: Any, issues: list[Any]) -> int:
    for key in ("failure_count", "blocking_issue_count", "error_count"):
        explicit = _int_value(result, key)
        if explicit is not None:
            return explicit
    return sum(1 for issue in issues if _issue_severity(issue) == "error")


def _warning_count(result: Any, issues: list[Any], *, warning_level: str) -> int:
    explicit = _int_value(result, f"{warning_level}_warning_count")
    if explicit is not None:
        return explicit
    if warning_level == "soft":
        generic_warning_count = _int_value(result, "warning_count")
        if generic_warning_count is not None:
            hard_warning_count = _int_value(result, "hard_warning_count")
            if hard_warning_count is not None:
                return max(0, generic_warning_count - hard_warning_count)
            return generic_warning_count
    return sum(
        1
        for issue in issues
        if _issue_severity(issue) == "warning"
        and _result_value(issue, "warning_level", default=None) == warning_level
    )


def _updated_source_at(result: Any) -> datetime | None:
    for key in (
        "updated_source_at",
        "source_updated_at",
        "source_modified_at",
        "latest_modified_at",
        "modified_at",
    ):
        parsed = _parse_datetime(_result_value(result, key, default=None))
        if parsed is not None:
            return parsed
    metrics = _result_value(result, "metrics", default=None)
    if isinstance(metrics, dict):
        for key in ("updated_source_at", "latest_modified_at", "modified_at"):
            parsed = _parse_datetime(metrics.get(key))
            if parsed is not None:
                return parsed
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value
        return value.astimezone(UTC).replace(tzinfo=None)
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed
    return parsed.astimezone(UTC).replace(tzinfo=None)


def _file_sha256(path: Path | None) -> str | None:
    if path is None or not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_list(value: Any) -> list[str]:
    if not value:
        return []
    try:
        payload = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return []
    if not isinstance(payload, list):
        return []
    return [str(item) for item in payload]


def _json_dict(value: Any) -> dict[str, str]:
    if not value:
        return {}
    try:
        payload = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    return {str(key): str(val) for key, val in payload.items()}


def _subtract_months(value: datetime, months: int) -> datetime:
    month_index = (value.year * 12 + value.month - 1) - months
    year, month_zero = divmod(month_index, 12)
    month = month_zero + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def _subtract_years(value: datetime, years: int) -> datetime:
    year = value.year - years
    day = min(value.day, calendar.monthrange(year, value.month)[1])
    return value.replace(year=year, day=day)


def _to_utc_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.astimezone()
    return value.astimezone(UTC).replace(tzinfo=None)


def _utc_now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _target_slug(target: str) -> str:
    return "".join(character if character.isalnum() else "-" for character in target).strip("-")
