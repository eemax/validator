from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import yaml

from centric_mdm_validation.centric.config import resolve_optional_private_config_path

CHANGELOG_CONFIG_PATH = Path("changelog.yml")


@dataclass(frozen=True)
class EndpointChangelogEndpoint:
    name: str
    fields: tuple[str, ...]
    include_missing: bool = False
    drop_empty: bool = False
    sort_arrays: bool = False


@dataclass(frozen=True)
class EndpointChangelogConfig:
    path: Path
    config_sha256: str
    endpoints: dict[str, EndpointChangelogEndpoint]


@dataclass(frozen=True)
class EndpointChangelogRun:
    run_id: str
    endpoint_count: int
    record_count: int
    event_count: int
    full_refresh: bool = False
    scoped_record_count: int = 0


@dataclass(frozen=True)
class EndpointChangelogIndexRow:
    endpoint: str
    record_id: str
    payload_hash: str
    tracked_payload_json: str
    config_sha256: str | None = None


def load_endpoint_changelog_config(path: Path | None = None) -> EndpointChangelogConfig:
    resolved_path = (
        Path(path)
        if path is not None
        else resolve_optional_private_config_path(CHANGELOG_CONFIG_PATH)
    )
    if resolved_path is None:
        raise ValueError(
            "Endpoint changelog config not found. Create CENTRIC_CONFIG_DIR/changelog.yml, "
            ".local/changelog.yml, or pass --config."
        )
    if not resolved_path.is_file():
        raise ValueError(f"Endpoint changelog config file not found: {resolved_path}")

    payload = yaml.safe_load(resolved_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Endpoint changelog config root must be an object: {resolved_path}")
    endpoints_payload = payload.get("endpoints")
    if not isinstance(endpoints_payload, dict) or not endpoints_payload:
        raise ValueError(
            f"Endpoint changelog config must define non-empty endpoints: {resolved_path}"
        )

    defaults = payload.get("defaults") or {}
    if not isinstance(defaults, dict):
        raise ValueError(f"Endpoint changelog defaults must be an object: {resolved_path}")

    endpoints: dict[str, EndpointChangelogEndpoint] = {}
    for endpoint_name, raw_config in endpoints_payload.items():
        if raw_config is None:
            raw_config = {}
        if isinstance(raw_config, list):
            raw_config = {"fields": raw_config}
        if not isinstance(raw_config, dict):
            raise ValueError(
                f"Endpoint changelog config for {endpoint_name!r} must be an object or field list."
            )
        fields = _field_tuple(raw_config.get("fields"), endpoint_name=str(endpoint_name))
        endpoints[str(endpoint_name)] = EndpointChangelogEndpoint(
            name=str(endpoint_name),
            fields=fields,
            include_missing=_bool_setting(raw_config, defaults, "include_missing", default=False),
            drop_empty=_bool_setting(raw_config, defaults, "drop_empty", default=False),
            sort_arrays=_bool_setting(raw_config, defaults, "sort_arrays", default=False),
        )

    return EndpointChangelogConfig(
        path=resolved_path,
        config_sha256=_file_sha256(resolved_path),
        endpoints=endpoints,
    )


def record_endpoint_changelog(
    db_path: Path,
    *,
    config: EndpointChangelogConfig,
    endpoints: set[str] | None = None,
    record_ids_by_endpoint: dict[str, set[str]] | None = None,
    deleted_record_ids_by_endpoint: dict[str, set[str]] | None = None,
    full: bool = False,
) -> EndpointChangelogRun:
    if not db_path.is_file():
        raise ValueError(f"DuckDB store not found. Run ingest first: {db_path}")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    created_at = _utc_now()
    with duckdb.connect(str(db_path)) as conn:
        ensure_endpoint_changelog_tables(conn)
        if not _has_table(conn, "endpoint_records"):
            raise ValueError(
                f"DuckDB store has no endpoint_records table. Run ingest first: {db_path}"
            )
        run_id = _allocate_run_id(conn, created_at)
        has_record_scope = (
            record_ids_by_endpoint is not None or deleted_record_ids_by_endpoint is not None
        )
        scoped_config, full_refresh = _scope_config(
            conn,
            config,
            endpoints=endpoints,
            full=full or not has_record_scope,
        )
        if not scoped_config.endpoints:
            return EndpointChangelogRun(
                run_id=run_id,
                endpoint_count=0,
                record_count=0,
                event_count=0,
                full_refresh=False,
                scoped_record_count=0,
            )
        scoped_record_count = _scoped_record_count(record_ids_by_endpoint)
        previous_index = _load_current_index(conn, endpoints=set(scoped_config.endpoints))
        if full_refresh:
            current_index = _build_current_index(conn, scoped_config)
        else:
            current_index = _build_scoped_current_index(
                conn,
                scoped_config,
                record_ids_by_endpoint=record_ids_by_endpoint or {},
            )
            previous_index = _filter_previous_index_for_scoped_update(
                previous_index,
                current_index=current_index,
                deleted_record_ids_by_endpoint=deleted_record_ids_by_endpoint or {},
            )
        events = _diff_endpoint_indexes(
            run_id=run_id,
            changed_at=created_at,
            previous_index=previous_index,
            current_index=current_index,
        )

        conn.execute("BEGIN TRANSACTION")
        try:
            _insert_run(
                conn,
                run_id=run_id,
                created_at=created_at,
                config=config,
                endpoint_count=len(scoped_config.endpoints),
                record_count=len(current_index),
                event_count=len(events),
                full_refresh=full_refresh,
                scoped_record_count=scoped_record_count,
            )
            if events:
                conn.executemany(
                    """
                    INSERT INTO endpoint_change_events (
                        run_id, endpoint, record_id, changed_at, change_type,
                        previous_hash, current_hash, changed_fields_json,
                        previous_payload_json, current_payload_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    events,
                )
            endpoint_names = sorted(scoped_config.endpoints)
            if full_refresh and endpoint_names:
                conn.execute(
                    f"""
                    DELETE FROM endpoint_changelog_index_current
                    WHERE endpoint IN ({",".join("?" for _ in endpoint_names)})
                    """,
                    endpoint_names,
                )
            elif not full_refresh and previous_index:
                keys = sorted(previous_index)
                conn.executemany(
                    """
                    DELETE FROM endpoint_changelog_index_current
                    WHERE endpoint = ? AND record_id = ?
                    """,
                    [[endpoint, record_id] for endpoint, record_id in keys],
                )
            if current_index:
                conn.executemany(
                    """
                    INSERT INTO endpoint_changelog_index_current (
                        endpoint, record_id, payload_hash, tracked_payload_json,
                        config_sha256, updated_at, run_id
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        [
                            row.endpoint,
                            row.record_id,
                            row.payload_hash,
                            row.tracked_payload_json,
                            config.config_sha256,
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

    return EndpointChangelogRun(
        run_id=run_id,
        endpoint_count=len(scoped_config.endpoints),
        record_count=len(current_index),
        event_count=len(events),
        full_refresh=full_refresh,
        scoped_record_count=scoped_record_count,
    )


def ensure_endpoint_changelog_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS endpoint_changelog_runs (
            run_id VARCHAR PRIMARY KEY,
            created_at TIMESTAMP NOT NULL,
            config_path VARCHAR NOT NULL,
            config_sha256 VARCHAR NOT NULL,
            endpoint_count BIGINT NOT NULL,
            record_count BIGINT NOT NULL,
            event_count BIGINT NOT NULL,
            full_refresh BOOLEAN,
            scoped_record_count BIGINT
        )
        """
    )
    _ensure_column(conn, "endpoint_changelog_runs", "full_refresh", "BOOLEAN")
    _ensure_column(conn, "endpoint_changelog_runs", "scoped_record_count", "BIGINT")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS endpoint_changelog_index_current (
            endpoint VARCHAR NOT NULL,
            record_id VARCHAR NOT NULL,
            payload_hash VARCHAR NOT NULL,
            tracked_payload_json VARCHAR NOT NULL,
            config_sha256 VARCHAR,
            updated_at TIMESTAMP NOT NULL,
            run_id VARCHAR NOT NULL,
            PRIMARY KEY (endpoint, record_id)
        )
        """
    )
    _ensure_column(conn, "endpoint_changelog_index_current", "config_sha256", "VARCHAR")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS endpoint_change_events (
            run_id VARCHAR NOT NULL,
            endpoint VARCHAR NOT NULL,
            record_id VARCHAR NOT NULL,
            changed_at TIMESTAMP NOT NULL,
            change_type VARCHAR NOT NULL,
            previous_hash VARCHAR,
            current_hash VARCHAR,
            changed_fields_json VARCHAR NOT NULL,
            previous_payload_json VARCHAR,
            current_payload_json VARCHAR
        )
        """
    )


def list_endpoint_changelog_runs(
    db_path: Path,
    *,
    since: datetime | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    if not db_path.is_file():
        return []
    clause, params = _since_filter(since, "created_at")
    query = f"""
        SELECT run_id, created_at, config_path, endpoint_count, record_count, event_count,
               full_refresh, scoped_record_count
        FROM endpoint_changelog_runs
        {clause}
        ORDER BY created_at DESC
        LIMIT ?
    """
    with duckdb.connect(str(db_path), read_only=True) as conn:
        if not _has_table(conn, "endpoint_changelog_runs"):
            return []
        rows = conn.execute(query, [*params, limit]).fetchall()
    return [
        {
            "run_id": row[0],
            "created_at": row[1],
            "config_path": row[2],
            "endpoint_count": row[3],
            "record_count": row[4],
            "event_count": row[5],
            "full_refresh": bool(row[6]),
            "scoped_record_count": row[7] or 0,
        }
        for row in rows
    ]


def list_endpoint_change_summary(
    db_path: Path,
    *,
    since: datetime | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    if not db_path.is_file():
        return []
    clause, params = _since_filter(since, "changed_at")
    query = f"""
        SELECT endpoint, change_type, COUNT(*) AS count
        FROM endpoint_change_events
        {clause}
        GROUP BY endpoint, change_type
        ORDER BY endpoint, change_type
        LIMIT ?
    """
    with duckdb.connect(str(db_path), read_only=True) as conn:
        if not _has_table(conn, "endpoint_change_events"):
            return []
        rows = conn.execute(query, [*params, limit]).fetchall()
    return [{"endpoint": row[0], "change_type": row[1], "count": row[2]} for row in rows]


def list_endpoint_changes(
    db_path: Path,
    *,
    endpoint: str | None = None,
    since: datetime | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    if not db_path.is_file():
        return []
    clauses: list[str] = []
    params: list[Any] = []
    if endpoint:
        clauses.append("endpoint = ?")
        params.append(endpoint)
    if since is not None:
        clauses.append("changed_at >= ?")
        params.append(since)
    clause = "WHERE " + " AND ".join(clauses) if clauses else ""
    query = f"""
        SELECT run_id, endpoint, record_id, changed_at, change_type, changed_fields_json,
               previous_payload_json, current_payload_json
        FROM endpoint_change_events
        {clause}
        ORDER BY changed_at DESC, endpoint, record_id
        LIMIT ?
    """
    with duckdb.connect(str(db_path), read_only=True) as conn:
        if not _has_table(conn, "endpoint_change_events"):
            return []
        rows = conn.execute(query, [*params, limit]).fetchall()
    return [
        {
            "run_id": row[0],
            "endpoint": row[1],
            "record_id": row[2],
            "changed_at": row[3],
            "change_type": row[4],
            "changed_fields": _json_list(row[5]),
            "previous_payload": _json_dict(row[6]),
            "current_payload": _json_dict(row[7]),
        }
        for row in rows
    ]


def _build_current_index(
    conn: duckdb.DuckDBPyConnection,
    config: EndpointChangelogConfig,
) -> dict[tuple[str, str], EndpointChangelogIndexRow]:
    endpoint_names = sorted(config.endpoints)
    if not endpoint_names:
        return {}
    rows = conn.execute(
        f"""
        SELECT endpoint, record_id, payload
        FROM endpoint_records
        WHERE endpoint IN ({",".join("?" for _ in endpoint_names)})
        ORDER BY endpoint, record_id
        """,
        endpoint_names,
    ).fetchall()
    index: dict[tuple[str, str], EndpointChangelogIndexRow] = {}
    for endpoint, record_id, payload_json in rows:
        endpoint_config = config.endpoints.get(endpoint)
        if endpoint_config is None:
            continue
        try:
            payload = json.loads(payload_json)
        except json.JSONDecodeError:
            continue
        tracked_payload = _tracked_payload(payload, endpoint_config)
        canonical_payload = _canonical_json(tracked_payload)
        index[(endpoint, record_id)] = EndpointChangelogIndexRow(
            endpoint=endpoint,
            record_id=record_id,
            payload_hash=_payload_hash(canonical_payload),
            tracked_payload_json=canonical_payload,
            config_sha256=config.config_sha256,
        )
    return index


def _build_scoped_current_index(
    conn: duckdb.DuckDBPyConnection,
    config: EndpointChangelogConfig,
    *,
    record_ids_by_endpoint: dict[str, set[str]],
) -> dict[tuple[str, str], EndpointChangelogIndexRow]:
    index: dict[tuple[str, str], EndpointChangelogIndexRow] = {}
    for endpoint, record_ids in sorted(record_ids_by_endpoint.items()):
        endpoint_config = config.endpoints.get(endpoint)
        if endpoint_config is None or not record_ids:
            continue
        rows = conn.execute(
            f"""
            SELECT endpoint, record_id, payload
            FROM endpoint_records
            WHERE endpoint = ?
              AND record_id IN ({",".join("?" for _ in record_ids)})
            ORDER BY endpoint, record_id
            """,
            [endpoint, *sorted(record_ids)],
        ).fetchall()
        for row_endpoint, record_id, payload_json in rows:
            try:
                payload = json.loads(payload_json)
            except json.JSONDecodeError:
                continue
            tracked_payload = _tracked_payload(payload, endpoint_config)
            canonical_payload = _canonical_json(tracked_payload)
            index[(row_endpoint, record_id)] = EndpointChangelogIndexRow(
                endpoint=row_endpoint,
                record_id=record_id,
                payload_hash=_payload_hash(canonical_payload),
                tracked_payload_json=canonical_payload,
                config_sha256=config.config_sha256,
            )
    return index


def _load_current_index(
    conn: duckdb.DuckDBPyConnection,
    *,
    endpoints: set[str],
) -> dict[tuple[str, str], EndpointChangelogIndexRow]:
    if not _has_table(conn, "endpoint_changelog_index_current") or not endpoints:
        return {}
    endpoint_names = sorted(endpoints)
    rows = conn.execute(
        f"""
        SELECT endpoint, record_id, payload_hash, tracked_payload_json, config_sha256
        FROM endpoint_changelog_index_current
        WHERE endpoint IN ({",".join("?" for _ in endpoint_names)})
        """,
        endpoint_names,
    ).fetchall()
    return {
        (row[0], row[1]): EndpointChangelogIndexRow(
            endpoint=row[0],
            record_id=row[1],
            payload_hash=row[2],
            tracked_payload_json=row[3],
            config_sha256=row[4],
        )
        for row in rows
    }


def _diff_endpoint_indexes(
    *,
    run_id: str,
    changed_at: datetime,
    previous_index: dict[tuple[str, str], EndpointChangelogIndexRow],
    current_index: dict[tuple[str, str], EndpointChangelogIndexRow],
) -> list[list[Any]]:
    events: list[list[Any]] = []
    for endpoint, record_id in sorted(set(previous_index) | set(current_index)):
        previous = previous_index.get((endpoint, record_id))
        current = current_index.get((endpoint, record_id))
        if previous is None and current is not None:
            change_type = "added"
        elif previous is not None and current is None:
            change_type = "removed"
        elif (
            previous is not None
            and current is not None
            and previous.payload_hash != current.payload_hash
        ):
            change_type = "changed"
        else:
            continue
        events.append(
            [
                run_id,
                endpoint,
                record_id,
                changed_at,
                change_type,
                previous.payload_hash if previous else None,
                current.payload_hash if current else None,
                json.dumps(_changed_fields(previous, current), sort_keys=True),
                previous.tracked_payload_json if previous else None,
                current.tracked_payload_json if current else None,
            ]
        )
    return events


def _scope_config(
    conn: duckdb.DuckDBPyConnection,
    config: EndpointChangelogConfig,
    *,
    endpoints: set[str] | None,
    full: bool,
) -> tuple[EndpointChangelogConfig, bool]:
    endpoint_names = sorted((endpoints or set(config.endpoints)) & set(config.endpoints))
    scoped_endpoints = {name: config.endpoints[name] for name in endpoint_names}
    scoped_config = EndpointChangelogConfig(
        path=config.path,
        config_sha256=config.config_sha256,
        endpoints=scoped_endpoints,
    )
    if full or not endpoint_names:
        return scoped_config, full
    if not _has_table(conn, "endpoint_changelog_index_current"):
        return scoped_config, True
    rows = conn.execute(
        f"""
        SELECT endpoint, COUNT(*) AS current_rows,
               COUNT(*) FILTER (WHERE config_sha256 = ?) AS matching_config_rows
        FROM endpoint_changelog_index_current
        WHERE endpoint IN ({",".join("?" for _ in endpoint_names)})
        GROUP BY endpoint
        """,
        [config.config_sha256, *endpoint_names],
    ).fetchall()
    by_endpoint = {row[0]: (int(row[1] or 0), int(row[2] or 0)) for row in rows}
    for endpoint in endpoint_names:
        current_rows, matching_rows = by_endpoint.get(endpoint, (0, 0))
        if current_rows == 0 or matching_rows != current_rows:
            return scoped_config, True
    return scoped_config, False


def _filter_previous_index_for_scoped_update(
    previous_index: dict[tuple[str, str], EndpointChangelogIndexRow],
    *,
    current_index: dict[tuple[str, str], EndpointChangelogIndexRow],
    deleted_record_ids_by_endpoint: dict[str, set[str]],
) -> dict[tuple[str, str], EndpointChangelogIndexRow]:
    keys = set(current_index)
    for endpoint, record_ids in deleted_record_ids_by_endpoint.items():
        keys.update((endpoint, record_id) for record_id in record_ids)
    return {key: previous_index[key] for key in keys if key in previous_index}


def _scoped_record_count(record_ids_by_endpoint: dict[str, set[str]] | None) -> int:
    if not record_ids_by_endpoint:
        return 0
    return sum(len(record_ids) for record_ids in record_ids_by_endpoint.values())


def _insert_run(
    conn: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    created_at: datetime,
    config: EndpointChangelogConfig,
    endpoint_count: int,
    record_count: int,
    event_count: int,
    full_refresh: bool,
    scoped_record_count: int,
) -> None:
    conn.execute(
        """
        INSERT INTO endpoint_changelog_runs (
            run_id, created_at, config_path, config_sha256,
            endpoint_count, record_count, event_count, full_refresh, scoped_record_count
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            created_at,
            str(config.path),
            config.config_sha256,
            endpoint_count,
            record_count,
            event_count,
            full_refresh,
            scoped_record_count,
        ],
    )


def _tracked_payload(payload: dict[str, Any], config: EndpointChangelogEndpoint) -> dict[str, Any]:
    tracked: dict[str, Any] = {}
    for field in config.fields:
        found, value = _extract_path(payload, field)
        if not found and not config.include_missing:
            continue
        canonical_value = (
            None if not found else _canonical_value(value, sort_arrays=config.sort_arrays)
        )
        if config.drop_empty and _is_empty(canonical_value):
            continue
        _set_path(tracked, field, canonical_value)
    return tracked


def _extract_path(payload: dict[str, Any], path: str) -> tuple[bool, Any]:
    current: Any = payload
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return False, None
        current = current[part]
    return True, current


def _set_path(payload: dict[str, Any], path: str, value: Any) -> None:
    parts = path.split(".")
    current = payload
    for part in parts[:-1]:
        next_value = current.get(part)
        if not isinstance(next_value, dict):
            next_value = {}
            current[part] = next_value
        current = next_value
    current[parts[-1]] = value


def _canonical_value(value: Any, *, sort_arrays: bool) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _canonical_value(value[key], sort_arrays=sort_arrays)
            for key in sorted(value, key=str)
        }
    if isinstance(value, list):
        values = [_canonical_value(item, sort_arrays=sort_arrays) for item in value]
        if sort_arrays:
            return sorted(values, key=lambda item: _canonical_json(item))
        return values
    return value


def _changed_fields(
    previous: EndpointChangelogIndexRow | None,
    current: EndpointChangelogIndexRow | None,
) -> list[str]:
    previous_payload = _json_dict(previous.tracked_payload_json if previous else None)
    current_payload = _json_dict(current.tracked_payload_json if current else None)
    return sorted(
        field
        for field in set(previous_payload) | set(current_payload)
        if previous_payload.get(field) != current_payload.get(field)
    )


def _allocate_run_id(conn: duckdb.DuckDBPyConnection, created_at: datetime) -> str:
    base = f"{created_at:%Y-%m-%dT%H%M%SZ}-endpoint-changelog"
    for index in range(100):
        suffix = "" if index == 0 else f"-{index + 1}"
        run_id = f"{base}{suffix}"
        exists = conn.execute(
            "SELECT COUNT(*) FROM endpoint_changelog_runs WHERE run_id = ?",
            [run_id],
        ).fetchone()[0]
        if not exists:
            return run_id
    raise RuntimeError("Could not allocate endpoint changelog run id.")


def _field_tuple(value: Any, *, endpoint_name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError(
            f"Endpoint changelog endpoint {endpoint_name!r} needs a non-empty fields list."
        )
    fields = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ValueError(
                f"Endpoint changelog endpoint {endpoint_name!r} has an invalid field name."
            )
        fields.append(item.strip())
    return tuple(fields)


def _bool_setting(
    config: dict[str, Any],
    defaults: dict[str, Any],
    key: str,
    *,
    default: bool,
) -> bool:
    value = config.get(key, defaults.get(key, default))
    if not isinstance(value, bool):
        raise ValueError(f"Endpoint changelog setting {key!r} must be true or false.")
    return value


def _since_filter(since: datetime | None, column: str) -> tuple[str, list[Any]]:
    if since is None:
        return "", []
    return f"WHERE {column} >= ?", [since]


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


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _payload_hash(canonical_payload: str) -> str:
    return hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()


def _file_sha256(path: Path) -> str:
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


def _json_dict(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    try:
        payload = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _is_empty(value: Any) -> bool:
    return value is None or value == "" or value == [] or value == {}


def _utc_now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)
