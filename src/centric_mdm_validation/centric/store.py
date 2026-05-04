from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from centric_mdm_validation.centric.mapper import ProjectionMapping
from centric_mdm_validation.centric.reconstruction import reconstruct_products_from_records
from centric_mdm_validation.centric.schema import EndpointSchema
from centric_mdm_validation.io import write_jsonl
from centric_mdm_validation.models import CentricProductPayload


@dataclass(frozen=True)
class RawFile:
    path: Path
    endpoint: str
    is_delta: bool
    source_run_id: str
    run_mode: str | None = None
    manifest_path: Path | None = None
    manifest_sha256: str | None = None


@dataclass(frozen=True)
class IngestResult:
    applied_files: int
    skipped_files: int
    records_read: int
    records_upserted: int
    records_deleted: int
    endpoints: dict[str, int]


@dataclass(frozen=True)
class IngestFileProgress:
    action: str
    raw_file: RawFile
    file_index: int
    total_files: int
    records_read: int = 0
    records_upserted: int = 0
    records_deleted: int = 0


IngestProgressCallback = Callable[[IngestFileProgress], None]


def ingest_raw_dir(
    raw_dir: Path,
    db_path: Path,
    *,
    schemas: dict[str, EndpointSchema],
    progress: IngestProgressCallback | None = None,
) -> IngestResult:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    raw_files = discover_raw_files(raw_dir)
    endpoints: defaultdict[str, int] = defaultdict(int)
    applied_files = 0
    skipped_files = 0
    records_read = 0
    records_upserted = 0
    records_deleted = 0

    with duckdb.connect(str(db_path)) as conn:
        initialize_store(conn)
        ensure_current_endpoint_views(conn, schemas)
        total_files = len(raw_files)
        for file_index, raw_file in enumerate(raw_files, start=1):
            content_hash = _sha256(raw_file.path)
            applied_hash = _applied_hash(conn, raw_file.path)
            if applied_hash == content_hash:
                skipped_files += 1
                _emit_ingest_progress(
                    progress,
                    action="skipped",
                    raw_file=raw_file,
                    file_index=file_index,
                    total_files=total_files,
                )
                continue
            if applied_hash is not None and applied_hash != content_hash:
                raise ValueError(
                    f"Raw file changed after ingest: {raw_file.path}. "
                    "Raw evidence files are expected to be immutable."
                )

            schema = schemas.get(raw_file.endpoint, EndpointSchema(name=raw_file.endpoint))
            _validate_full_snapshot_mode(raw_file, schema)
            ingested_at = _format_datetime(datetime.now(UTC))
            _emit_ingest_progress(
                progress,
                action="start",
                raw_file=raw_file,
                file_index=file_index,
                total_files=total_files,
            )

            conn.execute("BEGIN TRANSACTION")
            try:
                file_record_count, file_upserts, file_deletes = _apply_records_for_file(
                    conn,
                    raw_file=raw_file,
                    schema=schema,
                    ingested_at=ingested_at,
                )

                conn.execute(
                    """
                    INSERT INTO applied_raw_files (
                        file_path, endpoint, source_run_id, is_delta, record_count,
                        content_sha256, manifest_path, manifest_sha256, run_mode,
                        ingested_at, ingested_at_ts, full_snapshot_mode
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, try_cast(? AS TIMESTAMP), ?)
                    """,
                    [
                        str(raw_file.path),
                        raw_file.endpoint,
                        raw_file.source_run_id,
                        raw_file.is_delta,
                        file_record_count,
                        content_hash,
                        str(raw_file.manifest_path) if raw_file.manifest_path is not None else None,
                        raw_file.manifest_sha256,
                        raw_file.run_mode,
                        ingested_at,
                        ingested_at,
                        schema.full_snapshot_mode,
                    ],
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

            applied_files += 1
            records_read += file_record_count
            records_upserted += file_upserts
            records_deleted += file_deletes
            endpoints[raw_file.endpoint] += file_record_count
            _emit_ingest_progress(
                progress,
                action="applied",
                raw_file=raw_file,
                file_index=file_index,
                total_files=total_files,
                records_read=file_record_count,
                records_upserted=file_upserts,
                records_deleted=file_deletes,
            )

    return IngestResult(
        applied_files=applied_files,
        skipped_files=skipped_files,
        records_read=records_read,
        records_upserted=records_upserted,
        records_deleted=records_deleted,
        endpoints=dict(sorted(endpoints.items())),
    )


def _validate_full_snapshot_mode(raw_file: RawFile, schema: EndpointSchema) -> None:
    if raw_file.is_delta or schema.full_snapshot_mode == "upsert_only":
        return
    raise ValueError(
        f"Unsupported full_snapshot_mode={schema.full_snapshot_mode!r} for "
        f"{raw_file.endpoint}. Only 'upsert_only' is currently implemented."
    )


def _emit_ingest_progress(
    progress: IngestProgressCallback | None,
    *,
    action: str,
    raw_file: RawFile,
    file_index: int,
    total_files: int,
    records_read: int = 0,
    records_upserted: int = 0,
    records_deleted: int = 0,
) -> None:
    if progress is None:
        return
    progress(
        IngestFileProgress(
            action=action,
            raw_file=raw_file,
            file_index=file_index,
            total_files=total_files,
            records_read=records_read,
            records_upserted=records_upserted,
            records_deleted=records_deleted,
        )
    )


def write_reconstructed_products(
    db_path: Path,
    output_path: Path,
    *,
    mapping: ProjectionMapping | None = None,
) -> list[CentricProductPayload]:
    payloads = reconstruct_products(db_path, mapping=mapping)
    write_jsonl(
        output_path,
        (payload.model_dump(mode="json", exclude_none=True) for payload in payloads),
    )
    return payloads


def reconstruct_products(
    db_path: Path,
    *,
    mapping: ProjectionMapping | None = None,
) -> list[CentricProductPayload]:
    with duckdb.connect(str(db_path)) as conn:
        records_by_endpoint = load_current_endpoint_records(conn)
    return reconstruct_products_from_records(records_by_endpoint, mapping=mapping)


def load_current_endpoint_records(
    conn: duckdb.DuckDBPyConnection,
) -> dict[str, list[dict[str, Any]]]:
    initialize_store(conn)
    rows = conn.execute(
        """
        SELECT endpoint, payload
        FROM endpoint_records
        ORDER BY endpoint, record_id
        """
    ).fetchall()
    records_by_endpoint: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for endpoint, payload in rows:
        records_by_endpoint[str(endpoint)].append(json.loads(payload))
    return dict(records_by_endpoint)


def discover_raw_files(raw_dir: Path) -> list[RawFile]:
    if not raw_dir.exists():
        return []
    files: list[RawFile] = []
    for path in raw_dir.rglob("*.jsonl"):
        if path.name.startswith("."):
            continue
        endpoint, is_delta = _endpoint_from_filename(path.name)
        if endpoint is None:
            continue
        manifest = _load_manifest(path.parent)
        source_run_id = _manifest_run_id(manifest) or (
            path.parent.name if path.parent != raw_dir else "root"
        )
        run_mode = _manifest_mode(manifest)
        manifest_path = path.parent / "manifest.json" if manifest is not None else None
        manifest_sha256 = _sha256(manifest_path) if manifest_path is not None else None
        is_delta = _manifest_file_is_delta(manifest, path.name, default=is_delta)
        files.append(
            RawFile(
                path=path,
                endpoint=endpoint,
                is_delta=is_delta,
                source_run_id=source_run_id,
                run_mode=run_mode,
                manifest_path=manifest_path,
                manifest_sha256=manifest_sha256,
            )
        )
    return sorted(files, key=lambda item: (_run_sort_key(item), item.endpoint, str(item.path)))


def initialize_store(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS applied_raw_files (
            file_path VARCHAR PRIMARY KEY,
            endpoint VARCHAR NOT NULL,
            source_run_id VARCHAR NOT NULL,
            is_delta BOOLEAN NOT NULL,
            record_count INTEGER NOT NULL,
            content_sha256 VARCHAR NOT NULL,
            manifest_path VARCHAR,
            manifest_sha256 VARCHAR,
            run_mode VARCHAR,
            ingested_at VARCHAR NOT NULL,
            ingested_at_ts TIMESTAMP,
            full_snapshot_mode VARCHAR
        )
        """
    )
    _ensure_column(conn, "applied_raw_files", "manifest_path", "VARCHAR")
    _ensure_column(conn, "applied_raw_files", "manifest_sha256", "VARCHAR")
    _ensure_column(conn, "applied_raw_files", "run_mode", "VARCHAR")
    _ensure_column(conn, "applied_raw_files", "ingested_at_ts", "TIMESTAMP")
    _ensure_column(conn, "applied_raw_files", "full_snapshot_mode", "VARCHAR")
    conn.execute(
        """
        UPDATE applied_raw_files
        SET ingested_at_ts = try_cast(ingested_at AS TIMESTAMP)
        WHERE ingested_at_ts IS NULL
          AND ingested_at IS NOT NULL
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS endpoint_records (
            endpoint VARCHAR NOT NULL,
            record_id VARCHAR NOT NULL,
            payload VARCHAR NOT NULL,
            modified_at VARCHAR,
            modified_at_ts TIMESTAMP,
            source_file VARCHAR NOT NULL,
            source_run_id VARCHAR NOT NULL,
            ingested_at VARCHAR NOT NULL,
            ingested_at_ts TIMESTAMP,
            PRIMARY KEY (endpoint, record_id)
        )
        """
    )
    _ensure_column(conn, "endpoint_records", "modified_at_ts", "TIMESTAMP")
    _ensure_column(conn, "endpoint_records", "ingested_at_ts", "TIMESTAMP")
    conn.execute(
        """
        UPDATE endpoint_records
        SET
            modified_at_ts = coalesce(modified_at_ts, try_cast(modified_at AS TIMESTAMP)),
            ingested_at_ts = coalesce(ingested_at_ts, try_cast(ingested_at AS TIMESTAMP))
        WHERE (modified_at_ts IS NULL AND modified_at IS NOT NULL)
           OR (ingested_at_ts IS NULL AND ingested_at IS NOT NULL)
        """
    )
    ensure_current_endpoint_views(conn)


def ensure_current_endpoint_views(
    conn: duckdb.DuckDBPyConnection,
    schemas: dict[str, EndpointSchema] | None = None,
) -> None:
    conn.execute(
        """
        CREATE OR REPLACE VIEW current_endpoint_records AS
        SELECT
            endpoint,
            record_id,
            payload,
            payload::JSON AS payload_json,
            modified_at AS modified_at_raw,
            modified_at_ts,
            source_file,
            source_run_id,
            ingested_at AS ingested_at_raw,
            ingested_at_ts
        FROM endpoint_records
        """
    )
    if schemas is None:
        return
    for endpoint in schemas:
        conn.execute(
            f"""
            CREATE OR REPLACE VIEW {_quote_identifier("current_" + endpoint)} AS
            SELECT *
            FROM current_endpoint_records
            WHERE endpoint = {_quote_literal(endpoint)}
            """,
        )


def _applied_hash(conn: duckdb.DuckDBPyConnection, path: Path) -> str | None:
    row = conn.execute(
        "SELECT content_sha256 FROM applied_raw_files WHERE file_path = ?",
        [str(path)],
    ).fetchone()
    if row is None:
        return None
    return str(row[0])


def _apply_records_for_file(
    conn: duckdb.DuckDBPyConnection,
    *,
    raw_file: RawFile,
    schema: EndpointSchema,
    ingested_at: str,
) -> tuple[int, int, int]:
    conn.execute("DROP TABLE IF EXISTS ingest_stage")
    modified_expr = _modified_at_sql_expr(schema)
    modified_ts_expr = _modified_at_ts_sql_expr(schema)
    delete_expr = _delete_sql_expr(schema)
    conn.execute(
        f"""
        CREATE TEMP TABLE ingest_stage AS
        WITH lines AS (
            SELECT
                row_number() OVER () - 1 AS row_order,
                trim(line) AS payload
            FROM (
                SELECT unnest(string_split(content, '\n')) AS line
                FROM read_text(?)
            )
            WHERE trim(line) <> ''
        ),
        extracted AS (
            SELECT
                CAST(row_order AS INTEGER) AS row_order,
                ? AS endpoint,
                json_extract_string(payload, ?) AS record_id,
                payload,
                {modified_expr} AS modified_at,
                {modified_ts_expr} AS modified_at_ts,
                ? AS source_file,
                ? AS source_run_id,
                ? AS ingested_at,
                try_cast(? AS TIMESTAMP) AS ingested_at_ts,
                coalesce({delete_expr}, false) AS is_delete
            FROM lines
        )
        SELECT *
        FROM extracted
        WHERE record_id IS NOT NULL AND trim(record_id) <> ''
        """,
        [
            str(raw_file.path),
            raw_file.endpoint,
            _json_path(schema.primary_key),
            str(raw_file.path),
            raw_file.source_run_id,
            ingested_at,
            ingested_at,
        ],
    )

    file_record_count = conn.execute("SELECT COUNT(*) FROM ingest_stage").fetchone()[0]

    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE ingest_stage_winners AS
        SELECT
            endpoint,
            record_id,
            payload,
            modified_at,
            modified_at_ts,
            source_file,
            source_run_id,
            ingested_at,
            ingested_at_ts,
            is_delete
        FROM ingest_stage
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY endpoint, record_id
            ORDER BY
                modified_at_ts IS NOT NULL DESC,
                modified_at_ts DESC NULLS LAST,
                modified_at IS NOT NULL DESC,
                modified_at DESC NULLS LAST,
                row_order DESC
        ) = 1
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE ingest_applicable AS
        SELECT winners.*
        FROM ingest_stage_winners winners
        LEFT JOIN endpoint_records existing
          ON existing.endpoint = winners.endpoint
         AND existing.record_id = winners.record_id
        WHERE existing.record_id IS NULL
           OR winners.modified_at_ts IS NULL
           OR existing.modified_at_ts IS NULL
           OR winners.modified_at_ts >= existing.modified_at_ts
        """
    )

    file_deletes = conn.execute(
        "SELECT COUNT(*) FROM ingest_applicable WHERE is_delete"
    ).fetchone()[0]
    file_upserts = conn.execute(
        "SELECT COUNT(*) FROM ingest_applicable WHERE NOT is_delete"
    ).fetchone()[0]

    conn.execute(
        """
        DELETE FROM endpoint_records
        USING ingest_applicable
        WHERE endpoint_records.endpoint = ingest_applicable.endpoint
          AND endpoint_records.record_id = ingest_applicable.record_id
        """
    )
    conn.execute(
        """
        INSERT INTO endpoint_records (
            endpoint, record_id, payload, modified_at, modified_at_ts, source_file,
            source_run_id, ingested_at, ingested_at_ts
        )
        SELECT
            endpoint, record_id, payload, modified_at, modified_at_ts, source_file,
            source_run_id, ingested_at, ingested_at_ts
        FROM ingest_applicable
        WHERE NOT is_delete
        """
    )
    return int(file_record_count), int(file_upserts), int(file_deletes)


def _modified_at_sql_expr(schema: EndpointSchema) -> str:
    fields = [_json_path(field) for field in schema.modified_at_fields]
    if not fields:
        return "NULL"
    extracts = ", ".join(f"json_extract_string(payload, '{field}')" for field in fields)
    return f"coalesce({extracts})"


def _modified_at_ts_sql_expr(schema: EndpointSchema) -> str:
    modified_expr = _modified_at_sql_expr(schema)
    if modified_expr == "NULL":
        return "NULL"
    return f"try_cast({modified_expr} AS TIMESTAMP)"


def _delete_sql_expr(schema: EndpointSchema) -> str:
    if schema.delete_field is None:
        return "false"
    delete_value = _delete_when_text(schema.delete_when)
    return f"json_extract_string(payload, '{_json_path(schema.delete_field)}') = '{delete_value}'"


def _delete_when_text(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def _json_path(field: str) -> str:
    return '$."' + field.replace('"', '\\"') + '"'


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _ensure_column(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    column_name: str,
    column_type: str,
) -> None:
    rows = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    existing_columns = {str(row[1]) for row in rows}
    if column_name not in existing_columns:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")


def _format_datetime(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _endpoint_from_filename(filename: str) -> tuple[str | None, bool]:
    if not filename.endswith(".jsonl"):
        return None, False
    stem = filename.removesuffix(".jsonl")
    is_delta = stem.endswith(".delta")
    endpoint = stem.removesuffix(".delta") if is_delta else stem
    if not endpoint:
        return None, False
    return endpoint, is_delta


def _load_manifest(run_dir: Path) -> dict[str, Any] | None:
    manifest_path = run_dir / "manifest.json"
    if not manifest_path.is_file():
        return None
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else None


def _manifest_run_id(manifest: dict[str, Any] | None) -> str | None:
    if manifest is None:
        return None
    run_id = manifest.get("run_id")
    return _clean_text(run_id)


def _manifest_mode(manifest: dict[str, Any] | None) -> str | None:
    if manifest is None:
        return None
    return _clean_text(manifest.get("mode"))


def _manifest_file_is_delta(
    manifest: dict[str, Any] | None,
    filename: str,
    *,
    default: bool,
) -> bool:
    if manifest is None:
        return default
    endpoints = manifest.get("endpoints")
    if not isinstance(endpoints, dict):
        return default
    for endpoint in endpoints.values():
        if not isinstance(endpoint, dict) or endpoint.get("file") != filename:
            continue
        is_delta = endpoint.get("is_delta")
        if isinstance(is_delta, bool):
            return is_delta
    mode = _manifest_mode(manifest)
    return mode == "delta" if mode is not None else default


def _run_sort_key(raw_file: RawFile) -> str:
    return "" if raw_file.source_run_id == "root" else raw_file.source_run_id


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _clean_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None
