from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from centric_mdm_validation.centric.mapper import ProjectionMapping, project_products
from centric_mdm_validation.centric.schema import EndpointSchema
from centric_mdm_validation.io import read_json_records, write_jsonl
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


def ingest_raw_dir(
    raw_dir: Path,
    db_path: Path,
    *,
    schemas: dict[str, EndpointSchema],
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
        for raw_file in raw_files:
            content_hash = _sha256(raw_file.path)
            applied_hash = _applied_hash(conn, raw_file.path)
            if applied_hash == content_hash:
                skipped_files += 1
                continue
            if applied_hash is not None and applied_hash != content_hash:
                raise ValueError(
                    f"Raw file changed after ingest: {raw_file.path}. "
                    "Raw evidence files are expected to be immutable."
                )

            schema = schemas.get(raw_file.endpoint, EndpointSchema(name=raw_file.endpoint))
            records = read_json_records(raw_file.path)
            ingested_at = _format_datetime(datetime.now(UTC))

            conn.execute("BEGIN TRANSACTION")
            try:
                file_upserts = 0
                file_deletes = 0
                for record in records:
                    if not isinstance(record, dict):
                        continue
                    record_id = _clean_text(record.get(schema.primary_key))
                    if record_id is None:
                        continue
                    if _is_delete_record(record, schema):
                        if _should_apply_record(conn, raw_file.endpoint, record_id, record, schema):
                            conn.execute(
                                "DELETE FROM endpoint_records WHERE endpoint = ? AND record_id = ?",
                                [raw_file.endpoint, record_id],
                            )
                            file_deletes += 1
                        continue
                    if not _should_apply_record(conn, raw_file.endpoint, record_id, record, schema):
                        continue

                    modified_at = _format_optional_datetime(_record_modified_at(record, schema))
                    payload = json.dumps(record, default=str, separators=(",", ":"))
                    conn.execute(
                        "DELETE FROM endpoint_records WHERE endpoint = ? AND record_id = ?",
                        [raw_file.endpoint, record_id],
                    )
                    conn.execute(
                        """
                        INSERT INTO endpoint_records (
                            endpoint, record_id, payload, modified_at, source_file,
                            source_run_id, ingested_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            raw_file.endpoint,
                            record_id,
                            payload,
                            modified_at,
                            str(raw_file.path),
                            raw_file.source_run_id,
                            ingested_at,
                        ],
                    )
                    file_upserts += 1

                conn.execute(
                    """
                    INSERT INTO applied_raw_files (
                        file_path, endpoint, source_run_id, is_delta, record_count,
                        content_sha256, manifest_path, manifest_sha256, run_mode, ingested_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        str(raw_file.path),
                        raw_file.endpoint,
                        raw_file.source_run_id,
                        raw_file.is_delta,
                        len(records),
                        content_hash,
                        str(raw_file.manifest_path) if raw_file.manifest_path is not None else None,
                        raw_file.manifest_sha256,
                        raw_file.run_mode,
                        ingested_at,
                    ],
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

            applied_files += 1
            records_read += len(records)
            records_upserted += file_upserts
            records_deleted += file_deletes
            endpoints[raw_file.endpoint] += len(records)

    return IngestResult(
        applied_files=applied_files,
        skipped_files=skipped_files,
        records_read=records_read,
        records_upserted=records_upserted,
        records_deleted=records_deleted,
        endpoints=dict(sorted(endpoints.items())),
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
    return project_products(records_by_endpoint, mapping=mapping)


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
            ingested_at VARCHAR NOT NULL
        )
        """
    )
    _ensure_column(conn, "applied_raw_files", "manifest_path", "VARCHAR")
    _ensure_column(conn, "applied_raw_files", "manifest_sha256", "VARCHAR")
    _ensure_column(conn, "applied_raw_files", "run_mode", "VARCHAR")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS endpoint_records (
            endpoint VARCHAR NOT NULL,
            record_id VARCHAR NOT NULL,
            payload VARCHAR NOT NULL,
            modified_at VARCHAR,
            source_file VARCHAR NOT NULL,
            source_run_id VARCHAR NOT NULL,
            ingested_at VARCHAR NOT NULL,
            PRIMARY KEY (endpoint, record_id)
        )
        """
    )


def _applied_hash(conn: duckdb.DuckDBPyConnection, path: Path) -> str | None:
    row = conn.execute(
        "SELECT content_sha256 FROM applied_raw_files WHERE file_path = ?",
        [str(path)],
    ).fetchone()
    if row is None:
        return None
    return str(row[0])


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


def _should_apply_record(
    conn: duckdb.DuckDBPyConnection,
    endpoint: str,
    record_id: str,
    record: dict[str, Any],
    schema: EndpointSchema,
) -> bool:
    row = conn.execute(
        """
        SELECT modified_at, ingested_at
        FROM endpoint_records
        WHERE endpoint = ? AND record_id = ?
        """,
        [endpoint, record_id],
    ).fetchone()
    if row is None:
        return True

    incoming_modified_at = _record_modified_at(record, schema)
    existing_modified_at = _parse_datetime(row[0])
    if incoming_modified_at is None or existing_modified_at is None:
        return True
    return incoming_modified_at >= existing_modified_at


def _record_modified_at(record: dict[str, Any], schema: EndpointSchema) -> datetime | None:
    for field in schema.modified_at_fields:
        parsed = _parse_datetime(record.get(field))
        if parsed is not None:
            return parsed
    return None


def _is_delete_record(record: dict[str, Any], schema: EndpointSchema) -> bool:
    if schema.delete_field is None:
        return False
    if schema.delete_field not in record:
        return False
    return record.get(schema.delete_field) == schema.delete_when


def _parse_datetime(value: Any) -> datetime | None:
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


def _format_datetime(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _format_optional_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _format_datetime(value)


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
