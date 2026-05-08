import json

import duckdb

from centric_mdm_validation.centric.schema import (
    DeleteCondition,
    EndpointSchema,
    load_endpoint_schemas,
)
from centric_mdm_validation.centric.store import (
    ingest_raw_dir,
    load_current_endpoint_records,
    rebuild_master_reconstruction,
)


def test_ingest_raw_dir_seeds_store_and_reconstructs_master(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("CENTRIC_CONFIG_DIR", raising=False)
    monkeypatch.chdir(tmp_path)

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    _write_jsonl(
        raw_dir / "styles.jsonl",
        [
            {
                "id": "S1",
                "_modified_at": "2026-04-29T09:00:00Z",
                "active": True,
                "node_name": "Seed Jacket",
                "brand_code": "BR",
                "product_type": "JACKET",
            }
        ],
    )
    _write_jsonl(
        raw_dir / "colorways.jsonl",
        [
            {
                "id": "C1",
                "_modified_at": "2026-04-29T09:00:00Z",
                "active": True,
                "style": "S1",
                "code": "001",
                "sys_id": "variant-global-1",
            }
        ],
    )
    db_path = tmp_path / "centric.duckdb"

    result = ingest_raw_dir(raw_dir, db_path, schemas=load_endpoint_schemas())
    master_result = rebuild_master_reconstruction(db_path)

    assert result.applied_files == 2
    assert result.records_upserted == 2
    assert master_result.products_reconstructed == 1
    assert master_result.source_refs == 1


def test_ingest_raw_dir_applies_delta_once_and_keeps_newest_record(tmp_path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    _write_jsonl(
        raw_dir / "styles.jsonl",
        [
            {
                "id": "S1",
                "_modified_at": "2026-04-29T09:00:00Z",
                "active": True,
                "node_name": "Seed Jacket",
                "brand_code": "BR",
            }
        ],
    )
    run_dir = raw_dir / "runs" / "2026-04-30T090000Z"
    run_dir.mkdir(parents=True)
    _write_jsonl(
        run_dir / "styles.delta.jsonl",
        [
            {
                "id": "S1",
                "_modified_at": "2026-04-30T09:00:00Z",
                "active": True,
                "node_name": "Updated Jacket",
                "brand_code": "BR",
            }
        ],
    )
    db_path = tmp_path / "centric.duckdb"

    first = ingest_raw_dir(raw_dir, db_path, schemas=load_endpoint_schemas())
    second = ingest_raw_dir(raw_dir, db_path, schemas=load_endpoint_schemas())

    assert first.applied_files == 2
    assert second.applied_files == 0
    assert second.skipped_files == 2
    with duckdb.connect(str(db_path)) as conn:
        records = load_current_endpoint_records(conn)
    assert records["styles"][0]["node_name"] == "Updated Jacket"


def test_ingest_raw_dir_dedupes_records_within_file_by_modified_at(tmp_path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    _write_jsonl(
        raw_dir / "styles.jsonl",
        [
            {
                "id": "S1",
                "_modified_at": "2026-04-30T09:00:00Z",
                "active": True,
                "node_name": "Newer Style",
                "brand_code": "BR",
            },
            {
                "id": "S1",
                "_modified_at": "2026-04-29T09:00:00Z",
                "active": True,
                "node_name": "Older Style",
                "brand_code": "BR",
            },
        ],
    )
    db_path = tmp_path / "centric.duckdb"

    result = ingest_raw_dir(raw_dir, db_path, schemas=load_endpoint_schemas())
    with duckdb.connect(str(db_path)) as conn:
        records = load_current_endpoint_records(conn)

    assert result.records_upserted == 1
    assert records["styles"][0]["node_name"] == "Newer Style"


def test_ingest_raw_dir_stores_typed_timestamps_and_current_views(tmp_path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    _write_jsonl(
        raw_dir / "styles.jsonl",
        [
            {
                "id": "S1",
                "_modified_at": "2026-04-30T09:00:00Z",
                "active": True,
                "node_name": "Typed Style",
            }
        ],
    )
    db_path = tmp_path / "centric.duckdb"

    ingest_raw_dir(raw_dir, db_path, schemas=load_endpoint_schemas())

    with duckdb.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            SELECT record_id, modified_at_raw, modified_at_ts, ingested_at_ts
            FROM current_styles
            WHERE record_id = 'S1'
            """
        ).fetchone()

    assert row[0] == "S1"
    assert row[1] == "2026-04-30T09:00:00Z"
    assert row[2].isoformat() == "2026-04-30T09:00:00"
    assert row[3] is not None


def test_ingest_raw_dir_applies_active_false_as_delete(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("CENTRIC_CONFIG_DIR", raising=False)
    monkeypatch.chdir(tmp_path)

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    _write_jsonl(
        raw_dir / "styles.jsonl",
        [
            {
                "id": "S1",
                "_modified_at": "2026-04-29T09:00:00Z",
                "active": True,
                "node_name": "Seed Jacket",
                "brand_code": "BR",
            }
        ],
    )
    run_dir = raw_dir / "runs" / "2026-04-30T090000Z"
    run_dir.mkdir(parents=True)
    _write_jsonl(
        run_dir / "styles.delta.jsonl",
        [{"id": "S1", "_modified_at": "2026-04-30T09:00:00Z", "active": False}],
    )
    db_path = tmp_path / "centric.duckdb"

    result = ingest_raw_dir(raw_dir, db_path, schemas=load_endpoint_schemas())
    master_result = rebuild_master_reconstruction(db_path)

    assert result.records_deleted == 1
    assert master_result.products_reconstructed == 0


def test_ingest_raw_dir_applies_delete_when_any_conditions(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("CENTRIC_CONFIG_DIR", raising=False)
    monkeypatch.chdir(tmp_path)

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    _write_jsonl(
        raw_dir / "styles.jsonl",
        [
            {
                "id": "S1",
                "_modified_at": "2026-04-29T09:00:00Z",
                "active": True,
                "node_name": "Seed Jacket",
            },
            {
                "id": "S2",
                "_modified_at": "2026-04-29T09:00:00Z",
                "active": True,
                "node_name": "Seed Shirt",
            },
        ],
    )
    run_dir = raw_dir / "runs" / "2026-04-30T090000Z"
    run_dir.mkdir(parents=True)
    _write_jsonl(
        run_dir / "styles.delta.jsonl",
        [
            {"id": "S1", "_modified_at": "2026-04-30T09:00:00Z", "active": False},
            {"id": "S2", "_modified_at": "2026-04-30T09:00:00Z", "state": "ABANDONED"},
        ],
    )
    db_path = tmp_path / "centric.duckdb"

    schemas = {
        "styles": EndpointSchema(
            name="styles",
            delete_when_any=(
                DeleteCondition(field="active", equals=False),
                DeleteCondition(field="state", equals="ABANDONED"),
            ),
        )
    }

    result = ingest_raw_dir(raw_dir, db_path, schemas=schemas)

    with duckdb.connect(str(db_path)) as conn:
        remaining = conn.execute("SELECT COUNT(*) FROM current_styles").fetchone()[0]

    assert result.records_deleted == 2
    assert remaining == 0


def test_ingest_raw_dir_rejects_unimplemented_full_snapshot_mode(tmp_path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    _write_jsonl(raw_dir / "styles.jsonl", [{"id": "S1", "active": True}])
    db_path = tmp_path / "centric.duckdb"

    schemas = {
        "styles": EndpointSchema(
            name="styles",
            full_snapshot_mode="replace_endpoint_scope",
        )
    }

    try:
        ingest_raw_dir(raw_dir, db_path, schemas=schemas)
    except ValueError as exc:
        assert "replace_endpoint_scope" in str(exc)
    else:
        raise AssertionError("Expected unsupported full_snapshot_mode to fail")


def test_load_current_endpoint_records_returns_endpoint_groups(tmp_path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    _write_jsonl(raw_dir / "materials.jsonl", [{"id": "M1", "node_name": "Cotton"}])
    db_path = tmp_path / "centric.duckdb"
    ingest_raw_dir(raw_dir, db_path, schemas=load_endpoint_schemas())

    with duckdb.connect(str(db_path)) as conn:
        records = load_current_endpoint_records(conn)

    assert records == {"materials": [{"id": "M1", "node_name": "Cotton"}]}


def test_rebuild_master_reconstruction_writes_master_tables(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("CENTRIC_CONFIG_DIR", raising=False)
    monkeypatch.chdir(tmp_path)

    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    _write_jsonl(
        raw_dir / "styles.jsonl",
        [{"id": "S1", "node_name": "Master Style", "brand_code": "BR"}],
    )
    db_path = tmp_path / "centric.duckdb"
    ingest_raw_dir(raw_dir, db_path, schemas=load_endpoint_schemas())

    result = rebuild_master_reconstruction(db_path)

    with duckdb.connect(str(db_path)) as conn:
        product_row = conn.execute(
            """
            SELECT product_id, brand_code, graph
            FROM current_reconstructed_products
            WHERE product_id = 'S1'
            """
        ).fetchone()
        source_count = conn.execute(
            """
            SELECT count(*)
            FROM reconstruction_source_refs
            WHERE product_id = 'S1'
            """
        ).fetchone()[0]

    assert result.products_reconstructed == 1
    assert product_row[0] == "S1"
    assert product_row[1] == "BR"
    assert product_row[2] is not None
    assert source_count == 1


def test_ingest_raw_dir_records_manifest_metadata(tmp_path) -> None:
    raw_dir = tmp_path / "raw"
    run_dir = raw_dir / "runs" / "2026-05-03T102233Z-months2"
    run_dir.mkdir(parents=True)
    _write_jsonl(run_dir / "styles.jsonl", [{"id": "S1", "node_name": "Window Style"}])
    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "2026-05-03T102233Z-months2",
                "mode": "months",
                "endpoints": {
                    "styles": {
                        "file": "styles.jsonl",
                        "is_delta": False,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    db_path = tmp_path / "centric.duckdb"

    ingest_raw_dir(raw_dir, db_path, schemas=load_endpoint_schemas())

    with duckdb.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            SELECT
                source_run_id,
                is_delta,
                run_mode,
                manifest_path,
                manifest_sha256,
                ingested_at_ts,
                full_snapshot_mode
            FROM applied_raw_files
            WHERE endpoint = 'styles'
            """
        ).fetchone()

    assert row[0] == "2026-05-03T102233Z-months2"
    assert row[1] is False
    assert row[2] == "months"
    assert row[3].endswith("manifest.json")
    assert isinstance(row[4], str)
    assert len(row[4]) == 64
    assert row[5] is not None
    assert row[6] == "upsert_only"


def _write_jsonl(path, records: list[dict]) -> None:
    path.write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records),
        encoding="utf-8",
    )
