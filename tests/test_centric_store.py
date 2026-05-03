import json

import duckdb

from centric_mdm_validation.centric.schema import load_endpoint_schemas
from centric_mdm_validation.centric.store import (
    ingest_raw_dir,
    load_current_endpoint_records,
    reconstruct_products,
)


def test_ingest_raw_dir_seeds_store_and_reconstructs_products(tmp_path) -> None:
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
    payloads = reconstruct_products(db_path)

    assert result.applied_files == 2
    assert result.records_upserted == 2
    assert payloads[0].centric_style_id == "S1"
    assert payloads[0].style_name == "Seed Jacket"
    assert payloads[0].variants[0].global_variant_id == "variant-global-1"


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
    payloads = reconstruct_products(db_path)

    assert first.applied_files == 2
    assert second.applied_files == 0
    assert second.skipped_files == 2
    assert payloads[0].style_name == "Updated Jacket"


def test_ingest_raw_dir_applies_active_false_as_delete(tmp_path) -> None:
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
    payloads = reconstruct_products(db_path)

    assert result.records_deleted == 1
    assert payloads == []


def test_load_current_endpoint_records_returns_endpoint_groups(tmp_path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    _write_jsonl(raw_dir / "materials.jsonl", [{"id": "M1", "node_name": "Cotton"}])
    db_path = tmp_path / "centric.duckdb"
    ingest_raw_dir(raw_dir, db_path, schemas=load_endpoint_schemas())

    with duckdb.connect(str(db_path)) as conn:
        records = load_current_endpoint_records(conn)

    assert records == {"materials": [{"id": "M1", "node_name": "Cotton"}]}


def _write_jsonl(path, records: list[dict]) -> None:
    path.write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records),
        encoding="utf-8",
    )
