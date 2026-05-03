import json

from typer.testing import CliRunner

from centric_mdm_validation.cli import app


def test_ingest_command_prints_file_progress(tmp_path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    (raw_dir / "styles.jsonl").write_text(
        json.dumps(
            {
                "id": "S1",
                "_modified_at": "2026-04-30T09:00:00Z",
                "active": True,
                "node_name": "Seed Jacket",
            },
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "centric.duckdb"

    result = CliRunner().invoke(
        app,
        [
            "ingest",
            "--raw-dir",
            str(raw_dir),
            "--db",
            str(db_path),
        ],
    )

    assert result.exit_code == 0
    assert "-> Ingest: discovered 1 raw JSONL files" in result.output
    assert "Ingest: [1/1] applying styles" in result.output
    assert "Ingest: [1/1] applied styles: 1 records, 1 upserts, 0 deletes" in result.output
    assert "OK Ingested 1 raw files" in result.output

