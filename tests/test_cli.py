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


def test_reconstruct_command_defaults_to_master_target(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("CENTRIC_CONFIG_DIR", raising=False)
    monkeypatch.chdir(tmp_path)
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    (raw_dir / "styles.jsonl").write_text(
        json.dumps({"id": "S1", "node_name": "Seed Jacket"}, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "centric.duckdb"
    ingest_result = CliRunner().invoke(
        app,
        [
            "ingest",
            "--raw-dir",
            str(raw_dir),
            "--db",
            str(db_path),
        ],
    )

    result = CliRunner().invoke(app, ["reconstruct", "--db", str(db_path)])

    assert ingest_result.exit_code == 0
    assert result.exit_code == 0
    assert "projecting target 'master'" in result.output
    assert (tmp_path / "data" / "results" / "master-products.jsonl").is_file()


def test_pipeline_requires_explicit_target(tmp_path) -> None:
    result = CliRunner().invoke(app, ["pipeline", "--raw-dir", str(tmp_path / "raw")])

    assert result.exit_code != 0
    assert "Missing option" in result.output
    assert "--target" in result.output
