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


def test_reconstruct_command_defaults_to_check_target(tmp_path, monkeypatch) -> None:
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
    assert "writing target 'check'" in result.output
    output_path = tmp_path / "data" / "results" / "reconstruction-check.jsonl"
    assert output_path.is_file()
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["style_id"] == "S1"
    assert "relationship_ids" in payload


def test_validate_and_report_default_to_reconstruction_check(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    check_path = tmp_path / "data" / "results" / "reconstruction-check.jsonl"
    check_path.parent.mkdir(parents=True)
    check_path.write_text(
        json.dumps(
            {
                "style_id": "S1",
                "relationship_ids": {"style_colorway_ids": ["C1"]},
                "counts": {
                    "relationship_ids": {"style_colorway_ids": 1},
                    "resolved_records": {"colorways": 0},
                    "unresolved_refs": 0,
                    "warnings": 0,
                },
                "applicability": {},
                "unresolved_refs": [],
                "warnings": [],
            },
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )

    validate_result = CliRunner().invoke(app, ["validate"])
    report_result = CliRunner().invoke(app, ["report"])

    assert validate_result.exit_code == 0
    assert "reading check records" in validate_result.output
    assert (tmp_path / "data" / "results" / "reconstruction-check-results.json").is_file()
    assert report_result.exit_code == 0
    assert "reading check records" in report_result.output
    summary_path = (
        tmp_path / "reports" / "reconstruction-check" / "reconstruction-check-summary.md"
    )
    assert summary_path.is_file()


def test_validate_and_report_can_use_private_target_hooks(tmp_path, monkeypatch) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    monkeypatch.setenv("CENTRIC_CONFIG_DIR", str(config_dir))
    monkeypatch.chdir(tmp_path)
    (config_dir / "reconstruction.py").write_text(
        """
from pathlib import Path

def validate_projected_products(target, payloads, *, rules=None):
    payloads = list(payloads)
    return {
        "rule_set_version": f"{target}-rules",
        "total_products": len(payloads),
        "ready_products": len(payloads),
        "readiness_percent": 100.0,
        "results": [],
    }

def report_validation_results(target, validation_result, output_dir):
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    Path(output_dir, "summary.txt").write_text(
        f"{target}:{validation_result['total_products']}",
        encoding="utf-8",
    )
""",
        encoding="utf-8",
    )
    input_path = tmp_path / "data" / "results" / "packaging-products.jsonl"
    input_path.parent.mkdir(parents=True)
    input_path.write_text(json.dumps({"style_id": "S1"}) + "\n", encoding="utf-8")

    validate_result = CliRunner().invoke(app, ["validate", "--target", "packaging"])
    report_result = CliRunner().invoke(app, ["report", "--target", "packaging"])

    assert validate_result.exit_code == 0
    assert "Validated 1 records: 1 ready" in validate_result.output
    assert (tmp_path / "data" / "results" / "packaging-results.json").is_file()
    assert report_result.exit_code == 0
    assert (tmp_path / "reports" / "packaging" / "summary.txt").read_text(
        encoding="utf-8"
    ) == "packaging:1"


def test_pipeline_requires_explicit_target(tmp_path) -> None:
    result = CliRunner().invoke(app, ["pipeline", "--raw-dir", str(tmp_path / "raw")])

    assert result.exit_code != 0
    assert "Missing option" in result.output
    assert "--target" in result.output
