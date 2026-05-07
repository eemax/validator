import json

from typer.testing import CliRunner

from centric_mdm_validation.cli import app


def test_top_level_help_shows_workflow_and_targets() -> None:
    result = CliRunner().invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "raw endpoint files" in result.output
    assert "check/dpp/md records" in result.output
    assert "centric-mdm examples" in result.output
    assert "delta-daemon" in result.output


def test_examples_command_prints_common_workflows() -> None:
    result = CliRunner().invoke(app, ["examples"])

    assert result.exit_code == 0
    assert "Default aggregate check" in result.output
    assert "pipeline --target dpp" in result.output
    assert "pipeline --target md" in result.output
    assert "delta-daemon --schedule" in result.output


def test_delta_daemon_invalid_schedule_prints_cron_guidance() -> None:
    result = CliRunner().invoke(app, ["delta-daemon", "--schedule", "hourly"])

    assert result.exit_code == 1
    assert "5-field cron syntax" in result.output
    assert "hourly:       0 * * * *" in result.output


def test_fetch_help_routes_to_fetcher_options() -> None:
    result = CliRunner().invoke(app, ["fetch", "--help"])

    assert result.exit_code == 0
    assert "usage: centric-mdm fetch run" in result.output
    assert "--config" in result.output
    assert "--delta" in result.output
    assert "--days" in result.output
    assert "--json" in result.output


def test_fetch_without_args_uses_default_config(monkeypatch) -> None:
    captured_args = []

    def fake_fetcher_main(args):
        captured_args.extend(args)
        return 0

    monkeypatch.setattr("centric_mdm_validation.cli.fetcher_main", fake_fetcher_main)

    result = CliRunner().invoke(app, ["fetch"])

    assert result.exit_code == 0
    assert captured_args == ["run"]


def test_fetch_caffeinate_wraps_fetch_on_macos(monkeypatch) -> None:
    captured = {}

    monkeypatch.setattr("centric_mdm_validation.cli.platform.system", lambda: "Darwin")
    monkeypatch.setattr("centric_mdm_validation.cli.shutil.which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr("centric_mdm_validation.cli.sys.executable", "/python")

    def fake_call(command, env):
        captured["command"] = command
        captured["env"] = env
        return 0

    monkeypatch.setattr("centric_mdm_validation.cli.subprocess.call", fake_call)

    result = CliRunner().invoke(app, ["fetch", "--delta", "--caffeinate"])

    assert result.exit_code == 0
    assert captured["command"] == [
        "/usr/bin/caffeinate",
        "-i",
        "/python",
        "-m",
        "centric_mdm_validation.cli",
        "fetch",
        "--delta",
    ]
    assert captured["env"]["CENTRIC_MDM_CAFFEINATED"] == "1"


def test_fetch_caffeinate_does_not_recurse(monkeypatch) -> None:
    captured_args = []

    def fake_fetcher_main(args):
        captured_args.extend(args)
        return 0

    monkeypatch.setenv("CENTRIC_MDM_CAFFEINATED", "1")
    monkeypatch.setattr("centric_mdm_validation.cli.fetcher_main", fake_fetcher_main)

    result = CliRunner().invoke(app, ["fetch", "--caffeinate", "--delta"])

    assert result.exit_code == 0
    assert captured_args == ["run", "--delta"]


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

    second_result = CliRunner().invoke(
        app,
        [
            "ingest",
            "--raw-dir",
            str(raw_dir),
            "--db",
            str(db_path),
        ],
    )

    assert second_result.exit_code == 0
    assert "skipped already-applied" not in second_result.output
    assert "OK Ingested 0 raw files" in second_result.output


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
    assert "Check: measuring aggregate endpoint coverage" in result.output
    output_path = tmp_path / "data" / "results" / "reconstruction-check-results.json"
    assert output_path.is_file()
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["summary"]["styles"] == 1
    assert "relationship_coverage" in payload


def test_validate_and_report_default_to_reconstruction_check(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    check_path = tmp_path / "data" / "results" / "reconstruction-check-results.json"
    check_path.parent.mkdir(parents=True)
    check_path.write_text(
        json.dumps(
            {
                "context": "reconstruction_check",
                "rule_set_version": "reconstruction-check-coverage-v1",
                "total_products": 1,
                "ready_products": 0,
                "readiness_percent": 0.0,
                "summary": {
                    "styles": 1,
                    "endpoints": 2,
                    "relationships_checked": 1,
                    "declared_refs": 1,
                    "seen_refs": 0,
                    "missing_refs": 1,
                    "invalid_refs": 0,
                    "coverage_percent": 0.0,
                },
                "relationship_coverage": [
                    {
                        "relationship": "style_colorways",
                        "source_endpoint": "styles",
                        "target_endpoint": "colorways",
                        "source_records": 1,
                        "source_records_with_refs": 1,
                        "source_records_with_missing_refs": 1,
                        "declared_refs": 1,
                        "seen_refs": 0,
                        "missing_refs": 1,
                        "invalid_refs": 0,
                        "coverage_percent": 0.0,
                    }
                ],
                "endpoint_coverage": [
                    {"endpoint": "styles", "records": 1},
                    {"endpoint": "colorways", "records": 0},
                ],
                "unresolved_refs": [
                    {
                        "relationship": "style_colorways",
                        "target_endpoint": "colorways",
                        "status": "not_seen",
                        "count": 1,
                    }
                ],
                "issue_counts": [
                    {
                        "code": "REFERENCED_RECORD_NOT_SEEN",
                        "severity": "error",
                        "bucket": "style_colorways",
                        "count": 1,
                    }
                ],
                "results": [],
            },
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )

    validate_result = CliRunner().invoke(app, ["validate"])
    report_result = CliRunner().invoke(app, ["report"])

    assert validate_result.exit_code == 0
    assert "reading check records" in validate_result.output
    assert (tmp_path / "data" / "results" / "reconstruction-check-results.json").is_file()
    assert report_result.exit_code == 0
    assert "reading check records" in report_result.output
    summary_path = tmp_path / "reports" / "reconstruction-check" / "reconstruction-check-summary.md"
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


def test_report_accepts_existing_validation_result_json(tmp_path, monkeypatch) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    monkeypatch.setenv("CENTRIC_CONFIG_DIR", str(config_dir))
    monkeypatch.chdir(tmp_path)
    (config_dir / "reconstruction.py").write_text(
        """
from pathlib import Path

def validate_projected_products(target, payloads, *, rules=None):
    raise AssertionError("report should not re-validate result JSON")

def report_validation_results(target, validation_result, output_dir):
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    Path(output_dir, "summary.txt").write_text(
        f"{target}:{validation_result['total_products']}",
        encoding="utf-8",
    )
""",
        encoding="utf-8",
    )
    result_path = tmp_path / "data" / "results" / "packaging-results.json"
    result_path.parent.mkdir(parents=True)
    result_path.write_text(
        json.dumps(
            {
                "rule_set_version": "packaging-rules",
                "total_products": 7,
                "ready_products": 5,
                "readiness_percent": 71.43,
                "results": [],
            }
        ),
        encoding="utf-8",
    )

    report_result = CliRunner().invoke(app, ["report", "--target", "packaging"])

    assert report_result.exit_code == 0
    assert (tmp_path / "reports" / "packaging" / "summary.txt").read_text(
        encoding="utf-8"
    ) == "packaging:7"


def test_pipeline_requires_explicit_target(tmp_path) -> None:
    result = CliRunner().invoke(app, ["pipeline", "--raw-dir", str(tmp_path / "raw")])

    assert result.exit_code != 0
    assert "Pipeline needs an explicit target" in result.output
    assert "pipeline --target check" in result.output
    assert "--target" in result.output


def test_validate_missing_default_input_prints_guidance(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    result = CliRunner().invoke(app, ["validate"])

    assert result.exit_code == 2
    assert "Input file not found" in result.output
    assert "uv run centric-mdm reconstruct" in result.output
    assert "uv run centric-mdm pipeline --target check" in result.output


def test_pipeline_supports_public_check_target(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("CENTRIC_CONFIG_DIR", raising=False)
    monkeypatch.chdir(tmp_path)
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    (raw_dir / "styles.jsonl").write_text(
        json.dumps({"id": "S1", "node_name": "Seed Jacket"}, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "pipeline",
            "--target",
            "check",
            "--raw-dir",
            str(raw_dir),
            "--db",
            str(tmp_path / "centric.duckdb"),
        ],
    )

    assert result.exit_code == 0
    assert "Pipeline: checking aggregate endpoint coverage" in result.output
    assert "Validated" not in result.output
    assert (tmp_path / "data" / "results" / "reconstruction-check-results.json").is_file()
    assert (tmp_path / "reports" / "reconstruction-check" / "reconstruction-check.xlsx").is_file()


def test_pipeline_uses_registry_defaults_for_private_targets(tmp_path, monkeypatch) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    monkeypatch.setenv("CENTRIC_CONFIG_DIR", str(config_dir))
    monkeypatch.chdir(tmp_path)
    (config_dir / "reconstruction.py").write_text(
        """
from pathlib import Path

def reconstruct_target_records(target, records_by_endpoint):
    return [{"target": target, "style_id": style["id"]} for style in records_by_endpoint["styles"]]

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
    Path(output_dir, "summary.txt").write_text(target, encoding="utf-8")
""",
        encoding="utf-8",
    )
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    (raw_dir / "styles.jsonl").write_text(
        json.dumps({"id": "S1", "node_name": "Seed Jacket"}, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "pipeline",
            "--target",
            "md",
            "--raw-dir",
            str(raw_dir),
            "--db",
            str(tmp_path / "centric.duckdb"),
        ],
    )

    assert result.exit_code == 0
    assert "Pipeline: building md records" in result.output
    assert (tmp_path / "data" / "results" / "md-products.jsonl").is_file()
    assert (tmp_path / "data" / "results" / "md-results.json").is_file()
    assert (tmp_path / "reports" / "md-readiness" / "summary.txt").read_text(
        encoding="utf-8"
    ) == "md"


def test_pipeline_report_output_dir_overrides_registered_default(
    tmp_path,
    monkeypatch,
) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    monkeypatch.setenv("CENTRIC_CONFIG_DIR", str(config_dir))
    monkeypatch.chdir(tmp_path)
    (config_dir / "reconstruction.py").write_text(
        """
from pathlib import Path

def reconstruct_target_records(target, records_by_endpoint):
    return [{"target": target, "style_id": style["id"]} for style in records_by_endpoint["styles"]]

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
    Path(output_dir, "summary.txt").write_text(target, encoding="utf-8")
""",
        encoding="utf-8",
    )
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    (raw_dir / "styles.jsonl").write_text(
        json.dumps({"id": "S1", "node_name": "Seed Jacket"}, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "pipeline",
            "--target",
            "md",
            "--raw-dir",
            str(raw_dir),
            "--db",
            str(tmp_path / "centric.duckdb"),
            "--report-output-dir",
            str(tmp_path / "custom-reports"),
        ],
    )

    assert result.exit_code == 0
    assert (tmp_path / "custom-reports" / "summary.txt").read_text(encoding="utf-8") == "md"
