import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

from centric_mdm_validation.centric import cli
from centric_mdm_validation.centric.models import (
    AuthSettings,
    EndpointSpec,
    FetcherConfig,
    FetchProgressEvent,
    FetchRunResult,
)


def test_window_fetch_writes_to_run_directory(tmp_path, monkeypatch) -> None:
    captured_output_dirs: list[Path] = []

    def fake_load_fetcher_settings(*args, **kwargs):
        return (
            FetcherConfig(
                output_dir=tmp_path / "raw",
                checkpoint_dir=tmp_path / "checkpoints",
            ),
            AuthSettings(),
            [EndpointSpec(name="styles", api_version="v2", path="styles")],
        )

    @contextmanager
    def fake_auth_context(*args, **kwargs):
        yield SimpleNamespace(base_url="https://centric.example.com", timeout=30.0)

    def fake_run_endpoint(spec, auth_ctx, fetcher_cfg, **kwargs):
        captured_output_dirs.append(fetcher_cfg.output_dir)
        return FetchRunResult(
            endpoint=spec.name,
            pages_fetched=0,
            items_fetched=0,
            expected_count=0,
            retries_used=0,
            start_skip=0,
            next_skip=0,
            duration_seconds=0.0,
            output_file=fetcher_cfg.output_dir / "styles.jsonl",
            checkpoint_file=fetcher_cfg.checkpoint_dir / "styles.json",
            count_validation_status="passed",
            id_validation_status="passed",
        )

    monkeypatch.setattr(cli, "load_fetcher_settings", fake_load_fetcher_settings)
    monkeypatch.setattr(cli, "init_auth_context", fake_auth_context)
    monkeypatch.setattr(cli, "run_endpoint", fake_run_endpoint)

    exit_code = cli.main(["run", "--days", "45", "--quiet"])

    assert exit_code == 0
    assert len(captured_output_dirs) == 1
    output_dir = captured_output_dirs[0]
    assert output_dir.parent == tmp_path / "raw" / "runs"
    assert output_dir.name.endswith("-days45")
    manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["run_id"] == output_dir.name
    assert manifest["mode"] == "days"
    assert manifest["modified_since"] is not None
    assert manifest["endpoints"]["styles"]["file"] == "styles.jsonl"
    assert manifest["endpoints"]["styles"]["is_delta"] is False


def test_months_fetch_still_writes_month_run_directory(tmp_path, monkeypatch) -> None:
    captured_output_dirs: list[Path] = []

    def fake_load_fetcher_settings(*args, **kwargs):
        return (
            FetcherConfig(
                output_dir=tmp_path / "raw",
                checkpoint_dir=tmp_path / "checkpoints",
            ),
            AuthSettings(),
            [EndpointSpec(name="styles", api_version="v2", path="styles")],
        )

    @contextmanager
    def fake_auth_context(*args, **kwargs):
        yield SimpleNamespace(base_url="https://centric.example.com", timeout=30.0)

    def fake_run_endpoint(spec, auth_ctx, fetcher_cfg, **kwargs):
        captured_output_dirs.append(fetcher_cfg.output_dir)
        return FetchRunResult(
            endpoint=spec.name,
            pages_fetched=0,
            items_fetched=0,
            expected_count=0,
            retries_used=0,
            start_skip=0,
            next_skip=0,
            duration_seconds=0.0,
            output_file=fetcher_cfg.output_dir / "styles.jsonl",
            checkpoint_file=fetcher_cfg.checkpoint_dir / "styles.json",
            count_validation_status="passed",
            id_validation_status="passed",
        )

    monkeypatch.setattr(cli, "load_fetcher_settings", fake_load_fetcher_settings)
    monkeypatch.setattr(cli, "init_auth_context", fake_auth_context)
    monkeypatch.setattr(cli, "run_endpoint", fake_run_endpoint)

    exit_code = cli.main(["run", "--months", "2", "--quiet"])

    assert exit_code == 0
    output_dir = captured_output_dirs[0]
    assert output_dir.name.endswith("-months2")
    manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["mode"] == "months"


def test_days_and_months_are_mutually_exclusive(capsys) -> None:
    exit_code = cli.main(["run", "--days", "45", "--months", "2", "--quiet"])

    assert exit_code == 1
    assert "Use either --days or --months, not both." in capsys.readouterr().err


def test_no_params_disables_auto_fetch_params(monkeypatch) -> None:
    captured_params_paths = []

    def fake_resolve_fetch_params_path(*args, **kwargs):  # pragma: no cover - should not run
        raise AssertionError("resolve_fetch_params_path should not be called")

    def fake_load_fetcher_settings(config_path, **kwargs):
        captured_params_paths.append(kwargs.get("params_path"))
        return (
            FetcherConfig(),
            AuthSettings(),
            [],
        )

    @contextmanager
    def fake_auth_context(*args, **kwargs):
        yield SimpleNamespace(base_url="https://centric.example.com", timeout=30.0)

    monkeypatch.setattr(cli, "resolve_fetch_params_path", fake_resolve_fetch_params_path)
    monkeypatch.setattr(cli, "load_fetcher_settings", fake_load_fetcher_settings)
    monkeypatch.setattr(cli, "init_auth_context", fake_auth_context)

    exit_code = cli.main(["run", "--no-params", "--quiet"])

    assert exit_code == 0
    assert captured_params_paths == [None]


def test_params_and_no_params_are_mutually_exclusive(capsys) -> None:
    exit_code = cli.main(["run", "--params", "private.yml", "--no-params", "--quiet"])

    assert exit_code == 1
    assert "Use either --params or --no-params, not both." in capsys.readouterr().err


def test_run_uses_default_fetcher_config(monkeypatch) -> None:
    captured_config_paths = []

    def fake_load_fetcher_settings(config_path, **kwargs):
        captured_config_paths.append(config_path)
        return (
            FetcherConfig(),
            AuthSettings(),
            [],
        )

    @contextmanager
    def fake_auth_context(*args, **kwargs):
        yield SimpleNamespace(base_url="https://centric.example.com", timeout=30.0)

    monkeypatch.setattr(cli, "load_fetcher_settings", fake_load_fetcher_settings)
    monkeypatch.setattr(cli, "init_auth_context", fake_auth_context)

    exit_code = cli.main(["run", "--quiet"])

    assert exit_code == 0
    assert captured_config_paths == ["config/fetcher.yml"]


def test_fetch_prints_human_summary_by_default(tmp_path, monkeypatch, capsys) -> None:
    def fake_load_fetcher_settings(*args, **kwargs):
        return (
            FetcherConfig(
                output_dir=tmp_path / "raw",
                checkpoint_dir=tmp_path / "checkpoints",
            ),
            AuthSettings(),
            [EndpointSpec(name="styles", api_version="v2", path="styles")],
        )

    @contextmanager
    def fake_auth_context(*args, **kwargs):
        yield SimpleNamespace(base_url="https://centric.example.com", timeout=30.0)

    def fake_run_endpoint(spec, auth_ctx, fetcher_cfg, **kwargs):
        return FetchRunResult(
            endpoint=spec.name,
            pages_fetched=2,
            items_fetched=75,
            expected_count=75,
            retries_used=0,
            start_skip=0,
            next_skip=100,
            duration_seconds=1.25,
            output_file=fetcher_cfg.output_dir / "styles.jsonl",
            checkpoint_file=fetcher_cfg.checkpoint_dir / "styles.json",
            count_validation_status="passed",
            id_validation_status="passed",
            id_validation_checked_items=75,
            id_validation_unique_ids=75,
        )

    monkeypatch.setattr(cli, "load_fetcher_settings", fake_load_fetcher_settings)
    monkeypatch.setattr(cli, "init_auth_context", fake_auth_context)
    monkeypatch.setattr(cli, "run_endpoint", fake_run_endpoint)

    exit_code = cli.main(["run", "--days", "1"])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Fetch Complete" in output
    assert "Mode: days window (1 day)" in output
    assert "Raw:" in output
    assert "Endpoint" in output
    assert "styles" in output
    assert "75" in output
    assert not output.lstrip().startswith("{")


def test_fetch_json_flag_prints_jsonl_results(tmp_path, monkeypatch, capsys) -> None:
    def fake_load_fetcher_settings(*args, **kwargs):
        return (
            FetcherConfig(
                output_dir=tmp_path / "raw",
                checkpoint_dir=tmp_path / "checkpoints",
            ),
            AuthSettings(),
            [EndpointSpec(name="styles", api_version="v2", path="styles")],
        )

    @contextmanager
    def fake_auth_context(*args, **kwargs):
        yield SimpleNamespace(base_url="https://centric.example.com", timeout=30.0)

    def fake_run_endpoint(spec, auth_ctx, fetcher_cfg, **kwargs):
        return FetchRunResult(
            endpoint=spec.name,
            pages_fetched=1,
            items_fetched=25,
            expected_count=25,
            retries_used=0,
            start_skip=0,
            next_skip=50,
            duration_seconds=0.75,
            output_file=fetcher_cfg.output_dir / "styles.jsonl",
            checkpoint_file=fetcher_cfg.checkpoint_dir / "styles.json",
            count_validation_status="passed",
            id_validation_status="passed",
            id_validation_checked_items=25,
            id_validation_unique_ids=25,
        )

    monkeypatch.setattr(cli, "load_fetcher_settings", fake_load_fetcher_settings)
    monkeypatch.setattr(cli, "init_auth_context", fake_auth_context)
    monkeypatch.setattr(cli, "run_endpoint", fake_run_endpoint)

    exit_code = cli.main(["run", "--days", "1", "--json", "--quiet"])

    lines = capsys.readouterr().out.splitlines()
    assert exit_code == 0
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["endpoint"] == "styles"
    assert payload["status"] == "ok"
    assert payload["items_fetched"] == 25


def test_fetch_progress_line_includes_page_eta(capsys) -> None:
    cli._write_progress_line(
        FetchProgressEvent(
            kind="page_fetched",
            endpoint="styles",
            page_index=3,
            page_items=500,
            pages_fetched=3,
            items_fetched=1500,
            skip=1000,
            next_skip=1500,
            expected_count=6000,
            expected_pages=12,
            percent_complete=25.0,
            rolling_avg_seconds=0.75,
            estimated_remaining_seconds=90.0,
            elapsed_seconds=2.0,
        )
    )

    output = capsys.readouterr().err
    assert "[styles] page 3/12" in output
    assert "avg_page=750ms" in output
    assert "eta=1m 30s" in output
