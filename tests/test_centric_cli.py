import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

from centric_mdm_validation.centric import cli
from centric_mdm_validation.centric.models import (
    AuthSettings,
    EndpointSpec,
    FetcherConfig,
    FetchRunResult,
)


def test_months_fetch_writes_to_run_directory(tmp_path, monkeypatch) -> None:
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
    assert len(captured_output_dirs) == 1
    output_dir = captured_output_dirs[0]
    assert output_dir.parent == tmp_path / "raw" / "runs"
    assert output_dir.name.endswith("-months2")
    manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["run_id"] == output_dir.name
    assert manifest["mode"] == "months"
    assert manifest["modified_since"] is not None
    assert manifest["endpoints"]["styles"]["file"] == "styles.jsonl"
    assert manifest["endpoints"]["styles"]["is_delta"] is False


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
