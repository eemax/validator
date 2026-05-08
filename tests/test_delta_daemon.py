from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from centric_mdm_validation import delta_daemon
from centric_mdm_validation.centric.models import AuthSettings, EndpointSpec, FetcherConfig
from centric_mdm_validation.delta_daemon import (
    DEFAULT_DELTA_CYCLE_DIR,
    DEFAULT_DELTA_DAEMON_LOCK_PATH,
    DeltaDaemonError,
    DeltaDaemonOptions,
    build_fetch_args,
    next_scheduled_runs,
    resolve_effective_fetch_targets,
    run_delta_daemon,
    run_delta_fetch_once,
)


def _options(tmp_path, **overrides) -> DeltaDaemonOptions:
    values = {
        "schedule": "*/30 * * * *",
        "endpoints": [],
        "then_pipelines": [],
        "pipeline_reports": True,
        "config": None,
        "params": None,
        "delta_state_file": None,
        "output_dir": None,
        "checkpoint_dir": None,
        "lock_file": tmp_path / "locks" / "delta.lock",
        "log_file": tmp_path / "logs" / "delta-daemon.log",
        "runs_log_file": tmp_path / "logs" / "delta-runs.jsonl",
        "cycle_dir": tmp_path / "cron",
    }
    values.update(overrides)
    return DeltaDaemonOptions(**values)


def test_next_scheduled_runs_uses_clock_aligned_cron() -> None:
    local_tz = datetime.now().astimezone().tzinfo
    base = datetime(2026, 5, 7, 9, 17, tzinfo=local_tz)

    runs = next_scheduled_runs("*/30 * * * *", base=base, count=3)

    assert [run.minute for run in runs] == [30, 0, 30]
    assert [run.hour for run in runs] == [9, 10, 10]


def test_invalid_cron_schedule_has_clear_error() -> None:
    with pytest.raises(DeltaDaemonError, match='Invalid cron schedule: "hourly"'):
        next_scheduled_runs("hourly")


def test_build_fetch_args_passes_direct_fetch_options(tmp_path) -> None:
    options = _options(
        tmp_path,
        endpoints=["styles", "bomrows"],
        config=tmp_path / "fetcher.yml",
        params=tmp_path / "fetch-params.yml",
        delta_state_file=tmp_path / "delta_fetcher.yml",
        output_dir=tmp_path / "raw",
        checkpoint_dir=tmp_path / "checkpoints",
    )

    assert build_fetch_args(options) == [
        "run",
        "--delta",
        "--quiet",
        "--json",
        "--config",
        str(tmp_path / "fetcher.yml"),
        "--params",
        str(tmp_path / "fetch-params.yml"),
        "--delta-state-file",
        str(tmp_path / "delta_fetcher.yml"),
        "--output-dir",
        str(tmp_path / "raw"),
        "--checkpoint-dir",
        str(tmp_path / "checkpoints"),
        "--endpoint",
        "styles",
        "--endpoint",
        "bomrows",
    ]


def test_resolve_effective_fetch_targets_prints_defaults(tmp_path, monkeypatch) -> None:
    def fake_load_fetcher_settings(config, **kwargs):
        assert config == Path("config/fetcher.yml")
        return (
            FetcherConfig(
                output_dir=tmp_path / "raw",
                checkpoint_dir=tmp_path / "checkpoints",
            ),
            AuthSettings(),
            [
                EndpointSpec(name="styles", api_version="v2", path="styles"),
                EndpointSpec(name="bomrows", api_version="v2", path="bomrows"),
            ],
        )

    monkeypatch.delenv("CENTRIC_CONFIG_DIR", raising=False)
    monkeypatch.setattr(delta_daemon, "load_fetcher_settings", fake_load_fetcher_settings)
    monkeypatch.setattr(delta_daemon, "resolve_fetch_params_path", lambda *_: None)

    targets = resolve_effective_fetch_targets(_options(tmp_path))

    assert targets.endpoints == ["styles", "bomrows"]
    assert targets.config == Path("config/fetcher.yml")
    assert targets.params is None
    assert targets.delta_state_file == Path(".local/delta_fetcher.yml")
    assert targets.output_dir == tmp_path / "raw"
    assert targets.checkpoint_dir == tmp_path / "checkpoints"


def test_delta_daemon_startup_prints_effective_fetch_targets(tmp_path, monkeypatch) -> None:
    def fake_load_fetcher_settings(config, **kwargs):
        return (
            FetcherConfig(
                output_dir=tmp_path / "raw",
                checkpoint_dir=tmp_path / "checkpoints",
            ),
            AuthSettings(),
            [EndpointSpec(name="styles", api_version="v2", path="styles")],
        )

    monkeypatch.delenv("CENTRIC_CONFIG_DIR", raising=False)
    monkeypatch.setattr(delta_daemon, "load_fetcher_settings", fake_load_fetcher_settings)
    monkeypatch.setattr(delta_daemon, "resolve_fetch_params_path", lambda *_: None)
    messages: list[str] = []

    run_delta_daemon(_options(tmp_path), max_runs=0, echo=messages.append)

    output = "\n".join(messages)
    assert "Targets:" in output
    assert "endpoints: styles" in output
    assert "config: config/fetcher.yml" in output
    assert "params: none" in output
    assert "delta state: .local/delta_fetcher.yml" in output
    assert f"output dir: {tmp_path / 'raw'}" in output
    assert f"checkpoint dir: {tmp_path / 'checkpoints'}" in output


def test_delta_daemon_startup_prints_post_fetch_pipelines(tmp_path, monkeypatch) -> None:
    def fake_load_fetcher_settings(config, **kwargs):
        return (
            FetcherConfig(
                output_dir=tmp_path / "raw",
                checkpoint_dir=tmp_path / "checkpoints",
            ),
            AuthSettings(),
            [EndpointSpec(name="styles", api_version="v2", path="styles")],
        )

    monkeypatch.delenv("CENTRIC_CONFIG_DIR", raising=False)
    monkeypatch.setattr(delta_daemon, "load_fetcher_settings", fake_load_fetcher_settings)
    monkeypatch.setattr(delta_daemon, "resolve_fetch_params_path", lambda *_: None)
    messages: list[str] = []

    run_delta_daemon(
        _options(tmp_path, then_pipelines=["dpp", "md"], pipeline_reports=False),
        max_runs=0,
        echo=messages.append,
    )

    output = "\n".join(messages)
    assert "After successful fetch:" in output
    assert "pipeline target dpp (without reports)" in output
    assert "pipeline target md (without reports)" in output
    assert f"cycle summaries: {tmp_path / 'cron'}" in output


def test_delta_fetch_once_captures_json_summary_and_releases_lock(tmp_path, monkeypatch) -> None:
    options = _options(tmp_path)
    timestamps = iter(
        [
            datetime(2026, 5, 7, 9, 30, tzinfo=UTC),
            datetime(2026, 5, 7, 9, 31, tzinfo=UTC),
        ]
    )

    def fake_fetcher_main(args):
        assert args[:4] == ["run", "--delta", "--quiet", "--json"]
        print(json.dumps({"endpoint": "styles", "status": "ok", "items_fetched": 12}))
        print(json.dumps({"endpoint": "bomrows", "status": "failed", "error": "boom"}))
        return 1

    monkeypatch.setattr(delta_daemon, "fetcher_main", fake_fetcher_main)

    run = run_delta_fetch_once(options, now=lambda: next(timestamps))

    assert run.status == "FAILED"
    assert run.exit_code == 1
    assert run.endpoints_ok == 1
    assert run.endpoints_failed == 1
    assert run.items_fetched == 12
    assert not options.lock_file.exists()


def test_delta_fetch_once_skips_active_lock(tmp_path) -> None:
    options = _options(tmp_path)
    options.lock_file.parent.mkdir(parents=True)
    options.lock_file.write_text(json.dumps({"pid": 1}), encoding="utf-8")

    run = run_delta_fetch_once(options)

    assert run.status == "SKIPPED"
    assert run.lock_skipped is True
    assert run.error is not None


def test_run_delta_daemon_waits_until_next_scheduled_clock_time(tmp_path, monkeypatch) -> None:
    options = _options(tmp_path, schedule="*/30 * * * *")
    current = [datetime(2026, 5, 7, 9, 17, tzinfo=UTC)]
    messages: list[str] = []

    def fake_sleep(seconds: float) -> None:
        current[0] += timedelta(seconds=seconds)

    def fake_fetcher_main(args):
        print(json.dumps({"endpoint": "styles", "status": "ok", "items_fetched": 1}))
        return 0

    monkeypatch.setattr(delta_daemon, "fetcher_main", fake_fetcher_main)

    exit_code = run_delta_daemon(
        options,
        max_runs=1,
        now=lambda: current[0],
        sleep=fake_sleep,
        echo=messages.append,
    )

    assert exit_code == 0
    assert current[0].minute == 30
    assert any("Schedule: */30 * * * *" in message for message in messages)
    assert any("Waiting until" in message for message in messages)
    assert any("Delta fetch finished: status=OK" in message for message in messages)
    assert options.runs_log_file.is_file()


def test_run_delta_daemon_runs_post_fetch_pipelines_and_records_cycle(
    tmp_path,
    monkeypatch,
) -> None:
    options = _options(tmp_path, then_pipelines=["dpp", "md"], pipeline_reports=False)
    current = [datetime(2026, 5, 7, 9, 17, tzinfo=UTC)]
    pipeline_calls: list[tuple[str, Path, bool]] = []

    def fake_sleep(seconds: float) -> None:
        current[0] += timedelta(seconds=seconds)

    def fake_fetcher_main(args):
        print(json.dumps({"endpoint": "styles", "status": "ok", "items_fetched": 1}))
        return 0

    def fake_pipeline_runner(target, *, raw_dir, include_report):
        pipeline_calls.append((target, raw_dir, include_report))
        if target == "dpp":
            raise RuntimeError("validation exploded")
        return {"target": target}

    monkeypatch.setattr(delta_daemon, "fetcher_main", fake_fetcher_main)

    exit_code = run_delta_daemon(
        options,
        max_runs=1,
        now=lambda: current[0],
        sleep=fake_sleep,
        echo=lambda _: None,
        pipeline_runner=fake_pipeline_runner,
    )

    assert exit_code == 0
    assert pipeline_calls == [
        ("dpp", Path("data/raw"), False),
        ("md", Path("data/raw"), False),
    ]
    cycle_files = list((tmp_path / "cron" / "delta-daemon").glob("*.json"))
    assert len(cycle_files) == 1
    cycle = json.loads(cycle_files[0].read_text(encoding="utf-8"))
    assert cycle["status"] == "PARTIAL_FAILURE"
    assert cycle["fetch"]["status"] == "OK"
    assert [run["status"] for run in cycle["pipelines"]] == ["FAILED", "OK"]


def test_run_delta_daemon_skips_pipelines_when_fetch_fails(tmp_path, monkeypatch) -> None:
    options = _options(tmp_path, then_pipelines=["dpp"])
    current = [datetime(2026, 5, 7, 9, 17, tzinfo=UTC)]
    pipeline_calls: list[str] = []

    def fake_sleep(seconds: float) -> None:
        current[0] += timedelta(seconds=seconds)

    def fake_fetcher_main(args):
        print(json.dumps({"endpoint": "styles", "status": "failed"}))
        return 1

    def fake_pipeline_runner(target, **kwargs):
        pipeline_calls.append(target)

    monkeypatch.setattr(delta_daemon, "fetcher_main", fake_fetcher_main)

    run_delta_daemon(
        options,
        max_runs=1,
        now=lambda: current[0],
        sleep=fake_sleep,
        echo=lambda _: None,
        pipeline_runner=fake_pipeline_runner,
    )

    assert pipeline_calls == []
    cycle_files = list((tmp_path / "cron" / "delta-daemon").glob("*.json"))
    assert len(cycle_files) == 1
    cycle = json.loads(cycle_files[0].read_text(encoding="utf-8"))
    assert cycle["status"] == "FAILED"
    assert cycle["pipelines"] == []


def test_default_lock_path_is_under_data_cron_locks() -> None:
    assert Path("data/cron/locks/delta-daemon.lock") == DEFAULT_DELTA_DAEMON_LOCK_PATH


def test_default_cycle_dir_is_under_data_cron() -> None:
    assert Path("data/cron") == DEFAULT_DELTA_CYCLE_DIR
