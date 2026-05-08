from __future__ import annotations

import contextlib
import io
import json
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from croniter import croniter

from centric_mdm_validation.centric.cli import main as fetcher_main
from centric_mdm_validation.centric.config import (
    ConfigError,
    load_fetcher_settings,
    resolve_fetch_params_path,
    resolve_private_config_path,
)

DEFAULT_FETCHER_CONFIG_PATH = Path("config/fetcher.yml")
DEFAULT_DELTA_STATE_CONFIG_PATH = Path("delta_fetcher.yml")
DEFAULT_DELTA_DAEMON_LOCK_PATH = Path("data/cron/locks/delta-daemon.lock")
DEFAULT_DELTA_DAEMON_LOG_PATH = Path("data/logs/delta-daemon.log")
DEFAULT_DELTA_RUNS_LOG_PATH = Path("data/logs/delta-runs.jsonl")
DEFAULT_DELTA_CYCLE_DIR = Path("data/cron")

SleepFn = Callable[[float], None]
NowFn = Callable[[], datetime]
EchoFn = Callable[[str], None]
PipelineRunner = Callable[..., Any]


class DeltaDaemonError(RuntimeError):
    pass


@dataclass(frozen=True)
class DeltaDaemonOptions:
    schedule: str
    endpoints: list[str]
    then_pipelines: list[str]
    pipeline_reports: bool
    config: Path | None
    params: Path | None
    delta_state_file: Path | None
    output_dir: Path | None
    checkpoint_dir: Path | None
    lock_file: Path
    log_file: Path
    runs_log_file: Path
    cycle_dir: Path


@dataclass(frozen=True)
class DeltaFetchRun:
    status: str
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    exit_code: int
    endpoints_ok: int
    endpoints_failed: int
    items_fetched: int
    pages_fetched: int
    records: list[dict[str, Any]]
    stderr: str
    lock_skipped: bool = False
    error: str | None = None


@dataclass(frozen=True)
class DeltaPipelineRun:
    target: str
    status: str
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    error: str | None = None
    summary: dict[str, Any] | None = None


@dataclass(frozen=True)
class DeltaDaemonCycle:
    cycle_id: str
    status: str
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    fetch: DeltaFetchRun
    pipelines: list[DeltaPipelineRun]


@dataclass(frozen=True)
class EffectiveDeltaFetchTargets:
    endpoints: list[str]
    config: Path
    params: Path | None
    delta_state_file: Path
    output_dir: Path
    checkpoint_dir: Path


def validate_cron_schedule(schedule: str) -> None:
    fields = schedule.split()
    if len(fields) != 5:
        raise DeltaDaemonError(f'Invalid cron schedule: "{schedule}"')
    if not croniter.is_valid(schedule):
        raise DeltaDaemonError(f'Invalid cron schedule: "{schedule}"')


def local_now() -> datetime:
    return datetime.now().astimezone()


def next_scheduled_runs(
    schedule: str,
    *,
    base: datetime | None = None,
    count: int = 3,
) -> list[datetime]:
    validate_cron_schedule(schedule)
    iterator = croniter(schedule, base or local_now())
    return [iterator.get_next(datetime).astimezone() for _ in range(count)]


def resolve_effective_fetch_targets(options: DeltaDaemonOptions) -> EffectiveDeltaFetchTargets:
    config = options.config or DEFAULT_FETCHER_CONFIG_PATH
    params = resolve_fetch_params_path(options.params)
    delta_state_file = resolve_private_config_path(
        DEFAULT_DELTA_STATE_CONFIG_PATH,
        options.delta_state_file,
    )
    try:
        fetcher_cfg, _, endpoint_specs = load_fetcher_settings(config, params_path=params)
    except ConfigError as exc:
        raise DeltaDaemonError(str(exc)) from exc
    return EffectiveDeltaFetchTargets(
        endpoints=options.endpoints or [spec.name for spec in endpoint_specs],
        config=config,
        params=params,
        delta_state_file=delta_state_file,
        output_dir=options.output_dir or fetcher_cfg.output_dir,
        checkpoint_dir=options.checkpoint_dir or fetcher_cfg.checkpoint_dir,
    )


def run_delta_daemon(
    options: DeltaDaemonOptions,
    *,
    max_runs: int | None = None,
    now: NowFn = local_now,
    sleep: SleepFn = time.sleep,
    echo: EchoFn = print,
    pipeline_runner: PipelineRunner | None = None,
) -> int:
    validate_cron_schedule(options.schedule)
    _write_human_log(options.log_file, "Delta daemon starting")
    _print_startup(options, now=now, echo=echo)

    runs_completed = 0
    while max_runs is None or runs_completed < max_runs:
        next_run = next_scheduled_runs(options.schedule, base=now(), count=1)[0]
        _announce_wait(next_run, now=now, echo=echo, log_file=options.log_file)
        _sleep_until(next_run, now=now, sleep=sleep)

        cycle = run_delta_cycle(options, now=now, pipeline_runner=pipeline_runner)
        runs_completed += 1
        _write_cycle_logs(options, cycle)
        _print_cycle_summary(cycle, echo=echo, cycle_dir=options.cycle_dir)

    return 0


def run_delta_cycle(
    options: DeltaDaemonOptions,
    *,
    now: NowFn = local_now,
    pipeline_runner: PipelineRunner | None = None,
) -> DeltaDaemonCycle:
    started_at = now()
    fetch_run = run_delta_fetch_once(options, now=now)
    pipelines: list[DeltaPipelineRun] = []
    if fetch_run.status == "OK" and options.then_pipelines:
        pipelines = _run_then_pipelines(options, now=now, pipeline_runner=pipeline_runner)
    finished_at = now()
    return DeltaDaemonCycle(
        cycle_id=_cycle_id(started_at),
        status=_cycle_status(fetch_run, pipelines),
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=(finished_at - started_at).total_seconds(),
        fetch=fetch_run,
        pipelines=pipelines,
    )


def run_delta_fetch_once(
    options: DeltaDaemonOptions,
    *,
    now: NowFn = local_now,
) -> DeltaFetchRun:
    started_at = now()
    lock = _acquire_lock(options.lock_file)
    if lock is None:
        finished_at = now()
        return DeltaFetchRun(
            status="SKIPPED",
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=(finished_at - started_at).total_seconds(),
            exit_code=0,
            endpoints_ok=0,
            endpoints_failed=0,
            items_fetched=0,
            pages_fetched=0,
            records=[],
            stderr="",
            lock_skipped=True,
            error=f"Lock is active: {options.lock_file}",
        )

    try:
        exit_code, stdout, stderr = _run_fetcher(options)
    except Exception as exc:
        finished_at = now()
        return DeltaFetchRun(
            status="FAILED",
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=(finished_at - started_at).total_seconds(),
            exit_code=1,
            endpoints_ok=0,
            endpoints_failed=0,
            items_fetched=0,
            pages_fetched=0,
            records=[],
            stderr="",
            error=str(exc),
        )
    finally:
        lock.release()

    finished_at = now()
    records = _parse_jsonl(stdout)
    endpoints_failed = sum(1 for record in records if record.get("status") == "failed")
    endpoints_ok = sum(1 for record in records if record.get("status") == "ok")
    items_fetched = sum(_safe_int(record.get("items_fetched")) for record in records)
    pages_fetched = sum(_safe_int(record.get("pages_fetched")) for record in records)
    status = "OK" if exit_code == 0 and endpoints_failed == 0 else "FAILED"
    return DeltaFetchRun(
        status=status,
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=(finished_at - started_at).total_seconds(),
        exit_code=exit_code,
        endpoints_ok=endpoints_ok,
        endpoints_failed=endpoints_failed,
        items_fetched=items_fetched,
        pages_fetched=pages_fetched,
        records=records,
        stderr=stderr,
    )


def _run_then_pipelines(
    options: DeltaDaemonOptions,
    *,
    now: NowFn,
    pipeline_runner: PipelineRunner | None,
) -> list[DeltaPipelineRun]:
    raw_dir = resolve_effective_fetch_targets(options).output_dir
    runs: list[DeltaPipelineRun] = []
    for target in options.then_pipelines:
        started_at = now()
        summary = None
        error = None
        status = "OK"
        try:
            if pipeline_runner is None:
                raise DeltaDaemonError("Pipeline runner is not configured.")
            summary = pipeline_runner(
                target,
                raw_dir=raw_dir,
                include_report=options.pipeline_reports,
            )
        except Exception as exc:
            status = "FAILED"
            error = str(exc)
        finished_at = now()
        runs.append(
            DeltaPipelineRun(
                target=target,
                status=status,
                started_at=started_at,
                finished_at=finished_at,
                duration_seconds=(finished_at - started_at).total_seconds(),
                error=error,
                summary=_pipeline_summary_to_record(summary) if summary is not None else None,
            )
        )
    return runs


def build_fetch_args(options: DeltaDaemonOptions) -> list[str]:
    args = ["run", "--delta", "--quiet", "--json"]
    if options.config is not None:
        args.extend(["--config", str(options.config)])
    if options.params is not None:
        args.extend(["--params", str(options.params)])
    if options.delta_state_file is not None:
        args.extend(["--delta-state-file", str(options.delta_state_file)])
    if options.output_dir is not None:
        args.extend(["--output-dir", str(options.output_dir)])
    if options.checkpoint_dir is not None:
        args.extend(["--checkpoint-dir", str(options.checkpoint_dir)])
    for endpoint in options.endpoints:
        args.extend(["--endpoint", endpoint])
    return args


def _display_fetch_args(options: DeltaDaemonOptions) -> list[str]:
    args = ["--delta"]
    if options.config is not None:
        args.extend(["--config", str(options.config)])
    if options.params is not None:
        args.extend(["--params", str(options.params)])
    if options.delta_state_file is not None:
        args.extend(["--delta-state-file", str(options.delta_state_file)])
    if options.output_dir is not None:
        args.extend(["--output-dir", str(options.output_dir)])
    if options.checkpoint_dir is not None:
        args.extend(["--checkpoint-dir", str(options.checkpoint_dir)])
    for endpoint in options.endpoints:
        args.extend(["--endpoint", endpoint])
    return args


def format_local_datetime(value: datetime) -> str:
    local_value = value.astimezone()
    offset = local_value.strftime("%z")
    formatted_offset = offset[:-2] + ":" + offset[-2:] if len(offset) == 5 else offset
    return f"{local_value:%Y-%m-%d %H:%M:%S} {formatted_offset}"


def format_duration(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, remaining = divmod(int(round(seconds)), 60)
    if minutes < 60:
        return f"{minutes}m {remaining:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m"


def _print_startup(options: DeltaDaemonOptions, *, now: NowFn, echo: EchoFn) -> None:
    current = now()
    next_runs = next_scheduled_runs(options.schedule, base=current, count=3)
    targets = resolve_effective_fetch_targets(options)
    echo("Delta daemon starting")
    echo("")
    echo(f"Schedule: {options.schedule}")
    echo(f"Timezone: local ({current.tzname() or current.strftime('%z')})")
    echo("Next runs:")
    for run_at in next_runs:
        echo(f"  {format_local_datetime(run_at)}")
    echo("")
    echo("Fetch:")
    echo(f"  command: centric-mdm fetch {' '.join(_display_fetch_args(options))}")
    echo("Targets:")
    echo(f"  endpoints: {', '.join(targets.endpoints) if targets.endpoints else 'none'}")
    echo(f"  config: {targets.config}")
    echo(f"  params: {targets.params if targets.params is not None else 'none'}")
    echo(f"  delta state: {targets.delta_state_file}")
    echo(f"  output dir: {targets.output_dir}")
    echo(f"  checkpoint dir: {targets.checkpoint_dir}")
    echo(f"  lock: {options.lock_file}")
    echo(f"  log: {options.log_file}")
    echo(f"  run history: {options.runs_log_file}")
    echo(f"  cycle summaries: {options.cycle_dir}")
    if options.then_pipelines:
        echo("")
        echo("After successful fetch:")
        for target in options.then_pipelines:
            report_mode = "with reports" if options.pipeline_reports else "without reports"
            echo(f"  pipeline target {target} ({report_mode})")
    echo("")


def _announce_wait(
    next_run: datetime,
    *,
    now: NowFn,
    echo: EchoFn,
    log_file: Path,
) -> None:
    wait_seconds = max((next_run - now()).total_seconds(), 0.0)
    message = f"Waiting until {format_local_datetime(next_run)} ({format_duration(wait_seconds)})"
    echo(message)
    _write_human_log(log_file, message)


def _sleep_until(target: datetime, *, now: NowFn, sleep: SleepFn) -> None:
    while True:
        remaining = (target - now()).total_seconds()
        if remaining <= 0:
            return
        sleep(min(remaining, 60.0))


def _run_fetcher(options: DeltaDaemonOptions) -> tuple[int, str, str]:
    stdout = io.StringIO()
    stderr = io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        exit_code = fetcher_main(build_fetch_args(options))
    return exit_code, stdout.getvalue(), stderr.getvalue()


def _parse_jsonl(text: str) -> list[dict[str, Any]]:
    records = []
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            records.append(payload)
    return records


def _write_cycle_logs(options: DeltaDaemonOptions, cycle: DeltaDaemonCycle) -> None:
    _write_run_logs(options, cycle.fetch)
    _write_cycle_summary(options, cycle)
    message = (
        f"Delta cycle {cycle.status}: id={cycle.cycle_id} "
        f"duration={format_duration(cycle.duration_seconds)} "
        f"pipelines={_pipeline_status_text(cycle.pipelines)}"
    )
    _write_human_log(options.log_file, message)


def _write_run_logs(options: DeltaDaemonOptions, run: DeltaFetchRun) -> None:
    message = (
        f"Delta fetch {run.status}: exit_code={run.exit_code} "
        f"duration={format_duration(run.duration_seconds)} "
        f"endpoints={run.endpoints_ok} ok/{run.endpoints_failed} failed "
        f"records={run.items_fetched} pages={run.pages_fetched}"
    )
    if run.error:
        message += f" error={run.error}"
    _write_human_log(options.log_file, message)
    _append_jsonl(options.runs_log_file, _run_to_record(run))


def _print_cycle_summary(cycle: DeltaDaemonCycle, *, echo: EchoFn, cycle_dir: Path) -> None:
    _print_run_summary(cycle.fetch, echo=echo)
    for pipeline_run in cycle.pipelines:
        if pipeline_run.status == "OK":
            echo(
                f"Pipeline {pipeline_run.target} finished: status=OK "
                f"duration={format_duration(pipeline_run.duration_seconds)}"
            )
            continue
        echo(
            f"Pipeline {pipeline_run.target} failed: "
            f"duration={format_duration(pipeline_run.duration_seconds)} "
            f"error={pipeline_run.error}"
        )
    if cycle.pipelines:
        summary_path = _cycle_summary_path(cycle, cycle_dir)
        echo(f"Cycle finished: status={cycle.status} summary={summary_path}")


def _print_run_summary(run: DeltaFetchRun, *, echo: EchoFn) -> None:
    if run.lock_skipped:
        echo(f"Delta fetch skipped: {run.error}")
        return
    echo(
        f"Delta fetch finished: status={run.status} exit_code={run.exit_code} "
        f"duration={format_duration(run.duration_seconds)} "
        f"endpoints={run.endpoints_ok} ok/{run.endpoints_failed} failed "
        f"records={run.items_fetched}"
    )
    if run.stderr.strip():
        echo(run.stderr.strip())


def _write_cycle_summary(options: DeltaDaemonOptions, cycle: DeltaDaemonCycle) -> None:
    path = _cycle_summary_path(cycle, options.cycle_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(_cycle_to_record(cycle), handle, indent=2, sort_keys=True)
        handle.write("\n")


def _cycle_summary_path(cycle: DeltaDaemonCycle, cycle_dir: Path | None) -> Path:
    base = cycle_dir or DEFAULT_DELTA_CYCLE_DIR
    return base / "delta-daemon" / f"{cycle.cycle_id}.json"


def _write_human_log(path: Path, message: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    timestamp = format_local_datetime(local_now())
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"[{timestamp}] {message}\n")


def _append_jsonl(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")


def _run_to_record(run: DeltaFetchRun) -> dict[str, Any]:
    return {
        "status": run.status,
        "started_at": run.started_at.isoformat(),
        "finished_at": run.finished_at.isoformat(),
        "duration_seconds": round(run.duration_seconds, 3),
        "exit_code": run.exit_code,
        "endpoints_ok": run.endpoints_ok,
        "endpoints_failed": run.endpoints_failed,
        "items_fetched": run.items_fetched,
        "pages_fetched": run.pages_fetched,
        "lock_skipped": run.lock_skipped,
        "error": run.error,
        "records": run.records,
    }


def _cycle_to_record(cycle: DeltaDaemonCycle) -> dict[str, Any]:
    return {
        "cycle_id": cycle.cycle_id,
        "status": cycle.status,
        "started_at": cycle.started_at.isoformat(),
        "finished_at": cycle.finished_at.isoformat(),
        "duration_seconds": round(cycle.duration_seconds, 3),
        "fetch": _run_to_record(cycle.fetch),
        "pipelines": [_pipeline_run_to_record(run) for run in cycle.pipelines],
    }


def _pipeline_run_to_record(run: DeltaPipelineRun) -> dict[str, Any]:
    return {
        "target": run.target,
        "status": run.status,
        "started_at": run.started_at.isoformat(),
        "finished_at": run.finished_at.isoformat(),
        "duration_seconds": round(run.duration_seconds, 3),
        "error": run.error,
        "summary": run.summary,
    }


def _cycle_status(fetch_run: DeltaFetchRun, pipelines: list[DeltaPipelineRun]) -> str:
    if fetch_run.status != "OK":
        return fetch_run.status
    if any(run.status != "OK" for run in pipelines):
        return "PARTIAL_FAILURE"
    return "OK"


def _cycle_id(started_at: datetime) -> str:
    return started_at.astimezone().strftime("%Y-%m-%dT%H%M%S%z")


def _pipeline_status_text(pipelines: list[DeltaPipelineRun]) -> str:
    if not pipelines:
        return "none"
    return ", ".join(f"{run.target}:{run.status}" for run in pipelines)


def _pipeline_summary_to_record(summary: Any) -> dict[str, Any]:
    fields = (
        "target",
        "raw_files_applied",
        "raw_files_skipped",
        "records_reconstructed",
        "total_records",
        "ready_records",
        "validation_output",
        "validation_run_id",
        "report_output_dir",
    )
    record = {}
    for field in fields:
        value = getattr(summary, field, None)
        if isinstance(value, Path):
            value = str(value)
        record[field] = value
    return record


def _safe_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    return 0


class _Lock:
    def __init__(self, path: Path, fd: int):
        self.path = path
        self.fd = fd

    def release(self) -> None:
        with contextlib.suppress(OSError):
            os.close(self.fd)
        with contextlib.suppress(FileNotFoundError):
            self.path.unlink()


def _acquire_lock(path: Path) -> _Lock | None:
    path.parent.mkdir(parents=True, exist_ok=True)
    while True:
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
        except FileExistsError:
            if _lock_is_stale(path):
                with contextlib.suppress(FileNotFoundError):
                    path.unlink()
                continue
            return None
        else:
            payload = json.dumps({"pid": os.getpid(), "started_at": local_now().isoformat()})
            os.write(fd, payload.encode("utf-8"))
            return _Lock(path, fd)


def _lock_is_stale(path: Path) -> bool:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    pid = payload.get("pid")
    if not isinstance(pid, int) or pid <= 0:
        return False
    return not _process_exists(pid)


def _process_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True
