import os
import platform
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from textwrap import dedent
from typing import Annotated

import typer

from centric_mdm_validation.centric.cli import main as fetcher_main
from centric_mdm_validation.centric.config import resolve_private_config_path
from centric_mdm_validation.centric.reconstruction import (
    has_private_report_hook,
    has_private_validation_hook,
    inspect_reconstruction_runtime,
    report_validation_results,
    validate_projected_products,
)
from centric_mdm_validation.centric.schema import load_endpoint_schemas
from centric_mdm_validation.centric.store import (
    IngestFileProgress,
    discover_raw_files,
    ingest_raw_dir,
    run_reconstruction_coverage_check,
    write_target_reconstruction,
)
from centric_mdm_validation.delta_daemon import (
    DEFAULT_DELTA_CYCLE_DIR,
    DEFAULT_DELTA_DAEMON_LOCK_PATH,
    DEFAULT_DELTA_DAEMON_LOG_PATH,
    DEFAULT_DELTA_RUNS_LOG_PATH,
    DeltaDaemonError,
    DeltaDaemonOptions,
    run_delta_daemon,
)
from centric_mdm_validation.endpoint_changelog import (
    list_endpoint_change_summary,
    list_endpoint_changelog_runs,
    list_endpoint_changes,
    load_endpoint_changelog_config,
    record_endpoint_changelog,
)
from centric_mdm_validation.io import read_json_records, write_json
from centric_mdm_validation.models import (
    CentricProductPayload,
    ReconstructionCheckPayload,
    ValidationRunResult,
)
from centric_mdm_validation.progress import (
    ProgressReporter,
    progress_enabled,
    progress_message,
    progress_section,
)
from centric_mdm_validation.reporting import DppReadinessReporter, ReconstructionCheckReporter
from centric_mdm_validation.validation import (
    DppReadinessValidator,
    DppRuleSet,
    ReconstructionCheckValidator,
)
from centric_mdm_validation.validation_history import (
    ValidationHistoryRun,
    list_validation_changes,
    list_validation_issue_counts,
    list_validation_runs,
    parse_history_since,
    record_validation_history,
)

APP_HELP = """
Centric MDM validation tools.

Workflow:
  raw endpoint files -> DuckDB store -> check/dpp/md records -> validation -> reports
  validation changes -> DuckDB history events
  selected endpoint fields -> DuckDB semantic changelog events

Targets:
  check  Aggregate endpoint/reference coverage.
  dpp    DPP readiness.
  md     Merchandise data readiness.

Run `centric-mdm examples` for copy-paste workflows.
"""

app = typer.Typer(
    help=dedent(APP_HELP).strip(),
    no_args_is_help=True,
    context_settings={"help_option_names": ["--help", "-h"]},
)
history_app = typer.Typer(
    help="Inspect compact DuckDB validation history.",
    no_args_is_help=True,
    context_settings={"help_option_names": ["--help", "-h"]},
)
changelog_app = typer.Typer(
    help="Track semantic endpoint payload changes from selected current-state fields.",
    no_args_is_help=True,
    context_settings={"help_option_names": ["--help", "-h"]},
)
app.add_typer(history_app, name="history")
app.add_typer(changelog_app, name="changelog")

RulesOption = Annotated[
    Path | None,
    typer.Option(
        "--rules",
        "-r",
        help=(
            "DPP readiness rule YAML. Defaults to CENTRIC_CONFIG_DIR/rules/dpp-readiness.yml "
            "or .local/rules/dpp-readiness.yml."
        ),
    ),
]
ProgressOption = Annotated[
    bool | None,
    typer.Option(
        "--progress/--no-progress",
        help=(
            "Show live progress and detailed stage output. Defaults to on in interactive terminals."
        ),
    ),
]
RULES_CONFIG_PATH = Path("rules/dpp-readiness.yml")
DEFAULT_DB_PATH = Path("data/centric.duckdb")
DEFAULT_RESULTS_DIR = Path("data/results")
DEFAULT_LATEST_RESULTS_DIR = DEFAULT_RESULTS_DIR / "latest"
DEFAULT_RECONSTRUCTION_CHECK_RESULTS_PATH = DEFAULT_LATEST_RESULTS_DIR / "check-results.json"
DEFAULT_DPP_PRODUCTS_PATH = DEFAULT_LATEST_RESULTS_DIR / "dpp-products.jsonl"
DEFAULT_DPP_RESULTS_PATH = DEFAULT_LATEST_RESULTS_DIR / "dpp-results.json"
DEFAULT_RECONSTRUCTION_CHECK_REPORT_DIR = Path("reports/reconstruction-check")
DEFAULT_DPP_REPORT_DIR = Path("reports/dpp-readiness")


@dataclass(frozen=True)
class PipelineTarget:
    name: str
    reconstructed_output: Path
    validation_output: Path
    report_output_dir: Path


@dataclass(frozen=True)
class PipelineRunSummary:
    target: str
    raw_files_applied: int
    raw_files_skipped: int
    records_reconstructed: int
    total_records: int
    ready_records: int
    validation_output: Path
    validation_run_id: str | None
    report_output_dir: Path | None


PIPELINE_TARGETS = {
    "check": PipelineTarget(
        name="check",
        reconstructed_output=DEFAULT_RECONSTRUCTION_CHECK_RESULTS_PATH,
        validation_output=DEFAULT_RECONSTRUCTION_CHECK_RESULTS_PATH,
        report_output_dir=DEFAULT_RECONSTRUCTION_CHECK_REPORT_DIR,
    ),
    "dpp": PipelineTarget(
        name="dpp",
        reconstructed_output=DEFAULT_DPP_PRODUCTS_PATH,
        validation_output=DEFAULT_DPP_RESULTS_PATH,
        report_output_dir=DEFAULT_DPP_REPORT_DIR,
    ),
    "md": PipelineTarget(
        name="md",
        reconstructed_output=DEFAULT_LATEST_RESULTS_DIR / "md-products.jsonl",
        validation_output=DEFAULT_LATEST_RESULTS_DIR / "md-results.json",
        report_output_dir=Path("reports/md-readiness"),
    ),
}
PIPELINE_TARGET_HELP = (
    "Required target to reconstruct, validate, and optionally report. "
    f"Registered targets: {', '.join(PIPELINE_TARGETS)}."
)
REPORT_TEMPLATES = {
    "check": {"default"},
    "dpp": {"default", "brands"},
    "md": {"default"},
}
DEFAULT_PIPELINE_WEIGHTS = {
    "ingest": 0.10,
    "reconstruct": 0.25,
    "validate": 0.15,
    "report": 0.50,
}
DPP_PIPELINE_WEIGHTS = {
    # Based on a local DPP benchmark where report generation dominated runtime.
    "ingest": 0.01,
    "reconstruct": 0.14,
    "validate": 0.04,
    "report": 0.81,
}
DPP_PIPELINE_ESTIMATES = {
    # Seconds from the same DPP benchmark used for the initial weights.
    "ingest": 0.1,
    "reconstruct": 8.4,
    "validate": 2.2,
    "report": 47.9,
}
CHECK_PIPELINE_WEIGHTS = {
    "ingest": 0.20,
    "reconstruct": 0.35,
    "validate": 0.05,
    "report": 0.40,
}
CHECK_PIPELINE_ESTIMATES = {
    "ingest": 0.2,
    "reconstruct": 0.2,
    "validate": 0.1,
    "report": 0.2,
}
DEFAULT_PIPELINE_ESTIMATES = {
    "ingest": 1.0,
    "reconstruct": 10.0,
    "validate": 5.0,
    "report": 20.0,
}

EXAMPLES_TEXT = """
Common workflows

Default aggregate check:
  uv run centric-mdm ingest
  uv run centric-mdm reconstruct
  uv run centric-mdm validate
  uv run centric-mdm report

One-shot check pipeline:
  uv run centric-mdm pipeline --target check

DPP readiness:
  uv run centric-mdm pipeline --target dpp

MD readiness:
  uv run centric-mdm pipeline --target md

Force live progress output:
  uv run centric-mdm pipeline --target dpp --progress

Run steps manually:
  uv run centric-mdm reconstruct --target dpp
  uv run centric-mdm validate --target dpp
  uv run centric-mdm report --target dpp

Inspect validation history:
  uv run centric-mdm history runs --target dpp
  uv run centric-mdm history changes --target dpp --since 2d
  uv run centric-mdm history issues --target dpp --since 3m

Track endpoint semantic changes:
  uv run centric-mdm changelog update
  uv run centric-mdm changelog summary --since 2d
  uv run centric-mdm changelog changes --endpoint styles --since 10h

Fetch data:
  uv run centric-mdm fetch --endpoint styles
  uv run centric-mdm fetch --days 60
  uv run centric-mdm fetch --days 3 --no-params
  uv run centric-mdm fetch --delta
  uv run centric-mdm fetch --delta --json

Run recurring delta fetches:
  uv run centric-mdm delta-daemon --schedule "0 * * * *"
  uv run centric-mdm delta-daemon --schedule "0 * * * *" \\
    --then-pipeline dpp --then-pipeline md --no-report

More help:
  uv run centric-mdm --help
  uv run centric-mdm pipeline --help
  uv run centric-mdm history --help
  uv run centric-mdm changelog --help
  uv run centric-mdm fetch --help
"""


@app.command(
    context_settings={
        "allow_extra_args": True,
        "ignore_unknown_options": True,
        "help_option_names": [],
    }
)
def fetch(ctx: typer.Context) -> None:
    """Run Centric fetch jobs."""

    args = list(ctx.args)
    caffeinate = _pop_fetch_caffeinate_flag(args)
    if args in (["--help"], ["-h"]):
        args = ["run", "--help"]
    if not args or args[0] != "run":
        args.insert(0, "run")
    if caffeinate:
        _run_caffeinated_fetch(args)
        return
    raise typer.Exit(fetcher_main(args))


@app.command()
def examples() -> None:
    """Show copy-paste examples for common workflows."""

    typer.echo(dedent(EXAMPLES_TEXT).strip())


@history_app.command("runs")
def history_runs(
    target: Annotated[
        str | None,
        typer.Option("--target", "-t", help="Validation target to filter."),
    ] = None,
    since: Annotated[
        str | None,
        typer.Option(
            "--since",
            help="Absolute date/time or relative duration: 10h, 2d, 3m, 1y.",
        ),
    ] = None,
    db: Annotated[
        Path,
        typer.Option("--db", help="DuckDB validation history store."),
    ] = DEFAULT_DB_PATH,
    limit: Annotated[
        int,
        typer.Option("--limit", help="Maximum rows to display."),
    ] = 20,
) -> None:
    """List validation history runs."""

    since_dt = _parse_history_since_or_fail(since)
    rows = list_validation_runs(db, target=target, since=since_dt, limit=limit)
    if not rows:
        _echo_step("History: no validation runs found.")
        return
    _print_history_table(
        ["created_at", "target", "run_id", "ready", "changes", "issue_changes"],
        [
            {
                "created_at": _format_history_datetime(row["created_at"]),
                "target": row["target"],
                "run_id": row["run_id"],
                "ready": f"{row['ready_records']}/{row['total_records']}",
                "changes": row["product_changes"],
                "issue_changes": row["issue_changes"],
            }
            for row in rows
        ],
    )


@history_app.command("changes")
def history_changes(
    target: Annotated[
        str | None,
        typer.Option("--target", "-t", help="Validation target to filter."),
    ] = None,
    since: Annotated[
        str | None,
        typer.Option(
            "--since",
            help="Absolute date/time or relative duration: 10h, 2d, 3m, 1y.",
        ),
    ] = None,
    db: Annotated[
        Path,
        typer.Option("--db", help="DuckDB validation history store."),
    ] = DEFAULT_DB_PATH,
    limit: Annotated[
        int,
        typer.Option("--limit", help="Maximum rows to display."),
    ] = 50,
) -> None:
    """List changed products from validation history."""

    since_dt = _parse_history_since_or_fail(since)
    rows = list_validation_changes(db, target=target, since=since_dt, limit=limit)
    if not rows:
        _echo_step("History: no validation changes found.")
        return
    _print_history_table(
        ["changed_at", "target", "product_id", "change", "status", "issues"],
        [
            {
                "changed_at": _format_history_datetime(row["changed_at"]),
                "target": row["target"],
                "product_id": row["product_id"],
                "change": row["change_type"],
                "status": f"{row['previous_status'] or '-'} -> {row['current_status'] or '-'}",
                "issues": (
                    f"{len(row['previous_issue_codes'])} -> {len(row['current_issue_codes'])}"
                ),
            }
            for row in rows
        ],
    )


@history_app.command("issues")
def history_issues(
    target: Annotated[
        str | None,
        typer.Option("--target", "-t", help="Validation target to filter."),
    ] = None,
    since: Annotated[
        str | None,
        typer.Option(
            "--since",
            help="Absolute date/time or relative duration: 10h, 2d, 3m, 1y.",
        ),
    ] = None,
    db: Annotated[
        Path,
        typer.Option("--db", help="DuckDB validation history store."),
    ] = DEFAULT_DB_PATH,
    limit: Annotated[
        int,
        typer.Option("--limit", help="Maximum rows to display."),
    ] = 50,
) -> None:
    """Summarize added/resolved issue codes from validation history."""

    since_dt = _parse_history_since_or_fail(since)
    rows = list_validation_issue_counts(db, target=target, since=since_dt, limit=limit)
    if not rows:
        _echo_step("History: no validation issue changes found.")
        return
    _print_history_table(
        ["target", "issue_code", "change", "severity", "count"],
        [
            {
                "target": row["target"],
                "issue_code": row["issue_code"],
                "change": row["change_type"],
                "severity": row["severity"] or "",
                "count": row["count"],
            }
            for row in rows
        ],
    )


@changelog_app.command("update")
def changelog_update(
    db: Annotated[
        Path,
        typer.Option("--db", help="DuckDB reconstruction store."),
    ] = DEFAULT_DB_PATH,
    config: Annotated[
        Path | None,
        typer.Option(
            "--config",
            help=(
                "Endpoint changelog field-selection YAML. Defaults to "
                "CENTRIC_CONFIG_DIR/changelog.yml or .local/changelog.yml."
            ),
        ),
    ] = None,
) -> None:
    """Record semantic endpoint changes from configured current-state fields."""

    try:
        changelog_config = load_endpoint_changelog_config(config)
        changelog_run = record_endpoint_changelog(db, config=changelog_config)
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--config") from exc
    _echo_done(
        f"Endpoint changelog updated: {changelog_run.record_count} records tracked across "
        f"{changelog_run.endpoint_count} endpoints, {changelog_run.event_count} events. "
        f"Run: {changelog_run.run_id}"
    )


@changelog_app.command("runs")
def changelog_runs(
    since: Annotated[
        str | None,
        typer.Option(
            "--since",
            help="Absolute date/time or relative duration: 10h, 2d, 3m, 1y.",
        ),
    ] = None,
    db: Annotated[
        Path,
        typer.Option("--db", help="DuckDB endpoint changelog store."),
    ] = DEFAULT_DB_PATH,
    limit: Annotated[
        int,
        typer.Option("--limit", help="Maximum rows to display."),
    ] = 20,
) -> None:
    """List endpoint changelog runs."""

    since_dt = _parse_history_since_or_fail(since)
    rows = list_endpoint_changelog_runs(db, since=since_dt, limit=limit)
    if not rows:
        _echo_step("Changelog: no endpoint changelog runs found.")
        return
    _print_history_table(
        ["created_at", "run_id", "endpoints", "records", "events", "config"],
        [
            {
                "created_at": _format_history_datetime(row["created_at"]),
                "run_id": row["run_id"],
                "endpoints": row["endpoint_count"],
                "records": row["record_count"],
                "events": row["event_count"],
                "config": row["config_path"],
            }
            for row in rows
        ],
    )


@changelog_app.command("summary")
def changelog_summary(
    since: Annotated[
        str | None,
        typer.Option(
            "--since",
            help="Absolute date/time or relative duration: 10h, 2d, 3m, 1y.",
        ),
    ] = None,
    db: Annotated[
        Path,
        typer.Option("--db", help="DuckDB endpoint changelog store."),
    ] = DEFAULT_DB_PATH,
    limit: Annotated[
        int,
        typer.Option("--limit", help="Maximum rows to display."),
    ] = 100,
) -> None:
    """Summarize endpoint changes by endpoint and change type."""

    since_dt = _parse_history_since_or_fail(since)
    rows = list_endpoint_change_summary(db, since=since_dt, limit=limit)
    if not rows:
        _echo_step("Changelog: no endpoint changes found.")
        return
    _print_history_table(
        ["endpoint", "change", "count"],
        [
            {
                "endpoint": row["endpoint"],
                "change": row["change_type"],
                "count": row["count"],
            }
            for row in rows
        ],
    )


@changelog_app.command("changes")
def changelog_changes(
    endpoint: Annotated[
        str | None,
        typer.Option("--endpoint", "-e", help="Endpoint to filter, for example styles."),
    ] = None,
    since: Annotated[
        str | None,
        typer.Option(
            "--since",
            help="Absolute date/time or relative duration: 10h, 2d, 3m, 1y.",
        ),
    ] = None,
    db: Annotated[
        Path,
        typer.Option("--db", help="DuckDB endpoint changelog store."),
    ] = DEFAULT_DB_PATH,
    limit: Annotated[
        int,
        typer.Option("--limit", help="Maximum rows to display."),
    ] = 50,
) -> None:
    """List changed endpoint records."""

    since_dt = _parse_history_since_or_fail(since)
    rows = list_endpoint_changes(db, endpoint=endpoint, since=since_dt, limit=limit)
    if not rows:
        _echo_step("Changelog: no endpoint changes found.")
        return
    _print_history_table(
        ["changed_at", "endpoint", "record_id", "change", "fields"],
        [
            {
                "changed_at": _format_history_datetime(row["changed_at"]),
                "endpoint": row["endpoint"],
                "record_id": row["record_id"],
                "change": row["change_type"],
                "fields": ", ".join(row["changed_fields"]),
            }
            for row in rows
        ],
    )


@app.command("delta-daemon")
def delta_daemon(
    schedule: Annotated[
        str,
        typer.Option(
            "--schedule",
            help=(
                "5-field local-time cron schedule for delta fetches, for example "
                "'0 * * * *' hourly or '*/30 * * * *' every 30 minutes."
            ),
        ),
    ],
    endpoint: Annotated[
        list[str] | None,
        typer.Option("--endpoint", "-e", help="Endpoint name to fetch. Repeat for multiple."),
    ] = None,
    then_pipeline: Annotated[
        list[str] | None,
        typer.Option(
            "--then-pipeline",
            help="Pipeline target to run after a successful delta fetch. Repeat for multiple.",
        ),
    ] = None,
    pipeline_reports: Annotated[
        bool,
        typer.Option("--report/--no-report", help="Write reports for post-fetch pipelines."),
    ] = True,
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Fetcher config path."),
    ] = None,
    params: Annotated[
        Path | None,
        typer.Option("--params", help="Private fetch params YAML."),
    ] = None,
    delta_state_file: Annotated[
        Path | None,
        typer.Option("--delta-state-file", help="Delta state YAML path."),
    ] = None,
    output_dir: Annotated[
        Path | None,
        typer.Option("--output-dir", help="Override fetcher output directory."),
    ] = None,
    checkpoint_dir: Annotated[
        Path | None,
        typer.Option("--checkpoint-dir", help="Override fetcher checkpoint directory."),
    ] = None,
    lock_file: Annotated[
        Path,
        typer.Option("--lock-file", help="Delta daemon lock file."),
    ] = DEFAULT_DELTA_DAEMON_LOCK_PATH,
    log_file: Annotated[
        Path,
        typer.Option("--log-file", help="Human-readable delta daemon log file."),
    ] = DEFAULT_DELTA_DAEMON_LOG_PATH,
    runs_log_file: Annotated[
        Path,
        typer.Option("--runs-log-file", help="JSONL delta daemon run history file."),
    ] = DEFAULT_DELTA_RUNS_LOG_PATH,
    cycle_dir: Annotated[
        Path,
        typer.Option("--cycle-dir", help="Directory for JSON delta daemon cycle summaries."),
    ] = DEFAULT_DELTA_CYCLE_DIR,
    max_runs: Annotated[
        int | None,
        typer.Option("--max-runs", hidden=True),
    ] = None,
) -> None:
    """Run the delta fetcher on a foreground local-time cron schedule."""

    options = DeltaDaemonOptions(
        schedule=schedule,
        endpoints=endpoint or [],
        then_pipelines=then_pipeline or [],
        pipeline_reports=pipeline_reports,
        config=config,
        params=params,
        delta_state_file=delta_state_file,
        output_dir=output_dir,
        checkpoint_dir=checkpoint_dir,
        lock_file=lock_file,
        log_file=log_file,
        runs_log_file=runs_log_file,
        cycle_dir=cycle_dir,
    )
    try:
        raise typer.Exit(
            run_delta_daemon(
                options,
                max_runs=max_runs,
                pipeline_runner=_run_delta_daemon_pipeline,
            )
        )
    except DeltaDaemonError as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        _echo_cron_help(err=True)
        raise typer.Exit(1) from exc
    except KeyboardInterrupt:
        typer.echo("Delta daemon stopped by user.", err=True)
        raise typer.Exit(130) from None


@app.command()
def ingest(
    raw_dir: Annotated[
        Path,
        typer.Option("--raw-dir", "-r", help="Directory containing raw endpoint JSONL files."),
    ] = Path("data/raw"),
    db: Annotated[
        Path,
        typer.Option("--db", help="DuckDB reconstruction store."),
    ] = DEFAULT_DB_PATH,
    schema: Annotated[
        Path | None,
        typer.Option("--schema", help="Endpoint merge schema YAML."),
    ] = None,
    progress: ProgressOption = None,
) -> None:
    """Catch up the DuckDB reconstruction store from immutable raw endpoint files."""

    with ProgressReporter(enabled=progress) as progress_reporter:
        progress_section("Ingest raw files")
        result = _run_ingest(
            raw_dir=raw_dir,
            db=db,
            schema=schema,
            progress=progress_reporter,
        )
    _echo_done(
        f"Ingested {result.applied_files} raw files into {db} "
        f"({result.skipped_files} already applied, {result.records_upserted} upserts, "
        f"{result.records_deleted} deletes)."
    )


@app.command()
def reconstruct(
    db: Annotated[
        Path,
        typer.Option("--db", help="DuckDB reconstruction store."),
    ] = DEFAULT_DB_PATH,
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Output JSON or JSONL."),
    ] = None,
    target: Annotated[
        str,
        typer.Option("--target", "-t", help="Target to reconstruct. One of: check, dpp, md."),
    ] = "check",
    progress: ProgressOption = None,
) -> None:
    """Build aggregate check state or materialize a target reconstruction."""

    output_path = output or _default_reconstruct_output(target)
    with ProgressReporter(enabled=progress) as progress_reporter:
        if target != "check":
            progress_section(f"Reconstruct {target}")
            _echo_reconstruction_runtime(target)
            _echo_step(f"Reconstruct: building {target} records from endpoint state in {db}")
            payloads = write_target_reconstruction(
                db,
                output_path,
                target=target,
                progress=progress_reporter,
            )
            _echo_done(f"Wrote {len(payloads)} {target} records to {output_path}")
            return

        progress_section("Reconstruction check")
        _echo_step(f"Check: measuring aggregate endpoint coverage from {db}")
        run = run_reconstruction_coverage_check(db, progress=progress_reporter)
        _write_validation_result(output_path, run)
        _echo_done(f"Wrote aggregate check results to {output_path}")


@app.command()
def pipeline(
    raw_dir: Annotated[
        Path,
        typer.Option("--raw-dir", help="Directory containing raw endpoint JSONL files."),
    ] = Path("data/raw"),
    db: Annotated[
        Path,
        typer.Option("--db", help="DuckDB reconstruction store."),
    ] = DEFAULT_DB_PATH,
    reconstruction_output: Annotated[
        Path | None,
        typer.Option(
            "--reconstruction-output",
            "--projected-output",
            help=(
                "Reconstructed target JSONL. `--projected-output` is kept as a compatibility alias."
            ),
        ),
    ] = None,
    target: Annotated[
        str | None,
        typer.Option("--target", "-t", help=PIPELINE_TARGET_HELP),
    ] = None,
    schema: Annotated[
        Path | None,
        typer.Option("--schema", help="Endpoint merge schema YAML."),
    ] = None,
    rules: RulesOption = None,
    validation_output: Annotated[
        Path | None,
        typer.Option("--validation-output", help="Validation result JSON."),
    ] = None,
    report_output_dir: Annotated[
        Path | None,
        typer.Option(
            "--report-output-dir",
            help="Report output directory. Defaults to the registered target report directory.",
        ),
    ] = None,
    include_report: Annotated[
        bool,
        typer.Option("--report/--no-report", help="Write reports after validation."),
    ] = True,
    progress: ProgressOption = None,
) -> None:
    """Ingest raw files, reconstruct products, validate them, and optionally write reports."""

    if target is None:
        _fail_with_guidance(
            "Pipeline needs an explicit target.",
            [
                "Choose one of the registered targets:",
                "  uv run centric-mdm pipeline --target check",
                "  uv run centric-mdm pipeline --target dpp",
                "  uv run centric-mdm pipeline --target md",
                "",
                "Run `uv run centric-mdm examples` for full workflows.",
            ],
        )

    _run_pipeline_once(
        raw_dir=raw_dir,
        db=db,
        reconstruction_output=reconstruction_output,
        target=target,
        schema=schema,
        rules=rules,
        validation_output=validation_output,
        report_output_dir=report_output_dir,
        include_report=include_report,
        progress=progress,
        echo_summary=True,
    )


def _run_pipeline_once(
    *,
    raw_dir: Path,
    db: Path,
    reconstruction_output: Path | None,
    target: str,
    schema: Path | None,
    rules: Path | None,
    validation_output: Path | None,
    report_output_dir: Path | None,
    include_report: bool,
    progress: bool | None,
    echo_summary: bool,
) -> PipelineRunSummary:
    target_config = _pipeline_target_config(target)
    projected_output_path = reconstruction_output or target_config.reconstructed_output
    validation_output_path = validation_output or target_config.validation_output
    with ProgressReporter(enabled=progress) as progress_reporter:
        progress_message(f"Pipeline: {target}")
        weights = _pipeline_weights(target, include_report=include_report)
        estimates = _pipeline_estimates(target)
        progress_reporter.start_overall("Overall")
        progress_reporter.begin_overall_stage(
            weights["ingest"],
            estimated_seconds=estimates["ingest"],
        )
        progress_section("[1/4] Ingest raw files")
        _echo_step("Pipeline: starting ingest")
        ingest_result = _run_ingest(
            raw_dir=raw_dir,
            db=db,
            schema=schema,
            progress=progress_reporter,
        )
        progress_reporter.finish_overall_stage()
        if target == "check":
            progress_reporter.begin_overall_stage(
                weights["reconstruct"],
                estimated_seconds=estimates["reconstruct"],
            )
            progress_section("[2/4] Check endpoint coverage")
            _echo_step("Pipeline: checking aggregate endpoint coverage")
            run = run_reconstruction_coverage_check(db, progress=progress_reporter)
            progress_reporter.finish_overall_stage()
            progress_reporter.begin_overall_stage(
                weights["validate"],
                estimated_seconds=estimates["validate"],
            )
            progress_section("[3/4] Write validation results")
            _echo_step(f"Pipeline: writing check results to {validation_output_path}")
            history_run = _write_validation_outputs(
                db=db,
                target=target,
                latest_output_path=validation_output_path,
                run=run,
                input_path=projected_output_path,
            )
            progress_reporter.finish_overall_stage()
            report_path = None
            if include_report:
                report_path = report_output_dir or target_config.report_output_dir
                progress_reporter.begin_overall_stage(
                    weights["report"],
                    estimated_seconds=estimates["report"],
                )
                progress_section("[4/4] Write reports")
                _echo_step(f"Pipeline: writing aggregate check report to {report_path}")
                _write_report_for_target(target, run, report_path, progress=progress_reporter)
                progress_reporter.finish_overall_stage()
            progress_reporter.finish_overall()
            summary = run.get("summary", {})
            if echo_summary:
                report_text = f". Reports: {report_path}" if report_path is not None else ""
                _echo_done(
                    f"Check complete: {ingest_result.applied_files} raw files applied "
                    f"({ingest_result.skipped_files} skipped), "
                    f"{summary.get('declared_refs', 0)} refs checked, "
                    f"{summary.get('coverage_percent', 0.0)}% coverage. "
                    f"Results: {validation_output_path}"
                    + _history_run_text(history_run)
                    + report_text
                )
            return PipelineRunSummary(
                target=target,
                raw_files_applied=ingest_result.applied_files,
                raw_files_skipped=ingest_result.skipped_files,
                records_reconstructed=0,
                total_records=int(summary.get("declared_refs", 0) or 0),
                ready_records=int(summary.get("seen_refs", 0) or 0),
                validation_output=validation_output_path,
                validation_run_id=history_run.run_id if history_run is not None else None,
                report_output_dir=report_path,
            )

        progress_reporter.begin_overall_stage(
            weights["reconstruct"],
            estimated_seconds=estimates["reconstruct"],
        )
        progress_section(f"[2/4] Reconstruct {target.upper()}")
        _echo_reconstruction_runtime(target)
        _echo_step(f"Pipeline: building {target} records from endpoint state")
        projected_payloads = _write_reconstruction_for_target(
            db=db,
            output=projected_output_path,
            target=target,
            progress=progress_reporter,
        )
        if not progress_enabled():
            _echo_done(
                f"Reconstructed {len(projected_payloads)} products into {projected_output_path}"
            )
        progress_reporter.finish_overall_stage()
        progress_reporter.begin_overall_stage(
            weights["validate"],
            estimated_seconds=estimates["validate"],
        )
        progress_section(f"[3/4] Validate {target.upper()}")
        _echo_step(f"Pipeline: validating {len(projected_payloads)} products")
        run = _validate_records(
            projected_payloads,
            rules,
            target=target,
            progress=progress_reporter,
        )
        progress_reporter.finish_overall_stage()
        progress_section("[4/4] Write outputs")
        _echo_step(f"Pipeline: writing validation results to {validation_output_path}")
        history_run = _write_validation_outputs(
            db=db,
            target=target,
            latest_output_path=validation_output_path,
            run=run,
            input_path=projected_output_path,
        )
        report_path = None
        if include_report:
            progress_reporter.begin_overall_stage(
                weights["report"],
                estimated_seconds=estimates["report"],
            )
            progress_section("[4/4] Write reports")
            report_path = report_output_dir or target_config.report_output_dir
            _echo_step(f"Pipeline: writing reports to {report_path}")
            _write_report_for_target(target, run, report_path, progress=progress_reporter)
            progress_reporter.finish_overall_stage()
        progress_reporter.finish_overall()

        total, ready = _validation_counts(run)
        if echo_summary:
            run_text = f"Run: {history_run.run_id}. " if history_run is not None else ""
            report_text = (
                f"Reports: {report_path}" if report_path is not None else "Reports skipped"
            )
            _echo_done(
                f"Pipeline complete: {ingest_result.applied_files} raw files applied "
                f"({ingest_result.skipped_files} skipped), "
                f"{len(projected_payloads)} products reconstructed, "
                f"{ready}/{total} ready. Results: {validation_output_path}. "
                f"{run_text}"
                f"{report_text}"
            )
        return PipelineRunSummary(
            target=target,
            raw_files_applied=ingest_result.applied_files,
            raw_files_skipped=ingest_result.skipped_files,
            records_reconstructed=len(projected_payloads),
            total_records=total,
            ready_records=ready,
            validation_output=validation_output_path,
            validation_run_id=history_run.run_id if history_run is not None else None,
            report_output_dir=report_path,
        )


def _run_delta_daemon_pipeline(
    target: str,
    *,
    raw_dir: Path,
    include_report: bool,
) -> PipelineRunSummary:
    return _run_pipeline_once(
        raw_dir=raw_dir,
        db=DEFAULT_DB_PATH,
        reconstruction_output=None,
        target=target,
        schema=None,
        rules=None,
        validation_output=None,
        report_output_dir=None,
        include_report=include_report,
        progress=False,
        echo_summary=False,
    )


@app.command()
def validate(
    input_path: Annotated[
        Path | None,
        typer.Option("--input", "-i", help="Input JSON/JSONL."),
    ] = None,
    db: Annotated[
        Path,
        typer.Option("--db", help="DuckDB reconstruction and validation history store."),
    ] = DEFAULT_DB_PATH,
    target: Annotated[
        str,
        typer.Option("--target", "-t", help="Validation target."),
    ] = "check",
    rules: RulesOption = None,
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Validation result JSON."),
    ] = None,
    progress: ProgressOption = None,
) -> None:
    """Validate aggregate check results or target reconstruction payloads."""

    input_file = input_path or _default_validate_input(target)
    output_file = output or _default_validate_output(target)
    with ProgressReporter(enabled=progress) as progress_reporter:
        progress_section(f"Validate {target}")
        _echo_step(f"Validate: reading {target} records from {input_file}")
        run = _validate(input_file, rules, target=target, progress=progress_reporter)
        _echo_step(f"Validate: writing results to {output_file}")
        history_run = _write_validation_outputs(
            db=db,
            target=target,
            latest_output_path=output_file,
            run=run,
            input_path=input_file,
        )
    if target == "check" and isinstance(run, dict):
        summary = run.get("summary", {})
        _echo_done(
            f"Checked {summary.get('declared_refs', 0)} refs: "
            f"{summary.get('seen_refs', 0)} seen "
            f"({summary.get('coverage_percent', 0.0)}%). Results: {output_file}"
            + _history_run_text(history_run)
        )
        return
    total, ready = _validation_counts(run)
    readiness = _readiness_percent(run)
    _echo_done(
        f"Validated {total} records: {ready} ready ({readiness}%). Results: {output_file}"
        + _history_run_text(history_run)
    )


@app.command()
def report(
    input_path: Annotated[
        Path | None,
        typer.Option("--input", "-i", help="Input JSON/JSONL."),
    ] = None,
    target: Annotated[
        str,
        typer.Option("--target", "-t", help="Report target."),
    ] = "check",
    rules: RulesOption = None,
    output_dir: Annotated[
        Path | None,
        typer.Option("--output-dir", "-o", help="Directory for report files."),
    ] = None,
    template: Annotated[
        str,
        typer.Option(
            "--template",
            help="Report template. DPP supports: default, brands.",
        ),
    ] = "default",
    progress: ProgressOption = None,
) -> None:
    """Create reconstruction check or target readiness reports."""

    input_file = input_path or _default_validate_output(target)
    output_path = output_dir or _default_report_output_dir(target)
    _validate_report_template(target, template)
    with ProgressReporter(enabled=progress) as progress_reporter:
        progress_section(f"Report {target} ({template})")
        _echo_step(f"Report: reading {target} records from {input_file}")
        run = _read_report_input(input_file, rules, target=target, progress=progress_reporter)
        _echo_step(f"Report: writing {template} report files to {output_path}")
        _write_report_for_target(
            target,
            run,
            output_path,
            template=template,
            progress=progress_reporter,
        )
    if target == "check" and isinstance(run, dict):
        summary = run.get("summary", {})
        _echo_done(
            f"Wrote check coverage report for {summary.get('declared_refs', 0)} refs "
            f"into {output_path}"
        )
        return
    total, _ = _validation_counts(run)
    _echo_done(f"Wrote {target} {template} reports for {total} records into {output_path}")


def _run_ingest(
    raw_dir: Path,
    db: Path,
    schema: Path | None,
    *,
    progress: ProgressReporter | None = None,
):
    raw_files = discover_raw_files(raw_dir)
    _echo_step(f"Ingest: discovered {len(raw_files)} raw JSONL files under {raw_dir}")
    _echo_step(f"Ingest: updating DuckDB store at {db}")
    progress_callback = (
        _rich_ingest_progress(progress) if progress and progress.enabled else _echo_ingest_progress
    )
    result = ingest_raw_dir(
        raw_dir,
        db,
        schemas=load_endpoint_schemas(schema),
        progress=progress_callback,
    )
    if progress and progress.enabled:
        progress.emit(
            "Ingesting raw files",
            "finish",
            total=len(raw_files) or None,
            message=(f"{result.applied_files} applied, {result.skipped_files} skipped"),
        )
    if result.endpoints:
        endpoint_counts = ", ".join(
            f"{endpoint}={count}" for endpoint, count in result.endpoints.items()
        )
        _echo_step(f"Ingest: records read by endpoint: {endpoint_counts}")
    return result


def _rich_ingest_progress(progress: ProgressReporter):
    def _callback(event: IngestFileProgress) -> None:
        if event.action == "start" and event.file_index == 1:
            progress.emit(
                "Ingesting raw files",
                "start",
                current=0,
                total=event.total_files,
                unit="files",
            )
            return
        if event.action == "skipped":
            progress.emit(
                "Ingesting raw files",
                "update",
                current=event.file_index,
                total=event.total_files,
            )
            return
        if event.action == "applied":
            progress.emit(
                "Ingesting raw files",
                "update",
                current=event.file_index,
                total=event.total_files,
            )
            path = event.raw_file.path
            suffix = " delta" if event.raw_file.is_delta else ""
            progress_message(
                f"      Ingested {event.raw_file.endpoint}{suffix} from {path}: "
                f"{event.records_read} records, {event.records_upserted} upserts, "
                f"{event.records_deleted} deletes"
            )

    return _callback


def _echo_ingest_progress(event: IngestFileProgress) -> None:
    prefix = f"Ingest: [{event.file_index}/{event.total_files}]"
    path = event.raw_file.path
    run = f", run={event.raw_file.source_run_id}" if event.raw_file.source_run_id else ""
    suffix = " delta" if event.raw_file.is_delta else ""
    if event.action == "start":
        typer.echo(f"{prefix} applying {event.raw_file.endpoint}{suffix} from {path}{run}")
        return
    if event.action == "skipped":
        return
    if event.action == "applied":
        typer.echo(
            f"{prefix} applied {event.raw_file.endpoint}: "
            f"{event.records_read} records, {event.records_upserted} upserts, "
            f"{event.records_deleted} deletes"
        )


def _pop_fetch_caffeinate_flag(args: list[str]) -> bool:
    caffeinate = False
    remaining = []
    for arg in args:
        if arg == "--caffeinate":
            caffeinate = True
            continue
        remaining.append(arg)
    args[:] = remaining
    return caffeinate


def _run_caffeinated_fetch(args: list[str]) -> None:
    if os.environ.get("CENTRIC_MDM_CAFFEINATED") == "1":
        raise typer.Exit(fetcher_main(args))
    if platform.system() != "Darwin":
        typer.secho(
            "Warning: --caffeinate is only supported on macOS; running fetch normally.",
            fg=typer.colors.YELLOW,
            err=True,
        )
        raise typer.Exit(fetcher_main(args))
    caffeinate_bin = shutil.which("caffeinate")
    if caffeinate_bin is None:
        typer.secho(
            "Warning: macOS caffeinate command was not found; running fetch normally.",
            fg=typer.colors.YELLOW,
            err=True,
        )
        raise typer.Exit(fetcher_main(args))

    env = {**os.environ, "CENTRIC_MDM_CAFFEINATED": "1"}
    command = [
        caffeinate_bin,
        "-i",
        sys.executable,
        "-m",
        "centric_mdm_validation.cli",
        "fetch",
        *args[1:],
    ]
    raise typer.Exit(subprocess.call(command, env=env))


def _echo_cron_help(*, err: bool = False) -> None:
    typer.echo("Use 5-field cron syntax: minute hour day-of-month month day-of-week", err=err)
    typer.echo("Examples:", err=err)
    typer.echo("  hourly:       0 * * * *", err=err)
    typer.echo("  every 30 min: */30 * * * *", err=err)
    typer.echo("  daily 03:00:  0 3 * * *", err=err)


def _echo_reconstruction_runtime(target: str) -> None:
    runtime = inspect_reconstruction_runtime(target=target)
    if runtime.path is None:
        _echo_step("Reconstruction: using public fallback module")
    else:
        _echo_step(f"Reconstruction: using private module {runtime.path}")
    _echo_step(f"Reconstruction: strategy is {runtime.master_strategy}")
    _echo_step(f"Reconstruction: projection strategy is {runtime.projection_strategy}")


def _echo_step(message: str) -> None:
    if progress_enabled():
        return
    typer.echo(f"-> {message}")


def _echo_done(message: str) -> None:
    if progress_enabled():
        progress_message(f"OK {message}")
        return
    typer.echo(f"OK {message}")


def _parse_history_since_or_fail(value: str | None):
    try:
        return parse_history_since(value)
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--since") from exc


def _format_history_datetime(value) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M")
    return str(value)


def _print_history_table(columns: list[str], rows: list[dict[str, object]]) -> None:
    widths = {
        column: max(len(column), *(len(str(row.get(column, ""))) for row in rows))
        for column in columns
    }
    typer.echo("  ".join(column.ljust(widths[column]) for column in columns))
    typer.echo("  ".join("-" * widths[column] for column in columns))
    for row in rows:
        typer.echo("  ".join(str(row.get(column, "")).ljust(widths[column]) for column in columns))


def _fail_with_guidance(message: str, guidance: list[str]) -> None:
    typer.secho(f"Error: {message}", fg=typer.colors.RED, err=True)
    if guidance:
        typer.echo("", err=True)
        for line in guidance:
            typer.echo(line, err=True)
    raise typer.Exit(2)


def _missing_input_guidance(*, input_path: Path, target: str) -> list[str]:
    if target == "check":
        return [
            "Build the aggregate check result first:",
            "  uv run centric-mdm reconstruct",
            "",
            "Or run the whole check workflow:",
            "  uv run centric-mdm pipeline --target check",
        ]
    return [
        f"Build {target!r} records first:",
        f"  uv run centric-mdm reconstruct --target {target} --output {input_path}",
        "",
        "Or run the whole target workflow:",
        f"  uv run centric-mdm pipeline --target {target}",
    ]


def _validate(
    input_path: Path,
    rules: Path | None,
    *,
    target: str,
    progress: ProgressReporter | None = None,
):
    if not input_path.is_file():
        _fail_with_guidance(
            f"Input file not found: {input_path}",
            _missing_input_guidance(input_path=input_path, target=target),
        )
    records = read_json_records(input_path)
    return _validate_records(records, rules, target=target, progress=progress)


def _read_report_input(
    input_path: Path,
    rules: Path | None,
    *,
    target: str,
    progress: ProgressReporter | None = None,
):
    if not input_path.is_file():
        _fail_with_guidance(
            f"Input file not found: {input_path}",
            _missing_input_guidance(input_path=input_path, target=target),
        )
    records = read_json_records(input_path)
    if len(records) == 1 and _is_validation_result(records[0]):
        run = records[0]
        if target == "dpp" and not has_private_report_hook():
            return ValidationRunResult.model_validate(run)
        return run
    return _validate_records(records, rules, target=target, progress=progress)


def _is_validation_result(record: dict) -> bool:
    return {
        "results",
        "total_products",
        "ready_products",
        "readiness_percent",
    }.issubset(record)


def _write_reconstruction_for_target(
    *,
    db: Path,
    output: Path,
    target: str,
    progress: ProgressReporter | None = None,
):
    if target == "check":
        run = run_reconstruction_coverage_check(db, progress=progress)
        _write_validation_result(output, run)
        return [run]

    return write_target_reconstruction(db, output, target=target, progress=progress)


def _validate_records(
    records,
    rules: Path | None,
    *,
    target: str,
    progress: ProgressReporter | None = None,
):
    if target == "check":
        if len(records) == 1 and "relationship_coverage" in records[0]:
            return records[0]
        if progress is not None:
            progress.emit(
                "Validating check records",
                "start",
                current=0,
                total=len(records),
                unit="records",
            )
        payloads = [ReconstructionCheckPayload.model_validate(record) for record in records]
        run = ReconstructionCheckValidator().validate_many(payloads)
        if progress is not None:
            progress.emit(
                "Validating check records",
                "finish",
                total=len(records),
                message="done",
            )
        return run
    if has_private_validation_hook():
        return validate_projected_products(target, records, rules=rules, progress=progress)
    if target != "dpp":
        raise typer.BadParameter(f"Private validation required for target {target!r}.")
    payloads = [CentricProductPayload.model_validate(record) for record in records]
    return _validate_payloads(payloads, rules)


def _validate_payloads(payloads: list[CentricProductPayload], rules: Path | None):
    rule_path = resolve_private_config_path(RULES_CONFIG_PATH, rules)
    rule_set = DppRuleSet.from_yaml(rule_path)
    return DppReadinessValidator(rule_set).validate_many(payloads)


def _default_reconstruct_output(target: str) -> Path:
    if target in PIPELINE_TARGETS:
        return PIPELINE_TARGETS[target].reconstructed_output
    return DEFAULT_LATEST_RESULTS_DIR / f"{_target_slug(target)}-products.jsonl"


def _default_validate_input(target: str) -> Path:
    if target in PIPELINE_TARGETS:
        return PIPELINE_TARGETS[target].reconstructed_output
    return DEFAULT_LATEST_RESULTS_DIR / f"{_target_slug(target)}-products.jsonl"


def _default_validate_output(target: str) -> Path:
    if target in PIPELINE_TARGETS:
        return PIPELINE_TARGETS[target].validation_output
    return DEFAULT_LATEST_RESULTS_DIR / f"{_target_slug(target)}-results.json"


def _default_report_output_dir(target: str) -> Path:
    if target in PIPELINE_TARGETS:
        return PIPELINE_TARGETS[target].report_output_dir
    return Path("reports") / _target_slug(target)


def _validate_report_template(target: str, template: str) -> None:
    templates = REPORT_TEMPLATES.get(target)
    if templates is None:
        if template == "default":
            return
        raise typer.BadParameter(f"Template {template!r} is not registered for target {target!r}.")
    if template not in templates:
        choices = ", ".join(sorted(templates))
        raise typer.BadParameter(
            f"Template {template!r} is not registered for target {target!r}. "
            f"Available templates: {choices}."
        )


def _write_report_for_target(
    target: str,
    run,
    output_dir: Path,
    *,
    template: str = "default",
    progress: ProgressReporter | None = None,
) -> None:
    if target not in {"check", "dpp"} and has_private_report_hook():
        report_validation_results(target, run, output_dir, template=template, progress=progress)
        return
    if target == "check":
        if template != "default":
            raise typer.BadParameter("Only the default report template is available for check.")
        if progress is not None:
            progress.emit("Writing check report", "start", message=str(output_dir))
        ReconstructionCheckReporter().write_all(run, output_dir)
        if progress is not None:
            progress.emit("Writing check report", "finish", message=str(output_dir))
        return
    if target == "dpp":
        if has_private_report_hook():
            report_validation_results(target, run, output_dir, template=template, progress=progress)
            return
        if template != "default":
            raise typer.BadParameter(
                "Private reporting is required for DPP report template 'brands'."
            )
        if progress is not None:
            progress.emit("Writing dpp report", "start", message=str(output_dir))
        DppReadinessReporter().write_all(run, output_dir)
        if progress is not None:
            progress.emit("Writing dpp report", "finish", message=str(output_dir))
        return
    if has_private_report_hook():
        report_validation_results(target, run, output_dir, template=template, progress=progress)
        return
    raise typer.BadParameter(f"Private reporting required for target {target!r}.")


def _write_validation_result(output_path: Path, run) -> None:
    if hasattr(run, "model_dump"):
        write_json(output_path, run.model_dump(mode="json"))
    else:
        write_json(output_path, run)


def _write_validation_outputs(
    *,
    db: Path,
    target: str,
    latest_output_path: Path,
    run,
    input_path: Path | None = None,
) -> ValidationHistoryRun:
    _write_validation_result(latest_output_path, run)
    return record_validation_history(
        db,
        target=target,
        run=run,
        input_path=input_path,
        latest_result_path=latest_output_path,
    )


def _history_run_text(history_run: ValidationHistoryRun | None) -> str:
    return f". Run: {history_run.run_id}" if history_run is not None else ""


def _validation_counts(run) -> tuple[int, int]:
    total = _result_value(run, "total_products", default=0)
    ready = _result_value(run, "ready_products", default=0)
    return int(total or 0), int(ready or 0)


def _readiness_percent(run) -> float:
    return float(_result_value(run, "readiness_percent", default=0.0) or 0.0)


def _result_value(run, key: str, *, default):
    if isinstance(run, dict):
        return run.get(key, default)
    return getattr(run, key, default)


def _target_slug(target: str) -> str:
    return "".join(character if character.isalnum() else "-" for character in target).strip("-")


def _pipeline_target_config(target: str) -> PipelineTarget:
    return PIPELINE_TARGETS.get(
        target,
        PipelineTarget(
            name=target,
            reconstructed_output=_default_reconstruct_output(target),
            validation_output=_default_validate_output(target),
            report_output_dir=_default_report_output_dir(target),
        ),
    )


def _pipeline_weights(target: str, *, include_report: bool = True) -> dict[str, float]:
    if target == "dpp":
        weights = DPP_PIPELINE_WEIGHTS
    elif target == "check":
        weights = CHECK_PIPELINE_WEIGHTS
    else:
        weights = DEFAULT_PIPELINE_WEIGHTS
    if include_report:
        return weights
    kept = {key: value for key, value in weights.items() if key != "report"}
    total = sum(kept.values())
    if total <= 0:
        return kept
    return {key: value / total for key, value in kept.items()}


def _pipeline_estimates(target: str) -> dict[str, float]:
    if target == "dpp":
        return DPP_PIPELINE_ESTIMATES
    if target == "check":
        return CHECK_PIPELINE_ESTIMATES
    return DEFAULT_PIPELINE_ESTIMATES
