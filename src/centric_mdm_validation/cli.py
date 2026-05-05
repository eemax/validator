from pathlib import Path
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
    rebuild_master_reconstruction,
    write_projected_products_from_master,
)
from centric_mdm_validation.io import read_json_records, write_json
from centric_mdm_validation.models import CentricProductPayload, ReconstructionCheckPayload
from centric_mdm_validation.reporting import DppReadinessReporter, ReconstructionCheckReporter
from centric_mdm_validation.validation import (
    DppReadinessValidator,
    DppRuleSet,
    ReconstructionCheckValidator,
)

app = typer.Typer(help="Centric MDM validation tools.")

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
RULES_CONFIG_PATH = Path("rules/dpp-readiness.yml")
DEFAULT_DB_PATH = Path("data/centric.duckdb")
DEFAULT_RECONSTRUCTION_CHECK_PATH = Path("data/results/reconstruction-check.jsonl")
DEFAULT_RECONSTRUCTION_CHECK_RESULTS_PATH = Path("data/results/reconstruction-check-results.json")
DEFAULT_PROJECTED_PRODUCTS_PATH = Path("data/results/projected-products.jsonl")
DEFAULT_DPP_RESULTS_PATH = Path("data/results/dpp-readiness-results.json")
DEFAULT_RECONSTRUCTION_CHECK_REPORT_DIR = Path("reports/reconstruction-check")
DEFAULT_DPP_REPORT_DIR = Path("reports/dpp-readiness")


@app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def fetch(ctx: typer.Context) -> None:
    """Run Centric fetch jobs. Accepts the same arguments as `centric-fetch run`."""

    args = list(ctx.args)
    if not args or args[0] != "run":
        args.insert(0, "run")
    raise typer.Exit(fetcher_main(args))


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
) -> None:
    """Catch up the DuckDB reconstruction store from immutable raw endpoint files."""

    result = _run_ingest(raw_dir=raw_dir, db=db, schema=schema)
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
        typer.Option("--output", "-o", help="Output JSONL."),
    ] = None,
    target: Annotated[
        str,
        typer.Option("--target", "-t", help="Projection target to materialize."),
    ] = "check",
) -> None:
    """Build reconstruction state and materialize a compact check or target projection."""

    output_path = output or _default_reconstruct_output(target)
    _echo_reconstruction_runtime(target)
    _echo_step(f"Reconstruct: building style relationship state from {db}")
    master_result = rebuild_master_reconstruction(db)
    _echo_done(
        f"Reconstruction stored {master_result.products_reconstructed} styles "
        f"({master_result.source_refs} source refs, {master_result.warnings} warnings)"
    )
    _echo_step(f"Reconstruct: writing target {target!r} into {output_path}")
    payloads = write_projected_products_from_master(
        db,
        output_path,
        target=target,
    )
    _echo_done(f"Wrote {len(payloads)} {target} records to {output_path}")


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
    projected_output: Annotated[
        Path,
        typer.Option("--projected-output", help="Projected product JSONL."),
    ] = DEFAULT_PROJECTED_PRODUCTS_PATH,
    target: Annotated[
        str,
        typer.Option("--target", "-t", help="Required projection target to validate/report."),
    ] = ...,
    schema: Annotated[
        Path | None,
        typer.Option("--schema", help="Endpoint merge schema YAML."),
    ] = None,
    rules: RulesOption = None,
    validation_output: Annotated[
        Path,
        typer.Option("--validation-output", help="Validation result JSON."),
    ] = Path("data/results/dpp-readiness-results.json"),
    report_output_dir: Annotated[
        Path | None,
        typer.Option("--report-output-dir", help="Optional directory for report files."),
    ] = None,
) -> None:
    """Ingest raw files, reconstruct products, validate them, and optionally write reports."""

    _echo_step("Pipeline: starting ingest")
    ingest_result = _run_ingest(raw_dir=raw_dir, db=db, schema=schema)
    _echo_reconstruction_runtime(target)
    _echo_step("Pipeline: building reconstruction state")
    master_result = rebuild_master_reconstruction(db)
    _echo_done(f"Reconstruction stored {master_result.products_reconstructed} styles")
    _echo_step(f"Pipeline: projecting {target} payloads")
    projected_payloads = write_projected_products_from_master(
        db,
        projected_output,
        target=target,
    )
    _echo_done(f"Projected {len(projected_payloads)} products into {projected_output}")
    _echo_step(f"Pipeline: validating {len(projected_payloads)} products")
    run = _validate_records(projected_payloads, rules, target=target)
    _echo_step(f"Pipeline: writing validation results to {validation_output}")
    _write_validation_result(validation_output, run)
    if report_output_dir is not None:
        _echo_step(f"Pipeline: writing reports to {report_output_dir}")
        _write_report_for_target(target, run, report_output_dir)

    total, ready = _validation_counts(run)
    _echo_done(
        f"Pipeline complete: {ingest_result.applied_files} raw files applied "
        f"({ingest_result.skipped_files} skipped), "
        f"{len(projected_payloads)} products reconstructed, "
        f"{ready}/{total} ready. Results: {validation_output}"
    )


@app.command()
def validate(
    input_path: Annotated[
        Path | None,
        typer.Option("--input", "-i", help="Input JSON/JSONL."),
    ] = None,
    target: Annotated[
        str,
        typer.Option("--target", "-t", help="Validation target."),
    ] = "check",
    rules: RulesOption = None,
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Validation result JSON."),
    ] = None,
) -> None:
    """Validate reconstruction check or projected target payloads."""

    input_file = input_path or _default_validate_input(target)
    output_file = output or _default_validate_output(target)
    _echo_step(f"Validate: reading {target} records from {input_file}")
    run = _validate(input_file, rules, target=target)
    _echo_step(f"Validate: writing results to {output_file}")
    _write_validation_result(output_file, run)
    total, ready = _validation_counts(run)
    readiness = _readiness_percent(run)
    _echo_done(
        f"Validated {total} records: {ready} ready "
        f"({readiness}%). Results: {output_file}"
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
) -> None:
    """Create reconstruction check or target readiness reports."""

    input_file = input_path or _default_validate_input(target)
    output_path = output_dir or _default_report_output_dir(target)
    _echo_step(f"Report: reading {target} records from {input_file}")
    run = _validate(input_file, rules, target=target)
    _echo_step(f"Report: writing report files to {output_path}")
    _write_report_for_target(target, run, output_path)
    total, _ = _validation_counts(run)
    _echo_done(f"Wrote {target} reports for {total} records into {output_path}")


def _run_ingest(raw_dir: Path, db: Path, schema: Path | None):
    raw_files = discover_raw_files(raw_dir)
    _echo_step(f"Ingest: discovered {len(raw_files)} raw JSONL files under {raw_dir}")
    _echo_step(f"Ingest: updating DuckDB store at {db}")
    result = ingest_raw_dir(
        raw_dir,
        db,
        schemas=load_endpoint_schemas(schema),
        progress=_echo_ingest_progress,
    )
    if result.endpoints:
        endpoint_counts = ", ".join(
            f"{endpoint}={count}" for endpoint, count in result.endpoints.items()
        )
        _echo_step(f"Ingest: records read by endpoint: {endpoint_counts}")
    return result


def _echo_ingest_progress(event: IngestFileProgress) -> None:
    prefix = f"Ingest: [{event.file_index}/{event.total_files}]"
    path = event.raw_file.path
    run = f", run={event.raw_file.source_run_id}" if event.raw_file.source_run_id else ""
    suffix = " delta" if event.raw_file.is_delta else ""
    if event.action == "start":
        typer.echo(f"{prefix} applying {event.raw_file.endpoint}{suffix} from {path}{run}")
        return
    if event.action == "skipped":
        typer.echo(f"{prefix} skipped already-applied {event.raw_file.endpoint} from {path}{run}")
        return
    if event.action == "applied":
        typer.echo(
            f"{prefix} applied {event.raw_file.endpoint}: "
            f"{event.records_read} records, {event.records_upserted} upserts, "
            f"{event.records_deleted} deletes"
        )


def _echo_reconstruction_runtime(target: str) -> None:
    runtime = inspect_reconstruction_runtime(target=target)
    if runtime.path is None:
        _echo_step("Reconstruction: using public fallback module")
    else:
        _echo_step(f"Reconstruction: using private module {runtime.path}")
    _echo_step(f"Reconstruction: strategy is {runtime.master_strategy}")
    _echo_step(f"Reconstruction: projection strategy is {runtime.projection_strategy}")


def _echo_step(message: str) -> None:
    typer.echo(f"-> {message}")


def _echo_done(message: str) -> None:
    typer.echo(f"OK {message}")


def _validate(input_path: Path, rules: Path | None, *, target: str):
    records = read_json_records(input_path)
    return _validate_records(records, rules, target=target)


def _validate_records(records, rules: Path | None, *, target: str):
    if target == "check":
        payloads = [ReconstructionCheckPayload.model_validate(record) for record in records]
        return ReconstructionCheckValidator().validate_many(payloads)
    if has_private_validation_hook():
        return validate_projected_products(target, records, rules=rules)
    if target != "dpp":
        raise typer.BadParameter(f"Private validation required for target {target!r}.")
    payloads = [CentricProductPayload.model_validate(record) for record in records]
    return _validate_payloads(payloads, rules)


def _coerce_dpp_payload(payload) -> CentricProductPayload:
    if hasattr(payload, "model_dump"):
        payload = payload.model_dump(mode="json", exclude_none=True)
    return CentricProductPayload.model_validate(payload)


def _validate_payloads(payloads: list[CentricProductPayload], rules: Path | None):
    rule_path = resolve_private_config_path(RULES_CONFIG_PATH, rules)
    rule_set = DppRuleSet.from_yaml(rule_path)
    return DppReadinessValidator(rule_set).validate_many(payloads)


def _default_reconstruct_output(target: str) -> Path:
    if target == "check":
        return DEFAULT_RECONSTRUCTION_CHECK_PATH
    if target == "dpp":
        return DEFAULT_PROJECTED_PRODUCTS_PATH
    return Path("data/results") / f"{_target_slug(target)}-products.jsonl"


def _default_validate_input(target: str) -> Path:
    if target == "check":
        return DEFAULT_RECONSTRUCTION_CHECK_PATH
    if target == "dpp":
        return DEFAULT_PROJECTED_PRODUCTS_PATH
    return Path("data/results") / f"{_target_slug(target)}-products.jsonl"


def _default_validate_output(target: str) -> Path:
    if target == "check":
        return DEFAULT_RECONSTRUCTION_CHECK_RESULTS_PATH
    if target == "dpp":
        return DEFAULT_DPP_RESULTS_PATH
    return Path("data/results") / f"{_target_slug(target)}-results.json"


def _default_report_output_dir(target: str) -> Path:
    if target == "check":
        return DEFAULT_RECONSTRUCTION_CHECK_REPORT_DIR
    if target == "dpp":
        return DEFAULT_DPP_REPORT_DIR
    return Path("reports") / _target_slug(target)


def _write_report_for_target(target: str, run, output_dir: Path) -> None:
    if target not in {"check", "dpp"} and has_private_report_hook():
        report_validation_results(target, run, output_dir)
        return
    if target == "check":
        ReconstructionCheckReporter().write_all(run, output_dir)
        return
    if target == "dpp":
        if has_private_report_hook():
            report_validation_results(target, run, output_dir)
            return
        DppReadinessReporter().write_all(run, output_dir)
        return
    if has_private_report_hook():
        report_validation_results(target, run, output_dir)
        return
    raise typer.BadParameter(f"Private reporting required for target {target!r}.")


def _write_validation_result(output_path: Path, run) -> None:
    if hasattr(run, "model_dump"):
        write_json(output_path, run.model_dump(mode="json"))
    else:
        write_json(output_path, run)


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
