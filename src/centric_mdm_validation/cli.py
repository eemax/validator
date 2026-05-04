from pathlib import Path
from typing import Annotated

import typer

from centric_mdm_validation.centric.cli import main as fetcher_main
from centric_mdm_validation.centric.config import resolve_private_config_path
from centric_mdm_validation.centric.reconstruction import inspect_reconstruction_runtime
from centric_mdm_validation.centric.schema import load_endpoint_schemas
from centric_mdm_validation.centric.store import (
    IngestFileProgress,
    discover_raw_files,
    ingest_raw_dir,
    rebuild_master_reconstruction,
    write_projected_products_from_master,
)
from centric_mdm_validation.io import read_json_records, write_json
from centric_mdm_validation.models import CentricProductPayload
from centric_mdm_validation.reporting import DppReadinessReporter
from centric_mdm_validation.validation import DppReadinessValidator, DppRuleSet

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
DEFAULT_MASTER_RECONSTRUCTION_PATH = Path("data/results/master-products.jsonl")
DEFAULT_PROJECTED_PRODUCTS_PATH = Path("data/results/projected-products.jsonl")


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
        Path,
        typer.Option("--output", "-o", help="Target projection JSONL."),
    ] = DEFAULT_MASTER_RECONSTRUCTION_PATH,
    target: Annotated[
        str,
        typer.Option("--target", "-t", help="Projection target to materialize from master state."),
    ] = "master",
) -> None:
    """Build master reconstruction state and materialize a target projection."""

    _echo_reconstruction_runtime(target)
    _echo_step(f"Reconstruct: building master product graph from {db}")
    master_result = rebuild_master_reconstruction(db)
    _echo_done(
        f"Master reconstruction stored {master_result.products_reconstructed} products "
        f"({master_result.source_refs} source refs, {master_result.warnings} warnings)"
    )
    _echo_step(f"Reconstruct: projecting target {target!r} into {output}")
    payloads = write_projected_products_from_master(
        db,
        output,
        target=target,
    )
    _echo_done(f"Projected {len(payloads)} {target} payloads from master reconstruction")


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
    if target != "dpp":
        raise typer.BadParameter("Pipeline validation/reporting currently supports target 'dpp'.")
    _echo_reconstruction_runtime(target)
    _echo_step("Pipeline: building master reconstruction")
    master_result = rebuild_master_reconstruction(db)
    _echo_done(f"Master reconstruction stored {master_result.products_reconstructed} products")
    _echo_step(f"Pipeline: projecting {target} payloads")
    projected_payloads = write_projected_products_from_master(
        db,
        projected_output,
        target=target,
    )
    payloads = [_coerce_dpp_payload(payload) for payload in projected_payloads]
    _echo_done(f"Projected {len(payloads)} products into {projected_output}")
    _echo_step(f"Pipeline: validating {len(payloads)} products")
    run = _validate_payloads(payloads, rules)
    _echo_step(f"Pipeline: writing validation results to {validation_output}")
    write_json(validation_output, run.model_dump(mode="json"))
    if report_output_dir is not None:
        _echo_step(f"Pipeline: writing reports to {report_output_dir}")
        DppReadinessReporter().write_all(run, report_output_dir)

    _echo_done(
        f"Pipeline complete: {ingest_result.applied_files} raw files applied "
        f"({ingest_result.skipped_files} skipped), {len(payloads)} products reconstructed, "
        f"{run.ready_products}/{run.total_products} ready. Results: {validation_output}"
    )


@app.command()
def validate(
    input_path: Annotated[
        Path,
        typer.Option("--input", "-i", help="Projected product JSON/JSONL."),
    ],
    rules: RulesOption = None,
    output: Annotated[
        Path,
        typer.Option("--output", "-o", help="Validation result JSON."),
    ] = Path("data/results/dpp-readiness-results.json"),
) -> None:
    """Validate projected Centric products for DPP readiness."""

    _echo_step(f"Validate: reading projected products from {input_path}")
    run = _validate(input_path, rules)
    _echo_step(f"Validate: writing results to {output}")
    write_json(output, run.model_dump(mode="json"))
    _echo_done(
        f"Validated {run.total_products} products: {run.ready_products} ready "
        f"({run.readiness_percent}%). Results: {output}"
    )


@app.command()
def report(
    input_path: Annotated[
        Path,
        typer.Option("--input", "-i", help="Projected product JSON/JSONL."),
    ],
    rules: RulesOption = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", "-o", help="Directory for report files."),
    ] = Path("reports/dpp-readiness"),
) -> None:
    """Create DPP readiness reports."""

    _echo_step(f"Report: reading projected products from {input_path}")
    run = _validate(input_path, rules)
    _echo_step(f"Report: writing report files to {output_dir}")
    DppReadinessReporter().write_all(run, output_dir)
    _echo_done(
        f"Wrote DPP readiness reports for {run.total_products} products into {output_dir}"
    )


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
    _echo_step(f"Reconstruction: master strategy is {runtime.master_strategy}")
    _echo_step(f"Reconstruction: projection strategy is {runtime.projection_strategy}")


def _echo_step(message: str) -> None:
    typer.echo(f"-> {message}")


def _echo_done(message: str) -> None:
    typer.echo(f"OK {message}")


def _validate(input_path: Path, rules: Path | None):
    records = read_json_records(input_path)
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
