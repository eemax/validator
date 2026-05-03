from pathlib import Path
from typing import Annotated

import typer

from centric_mdm_validation.centric.cli import main as fetcher_main
from centric_mdm_validation.centric.config import resolve_private_config_path
from centric_mdm_validation.centric.mapper import load_projection_mapping, write_projected_products
from centric_mdm_validation.centric.schema import load_endpoint_schemas
from centric_mdm_validation.centric.store import ingest_raw_dir, write_reconstructed_products
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
DEFAULT_PROJECTED_PRODUCTS_PATH = Path("data/results/projected-products.jsonl")


@app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def fetch(ctx: typer.Context) -> None:
    """Run Centric fetch jobs. Accepts the same arguments as `centric-fetch run`."""

    args = list(ctx.args)
    if not args or args[0] != "run":
        args.insert(0, "run")
    raise typer.Exit(fetcher_main(args))


@app.command()
def project(
    input_dir: Annotated[
        Path,
        typer.Option(
            "--input-dir",
            "-i",
            help="Directory containing fetched endpoint JSONL files.",
        ),
    ] = Path("data/raw"),
    output: Annotated[
        Path,
        typer.Option("--output", "-o", help="Projected product JSONL."),
    ] = DEFAULT_PROJECTED_PRODUCTS_PATH,
    mapping: Annotated[
        Path | None,
        typer.Option("--mapping", "-m", help="Optional local projection field mapping YAML."),
    ] = None,
) -> None:
    """Project fetched Centric endpoint payloads into validator product payloads."""

    payloads = write_projected_products(input_dir, output, mapping)
    typer.echo(f"Projected {len(payloads)} products from {input_dir} into {output}")


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

    result = ingest_raw_dir(raw_dir, db, schemas=load_endpoint_schemas(schema))
    typer.echo(
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
        typer.Option("--output", "-o", help="Projected product JSONL."),
    ] = DEFAULT_PROJECTED_PRODUCTS_PATH,
    mapping: Annotated[
        Path | None,
        typer.Option("--mapping", "-m", help="Optional local projection field mapping YAML."),
    ] = None,
) -> None:
    """Project current reconstructed store state into validator product payloads."""

    payloads = write_reconstructed_products(db, output, mapping=load_projection_mapping(mapping))
    typer.echo(f"Reconstructed {len(payloads)} products from {db} into {output}")


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
    schema: Annotated[
        Path | None,
        typer.Option("--schema", help="Endpoint merge schema YAML."),
    ] = None,
    mapping: Annotated[
        Path | None,
        typer.Option("--mapping", "-m", help="Optional local projection field mapping YAML."),
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

    ingest_result = ingest_raw_dir(raw_dir, db, schemas=load_endpoint_schemas(schema))
    payloads = write_reconstructed_products(
        db,
        projected_output,
        mapping=load_projection_mapping(mapping),
    )
    run = _validate_payloads(payloads, rules)
    write_json(validation_output, run.model_dump(mode="json"))
    if report_output_dir is not None:
        DppReadinessReporter().write_all(run, report_output_dir)

    typer.echo(
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

    run = _validate(input_path, rules)
    write_json(output, run.model_dump(mode="json"))
    typer.echo(
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

    run = _validate(input_path, rules)
    DppReadinessReporter().write_all(run, output_dir)
    typer.echo(
        f"Wrote DPP readiness reports for {run.total_products} products into {output_dir}"
    )


def _validate(input_path: Path, rules: Path | None):
    records = read_json_records(input_path)
    payloads = [CentricProductPayload.model_validate(record) for record in records]
    return _validate_payloads(payloads, rules)


def _validate_payloads(payloads: list[CentricProductPayload], rules: Path | None):
    rule_path = resolve_private_config_path(RULES_CONFIG_PATH, rules)
    rule_set = DppRuleSet.from_yaml(rule_path)
    return DppReadinessValidator(rule_set).validate_many(payloads)
