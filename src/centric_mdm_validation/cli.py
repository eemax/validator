from pathlib import Path
from typing import Annotated

import typer

from centric_mdm_validation.centric.cli import main as fetcher_main
from centric_mdm_validation.io import read_json_records, write_json
from centric_mdm_validation.models import CentricProductPayload
from centric_mdm_validation.reporting import DppReadinessReporter
from centric_mdm_validation.validation import DppReadinessValidator, DppRuleSet

app = typer.Typer(help="Centric MDM validation tools.")

RulesOption = Annotated[
    Path,
    typer.Option("--rules", "-r", help="DPP readiness rule YAML."),
]


@app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
def fetch(ctx: typer.Context) -> None:
    """Run Centric fetch jobs. Accepts the same arguments as `centric-fetch run`."""

    args = list(ctx.args)
    if not args or args[0] != "run":
        args.insert(0, "run")
    raise typer.Exit(fetcher_main(args))


@app.command()
def validate(
    input_path: Annotated[
        Path,
        typer.Option("--input", "-i", help="Projected product JSON/JSONL."),
    ],
    rules: RulesOption = Path("config/rules/dpp-readiness.example.yml"),
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
    rules: RulesOption = Path("config/rules/dpp-readiness.example.yml"),
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


def _validate(input_path: Path, rules: Path):
    records = read_json_records(input_path)
    payloads = [CentricProductPayload.model_validate(record) for record in records]
    rule_set = DppRuleSet.from_yaml(rules)
    return DppReadinessValidator(rule_set).validate_many(payloads)
