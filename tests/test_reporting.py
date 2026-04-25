from pathlib import Path

from centric_mdm_validation.io import read_json_records
from centric_mdm_validation.models import CentricProductPayload
from centric_mdm_validation.reporting import DppReadinessReporter
from centric_mdm_validation.validation import DppReadinessValidator, DppRuleSet


def test_reporter_writes_expected_files(tmp_path: Path) -> None:
    rules = DppRuleSet.from_yaml(Path("config/rules/dpp-readiness.example.yml"))
    records = read_json_records(Path("tests/fixtures/projected-products.json"))
    payloads = [CentricProductPayload.model_validate(record) for record in records]
    run = DppReadinessValidator(rules).validate_many(payloads)

    DppReadinessReporter().write_all(run, tmp_path)

    assert (tmp_path / "dpp-readiness-summary.md").exists()
    assert (tmp_path / "dpp-readiness-products.csv").exists()
    assert (tmp_path / "dpp-readiness-issues.csv").exists()
    assert (tmp_path / "dpp-readiness.xlsx").exists()

    summary = (tmp_path / "dpp-readiness-summary.md").read_text(encoding="utf-8")
    assert "66.67%" in summary
    assert "MATERIAL_COMPOSITION" in summary
