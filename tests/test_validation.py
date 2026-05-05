from pathlib import Path

from centric_mdm_validation.io import read_json_records
from centric_mdm_validation.models import CentricProductPayload, ReconstructionCheckPayload
from centric_mdm_validation.validation import (
    DppReadinessValidator,
    DppRuleSet,
    ReconstructionCheckValidator,
)


def test_dpp_readiness_splits_ready_and_blocked_products() -> None:
    rules = DppRuleSet.from_yaml(Path("tests/fixtures/dpp-readiness.yml"))
    records = read_json_records(Path("tests/fixtures/projected-products.json"))
    payloads = [CentricProductPayload.model_validate(record) for record in records]

    run = DppReadinessValidator(rules).validate_many(payloads)

    assert run.total_products == 3
    assert run.ready_products == 2
    assert run.readiness_percent == 66.67

    blocked = next(result for result in run.results if result.centric_style_id == "CENTRIC-1002")
    assert blocked.ready is False
    assert blocked.score == 0
    assert {issue.code for issue in blocked.issues} >= {
        "GLOBAL_STYLE_ID_MISSING",
        "GLOBAL_VARIANT_ID_MISSING",
        "DPP_REQUIRED_ATTRIBUTE_MISSING",
        "ATTRIBUTE_TYPE_INVALID",
    }


def test_warning_only_product_remains_ready() -> None:
    rules = DppRuleSet.from_yaml(Path("tests/fixtures/dpp-readiness.yml"))
    records = read_json_records(Path("tests/fixtures/projected-products.json"))
    payloads = [CentricProductPayload.model_validate(record) for record in records]

    run = DppReadinessValidator(rules).validate_many(payloads)
    fleece = next(result for result in run.results if result.centric_style_id == "CENTRIC-2001")

    assert fleece.ready is True
    assert fleece.warning_count == 2
    assert fleece.score == 90


def test_reconstruction_check_labels_reference_coverage_issues() -> None:
    payload = ReconstructionCheckPayload.model_validate(
        {
            "style_id": "S1",
            "relationship_ids": {},
            "counts": {"resolved_records": {}},
            "unresolved_refs": [
                {
                    "relation": "style.active_colorways",
                    "record_id": "C0/CBMISSING|Colorway",
                    "status": "not_seen",
                },
                {
                    "relation": "style.original_season",
                    "record_id": "FA23CUTTERBUCK",
                    "status": "invalid_ref",
                },
            ],
        }
    )

    result = ReconstructionCheckValidator().validate(payload)

    issue_codes = {issue.code for issue in result.issues}
    assert "REFERENCED_RECORD_NOT_SEEN" in issue_codes
    assert "INVALID_REFERENCE_VALUE" in issue_codes
