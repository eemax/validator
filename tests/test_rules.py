from pathlib import Path

from centric_mdm_validation.validation import DppRuleSet


def test_loads_example_rule_set() -> None:
    rules = DppRuleSet.from_yaml(Path("tests/fixtures/dpp-readiness.yml"))

    assert rules.version == "2026.04.v1"
    assert "SHELL_JACKET" in rules.product_types
    assert "MATERIAL_COMPOSITION" in rules.attributes
    assert rules.brands["HH"].required_attributes == ["BRAND_DPP_OWNER"]
