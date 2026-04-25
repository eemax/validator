from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field


class AttributeRule(BaseModel):
    code: str
    label: str | None = None
    data_type: str = "string"
    required: bool = True
    blocking: bool = True


class ProductTypeRule(BaseModel):
    code: str
    label: str | None = None
    dpp_template_code: str | None = None
    required_attributes: list[str] = Field(default_factory=list)
    warning_attributes: list[str] = Field(default_factory=list)


class BrandRule(BaseModel):
    code: str
    name: str
    required_attributes: list[str] = Field(default_factory=list)


class DppRuleSet(BaseModel):
    version: str
    attributes: dict[str, AttributeRule]
    product_types: dict[str, ProductTypeRule]
    brands: dict[str, BrandRule] = Field(default_factory=dict)
    score: dict[str, int] = Field(
        default_factory=lambda: {"blocking_error": 20, "warning": 5},
    )

    @classmethod
    def from_yaml(cls, path: Path) -> "DppRuleSet":
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        return cls.model_validate(_normalize_rules(data))


def _normalize_rules(data: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(data)
    normalized["attributes"] = {
        item["code"]: item for item in normalized.get("attributes", [])
    }
    normalized["product_types"] = {
        item["code"]: item for item in normalized.get("product_types", [])
    }
    normalized["brands"] = {item["code"]: item for item in normalized.get("brands", [])}
    return normalized
