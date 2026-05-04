from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class Severity(StrEnum):
    ERROR = "error"
    WARNING = "warning"


class FixLocation(StrEnum):
    CENTRIC = "Centric"
    MDM = "MDM"
    ERP = "ERP"
    DPP = "DPP"


class ReadinessContext(StrEnum):
    DPP = "dpp"


class CentricVariant(BaseModel):
    model_config = ConfigDict(extra="allow")

    centric_variant_id: str | None = None
    global_variant_id: str | None = None
    sku: str | None = None
    gtin: str | None = None
    color_name: str | None = None
    size_value: str | None = None
    external_ids: dict[str, str] = Field(default_factory=dict)


class CentricProductPayload(BaseModel):
    """Normalized product/style payload used by validators.

    Raw Centric shapes are expected to be projected into this model before rules run.
    Keeping this contract small makes target-specific projections easier to govern.
    """

    model_config = ConfigDict(extra="allow")

    centric_style_id: str
    brand_code: str | None = None
    brand_name: str | None = None
    global_style_id: str | None = None
    style_name: str | None = None
    product_type_code: str | None = None
    lifecycle_status: str | None = None
    season: str | None = None
    attributes: dict[str, Any] = Field(default_factory=dict)
    variants: list[CentricVariant] = Field(default_factory=list)


class ValidationIssue(BaseModel):
    code: str
    message: str
    severity: Severity
    fix_location: FixLocation
    source_field: str
    rule_id: str
    blocking: bool = True


class ProductValidationResult(BaseModel):
    centric_style_id: str
    brand_code: str | None = None
    brand_name: str | None = None
    product_type_code: str | None = None
    context: ReadinessContext = ReadinessContext.DPP
    ready: bool
    score: int
    rule_set_version: str
    issue_count: int
    blocking_issue_count: int
    warning_count: int
    issues: list[ValidationIssue]


class ValidationRunResult(BaseModel):
    context: ReadinessContext = ReadinessContext.DPP
    rule_set_version: str
    total_products: int
    ready_products: int
    readiness_percent: float
    results: list[ProductValidationResult]
