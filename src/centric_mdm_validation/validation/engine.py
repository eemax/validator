from typing import Any

from centric_mdm_validation.models import (
    CentricProductPayload,
    FixLocation,
    ProductValidationResult,
    Severity,
    ValidationIssue,
    ValidationRunResult,
)
from centric_mdm_validation.validation.rules import AttributeRule, DppRuleSet


class DppReadinessValidator:
    def __init__(self, rules: DppRuleSet) -> None:
        self._rules = rules

    def validate_many(self, payloads: list[CentricProductPayload]) -> ValidationRunResult:
        results = [self.validate(payload) for payload in payloads]
        ready_products = sum(1 for result in results if result.ready)
        readiness_percent = round((ready_products / len(results)) * 100, 2) if results else 0.0
        return ValidationRunResult(
            rule_set_version=self._rules.version,
            total_products=len(results),
            ready_products=ready_products,
            readiness_percent=readiness_percent,
            results=results,
        )

    def validate(self, payload: CentricProductPayload) -> ProductValidationResult:
        issues = [
            *self._identity_issues(payload),
            *self._product_type_issues(payload),
            *self._attribute_issues(payload),
            *self._identifier_issues(payload),
        ]
        blocking_errors = [
            issue for issue in issues if issue.severity == Severity.ERROR and issue.blocking
        ]
        warnings = [issue for issue in issues if issue.severity == Severity.WARNING]
        return ProductValidationResult(
            centric_style_id=payload.centric_style_id,
            brand_code=payload.brand_code,
            brand_name=payload.brand_name,
            product_type_code=payload.product_type_code,
            ready=not blocking_errors,
            score=self._score(issues),
            rule_set_version=self._rules.version,
            issue_count=len(issues),
            blocking_issue_count=len(blocking_errors),
            warning_count=len(warnings),
            issues=issues,
        )

    def _identity_issues(self, payload: CentricProductPayload) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        if not payload.global_style_id:
            issues.append(
                ValidationIssue(
                    code="GLOBAL_STYLE_ID_MISSING",
                    message="Centric payload is missing the global style ID.",
                    severity=Severity.ERROR,
                    fix_location=FixLocation.CENTRIC,
                    source_field="global_style_id",
                    rule_id="identity.global_style_id.required",
                )
            )

        if not payload.brand_code:
            issues.append(
                ValidationIssue(
                    code="BRAND_MISSING",
                    message="Centric payload is missing brand_code.",
                    severity=Severity.ERROR,
                    fix_location=FixLocation.CENTRIC,
                    source_field="brand_code",
                    rule_id="taxonomy.brand.required",
                )
            )
        elif self._rules.brands and payload.brand_code not in self._rules.brands:
            issues.append(
                ValidationIssue(
                    code="BRAND_UNKNOWN",
                    message=f"Brand {payload.brand_code} is not governed in the DPP rule set.",
                    severity=Severity.ERROR,
                    fix_location=FixLocation.MDM,
                    source_field="brand_code",
                    rule_id="taxonomy.brand.known",
                )
            )

        for index, variant in enumerate(payload.variants):
            prefix = f"variants[{index}]"
            if not variant.global_variant_id:
                variant_label = variant.sku or variant.centric_variant_id or index
                issues.append(
                    ValidationIssue(
                        code="GLOBAL_VARIANT_ID_MISSING",
                        message=f"Variant {variant_label} is missing a global variant ID.",
                        severity=Severity.ERROR,
                        fix_location=FixLocation.CENTRIC,
                        source_field=f"{prefix}.global_variant_id",
                        rule_id="identity.global_variant_id.required",
                    )
                )
            if not variant.sku:
                issues.append(
                    ValidationIssue(
                        code="SKU_MISSING",
                        message=f"Variant {variant.centric_variant_id or index} is missing SKU.",
                        severity=Severity.ERROR,
                        fix_location=FixLocation.CENTRIC,
                        source_field=f"{prefix}.sku",
                        rule_id="identity.sku.required",
                    )
                )
        return issues

    def _product_type_issues(self, payload: CentricProductPayload) -> list[ValidationIssue]:
        if not payload.product_type_code:
            return [
                ValidationIssue(
                    code="PRODUCT_TYPE_MISSING",
                    message="Centric payload is missing product_type_code.",
                    severity=Severity.ERROR,
                    fix_location=FixLocation.CENTRIC,
                    source_field="product_type_code",
                    rule_id="taxonomy.product_type.required",
                )
            ]

        product_type = self._rules.product_types.get(payload.product_type_code)
        if not product_type:
            return [
                ValidationIssue(
                    code="PRODUCT_TYPE_UNKNOWN",
                    message=f"Product type {payload.product_type_code} is not governed for DPP.",
                    severity=Severity.ERROR,
                    fix_location=FixLocation.MDM,
                    source_field="product_type_code",
                    rule_id="taxonomy.product_type.known",
                )
            ]

        if not product_type.dpp_template_code:
            return [
                ValidationIssue(
                    code="DPP_TEMPLATE_MAPPING_MISSING",
                    message=f"Product type {product_type.code} has no DPP template mapping.",
                    severity=Severity.ERROR,
                    fix_location=FixLocation.MDM,
                    source_field="product_type_code",
                    rule_id="mapping.dpp.template.required",
                )
            ]
        return []

    def _attribute_issues(self, payload: CentricProductPayload) -> list[ValidationIssue]:
        product_type = self._rules.product_types.get(payload.product_type_code or "")
        required_codes = set(product_type.required_attributes if product_type else [])
        warning_codes = set(product_type.warning_attributes if product_type else [])

        if payload.brand_code in self._rules.brands:
            required_codes.update(self._rules.brands[payload.brand_code].required_attributes)

        issues: list[ValidationIssue] = []
        for code in sorted(required_codes):
            issues.extend(self._missing_or_type_issues(payload, code, blocking=True))
        for code in sorted(warning_codes - required_codes):
            issues.extend(self._missing_or_type_issues(payload, code, blocking=False))
        return issues

    def _missing_or_type_issues(
        self,
        payload: CentricProductPayload,
        code: str,
        *,
        blocking: bool,
    ) -> list[ValidationIssue]:
        attribute = self._rules.attributes.get(code) or AttributeRule(code=code)
        value = payload.attributes.get(code)
        if value in (None, "", []):
            return [
                ValidationIssue(
                    code="DPP_REQUIRED_ATTRIBUTE_MISSING"
                    if blocking
                    else "DPP_RECOMMENDED_ATTRIBUTE_MISSING",
                    message=f"{code} is required for DPP readiness."
                    if blocking
                    else f"{code} is recommended for DPP readiness.",
                    severity=Severity.ERROR if blocking else Severity.WARNING,
                    fix_location=FixLocation.CENTRIC,
                    source_field=f"attributes.{code}",
                    rule_id=f"readiness.dpp.attribute.{code}.required",
                    blocking=blocking,
                )
            ]

        type_issue = self._type_issue(code, value, attribute)
        return [type_issue] if type_issue else []

    def _type_issue(
        self,
        code: str,
        value: Any,
        attribute: AttributeRule,
    ) -> ValidationIssue | None:
        expected = attribute.data_type
        invalid = (
            (expected == "string" and not isinstance(value, str))
            or (
                expected == "number"
                and (not isinstance(value, int | float) or isinstance(value, bool))
            )
            or (expected == "boolean" and not isinstance(value, bool))
            or (expected == "list" and not isinstance(value, list))
        )
        if not invalid:
            return None

        return ValidationIssue(
            code="ATTRIBUTE_TYPE_INVALID",
            message=f"{code} must be a {expected}.",
            severity=Severity.ERROR,
            fix_location=FixLocation.CENTRIC,
            source_field=f"attributes.{code}",
            rule_id=f"attribute.{code}.data_type",
        )

    def _identifier_issues(self, payload: CentricProductPayload) -> list[ValidationIssue]:
        if any(variant.gtin for variant in payload.variants):
            return []
        return [
            ValidationIssue(
                code="DPP_GTIN_OR_RESOLVER_ID_MISSING",
                message=(
                    "DPP readiness requires at least one GTIN or resolver "
                    "identifier candidate."
                ),
                severity=Severity.WARNING,
                fix_location=FixLocation.CENTRIC,
                source_field="variants[].gtin",
                rule_id="readiness.dpp.identifier.present",
                blocking=False,
            )
        ]

    def _score(self, issues: list[ValidationIssue]) -> int:
        score = 100
        for issue in issues:
            if issue.severity == Severity.ERROR and issue.blocking:
                score -= self._rules.score.get("blocking_error", 20)
            elif issue.severity == Severity.WARNING:
                score -= self._rules.score.get("warning", 5)
        return max(score, 0)
