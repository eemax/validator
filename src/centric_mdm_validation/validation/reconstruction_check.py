from __future__ import annotations

from typing import Any

from centric_mdm_validation.models import (
    FixLocation,
    ProductValidationResult,
    ReadinessContext,
    ReconstructionCheckPayload,
    Severity,
    ValidationIssue,
    ValidationRunResult,
)

RULE_SET_VERSION = "reconstruction-check-v1"


class ReconstructionCheckValidator:
    """Validate compact reconstruction coverage records."""

    def validate_many(
        self,
        payloads: list[ReconstructionCheckPayload],
    ) -> ValidationRunResult:
        results = [self.validate(payload) for payload in payloads]
        ready_products = sum(1 for result in results if result.ready)
        readiness_percent = round((ready_products / len(results)) * 100, 2) if results else 0.0
        return ValidationRunResult(
            context=ReadinessContext.RECONSTRUCTION_CHECK,
            rule_set_version=RULE_SET_VERSION,
            total_products=len(results),
            ready_products=ready_products,
            readiness_percent=readiness_percent,
            results=results,
        )

    def validate(self, payload: ReconstructionCheckPayload) -> ProductValidationResult:
        issues = [
            *self._missing_relationship_issues(payload),
            *self._resolution_issues(payload),
            *self._unresolved_ref_issues(payload),
            *self._warning_issues(payload),
        ]
        blocking_errors = [
            issue for issue in issues if issue.severity == Severity.ERROR and issue.blocking
        ]
        warnings = [issue for issue in issues if issue.severity == Severity.WARNING]
        return ProductValidationResult(
            context=ReadinessContext.RECONSTRUCTION_CHECK,
            centric_style_id=payload.style_id,
            ready=not blocking_errors,
            score=self._score(issues),
            rule_set_version=RULE_SET_VERSION,
            issue_count=len(issues),
            blocking_issue_count=len(blocking_errors),
            warning_count=len(warnings),
            issues=issues,
        )

    def _missing_relationship_issues(
        self,
        payload: ReconstructionCheckPayload,
    ) -> list[ValidationIssue]:
        resolved_counts = _count_bucket(payload, "resolved_records")
        checks = {
            "colorways": "Style has no resolved colorway records.",
            "sizes": "Style has no resolved size records.",
            "boms": "Style has no resolved BOM revision records.",
            "bom_rows": "Style has no resolved BOM row records.",
            "materials": "Style has no resolved material records.",
            "supplier_quotes": "Style has no resolved supplier quote records.",
            "factories": "Style has no resolved factory records.",
            "suppliers": "Style has no resolved supplier records.",
        }
        issues: list[ValidationIssue] = []
        for bucket, message in checks.items():
            if resolved_counts.get(bucket, 0) == 0:
                issues.append(
                    ValidationIssue(
                        code=f"{bucket.upper()}_MISSING",
                        message=message,
                        severity=Severity.WARNING,
                        fix_location=FixLocation.CENTRIC,
                        source_field=f"counts.resolved_records.{bucket}",
                        rule_id=f"reconstruction.relationship.{bucket}.present",
                        blocking=False,
                    )
                )
        return issues

    def _resolution_issues(
        self,
        payload: ReconstructionCheckPayload,
    ) -> list[ValidationIssue]:
        declared_counts = _count_bucket(payload, "relationship_ids")
        resolved_counts = _count_bucket(payload, "resolved_records")
        mappings = {
            "style_colorway_ids": "colorways",
            "style_size_ids": "sizes",
            "bom_revision_ids": "boms",
            "bom_row_ids": "bom_rows",
            "material_ids": "materials",
            "supplier_quote_revision_ids": "supplier_quotes",
            "factory_ids": "factories",
            "supplier_ids": "suppliers",
            "season_ids": "seasons",
        }
        issues: list[ValidationIssue] = []
        for relationship_key, resolved_key in mappings.items():
            declared = declared_counts.get(relationship_key, 0)
            resolved = resolved_counts.get(resolved_key, 0)
            if declared > resolved:
                issues.append(
                    ValidationIssue(
                        code="RELATIONSHIP_RECORDS_UNRESOLVED",
                        message=(
                            f"{relationship_key} declares {declared} ids, but only "
                            f"{resolved} matching {resolved_key} records were resolved."
                        ),
                        severity=Severity.ERROR,
                        fix_location=FixLocation.MDM,
                        source_field=f"relationship_ids.{relationship_key}",
                        rule_id=f"reconstruction.relationship.{relationship_key}.resolved",
                    )
                )
        return issues

    def _unresolved_ref_issues(
        self,
        payload: ReconstructionCheckPayload,
    ) -> list[ValidationIssue]:
        return [
            ValidationIssue(
                code=_unresolved_code(ref),
                message=_unresolved_message(ref),
                severity=Severity.ERROR,
                fix_location=FixLocation.MDM,
                source_field="unresolved_refs",
                rule_id=f"reconstruction.refs.{_unresolved_status(ref)}",
            )
            for ref in payload.unresolved_refs
        ]

    def _warning_issues(
        self,
        payload: ReconstructionCheckPayload,
    ) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        for warning in payload.warnings:
            code = str(warning.get("code") or "RECONSTRUCTION_WARNING")
            message = str(warning.get("message") or code)
            issues.append(
                ValidationIssue(
                    code=code,
                    message=message,
                    severity=Severity.WARNING,
                    fix_location=FixLocation.MDM,
                    source_field="warnings",
                    rule_id=f"reconstruction.warning.{code.lower()}",
                    blocking=False,
                )
            )
        return issues

    def _score(self, issues: list[ValidationIssue]) -> int:
        penalty = 0
        for issue in issues:
            penalty += 20 if issue.severity == Severity.ERROR else 5
        return max(0, 100 - penalty)


def _count_bucket(payload: ReconstructionCheckPayload, key: str) -> dict[str, int]:
    counts = payload.counts.get(key)
    if not isinstance(counts, dict):
        return {}
    return {
        str(name): int(value)
        for name, value in counts.items()
        if isinstance(value, int | float)
    }


def _unresolved_message(ref: dict[str, Any]) -> str:
    status = _unresolved_status(ref)
    relation = ref.get("relation") or ref.get("relation_type") or "relationship"
    endpoint = ref.get("endpoint") or ref.get("source_endpoint") or "endpoint"
    record_id = ref.get("record_id") or ref.get("source_record_id") or ref.get("id")
    if status == "invalid_ref":
        if record_id:
            return f"Invalid {relation} reference value {record_id}; it is not a Centric record id."
        return f"Invalid {relation} reference value; it is not a Centric record id."
    if status == "not_seen":
        if record_id:
            return (
                f"{relation} references {endpoint} record {record_id}, "
                "but that record was not seen in the current ingested data store."
            )
        return (
            f"{relation} references {endpoint}, but that record was not seen "
            "in the current ingested data store."
        )
    if record_id:
        return f"Unresolved {relation} reference to {endpoint} record {record_id}."
    return f"Unresolved {relation} reference to {endpoint}."


def _unresolved_code(ref: dict[str, Any]) -> str:
    status = _unresolved_status(ref)
    if status == "invalid_ref":
        return "INVALID_REFERENCE_VALUE"
    if status == "not_seen":
        return "REFERENCED_RECORD_NOT_SEEN"
    return "UNRESOLVED_RECONSTRUCTION_REF"


def _unresolved_status(ref: dict[str, Any]) -> str:
    status = ref.get("status")
    return str(status or "unresolved")
