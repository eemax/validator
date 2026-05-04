from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path

from openpyxl import Workbook

from centric_mdm_validation.models import ProductValidationResult, ValidationRunResult


class ReconstructionCheckReporter:
    def write_all(self, run: ValidationRunResult, output_dir: Path) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        self.write_summary_markdown(run, output_dir / "reconstruction-check-summary.md")
        self.write_product_csv(run, output_dir / "reconstruction-check-products.csv")
        self.write_issue_csv(run, output_dir / "reconstruction-check-issues.csv")
        self.write_workbook(run, output_dir / "reconstruction-check.xlsx")

    def write_summary_markdown(self, run: ValidationRunResult, path: Path) -> None:
        issue_rows = self._issue_rows(run.results)
        source_rows = self._source_field_rows(run.results)
        lines = [
            "# Reconstruction Check Summary",
            "",
            f"- Rule set: `{run.rule_set_version}`",
            f"- Styles checked: `{run.total_products}`",
            f"- Styles without blocking graph issues: `{run.ready_products}`",
            f"- Graph readiness: `{run.readiness_percent}%`",
            "",
            "## Most Common Issues",
            "",
            "| Issue | Count |",
            "| --- | ---: |",
        ]
        lines.extend(f"| {row['code']} | {row['count']} |" for row in issue_rows)
        lines.extend(
            [
                "",
                "## Most Common Source Fields",
                "",
                "| Source field | Count |",
                "| --- | ---: |",
            ]
        )
        lines.extend(f"| {row['source_field']} | {row['count']} |" for row in source_rows)
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def write_product_csv(self, run: ValidationRunResult, path: Path) -> None:
        with path.open("w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(
                file,
                fieldnames=[
                    "style_id",
                    "ready",
                    "score",
                    "issue_count",
                    "blocking_issue_count",
                    "warning_count",
                ],
            )
            writer.writeheader()
            for result in run.results:
                writer.writerow(
                    {
                        "style_id": result.centric_style_id,
                        "ready": result.ready,
                        "score": result.score,
                        "issue_count": result.issue_count,
                        "blocking_issue_count": result.blocking_issue_count,
                        "warning_count": result.warning_count,
                    }
                )

    def write_issue_csv(self, run: ValidationRunResult, path: Path) -> None:
        with path.open("w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(
                file,
                fieldnames=[
                    "style_id",
                    "code",
                    "severity",
                    "blocking",
                    "source_field",
                    "rule_id",
                    "message",
                ],
            )
            writer.writeheader()
            for result in run.results:
                for issue in result.issues:
                    writer.writerow(
                        {
                            "style_id": result.centric_style_id,
                            "code": issue.code,
                            "severity": issue.severity,
                            "blocking": issue.blocking,
                            "source_field": issue.source_field,
                            "rule_id": issue.rule_id,
                            "message": issue.message,
                        }
                    )

    def write_workbook(self, run: ValidationRunResult, path: Path) -> None:
        workbook = Workbook()
        summary = workbook.active
        summary.title = "Summary"
        summary.append(["Metric", "Value"])
        summary.append(["Rule set", run.rule_set_version])
        summary.append(["Styles checked", run.total_products])
        summary.append(["Styles without blocking graph issues", run.ready_products])
        summary.append(["Graph readiness percent", run.readiness_percent])

        issues = workbook.create_sheet("Issues")
        issues.append(["Issue", "Count"])
        for row in self._issue_rows(run.results):
            issues.append([row["code"], row["count"]])

        products = workbook.create_sheet("Styles")
        products.append(["Style ID", "Ready", "Score", "Issues", "Blocking Issues", "Warnings"])
        for result in run.results:
            products.append(
                [
                    result.centric_style_id,
                    result.ready,
                    result.score,
                    result.issue_count,
                    result.blocking_issue_count,
                    result.warning_count,
                ]
            )

        issue_detail = workbook.create_sheet("Issue Detail")
        issue_detail.append(["Style ID", "Code", "Severity", "Blocking", "Source Field", "Message"])
        for result in run.results:
            for issue in result.issues:
                issue_detail.append(
                    [
                        result.centric_style_id,
                        issue.code,
                        issue.severity,
                        issue.blocking,
                        issue.source_field,
                        issue.message,
                    ]
                )

        workbook.save(path)

    def _issue_rows(self, results: list[ProductValidationResult]) -> list[dict[str, object]]:
        counts: Counter[str] = Counter()
        for result in results:
            for issue in result.issues:
                counts[issue.code] += 1
        return [{"code": code, "count": count} for code, count in counts.most_common()]

    def _source_field_rows(
        self,
        results: list[ProductValidationResult],
    ) -> list[dict[str, object]]:
        counts: Counter[str] = Counter()
        for result in results:
            for issue in result.issues:
                counts[issue.source_field] += 1
        return [
            {"source_field": source_field, "count": count}
            for source_field, count in counts.most_common()
        ]
