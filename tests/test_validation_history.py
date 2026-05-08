from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

from centric_mdm_validation.io import write_json
from centric_mdm_validation.validation_history import (
    list_validation_changes,
    list_validation_issue_counts,
    parse_history_since,
    record_validation_history,
)


def _run(*, ready=True, issues=None):
    issues = issues or []
    return {
        "rule_set_version": "test-rules",
        "total_products": 1,
        "ready_products": 1 if ready else 0,
        "readiness_percent": 100.0 if ready else 0.0,
        "results": [
            {
                "style_id": "S1",
                "ready": ready,
                "issues": issues,
            }
        ],
    }


def test_validation_history_records_current_index_and_change_events(tmp_path: Path) -> None:
    db_path = tmp_path / "centric.duckdb"
    input_path = tmp_path / "products.jsonl"
    result_path = tmp_path / "results.json"
    input_path.write_text('{"style_id":"S1"}\n', encoding="utf-8")

    first_run = _run(ready=False, issues=[_issue("MISSING_COLOR", "error")])
    write_json(result_path, first_run)
    first_history = record_validation_history(
        db_path,
        target="md",
        run=first_run,
        input_path=input_path,
        latest_result_path=result_path,
    )

    unchanged_run = _run(ready=False, issues=[_issue("MISSING_COLOR", "error")])
    write_json(result_path, unchanged_run)
    second_history = record_validation_history(
        db_path,
        target="md",
        run=unchanged_run,
        input_path=input_path,
        latest_result_path=result_path,
    )

    fixed_run = _run(ready=True, issues=[])
    write_json(result_path, fixed_run)
    third_history = record_validation_history(
        db_path,
        target="md",
        run=fixed_run,
        input_path=input_path,
        latest_result_path=result_path,
    )

    assert first_history.product_change_count == 1
    assert first_history.issue_change_count == 1
    assert second_history.product_change_count == 0
    assert second_history.issue_change_count == 0
    assert third_history.product_change_count == 1
    assert third_history.issue_change_count == 1

    changes = list_validation_changes(db_path, target="md")
    assert [row["change_type"] for row in changes] == ["changed", "added"]

    issue_counts = list_validation_issue_counts(db_path, target="md")
    assert {(row["issue_code"], row["change_type"], row["count"]) for row in issue_counts} == {
        ("MISSING_COLOR", "added", 1),
        ("MISSING_COLOR", "resolved", 1),
    }

    with duckdb.connect(str(db_path), read_only=True) as conn:
        current = conn.execute(
            """
            SELECT ready, issue_codes_json
            FROM validation_result_index_current
            WHERE target = 'md' AND product_id = 'S1'
            """
        ).fetchone()
    assert current == (True, "[]")


def test_parse_history_since_supports_absolute_minutes_and_relative_durations() -> None:
    bangkok = timezone(timedelta(hours=7))
    now = datetime(2026, 5, 8, 13, 30, tzinfo=bangkok)

    assert parse_history_since("2026-05-08T12:45+07:00") == datetime(2026, 5, 8, 5, 45)
    assert parse_history_since("10h", now=now) == datetime(2026, 5, 7, 20, 30)
    assert parse_history_since("2d", now=now) == datetime(2026, 5, 6, 6, 30)
    assert parse_history_since("3m", now=now) == datetime(2026, 2, 8, 6, 30)
    assert parse_history_since("1y", now=now) == datetime(2025, 5, 8, 6, 30)


def _issue(code: str, severity: str) -> dict[str, object]:
    return {
        "code": code,
        "severity": severity,
        "source_field": "records.style.active_colorways",
        "rule_id": f"rule.{code}",
        "blocking": severity == "error",
    }
