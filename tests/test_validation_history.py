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
                "style_name": "Style One",
                "brand": "CRAFT",
                "season": "AW27",
                "ready": ready,
                "status": "passed" if ready else "failed",
                "score": 82.5,
                "issue_count": len(issues),
                "blocking_issue_count": sum(
                    1 for issue in issues if issue.get("severity") == "error"
                ),
                "hard_warning_count": sum(
                    1 for issue in issues if issue.get("warning_level") == "hard"
                ),
                "soft_warning_count": sum(
                    1 for issue in issues if issue.get("warning_level") == "soft"
                ),
                "updated_source_at": "2026-05-08T07:30:00Z",
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
            SELECT ready, issue_codes_json, display_name, brand, season, season_year, group_key,
                   score, issue_count, failure_count, hard_warning_count, soft_warning_count,
                   updated_source_at
            FROM validation_result_index_current
            WHERE target = 'md' AND product_id = 'S1'
            """
        ).fetchone()
    assert current == (
        True,
        "[]",
        "Style One",
        "CRAFT",
        "AW27",
        27,
        "CRAFT|AW27",
        82.5,
        0,
        0,
        0,
        0,
        datetime(2026, 5, 8, 7, 30),
    )


def test_validation_history_hashes_multiple_issues_stably(tmp_path: Path) -> None:
    db_path = tmp_path / "centric.duckdb"
    input_path = tmp_path / "products.jsonl"
    result_path = tmp_path / "results.json"
    input_path.write_text('{"style_id":"S1"}\n', encoding="utf-8")

    first_run = _run(
        ready=False,
        issues=[
            _issue("MISSING_COLOR", "error"),
            _issue("MISSING_SIZE", "error"),
        ],
    )
    write_json(result_path, first_run)
    first_history = record_validation_history(
        db_path,
        target="md",
        run=first_run,
        input_path=input_path,
        latest_result_path=result_path,
    )

    reordered_run = _run(
        ready=False,
        issues=[
            _issue("MISSING_SIZE", "error"),
            _issue("MISSING_COLOR", "error"),
        ],
    )
    write_json(result_path, reordered_run)
    second_history = record_validation_history(
        db_path,
        target="md",
        run=reordered_run,
        input_path=input_path,
        latest_result_path=result_path,
    )

    assert first_history.product_change_count == 1
    assert first_history.issue_change_count == 2
    assert second_history.product_change_count == 0
    assert second_history.issue_change_count == 0


def test_validation_history_updates_scoped_products_only(tmp_path: Path) -> None:
    db_path = tmp_path / "centric.duckdb"
    result_path = tmp_path / "results.json"
    full_run = {
        "rule_set_version": "test-rules",
        "total_products": 2,
        "ready_products": 1,
        "readiness_percent": 50.0,
        "results": [
            _result("S1", ready=False, issues=[_issue("MISSING_COLOR", "error")]),
            _result("S2", ready=True, issues=[]),
        ],
    }
    write_json(result_path, full_run)
    record_validation_history(
        db_path,
        target="md",
        run=full_run,
        input_path=None,
        latest_result_path=result_path,
    )

    scoped_run = {
        "rule_set_version": "test-rules",
        "total_products": 2,
        "ready_products": 2,
        "readiness_percent": 100.0,
        "results": [
            _result("S1", ready=True, issues=[]),
            _result("S3", ready=True, issues=[]),
        ],
    }
    scoped_history = record_validation_history(
        db_path,
        target="md",
        run=scoped_run,
        input_path=None,
        latest_result_path=None,
        scoped_product_ids={"S1"},
    )

    assert scoped_history.product_change_count == 1
    assert scoped_history.issue_change_count == 1
    with duckdb.connect(str(db_path), read_only=True) as conn:
        current = conn.execute(
            """
            SELECT product_id, ready
            FROM validation_result_index_current
            WHERE target = 'md'
            ORDER BY product_id
            """
        ).fetchall()
        run_row = conn.execute(
            """
            SELECT total_records, ready_records, readiness_percent, latest_result_path
            FROM validation_runs
            WHERE run_id = ?
            """,
            [scoped_history.run_id],
        ).fetchone()

    assert current == [("S1", True), ("S2", True)]
    assert run_row == (2, 2, 100.0, None)


def test_parse_history_since_supports_absolute_minutes_and_relative_durations() -> None:
    bangkok = timezone(timedelta(hours=7))
    now = datetime(2026, 5, 8, 13, 30, tzinfo=bangkok)

    assert parse_history_since("2026-05-08T12:45+07:00") == datetime(2026, 5, 8, 5, 45)
    assert parse_history_since("10h", now=now) == datetime(2026, 5, 7, 20, 30)
    assert parse_history_since("2d", now=now) == datetime(2026, 5, 6, 6, 30)
    assert parse_history_since("3m", now=now) == datetime(2026, 2, 8, 6, 30)
    assert parse_history_since("1y", now=now) == datetime(2025, 5, 8, 6, 30)


def test_validation_history_adds_dashboard_columns_to_existing_current_index(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "centric.duckdb"
    input_path = tmp_path / "products.jsonl"
    result_path = tmp_path / "results.json"
    input_path.write_text('{"style_id":"S1"}\n', encoding="utf-8")
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE validation_result_index_current (
                target VARCHAR NOT NULL,
                product_id VARCHAR NOT NULL,
                ready BOOLEAN,
                status VARCHAR,
                issue_hash VARCHAR,
                issue_codes_json VARCHAR,
                issue_severities_json VARCHAR,
                updated_at TIMESTAMP,
                run_id VARCHAR,
                PRIMARY KEY (target, product_id)
            )
            """
        )

    run = _run()
    write_json(result_path, run)
    record_validation_history(
        db_path,
        target="md",
        run=run,
        input_path=input_path,
        latest_result_path=result_path,
    )

    with duckdb.connect(str(db_path), read_only=True) as conn:
        columns = {
            row[0]
            for row in conn.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'validation_result_index_current'
                """
            ).fetchall()
        }
    assert {
        "display_name",
        "brand",
        "season",
        "season_year",
        "group_key",
        "score",
        "failure_count",
        "hard_warning_count",
        "soft_warning_count",
        "updated_source_at",
    }.issubset(columns)


def _issue(code: str, severity: str) -> dict[str, object]:
    return {
        "code": code,
        "severity": severity,
        "source_field": "records.style.active_colorways",
        "rule_id": f"rule.{code}",
        "blocking": severity == "error",
    }


def _result(style_id: str, *, ready: bool, issues: list[dict[str, object]]) -> dict[str, object]:
    return {
        "style_id": style_id,
        "style_name": f"Style {style_id}",
        "brand": "CRAFT",
        "season": "AW27",
        "ready": ready,
        "status": "passed" if ready else "failed",
        "issue_count": len(issues),
        "blocking_issue_count": sum(1 for issue in issues if issue.get("severity") == "error"),
        "issues": issues,
    }
