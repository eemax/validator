from pathlib import Path

import duckdb
import pytest

from centric_mdm_validation.centric.config import CONFIG_DIR_ENV_VAR
from centric_mdm_validation.endpoint_changelog import (
    list_endpoint_change_summary,
    list_endpoint_changes,
    load_endpoint_changelog_config,
    record_endpoint_changelog,
)


def test_endpoint_changelog_records_added_changed_and_removed_events(tmp_path: Path) -> None:
    db_path = tmp_path / "centric.duckdb"
    config_path = _write_changelog_config(tmp_path / "changelog.yml")
    config = load_endpoint_changelog_config(config_path)

    _write_endpoint_records(
        db_path,
        [
            ("styles", "S1", {"id": "S1", "node_name": "A", "active": True, "ignored": 1}),
            ("materials", "M1", {"id": "M1", "code": "FK1", "active": True}),
        ],
    )
    first_run = record_endpoint_changelog(db_path, config=config)
    second_run = record_endpoint_changelog(db_path, config=config)

    _write_endpoint_records(
        db_path,
        [
            ("styles", "S1", {"id": "S1", "node_name": "B", "active": True, "ignored": 1}),
        ],
    )
    third_run = record_endpoint_changelog(db_path, config=config)

    assert first_run.event_count == 2
    assert second_run.event_count == 0
    assert third_run.event_count == 2

    changes = list_endpoint_changes(db_path, limit=10)
    change_keys = {(row["endpoint"], row["record_id"], row["change_type"]) for row in changes}
    assert change_keys == {
        ("styles", "S1", "added"),
        ("materials", "M1", "added"),
        ("styles", "S1", "changed"),
        ("materials", "M1", "removed"),
    }
    style_change = next(
        row for row in changes if row["endpoint"] == "styles" and row["change_type"] == "changed"
    )
    assert style_change["changed_fields"] == ["node_name"]
    assert style_change["previous_payload"] == {
        "active": True,
        "id": "S1",
        "node_name": "A",
    }
    assert style_change["current_payload"] == {
        "active": True,
        "id": "S1",
        "node_name": "B",
    }

    summary = list_endpoint_change_summary(db_path)
    assert {(row["endpoint"], row["change_type"], row["count"]) for row in summary} == {
        ("materials", "added", 1),
        ("materials", "removed", 1),
        ("styles", "added", 1),
        ("styles", "changed", 1),
    }


def test_endpoint_changelog_config_resolves_from_private_config_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_dir = tmp_path / "private"
    config_dir.mkdir()
    _write_changelog_config(config_dir / "changelog.yml")
    monkeypatch.setenv(CONFIG_DIR_ENV_VAR, str(config_dir))

    config = load_endpoint_changelog_config()

    assert config.path == config_dir / "changelog.yml"
    assert sorted(config.endpoints) == ["materials", "styles"]


def _write_changelog_config(path: Path) -> Path:
    path.write_text(
        """
defaults:
  include_missing: false
  sort_arrays: true
endpoints:
  styles:
    fields:
      - id
      - node_name
      - active
  materials:
    fields:
      - id
      - code
      - active
""",
        encoding="utf-8",
    )
    return path


def _write_endpoint_records(
    db_path: Path,
    rows: list[tuple[str, str, dict[str, object]]],
) -> None:
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS endpoint_records (
                endpoint VARCHAR NOT NULL,
                record_id VARCHAR NOT NULL,
                payload VARCHAR NOT NULL,
                PRIMARY KEY (endpoint, record_id)
            )
            """
        )
        conn.execute("DELETE FROM endpoint_records")
        conn.executemany(
            "INSERT INTO endpoint_records (endpoint, record_id, payload) VALUES (?, ?, ?)",
            [(endpoint, record_id, _json(payload)) for endpoint, record_id, payload in rows],
        )


def _json(payload: dict[str, object]) -> str:
    import json

    return json.dumps(payload, sort_keys=True)
