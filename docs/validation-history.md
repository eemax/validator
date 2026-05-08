# Validation History

Validation history is the compact, DuckDB-native changelog layer for validation runs.

The CLI writes full validation output only to `data/results/latest/`. It does not archive full
historical result JSON. Instead, each validation run updates compact current-state indexes and
appends product/issue change events to `data/centric.duckdb`.

Raw Centric files remain the source of truth for reconstructing full historical state.

## Tables

### `validation_runs`

One row per validation run.

Important columns:

- `run_id`: stable run identifier, for example `2026-05-08T064512Z-dpp`.
- `target`: validation target, for example `check`, `dpp`, `md`, or future targets.
- `created_at`: UTC timestamp for the validation history write.
- `input_path`: projected/reconstructed input used for validation.
- `input_sha256`: SHA-256 of the validation input file when available.
- `latest_result_path`: latest result JSON path written by the run.
- `latest_result_sha256`: SHA-256 of the latest result JSON.
- `rule_set_version`: validation rule set version from the validation result.
- `total_records`: total validated records/products.
- `ready_records`: ready/passing records/products.
- `readiness_percent`: ready percentage.

### `validation_result_index_current`

Current compact validation state, one row per validated product/style per target.

This table is not append-only. For each validation run, rows for that target are replaced with the
new current index.

Important columns:

- `target`
- `product_id`
- `ready`
- `status`
- `issue_hash`
- `issue_codes_json`
- `issue_severities_json`
- `updated_at`
- `run_id`

### `validation_change_events`

Append-only product/style validation change events.

One row is written per product/style whose compact validation state changed compared with the
previous `validation_result_index_current` for the same target.

Important columns:

- `run_id`
- `target`
- `changed_at`
- `product_id`
- `change_type`: `added`, `removed`, or `changed`.
- `previous_ready`
- `current_ready`
- `previous_status`
- `current_status`
- `previous_issue_hash`
- `current_issue_hash`
- `previous_issue_codes_json`
- `current_issue_codes_json`

### `validation_issue_change_events`

Append-only issue-code change events.

One row is written per issue code added or resolved on a changed product/style.

Important columns:

- `run_id`
- `target`
- `changed_at`
- `product_id`
- `issue_code`
- `change_type`: `added` or `resolved`.
- `severity`

## Event Semantics

Validation history is scoped by target.

For example:

```bash
centric-mdm pipeline --target dpp
```

compares the new DPP validation result against the previous DPP compact index only. It does not
compare against MD or check history.

First run behavior:

- If no current index exists for a target, every validated product/style is recorded as
  `change_type = added`.
- This establishes the target baseline.
- Later runs write only real deltas compared with the previous current index.

Append-only behavior:

- `validation_runs`, `validation_change_events`, and `validation_issue_change_events` are
  append-only.
- `validation_result_index_current` is replaced per target after each validation run.

## CLI

```bash
centric-mdm history runs --target dpp
centric-mdm history changes --target dpp --since 2d
centric-mdm history issues --target dpp --since 3m
```

`--since` accepts:

- absolute dates: `2026-05-08`
- absolute date-times down to the minute: `2026-05-08T14:30`
- relative durations: `10h`, `2d`, `3m`, `1y`

`m` means months. Minutes and seconds are intentionally not supported as relative units.

## Changelog Queries

Product/style changes since a point in time:

```sql
SELECT *
FROM validation_change_events
WHERE target = 'dpp'
  AND changed_at >= TIMESTAMP '2026-05-01 00:00:00'
ORDER BY changed_at, product_id;
```

Products/styles that became ready:

```sql
SELECT *
FROM validation_change_events
WHERE target = 'dpp'
  AND previous_ready = false
  AND current_ready = true
ORDER BY changed_at, product_id;
```

Products/styles that regressed:

```sql
SELECT *
FROM validation_change_events
WHERE target = 'dpp'
  AND previous_ready = true
  AND current_ready = false
ORDER BY changed_at, product_id;
```

Issue codes added or resolved since a point in time:

```sql
SELECT issue_code, change_type, severity, COUNT(*) AS count
FROM validation_issue_change_events
WHERE target = 'dpp'
  AND changed_at >= TIMESTAMP '2026-05-01 00:00:00'
GROUP BY issue_code, change_type, severity
ORDER BY count DESC, issue_code, change_type;
```

Latest validation run per target:

```sql
SELECT *
FROM validation_runs
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY target
  ORDER BY created_at DESC
) = 1;
```

Current failing products/styles:

```sql
SELECT *
FROM validation_result_index_current
WHERE target = 'dpp'
  AND ready = false
ORDER BY product_id;
```

## Historical Truth

Validation history is a changelog/index layer, not a full historical snapshot system.

For full historical truth, reconstruct from:

- immutable raw files under `data/raw/runs/`
- applied raw file ledger in DuckDB
- the relevant public/private code and config versions

The latest full validation result is operational output. Historical validation deltas live in
DuckDB events.
