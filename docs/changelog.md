# Endpoint Changelog

The endpoint changelog tracks semantic changes in current Centric endpoint state without storing
duplicate full endpoint payloads.

It sits next to validation history:

```text
raw JSONL evidence
  -> DuckDB endpoint_records current state
  -> selected endpoint fields
  -> endpoint changelog current index
  -> append-only endpoint change events
```

The raw fetch files remain the full historical evidence. The changelog is the compact operational
answer to: "Which meaningful endpoint fields changed since a point in time?"

## Commands

Record a changelog run:

```bash
uv run centric-mdm changelog update
```

Inspect runs and changes:

```bash
uv run centric-mdm changelog runs
uv run centric-mdm changelog summary --since 2d
uv run centric-mdm changelog changes --endpoint styles --since 10h
```

`--since` accepts absolute dates/times such as `2026-05-08` or `2026-05-08T14:30`, and relative
durations `10h`, `2d`, `3m`, or `1y`. The `m` unit means months.

## Config

The default config is private:

1. `CENTRIC_CONFIG_DIR/changelog.yml`
2. `.local/changelog.yml` when `CENTRIC_CONFIG_DIR` is not set

You can also pass `--config path/to/changelog.yml`.

Example:

```yaml
defaults:
  include_missing: false
  drop_empty: false
  sort_arrays: false

endpoints:
  styles:
    fields:
      - id
      - node_name
      - active
      - parent_season
      - product_sizes
      - active_colorways

  materials:
    fields:
      - id
      - node_name
      - active
      - default_quote
      - composition
```

Only configured endpoints and fields are tracked.

Field paths can be dotted for nested objects. Missing fields are omitted unless `include_missing`
is true. Empty values are preserved unless `drop_empty` is true. Object keys are canonicalized for
stable hashing. Arrays keep Centric order unless `sort_arrays` is true.

## Tables

### `endpoint_changelog_runs`

One row per changelog update.

Important columns:

- `run_id`: unique run id.
- `created_at`: UTC timestamp for the changelog write.
- `config_path`: field-selection config used for the run.
- `config_sha256`: hash of that config.
- `endpoint_count`: configured endpoints.
- `record_count`: current endpoint records tracked.
- `event_count`: change events written.

### `endpoint_changelog_index_current`

Current compact tracked state, one row per configured endpoint record.

This table is not append-only. Each changelog update replaces rows for endpoints in the current
config with the latest tracked payload hash and compact payload JSON.

### `endpoint_change_events`

Append-only endpoint record change events.

Important columns:

- `endpoint`
- `record_id`
- `change_type`: `added`, `changed`, or `removed`.
- `changed_fields_json`: top-level tracked fields that changed.
- `previous_payload_json`: previous compact tracked payload, or null for added records.
- `current_payload_json`: current compact tracked payload, or null for removed records.

First run behavior is intentional: the first changelog update records all configured current
records as `added` baseline events. Later runs only record changes relative to the current compact
index.

## Query Examples

Changes by endpoint:

```sql
SELECT endpoint, change_type, COUNT(*) AS changes
FROM endpoint_change_events
WHERE changed_at >= TIMESTAMP '2026-05-08 00:00:00'
GROUP BY endpoint, change_type
ORDER BY endpoint, change_type;
```

Latest style changes:

```sql
SELECT changed_at, record_id, change_type, changed_fields_json
FROM endpoint_change_events
WHERE endpoint = 'styles'
ORDER BY changed_at DESC
LIMIT 50;
```

Payload before/after for a record:

```sql
SELECT changed_at, change_type, previous_payload_json, current_payload_json
FROM endpoint_change_events
WHERE endpoint = 'materials'
  AND record_id = 'C0/CBFK3029|Material'
ORDER BY changed_at DESC;
```
