# Centric MDM Validation

Focused validator for Centric product data, starting with DPP readiness.

The project fetches or receives Centric product data, ingests it into a local DuckDB
reconstruction store, runs an aggregate reconstruction coverage check, materializes
target-specific validation payloads, runs governed rules, and creates readiness reports.

## Current State

- Python package managed by `uv`
- CLI command: `centric-mdm`
- YAML-driven DPP readiness rules
- Pydantic models for target validation payloads
- Product-level DPP validation with issue source fields and fix locations
- CSV, XLSX, and Markdown readiness reports
- Example payloads and tests
- Centric API fetcher with pagination, retries, checkpoints, resume, delta mode,
  count preflight, ID integrity checks, and structured logs
- DuckDB-backed ingest/reconstruction path for applying full and delta raw endpoint files
- Default reconstruction check reporting for aggregate endpoint/reference coverage
- DuckDB endpoint changelog for selected semantic endpoint field changes

## Install

```bash
uv sync --dev
```

## Default Reconstruction Check

The no-argument command path is the reconstruction check. It is an aggregate store coverage
status, not a product payload:

```bash
uv run centric-mdm reconstruct
uv run centric-mdm validate
uv run centric-mdm report
uv run centric-mdm examples
```

Defaults:

- `reconstruct` writes `data/results/latest/check-results.json`
- `validate` reads that file, refreshes latest results, and records compact DuckDB history events
- `report` reads that file and writes `reports/reconstruction-check/`

The check result contains counts only: endpoint record counts, declared refs, seen refs, missing
refs, invalid refs, relationship coverage, unresolved ref counts, and issue counts. Full endpoint
records stay in DuckDB instead of being duplicated into check output.

## Validate Public DPP Fixture

```bash
uv run centric-mdm validate \
  --target dpp \
  --input tests/fixtures/projected-products.json \
  --rules tests/fixtures/dpp-readiness.yml \
  --output data/results/latest/dpp-results.json
```

## Ingest And Reconstruct

Raw endpoint files are immutable evidence. A full fetch may produce files such as:

```text
data/raw/styles.jsonl
data/raw/colorways.jsonl
data/raw/materials.jsonl
```

Delta fetches are written under immutable run directories:

```text
data/raw/runs/2026-04-30T090000Z/styles.delta.jsonl
data/raw/runs/2026-04-30T090000Z/bomrows.delta.jsonl
data/raw/runs/2026-04-30T090000Z/manifest.json
```

Catch the local DuckDB store up to all unapplied raw files:

```bash
uv run centric-mdm ingest \
  --raw-dir data/raw \
  --db data/centric.duckdb
```

When a changelog config exists, `ingest` also updates the endpoint changelog for affected record
ids. Use `--no-changelog` to skip that chained update.

Build the aggregate reconstruction check from the current DuckDB state:

```bash
uv run centric-mdm reconstruct \
  --db data/centric.duckdb \
  --output data/results/latest/check-results.json
```

Use explicit targets for target-specific reconstruction. Current targets are `check`, `dpp`,
and `md`; `packaging` is expected later.

Or run ingest, reconstruct, validation, and optional reporting together for an explicit target:

```bash
uv run centric-mdm pipeline --target dpp
```

Long-running commands show live progress by default in interactive terminals. Use
`--no-progress` for plain milestone output, or `--progress` to force progress output where the
terminal is not detected as interactive:

```bash
uv run centric-mdm pipeline --target dpp --progress
```

`pipeline` writes the registered default outputs for the target. For `dpp`, that means
`data/results/latest/dpp-products.jsonl`, `data/results/latest/dpp-results.json`,
DuckDB validation history rows, and `reports/dpp-readiness/`. Use
`--reconstruction-output`, `--validation-output`, or `--report-output-dir` only when you want to
override those defaults.

Use `--no-report` when you only want ingest, reconstruction, validation, latest results, and
validation run history:

```bash
uv run centric-mdm pipeline --target dpp --no-report
```

Validation history is stored in DuckDB as compact append-only change events, not duplicated full
result JSON. The full latest result remains in `data/results/latest/` for reporting, while raw
fetch runs remain the source of truth for full historical reconstruction. The current validation
index also keeps nullable dashboard columns such as display name, brand, season, score, and issue
severity counts for fast latest-state filtering.

```bash
uv run centric-mdm history runs --target dpp
uv run centric-mdm history changes --target dpp --since 2d
uv run centric-mdm history issues --target dpp --since 3m
```

`--since` accepts absolute dates/times such as `2026-05-08` or `2026-05-08T14:30`, and relative
durations `10h`, `2d`, `3m`, or `1y`. The `m` unit means months, not minutes.

See [docs/validation-history.md](docs/validation-history.md) for DuckDB table semantics and
changelog query examples.

Endpoint semantic changes can also be tracked without archiving duplicate full payloads. The
changelog reads current DuckDB endpoint state, keeps only fields selected in a private YAML config,
and writes compact before/after events. Manual `changelog update` does a full refresh; ingest
chains record-scoped changelog updates by default after the first baseline/config refresh:

```bash
uv run centric-mdm changelog update
uv run centric-mdm changelog update --endpoint styles
uv run centric-mdm changelog summary --since 2d
uv run centric-mdm changelog changes --endpoint styles --since 10h
```

The changelog config resolves from `CENTRIC_CONFIG_DIR/changelog.yml` or `.local/changelog.yml`
by default. See [docs/changelog.md](docs/changelog.md) for the config contract and DuckDB table
semantics.

Endpoint merge behavior lives in `config/endpoint-schema.yml`. Each endpoint can define its
primary key, modified timestamp fields, inactive/tombstone handling, and full-file semantics.
Delete handling uses `delete_when_any`: if any listed condition matches an incoming raw record,
that record is removed from the current DuckDB endpoint state while the raw JSONL evidence remains
unchanged.

Private endpoint schema additions can live outside the public repo. By default the loader starts
with the public `config/endpoint-schema.yml`, then overlays one private file if present:

1. `CENTRIC_CONFIG_DIR/endpoint-schema.yml`
2. `.local/endpoint-schema.yml` when `CENTRIC_CONFIG_DIR` is not set

Passing `--schema path/to/endpoint-schema.yml` uses that file as the explicit overlay on top of
the public base and skips auto-discovery. Scalar endpoint fields replace the base value.
`delete_when_any` replaces the delete list, while `delete_when_any_add` appends private delete
conditions to the inherited list.

```yaml
endpoints:
  styles:
    primary_key: id
    modified_at_fields: [_modified_at]
    delete_when_any:
      - field: active
        equals: false
      - field: state
        equals: ABANDONED
```

For a private additive overlay, prefer:

```yaml
endpoints:
  styles:
    delete_when_any_add:
      - field: state
        equals: ABANDONED
```

The current full-file mode is `upsert_only`: a non-delta file updates records it contains, but
does not delete records merely because they are absent from that file. This is intentional for
window fetches such as `--days 60` or `--months 2`, which are filtered windows rather than
authoritative endpoint replacements.

The DuckDB store keeps raw JSON payload text for the current endpoint record while also deriving
typed timestamp columns and current-state views. `current_endpoint_records` exposes all current
records with `payload_json`, `modified_at_ts`, and `ingested_at_ts`; endpoint-specific views such
as `current_styles`, `current_bomrows`, and `current_supplierquotes` are created from the endpoint
schema. These views are the intended boundary for letting DuckDB handle set-based extraction,
joins, and affected-product discovery while private Python reconstruction handles proprietary
product semantics.

The detailed target reconstruction, validation, and reports are proprietary. They should live
outside the public repo and be resolved through `CENTRIC_CONFIG_DIR/reconstruction.py` or
`.local/reconstruction.py`. Keep that file as a small registry and split implementation behind
it, for example:

```text
CENTRIC_CONFIG_DIR/
  reconstruction.py
  projections/
    dpp.py
    md.py
    packaging.py  # later
  reports/
    dpp.py
    md.py
    packaging.py  # later
  validation/
    dpp.py
    md.py
    packaging.py  # later
  common/
    refs.py
    indexes.py
```

The public loader only imports the private `reconstruction.py` entrypoint. That entrypoint can
route to private modules using these hooks:

```python
def reconstruct_target_records(target, records_by_endpoint, *, progress=None):
    ...


def validate_projected_products(target, payloads, *, rules=None, progress=None):
    ...


def report_validation_results(
    target,
    validation_result,
    output_dir,
    *,
    template="default",
    progress=None,
):
    ...
```

`reconstruct` either writes the default aggregate `check` result or materializes the requested
private target contract directly from current DuckDB endpoint state. All non-check targets require
a private `reconstruct_target_records` hook. Non-check target validation/reporting can use private
hooks; `dpp` still has the public readiness validator as a fallback.

## Current Targets

- `check`: public aggregate endpoint/reference coverage check.
- `dpp`: private DPP reconstruction, validation, and readiness reporting.
- `md`: private merchandise data reconstruction, validation, and readiness reporting.
- `packaging`: planned future private target.

## Create DPP Reports

```bash
uv run centric-mdm reconstruct --target dpp --output data/results/latest/dpp-products.jsonl
uv run centric-mdm validate \
  --target dpp \
  --input data/results/latest/dpp-products.jsonl
uv run centric-mdm report \
  --target dpp \
  --output-dir reports/dpp-readiness
```

Alternative report templates can be selected without changing the target contract:

```bash
uv run centric-mdm report --target dpp --template brands
```

Outputs:

- `reports/dpp-readiness/dpp-summary.md`
- `reports/dpp-readiness/dpp-summary.xlsx`
- `reports/dpp-readiness/dpp-issues.xlsx`
- `reports/dpp-readiness/brands/index.xlsx` and one `*-dpp-issues.xlsx` per brand when using
  `--template brands`.

## Create MD Reports

```bash
uv run centric-mdm reconstruct --target md --output data/results/latest/md-products.jsonl
uv run centric-mdm validate \
  --target md \
  --input data/results/latest/md-products.jsonl
uv run centric-mdm report \
  --target md \
  --output-dir reports/md-readiness
```

Outputs:

- `reports/md-readiness/md-summary.md`
- `reports/md-readiness/md-summary.xlsx`
- `reports/md-readiness/md-issues.xlsx`
- `reports/md-readiness/md-season-warnings.xlsx`
- `reports/md-readiness/md-reference-coverage.xlsx`

## Fetch Centric Data

Connection details are intentionally not stored in fetch config. Export them in your shell or
place them in `CENTRIC_CONFIG_DIR/local.env` or `.local/local.env`:

```bash
export CENTRIC_BASE_URL="https://centric.example.com"
export CENTRIC_USERNAME="your-user"
export CENTRIC_PASSWORD="your-password"
```

The session token is created from `CENTRIC_USERNAME` / `CENTRIC_PASSWORD`, kept in memory for
the current process, refreshed on `401`, and never written to disk.

Run the fetcher through the main CLI:

```bash
uv run centric-mdm fetch --endpoint styles
```

Run a filtered catch-up window with:

```bash
uv run centric-mdm fetch --days 60
```

Run a fresh delta window with:

```bash
uv run centric-mdm fetch --delta
```

Run recurring delta fetches in the foreground on a local-time cron schedule:

```bash
uv run centric-mdm delta-daemon --schedule "0 * * * *"
```

The daemon waits for the next scheduled clock time, runs `fetch --delta`, logs each run, then
waits again. It uses local timezone only. Useful schedules:

```text
0 * * * *      hourly at minute 0
*/30 * * * *   every 30 minutes on the clock
0 */2 * * *    every two hours
0 0,12 * * *   twice daily
0 3 * * *      daily at 03:00
```

Optionally run target pipelines after a successful delta fetch:

```bash
uv run centric-mdm delta-daemon \
  --schedule "0 * * * *" \
  --then-pipeline dpp \
  --then-pipeline md \
  --no-report
```

Post-fetch pipelines run only after the fetch succeeds. Each target is attempted independently:
if `dpp` fails, `md` still runs, the fetch remains successful, and the cycle is recorded as a
partial failure. Because ingest is idempotent, the next pipeline run can catch up from the raw
files already written by the successful fetch.

On macOS, keep the machine from idle sleeping while fetch is running:

```bash
uv run centric-mdm fetch --delta --caffeinate
```

Use `--resume` only to continue an interrupted fetch window from its checkpoint.

Installation-specific fetch filters also live outside the public repo. The fetcher looks for
private params in this order:

1. `--params /path/to/private/fetch-params.yml`
2. `CENTRIC_CONFIG_DIR/fetch-params.yml`
3. `.local/fetch-params.yml`

Use `--no-params` to ignore auto-discovered private params for a broad raw catch-up fetch:

```bash
uv run centric-mdm fetch --days 3 --no-params
```

The fetcher uses `config/fetcher.yml` by default. The only repo runtime config currently kept
under `config/` is `config/fetcher.yml`.

Useful modes inherited from the standalone fetcher:

- `--resume` continues from endpoint checkpoints.
- `--delta` uses `_modified_at` floors from the delta state file
  (`CENTRIC_CONFIG_DIR/delta_fetcher.yml` or `.local/delta_fetcher.yml` by default) and writes
  `data/raw/runs/<run-id>/<endpoint>.delta.jsonl`.
- `--delta-dry-run` shows injected delta filters without fetching data.
- `--days 60` fetches records modified in the last 60 days and writes
  `data/raw/runs/<run-id>-days60/<endpoint>.jsonl`.
- `--no-params` ignores auto-discovered private fetch params for that run.
- `--months 24` fetches records modified in the last 24 calendar months and writes
  `data/raw/runs/<run-id>-months24/<endpoint>.jsonl`.
- `--days` and `--months` are mutually exclusive. Prefer `--days` for operational catch-up runs
  where an exact duration is clearer.
- Delta and window run folders include `manifest.json` with run mode, selected endpoints,
  per-endpoint output files, counts, status, and filter metadata.
- By default, fetch prints a human-readable completion summary. Use `--json` when a script needs
  line-delimited JSON endpoint result records.
- `--log-level summary|http|debug` enables structured fetch logs.

Delta daemon operational files:

- `data/cron/locks/delta-daemon.lock`: prevents overlapping daemon fetches.
- `data/logs/delta-daemon.log`: human-readable daemon activity log.
- `data/logs/delta-runs.jsonl`: JSONL daemon run history.
- `data/logs/delta.log`: fetcher's delta endpoint/run log.
- `data/cron/delta-daemon/*.json`: structured fetch-plus-pipeline cycle summaries.

## Project Boundary

Centric owns the product data. This project owns validation rules, readiness checks, evidence,
fetch integrity, and reports. It should not become a full MDM platform unless the validator use
case demands it.
