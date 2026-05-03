# Centric MDM Validation

Focused validator for Centric product data, starting with DPP readiness.

The project fetches or receives Centric product data, projects it into a narrow validation
payload, runs governed YAML rules, and creates readiness reports by brand/product. Continuous
delta fetches are ingested into a local DuckDB reconstruction store before validation payloads are
materialized.

## Current State

- Python package managed by `uv`
- CLI commands: `centric-mdm` and `centric-fetch`
- YAML-driven DPP readiness rules
- Pydantic models for projected Centric style payloads
- Product-level DPP validation with issue source fields and fix locations
- CSV, XLSX, and Markdown readiness reports
- Example payloads and tests
- Centric API fetcher with pagination, retries, checkpoints, resume, delta mode,
  count preflight, ID integrity checks, and structured logs
- DuckDB-backed ingest/reconstruction path for applying full and delta raw endpoint files

## Install

```bash
uv sync --dev
```

## Validate Example Payloads

```bash
uv run centric-mdm validate \
  --input tests/fixtures/projected-products.json \
  --rules .local/rules/dpp-readiness.yml \
  --output data/results/dpp-readiness-results.json
```

## Project Fetched Centric Data

```bash
uv run centric-mdm project \
  --input-dir data/raw \
  --output data/results/projected-products.jsonl
```

This legacy command projects directly from endpoint files in one directory. For continuous delta
workflows, prefer the reconstruction store commands below.

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

Project the current reconstructed store state back to the validator JSONL contract:

```bash
uv run centric-mdm reconstruct \
  --db data/centric.duckdb \
  --output data/results/projected-products.jsonl
```

Or run ingest, reconstruct, and validation together:

```bash
uv run centric-mdm pipeline \
  --raw-dir data/raw \
  --db data/centric.duckdb \
  --projected-output data/results/projected-products.jsonl \
  --validation-output data/results/dpp-readiness-results.json
```

Endpoint merge behavior lives in `config/endpoint-schema.yml`. Each endpoint can define its
primary key, modified timestamp fields, and inactive/tombstone handling.

The detailed reconstruction rules are expected to be proprietary. They should live outside the
public repo and be resolved later from an explicit CLI path, `CENTRIC_RECONSTRUCTION_CONFIG`,
`CENTRIC_CONFIG_DIR/reconstruction.yml`, or `.local/reconstruction.yml`.

Company-specific Centric attribute names live outside the public repo. Put private config under
`CENTRIC_CONFIG_DIR`, or use `.local/` for repo-adjacent local work. `.local/` is gitignored.
The project looks for projection mappings in this order:

1. `--mapping /path/to/private/field-mapping.yml`
2. `CENTRIC_CONFIG_DIR/field-mapping.yml`
3. `.local/field-mapping.yml`

## Create DPP Reports

```bash
uv run centric-mdm report \
  --input tests/fixtures/projected-products.json \
  --rules .local/rules/dpp-readiness.yml \
  --output-dir reports/dpp-readiness
```

Outputs:

- `reports/dpp-readiness/dpp-readiness-summary.md`
- `reports/dpp-readiness/dpp-readiness-products.csv`
- `reports/dpp-readiness/dpp-readiness-issues.csv`
- `reports/dpp-readiness/dpp-readiness.xlsx`

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

Run the fetcher through either CLI:

```bash
uv run centric-fetch run --config config/fetcher.yml --endpoint styles
uv run centric-mdm fetch --config config/fetcher.yml --endpoint styles
```

Run a fresh delta window with:

```bash
uv run centric-mdm fetch --config config/fetcher.yml --delta
```

Use `--resume` only to continue an interrupted fetch window from its checkpoint.

Installation-specific fetch filters also live outside the public repo. The fetcher looks for
private params in this order:

1. `--params /path/to/private/fetch-params.yml`
2. `CENTRIC_CONFIG_DIR/fetch-params.yml`
3. `.local/fetch-params.yml`

The only repo runtime config currently kept under `config/` is `config/fetcher.yml`.

Useful modes inherited from the standalone fetcher:

- `--resume` continues from endpoint checkpoints.
- `--delta` uses `_modified_at` floors from the delta state file
  (`CENTRIC_CONFIG_DIR/delta_fetcher.yml` or `.local/delta_fetcher.yml` by default) and writes
  `data/raw/runs/<run-id>/<endpoint>.delta.jsonl`.
- `--delta-dry-run` shows injected delta filters without fetching data.
- `--months 24` fetches records modified in the last 24 calendar months and writes
  `data/raw/runs/<run-id>-months24/<endpoint>.jsonl`.
- Delta and month-window run folders include `manifest.json` with run mode, selected endpoints,
  per-endpoint output files, counts, status, and filter metadata.
- `--log-level summary|http|debug` enables structured fetch logs.

## Project Boundary

Centric owns the product data. This project owns validation rules, readiness checks, evidence,
fetch integrity, and reports. It should not become a full MDM platform unless the validator use
case demands it.
