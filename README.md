# Centric MDM Validation

Focused validator for Centric product data, starting with DPP readiness.

The project fetches or receives Centric product data, projects it into a narrow validation
payload, runs governed YAML rules, and creates readiness reports by brand/product.

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

## Install

```bash
uv sync --dev
```

## Validate Example Payloads

```bash
uv run centric-mdm validate \
  --input tests/fixtures/projected-products.json \
  --rules config/rules/dpp-readiness.example.yml \
  --output data/results/dpp-readiness-results.json
```

## Project Fetched Centric Data

```bash
uv run centric-mdm project \
  --input-dir data/raw \
  --output data/results/projected-products.jsonl
```

Company-specific Centric attribute names should live outside the public repo. The project looks
for projection mappings in this order:

1. `--mapping /path/to/private/field-mapping.yml`
2. `CENTRIC_FIELD_MAPPING=/path/to/private/field-mapping.yml`
3. `.local/field-mapping.yml`

Use `config/field-mapping.example.yml` as the public template. `.local/` is gitignored for
repo-adjacent private mappings.

## Create DPP Reports

```bash
uv run centric-mdm report \
  --input tests/fixtures/projected-products.json \
  --rules config/rules/dpp-readiness.example.yml \
  --output-dir reports/dpp-readiness
```

Outputs:

- `reports/dpp-readiness/dpp-readiness-summary.md`
- `reports/dpp-readiness/dpp-readiness-products.csv`
- `reports/dpp-readiness/dpp-readiness-issues.csv`
- `reports/dpp-readiness/dpp-readiness.xlsx`

## Fetch Centric Data

Connection details are intentionally not stored in fetch config. Export them in your shell or
place them in `.env`:

```bash
export CENTRIC_BASE_URL="https://centric.example.com"
export CENTRIC_USERNAME="your-user"
export CENTRIC_PASSWORD="your-password"
```

The session token is kept in memory for the current process. It is refreshed on `401` and is
not written to disk. `CENTRIC_TOKEN` can be provided as an initial in-memory token when needed.

Run the fetcher through either CLI:

```bash
uv run centric-fetch run --config config/fetcher.example.yml --endpoint styles
uv run centric-mdm fetch --config config/fetcher.example.yml --endpoint styles
```

Installation-specific fetch filters should also live outside the public repo. The fetcher looks
for private params in this order:

1. `--params /path/to/private/fetch-params.yml`
2. `CENTRIC_FETCH_PARAMS=/path/to/private/fetch-params.yml`
3. `.local/fetch-params.yml`

Use `config/fetch-params.example.yml` as the public template.

Useful modes inherited from the standalone fetcher:

- `--resume` continues from endpoint checkpoints.
- `--delta` uses `_modified_at` floors from the delta state file
  (`config/delta_fetcher.yaml` by default; see `config/delta_fetcher.example.yml`).
- `--delta-dry-run` shows injected delta filters without fetching data.
- `--months 24` fetches records modified in the last 24 calendar months.
- `--log-level summary|http|debug` enables structured fetch logs.

## Project Boundary

Centric owns the product data. This project owns validation rules, readiness checks, evidence,
fetch integrity, and reports. It should not become a full MDM platform unless the validator use
case demands it.
