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
uv run centric-fetch run --config config/centric.example.yml --endpoint styles
uv run centric-mdm fetch --config config/centric.example.yml --endpoint styles
```

Useful modes inherited from the standalone fetcher:

- `--resume` continues from endpoint checkpoints.
- `--delta` uses `_modified_at` floors from `config/delta_fetcher.yaml`.
- `--delta-dry-run` shows injected delta filters without fetching data.
- `--months 24` fetches records modified in the last 24 calendar months.
- `--log-level summary|http|debug` enables structured fetch logs.

## Project Boundary

Centric owns the product data. This project owns validation rules, readiness checks, evidence,
fetch integrity, and reports. It should not become a full MDM platform unless the validator use
case demands it.
