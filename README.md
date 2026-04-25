# Centric MDM Validation

Focused validator for Centric product data, starting with DPP readiness.

The project fetches or receives Centric product data, projects it into a narrow validation
payload, runs governed YAML rules, and creates readiness reports by brand/product.

## Current First Pass

- Python package managed by `uv`
- CLI command: `centric-mdm`
- YAML-driven DPP readiness rules
- Pydantic models for projected Centric style payloads
- Product-level DPP validation with issue source fields and fix locations
- CSV, XLSX, and Markdown readiness reports
- Example payloads and tests
- Minimal Centric endpoint fetch shell for later credential hardening

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

## Fetch Shell

The fetch command is intentionally simple in this first pass. It proves the config shape and
JSONL output path; token refresh, checkpoints, delta windows, count checks, and richer logging
can be lifted from `~/centric-api-fetcher` once the validation/reporting flow is settled.

```bash
uv run centric-mdm fetch \
  --config config/centric.example.yml \
  --endpoint styles \
  --output data/raw/styles.jsonl
```

## Project Boundary

Centric owns the product data. This project owns validation rules, readiness checks, evidence,
and reports. It should not become a full MDM platform unless the validator use case demands it.
