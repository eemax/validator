# Architecture

```text
Centric API or export
  -> raw JSON/JSONL
  -> projected CentricProductPayload
  -> DPP rules
  -> validation results
  -> reports
```

## Boundaries

- Centric remains the source of product/style/variant data.
- This project owns rules, validation results, and readiness reporting.
- Product payload projection is intentionally separate from validation logic.
- Reports are file-based first; a FastAPI layer can be added once consumers need live access.

## Near-Term Modules

- `centric`: API fetcher, auth, config, checkpoint/resume, delta mode, and fetch integrity.
- `validation`: rule loading and readiness checks.
- `reporting`: brand/product/issue exports.
- `models`: normalized payload and result contracts.

## Fetcher Contract

- Fetch configs define endpoint behavior, output directories, checkpoints, retry settings, and
  optional `.env` path only.
- `CENTRIC_BASE_URL`, `CENTRIC_USERNAME`, and `CENTRIC_PASSWORD` come from process environment
  or `.env`, never from fetch config.
- Session tokens are process memory only. No token cache file is written.
- The `archive-output` feature from `~/centric-api-fetcher` is intentionally omitted.

## Next Technical Steps

1. Add a Centric raw-to-projected mapper using real payloads.
2. Add richer project-specific endpoint examples once the exact Centric payloads are finalized.
3. Add DuckDB-backed report queries once result history spans multiple runs.
4. Add trend reports by season, brand, product type, and rule-set version.
