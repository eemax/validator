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

- `centric`: API fetch shell and, later, robust checkpoint/delta behavior.
- `validation`: rule loading and readiness checks.
- `reporting`: brand/product/issue exports.
- `models`: normalized payload and result contracts.

## Next Technical Steps

1. Add a Centric raw-to-projected mapper using real payloads.
2. Move token/cache/retry/checkpoint behavior from the fetcher reference repo.
3. Add DuckDB-backed report queries once result history spans multiple runs.
4. Add trend reports by season, brand, product type, and rule-set version.
