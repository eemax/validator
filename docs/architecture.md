# Architecture

```text
Centric API or export
  -> raw JSON/JSONL
  -> DuckDB reconstruction store
  -> projected CentricProductPayload
  -> DPP rules
  -> validation results
  -> reports
```

## Boundaries

- Centric remains the source of product/style/variant data.
- This project owns rules, validation results, and readiness reporting.
- Product payload projection is intentionally separate from validation logic.
- Raw endpoint files are immutable evidence. They answer what Centric returned at fetch time.
- The DuckDB reconstruction store is the current endpoint truth assembled from full and delta
  fetches.
- Projected JSONL payloads are materialized validation inputs, not the database.
- Reports are file-based first; a FastAPI layer can be added once consumers need live access.

## Reconstruction Pipeline

```text
fetch
  -> append raw endpoint evidence
ingest
  -> apply new raw files into DuckDB endpoint snapshots
reconstruct
  -> project current store state into CentricProductPayload JSONL
validate
  -> run DPP readiness rules against projected payloads
report
  -> write CSV, XLSX, and Markdown outputs
```

Delta fetches are never validated directly. A delta file updates endpoint state first, then
products are reconstructed from all current endpoint snapshots. For example, a changed
`bomrows.delta.jsonl` file should update the `bomrows` state, identify affected styles once that
relationship is modeled, and reconstruct product payloads from current styles, variants, materials,
and BOM state.

The detailed product graph reconstruction logic is installation-specific and proprietary. It should
not be committed to the public repo. The reconstruction loader resolves private logic in this order:

1. Explicit Python module path passed by internal callers.
2. `CENTRIC_CONFIG_DIR/reconstruction.py`.
3. `.local/reconstruction.py`.

That private module is expected to define `reconstruct_projected_products(records_by_endpoint, *,
mapping=None)` and return `CentricProductPayload` objects. It owns endpoint relationships and
product assembly rules, such as how BOM rows attach to styles, how current BOM revisions are
selected, which material/supplier/factory relationships feed DPP attributes, and how affected
product IDs are derived from deltas.

The initial store implementation uses one generic DuckDB table for current endpoint records:

```text
endpoint_records(endpoint, record_id, payload, modified_at, source_file, source_run_id, ingested_at)
applied_raw_files(file_path, endpoint, source_run_id, is_delta, record_count, content_sha256)
```

This gives us idempotent catch-up without committing too early to physical tables per endpoint.
Endpoint merge behavior is configured by `config/endpoint-schema.yml`.

## Near-Term Modules

- `centric`: API fetcher, auth, config, checkpoint/resume, delta mode, and fetch integrity.
- `centric.store`: DuckDB ingest/catch-up and current endpoint state.
- `centric.schema`: endpoint primary key, modified timestamp, and delete/tombstone rules.
- `validation`: rule loading and readiness checks.
- `reporting`: brand/product/issue exports.
- `models`: normalized payload and result contracts.

## Fetcher Contract

- Fetch configs define endpoint behavior, output directories, checkpoints, retry settings, and
  optional `.env` path only.
- `CENTRIC_BASE_URL`, `CENTRIC_USERNAME`, and `CENTRIC_PASSWORD` come from process environment
  or `.env`, never from fetch config.
- Session tokens are process memory only. No token cache file is written.

## Next Technical Steps

1. Expand private reconstruction beyond the current styles/colorways/seasons/materials projection
   into BOM rows, suppliers, factories, and supplier quotes.
3. Add affected-product tracking for incremental reconstruction.
4. Add richer project-specific endpoint examples once the exact Centric payloads are finalized.
5. Add DuckDB-backed report queries once result history spans multiple runs.
6. Add trend reports by season, brand, product type, and rule-set version.
