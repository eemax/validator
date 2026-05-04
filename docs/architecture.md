# Architecture

```text
Centric API or export
  -> raw JSON/JSONL
  -> DuckDB reconstruction store
  -> compact reconstruction check
  -> check validation/reporting
```

Target-specific flows are explicit and private:

```text
DuckDB reconstruction store
  -> private target projection
  -> target validation rules
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
- The default reconstruction JSONL is a compact check artifact, not the database and not a product
  payload.
- Target-projected JSONL payloads are explicit materialized validation inputs.
- Reports are file-based first; a FastAPI layer can be added once consumers need live access.

## Reconstruction Pipeline

```text
fetch
  -> append raw endpoint evidence
ingest
  -> apply new raw files into DuckDB endpoint snapshots
reconstruct
  -> write compact style relationship check JSONL
validate
  -> validate missing/unresolved relationship coverage
report
  -> write CSV, XLSX, and Markdown outputs
```

Delta fetches are never validated directly. A delta file updates endpoint state first, then
products are reconstructed from all current endpoint snapshots. For example, a changed
`bomrows.delta.jsonl` file should update the `bomrows` state, identify affected styles once that
relationship is modeled, and refresh the compact check or private target payloads from current
styles, variants, materials, and BOM state.

The detailed reconstruction logic is installation-specific and proprietary. It should not be
committed to the public repo. The reconstruction loader resolves private logic in this order:

1. Explicit Python module path passed by internal callers.
2. `CENTRIC_CONFIG_DIR/reconstruction.py`.
3. `.local/reconstruction.py`.

That private module is expected to define `reconstruct_master_products(records_by_endpoint)` for
style relationship reconstruction and, for non-check targets,
`project_reconstructed_products(target, reconstructed_products)`. It owns endpoint relationships and
target assembly rules, such as how BOM rows attach to styles, how current BOM revisions are
selected, which material/supplier/factory relationships feed DPP attributes, and how affected style
IDs are derived from deltas.

The initial store implementation uses one generic DuckDB table for current endpoint records:

```text
endpoint_records(endpoint, record_id, payload, modified_at, source_file, source_run_id, ingested_at)
applied_raw_files(file_path, endpoint, source_run_id, is_delta, record_count, content_sha256)
reconstructed_products(product_id, style_id, graph_json, warning_count, reconstructed_at)
reconstruction_source_refs(product_id, source_endpoint, source_record_id, relation_type)
reconstruction_warnings(product_id, severity, code, message, source_endpoint, source_record_id)
```

This gives us idempotent catch-up without committing too early to physical tables per endpoint.
Endpoint merge behavior is configured by `config/endpoint-schema.yml`.

## Near-Term Modules

- `centric`: API fetcher, auth, config, checkpoint/resume, delta mode, and fetch integrity.
- `centric.store`: DuckDB ingest/catch-up and current endpoint state.
- `centric.schema`: endpoint primary key, modified timestamp, and delete/tombstone rules.
- `validation`: DPP rules and reconstruction coverage checks.
- `reporting`: DPP exports and reconstruction check exports.
- `models`: normalized payload and result contracts.

## Fetcher Contract

- Fetch configs define endpoint behavior, output directories, checkpoints, retry settings, and
  optional `.env` path only.
- `CENTRIC_BASE_URL`, `CENTRIC_USERNAME`, and `CENTRIC_PASSWORD` come from process environment
  or `.env`, never from fetch config.
- Session tokens are process memory only. No token cache file is written.

## Next Technical Steps

1. Tighten private reconstruction warnings as endpoint relationships are confirmed.
2. Add private DPP, packaging, and ERP item master projections.
3. Add affected-style tracking for incremental reconstruction.
4. Add richer project-specific endpoint examples once the exact Centric payloads are finalized.
5. Add DuckDB-backed report queries once result history spans multiple runs.
6. Add trend reports by season, brand, product type, and rule-set version.
