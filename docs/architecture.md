# Architecture

```text
Centric API or export
  -> raw JSON/JSONL
  -> DuckDB reconstruction store
  -> aggregate reconstruction coverage check
  -> check reporting
```

Target-specific flows are explicit and private:

```text
DuckDB reconstruction store
  -> private target reconstruction
  -> target validation rules
  -> validation results
  -> reports
```

Current registered targets are:

- `check`: public aggregate endpoint/reference coverage check.
- `dpp`: private DPP reconstruction, validation, and reporting.
- `md`: private merchandise data reconstruction, validation, and reporting.
- `packaging`: planned future private target.

## Boundaries

- Centric remains the source of product/style/variant data.
- This project owns rules, validation results, and readiness reporting.
- Target reconstruction is intentionally separate from validation logic.
- Raw endpoint files are immutable evidence. They answer what Centric returned at fetch time.
- The DuckDB reconstruction store is the current endpoint truth assembled from full and delta
  fetches.
- The default reconstruction check is an aggregate coverage result, not the database and not a
  product payload.
- Target JSONL payloads are explicit materialized validation inputs.
- Reports are file-based first; a FastAPI layer can be added once consumers need live access.

## Reconstruction Pipeline

```text
fetch
  -> append raw endpoint evidence
ingest
  -> apply new raw files into DuckDB endpoint snapshots
reconstruct
  -> write aggregate endpoint/reference coverage JSON
validate
  -> pass through the aggregate check result
report
  -> write counts-only XLSX and Markdown outputs
```

Delta fetches are never validated directly. A delta file updates endpoint state first, then
products are reconstructed from all current endpoint snapshots. For example, a changed
`bomrows.delta.jsonl` file should update the `bomrows` state, identify affected styles once that
relationship is modeled, and refresh the aggregate check or private target payloads from current
styles, variants, materials, and BOM state.

`centric-mdm delta-daemon` is a foreground local-time cron scheduler for recurring delta fetches.
It owns scheduling, lock protection, daemon logs, and structured cycle summaries under
`data/cron`. The fetcher continues to own delta state, checkpoints, raw run directories, and
endpoint integrity checks. The daemon can trigger existing target pipelines after a successful
fetch with repeated `--then-pipeline` options, but it does not own reconstruction or validation
logic. If one post-fetch pipeline fails, the cycle is recorded as a partial failure and later
pipelines still run.

The detailed reconstruction logic is installation-specific and proprietary. It should not be
committed to the public repo. The reconstruction loader resolves private logic in this order:

1. Explicit Python module path passed by internal callers.
2. `CENTRIC_CONFIG_DIR/reconstruction.py`.
3. `.local/reconstruction.py`.

That private entrypoint is expected to stay small and route into split private modules:

```text
CENTRIC_CONFIG_DIR/
  reconstruction.py
  projections/
    dpp.py
    md.py
    packaging.py  # later
  validation/
    dpp.py
    md.py
    packaging.py  # later
  reports/
    dpp.py
    md.py
    packaging.py  # later
  common/
```

The public loader only imports `reconstruction.py`. That module can define:

```python
def reconstruct_target_records(target, records_by_endpoint, *, progress=None):
    ...

def validate_projected_products(target, payloads, *, rules=None, progress=None):
    ...

def report_validation_results(target, validation_result, output_dir, *, progress=None):
    ...
```

The optional `progress` callback receives generic progress events from long-running private
reconstruction, validation, and reporting work. The public CLI renders those events with live
progress bars in interactive terminals and falls back to plain milestone output otherwise.

Private target reconstructors own endpoint relationships and target assembly rules, such as how
BOM rows attach to styles, how current BOM revisions are selected, which material/supplier/factory
relationships feed target attributes, and how affected style IDs are derived from deltas.

The initial store implementation uses one generic DuckDB table for current endpoint records:

```text
endpoint_records(endpoint, record_id, payload, modified_at, source_file, source_run_id, ingested_at)
applied_raw_files(file_path, endpoint, source_run_id, is_delta, record_count, content_sha256)
```

The legacy reconstructed product tables still exist for compatibility, but the active CLI path
now reads current endpoint state directly for the aggregate check and for private target
reconstructors. This gives us idempotent catch-up without committing too early to physical tables
per endpoint. Endpoint merge behavior starts with `config/endpoint-schema.yml` and can be extended
by `CENTRIC_CONFIG_DIR/endpoint-schema.yml`, `.local/endpoint-schema.yml` when
`CENTRIC_CONFIG_DIR` is not set, or an explicit `--schema` overlay. Scalar endpoint fields replace
inherited values; `delete_when_any_add` appends private delete conditions without duplicating the
public list.

## Near-Term Modules

- `centric`: API fetcher, auth, config, checkpoint/resume, delta mode, and fetch integrity.
- `centric.store`: DuckDB ingest/catch-up and current endpoint state.
- `centric.schema`: endpoint primary key, modified timestamp, and `delete_when_any` current-state
  delete rules.
- `validation`: DPP rules and target validation hooks.
- `reporting`: DPP exports and aggregate reconstruction check exports.
- `models`: normalized payload and result contracts.

## Fetcher Contract

- Fetch configs define endpoint behavior, output directories, checkpoints, retry settings, and
  optional `.env` path only.
- `CENTRIC_BASE_URL`, `CENTRIC_USERNAME`, and `CENTRIC_PASSWORD` come from process environment
  or `.env`, never from fetch config.
- Session tokens are process memory only. No token cache file is written.

## Next Technical Steps

1. Tighten private reconstruction warnings as endpoint relationships are confirmed.
2. Continue refining private DPP and MD validation/reporting.
3. Add affected-style tracking for incremental reconstruction.
4. Add the private packaging target when its contract is ready.
5. Add richer project-specific endpoint examples once the exact Centric payloads are finalized.
6. Add DuckDB-backed report queries once result history spans multiple runs.
7. Add trend reports by season, brand, product type, and rule-set version.
