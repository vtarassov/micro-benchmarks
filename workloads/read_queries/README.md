# Read Query Workloads

Point and range queries on scalar and array fields with parameterized selectivity.

## Files

| File | Role | Query Type | Field Type | Limit |
|---|---|---|---|---|
| `find_read_loader.py` | Data loader (concrete class) | N/A (load only) | All fields | N/A |
| `find_read_base.py` | Schema + base class (abstract) | — | — | — |
| `point_scalar.py` | Query workload | Equality (`{field: val}`) | Scalar (IntegerRange) | limit = selectivity |
| `point_arr.py` | Query workload | Equality (`{field: val}`) | Array (NumericArrayRange, multikey) | limit = selectivity |
| `range_scalar.py` | Query workload | Bounded 30% range (`$gte/$lte`) | Scalar | limit = document_count |
| `range_arr.py` | Query workload | Bounded 30% range (`$gte/$lte`) | Array (multikey) | No limit (exhaust) |
| `index_build.py` | Index build benchmark | N/A | — | — |

`find_read_base.py` contains the shared 21-field schema (10 scalar + 10 array + padding) and is inherited by `find_read_loader.py` (pure loader) and all query workload files.

## Schema

Each document has 21 fields:
- 10 scalar fields: `scalar_unique`, `scalar_sel5`, `scalar_sel10`, `scalar_sel50`, `scalar_sel100`, `scalar_sel1k`, `scalar_sel2k`, `scalar_sel3k`, `scalar_sel4k`, `scalar_sel5k`
- 10 array fields (array_size=3): `arr_unique`, `arr_sel5`, `arr_sel10`, `arr_sel50`, `arr_sel100`, `arr_sel1k`, `arr_sel2k`, `arr_sel3k`, `arr_sel4k`, `arr_sel5k`
- 1 padding field: `pad`

Selectivity = docs per value. `scalar_sel100` means each distinct value appears in ~100 documents.

## Usage

### Step 1: Load data (once)

```bash
bash test/run_locust.sh \
  --locustfile workloads/read_queries/find_read_loader.py \
  --users 70 --run-time 9999s \
  --document-count 1000000 --load-batch-size 100
```

The loader drops the collection, inserts N documents, and then quits automatically (watch for `[POST_LOAD] Pure loader run. Sending quit message to master...`).

### Step 2: Run queries

Query workloads detect existing data automatically, or you can force-skip with `--skip-data-load`.

```bash
bash test/run_locust.sh \
  --locustfile workloads/read_queries/point_scalar.py \
  --users 1 --run-time 300s \
  --document-count 1000000 --selectivity 100 --csv=results [--skip-data-load] [--skip-index-setup] 
```

On start, the framework decides whether to skip data loading:
1. If `--skip-data-load` is passed → skip unconditionally
2. Otherwise, if `estimated_document_count() >= --document-count * 0.95` → skip automatically (log: `[SKIP_DATA_LOAD] Collection has N docs ...`). The 0.95 tolerance accounts for the approximate nature of `estimated_document_count()`.
3. Otherwise → drop collection and reload

ValueRange parameters (`max_count`, etc.) are configured by the framework via a dry-run during `on_start()`, so query methods (`random()`, `get_percentile()`) work regardless of whether data loading happened.

By default, each query workload drops all existing indexes and creates a single index on its target field in `@post_load`. Pass `--skip-index-setup` to skip this step (useful when running multiple runs of the same workload, or when you've pre-built the index).

**Note**: `--run-time` counts the full test duration including index build time. If your index takes ~60s to build and you pass `--run-time 60s`, the query phase will have no time to run. Set `--run-time` large enough to cover both index build and the actual query workload (e.g. `--run-time 300s`), or pre-build the index and use `--skip-index-setup`.

### Run all 40 combinations

```bash
DOC_COUNT=1000000
RUN_TIME=300s

for sel in unique 5 10 50 100 1k 2k 3k 4k 5k; do
  for wl in point_scalar point_arr range_scalar range_arr; do
    echo "=== ${wl} --selectivity=${sel} ==="
    bash test/run_locust.sh \
      --locustfile workloads/read_queries/${wl}.py \
      --users 1 --run-time $RUN_TIME \
      --document-count $DOC_COUNT --selectivity $sel
  done
done
```

## --selectivity choices

| CLI value | Field suffix | Numeric value | Point limit |
|---|---|---|---|
| `unique` | `_unique` | 1 | 1 |
| `5` | `_sel5` | 5 | 5 |
| `10` | `_sel10` | 10 | 10 |
| `50` | `_sel50` | 50 | 50 |
| `100` | `_sel100` | 100 | 100 |
| `1k` | `_sel1k` | 1000 | 1000 |
| `2k` | `_sel2k` | 2000 | 2000 |
| `3k` | `_sel3k` | 3000 | 3000 |
| `4k` | `_sel4k` | 4000 | 4000 |
| `5k` | `_sel5k` | 5000 | 5000 |

## How it works

1. **`find_read_loader.py` as loader** (direct run): inherits `FindReadWorkload`, provides a `noop` fallback workload and `quit_after_load()`. On start, framework dry-runs each document shape to configure all ValueRanges (sets `max_count`). PRE_LOAD drops the collection, DATA_LOAD inserts N docs, POST_LOAD sends a quit message to master (no real workloads present, only the `noop` fallback).

2. **Query workloads** (e.g. `point_scalar.py --selectivity=100`) inherit `find_read_base.py`:
   - `on_start()` (framework): ValueRanges configured via dry-run + smart-skip detection based on existing collection count
   - `@pre_load` (inherited): drops collection only if we're going to load (i.e. `_skip_data_load=False`)
   - `DATA_LOAD`: skipped instantly when `_skip_data_load=True`, otherwise inserts missing docs
   - `@post_load` (subclass-specific): `drop_indexes()` + `create_index()` on the target field — blocks until ready. **Note: index build time is included in this phase and happens before any query runs.**
   - `WORKLOAD`: runs the single query for `--run-time` duration

3. All queries use `db.command()` with `singleBatch: True` + explicit `batchSize` — single round trip, no getMore.

4. Only one index exists at query time — planner picks it automatically.

## Index Build Tests

`index_build.py` measures index creation time. Specify fields with `--index-fields` (comma-separated, order matters for compound indexes).

### Usage

```bash
bash test/run_locust.sh \
  --locustfile workloads/read_queries/index_build.py \
  --users 1 --run-time 9999s \
  --document-count 1000000 --index-fields=scalar_sel100
```

The framework's smart-skip detects existing data automatically. Add `--background` for background index builds.

### Suggested combinations

| Type | --index-fields |
|---|---|
| Single scalar (unique) | `scalar_unique` |
| Single scalar (100 doc/val) | `scalar_sel100` |
| Single scalar (5000 doc/val) | `scalar_sel5k` |
| Single array (unique) | `arr_unique` |
| Single array (100 doc/val) | `arr_sel100` |
| Single array (5000 doc/val) | `arr_sel5k` |
| Compound scalar+scalar | `scalar_sel100,scalar_sel1k` |
| Compound scalar+scalar (reversed) | `scalar_sel1k,scalar_sel100` |
| Compound scalar+scalar (high+low) | `scalar_unique,scalar_sel5k` |
| Compound arr+scalar (arr prefix) | `arr_sel100,scalar_sel100` |
| Compound arr+scalar (arr prefix, high sel) | `arr_sel100,scalar_sel1k` |
| Compound arr+scalar (low sel arr) | `arr_sel5k,scalar_sel100` |
| Compound scalar+arr (scalar prefix) | `scalar_sel100,arr_sel100` |
| Compound scalar+arr (scalar prefix, high sel) | `scalar_sel1k,arr_sel100` |
| Compound scalar+arr (low sel arr) | `scalar_sel100,arr_sel5k` |
