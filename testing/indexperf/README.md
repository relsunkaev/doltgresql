# Index Performance Benchmarks

`run_paired_benchmarks.sh` runs the `dg-idxperf` paired baseline harness against
a local Doltgres test server and a disposable PostgreSQL 18 container.

```sh
testing/indexperf/run_paired_benchmarks.sh
```

The harness covers:

- btree equality and range reads, including Doltgres scan and indexed plans
- btree lookup joins, with Doltgres scan and lookup-join indexed plans
- btree index build and indexed DML maintenance
- JSONB GIN containment, key existence, `?|`, `?&`, `jsonb_path_ops`
  containment, and skewed-document reads, with Doltgres v1 and v2 indexed
  timings where v2 is supported
- JSONB GIN v2 fallback row-reference reads, currently including a numeric
  primary-key shape that logs `dg_v2_direct_fetch=false`
- JSONB GIN index build for representative and skewed `jsonb_ops` and
  `jsonb_path_ops` documents, reporting Doltgres v1, Doltgres v2, PostgreSQL
  timings, and Doltgres sidecar row counts
- JSONB GIN indexed DML maintenance, including separate INSERT, UPDATE, and
  DELETE buckets for Doltgres v1, Doltgres v2, and PostgreSQL
- representative scan-boundary cases such as a suffix-only btree predicate

Each benchmark logs a `paired-index-baseline` line and emits Go benchmark
metrics for `dg_scan_us/op`, `dg_index_us/op`, `dg_v1_index_us/op`,
`dg_v2_index_us/op`, `pg_us/op`, `dg_index_vs_scan`, `dg_index_vs_pg`,
`dg_v1_index_vs_pg`, `dg_v2_index_vs_pg`, `dg_v2_vs_v1`,
`dg_v1_sidecar_rows/op`, and `dg_v2_sidecar_rows/op` where those fields apply.
PostgreSQL plans are included for read benchmarks so Doltgres changes can be
compared to the baseline plan shape as well as elapsed time.
JSONB GIN v2 read benchmarks also log `dg_v2_direct_fetch=true|false`; false
marks an opaque row-reference fallback where Doltgres must scan and recheck
candidate rows instead of fetching directly by decoded primary-key values.

JSONB GIN v2 should not become the default storage format until the paired
PostgreSQL 18 output shows:

- v2 CREATE INDEX is at least 3x faster than v1 and no worse than 6x
  PostgreSQL 18 for both `jsonb_ops` and `jsonb_path_ops` in the local suite.
- v2 lookup buckets are no worse than 1.10x v1 for selective containment, broad
  containment, `?`, `?|`, `?&`, `jsonb_path_ops`, and skewed-document cases
  unless the planner intentionally chooses the scan boundary.
- v2 fallback row-reference buckets explicitly report `dg_v2_direct_fetch=false`
  and should be tracked separately until direct references cover that key shape.
- v2 INSERT, UPDATE, and DELETE buckets are no worse than 1.25x v1 and have a
  documented PostgreSQL 18 ratio in the same run.

Useful overrides:

```sh
DOLTGRES_PAIRED_INDEX_BENCH_ITERS=100 testing/indexperf/run_paired_benchmarks.sh
POSTGRES_PORT=15439 testing/indexperf/run_paired_benchmarks.sh
POSTGRES_IMAGE=postgres:18-alpine testing/indexperf/run_paired_benchmarks.sh
```

To capture the local JSONB GIN v2 build-stage benchmarks plus CPU and memory
pprof artifacts, use:

```sh
testing/indexperf/profile_jsonb_gin_build.sh
```

The profile script writes a timestamped report and raw artifacts under
`.local_benchmarks/jsonb-gin-build-profile-*`. It also runs the paired
PostgreSQL 18 index benchmark so the report includes v1/v2/PostgreSQL build
ratios and sidecar row counts.
The latest checked-in findings are summarized in
`testing/indexperf/jsonb_gin_build_profile.md`.

To use an already-running PostgreSQL instead of the script-managed container,
run the Go benchmark directly:

```sh
DOLTGRES_POSTGRES_BASELINE_URL='postgres://postgres:password@127.0.0.1:5432/postgres?sslmode=disable' \
go test ./testing/go -run '^$' -bench '^BenchmarkPairedIndexBaselines$' -benchtime=1x -count=1 -v
```

For the local suite that runs the CI Sysbench case list against Doltgres and
PostgreSQL 18 before appending these paired index benchmarks, use
`testing/perf/run_local_full_benchmarks.sh`.
