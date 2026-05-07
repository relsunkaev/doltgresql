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
  containment, and skewed-document reads
- JSONB GIN row-reference reads, including a numeric primary-key shape that
  verifies direct candidate fetch
- JSONB GIN index build for representative and skewed `jsonb_ops` and
  `jsonb_path_ops` documents, reporting Doltgres, PostgreSQL timings, Doltgres
  sidecar row counts, and encoded sidecar bytes
- JSONB GIN indexed DML maintenance, including separate INSERT, UPDATE, and
  DELETE buckets for Doltgres and PostgreSQL
- representative scan-boundary cases such as a suffix-only btree predicate

Each benchmark logs a `paired-index-baseline` line and emits Go benchmark
metrics for `dg_scan_us/op`, `dg_index_us/op`, `pg_us/op`,
`dg_index_vs_scan`, `dg_index_vs_pg`, `dg_sidecar_rows/op`, and
`dg_sidecar_bytes/op` where those fields apply.
PostgreSQL plans are included for read benchmarks so Doltgres changes can be
compared to the baseline plan shape as well as elapsed time.
JSONB GIN read benchmarks also log `dg_direct_fetch=true`; the numeric
primary-key case is included to keep decoded primary-key candidate fetch
covered by the paired benchmark output.

JSONB GIN uses chunked posting storage by default. The paired PostgreSQL 18
output should keep reporting build, lookup, and DML ratios for the current
storage path, including numeric primary-key row references.

Useful overrides:

```sh
DOLTGRES_PAIRED_INDEX_BENCH_ITERS=100 testing/indexperf/run_paired_benchmarks.sh
POSTGRES_PORT=15439 testing/indexperf/run_paired_benchmarks.sh
POSTGRES_IMAGE=postgres:18-alpine testing/indexperf/run_paired_benchmarks.sh
```

To capture the local JSONB GIN build-stage benchmarks plus CPU and memory
pprof artifacts, use:

```sh
testing/indexperf/profile_jsonb_gin_build.sh
```

The profile script writes a timestamped report and raw artifacts under
`.local_benchmarks/jsonb-gin-build-profile-*`. It also runs the paired
PostgreSQL 18 index benchmark so the report includes Doltgres/PostgreSQL build
ratios and sidecar row counts.
The latest checked-in findings are summarized in
`testing/indexperf/jsonb_gin_build_profile.md`. The node-stage benchmark
`BenchmarkJsonbGinPostingChunkRowsToSink` also reports `payload_bytes/op`,
`avg_refs/chunk`, and `max_refs/chunk` across multiple fixed chunk sizes for
JSONB GIN payload-format evaluation.

To use an already-running PostgreSQL instead of the script-managed container,
run the Go benchmark directly:

```sh
DOLTGRES_POSTGRES_BASELINE_URL='postgres://postgres:password@127.0.0.1:5432/postgres?sslmode=disable' \
go test ./testing/go -run '^$' -bench '^BenchmarkPairedIndexBaselines$' -benchtime=1x -count=1 -v
```

For the local suite that runs the CI Sysbench case list against Doltgres and
PostgreSQL 18 before appending these paired index benchmarks, use
`testing/perf/run_local_full_benchmarks.sh`.
