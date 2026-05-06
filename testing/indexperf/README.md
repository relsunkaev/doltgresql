# Index Performance Benchmarks

`run_paired_benchmarks.sh` runs the `dg-idxperf` paired baseline harness against
a local Doltgres test server and a disposable PostgreSQL 16 container.

```sh
testing/indexperf/run_paired_benchmarks.sh
```

The harness covers:

- btree equality and range reads, including Doltgres scan and indexed plans
- btree lookup joins, with Doltgres scan and lookup-join indexed plans
- btree index build and indexed DML maintenance
- JSONB GIN containment, key existence, and `jsonb_path_ops` containment reads
- JSONB GIN index build and indexed DML maintenance
- representative scan-boundary cases such as a suffix-only btree predicate

Each benchmark logs a `paired-index-baseline` line and emits Go benchmark
metrics for `dg_scan_us/op`, `dg_index_us/op`, `pg_us/op`,
`dg_index_vs_scan`, and `dg_index_vs_pg` where those fields apply. PostgreSQL
plans are included for read benchmarks so Doltgres changes can be compared to
the baseline plan shape as well as elapsed time.

Useful overrides:

```sh
DOLTGRES_PAIRED_INDEX_BENCH_ITERS=100 testing/indexperf/run_paired_benchmarks.sh
POSTGRES_PORT=15439 testing/indexperf/run_paired_benchmarks.sh
POSTGRES_IMAGE=postgres:16.13-alpine testing/indexperf/run_paired_benchmarks.sh
```

To use an already-running PostgreSQL instead of the script-managed container,
run the Go benchmark directly:

```sh
DOLTGRES_POSTGRES_BASELINE_URL='postgres://postgres:password@127.0.0.1:5432/postgres?sslmode=disable' \
go test ./testing/go -run '^$' -bench '^BenchmarkPairedIndexBaselines$' -benchtime=1x -count=1 -v
```
