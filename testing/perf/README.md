# Local Full Benchmark Suite

`run_local_full_benchmarks.sh` runs the same Sysbench case list used by the
Postgres-vs-Doltgres CI latency workflow, then appends the paired index parity
benchmarks from `testing/go/index_paired_benchmark_test.go`.

The suite starts one local Doltgres process and one PostgreSQL 18 Docker
container, runs every Sysbench case against both, appends the paired btree and
JSONB GIN index benchmarks, and writes a Markdown report, CSV, and raw logs
under `.local_benchmarks/`. The JSONB GIN paired output includes Doltgres and
PostgreSQL 18 timings for build, lookup, and DML buckets.

```sh
testing/perf/run_local_full_benchmarks.sh
```

Default Sysbench cases:

- read: `oltp_read_only`, `oltp_point_select`, `select_random_points`,
  `select_random_ranges`, `covering_index_scan_postgres`,
  `index_scan_postgres`, `table_scan_postgres`, `groupby_scan_postgres`,
  `index_join_scan_postgres`, `types_table_scan_postgres`,
  `index_join_postgres`
- write: `oltp_read_write`, `oltp_update_index`, `oltp_update_non_index`,
  `oltp_insert`, `oltp_write_only`, `oltp_delete_insert_postgres`,
  `types_delete_insert_postgres`

Useful overrides:

```sh
POSTGRES_IMAGE=postgres:18-alpine \
SYSBENCH_TIME=60 \
SYSBENCH_THREADS=4 \
SYSBENCH_TABLE_SIZE=10000 \
DOLTGRES_PAIRED_INDEX_BENCH_ITERS=100 \
testing/perf/run_local_full_benchmarks.sh
```

To reuse an existing Doltgres binary:

```sh
DOLTGRES_BUILD=0 DOLTGRES_BIN=/path/to/doltgres testing/perf/run_local_full_benchmarks.sh
```

The script requires `docker`, `go`, `git`, and `sysbench` on the host. It uses
`postgres:18-alpine` by default and removes the PostgreSQL container on exit
unless `KEEP_CONTAINERS=1` is set.
