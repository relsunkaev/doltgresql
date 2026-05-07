# Doltgres performance parity status

Last measured: 2026-05-06

This note records the current `dg-perfparity` status after the local
Doltgres-only optimization pass. The target is PostgreSQL parity on core
read/write workloads, defined here as no Doltgres benchmark running more than
1.5x slower than PostgreSQL on the same local harness.

## Measurement

Command:

```sh
SYSBENCH_TIME=5 SYSBENCH_DB_PS_MODE=auto testing/perf/run_local_full_benchmarks.sh
```

Final report:

`.local_benchmarks/full-20260506-233410/report.md`

The report was produced from the committed tree ending at
`24006a2d perf: make JSONB GIN storage current-only`.

## Current result

The parity target is not met.

Read-heavy workloads are mixed. Several are at parity or faster, including
`select_random_points`, `select_random_ranges`, `groupby_scan_postgres`,
`index_join_scan_postgres`, and `index_join_postgres`. Remaining read misses
include:

| workload | Doltgres/PostgreSQL QPS |
| --- | ---: |
| `index_scan_postgres` | 0.65x |
| `table_scan_postgres` | 0.64x |
| `types_table_scan_postgres` | 0.51x |

Write-heavy workloads are the main blocker:

| workload | Doltgres/PostgreSQL QPS |
| --- | ---: |
| `oltp_update_index` | 0.06x |
| `oltp_update_non_index` | 0.06x |
| `oltp_insert` | 0.06x |
| `oltp_write_only` | 0.25x |
| `oltp_delete_insert_postgres` | 0.06x |
| `types_delete_insert_postgres` | 0.08x |

Paired index benchmarks show btree lookup cases are generally faster than
PostgreSQL, while btree join, JSONB GIN build, and several JSONB GIN DML
buckets remain slower than the target.

| paired benchmark bucket | Doltgres/PostgreSQL |
| --- | ---: |
| `btree/join` | 9.98x |
| `jsonb_gin/build_jsonb_ops` | 2.33x |
| `jsonb_gin/build_jsonb_path_ops` | 3.13x |
| `jsonb_gin/dml` | 1.79x |
| `jsonb_gin/dml_insert` | 3.23x |
| `jsonb_gin/dml_update` | 2.53x |
| `jsonb_gin/dml_delete` | 2.27x |

## Safe Doltgres-only changes landed

- `8bf60fe5 perf: reduce result encoding overhead`
- `fb05bbb2 perf: make JSONB GIN row references current-format by default`
- `53310acc perf: reuse bound result metadata`
- `f882597e perf: keep text result formats compact`
- `9c98f8be perf: trim bind conversion overhead`
- `8904fa1d perf: avoid default row description format allocation`
- `24006a2d perf: make JSONB GIN storage current-only`

These changes reduce protocol/result encoding and bind-path overhead without
relaxing Dolt transaction, working-set, rollback, merge, or storage guarantees.
The JSONB GIN cleanup also removes the old Doltgres-only posting-row branch so
new indexes have a single chunked storage path.

## Remaining boundary

The write-heavy sysbench gap is dominated by the GMS/Dolt auto-commit path:

1. GMS wraps mutating statements in transaction-closing iterators.
2. Statement completion calls Dolt session `CommitTransaction`.
3. Dolt commits the dirty working set through working-set validation and root
   persistence.

A follow-up `oltp_insert` CPU profile on the same committed tree reproduced
the benchmark throughput at 157.86 QPS. The cumulative profile showed the hot
commit chain under
`server.resultForOkIter` ->
`rowexec.(*TransactionCommittingIter).Close` ->
`dsess.(*DoltSession).CommitTransaction` ->
`doltdb.(*DoltDB).UpdateWorkingSet` ->
`nbs.(*journalWriter).commitRootHashUnlocked`. Within that path,
`commitRootHashUnlocked` spent sampled time flushing and syncing the journal.
The visible Doltgres protocol/analyzer work in the same profile was small
relative to the working-set persistence path and cannot close the write gap by
itself.

Closing the remaining write gap enough to hit parity would require a
guarantee-preserving improvement in Dolt or GMS transaction / working-set
persistence internals. Doltgres should not shortcut conflict checks,
constraint-violation checks, root updates, rollback behavior, or durability to
win this benchmark.

Any future work in this lane should start by proving the optimization preserves
Dolt guarantees under ordinary commits, rollback, conflicts, constraint
violations, branch/reset behavior, and indexed sidecar maintenance.
