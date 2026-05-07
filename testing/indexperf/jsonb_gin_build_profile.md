# JSONB GIN Build Profile

Last refreshed: 2026-05-06 on Apple M4 Pro with `postgres:18-alpine`.

Run the profile locally with:

```sh
DOLTGRES_PAIRED_INDEX_BENCH_ITERS=1 testing/indexperf/profile_jsonb_gin_build.sh
```

The script writes a timestamped directory under `.local_benchmarks/` with:

- local node-stage benchmark output
- paired Doltgres versus PostgreSQL 18 benchmark output
- CPU and memory pprof files
- `go tool pprof -top` text summaries
- a generated `report.md`

## Current Snapshot

From `.local_benchmarks/full-20260506-212028/report.md`, refreshed after the
bucketed build sorter and JSON integer fast path:

| Case | Doltgres | PostgreSQL 18 | Doltgres vs PG18 | Sidecar rows | Sidecar bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| `jsonb_ops` representative | 12.491 ms/op | 3.511 ms/op | 3.56x | 215 | 242,012 |
| `jsonb_path_ops` representative | 7.702 ms/op | 2.010 ms/op | 3.83x | 232 | 134,064 |
| `jsonb_ops` skewed | 15.775 ms/op | 4.048 ms/op | 3.90x | 208 | 298,502 |
| `jsonb_path_ops` skewed | 8.974 ms/op | 2.076 ms/op | 4.32x | 206 | 160,204 |

The chunked storage shape has removed the durable sidecar row explosion. The
row-map chunk construction stage is no longer the dominant local bottleneck.
The remaining SQL-level gap is concentrated in base-table JSONB deserialization,
sidecar table writes, and the working-set/root update around the backfilled
sidecar.

## Chunk Size Signals

A one-iteration local run of
`BenchmarkJsonbGinPostingChunkRowsToSink/string/(jsonb_ops|jsonb_path_ops)`
with the in-memory sink produced:

| Opclass | Rows/chunk | Chunk rows | Payload bytes | Avg refs/chunk | Max refs/chunk |
| --- | ---: | ---: | ---: | ---: | ---: |
| `jsonb_ops` | 64 | 2,488 | 2,582,544 | 64 | 64 |
| `jsonb_ops` | 128 | 1,244 | 2,565,128 | 128 | 128 |
| `jsonb_ops` | 256 | 622 | 2,556,420 | 256 | 256 |
| `jsonb_ops` | 512 | 311 | 2,552,066 | 512 | 512 |
| `jsonb_path_ops` | 64 | 2,512 | 2,607,456 | 64 | 64 |
| `jsonb_path_ops` | 128 | 1,256 | 2,589,872 | 128 | 128 |
| `jsonb_path_ops` | 256 | 628 | 2,581,080 | 256 | 256 |
| `jsonb_path_ops` | 512 | 314 | 2,576,684 | 512 | 512 |

The fixed-size comparison shows larger chunks mostly reduce sidecar row count,
not payload bytes. Moving from 64 to 512 refs/chunk cut chunk rows by 8x but
only reduced encoded payload bytes by about 1.2%. That makes a compact payload
format a lower-leverage follow-up than extraction, row-reference encoding, and
scratch-reuse work.

The production default is 512 refs/chunk. A local 1024 refs/chunk trial improved
some build cases, but it made JSONB GIN DML maintenance noisier and worse in
delete/mixed buckets, so 512 is the better whole-benchmark tradeoff.

## Stage Signals

The latest focused local stage benchmarks reported:

```text
BenchmarkJsonbGinPostingChunkRowsToSink/string/jsonb_ops/chunk_512/memory       45.714 ms  311 chunk_rows/op  29.525 MB/op    832,145 allocs/op
BenchmarkJsonbGinPostingChunkRowsToSink/string/jsonb_path_ops/chunk_512/memory  68.651 ms  314 chunk_rows/op  54.427 MB/op  1,341,141 allocs/op
BenchmarkJsonbGinPostingChunkRowsToSink/document/jsonb_ops/chunk_512/memory     42.810 ms  311 chunk_rows/op  24.676 MB/op    594,073 allocs/op
BenchmarkJsonbGinPostingChunkRowsToSink/document/jsonb_path_ops/chunk_512/memory 64.443 ms 314 chunk_rows/op  50.321 MB/op  1,103,073 allocs/op
BenchmarkBuildSortedPrimaryRowIndexPostingRows  2.043 ms    4096 rows/op        1.456 MB/op    54,115 allocs/op
BenchmarkSortedPrimaryRowIndexBuilderPostingRows 1.799 ms   4096 rows/op        1.294 MB/op    53,263 allocs/op
```

The JSON text path now scans directly into tokens instead of materializing a
decoded map, and integer-heavy documents avoid decimal parsing on the common
canonical integer path. The focused extractor benchmark reports roughly
100-182 us and 73.9 KB/op for `jsonb_ops`, and 130-157 us and 93.4 KB/op for
`jsonb_path_ops`.

The pre-SQL stage has moved from hundreds of milliseconds to tens of
milliseconds on the local benchmark fixture. That leaves the full `CREATE
INDEX` case dominated by work outside token/chunk construction:

- deserializing base JSONB rows from Dolt storage
- writing the sidecar table through Dolt's table/root update path
- committing the sidecar root update after the backfill
- Postgres' much lower fixed overhead for small `CREATE INDEX` repeats

A local trial defaulting build workers to 4 was rejected. The worker benchmark
improved spill-heavy row construction, but the paired SQL build regressed
because the current parallel path always emits temp runs and then merges them.
Parallel build remains promising only after adding an in-memory partition/merge
path for non-spilling builds.

The spill-inclusive CPU profile is still useful for larger datasets: it is
dominated by temp-file I/O and entry sorting/merge. The default small-table path
now avoids that spill work.

## Follow-Up Queue

The profile points at these already-encoded beads:

- `dg-perfparity.10.19`: reduce base-row JSONB deserialization and sidecar write
  overhead. The next highest-leverage path is avoiding full JSONB object
  materialization during index backfill when the serialized Dolt value can feed
  the JSONB GIN extractor directly.
- `dg-perfparity.10.20`: parallelize JSONB GIN `CREATE INDEX` builds only after
  the parallel path can merge in memory for non-spilling builds. The current
  temp-run implementation is not a safe default for the benchmark-sized tables.
- `dg-perfparity.10.16`: compression is not justified as a default-promotion
  blocker from the current measurements. Keep compact payload work as an
  optional future storage experiment if larger datasets show payload bytes,
  rather than row production, dominate.
