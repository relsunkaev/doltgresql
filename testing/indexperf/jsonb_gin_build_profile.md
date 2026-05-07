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

From `.local_benchmarks/full-20260506-204618/report.md`, refreshed after
defaulting chunked posting storage to 512 refs/chunk:

| Case | Doltgres | PostgreSQL 18 | Doltgres vs PG18 | Sidecar rows | Sidecar bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| `jsonb_ops` representative | 16.239 ms/op | 3.889 ms/op | 4.18x | 215 | 242,012 |
| `jsonb_path_ops` representative | 10.732 ms/op | 2.537 ms/op | 4.23x | 232 | 134,064 |
| `jsonb_ops` skewed | 19.663 ms/op | 4.293 ms/op | 4.58x | 208 | 298,502 |
| `jsonb_path_ops` skewed | 11.656 ms/op | 2.733 ms/op | 4.26x | 206 | 160,204 |

The chunked storage shape has removed the durable sidecar row explosion. The
remaining build gap is now mostly pre-row-map work rather than Dolt row-map
construction.

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
BenchmarkJsonbGinPostingChunkRowsToSink/string/jsonb_ops/chunk_512/memory      130.900-134.238 ms  311 chunk_rows/op   72.702 MB/op  2,552,086 allocs/op
BenchmarkJsonbGinPostingChunkRowsToSink/string/jsonb_path_ops/chunk_512/memory 181.706-195.596 ms  314 chunk_rows/op  110.002 MB/op  3,071,804 allocs/op
BenchmarkJsonbGinPostingChunkRowsToSink/document/jsonb_ops/chunk_512/memory    165.768 ms          311 chunk_rows/op   58.837 MB/op  1,704,725 allocs/op
BenchmarkJsonbGinPostingChunkRowsToSink/document/jsonb_path_ops/chunk_512/memory 239.878 ms        314 chunk_rows/op   96.802 MB/op  2,224,439 allocs/op
BenchmarkBuildSortedPrimaryRowIndexPostingRows  2.043 ms    4096 rows/op        1.456 MB/op    54,115 allocs/op
BenchmarkSortedPrimaryRowIndexBuilderPostingRows 1.799 ms   4096 rows/op        1.294 MB/op    53,263 allocs/op
```

The JSON text path now scans directly into tokens instead of materializing a
decoded map. The focused extractor benchmark reports roughly 100-103 us and
41 KB/op for `jsonb_ops`, and 127-130 us and 80.5 KB/op for
`jsonb_path_ops`.

The memory profile still puts most allocations in token/entry generation:

- JSON string decoding and token string creation
- `CreateJsonbGinIndex.addPostingChunkEntries`
- posting build-entry sort/merge work
- `CreateJsonbGinIndex.writePostingChunkRowsFromEntries`
- `buildSortedPrimaryRowIndex` remains small by comparison

The CPU profile for the spill-inclusive run is dominated by temp-file I/O and
entry sorting/merge:

- `syscall.rawsyscalln`: 72.52% flat
- `jsonbGinPostingChunkEntrySorter.AddRowTokens`: 44.97% cumulative
- `jsonbGinPostingChunkEntrySorter.flushRun`: 46.34% cumulative
- `writePostingChunkRowsFromEntries`: 14.75% cumulative
- row-map chunker/write work is visible but not dominant

## Follow-Up Queue

The profile points at these already-encoded beads:

- `dg-perfparity.10.19`: reduce row-reference and JSONB build allocation
  overhead. This should target JSON document conversion, token extraction, and
  entry allocation first.
- `dg-perfparity.10.20`: parallelize JSONB GIN `CREATE INDEX` builds. The
  remaining work is mostly per-row/token production plus sort/merge, which can
  be partitioned into deterministic runs.
- `dg-perfparity.10.16`: compression is not justified as a default-promotion
  blocker from the current measurements. Keep versioned compact payload work as
  an optional future storage experiment if larger datasets show payload bytes,
  rather than row production, dominate.
