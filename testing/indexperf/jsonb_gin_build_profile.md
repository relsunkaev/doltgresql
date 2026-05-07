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

From `.local_benchmarks/jsonb-gin-build-profile-20260506-153427/report.md`,
plus the local byte-enabled paired smoke refreshed after the sidecar-byte metric
landed:

| Case | Doltgres | PostgreSQL 18 | Doltgres vs PG18 | Sidecar rows | Sidecar bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| `jsonb_ops` representative | 20.726 ms | 3.905 ms | 5.31x | 230 | 242,630 |
| `jsonb_path_ops` representative | 16.279 ms | 4.284 ms | 3.80x | 233 | 134,135 |
| `jsonb_ops` skewed | 24.460 ms | 4.243 ms | 5.76x | 232 | 299,492 |
| `jsonb_path_ops` skewed | 12.956 ms | 2.189 ms | 5.92x | 212 | 160,636 |

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

## Stage Signals

The same run reported:

```text
BenchmarkJsonbGinPostingChunkRowsToSink/memory  258.432 ms  1244 chunk_rows/op  259.640 MB/op  6,774,649 allocs/op
BenchmarkJsonbGinPostingChunkRowsToSink/spill   677.750 ms  1244 chunk_rows/op  269.287 MB/op  6,819,403 allocs/op
BenchmarkBuildSortedPrimaryRowIndexPostingRows  2.043 ms    4096 rows/op        1.456 MB/op    54,115 allocs/op
BenchmarkSortedPrimaryRowIndexBuilderPostingRows 1.799 ms   4096 rows/op        1.294 MB/op    53,263 allocs/op
```

The memory profile puts most allocations in JSON conversion and token/entry
generation:

- `server/types.ConvertToJsonDocument`: 148.56 MB cumulative
- `jsonbgin.ExtractEncoded`: 99.14 MB cumulative
- `CreateJsonbGinIndex.addPostingChunkEntries`: 418.51 MB cumulative
- `CreateJsonbGinIndex.writePostingChunkRowsFromEntries`: 81.68 MB cumulative
- `buildSortedPrimaryRowIndex`: 3.05 MB cumulative

The CPU profile for the spill-inclusive run is dominated by temp-file I/O and
entry sorting/merge:

- `syscall.rawsyscalln`: 600 ms flat
- `jsonbGinPostingChunkEntrySorter.Add`: 410 ms cumulative
- `bufio.Writer.Flush`: 100 ms cumulative
- `jsonbgin.ExtractEncoded`: 50 ms cumulative
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
