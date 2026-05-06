# JSONB GIN Build Profile

Last refreshed: 2026-05-06 on Apple M4 Pro with `postgres:18-alpine`.

Run the profile locally with:

```sh
DOLTGRES_PAIRED_INDEX_BENCH_ITERS=1 testing/indexperf/profile_jsonb_gin_build.sh
```

The script writes a timestamped directory under `.local_benchmarks/` with:

- local node-stage benchmark output
- paired Doltgres v1/v2 versus PostgreSQL 18 benchmark output
- CPU and memory pprof files
- `go tool pprof -top` text summaries
- a generated `report.md`

## Current Snapshot

From `.local_benchmarks/jsonb-gin-build-profile-20260506-153427/report.md`:

| Case | Doltgres v1 | Doltgres v2 | PostgreSQL 18 | v2 vs PG18 | v1 rows | v2 rows |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `jsonb_ops` representative | 115.697 ms | 18.455 ms | 3.939 ms | 4.68x | 14,579 | 230 |
| `jsonb_path_ops` representative | 58.747 ms | 11.914 ms | 2.285 ms | 5.21x | 7,338 | 233 |
| `jsonb_ops` skewed | 144.420 ms | 23.908 ms | 4.641 ms | 5.15x | 18,129 | 232 |
| `jsonb_path_ops` skewed | 75.980 ms | 12.943 ms | 55.451 ms | 0.23x | 9,088 | 212 |

The v2 storage shape has removed the durable sidecar row explosion. The
remaining build gap is now mostly pre-row-map work rather than Dolt row-map
construction.

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
- `dg-perfparity.10.20`: parallelize JSONB GIN v2 `CREATE INDEX` builds. The
  remaining work is mostly per-row/token production plus sort/merge, which can
  be partitioned into deterministic runs.
- `dg-perfparity.10.16`: evaluate compressed/adaptive posting chunks only after
  the extraction and build-pipeline costs are lower; chunk count is already
  small in v2.
