# JSONB GIN posting-list storage design

This document is the next storage target for Doltgres JSONB GIN. It builds on
`docs/jsonb-gin-architecture.md` and the current sidecar bridge implemented by
`server/node/create_jsonb_gin_index.go`,
`server/node/jsonb_gin_maintained_table.go`, and the in-memory posting model in
`server/jsonbgin/posting.go`.

## Current bridge

The current persisted shape is one sidecar row per `(token, row identity)`:

- `token TEXT NOT NULL`
- `row_id TEXT NOT NULL`
- one nullable column per base-table primary-key component
- primary key `(token, row_id)`

This layout is simple and correct. It supports deterministic token lookup,
union/intersection, rollback through the same statement lifecycle as the base
write, and direct base-row fetch when the posting row has enough primary-key
columns and the base table exposes primary-key indexed access.

The bridge is not the final performance layout. Common tokens create many
sidecar rows, broad-token queries still move large row-id sets through SQL row
iterators, and DML maintenance performs many row-level posting inserts/deletes.

## Target Layout

The next storage version should store one row per posting-list chunk instead of
one row per row identity. The sidecar remains a Dolt table so it participates in
branching, merge, rollback, clone, and ordinary root updates.

Suggested table name:

`dg_gin_<base_table>_<index_name>_posting_chunks`

Suggested schema:

| Column | Type | Key | Purpose |
| --- | --- | --- | --- |
| `token` | `TEXT` | primary | Encoded opclass-aware token from `jsonbgin.EncodeToken`. |
| `chunk_no` | `INT8` | primary | Stable chunk number within one token posting list. |
| `format_version` | `INT2` | | Payload encoding version. |
| `row_count` | `INT4` | | Number of row references in the chunk. |
| `first_row_ref` | `BYTEA` or `TEXT` | | Lowest encoded row reference in the chunk. |
| `last_row_ref` | `BYTEA` or `TEXT` | | Highest encoded row reference in the chunk. |
| `payload` | `BYTEA` | | Sorted, unique row references. |
| `checksum` | `BYTEA` | | Optional corruption/debug guard for decoded payloads. |

`token` already carries opclass, token kind, path, and value. Do not duplicate
opclass columns unless a future catalog/versioning need appears.

## Row Reference

The row reference must be ordered and fetchable:

- Primary-key tables: encode the Doltgres primary-key tuple in storage order,
  preserving type boundaries and NULL markers.
- Keyless tables: remain unsupported for chunked direct fetch until Dolt exposes
  a durable hidden row identity with merge/reset semantics that can be used as a
  posting reference.
- Tables whose primary key changes during UPDATE: treat the update as delete of
  the old row reference plus insert of the new row reference.

The existing hash `row_id` can remain for v1 compatibility, but v2 should not
depend on hashes for direct fetch. Hashes are useful for equality membership;
they are not enough to seek the base row without carrying primary-key values.

## Payload Encoding

Version 1 payload should be deliberately boring:

1. Length-prefixed encoded row references sorted ascending.
2. No duplicate references within a chunk.
3. A target chunk size of 4-16 KiB or a row-count cap around 256-1024,
   whichever comes first.

The first implementation can skip delta compression until benchmarks justify
it. If primary-key tuple encodings are byte-sortable, a later
`format_version = 2` can use prefix/delta compression inside the chunk.

## Lookup

Lookup flow:

1. Derive encoded lookup tokens from the predicate and opclass.
2. Fetch chunk rows for each token using the sidecar primary key.
3. For intersection (`@>`, `?&`), decode the shortest posting list first and
   probe/merge longer lists by sorted row reference.
4. For union (`?|`), merge decoded sorted streams and deduplicate references.
5. Fetch candidate base rows by primary-key reference when supported.
6. Recheck the original JSONB predicate for every candidate.

Recheck remains mandatory. Chunked storage changes candidate retrieval cost; it
does not make JSONB GIN predicates non-lossy.

## Write Maintenance

INSERT:

- Extract normalized unique tokens from the new JSONB value.
- Encode the row reference.
- Add the row reference to each token's posting list.

DELETE:

- Extract tokens from the old JSONB value.
- Remove the row reference from each token's posting list.
- Drop an empty chunk row when the last reference is removed.

UPDATE:

- If neither JSONB value nor primary-key tuple changes, do nothing.
- If only JSONB changes, remove tokens that disappeared and add tokens that
  appeared. Shared tokens remain untouched.
- If the primary-key tuple changes, remove the old row reference from old tokens
  and add the new row reference to new tokens.

Chunk edits should be copy-on-write at the Dolt row level. A single statement
may keep an in-memory pending map keyed by `(token, chunk_no)` and flush sorted
chunk rows at statement complete, matching the current
`jsonbGinPostingEditor` lifecycle.

## MVCC, Rollback, And Merge

Posting chunks must be written in the same root update as the base-table change.
No global mutable cache can be the source of truth.

- Statement error: discard pending chunk edits before statement complete.
- Transaction rollback: Dolt root rollback discards both base and sidecar rows.
- Commit: base and sidecar roots become visible together.
- Merge/conflict: chunk rows conflict when two branches edit the same
  `(token, chunk_no)`. The conflict resolver can rebuild the affected token
  posting list from base rows when automatic chunk merge is unsafe.
- Reset/clone: sidecar tables travel with the root like the current v1 posting
  table.

## Compatibility And Migration

Do not break existing v1 sidecar tables.

1. Add durable index metadata `posting_storage_version`.
2. Keep v1 readers and writers for existing indexes.
3. Gate v2 creation behind an internal feature flag until benchmark evidence is
   available.
4. Make `REINDEX INDEX` rebuild an index into the current default storage
   version.
5. Provide a one-index migration path: create v2 chunks from the base table,
   validate row counts/token counts against v1, swap metadata, then drop v1.

The planner/executor should select a posting reader by storage version. The
JSONB operator support boundary must not depend on whether the underlying
posting store is v1 rows or v2 chunks.

## Performance Tradeoffs

Expected wins:

- Fewer sidecar rows for broad tokens.
- Lower allocation count for union/intersection because decoded row references
  can stream in sorted order.
- Faster index build after bulk sorted chunk materialization.
- Lower catalog/table overhead for very common keys.

Expected costs:

- Higher write amplification for tiny updates that rewrite a partially full
  chunk.
- More complicated conflict behavior when concurrent branches edit the same
  broad token.
- Chunk split/merge code must be deterministic to avoid noisy diffs.

The target chunk size should be benchmarked rather than guessed. Small chunks
reduce DML rewrite cost; large chunks reduce lookup row count.

## Prototype And Measurement Plan

Prototype in this order:

1. Add a pure `jsonbgin.PostingChunk` encoder/decoder with property tests for
   sorted unique row references, corruption errors, and version dispatch.
2. Add an in-memory chunked store beside `jsonbgin.PostingStore` and verify
   `Lookup`, `Union`, and `Intersect` return byte-for-byte identical sorted row
   references.
3. Add microbenchmarks for chunk encode/decode, intersection, and union across
   selective, broad, and skewed token distributions.
4. Add a v2 sidecar build prototype that bulk-materializes sorted chunk rows
   using the existing `buildSortedPrimaryRowIndex` path.
5. Compare v1 and v2 with:
   - `BenchmarkJsonbGinSQLLookup`
   - `BenchmarkJsonbGinIndexBuild`
   - `BenchmarkJsonbGinDMLMaintenance`
   - `testing/indexperf/run_paired_benchmarks.sh`

Required measurement buckets:

- selective containment lookup
- broad containment lookup
- `?`, `?|`, and `?&`
- skewed documents where one token appears in most rows
- INSERT/UPDATE/DELETE maintenance with small and broad documents
- rollback after failed DML
- branch merge/rebuild of a touched broad token

Do not switch new indexes to v2 by default until lookup and build improve on
representative workloads without unacceptable DML regression.
