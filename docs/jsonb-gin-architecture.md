# JSONB GIN architecture

This document specializes the PostgreSQL index architecture for JSONB GIN. It
depends on `docs/postgresql-index-architecture.md`; any GIN implementation that
does not preserve access-method/opclass metadata and expose truthful catalogs is
not supported.

## Support boundary

The first supported JSONB GIN lane is:

- `CREATE INDEX ... USING gin (jsonb_column)`
- `CREATE INDEX ... USING gin (jsonb_column jsonb_ops)`
- `CREATE INDEX ... USING gin (jsonb_column jsonb_path_ops)`
- catalog introspection for the selected access method and opclass
- physical posting-list sidecar storage with create-time backfill, DML
  maintenance, and DDL cleanup
- indexed execution with sidecar token posting lookup and recheck for the first
  PostgreSQL-compatible JSONB containment/existence operators

The current implementation covers this first lane with Doltgres-managed sidecar
posting tables, opclass-aware token extraction, `EXPLAIN`-visible indexed lookup
plans, sidecar primary-key lookup by encoded token, and original-predicate
rechecks. It is not full parity yet: indexed execution still derives candidate
row identities from the sidecar and scans base rows to emit matching identities
rather than performing a direct primary-key fetch, JSONPath GIN operators are
not claimed, and performance gates remain open.

## DDL flow

DDL must keep the PostgreSQL declaration intact:

1. Parse the `USING gin` access method.
2. Resolve each indexed expression to a JSONB input expression.
3. Resolve the opclass. Missing opclass defaults to `jsonb_ops`.
4. Reject unsupported opclasses and opclass options explicitly.
5. Persist access-method and opclass metadata with the index definition.
6. Expose the same metadata through catalogs and index-definition functions.
7. Build physical GIN postings and keep them synchronized with table writes and
   index DDL.

The implemented first pass now handles GIN metadata preservation, sidecar
posting creation, DML maintenance, rename/drop lifecycle, and planner lookup for
the supported operators. `pg_get_indexdef` and broader PostgreSQL DDL lifecycle
features remain outside this first pass.

## Opclass semantics

The implementation must not hard-code one GIN behavior for every JSONB index.
`jsonb_ops` and `jsonb_path_ops` need separate key extraction, planner matching,
catalog rows, and tests.

`jsonb_ops` should support the broad JSONB GIN operator set, including
containment and top-level key/array-string existence operators where PostgreSQL
supports them.

`jsonb_path_ops` should use path/value-oriented containment keys and must not be
treated as equivalent to `jsonb_ops`. Operators that are not supported by
`jsonb_path_ops` must either choose another compatible index or fall back to a
table scan.

JSONPath operators and functions are a separate boundary decision. They should
not be claimed until their indexable subset, recheck semantics, and fallback
behavior are tested.

## Key extraction

JSONB GIN key extraction should be a pure, deterministic layer with fixtures and
microbenchmarks. It should not know about table storage or query planning.

Required coverage:

- nested objects and arrays
- duplicate keys and repeated values
- string, numeric, boolean, and null values
- empty objects and arrays
- top-level key existence
- top-level array string existence
- containment paths
- `jsonb_ops` versus `jsonb_path_ops` differences
- stable encoding for semantically equal JSONB values

The extractor output should be an internal token type that includes the opclass
and enough normalized path/value information for posting-list lookup. Do not use
formatted JSON strings as storage keys unless benchmarks prove the allocation
and comparison costs are acceptable.

## Physical storage

Physical GIN storage should be separate from scalar btree storage:

- token key to posting-list mapping
- deterministic ordering of posting-list row references
- efficient union and intersection
- compact storage for common/skewed keys
- duplicate-token handling within one document
- backfill during `CREATE INDEX`
- transactional maintenance for INSERT, UPDATE, DELETE, rollback, and commit
- drop, rename, rebuild, clone, merge, and reset behavior

Posting lists should reference the table primary key or stable row identity. For
keyless tables, the design must define the row identity before claiming support.

The current sidecar stores encoded GIN tokens and stable row-identity hashes as
the `(token, row_id)` primary key. Token lookup uses that sidecar primary key
when the table exposes indexed access, with a full sidecar scan only as a
fallback. That gives correct lookup candidates and avoids reading unrelated
posting lists, but it is still a bridge until execution can fetch rows directly
from indexed identities and the layout has benchmark evidence for large posting
lists.

## Planning and execution

The planner should only choose a JSONB GIN index when the operator is supported
by the index opclass.

Initial operator targets:

- `@>` containment
- `?` top-level key or array-string existence where supported
- `?|` any-key existence where supported
- `?&` all-keys existence where supported

Execution should derive lookup tokens from the query predicate, fetch candidate
posting lists by token, combine them with union/intersection as required, and
recheck the original JSONB predicate against every candidate row. Recheck is
required for correctness even when a lookup is expected to be selective.

`EXPLAIN` must show an indexed JSONB GIN path before planner support is claimed.
The current plan shape does this for `@>` on `jsonb_ops`/`jsonb_path_ops` and
for `?`, `?|`, and `?&` on `jsonb_ops`. Unsupported operators must fall back to
a table scan or another compatible index.

## Catalogs

The following catalog surfaces must agree for a JSONB GIN index:

- `pg_class.relam = gin`
- `pg_index.indclass` references the selected JSONB GIN opclass
- `pg_indexes.indexdef` round-trips `USING gin` and the opclass
- `pg_opclass` contains `jsonb_ops` and `jsonb_path_ops` rows for GIN
- `pg_opfamily`, `pg_amop`, and `pg_amproc` describe the supported operators and
  support functions before planner support is claimed
- `pg_get_indexdef` returns the same definition as `pg_indexes.indexdef`

The current catalog bridge covers the selected access method, opclass, and GIN
operator-family rows for the supported JSONB operators. `pg_get_indexdef`,
statistics/progress views, and broader PostgreSQL catalog parity are still
required for full GIN parity.

## Performance gates

JSONB GIN support is performance work, not just correctness work. Benchmarks
must cover:

- selective containment lookup
- broad containment lookup
- top-level key existence
- `?|` and `?&` posting-list union/intersection
- large documents
- skewed key distributions
- index build/backfill
- INSERT, UPDATE, and DELETE maintenance
- rollback and transaction cost

Benchmarks should compare indexed plans with the table-scan fallback and record
both read performance and write overhead. A GIN feature is not complete if the
planner uses it but common indexed lookups do not beat scans on representative
data.

## Non-parity shortcuts

These are allowed only when documented and tested as non-parity:

- accepting `USING gin` while storing data in scalar btree storage
- exposing catalog metadata before physical GIN storage exists
- falling back to table scans for unsupported operators
- warning or rejecting unsupported opclasses/options
- omitting JSONPath GIN support from the first implementation

Do not close the parent JSONB GIN support bead until physical storage, planner
use, executor rechecks, DML maintenance, catalogs, and performance gates are all
implemented.
