# PostgreSQL index architecture

This document is the support boundary and implementation plan for PostgreSQL
index parity in Doltgres. Index support is not complete when a statement parses
or when an index can be routed through the existing Dolt secondary-index path.
Support means the requested PostgreSQL index definition is preserved, exposed
truthfully in catalogs, used by the planner/executor with PostgreSQL semantics,
and backed by performance evidence.

## Source of truth

PostgreSQL index behavior is driven by access methods, operator classes,
operator families, comparison/support functions, collations, sort direction,
NULL ordering, predicates, and expression bodies. Doltgres must treat those
PostgreSQL objects as the semantic source of truth.

The existing GMS `MySQLRange` and Dolt secondary-index path remains useful as an
execution substrate for compatible btree cases, but it is not the semantic
source of truth. Any path that casts values through MySQL/GMS type behavior or
silently drops PostgreSQL index options is a compatibility shortcut, not parity.

## Ownership

| Layer | Owns | Must not own |
| --- | --- | --- |
| Doltgres parser/AST | PostgreSQL DDL syntax, access-method names, opclass references, collations, sort/null options, predicates, INCLUDE columns, expression bodies | Storage layout decisions |
| Doltgres semantic adapter | Opclass resolution, operator-family lookup, support-function selection, PostgreSQL comparison/layout/null semantics, catalog metadata projection | Physical row storage internals |
| GMS planner lifecycle | Existing index matching lifecycle, filter extraction, join/sort rewrite entry points, `IndexLookup` handoff where the index is compatible | PostgreSQL opclass semantics by assumption |
| Dolt storage | Durable index data, write maintenance, backfill, merge/reset/version-control behavior, row iterators | PostgreSQL catalog presentation |
| pg_catalog handlers | Truthful introspection for stored index metadata, not guessed metadata | Parser-only or warning-only feature claims |

## Durable metadata

Every PostgreSQL index definition needs a durable metadata record attached to the
table schema/root, not just a transient AST value or session cache. The metadata
must include at least:

- access method
- per-key opclass and opfamily
- per-key collation
- per-key sort direction and NULL ordering
- uniqueness and NULLS DISTINCT/NOT DISTINCT
- expression bodies and generated/hidden column mapping where used
- partial predicate
- INCLUDE columns
- storage parameters and tablespace metadata when accepted

The first metadata bridge for JSONB GIN stores access method and opclasses in a
Dolt index comment. That is an implementation bridge only. It is acceptable for
unblocking catalog truth and downstream branching, but it is not the final
storage model for full index parity.

## Planning and execution

The planner should preserve the existing index matching lifecycle where possible:
filters are still discovered by the analyzer, candidate indexes are still
ranked, and execution still flows through indexed table access when the selected
access method can provide it.

PostgreSQL-specific semantics enter before an `IndexLookup` is built:

1. Resolve the index access method and opclass metadata for each key.
2. Resolve the query operator to the index operator family.
3. Use the opclass support functions to decide whether the predicate is
   indexable and whether it needs recheck.
4. Build either a PostgreSQL-aware btree range or an access-method-specific
   lookup object.
5. Execute through the matching storage iterator and recheck when the access
   method is lossy or the operator requires it.

For btree-compatible indexes, the first bridge can translate PostgreSQL range
semantics into the existing GMS/Dolt range path only after PostgreSQL comparison,
collation, and NULL behavior have been resolved. For GIN/GiST/SP-GiST/BRIN/hash,
parser acceptance must not route to a btree range pretending to be that access
method.

## Btree path

Btree is the first parity target because the existing Dolt secondary-index path
is closest to PostgreSQL btree behavior. The btree adapter must still close the
semantic gaps before the feature is considered complete:

- PostgreSQL type comparison functions, including JSONB ordering and custom
  type behavior
- collation-aware text comparison
- ASC/DESC and NULLS FIRST/LAST, including PostgreSQL defaults
- opclass-specific operators such as pattern opclasses
- expression index identity and dependency checks
- partial-index predicates and recheck behavior
- unique-index NULLS DISTINCT/NOT DISTINCT behavior

Any btree case not yet represented by the adapter should fail explicitly or be
documented as warning/import-only behavior.

## Access-method extension points

Non-btree access methods should plug into the same metadata and catalog model
but may provide their own storage and lookup objects.

- GIN stores token to posting-list mappings and usually returns candidates that
  must be rechecked.
- GiST and SP-GiST need opclass-defined consistent/union/picksplit behavior and
  should not be modeled as scalar btree ranges.
- BRIN stores page/range summaries and is inherently lossy.
- Hash supports equality-style lookups only and must expose hash-specific
  catalog metadata.
- Extension-provided index families require catalog-backed opclass/operator
  registration before DDL can claim support.

Each access method must define DDL support, storage, write maintenance, planner
matching, executor behavior, catalog rows, EXPLAIN shape, and performance gates.

## Catalog truth

Catalog rows must be derived from durable index metadata. In-memory pg_catalog
caches can remain performance optimizations, but they cannot invent metadata.

The following surfaces are support gates:

- `pg_am`
- `pg_class.relam`
- `pg_index`, including `indclass`, `indcollation`, `indoption`, `indexprs`, and
  `indpred`
- `pg_indexes.indexdef`
- `pg_get_indexdef`
- `pg_opclass`, `pg_opfamily`, `pg_amop`, and `pg_amproc`
- index statistics and progress views where claimed

Metadata must survive create, drop, rename, table rebuild, merge, reset, clone,
and dump/restore paths. OIDs may remain deterministic wrappers over internal
IDs, but the referenced index identity and catalog meaning must travel with the
schema/root.

## Test gates

Tests must classify progress by support level:

- parser-only: statement is accepted but no semantic behavior is claimed
- import shortcut: warning/no-op behavior exists only for dump or migration flow
- metadata support: DDL metadata is durable and visible in catalogs
- semantic support: reads/writes match PostgreSQL behavior
- planner support: EXPLAIN and plan shape prove the index is used
- performance support: benchmarks prove indexed paths beat table scans where
  PostgreSQL would rely on the index

A feature bead cannot close on parser or metadata support alone unless the bead
is explicitly scoped to that layer.

## Current non-parity bridge

Commit `5c185f75` adds a metadata bridge for JSONB GIN access method and opclass
metadata. It makes `CREATE INDEX USING gin` preserve `jsonb_ops` and
`jsonb_path_ops` and exposes that metadata in `pg_class`, `pg_indexes`,
`pg_index`, and `pg_opclass`.

That bridge intentionally does not claim physical GIN storage, GIN planner use,
posting-list operations, lossy rechecks, or write-maintenance performance. Those
remain downstream implementation requirements.
