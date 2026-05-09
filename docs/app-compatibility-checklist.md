# Real-world application compatibility checklist

Last updated: 2026-05-09

This is the workload-prioritized view of `postgresql-parity-issues.md`. The
parity doc enumerates every known PostgreSQL feature gap, organized by the
major version that owns the behavior. This doc selects a subset of those gaps
and re-organizes them by workload importance — what real-world non-trivial
PostgreSQL applications actually exercise through ORMs, sync tools, dump and
restore tooling, and admin scripts.

Use this doc to decide what to work on next. Use `postgresql-parity-issues.md`
for the full feature specification of each item. Every item here corresponds
to one or more items there; items here are open until they have evidence from
a real workload, not just feature-level tests.

## Audit scope

Treat any candidate workload as having these surfaces, each of which needs its
own evidence:

- Schema/bootstrap: dump-style schema load through `pg_dump` / `psql`,
  including extensions, custom types, materialized views, triggers, generated
  columns, privileges, and `DO $$` blocks.
- ORM/driver runtime: ordinary CRUD, transactions, savepoints, prepared
  statements, and connection-pool behavior issued by libraries like Drizzle,
  `node-postgres` / `pg`, SQLAlchemy, psycopg, JDBC, pgx, and TypeORM.
- View and analytical query layer: hand-written views, materialized views,
  reporting/grid queries, and `LATERAL` / `DISTINCT ON` / window-function
  patterns.
- Replication and sync runtime: logical replication consumers (Electric,
  Zero, Debezium, other `pgoutput`-based pipelines), publications, slots, and
  `REPLICA IDENTITY FULL` semantics.
- Dump/admin/tooling: `pg_dump`, `psql` restore, Drizzle Kit / Alembic /
  Prisma migrate introspection, index diagnostics, and catalog inspection
  scripts.

## Status markers

- `[ ]` open: not implemented or not exercised by any harness yet.
- `[~]` partial: implementation has landed and a real-consumer harness
  (concurrent driver invocation, PL/pgSQL trigger execution, native
  driver running the migration-tool's catalog queries, etc.) covers the
  workload pattern. Some residual gap remains — typically a deeper
  workload-corpus run such as the full `drizzle-kit introspect` binary,
  a published dump corpus, or a live replication consumer.
- `[x]` done: implementation landed *and* the residual workload-corpus
  evidence is recorded per the rules below.

## Done rules

Do not check off an item until it has workload proof:

- Schema/bootstrap items need a Doltgres restore or migration test using a
  real-world dump or migration suite.
- Runtime items need a harness that exercises the real query path through the
  target ORM/driver, not just isolated SQL.
- Replication items need live consumer processes against Doltgres, not just
  publication/catalog metadata.
- Dump/admin items need the real `psql`, `pg_dump`, ORM kit, or script path.
- If an item is intentionally unsupported, add an explicit rejection test and a
  documented workaround or non-goal.

## Critical path TODO

- [~] Stand up at least one representative non-trivial PostgreSQL dump as a
  restore-gate corpus, and record the first hard failure for each.
  testing/go/import_dump_probe_test.go now restores the
  AlexTransit/venderctl pg_dump through `psql` without skipped
  statements. The restore still warns that descending scan order is
  metadata-only, so broader dump-corpus evidence remains before this
  graduates to done.
- [~] Triage restore failures into implement, dump-rewrite, skip, and
  explicit-non-goal buckets. The first gate now has an empty skip
  bucket after landing predicate-scoped unique partial indexes for
  the AlexTransit inventory indexes.
- [ ] Build a minimal-viable schema slice harness that excludes known
  unsupported DDL and proves ORM runtime queries on top of it.
- [ ] Run a real-world view rebuild path against Doltgres (CTEs, `LATERAL`,
  `DISTINCT ON`, window functions, JSONB expansion, regex SRFs).
- [ ] Run Electric and Zero (or equivalent logical-replication consumers)
  against Doltgres with `REPLICA IDENTITY FULL`-marked tables.
- [ ] Prove the round-trip dump/restore path: `pg_dump` -> file -> `psql`
  restore -> ORM introspection -> running app.

## Schema/bootstrap TODO

- [x] Dump version identity - the version-identity surface that
  pg_dump probes works end-to-end: `version()` returns a string
  prefixed with `PostgreSQL `, the `server_version` GUC is
  queryable via `current_setting`, and `server_version_num` is a
  parseable integer >= 90000 (the threshold above which pg_dump
  branches on dialect quirks). Coverage in
  testing/go/dump_version_identity_probe_test.go. Doltgres reports
  PostgreSQL 15; pg_dump output compatibility remains tracked by
  the separate schema-output and restore-path items.
- [~] Common extensions - `CREATE EXTENSION IF NOT EXISTS
  "uuid-ossp"` is accepted at DDL, is listed in
  `pg_catalog.pg_extension`, and `uuid_generate_v4()` is callable for
  UUID primary-key defaults.
  pg_dump's `CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA
  pg_catalog` shape is accepted and records the built-in PL/pgSQL
  runtime in `pg_extension`.
  `CREATE EXTENSION IF NOT EXISTS pgcrypto` is also accepted via a
  built-in compatibility shim, including `WITH SCHEMA` metadata:
  Doltgres does not load pgcrypto's PostgreSQL C library payload
  because it expects server symbols Doltgres does not export.
  `gen_random_uuid()` is registered as a native builtin and returns a
  36-char UUID, covering the common ORM/default-PK path. `CREATE
  EXTENSION vector` is accepted through a built-in pgvector shim and
  the native `vector(n)` type round-trips scalar embeddings.
  `CREATE EXTENSION btree_gist` is accepted as a catalog-only shim
  for dump restore; actual GiST operator classes and indexes remain
  tracked under the GiST item below. `CREATE EXTENSION citext`
  installs a case-insensitive text type in the target schema so dump
  schemas using `public.citext` can load, round-trip values, compare
  case-insensitively, and enforce case-insensitive `UNIQUE` checks on
  insert/update. Unsafe raw btree range planning over `citext` columns
  is disabled; performant PostgreSQL-style btree seek parity remains
  residual risk until physical citext index keys/opclasses are modeled.
  `CREATE EXTENSION hstore` similarly installs a text-compatible
  `hstore` type for dump schemas that declare `public.hstore`
  columns, with `fetchval(hstore, text)` / `hstore -> text` covering
  key lookup, missing-key NULLs, SQL NULL hstore values, and quoted
  external representation parsing. `hstore ? text`,
  `exist`/`isexists`, and `defined`/`isdefined` cover key existence
  and non-NULL value checks. `hstore ?| text[]`, `hstore ?& text[]`,
  `exists_any`, and `exists_all` cover any/all key existence checks,
  including PostgreSQL's NULL/empty key-array behavior. `hstore @>
  hstore`, `hstore <@ hstore`, `hs_contains`, and `hs_contained`
  cover containment with SQL NULL value equality. `hstore || hstore`
  / `hs_concat` and `delete(hstore, text|text[]|hstore)` / the
  corresponding `-` operators cover overwrite and deletion semantics,
  including NULL values and NULL key-array entries. `hstore -> text[]`
  / `slice_array` and `slice(hstore, text[])` cover ordered array
  value lookup plus hstore subset extraction for missing keys, SQL NULL
  hstore values, empty key arrays, and NULL key-array entries. `akeys`,
  `avals`, `skeys`, `svals`, `each`, `hstore_to_array`, and
  `hstore_to_matrix` cover sorted key/value introspection for populated,
  empty, and SQL NULL hstore inputs, including PostgreSQL's
  length-then-lexicographic hstore key order, NULL hstore values, quoted
  values and empty strings, flat key/value arrays, two-dimensional
  key/value matrices, row-major `array_to_string` flattening, and matrix
  dimension reporting through `array_length` / `array_upper`; `skeys`,
  `svals`, and `each` also cover projection/table-function forms with
  table column aliases. The unary operator aliases `%% hstore` and
  `%# hstore` cover the same array and matrix conversion output.
  `hstore(text, text)`, `hstore(text[], text[])`, `hstore(text[])`,
  and `hstore(record)` cover constructor semantics for NULL values,
  NULL key handling, duplicate-first key handling, empty arrays,
  malformed array inputs, named and anonymous record fields, boolean
  record value output, NULL record fields, NULL composite rows, and
  canonical constructor output ordering. `tconvert(text, text)` covers
  the legacy constructor alias. `populate_record(anyelement, hstore)`
  covers hstore-driven record population in projection and table
  forms, including ignored keys, exact field-name matching, base-record
  preservation for omitted keys and SQL NULL hstore inputs, hstore NULL
  values, scalar field access, array/composite/jsonb text input
  conversion, and invalid cast propagation. `hstore_version_diag`
  reports the current hstore storage version for valid hstore inputs.
  `anyelement #= hstore` covers the operator alias for `populate_record`.
  `hstore = hstore` / `hstore <> hstore` cover order-insensitive
  equality, SQL NULL value equality, NULL-vs-empty-string inequality,
  and missing-vs-extra key inequality. `hstore_cmp`, `hstore_lt`,
  `hstore_le`, `hstore_gt`, and `hstore_ge` cover btree comparison
  helper ordering for lexicographic keys and values, NULL values after
  non-NULL values, and shorter equal-prefix maps before longer maps.
  Custom comparison operators (`#<#`, `#<=#`, `#>#`, `#>=#`) cover the
  same ordering semantics, SQL NULL propagation, and extension-qualified
  `OPERATOR(public.#<#)` syntax. Index operator-class parity remains
  residual risk. `hstore_hash` and `hstore_hash_extended` cover
  PostgreSQL-compatible hashes for empty, populated, NULL-valued, escaped,
  order-independent, duplicate-key-normalized, SQL NULL, and seeded inputs.
  `hstore_in`, `hstore_out`, `hstore_recv`, and `hstore_send` cover
  canonical text IO, PostgreSQL-compatible binary payloads, malformed
  receive headers, empty, populated, NULL-valued, escaped, and SQL NULL inputs.
  `hstore_to_json`,
  `hstore_to_jsonb`, `hstore_to_json_loose`, `hstore_to_jsonb_loose`,
  and explicit `hstore` casts to `json`/`jsonb` cover sorted key output,
  SQL NULL hstore values as JSON nulls, string escaping, loose numeric
  promotion, and boolean-looking hstore text remaining JSON strings.
  Broader hstore operators, functions, and casts remain residual risk.
  `DROP EXTENSION IF EXISTS ...` is accepted for dump cleanup preludes
  and removes loaded extension rows from `pg_extension`.
  Pinned by testing/go/common_extensions_probe_test.go.
- [~] ICU nondeterministic collations - `CREATE COLLATION ... provider
  = icu, deterministic = false` is rejected at the parser
  (`at or near "collation": syntax error` SQLSTATE 42601). Apps
  that need case-insensitive equality on string columns must
  rewrite to either `lower(col)` expression indexes (covered) or
  a `citext`-style application-level rewrite. Pinned by
  testing/go/icu_collation_probe_test.go.
- [x] Explicit query collations - runtime `ORDER BY col COLLATE "C"`
  and `ORDER BY col COLLATE "POSIX"` both run and produce
  byte-order-correct ordering (uppercase before lowercase). Column-
  level `COLLATE "C"` survives DDL and round-trips through
  `information_schema.columns.collation_name`. Coverage in
  testing/go/explicit_collation_probe_test.go (and the existing
  testing/go/info_schema_collation_test.go for the
  default-vs-explicit collation_name assertion). ICU
  nondeterministic collations (`"en_US.utf8"`, etc.) remain a
  separate gap tracked above.
- [~] Materialized views - `CREATE MATERIALIZED VIEW ... AS SELECT`
  creates a table-backed snapshot that can be queried and dropped with
  `DROP MATERIALIZED VIEW`; later source-table writes do not change the
  snapshot. Materialized views are catalogued with `pg_class.relkind =
  'm'`, `pg_matviews` rows report their definition, populated state,
  and index presence, and ordinary/unique btree indexes may be created
  on the materialized view for restore-time or read-path access.
  Materialized-view column lists now apply PostgreSQL output-alias
  semantics, including shorter alias lists and duplicate-name
  validation, including when the matview is created `WITH NO DATA`.
  Unpopulated materialized views record `pg_matviews.ispopulated =
  false`, reject scans with PostgreSQL's "has not been populated"
  error, still accept indexes, and become scannable after `REFRESH
  MATERIALIZED VIEW ... WITH DATA`. `REFRESH MATERIALIZED VIEW ... WITH
  NO DATA` truncates the backing table and returns the matview to the
  unpopulated state without dropping indexes. Non-concurrent `REFRESH
  MATERIALIZED VIEW` with default `WITH DATA` semantics reruns the
  stored SELECT definition into the existing matview columns, preserves
  indexes, supports schema-qualified refresh targets, and leaves the
  prior snapshot intact when refresh data violates an existing unique
  index. This covers schemas that need restore-time snapshot data,
  indexed reads, unpopulated restore states, and ordinary scheduled
  refreshes. `REFRESH MATERIALIZED VIEW CONCURRENTLY` now accepts
  populated matviews with at least one usable all-row unique btree
  column index, rejects `WITH NO DATA`, unpopulated matviews, and
  matviews without a usable unique index with PostgreSQL-shaped errors,
  and preserves the prior snapshot on unique-index refresh failures.
  Full PostgreSQL matview semantics are still partial: concurrent
  refresh currently runs through the same synchronous truncate/insert
  refresh path, so lock-free concurrent writer behavior remains covered
  by the broader concurrent index/locking gaps. Pinned by
  testing/go/materialized_view_probe_test.go.
- [x] PL/pgSQL trigger functions - `CREATE FUNCTION ... RETURNS
  trigger AS $$ ... $$ LANGUAGE plpgsql;` plus `CREATE TRIGGER ...
  EXECUTE FUNCTION` works end-to-end for two real shapes:
  (a) AFTER-trigger audit-log writes to a side table — covered by
  testing/go/set_local_trigger_test.go and the AFTER-INSERT subtest
  of testing/go/plpgsql_trigger_function_probe_test.go;
  (b) BEFORE-trigger NEW-field assignment (e.g.
  `NEW.marked := upper(NEW.label);`) for both full-column and
  partial-column INSERTs. The panic that previously fired on
  partial-column INSERTs (`index out of range [2] with length 2`
  in plpgsql.InterpreterStack.GetVariable) is fixed: NEW/OLD rows
  are padded to schema length in NewRecord. The trigger-returned
  NEW row now also keeps the full target schema through later insert
  analysis, so columns omitted from the original INSERT can still be
  written by the BEFORE trigger. Pinned by the partial-column subtest
  of testing/go/plpgsql_trigger_function_probe_test.go.
- [~] Event triggers - `CREATE EVENT TRIGGER` is rejected at the
  parser today (`at or near "event": syntax error`). DMS-style
  intercept triggers must be stripped from the dump before import.
  Pinned by testing/go/unsupported_ddl_probes_test.go.
- [~] `CREATE AGGREGATE` - rejected with SQLSTATE 0A000 (`CREATE
  AGGREGATE is not yet supported`). Apps that depend on custom
  aggregates must rewrite to scalar UDFs / window functions.
  Pinned by testing/go/unsupported_ddl_probes_test.go.
- [~] GiST exclusion constraints - the `EXCLUDE USING gist (...)`
  table constraint is rejected at the parser today (`at or near
  "&": syntax error` while parsing the WITH-operator block).
  Apps that emit EXCLUDE constraints (range non-overlap
  enforcement) must rewrite to either application-level checks or
  an INSERT trigger that runs the overlap query. Pinned by
  testing/go/unsupported_ddl_probes_test.go.
- [~] Statement triggers and transition tables - `FOR EACH
  STATEMENT` table triggers now execute once per matching
  INSERT/UPDATE/DELETE statement, and AFTER statement triggers may
  declare `REFERENCING OLD TABLE AS ...` / `NEW TABLE AS ...`
  transition relations that are queryable inside the trigger function.
  BEFORE/AFTER TRUNCATE statement triggers now create, introspect via
  `pg_trigger` / `information_schema.triggers`, and fire with
  `TG_OP = 'TRUNCATE'`; row-level TRUNCATE triggers are rejected because
  PostgreSQL only supports TRUNCATE at statement level. Pinned by
  testing/go/trigger_test.go (TestStatementTriggerTransitionTables and
  the TRUNCATE statement trigger probe), including zero-row UPDATE
  transition sets. Remaining gaps: PostgreSQL's row-level
  transition-table triggers are still rejected with SQLSTATE 0A000, and
  plain AFTER statement trigger self-queries still inherit the existing
  AFTER-trigger target-table visibility limitation.
- [x] Trigger catalog introspection - `pg_trigger` now walks the
  persisted trigger collection and exposes created triggers with
  stable trigger OIDs, `tgrelid`, `tgfoid`, `tgtype`, `tgenabled`,
  argument count/bytes, transition-table names, and deferrability
  flags. `information_schema.triggers` is overridden with a
  PostgreSQL-shaped row per trigger event, `pg_get_triggerdef(oid)`
  returns the stored CREATE TRIGGER definition, and `pg_class` /
  `pg_tables` set their `relhastriggers` / `hastriggers` flags from
  the same trigger collection. Pinned by
  testing/go/pg_trigger_introspection_probe_test.go for the AFTER
  INSERT audit-trigger shape used by migration-tool introspection.
- [x] Generated columns - `GENERATED ALWAYS AS (...) STORED` DDL is
  accepted, the value is computed on INSERT, and is recomputed when
  source columns are UPDATEd. `information_schema.columns.is_generated`
  reports `ALWAYS` for generated columns and `NEVER` for ordinary
  columns so dump tools can reconstruct the DDL. Coverage in
  testing/go/generated_columns_probe_test.go.
- [~] Deferrable constraints - `DEFERRABLE INITIALLY DEFERRED` FK DDL
  is parsed and accepted. FK checks for constraints created in the
  current server process are deferred inside explicit transactions:
  inserting the child before the parent succeeds if the parent is
  inserted before COMMIT, default/NO ACTION parent-key UPDATEs and
  DELETEs can be repaired before COMMIT, and unresolved child rows fail
  COMMIT with SQLSTATE `23503` and roll the transaction back.
  Autocommit violations still reject at the statement boundary.
  `pg_constraint.condeferrable` / `pg_constraint.condeferred` expose
  the captured timing for constraints created in the current server
  process, including `DEFERRABLE INITIALLY DEFERRED`, `DEFERRABLE
  INITIALLY IMMEDIATE`, and `NOT DEFERRABLE`; `pg_get_constraintdef()`
  deparses the same timing metadata using PostgreSQL's suffix shape.
  `SET CONSTRAINTS ALL|<name> DEFERRED/IMMEDIATE` now updates
  transaction-local timing for supported child-side FK checks; switching
  to `IMMEDIATE` validates pending deferred rows immediately and raises
  SQLSTATE `23503` on unresolved violations. This is still partial
  PostgreSQL parity: deferrability is not durable in the underlying FK
  storage. Pinned by testing/go/deferrable_constraints_probe_test.go.
- [x] Privilege and ownership DDL - `ALTER TABLE OWNER TO <role>`,
  `GRANT/REVOKE SELECT ON <table> TO <role>`, and `ALTER DEFAULT
  PRIVILEGES ...` are accepted so pg_dump's ownership and privilege
  blocks replay cleanly. Full ACL/default-privilege enforcement is not
  claimed here; unsupported privilege effects are no-oped for
  dump-restore compatibility. Pinned by
  testing/go/privilege_ownership_ddl_probe_test.go.
- [~] `DO $$` blocks - anonymous `LANGUAGE plpgsql` DO blocks are
  parsed and executed through the PL/pgSQL interpreter, including the
  common conditional-DDL shape used by pg_dump repair blocks, Alembic
  migrations, and IF-NOT-EXISTS init scripts. Other procedural
  languages are rejected explicitly. Remaining risk follows the
  existing PL/pgSQL interpreter surface rather than the outer DO
  statement shape. Pinned by testing/go/do_block_probe_test.go.
- [x] `session_replication_role` - the GUC is settable and readable
  via SET / SHOW (`replica` and `origin` round-trip). `replica`
  suppresses ordinary FK checks and trigger firing during bulk-load
  style inserts, while returning to `origin` restores normal
  enforcement. Pinned by
  testing/go/session_replication_role_probe_test.go.
- [x] `REPLICA IDENTITY FULL` DDL - `ALTER TABLE ... REPLICA
  IDENTITY FULL` and `... REPLICA IDENTITY DEFAULT` are accepted and
  round-trip through `pg_class.relreplident`. The logical-replication
  source includes old update tuples for `REPLICA IDENTITY FULL`
  tables, which is the downstream Electric/Debezium contract. Pinned
  by testing/go/replica_identity_full_probe_test.go,
  testing/go/publication_subscription_test.go, and
  testing/go/logical_replication_source_test.go.

## Index/planner TODO

- [~] Partial indexes - non-unique partial indexes (e.g. `WHERE column
  IS NOT NULL`, `WHERE active = true`) are accepted at DDL: the index
  is created, round-trips through `pg_indexes`, and queries that match
  the predicate return the right rows. Partial *UNIQUE* indexes now
  build as non-unique physical indexes with PostgreSQL-visible
  uniqueness metadata and a DML wrapper that enforces uniqueness only
  for rows whose predicate evaluates true. The implemented predicate
  evaluator covers column truthiness/NOT, IS NULL/IS NOT NULL,
  comparisons, parentheses, AND, and OR, which covers the
  AlexTransit/venderctl `WHERE at_service` and `WHERE NOT at_service`
  restore path. `ON CONFLICT (col) WHERE arbiter_pred` now resolves
  exact predicate matches against metadata-backed partial unique
  indexes, and non-target partial-unique conflicts are preserved for
  multi-unique `DO NOTHING`. Residual gap: predicate implication is
  exact-string matching today rather than PostgreSQL's broader logical
  implication rules. DDL and DML coverage in
  testing/go/partial_expression_index_test.go; real dump proof in
  testing/go/import_dump_probe_test.go; upsert coverage in
  testing/go/insert_on_conflict_test.go.
- [x] Expression indexes - `CREATE INDEX ... ON t ((expr(col)))` works
  end-to-end for the common `lower(email)` shape: the index is
  created, round-trips through `pg_indexes`, and queries that match
  the expression return the right rows. Coverage in
  testing/go/partial_expression_index_test.go.
- [x] `CREATE INDEX CONCURRENTLY` keyword acceptance and btree
  two-phase catalog visibility - plain btree CONCURRENTLY drives
  PostgreSQL's two-phase state machine: register-and-build under
  (indisready=false, indisvalid=false), commit, then flip to
  (true, true) in a separate transaction. The flip is now
  metadata-only — it edits the index's IndexProperties.Comment
  through doltdb.Table.UpdateSchema and reuses the Phase 1 prolly
  tree verbatim (Dolt's upstream contract: "this method only
  updates the schema of a table; the row data is unchanged"). Other
  sessions observe the in-progress catalog state via pg_index, and
  the planner refuses to use the index until both bits are true.
  SQL-level coverage in testing/go/create_index_concurrently_test.go
  (plain, UNIQUE, IF NOT EXISTS, multi-column, IF EXISTS drop,
  REINDEX INDEX, REINDEX TABLE, post-state pg_index assertion,
  duplicate-data uniqueness violation cleanup). Cross-session
  evidence in
  testing/go/create_index_concurrently_contention_test.go: a test-
  only inter-phase hook deterministically pauses session A
  mid-build while session B observes (false, false) through
  pg_index and then (true, true) after release. Workload-corpus
  evidence in testing/go/alembic_concurrently_test.go: the harness
  installs Alembic + SQLAlchemy + psycopg in a venv and runs a real
  migration with op.create_index(..., postgresql_concurrently=True)
  / op.drop_index(..., postgresql_concurrently=True). What this
  does *not* deliver is PostgreSQL's "non-blocking on writers"
  contract — see the two follow-ups below.
- [ ] CONCURRENTLY non-blocking writes during Phase 1 - PG's whole
  point of CONCURRENTLY is that producers can keep writing while
  the index backfill runs. Doltgres' Phase 1 still holds a write
  lock for the duration of the build, so concurrent writers
  block. Closing this needs Dolt-side dual-write (writers
  maintain a pending index while the backfill runs); out of
  scope until that primitive lands.
- [~] CONCURRENTLY for metadata-backed and non-btree index shapes -
  btree `INCLUDE`, non-unique btree partial, supported JSONB GIN, and
  non-unique btree expression indexes now use the same two-phase
  `pg_index.indisready=false` / `pg_index.indisvalid=false` catalog
  visibility as plain btree `CREATE INDEX CONCURRENTLY`, then flip to
  ready/valid after the inter-phase commit; partial-unique btree indexes
  keep predicate-scoped uniqueness after the final flip. Unique
  expression `CONCURRENTLY` shapes still route through their existing
  synchronous build path; migration tools do not error, but that shape
  does not expose the two-phase catalog state yet. This does not change
  the separate non-blocking writer gap above. Pinned by
  testing/go/create_index_concurrently_test.go and
  testing/go/create_index_concurrently_contention_test.go.
- [x] `INCLUDE` indexes - `CREATE INDEX ... ON t (col) INCLUDE (a,
  b)` is accepted at DDL and the index round-trips through
  `pg_indexes`. Coverage in
  testing/go/include_jsonb_gin_index_probe_test.go pins the DDL
  acceptance shape that pg_dump and ORM introspection emit.
- [x] JSONB GIN indexes - `CREATE INDEX ... USING gin (jsonb_col)`
  is accepted, the index round-trips through `pg_indexes`, and the
  `@>` containment subset (`payload @> '{"kind": "click"}'`) returns
  the correct rows. Coverage in
  testing/go/include_jsonb_gin_index_probe_test.go.
- [~] GiST indexes - rejected with SQLSTATE 0A000 `index method
  gist is not yet supported`. Apps that need GiST (geometry, range
  non-overlap, btree_gist composite uniqueness) must rewrite to
  btree with a custom unique key, or strip the USING gist suffix
  from the dump. Pinned by testing/go/gist_index_probe_test.go.
- [x] Opclasses - explicit opclass declarations on btree columns
  (e.g. `text_ops`, `int4_ops`, `text_pattern_ops`) are accepted,
  type-validated, preserved in `pg_indexes`, exposed through
  `pg_index.indclass`, and available through `pg_opclass` joins.
  Pattern opclasses route prefix `LIKE 'foo%'` scans through the
  btree planner boundary while non-prefix patterns fall back to a
  table scan. Pinned by testing/go/index_opclass_nulls_probe_test.go,
  testing/go/index_test.go, testing/go/pg_index_indclass_any_test.go,
  testing/go/pgcatalog_test.go, and
  testing/go/index_benchmark_test.go.
- [~] Null ordering in indexes - `ASC NULLS LAST` / `DESC NULLS
  FIRST` is accepted at DDL and query `ORDER BY` now follows
  PostgreSQL null placement for defaults (`ASC` => NULLS LAST,
  `DESC` => NULLS FIRST) plus all explicit NULLS FIRST/LAST
  combinations. DDL still emits warnings that physical descending
  and NULLS LAST index scan ordering are metadata-only; the metadata
  is preserved through pg_index, but index storage/planner preference
  does not yet model those per-column scan choices. Pinned by
  testing/go/index_opclass_nulls_probe_test.go.
- [~] Materialized view indexes - ordinary and unique btree indexes can be
  created on table-backed materialized views, round-trip through
  `pg_indexes`, set `pg_class.relhasindex`, and flip
  `pg_matviews.hasindexes`. This covers indexed restore-time snapshots
  and read paths. Non-concurrent `REFRESH MATERIALIZED VIEW` preserves
  existing matview indexes, and unique-index violations leave the prior
  snapshot intact. `REFRESH MATERIALIZED VIEW CONCURRENTLY` now validates
  PostgreSQL-style unique-index eligibility for all-row unique btree
  column indexes and rejects missing or partial unique indexes; lock-free
  concurrent refresh behavior remains a residual gap.
  Pinned by testing/go/materialized_view_probe_test.go.

## View/query TODO

- [x] Dynamic view rebuild - `CREATE OR REPLACE VIEW` works end-to-end:
  same-shape body swap, view-on-view dependency chains where the inner
  view is rebuilt and outer aggregations reflect the new shape, and
  bodies built from CASE/COALESCE expressions. DROP VIEW + CREATE VIEW
  rebuild flow also works for shape-changing rebuilds. Coverage in
  testing/go/view_rebuild_test.go pins these workload shapes.
- [x] `LATERAL` joins - `CROSS JOIN LATERAL` works end-to-end for the
  top-N-per-group and computed-column-per-row shapes. `LEFT JOIN
  LATERAL ... ON true` projects matching rows and preserves outer
  rows whose lateral subquery returns empty by null-extending the
  inner side. Coverage in testing/go/lateral_test.go.
- [x] `DISTINCT ON` - "latest row per group" pattern works against both
  single-column and multi-column distinct keys, with WHERE filters and
  across NULL groups. Coverage in testing/go/distinct_on_test.go pins
  the four shapes real PG views use. Query-time PostgreSQL NULL
  ordering is covered separately by the "Null ordering in indexes"
  item above.
- [x] Window functions - `lag()`, `lead()`, `count(*) OVER (PARTITION
  BY)`, `count(*) OVER ()`, `first_value()`, and `last_value()` (with
  an explicit `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`
  frame) work end-to-end. The rank family (`row_number()`, `rank()`,
  `dense_rank()`, `percent_rank()`, `ntile()`) also works over
  partitions with PostgreSQL-shaped return types (`bigint`,
  `double precision`, and `integer` as applicable). Running `sum()` and
  `avg()` over explicit `ROWS BETWEEN ... PRECEDING ...` frames work
  without the former int32-vs-float64 wire-type panic and expose
  PostgreSQL return types for integer inputs (`bigint` and `numeric`).
  Coverage in testing/go/window_functions_test.go.
- [x] Aggregate `FILTER` - reporting/grid views rely on FILTER for
  two-axis counts and revenue-vs-refund splits. AST conversion in
  server/ast/func_expr.go now rewrites
  `func(args...) FILTER (WHERE pred)` to
  `func(CASE WHEN pred THEN arg ELSE NULL END, ...)`. Aggregates that
  ignore NULLs (sum/avg/count) naturally skip non-matching rows.
  count(*) is special-cased: the * is replaced with a literal 1 so
  the rewrite becomes count(CASE WHEN pred THEN 1 END). Coverage in
  testing/go/aggregate_filter_test.go: count(*)/sum/avg FILTER,
  FILTER+GROUP BY, FILTER returning NULL when no rows match, and
  FILTER mixed with non-filtered aggregates under COALESCE.
- [x] `string_agg(DISTINCT ...)` and `array_agg(DISTINCT ...)` - DISTINCT
  was being parsed but silently ignored for both. string_agg now
  threads distinct through to vitess.GroupConcatExpr.Distinct;
  ArrayAgg gained a `distinct` field and a per-buffer seen-set
  de-dup using the same jsonAggDistinctKey shape jsonb_agg uses.
  Coverage in testing/go/aggregate_distinct_test.go pins both shapes
  via length/array_length on the result and aggregate-internal ordering
  with `string_agg(DISTINCT tag, ',' ORDER BY tag DESC)` plus
  `array_agg(DISTINCT group_id ORDER BY group_id DESC)`. The parser
  now accepts `ORDER BY` after DISTINCT aggregate arguments and threads
  it through FuncExpr.OrderBy.
- [x] Regex set-returning functions - `regexp_matches(text, pattern[,
  flags])` and `regexp_split_to_table(text, pattern[, flags])` are now
  registered. Both work in projection and FROM-clause positions and
  honour the 'g' (global) and 'i' (case-insensitive) flags. Coverage in
  testing/go/regex_srf_test.go pins the workload shapes (single-match
  capture-group access, global-match row counting, split-to-rows over
  comma/whitespace separators).
- [x] `generate_series` and `unnest` - generate_series works end-to-end:
  2-/3-arg numeric ranges with positive and negative step, as a
  FROM-clause source feeding aggregates, and count(*) over a 1000-
  element series. unnest works in a bare projection, as a projected
  SRF ordered by its alias, and as a lateral table function over an
  array column (`CROSS JOIN unnest(vals) AS t`). Coverage in
  testing/go/srf_test.go.
- [x] JSONB expansion - `->`, `->>`, `#>`, `#>>`, `jsonb_array_elements`,
  and `jsonb_object_keys` all work end-to-end against object keys,
  nested paths, and array elements. Coverage in
  testing/go/jsonb_expansion_test.go pins the workload shapes real
  views rely on.
- [x] Date/time casts and helpers - text-to-date/timestamp casts,
  `make_date`/`make_timestamp`, `extract` (year/month/day/quarter/dow/hour),
  date arithmetic with `INTERVAL`, date subtraction, and `date_trunc` all
  work end-to-end. `make_date` was missing and is now registered.
  Coverage in testing/go/datetime_workload_test.go pins the workload
  shapes real reporting views rely on.

## Runtime SQL TODO

- [x] `SET LOCAL` - support transaction-local GUC settings used by audit
  contexts and trigger-control patterns. Implementation landed
  (snapshot/restore at COMMIT/ROLLBACK/autocommit boundary, plus
  savepoint-scoped restore below).
  testing/go/set_local_trigger_test.go now exercises the audit-context
  workflow end-to-end: a BEFORE INSERT trigger written in PL/pgSQL
  reads `current_setting('app.actor', true)` and writes the value into
  an audit row, with cases for COMMIT, ROLLBACK, autocommit, and
  session-scope-vs-transaction-local override. Pinned by
  testing/go/set_local_savepoint_test.go for the savepoint cases.
- [x] `SET LOCAL` snapshots scoped to savepoints - SAVEPOINT now
  records a transaction-local GUC frame, ROLLBACK TO SAVEPOINT
  restores the savepoint-time value, and SET LOCAL values first
  written after a savepoint are cleared when rolling back to that
  savepoint. RELEASE SAVEPOINT drops only the savepoint frame while
  preserving current variable values, matching PostgreSQL behavior.
  Implementation in server/functions/xact_vars.go with hooks in
  server/connection_handler.go. Coverage in
  testing/go/set_local_savepoint_test.go.
- [x] Transaction-local `set_config(..., true)` - support audit-context
  helpers and similar patterns. Implementation landed; covered by the
  same trigger harness above plus testing/go/set_test.go.
- [x] `current_setting(..., true)` reads from triggers - prove trigger
  functions can read custom GUCs set on the same transaction. Covered
  by testing/go/set_local_trigger_test.go.
- [x] Advisory transaction locks - implement
  `pg_advisory_xact_lock(hashtext(...))` and equivalent workflows.
  Implementation landed for `(int8)` and `(int4, int4)` with auto-
  release at transaction end. testing/go/lock_concurrency_test.go now
  races independent goroutine-driven pgwire connections against the
  same key and asserts the holder/waiter handoff at COMMIT and
  ROLLBACK, plus a hashtext-keyed duplicate-work scenario where 6
  workers race and only one acquires.
- [x] Hash helpers - implement `hashtext(...)` and `hashtextextended(...)`
  used to derive advisory-lock keys. Implementation landed byte-exact
  against PostgreSQL 16. Sequential coverage in
  server/functions/hashtext_test.go and testing/go/functions_test.go;
  contention coverage via the hashtext-keyed subtest in
  testing/go/lock_concurrency_test.go.
- [x] `pg_try_advisory_lock` semantics - prove non-blocking advisory
  locks for duplicate-work prevention. Single-attempt CAS path landed
  (no longer relies on a 1ms sleep). Concurrent contention covered by
  the 8-goroutine race in testing/go/lock_concurrency_test.go which
  asserts exactly one of N callers acquires.
- [x] Multi-unique `ON CONFLICT` - prove upsert targeting one specific unique
  constraint on a table with multiple unique constraints. DO UPDATE
  routes through OnConflictTargetGuard (raises on non-target unique
  conflicts); DO NOTHING routes through OnConflictDoNothingArbiterTable
  (server/node/on_conflict_do_nothing_arbiter_table.go), a pre-check
  inserter wrapper that pre-validates non-target unique indexes per
  row and returns a non-UniqueKeyError so GMS's IGNORE handler does
  not swallow it. Coverage in testing/go/insert_on_conflict_test.go's
  TestInsertOnConflictMultiUnique covers both paths plus the
  pgx-driven ORM-shape upsert. The concurrent DO NOTHING race where
  many sessions target one unique index while colliding on another
  is pinned by testing/go/on_conflict_do_nothing_concurrency_test.go:
  exactly one row commits and every loser receives SQLSTATE 23505
  instead of a silent no-op.
- [x] `ON CONFLICT ... DO UPDATE` variants - EXCLUDED pseudo-table,
  DO UPDATE SET ... WHERE pred, ON CONFLICT (col) WHERE arbiter_pred,
  and ON CONFLICT ON CONSTRAINT name all land. EXCLUDED rewrites to
  vitess.ValuesFuncExpr inside Context.WithExcludedRefs;
  DO UPDATE SET ... WHERE rewrites each `col = expr` to a CASE that
  preserves the existing value when the predicate is false; arbiter
  predicate `ON CONFLICT (col) WHERE pred` resolves exact predicate
  matches against partial unique indexes and rejects non-matching
  predicates instead of silently falling through to another unique
  index. Broader predicate implication remains tracked by the Partial
  indexes entry above. ON CONSTRAINT resolution looks the constraint up
  by GMS index ID and treats `<table>_pkey` as PG's auto-generated
  primary-key constraint name. Coverage in
  testing/go/insert_on_conflict_test.go's
  TestInsertOnConflictExcluded, TestInsertOnConflictDoUpdateWhere,
  TestInsertOnConflictArbiterPredicate, and
  TestInsertOnConflictOnConstraint. RETURNING / affected-row-count
  parity is covered separately below.
- [x] `INSERT ... ON CONFLICT ... RETURNING` and affected-row-count
  parity - Plain `INSERT ... RETURNING` works end-to-end (single-row
  and multi-row projecting subsets or full rows). `ON CONFLICT DO
  NOTHING ... RETURNING` now returns zero rows when the existing row
  is preserved and returns the inserted row on the no-conflict path.
  `ON CONFLICT DO UPDATE ... RETURNING` now returns the post-update
  row for single-row conflicts and for multi-row VALUES that mix
  insert and update outcomes. Command tags for these RETURNING forms
  report PostgreSQL-style affected row counts (`INSERT 0 n`).
  Coverage in testing/go/on_conflict_returning_test.go.
- [x] `FOR UPDATE` row locks - row-level pessimistic locking
  with cross-session contention. server/ast/locking_clause.go
  parses FOR UPDATE / FOR SHARE / FOR NO KEY UPDATE / FOR KEY
  SHARE plus NOWAIT / SKIP LOCKED / OF table-list; the
  AssignRowLevelLocking analyzer rule wraps every base table in
  scope with server/node/row_locking_table.go's RowLockingTable.
  Each row read acquires a transaction-scoped advisory lock on a
  structured (relationOID, primary-key) key — the lock-name
  encoding is a deterministic "row:<oid>:<pk>" string so a session
  staring at pg_locks can read a held lock back to a specific row.
  Keyless tables fall back to a table-level "reltable:<oid>" lock
  via tableLevelLockingTable so the silent "no lock at all"
  degradation is gone (over-serializing the table is correctness-
  safe). NOWAIT raises immediately on contention; SKIP LOCKED
  elides the held row (or the entire keyless table) and continues.
  Synthesized locks surface in pg_locks (locktype='tuple' for
  row-level, locktype='relation' for the keyless fallback) via a
  registry in server/node/row_lock_registry.go, populated at
  acquire/wait time and cleared at COMMIT/ROLLBACK alongside
  ReleaseSessionXactLocks. Deadlock detection runs as a 10ms-poll
  cycle walker (server/node/row_lock_deadlock.go); when two
  sessions hold opposite-order locks the smallest-ID participant
  aborts with SQLSTATE 40P01 deadlock_detected, which is what
  every PG ORM and transaction helper branches on to retry.
  Coverage in testing/go/select_for_update_test.go (parsing),
  select_for_update_contention_test.go (holder/waiter blocking,
  NOWAIT under 250ms, SKIP LOCKED elision, 8-way race
  serialization, keyless table-level fallback, keyless SKIP
  LOCKED elides whole table), pg_locks_for_update_test.go
  (granted/waiting visibility, commit clears registry), and
  select_for_update_deadlock_test.go (opposite-order cycle aborts
  exactly one transaction with 40P01 within milliseconds).
- [x] Savepoints - prove nested transaction behavior used by ORM transaction
  helpers. testing/go/savepoints_test.go exercises the wire-protocol
  surface end-to-end (RELEASE / ROLLBACK TO / nested rollback /
  case-insensitive identifiers / unknown savepoint errors) and
  TestSavepointsORMShape drives pgx.Tx.Begin / Commit / Rollback
  through the nested-transaction API. The residual ORM-suite
  evidence is now landed too:
  testing/go/sqlalchemy_savepoints_test.go installs SQLAlchemy +
  psycopg3 in a fresh venv and runs Session.begin_nested workflows
  (commit-commit, nested rollback, two-deep nesting with mixed
  rollback, outer rollback discarding nested commits) against a
  live Doltgres instance.
- [x] `LISTEN` / `UNLISTEN` / `pg_notify` / `NOTIFY` - notification
  channels work through the PostgreSQL wire protocol: listeners receive
  asynchronous `NotificationResponse` messages from both `NOTIFY` and
  `pg_notify`, `UNLISTEN channel` and `UNLISTEN *` remove registrations,
  and transaction boundaries match PostgreSQL's observable behavior
  (delivery after COMMIT, no delivery before COMMIT or after ROLLBACK,
  duplicate channel/payload pairs folded within a transaction). Pinned by
  testing/go/pg_notify_probe_test.go.
- [ ] Reader/writer pool topology - define the Doltgres deployment shape
  expected by ORM `withReplicas`-style reader/writer routing.
- [x] SSL / SCRAM / auth / client parameters - pgxpool can start a
  pooled connection over `sslmode=require`, authenticate with the
  SCRAM-backed default password role, receive the requested
  `application_name` through `current_setting`, and run pooled CRUD.
  Pinned by testing/go/ssl_test.go's
  TestPooledSSLStartupParametersAndScramAuth.
- [~] Secondary clients - `postgres.js` runs through the real client
  package with a pooled connection configuration, prepared tagged-template
  queries, parameter binding, JSONB insertion through `sql.json`, arrays,
  lateral JSONB expansion, concurrent reads across the pool, transaction
  commit, and rollback-on-error behavior. Pinned by
  testing/go/postgres_js_client_test.go. `ts-postgres` and other less common
  clients remain open until workloads require them.

## Replication and sync TODO

This section covers logical-replication consumer behavior that real apps hit
through tools like Electric, Zero, Debezium, and other `pgoutput`-based
pipelines. The full replication feature surface lives in
`postgresql-parity-issues.md`; this section tracks what real consumers
actually exercise.

- [x] Run `electricsql/electric` with
  `ELECTRIC_WRITE_TO_PG_MODE=logical_replication` against Doltgres. The
  Docker-backed harness starts `electricsql/electric:1.6.2` with Doltgres as
  the upstream PostgreSQL source, creates an Electric-managed logical
  replication slot, serves a shape for a `REPLICA IDENTITY FULL` table,
  observes insert/update/delete operations through the shape endpoint,
  advances `confirmed_flush_lsn`, and reconnects after a Doltgres restart.
  Pinned by testing/go/electric_sync_test.go's TestElectricSyncSmoke; verified
  with `DOLTGRES_ELECTRIC_SMOKE=1 go test ./testing/go -run
  '^TestElectricSyncSmoke$'` on 2026-05-09.
- [x] Prove Electric shape API behavior with `replica: "full"` and
  `REPLICA IDENTITY FULL` tables. TestElectricReplicaFullShapeAPI requests the
  shape endpoint with `replica=full`, updates one column on a
  `REPLICA IDENTITY FULL` table, and asserts Electric emits the full updated
  row including the unchanged non-key column. Pinned by
  testing/go/electric_sync_test.go; verified with
  `DOLTGRES_ELECTRIC_SMOKE=1 go test ./testing/go -run
  '^(TestElectricSyncSmoke|TestElectricReplicaFullShapeAPI)$'` on 2026-05-09.
- [ ] Run Zero with `ZERO_UPSTREAM_DB`, `ZERO_CVR_DB`, `ZERO_CHANGE_DB`, and
  `ZERO_CHANGE_STREAMER_MODE=discover` against Doltgres.
- [x] Prove publication-ownership flows where the consumer creates and owns
  publications and slots, not only repo-owned DDL. A non-superuser
  replication role now creates the source table, owns the publication, creates
  the logical replication slot, reads the relevant `pg_publication` and
  `pg_replication_slots` catalog rows, starts `pgoutput` for that publication,
  and receives relation/insert/commit messages. Pinned by
  testing/go/logical_replication_source_test.go's
  TestLogicalReplicationConsumerOwnedPublicationAndSlot; verified with
  `go test ./testing/go -run
  '^TestLogicalReplicationConsumerOwnedPublicationAndSlot$'` on 2026-05-09.
- [ ] Pin and test exactly the slot, publication, LSN, and replication-stat
  catalog queries each consumer issues.
- [ ] Document Doltgres as source-only unless live subscriber/apply behavior
  is implemented.
- [ ] Cover or reject Aurora / RDS-specific assumptions
  (`rds.logical_replication`, `pglogical`, `track_commit_timestamp`, RDS
  Proxy) that real-world stacks expose.
- [ ] Cover the rest of the replication feature surface in
  `postgresql-parity-issues.md` once consumers exercise it.

## Dump/admin/tooling TODO

- [x] `pg_dump` schema output against Doltgres - the real
  `pg_dump --schema-only --no-owner --no-privileges` can connect to
  Doltgres and emit table, default-expression, view, index, and foreign-key
  DDL for a representative schema. Pinned by
  testing/go/pg_dump_schema_probe_test.go. Broader dump/restore coverage
  remains tracked separately.
- [x] Query-form `COPY` - `COPY (SELECT ...) TO STDOUT` is parsed
  and streamed through the CopyOut protocol for filtered exports.
  Text format preserves `\N` NULLs and CSV format uses query output
  names for `HEADER TRUE`. Covered by
  testing/go/copy_form_probe_test.go.
- [x] `COPY FROM stdin` restore - `psql` can replay dump-shaped
  `COPY ... FROM stdin` text and CSV data streams into Doltgres,
  including `\N` NULLs, UUIDs, booleans, numeric values, jsonb
  payloads, and quoted CSV fields. Covered by
  testing/go/copy_from_stdin_restore_probe_test.go; broader pgx
  coverage in testing/go/copy_test.go also covers headers,
  generated-column column lists, chunking, and binary COPY round trips.
- [x] `information_schema.columns` - column-order queries used by
  pg_dump, drizzle-kit, prisma db pull, and Alembic autogenerate
  work end-to-end. `ordinal_position` reflects DDL order,
  `is_nullable` reports YES/NO accurately for NOT NULL constraints
  and PK columns, `data_type` emits PG type names (`integer`,
  `text`, `numeric`, `timestamp without time zone`, `character
  varying`), and `column_default` surfaces both literal and
  expression defaults (e.g. `CURRENT_TIMESTAMP`). Coverage in
  testing/go/info_schema_column_order_test.go pins the workload
  shapes.
- [x] `pg_matviews` - the catalog view exists and returns zero rows
  (with or without a `schemaname` filter), which is exactly the
  shape dump tools need to skip the matview repair branch cleanly.
  Materialized views themselves are not yet supported (tracked as
  the separate "Materialized views" item in the Schema/DDL TODO);
  this item only covers the no-matview catalog surface that dump
  tools can safely query today. Pinned by
  testing/go/pg_matviews_probe_test.go.
- [x] Extension availability catalogs -
  `pg_available_extensions` and `pg_available_extension_versions`
  list the supported extension shims (`btree_gist`, `citext`,
  `hstore`, `pgcrypto`, `plpgsql`, `uuid-ossp`, `vector`) plus any
  local PostgreSQL extension files Doltgres can see, and mark installed
  versions from `pg_extension`. Broader dump/restore coverage for
  extension-heavy schemas remains tracked by the restore-gate corpus.
  Pinned by
  testing/go/available_extensions_probe_test.go.
- [x] `pg_indexes` - prove index existence checks and conditional DDL.
  testing/go/migration_tool_introspect_test.go now installs and runs
  the real `pg` Node driver (the same driver Drizzle Kit, Prisma,
  and Sequelize sit on top of) against a live Doltgres instance and
  issues drizzle-kit's exact pg_class table-discovery and
  pg_constraint foreign-key-discovery queries — the workload-shape
  evidence the checklist asks for. Two underlying Doltgres bugs
  surfaced and were fixed along the way: column aliases on unknown-
  typed expressions were being clobbered to `?column?`, and bare
  literals/operator expressions were leaking input-expression text
  as the column name. The full `drizzle-kit introspect` binary
  harness in testing/go/drizzle_kit_introspect_test.go now runs end-
  to-end (the earlier opclass-join planner gap was closed by the
  `pg_index.indclass = ANY(...)` item below). Two assertions inside
  the binary harness (composite-PK and unique-constraint
  introspection) remain disabled — see the pg_constraint
  completeness item below for the dependency.
- [x] `pg_stat_user_indexes` - the view exists with the expected
  column shape (relname, idx_scan, last_idx_scan, idx_tup_read,
  idx_tup_fetch), returns one row per user index, and records live
  in-process counters for planner-chosen index scans. A lookup that
  returns no rows still increments idx_scan without inflating tuple
  counters; pinned by testing/go/pg_stat_user_indexes_probe_test.go.
- [x] `pg_class` / `pg_index` - low-level catalog inspection used by
  admin scripts and migration tools works end-to-end. The canonical
  "does the table exist?" join (`pg_class JOIN pg_namespace` filtered
  by `relkind='r'`) returns the right answer; `pg_index` reports
  `indisprimary` / `indisunique` accurately for PK indexes,
  non-PK unique indexes, and non-unique secondary indexes; and the
  `pg_index JOIN pg_class` enumeration that tools use to list every
  index on a table returns all three index types in alphabetical
  order. Coverage in testing/go/pg_class_pg_index_inspection_test.go.
- [x] `pg_constraint` completeness for primary-key and unique-constraint
  introspection - drizzle-kit, Prisma db pull, and Alembic autogenerate
  all join `information_schema.table_constraints` to
  `information_schema.constraint_column_usage` by `constraint_name`,
  then read `pg_constraint` with `contype='p'/'u'` to derive
  `primaryKey({...})` and `.unique()` blocks. Three gaps closed in
  doltgres: (1) `table_constraints` now overrides the upstream GMS
  default to emit PostgreSQL-style names (`<table>_pkey`,
  `<table>_<col>_key`) instead of the literal `PRIMARY`; (2)
  `constraint_column_usage` now emits one row per (constraint, column)
  pair for PK / unique / FK in addition to CHECK; (3) the
  `regnamespace` OID-alias type now exists with `regnamespace -> oid`
  implicit cast so drizzle's `connamespace = 'public'::regnamespace`
  composite-PK lookup resolves. Coverage in
  testing/go/pg_constraint_introspection_test.go pins all three
  surfaces against the exact drizzle-kit query shapes; the
  `drizzle-kit introspect` binary harness in
  testing/go/drizzle_kit_introspect_test.go now asserts
  composite-PK and unique-constraint shapes end-to-end.
- [x] Migration-tool introspection - `drizzle-kit introspect`
  runs end-to-end against Doltgres through the real `pg` Node driver
  and emits `schema.ts` for a schema slice with primary keys, a
  composite primary key, a unique constraint, non-unique indexes, and
  a foreign key. This covers one real migration-tool path; Prisma db
  pull and Alembic autogenerate remain useful future broadening but
  are no longer required to prove the checklist item. Covered by
  testing/go/drizzle_kit_introspect_test.go.
- [ ] Authorization-policy deployment - prove application-managed
  authorization-policy SQL (Zero `.permissions.sql`, Supabase RLS, or
  equivalent) loads and is interpreted correctly.

## Wire protocol and catalog metadata

These items track the wire-protocol and catalog-correctness surfaces
that GUI editors (TablePlus, DataGrip, DBeaver, pgAdmin) and ORM
introspection tools (Drizzle Kit, Prisma db pull, Alembic
autogenerate) inspect to drive editable result grids, schema diffs,
typed-exception handling, and client-side query timeouts. The
residual gap on each item is "real-consumer evidence": running the
actual GUI / migration binary against a live Doltgres instance
rather than only a Go-level harness.

- [x] `RowDescription.TableOID` - populate the source-table OID so
  GUI editors can resolve a result column back to a base table.
  Implementation in server/doltgres_handler.go walks the session
  search_path through the GMS provider and emits the same OID
  pg_class advertises. Workload-corpus evidence: the
  `drizzle-kit introspect` binary harness in
  testing/go/drizzle_kit_introspect_test.go runs end-to-end and
  produces a schema.ts that captures every table — drizzle's
  table-discovery query uses the TableOID-bearing pg_class scan.
- [x] `RowDescription.TableAttributeNumber` - emit the source-table
  attnum, not the result-set position. Implementation in
  server/doltgres_handler.go's sourceTableMeta cache. Drizzle Kit
  reads pg_attribute by attnum to map index columns back to
  their tables; the binary harness produces correct
  `index("name").on(table.col)` lines, which only works when
  attnum points at the right column.
- [x] Source attribution through `AliasedExpr` - keep the source
  table OID for `SELECT col AS x FROM t` AND for `SELECT a.id FROM t a`.
  extractAliasSourceHints walks plan.Project's expressions, and
  buildTableAliasMap walks the FROM-side of the plan to translate
  GetField table aliases back to the underlying ResolvedTable
  name. Anything not directly resolvable to a base column —
  computed expressions (`v + 1`), casts (`v::bigint`), function
  calls, scalar subqueries, derived tables (`FROM (SELECT …) sub`),
  CTE references, aggregates, CASE — falls through to TableOID=0,
  which matches what real PostgreSQL emits for non-attributable
  columns. Coverage in testing/go/select_field_metadata_test.go:
  TestSelectStarFieldMetadata pins the attributable side
  (including the "table-qualified aliased base column" subtest);
  TestSelectFieldMetadataNonAttributableColumns pins the
  non-attributable side (9 shapes that must report TableOID=0).
- [x] Startup `ParameterStatus` set - emit the same dozen messages
  real PG sends (`server_encoding`, `DateStyle`, `IntervalStyle`,
  `TimeZone`, `integer_datetimes`, `is_superuser`,
  `session_authorization`, `application_name` added alongside
  the four already present). Coverage by
  testing/go/parameter_status_test.go (pgx `PgConn().ParameterStatus`)
  and the JDBC-equivalent harness in
  testing/go/jdbc_evidence_test.go which asserts integer_datetimes
  drives binary-timestamp encoding the way JDBC consumes it.
- [x] `BackendKeyData` + `CancelRequest` - per-connection nonzero
  secret + a cancel-request handler that interrupts the active
  query. Implementation in server/cancel_registry.go; `pg_sleep`
  is context-aware so cancellation propagates. pgx coverage in
  testing/go/cancel_request_test.go. Real-binary-driver evidence
  in testing/go/psql_cancel_request_test.go: the harness spawns
  the actual psql client, runs `SELECT pg_sleep(20)`, sends
  SIGINT to psql's process group (psql's SIGINT handler is the
  exact CancelRequest path every PG GUI editor uses for "Stop
  query"), and asserts the query is interrupted in well under
  the 20s sleep.
- [x] `ErrorResponse` SQLSTATE codes - map common GMS / Dolt error
  kinds to the PostgreSQL SQLSTATE codes drivers branch on:
  23505 unique_violation, 23503 foreign_key_violation, 23502
  not_null_violation, 23514 check_violation, 42P01
  undefined_table, 42703 undefined_column, 22P02
  invalid_text_representation, 0A000 feature_not_supported,
  42P07 duplicate_table, 22012 division_by_zero, 22003
  numeric_value_out_of_range, 22001 string_data_right_truncation,
  42601 syntax_error, 42883 undefined_function, 25P02
  in_failed_sql_transaction, 40P01 deadlock_detected.
  Implementation landed in server/connection_handler.go's
  `errorResponseCode` across three layers — GMS error-kind matchers,
  MySQL-errno fallback, and message-prefix sniffing for errors that
  share errno 1105, including Dolt's commit-time "Unique Key
  Constraint Violation" shape. Coverage by testing/go/sqlstate_test.go (pgx,
  with cases for each code above) and
  testing/go/sqlalchemy_sqlstate_test.go which installs SQLAlchemy
  + psycopg3 in a venv and asserts each shape surfaces the right
  SQLAlchemyError subclass with the matching underlying SQLSTATE.
  Codes not yet mapped (notably 40001 serialization_failure for
  retry loops, 22008 datetime_field_overflow) fall through to
  XX000 internal_error.
- [x] `pg_attribute` index attribute names - the existing
  `indexAttributeName` helper already returns real column names
  for non-expression index attributes (the audit's
  "synthetic placeholder" claim was a false positive). Pinned by
  testing/go/pg_attribute_index_names_test.go which asserts every
  attname in pg_attribute matches the underlying table column.
- [x] `atttypmod` precision/scale preservation across the type
  families ORM introspection cares about - `TIMESTAMP(p)`,
  `TIME(p)`, `NUMERIC(p,s)`, and `VARCHAR(n)` all round-trip
  through CREATE TABLE → pg_attribute.atttypmod → format_type
  back to the original DDL textual form. Time-family OIDs go
  through `newTimeFamilyType`; numeric/varchar use the native
  typmod encoding (`((p<<16)|s)+4` for numeric, `n+4` for
  varchar, `-1` for unconstrained). Coverage by
  testing/go/time_precision_typmod_test.go (TIMESTAMP/TIME),
  testing/go/numeric_varchar_typmod_test.go (NUMERIC/VARCHAR
  including unconstrained `NUMERIC` / `VARCHAR` returning -1
  and format_type round-trip), and
  testing/go/jdbc_evidence_test.go which reads pg_attribute the
  way JDBC's ResultSetMetaData does and asserts typmod survives
  a binary-format round-trip.
- [x] `pg_class.reloftype=0` for ordinary tables - matches
  PostgreSQL's behavior (reloftype is only nonzero for typed
  tables created with `CREATE TABLE name OF composite_type`,
  which Doltgres does not yet support). Pinned by
  testing/go/pg_class_reloftype_test.go.
- [x] `information_schema.columns.collation_name` - reports NULL
  for default-collated string columns and non-string columns,
  matching PG, and surfaces the user-supplied collation name
  for columns declared with an explicit COLLATE. Parser fix in
  postgres/parser/sem/tree/create_table.go accepts the built-in
  PG collation names ("C", "POSIX", "default", "ucs_basic",
  `*.utf8` POSIX-style); resolver in
  server/ast/resolvable_type_reference.go threads the collation
  through DoltgresType.TypCollation; the
  information_schema columns_table reads it back via
  explicitCollationName. Coverage in
  testing/go/info_schema_collation_test.go positively asserts
  both halves: default-collated columns report NULL, and
  COLLATE "C" / "POSIX" surface the literal name.
- [x] `pg_index.indclass = ANY(...)` planner - resolve
  `oid = ANY(oidvector_col)` to a boolean predicate so
  drizzle-kit's exact opclass-discovery join executes. Fix in
  server/types/type.go: `ArrayBaseType` now drills through
  vector types (Oidvector, Int2vector). Coverage by
  testing/go/pg_index_indclass_any_test.go AND the full
  drizzle-kit introspect binary harness in
  testing/go/drizzle_kit_introspect_test.go (no longer skip-gated
  by `DOLTGRES_RUN_DRIZZLE_KIT=1`). The harness's composite-PK
  and unique-constraint assertions are still disabled — that's
  the `pg_constraint` completeness item in the dump/admin section,
  not a regression here.

## Lower-risk surfaces still requiring smoke tests

- [~] Basic driver pools and ORM CRUD across the advertised driver matrix.
  The pgx smoke harness covers the baseline app schema and transaction
  surface, and the postgres.js harness covers a secondary Node pooled-client
  path with CRUD, parameters, JSONB, arrays, concurrent reads, commit, and
  rollback. Broader ORM matrix proof remains open.
- [x] Basic `CREATE TABLE`, enums, regular FKs, simple unique constraints,
  and ordinary btree indexes. Pinned through a live pgx client by
  testing/go/app_compat_smoke_test.go.
- [x] UUID, timestamp / timestamptz, numeric, boolean, text / varchar, and
  JSONB column storage. Pinned through a live pgx client by
  testing/go/app_compat_smoke_test.go.
- [x] `jsonb_array_elements`, `jsonb_object_keys`, `jsonb_set`, JSON
  aggregates, and the JSONB GIN containment subset Doltgres supports.
  Pinned through a live pgx client by testing/go/app_compat_smoke_test.go
  and by the JSONB GIN coverage in testing/go/include_jsonb_gin_index_probe_test.go.
- [x] Arrays, `ANY`, `array_agg`, and ordinary aggregate behavior. Pinned
  through a live pgx client by testing/go/app_compat_smoke_test.go.
- [x] Basic transactions and simple savepoint nesting. Pinned through a
  live pgx client by testing/go/app_compat_smoke_test.go, with broader
  ORM-shape coverage in testing/go/savepoints_test.go and
  testing/go/sqlalchemy_savepoints_test.go.
- [x] Source-mode logical replication for the supported `pgoutput` subset:
  pglogrepl can identify the system, create/drop logical `pgoutput`
  slots, start replication for a publication, receive relation plus
  insert/update/delete/commit messages, advance confirmed flush LSN via
  standby status, and observe slot/stat catalog state. Pinned by
  testing/go/logical_replication_source_test.go's
  TestLogicalReplicationSourceProtocolAndCatalogs, with
  REPLICA IDENTITY FULL old-tuple coverage in the same file.

## Proposed dolt changes

Items here are gaps that doltgresql alone cannot close because the
seam lives inside the imported `github.com/dolthub/dolt/go` module.
Doltgresql consumes dolt as a published Go module (no `replace`
directive, no vendor copy, no local override), so any change to the
writer/editor/storage path requires an upstream dolt PR. The
investigation references below name the file:line targets so the
follow-up work has a starting point.

- [ ] CREATE INDEX CONCURRENTLY phase 4 — non-blocking writes during
  the index backfill. Today doltgresql runs phase 1 as a synchronous
  `IndexAlterableTable.CreateIndex`, which holds a write lock for
  the duration of the prolly-tree build. The phase 2 metadata flip
  is already lock-free (commit `665eba41`); only the build itself
  still serializes writes.

  The seam is in dolt's writer:
  `libraries/doltcore/sqle/writer/schema_cache.go:newWriterSchema`
  walks `Schema.Indexes().AllIndexes()` and populates
  `WriterState.SecIndexes`. The prolly table writer
  (`prolly_table_writer.go:Insert`/`Update`/`Delete`) iterates
  `w.secondary` for every row write. Adding a per-index "skip while
  pending" / "include during dual-write" decision in `newWriterSchema`
  is the single chokepoint.

  Minimal upstream patch (~100 lines across 4-5 files):
  1. `schema.IndexProperties` (libraries/doltcore/schema/index_coll.go)
     — add `NotReady bool` and `Invalid bool` fields, mirroring
     PostgreSQL's `pg_index.indisready` / `pg_index.indisvalid`.
  2. Schema serialization — preserve the new bits across the
     flatbuffers / nbf round-trip used by `UpdateSchema`.
  3. `newWriterSchema` — skip indexes flagged `NotReady=true` from
     `SecIndexes`, include `NotReady=false, Invalid=true` indexes so
     writers dual-write while the planner still ignores them.
  4. `AlterableDoltTable.CreateIndex`
     (libraries/doltcore/sqle/tables.go) — add a "register without
     building" mode so phase 1 can install the (pending, invalid)
     index entry without the synchronous backfill.
  5. Add a `BackfillIndex` method that populates the prolly tree
     from a snapshot read while writers continue against the live
     working set, with a final validation scan to pick up rows
     written between the snapshot point and the flip.

  Doltgresql side (post-patch): swap the synchronous `CreateIndex`
  in `server/node/create_index_concurrently.go` for the new
  register-then-backfill API, and have the state-machine flip drive
  the new bits through `flipIndexComment`'s peer (a
  `flipIndexBuildState` that toggles `NotReady`/`Invalid` directly
  on `IndexProperties` rather than the comment payload).

  Branch-and-merge alternative entirely inside doltgresql is
  technically possible via dolt's public branching/diff APIs, but
  that re-implements inside doltgresql what dolt already does
  internally and would run several hundred lines plus serious
  correctness work for iterative-catchup races. Not worth it when
  the upstream patch is ~100 lines.

  Why this is not urgent: doltgresql's prolly-tree builds are fast,
  so the phase 1 lock window is short for typical workloads. The
  state-machine + metadata-flip already in place gives ORMs the
  catalog/planner-visibility semantics they care about. Phase 4
  matters once table sizes push the build into multi-second
  territory.

## Current support claim

Do not claim end-to-end parity with arbitrary non-trivial PostgreSQL
applications until this checklist has workload evidence.

A defensible support claim looks like:

> Doltgres covers a meaningful subset of ordinary ORM and `pg`-style runtime
> SQL, but real-world PostgreSQL applications still hit hard compatibility
> blockers in schema bootstrap, materialized views, PL/pgSQL triggers,
> transaction-local settings, advisory lock helpers, advanced indexes,
> dump/restore tooling, and Electric / Zero-class replication runtime
> behavior.
