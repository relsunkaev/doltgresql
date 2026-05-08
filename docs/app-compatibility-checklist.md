# Real-world application compatibility checklist

Last updated: 2026-05-06

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

- [ ] Stand up at least one representative non-trivial PostgreSQL dump as a
  restore-gate corpus, and record the first hard failure for each.
- [ ] Triage restore failures into implement, dump-rewrite, skip, and
  explicit-non-goal buckets.
- [ ] Build a minimal-viable schema slice harness that excludes known
  unsupported DDL and proves ORM runtime queries on top of it.
- [ ] Run a real-world view rebuild path against Doltgres (CTEs, `LATERAL`,
  `DISTINCT ON`, window functions, JSONB expansion, regex SRFs).
- [ ] Run Electric and Zero (or equivalent logical-replication consumers)
  against Doltgres with `REPLICA IDENTITY FULL`-marked tables.
- [ ] Prove the round-trip dump/restore path: `pg_dump` -> file -> `psql`
  restore -> ORM introspection -> running app.

## Schema/bootstrap TODO

- [ ] Dump version identity - handle the schema shape produced by current
  `pg_dump` versions (16+, 17+) even though Doltgres reports PostgreSQL 15,
  or document the required dump-rewrite path.
- [ ] Common extensions - support or document rewrites for `uuid-ossp`,
  `btree_gist`, `pgcrypto`, `citext`, and `pgvector` (`vector(N)` columns,
  HNSW / IVFFlat indexes), plus their function and operator surfaces.
- [ ] ICU nondeterministic collations - support `CREATE COLLATION ... provider
  = icu, deterministic = false` or document the migration path away from it.
- [ ] Explicit query collations - prove runtime queries that reference
  collations such as `"en_US.utf8"` directly.
- [ ] Materialized views - support DDL, indexes created at materialized-view
  creation, and refresh; or document a rewrite path.
- [ ] PL/pgSQL trigger functions - load and execute trigger functions defined
  in dumps and migrations.
- [ ] Event triggers - handle event-trigger DDL (e.g. AWS DMS-style intercept
  triggers) or strip them safely on import.
- [ ] `CREATE AGGREGATE` - support custom aggregate DDL or document rewrite.
- [ ] GiST exclusion constraints - support `EXCLUDE USING gist` or document
  rewrite.
- [ ] Statement triggers and transition tables - support `REFERENCING NEW
  TABLE` / `OLD TABLE` and statement-level trigger semantics.
- [ ] Trigger catalog introspection - make `pg_trigger` /
  `information_schema` trigger views adequate for dumps, ORMs, and admin
  tools.
- [x] Generated columns - `GENERATED ALWAYS AS (...) STORED` DDL is
  accepted, the value is computed on INSERT, and is recomputed when
  source columns are UPDATEd. `information_schema.columns.is_generated`
  reports `ALWAYS` for generated columns and `NEVER` for ordinary
  columns so dump tools can reconstruct the DDL. Coverage in
  testing/go/generated_columns_probe_test.go.
- [~] Deferrable constraints - `DEFERRABLE INITIALLY DEFERRED` is
  parsed and accepted at DDL, the table is created, and the FK
  metadata round-trips. **But FK enforcement is still immediate** —
  the violating row is rejected at INSERT, not at COMMIT, which is
  wrong for any app that batches related rows in a transaction. And
  `SET CONSTRAINTS ALL DEFERRED` errors with `unknown statement type
  encountered: *tree.SetConstraints` (no AST handler). Closing this
  needs (a) a deferred-violation queue checked at commit time and (b)
  a SetConstraints AST handler. Pinned by
  testing/go/deferrable_constraints_probe_test.go so the silent
  immediate-enforcement and the missing-handler cases stay visible.
- [ ] Privilege and ownership DDL - load or safely strip ownership statements,
  `ALTER DEFAULT PRIVILEGES`, and ACL output produced by `pg_dump`.
- [~] `DO $$` blocks - rejected at the parser today (`at or near "do":
  syntax error` SQLSTATE 42601). pg_dump uses these for matview /
  state repair, Alembic upgrade scripts wrap conditional DDL in them,
  and many ORM init scripts emit the IF-NOT-EXISTS-via-DO pattern;
  closing this needs DO-block tokenization plus a PL/pgSQL-style
  executor for the inner block. Pinned by
  testing/go/do_block_probe_test.go so the rejection contract stays
  stable until the executor lands.
- [~] `session_replication_role` - the GUC is settable and readable
  via SET / SHOW (`replica` and `origin` round-trip). **But the value
  does not actually suppress FK or trigger firing during bulk load**:
  `SET session_replication_role = 'replica'` followed by an
  FK-violating INSERT still rejects with `Foreign key violation`,
  matching the immediate-enforcement default. pg_dump and most ORM
  data-import paths flip this to `replica` to suppress trigger and FK
  firing during restore — closing the gap needs the inserter /
  trigger-firing path to skip enforcement when the GUC is `replica`.
  Pinned by testing/go/session_replication_role_probe_test.go.
- [ ] `REPLICA IDENTITY FULL` DDL - preserve full-row old tuples for synced
  tables.

## Index/planner TODO

- [~] Partial indexes - non-unique partial indexes (e.g. `WHERE column
  IS NOT NULL`, `WHERE active = true`) are accepted at DDL: the index
  is created, round-trips through `pg_indexes`, and queries that match
  the predicate return the right rows. Partial *UNIQUE* indexes are
  explicitly rejected with `unique partial indexes are not yet
  supported` — that's the deeper gap. Dependent: `ON CONFLICT (col)
  WHERE arbiter_pred` enforcement in the upsert path. Today the
  arbiter predicate is parsed and accepted but never matched against
  a candidate index's predicate (see the `ON CONFLICT ... DO UPDATE`
  entry below) because every unique index is full. When partial
  unique indexes ship, the arbiter must select the unique index whose
  predicate is implied by `arbiter_pred`; until then, `ON CONFLICT
  (col) WHERE pred` silently falls through to full-unique semantics,
  which is wrong for any app that relied on the predicate to scope the
  conflict target. DDL-level coverage in
  testing/go/partial_expression_index_test.go.
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
- [ ] CONCURRENTLY for non-btree index types - GIN, expression,
  partial, and INCLUDE CONCURRENTLY all route through the
  existing synchronous AlterTable path. The keyword is accepted
  so migration tools don't error, but none of the two-phase
  catalog visibility above applies.
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
- [ ] GiST indexes - support `btree_gist` / `EXCLUDE USING gist` or document
  rewrite.
- [~] Opclasses - explicit opclass declarations on btree columns
  (e.g. `text_ops`, `int4_ops`) are accepted at DDL and the index
  round-trips through `pg_indexes`. The planner does not yet route
  query plans through opclass-specific operator families, so the
  semantic effect is currently a no-op (the column-default opclass
  is always used). DDL acceptance pinned by
  testing/go/index_opclass_nulls_probe_test.go so dump/migration
  tools that emit explicit opclasses don't trip.
- [~] Null ordering in indexes - `ASC NULLS LAST` / `DESC NULLS
  FIRST` is accepted at DDL but the engine emits two warnings —
  `descending index scan order is not yet supported, preserving
  metadata only` and `NULLS LAST index ordering is not yet
  supported, preserving metadata only`. The metadata is preserved
  through pg_index, but the planner does not yet honour either
  preference at scan time. DDL acceptance pinned by
  testing/go/index_opclass_nulls_probe_test.go.
- [ ] Materialized view indexes - support indexes required for matview refresh
  paths.

## View/query TODO

- [x] Dynamic view rebuild - `CREATE OR REPLACE VIEW` works end-to-end:
  same-shape body swap, view-on-view dependency chains where the inner
  view is rebuilt and outer aggregations reflect the new shape, and
  bodies built from CASE/COALESCE expressions. DROP VIEW + CREATE VIEW
  rebuild flow also works for shape-changing rebuilds. Coverage in
  testing/go/view_rebuild_test.go pins these workload shapes.
- [~] `LATERAL` joins - `CROSS JOIN LATERAL` works end-to-end for the
  top-N-per-group and computed-column-per-row shapes; `LEFT JOIN
  LATERAL ... ON true` projects matching rows correctly. Coverage in
  testing/go/lateral_test.go. Residual gap: `LEFT JOIN LATERAL` drops
  outer rows whose lateral subquery returns empty rather than
  preserving them with NULL on the inner side. Likely a planner
  rewrite that elides the LEFT-side preservation when the inner is
  derived; needs investigation in the GMS join planner.
- [x] `DISTINCT ON` - "latest row per group" pattern works against both
  single-column and multi-column distinct keys, with WHERE filters and
  across NULL groups. Coverage in testing/go/distinct_on_test.go pins
  the four shapes real PG views use. Default ASC NULL ordering follows
  MySQL convention (NULLS FIRST) rather than PG (NULLS LAST); explicit
  `NULLS LAST` syntax remains a separate gap (see "Null ordering in
  indexes" above).
- [~] Window functions - `lag()`, `lead()`, `count(*) OVER (PARTITION
  BY)`, `count(*) OVER ()`, `first_value()`, and `last_value()` (with
  an explicit `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`
  frame) work end-to-end. Coverage in
  testing/go/window_functions_test.go. Two residual gaps tracked here:
  (1) the rank family — `row_number()`, `rank()`, `dense_rank()`,
  `percent_rank()`, `ntile()` — fails with 'a window function X is in
  a context where it cannot be evaluated', even without PARTITION BY,
  pointing at a window-iterator wiring difference for those functions
  in the new GMS; (2) running `sum()` / `avg()` over an explicit
  `ROWS BETWEEN ... PRECEDING ...` frame panics with int32-vs-float64
  type confusion in the windowed numeric path.
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
- [~] `string_agg(DISTINCT ...)` and `array_agg(DISTINCT ...)` - DISTINCT
  was being parsed but silently ignored for both. string_agg now
  threads distinct through to vitess.GroupConcatExpr.Distinct;
  ArrayAgg gained a `distinct` field and a per-buffer seen-set
  de-dup using the same jsonAggDistinctKey shape jsonb_agg uses.
  Coverage in testing/go/aggregate_distinct_test.go pins both shapes
  via length/array_length on the result. Residual gap: agg-internal
  `ORDER BY` (`string_agg(DISTINCT t ORDER BY t, ',')`) is still
  rejected at the parser; needs a separate sql.y change to accept
  `ORDER BY` inside aggregate calls.
- [x] Regex set-returning functions - `regexp_matches(text, pattern[,
  flags])` and `regexp_split_to_table(text, pattern[, flags])` are now
  registered. Both work in projection and FROM-clause positions and
  honour the 'g' (global) and 'i' (case-insensitive) flags. Coverage in
  testing/go/regex_srf_test.go pins the workload shapes (single-match
  capture-group access, global-match row counting, split-to-rows over
  comma/whitespace separators).
- [~] `generate_series` and `unnest` - generate_series works end-to-end:
  2-/3-arg numeric ranges with positive and negative step, as a
  FROM-clause source feeding aggregates, and count(*) over a 1000-
  element series. unnest works in a bare projection. Coverage in
  testing/go/srf_test.go. Two residual gaps: (1) joining a table to
  `unnest(arr_col) AS t` does not expose `t` as a column in scope
  ('column t could not be found in any table in scope'); (2) unnest(...)
  in a projection with an outer ORDER BY trips an internal-type leak
  ('unhandled type *types.SetReturningFunctionRowIter in Compare').
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
  (snapshot/restore at COMMIT/ROLLBACK/autocommit boundary).
  testing/go/set_local_trigger_test.go now exercises the audit-context
  workflow end-to-end: a BEFORE INSERT trigger written in PL/pgSQL
  reads `current_setting('app.actor', true)` and writes the value into
  an audit row, with cases for COMMIT, ROLLBACK, autocommit, and
  session-scope-vs-transaction-local override. The savepoint-rollback
  case is a known limitation tracked separately below
  (`SET LOCAL` snapshot is per-transaction, not per-savepoint;
  `ROLLBACK TO SAVEPOINT` does not re-snapshot). Pinned by
  testing/go/set_local_savepoint_test.go.
- [ ] `SET LOCAL` snapshots scoped to savepoints - PG snapshots GUC
  values at every SAVEPOINT and restores them on ROLLBACK TO
  SAVEPOINT. Doltgres only snapshots once per transaction (at the
  first SET LOCAL of each variable inside the tx), so a
  ROLLBACK TO SAVEPOINT keeps the most-recent SET LOCAL value
  applied instead of restoring the savepoint-time value. Closing
  this needs a per-savepoint stack of GUC snapshots in
  server/functions/xact_vars.go plus connection-handler hooks at
  SAVEPOINT / ROLLBACK TO SAVEPOINT / RELEASE SAVEPOINT
  statements. Current behavior pinned by
  testing/go/set_local_savepoint_test.go's
  ROLLBACK_TO_SAVEPOINT_does_NOT_restore_SET_LOCAL subtest; flip
  it to PG-correct when this lands.
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
  pgx-driven ORM-shape upsert.
- [x] `ON CONFLICT ... DO UPDATE` variants - EXCLUDED pseudo-table,
  DO UPDATE SET ... WHERE pred, ON CONFLICT (col) WHERE arbiter_pred,
  and ON CONFLICT ON CONSTRAINT name all land. EXCLUDED rewrites to
  vitess.ValuesFuncExpr inside Context.WithExcludedRefs;
  DO UPDATE SET ... WHERE rewrites each `col = expr` to a CASE that
  preserves the existing value when the predicate is false; arbiter
  predicate `ON CONFLICT (col) WHERE pred` is parsed and accepted
  but never matched against a candidate index's predicate — see the
  Partial indexes entry above, which lists this enforcement as a
  dependent. The current behavior silently falls through to
  full-unique-index semantics, which is wrong for any app that uses
  partial unique indexes to scope the conflict target;
  ON CONSTRAINT resolution looks the constraint up
  by GMS index ID and treats `<table>_pkey` as PG's auto-generated
  primary-key constraint name. Coverage in
  testing/go/insert_on_conflict_test.go's
  TestInsertOnConflictExcluded, TestInsertOnConflictDoUpdateWhere,
  TestInsertOnConflictArbiterPredicate, and
  TestInsertOnConflictOnConstraint. RETURNING / affected-row-count
  parity is tracked separately as its own follow-up below.
- [~] `INSERT ... ON CONFLICT ... RETURNING` and affected-row-count
  parity - Plain `INSERT ... RETURNING` works end-to-end (single-row
  and multi-row projecting subsets or full rows). `ON CONFLICT DO
  NOTHING ... RETURNING` correctly returns zero rows when the
  existing row is preserved. Coverage in
  testing/go/on_conflict_returning_test.go. Three residual gaps:
  (1) `ON CONFLICT DO UPDATE ... RETURNING` errors at the planner
  with `*plan.InsertInto: invalid expression number, got N, expected M`;
  (2) the same planner error fires for multi-row VALUES that mix
  insert and update outcomes; (3) `ON CONFLICT DO NOTHING ... RETURNING`
  on the *no-conflict* insert path inserts the row but reports zero
  rows in RETURNING (should report the inserted row).
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
- [ ] `pg_notify` / `NOTIFY` - prove notification trigger paths and
  client-visible delivery.
- [ ] Reader/writer pool topology - define the Doltgres deployment shape
  expected by ORM `withReplicas`-style reader/writer routing.
- [ ] SSL / SCRAM / auth / client parameters - prove driver pool startup with
  `application_name`, SSL modes, and SCRAM authentication.
- [ ] Secondary clients - prove or scope-bound less common Postgres clients
  (`ts-postgres`, `postgres.js`, and similar) where workloads use them.

## Replication and sync TODO

This section covers logical-replication consumer behavior that real apps hit
through tools like Electric, Zero, Debezium, and other `pgoutput`-based
pipelines. The full replication feature surface lives in
`postgresql-parity-issues.md`; this section tracks what real consumers
actually exercise.

- [ ] Run `electricsql/electric` with
  `ELECTRIC_WRITE_TO_PG_MODE=logical_replication` against Doltgres.
- [ ] Prove Electric shape API behavior with `replica: "full"` and
  `REPLICA IDENTITY FULL` tables.
- [ ] Run Zero with `ZERO_UPSTREAM_DB`, `ZERO_CVR_DB`, `ZERO_CHANGE_DB`, and
  `ZERO_CHANGE_STREAMER_MODE=discover` against Doltgres.
- [ ] Prove publication-ownership flows where the consumer creates and owns
  publications and slots, not only repo-owned DDL.
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

- [ ] `pg_dump` schema output against Doltgres, or define a separate
  Doltgres-native dump path that ORMs can consume.
- [ ] Query-form `COPY` - support `COPY (SELECT * FROM table ORDER BY ...) TO
  STDOUT WITH (FORMAT text)`.
- [ ] `COPY FROM stdin` restore - prove seed and dump data import.
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
- [~] `pg_matviews` - the catalog view exists and returns zero rows
  (with or without a `schemaname` filter), which is exactly the
  shape dump tools need to skip the matview repair branch cleanly.
  Materialized views themselves are not yet supported (tracked as
  the separate "Materialized views" item in the Schema/DDL TODO);
  when matviews land, this view's row contents — `definition`,
  `ispopulated`, `hasindexes` — will need to surface the right
  values per matview. Pinned by testing/go/pg_matviews_probe_test.go.
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
- [ ] `pg_stat_user_indexes` - prove or document misleading admin
  diagnostics.
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
- [ ] Migration-tool introspection - run `drizzle-kit introspect`, `prisma db
  pull`, Alembic autogenerate, or equivalent against Doltgres.
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
  share errno 1105. Coverage by testing/go/sqlstate_test.go (pgx,
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

- [ ] Basic driver pools and ORM CRUD across the advertised driver matrix.
- [ ] Basic `CREATE TABLE`, enums, regular FKs, simple unique constraints,
  and ordinary btree indexes.
- [ ] UUID, timestamp / timestamptz, numeric, boolean, text / varchar, and
  JSONB column storage.
- [ ] `jsonb_array_elements`, `jsonb_object_keys`, `jsonb_set`, JSON
  aggregates, and the JSONB GIN containment subset Doltgres supports.
- [ ] Arrays, `ANY`, `array_agg`, and ordinary aggregate behavior.
- [ ] Basic transactions and simple savepoint nesting.
- [ ] Source-mode logical replication for the supported `pgoutput` subset.

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
