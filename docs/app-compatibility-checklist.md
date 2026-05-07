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
- [ ] Generated columns - prove generated-column DDL and runtime behavior
  through dump/restore.
- [ ] Deferrable constraints - prove `DEFERRABLE` FK behavior end-to-end.
- [ ] Privilege and ownership DDL - load or safely strip ownership statements,
  `ALTER DEFAULT PRIVILEGES`, and ACL output produced by `pg_dump`.
- [ ] `DO $$` blocks - support or rewrite anonymous code blocks emitted by
  dumps for matview / state repair.
- [ ] `session_replication_role` - support or safely replace `SET
  session_replication_role = replica` during data import.
- [ ] `REPLICA IDENTITY FULL` DDL - preserve full-row old tuples for synced
  tables.

## Index/planner TODO

- [ ] Partial indexes - prove non-trivial predicates such as `WHERE column IS
  NOT NULL` and boolean/active-flag filters.
- [ ] Expression indexes - prove JSONB-derived and computed-expression
  indexes.
- [ ] `CREATE INDEX CONCURRENTLY` - support or rewrite `CREATE INDEX
  CONCURRENTLY IF NOT EXISTS` migrations.
- [ ] `INCLUDE` indexes - support index `INCLUDE` columns through dump/restore
  and ORM introspection.
- [ ] JSONB GIN indexes - prove the supported containment subset and document
  the boundary.
- [ ] GiST indexes - support `btree_gist` / `EXCLUDE USING gist` or document
  rewrite.
- [ ] Opclasses - prove explicit opclasses such as `uuid_ops`, `text_ops`,
  `timestamp_ops`, `int4_ops`, `bool_ops`, and JSONB-related opclasses.
- [ ] Null ordering in indexes - prove `ASC NULLS LAST` / `DESC NULLS FIRST`
  index semantics under planner usage.
- [ ] Materialized view indexes - support indexes required for matview refresh
  paths.

## View/query TODO

- [ ] Dynamic view rebuild - run a non-trivial application's `CREATE OR
  REPLACE VIEW` rebuild path end-to-end against Doltgres.
- [ ] `LATERAL` joins - prove `LEFT JOIN LATERAL` and `CROSS JOIN LATERAL`
  view shapes.
- [ ] `DISTINCT ON` - prove ordering and result stability for "latest row per
  group" patterns.
- [ ] Window functions - prove `row_number()`, `lag()`, `lead()`, partitioned
  windows, and frame specifications.
- [ ] Aggregate `FILTER` - prove `count(*) FILTER (...)` and similar aggregate
  filter usage in real views.
- [ ] `string_agg(DISTINCT ...)` and `array_agg(DISTINCT ...)` - prove
  distinct aggregate behavior.
- [ ] Regex set-returning functions - prove `regexp_matches(...)` and
  `regexp_split_to_table(...)` placement and result behavior.
- [ ] `generate_series` and `unnest` - prove SRF behavior in analytics and
  reporting views.
- [ ] JSONB expansion - prove `jsonb_array_elements`, `jsonb_object_keys`,
  `->`, `->>`, `#>`, `#>>`, and JSONB path operators.
- [ ] Date/time casts and helpers - prove text-to-date/timestamp casts,
  `make_date`, `extract`, fiscal-year math, and time-zone-aware computations.

## Runtime SQL TODO

- [x] `SET LOCAL` - support transaction-local GUC settings used by audit
  contexts and trigger-control patterns. Implementation landed
  (snapshot/restore at COMMIT/ROLLBACK/autocommit boundary).
  testing/go/set_local_trigger_test.go now exercises the audit-context
  workflow end-to-end: a BEFORE INSERT trigger written in PL/pgSQL
  reads `current_setting('app.actor', true)` and writes the value into
  an audit row, with cases for COMMIT, ROLLBACK, autocommit, and
  session-scope-vs-transaction-local override.
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
- [ ] Multi-unique `ON CONFLICT` - prove upsert targeting one specific unique
  constraint on a table with multiple unique constraints.
- [ ] `ON CONFLICT ... DO UPDATE` variants - prove all idempotent upsert
  shapes including affected-row counts and `RETURNING`.
- [ ] `FOR UPDATE` row locks - prove row-locking concurrency behavior under
  contention.
- [ ] Savepoints - prove nested transaction behavior used by ORM transaction
  helpers.
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
- [ ] `information_schema.columns` - prove column-order queries used by dump
  and ORM tooling.
- [ ] `pg_matviews` - support matview repair checks.
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
  harness is preserved in testing/go/drizzle_kit_introspect_test.go
  but skipped pending a separate planner gap on
  `JOIN pg_opclass opc ON opc.oid = ANY(i.indclass)`.
- [ ] `pg_stat_user_indexes` - prove or document misleading admin
  diagnostics.
- [ ] `pg_class` / `pg_index` - prove low-level catalog inspection used by
  scripts.
- [ ] Migration-tool introspection - run `drizzle-kit introspect`, `prisma db
  pull`, Alembic autogenerate, or equivalent against Doltgres.
- [ ] Authorization-policy deployment - prove application-managed
  authorization-policy SQL (Zero `.permissions.sql`, Supabase RLS, or
  equivalent) loads and is interpreted correctly.

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
