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

- [x] Stand up at least one representative non-trivial PostgreSQL dump as a
  restore-gate corpus, and record the first hard failure for each.
  testing/go/import_dump_probe_test.go now restores the
  AlexTransit/venderctl pg_dump through `psql` without skipped
  statements.
- [x] Triage restore failures for the first restore-gate corpus into
  implement, dump-rewrite, skip, and explicit-non-goal buckets. The first
  gate now has an empty skip bucket after landing predicate-scoped unique
  partial indexes for the AlexTransit inventory indexes.
- [x] Expand restore-gate corpus beyond AlexTransit/venderctl. Add multiple
  external non-trivial application dumps, record the first hard failure for
  each dump, and keep each corpus runnable through the real `psql` restore
  path. testing/go/import_dump_probe_test.go now restores four additional
  external dumps through `psql`: Boluwatife-AJB/backend-in-node,
  linvivian7/fe-react-16-demo, kirooha/adtech-simple, and bartr/agency.
  Tracked by dg-7ug.1.
- [x] Triage expanded restore-corpus failures into implement, dump-rewrite,
  skip, and explicit-non-goal buckets, with each bucket tied to a tracked
  implementation task, documented rewrite, or documented non-goal. The
  expanded `psql` restore gate has an empty skip/rewrite/non-goal bucket. The
  former sequence-owning dump blocker
  (`SELECT last_value, is_called FROM public.goose_db_version_id_seq`) is now
  covered by the sequence relation-scan round-trip gate. Tracked by dg-7ug.1.
- [x] Build a minimal-viable schema slice harness that excludes known
  unsupported DDL and proves ORM runtime queries on top of it.
  testing/go/pg_dump_round_trip_test.go creates a representative
  UUID/JSONB/FK/index/view schema, dumps it with the real `pg_dump`,
  restores it through the real `psql`, introspects the restored schema
  with the real `drizzle-kit introspect` binary, and runs pgx app
  reads/writes on the restored database.
- [x] Run a real-world view rebuild path against Doltgres (CTEs, `LATERAL`,
  `DISTINCT ON`, window functions, JSONB expansion, regex SRFs).
  testing/go/view_rebuild_workload_test.go creates and then rebuilds
  an account-event analytical view that combines CTE staging,
  `JOIN LATERAL` JSONB and regex set-returning functions,
  `DISTINCT ON` latest-row selection, and partitioned window
  aggregates; both the initial view and `CREATE OR REPLACE VIEW`
  output are pinned.
- [x] Run Electric and Zero (or equivalent logical-replication consumers)
  against Doltgres with `REPLICA IDENTITY FULL`-marked tables. Electric is
  covered by testing/go/electric_sync_test.go's Docker-backed shape API
  harness. Zero is covered by testing/go/zero_sync_test.go's
  Docker-backed discover-mode harness, which runs Zero against separate
  upstream, CVR, and change databases, marks the source table
  `REPLICA IDENTITY FULL`, waits for the `pgoutput` slot to become active,
  deploys generated Zero permissions SQL, inserts through Doltgres, and
  verifies `confirmed_flush_lsn` advances.
- [x] Prove the schema-slice round-trip path: `pg_dump` -> file -> `psql`
  restore -> ORM introspection -> running app. The schema-slice
  harness in testing/go/pg_dump_round_trip_test.go now proves this
  path end-to-end with `pg_dump`, `psql`, `drizzle-kit introspect`,
  and pgx app queries.
- [x] Prove round-trip dump/restore for broader external app dumps without
  relying only on the schema-slice harness. The gate should document whether
  Doltgres' internal `dolt` schema is excluded, why, and what exact
  production-like dumps have passed `pg_dump` -> `psql` restore -> ORM
  introspection -> running app queries. testing/go/pg_dump_round_trip_test.go
  now restores external Boluwatife-AJB/backend-in-node and
  kirooha/adtech-simple dumps, dumps them back out with real
  `pg_dump --exclude-schema dolt`, restores that output through real `psql`,
  introspects the restored schema with real `drizzle-kit introspect`, and runs
  pgx reads/writes against the restored application tables. The `dolt` schema
  is excluded because it is Doltgres internal system state, not user
  application schema. Tracked by dg-7ug.2.
- [x] Support `pg_dump` sequence relation scans for sequence-owning dumps.
  PostgreSQL clients can resolve a sequence as a relation for
  `SELECT last_value, is_called FROM public.some_sequence`, while schema table
  enumeration still keeps sequence names out of ordinary table listings so
  `pg_dump` does not emit duplicate `CREATE TABLE` / `COPY` blocks for
  sequence relations. testing/go/sequences_test.go covers direct relation
  reads across `nextval` and `setval(..., false)`;
  testing/go/pg_dump_round_trip_test.go covers synthetic sequence-owned-table
  dump/restore and the kirooha/adtech-simple external dump that originally
  failed on `public.goose_db_version_id_seq`. Tracked by dg-7ug.13.

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
  `pg_catalog.pg_extension`, and the common runtime functions
  `uuid_nil`, `uuid_ns_dns`, `uuid_ns_url`, `uuid_ns_oid`,
  `uuid_ns_x500`, `uuid_generate_v1`, `uuid_generate_v1mc`,
  `uuid_generate_v3`, `uuid_generate_v4`, and `uuid_generate_v5` are
  callable for UUID defaults and deterministic namespace UUID generation.
  pg_dump's `CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA
  pg_catalog` shape is accepted and records the built-in PL/pgSQL
  runtime in `pg_extension`.
  `CREATE EXTENSION IF NOT EXISTS pgcrypto` is also accepted via a
  built-in compatibility shim, including `WITH SCHEMA` metadata:
  Doltgres does not load pgcrypto's PostgreSQL C library payload
  because it expects server symbols Doltgres does not export.
  `gen_random_uuid()` is registered as a native builtin and returns a
  36-char UUID, covering the common ORM/default-PK path. Native
  pgcrypto `digest(text, text)`, `digest(bytea, text)`,
  `hmac(text, text, text)`, and `hmac(bytea, bytea, text)` cover
  MD5, SHA1, SHA224, SHA256, SHA384, and SHA512, with unsupported
  algorithms rejected explicitly; `gen_random_bytes(int4)` returns
  cryptographic random `bytea` payloads for lengths 1-1024 and rejects
  out-of-range requests. ASCII armor helpers cover `armor(bytea)`,
  `armor(bytea, text[], text[])`, `dearmor(text)`, and
  `pgp_armor_headers(text)`, including header key/value lines, header
  extraction, CRC24 verification, and corrupt-armor rejection.
  PGP encryption/decryption and key-inspection routine signatures
  (`pgp_sym_*`, `pgp_pub_*`, and `pgp_key_id(bytea)`) are registered
  so dump-style routine ACLs resolve under extension schemas, but
  non-NULL runtime calls return an explicit feature-not-supported
  boundary until real PGP packet/key support exists.
  Extension-schema qualified calls such as
  `extensions.digest(...)`, `extensions.crypt(...)`,
  `extensions.armor(...)`, `extensions.dearmor(...)`,
  `extensions.pgp_armor_headers(...)`,
  `extensions.gen_random_bytes(...)`, and
  `extensions.gen_random_uuid()` honor dump-style `GRANT ALL ON FUNCTION`
  routine ACLs instead of bypassing them as unqualified builtins.
  `gen_salt('bf'[, int4])` and
  `crypt(text, text)` cover bcrypt/Blowfish password-hash generation and
  verification for the common `crypt(password, gen_salt('bf'))` and
  `stored_hash = crypt(password, stored_hash)` app flows, including
  PostgreSQL's `bf` default cost 6 and allowed cost range 4-31.
  `gen_salt('md5'[, int4])` and `crypt(text, '$1$...')` cover
  PostgreSQL-compatible MD5-crypt salt generation, the supported md5 round
  count, explicit bad-round rejection, stored-salt verification, salt
  truncation to eight characters, and wrong-password rejection.
  `gen_salt('des'[, int4])`, `gen_salt('xdes'[, int4])`, and
  `crypt(text, text)` cover PostgreSQL-compatible traditional DES and
  extended DES password hashes, including DES's eight-byte password
  truncation, xDES's default 725 rounds and odd 1..16777215 round range,
  stored-salt verification, and wrong-password rejection. Raw pgcrypto
  `encrypt`/`decrypt` and `encrypt_iv`/`decrypt_iv` cover the AES,
  Blowfish (`bf`), DES, and 3DES `cbc`/`ecb` modes with `pkcs` and
  `none` padding, including default zero-IV behavior, explicit CBC IVs,
  no-padding block-size validation, and algorithm-specific key-length
  validation. The
  `CREATE EXTENSION vector` shim is accepted and the native `vector(n)` type
  round-trips scalar embeddings.
  `CREATE EXTENSION btree_gist` is accepted as a catalog-only shim
  for dump restore and exposes extension-scoped `pg_opclass`,
  `pg_opfamily`, `pg_amop`, and `pg_amproc` rows for supported
  built-in scalar GiST opclasses (`oid`, integer, float, timestamp,
  time, date, interval, text, `bpchar`, `bytea`, `numeric`, bit,
  varbit, UUID, enum, and boolean families), while `CREATE INDEX ...
  USING gist` remains explicitly rejected. `CREATE EXTENSION citext`
  installs a case-insensitive text type in the target schema so dump
  schemas using `public.citext` can load, round-trip values, compare
  case-insensitively, and enforce case-insensitive `UNIQUE` checks on
  insert/update. Standalone btree `CREATE INDEX`, `CREATE UNIQUE INDEX`,
  `CREATE INDEX CONCURRENTLY`, inline `UNIQUE` constraints, and inline
  `PRIMARY KEY` constraints on `citext` columns, including multi-column index
  shapes, store normalized lower-key physical index slots while preserving
  PostgreSQL-facing `citext_ops` metadata; equality and range predicates use
  indexed access when the `citext` key is a usable btree prefix.
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
  `OPERATOR(public.#<#)` syntax. `hstore_hash` and
  `hstore_hash_extended` cover PostgreSQL-compatible hashes for empty,
  populated, NULL-valued, escaped,
  order-independent, duplicate-key-normalized, SQL NULL, and seeded inputs.
  hstore catalog introspection now exposes extension-scoped
  `btree_hstore_ops`, `gin_hstore_ops`, `gist_hstore_ops`, and
  `hash_hstore_ops` rows through `pg_opclass`, `pg_opfamily`,
  `pg_amop`, and `pg_amproc`, including the comparison, containment,
  existence, and support-procedure strategies used by dump/catalog
  clients. Physical hstore index execution remains an explicit
  unsupported boundary: hstore btree/GIN opclasses are rejected with
  opclass-specific unsupported errors, while hstore GiST/hash indexes
  remain rejected by unsupported index-method errors.
  `hstore_in`, `hstore_out`, `hstore_recv`, and `hstore_send` cover
  canonical text IO, PostgreSQL-compatible binary payloads, malformed
  receive headers, empty, populated, NULL-valued, escaped, and SQL NULL inputs.
  `hstore_to_json`,
  `hstore_to_jsonb`, `hstore_to_json_loose`, `hstore_to_jsonb_loose`,
  and explicit `hstore` casts to `json`/`jsonb` cover sorted key output,
  SQL NULL hstore values as JSON nulls, string escaping, loose numeric
  promotion, and boolean-looking hstore text remaining JSON strings.
  `CREATE EXTENSION vector` covers dense `vector(n)` storage, text IO,
  equality, `l2_distance`, `inner_product`, `vector_negative_inner_product`,
  `cosine_distance`, `vector_spherical_distance`, `l1_distance`, and the
  dense-vector distance operators `<->`, `<#>`, `<=>`, and `<+>`,
  including extension-qualified
  `OPERATOR(public.<->)` syntax and dimension-mismatch errors. The dense
  vector ordering functions/operators `vector_lt`, `vector_le`, `vector_ge`,
  `vector_gt`, `<`, `<=`, `>=`, and `>` use pgvector-compatible
  lexicographic ordering. Dense vector helpers and arithmetic cover
  `vector_dims`, `vector_norm`, `l2_normalize`, `subvector`,
  `binary_quantize`, `vector_add`, `vector_sub`, `vector_mul`,
  `vector_concat`, and the `+`, `-`, `*`, and `||` vector operators.
  The pgvector bit distance surface covers
  `hamming_distance(bit, bit)`, `jaccard_distance(bit, bit)`, and the
  `<~>` / `<%>` operators, including same-length validation and the
  zero-intersection Jaccard boundary.
  Dense vector aggregate support functions cover `vector_accum`,
  `vector_avg`, and `vector_combine`, including PostgreSQL's count-plus-sums
  state shape, empty-state average NULLs, zero-state combine behavior, and
  state/vector dimension mismatch rejection. Dense vector aggregate execution
  covers `sum(vector)` and `avg(vector)`, including empty/all-NULL NULL
  results and dimension mismatch rejection for unconstrained `vector` columns.
  Dense vector casts cover
  `array_to_vector(integer[]|real[]|double precision[]|numeric[], integer,
  boolean)`, `vector_to_float4(vector, integer, boolean)`, assignment and
  explicit casts from those array types to `vector(n)`, implicit and explicit
  casts from `vector` to `real[]`, NULL-array rejection, and typmod dimension
  mismatch errors.
  pgvector catalog introspection now exposes the extension-created `hnsw` and
  `ivfflat` access methods, dense-vector `vector_l2_ops`, `vector_ip_ops`,
  `vector_cosine_ops`, HNSW-only `vector_l1_ops`, bit `bit_hamming_ops` /
  `bit_jaccard_ops`, btree `vector_ops`, and their `pg_opfamily`,
  `pg_amop`, `pg_amproc`, and distance-operator rows after
  `CREATE EXTENSION vector`. Actual ANN index execution remains an explicit
  unsupported boundary: `CREATE INDEX ... USING hnsw` and
  `CREATE INDEX ... USING ivfflat` still return SQLSTATE `0A000`
  (`index method ... is not yet supported`).
  pgvector non-dense type shells now cover `halfvec(n)` and `sparsevec(n)`
  schema declarations and `pg_type` introspection. Non-NULL halfvec/sparsevec
  value IO remains an explicit unsupported boundary
  (`pgvector ... values are not yet supported`).
  `DROP EXTENSION IF EXISTS ...` is accepted for dump cleanup preludes
  and removes loaded extension rows from `pg_extension`.
  Pinned by testing/go/common_extensions_probe_test.go.
- [x] Replace common-extension shims with full parity or narrower tested
  non-goals. pgcrypto now has native runtime coverage for UUID/random bytes,
  digest/HMAC, raw encryption/decryption, password hashing, ASCII armor, and
  armor-header helpers; PGP encryption/decryption/key inspection routines are
  registered but explicitly rejected as unsupported. pgvector covers dense
  vector scalar operations, aggregates, casts, bit-distance families, ANN
  access-method/opclass catalogs, and halfvec/sparsevec schema/type shells;
  ANN index execution and non-NULL halfvec/sparsevec value IO remain explicit
  unsupported boundaries. `btree_gist` exposes the catalog/opclass surface
  needed by dumps while physical GiST index execution and exclusion semantics
  remain unsupported boundaries. Pinned by
  testing/go/common_extensions_probe_test.go. Tracked by dg-7ug.3.
- [x] Model physical `citext` index keys/opclasses and add a benchmark guardrail
  for PostgreSQL-style case-insensitive btree seeks. Standalone btree
  `CREATE INDEX`, `CREATE UNIQUE INDEX`, `CREATE INDEX CONCURRENTLY`, inline
  `UNIQUE` constraints, and inline `PRIMARY KEY` constraints on `citext`
  columns now build normalized lower-key physical indexes, preserve logical
  `citext_ops` catalog metadata, and use `IndexedTableAccess` for equality and
  range predicates when the `citext` key is a usable btree prefix. Pinned by
  testing/go/index_benchmark_test.go and catalog evidence in
  testing/go/common_extensions_probe_test.go. Tracked by dg-7ug.4.
- [x] Define the hstore operator-class catalog and physical-index boundary.
  `CREATE EXTENSION hstore` exposes btree/GIN/GiST/hash
  opclass/family/operator/procedure catalog rows, while physical hstore
  btree/GIN/GiST/hash index execution is explicitly unsupported rather
  than silently claimed. Pinned by testing/go/common_extensions_probe_test.go.
  Tracked by dg-7ug.5.
- [x] ICU nondeterministic collations - `CREATE COLLATION ... provider
  = icu, deterministic = false` is explicitly rejected with
  SQLSTATE `0A000` (`CREATE COLLATION is not yet supported`).
  Apps that need case-insensitive equality on string columns must
  rewrite to either `lower(col)` expression indexes (covered) or
  a `citext`-style application-level rewrite. Pinned by
  testing/go/icu_collation_probe_test.go and testing/go/sqlstate_test.go.
- [x] Explicit query collations - runtime `ORDER BY col COLLATE "C"`
  and `ORDER BY col COLLATE "POSIX"` both run and produce
  byte-order-correct ordering (uppercase before lowercase). Column-
  level `COLLATE "C"` survives DDL and round-trips through
  `information_schema.columns.collation_name`. Coverage in
  testing/go/explicit_collation_probe_test.go (and the existing
  testing/go/info_schema_collation_test.go for the
  default-vs-explicit collation_name assertion).
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
  and preserves the prior snapshot on unique-index refresh failures. Pinned by
  testing/go/materialized_view_probe_test.go.
- [x] Implement lock-free PostgreSQL-style `REFRESH MATERIALIZED VIEW
  CONCURRENTLY`. The accepted concurrent form currently validates the
  PostgreSQL preconditions, preserves the prior snapshot on failure, and
  builds replacement rows into a session-local staging table before touching
  the target matview, so readers keep seeing the old snapshot while the
  rebuild is in flight. The final target publication now builds a replacement
  Dolt table with the target schema and indexes off-root, validates inserts
  there, and publishes it with a single root-level table replacement. Pinned by
  testing/go/materialized_view_concurrently_contention_test.go. Tracked by
  dg-7ug.6.1.
- [x] Add materialized-view refresh performance guardrails for large snapshots,
  indexed matviews, and unique-index refresh failures. Concurrent refreshes
  stream staged rows into the off-root replacement builder instead of
  materializing a second full row slice in Go, preserve target indexes after
  larger refreshes, leave the old snapshot intact on duplicate-key refresh
  failures, and drop staging tables on errors. Pinned by
  testing/go/materialized_view_concurrently_contention_test.go. Tracked by
  dg-7ug.6.2.
- [x] Support `ALTER MATERIALIZED VIEW ... RENAME TO ...` for table-backed
  materialized views. Rename now preserves the materialized-view metadata, keeps
  the renamed relation queryable, removes the old name, leaves `pg_class.relkind`
  as `m`, and moves the `pg_matviews` row to the new matview name. Pinned by
  testing/go/materialized_view_probe_test.go. Tracked by dg-7ug.14.
- [x] Support `ALTER MATERIALIZED VIEW ... RENAME COLUMN ...` for table-backed
  materialized views. Rename-column now validates that the target relation is a
  materialized view, delegates the physical column rename through the existing
  table DDL path, restores the materialized-view metadata marker, keeps
  `pg_class.relkind = 'm'` / `pg_matviews` visibility intact, and preserves
  non-concurrent `REFRESH MATERIALIZED VIEW` into the renamed column. Pinned by
  testing/go/materialized_view_probe_test.go. Tracked by dg-7ug.15.
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
- [x] Event triggers - `CREATE EVENT TRIGGER` is rejected with
  PostgreSQL's event-trigger privilege boundary, SQLSTATE 42501
  (`permission denied to create event trigger`). DMS-style intercept
  triggers must be stripped from the dump before import. Pinned by
  testing/go/unsupported_ddl_probes_test.go and testing/go/sqlstate_test.go.
- [x] `CREATE AGGREGATE` - rejected with SQLSTATE 0A000 (`CREATE
  AGGREGATE is not yet supported`). Apps that depend on custom
  aggregates must rewrite to scalar UDFs / window functions.
  Pinned by testing/go/unsupported_ddl_probes_test.go.
- [x] GiST exclusion constraints - the `EXCLUDE USING gist (...)`
  table constraint is explicitly rejected with SQLSTATE `0A000`
  (`EXCLUDE constraints are not yet supported`). Apps that emit
  EXCLUDE constraints (range non-overlap enforcement) must rewrite
  to either application-level checks or an INSERT trigger that runs
  the overlap query. Pinned by testing/go/unsupported_ddl_probes_test.go
  and testing/go/sqlstate_test.go.
- [~] Statement triggers and transition tables - `FOR EACH
  STATEMENT` table triggers now execute once per matching
  INSERT/UPDATE/DELETE statement, and AFTER statement triggers may
  declare `REFERENCING OLD TABLE AS ...` / `NEW TABLE AS ...`
  transition relations that are queryable inside the trigger function.
  AFTER row-level INSERT/UPDATE/DELETE triggers may also declare those
  transition relations; each row firing receives its per-row `OLD` /
  `NEW` value while the transition relation contains the full
  statement-wide affected row set.
  BEFORE/AFTER TRUNCATE statement triggers now create, introspect via
  `pg_trigger` / `information_schema.triggers`, and fire with
  `TG_OP = 'TRUNCATE'`; row-level TRUNCATE triggers are rejected because
  PostgreSQL only supports TRUNCATE at statement level. Pinned by
  testing/go/trigger_test.go (TestStatementTriggerTransitionTables and
  the TRUNCATE statement trigger probe), including zero-row UPDATE transition
  sets, row-level transition-table triggers, and plain AFTER statement trigger
  self-queries against the target table.
- [x] Support PostgreSQL row-level transition-table triggers. AFTER
  INSERT/UPDATE/DELETE `FOR EACH ROW` triggers now accept `REFERENCING`
  transition-table declarations and expose the full statement-wide OLD/NEW row
  sets to each row firing. Pinned by
  testing/go/trigger_test.go's
  TestStatementTriggerTransitionTables/AFTER row triggers see statement
  transition tables. Tracked by dg-7ug.7.
- [x] Plain AFTER statement trigger self-query visibility against the target
  table - AFTER INSERT/UPDATE/DELETE statement triggers now close the DML
  source before firing trigger functions, so self-queries observe the
  post-statement table state. Pinned by
  testing/go/trigger_test.go's
  TestStatementTriggerTransitionTables/AFTER statement trigger self-query sees
  post-statement target state.
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
  the captured timing for persisted constraints, including
  `DEFERRABLE INITIALLY DEFERRED`, `DEFERRABLE INITIALLY IMMEDIATE`,
  and `NOT DEFERRABLE`; `pg_get_constraintdef()` deparses the same
  timing metadata using PostgreSQL's suffix shape.
  `SET CONSTRAINTS ALL|<name> DEFERRED/IMMEDIATE` now updates
  transaction-local timing for supported child-side FK checks; switching
  to `IMMEDIATE` validates pending deferred rows immediately and raises
  SQLSTATE `23503` on unresolved violations. Pinned by
  testing/go/deferrable_constraints_probe_test.go.
- [x] Persist deferrability metadata in Doltgres-owned root metadata so
  `DEFERRABLE` / `INITIALLY DEFERRED` behavior survives server restart,
  keeps `pg_constraint` / `pg_get_constraintdef()` output stable after
  reopening the database, and preserves `SET CONSTRAINTS` transaction
  behavior after the in-memory registry is cleared. Pinned by
  `TestDeferrableForeignKeyTimingSurvivesRestart`.
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
  migrations, and IF-NOT-EXISTS init scripts. Coverage also exercises
  declaration type aliases and arrays, integer loops, `SELECT INTO`
  plus `FOUND`, query loops over `RECORD`, `PERFORM`, and raised
  exceptions inside anonymous blocks. Dynamic `EXECUTE` supports
  `format(...)` command strings for DDL and DML, including scalar
  `USING` expressions evaluated at runtime and `EXECUTE ... INTO`
  variable assignment. Other procedural
  languages are rejected explicitly. Pinned by
  testing/go/do_block_probe_test.go and server/plpgsql/parse_test.go.
- [x] SQL string-construction helpers used by dynamic migration blocks -
  native `format(text, variadic any)` supports the common `%s`, `%I`,
  `%L`, `%%`, positional-argument, numeric-width, and left-justify
  shapes, sharing PostgreSQL-style identifier/literal quoting with
  `quote_ident` and `quote_literal`. Argument-sourced widths are not
  claimed. Pinned by testing/go/functions_test.go.
- [x] Expand anonymous `DO $$` coverage beyond conditional-DDL blocks to the
  broader PL/pgSQL interpreter surface that application migrations can embed
  inside DO statements, without claiming full PL/pgSQL parity.
- [x] Dynamic PL/pgSQL `EXECUTE INTO` row-count semantics - non-`STRICT`
  assignment keeps the first row from multi-row results, while `INTO STRICT`
  raises on no-row and multi-row results. Pinned by
  testing/go/do_block_probe_test.go and server/plpgsql/parse_test.go.
- [x] PL/pgSQL `GET DIAGNOSTICS ... ROW_COUNT` - anonymous DO blocks can
  assign the row count from the most recent SQL command, including regular
  DML, dynamic `EXECUTE` DML, zero-row DML, and `PERFORM` query results.
  Pinned by testing/go/do_block_probe_test.go and server/plpgsql/parse_test.go.
- [x] PL/pgSQL `GET DIAGNOSTICS ... PG_CONTEXT` - anonymous DO blocks can
  assign PostgreSQL-shaped current-frame context text, including the
  inline-code-block function name and get-diagnostics line number, and can
  retrieve it alongside `ROW_COUNT` in the same statement. Pinned by
  testing/go/do_block_probe_test.go and server/plpgsql/parse_test.go. Tracked
  by dg-7ug.17.
- [x] PL/pgSQL `PG_CONTEXT` includes nested PL/pgSQL routine frames. A
  `GET DIAGNOSTICS ... PG_CONTEXT` executed inside a function called from
  another PL/pgSQL routine now returns the current routine plus caller routine
  frames, including anonymous DO-block callers. Pinned by
  testing/go/do_block_probe_test.go. Tracked by dg-7ug.19.
- [x] PL/pgSQL `PG_CONTEXT` caller frames include PostgreSQL-shaped
  source-location/action text for representative assignment, SQL statement,
  and anonymous DO-block `PERFORM` call sites, including SQL statement
  snippets where available. Pinned by testing/go/do_block_probe_test.go.
  Tracked by dg-7ug.21.
- [x] PL/pgSQL `GET DIAGNOSTICS ... PG_ROUTINE_OID` - named PL/pgSQL
  functions can assign their current routine OID to an `oid` variable and
  return it as a nonzero routine identity. Anonymous DO blocks return OID zero
  because they do not have a `pg_proc` routine entry. Pinned by
  testing/go/do_block_probe_test.go and server/plpgsql/parse_test.go. Tracked
  by dg-7ug.18.
- [ ] Implement the remaining PL/pgSQL diagnostics surface (`GET STACKED
  DIAGNOSTICS` and exception diagnostics) before claiming full diagnostics
  parity. Tracked by dg-7ug.20.
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
  comparisons, IN / NOT IN lists, BETWEEN / NOT BETWEEN / BETWEEN SYMMETRIC,
  simple `lower(text)` / `upper(text)` calls, parentheses, AND, and OR,
  which covers the
  AlexTransit/venderctl `WHERE at_service` and `WHERE NOT at_service`
  restore path. `ON CONFLICT (col) WHERE arbiter_pred` now resolves
  exact predicate matches and simple conjunctive arbiter predicates that imply
  the metadata-backed partial unique index predicate, boolean equivalences such
  as `WHERE active` / `WHERE active = true` and `WHERE NOT active` /
  `WHERE active = false`, plus same-column numeric inequality arbiters that
  describe a subset of the partial index predicate (for example `score > 10`
  targeting an index predicate `score > 0`, or `score BETWEEN 10 AND 90`
  targeting `score > 0 AND score < 100`), same-column equality/IN-list
  value-set subsets, same-expression `lower(text)` / `upper(text)`
  equality/IN-list value-set subsets, and simple OR implication where the
  arbiter predicate implies one index-predicate disjunct or every reordered
  arbiter disjunct implies the index predicate;
  non-target partial-unique conflicts are preserved for multi-unique
  `DO NOTHING`. The btree planner now hides partial indexes from generic
  costing and opts them back in only when the query's scalar filters imply
  the partial-index predicate for simple column lookup shapes, plus safe
  prefix `LIKE` lookups through partial `text_pattern_ops` indexes, and
  same-column null-safe `IS NOT DISTINCT FROM NULL` predicates that prove
  matching `IS NULL` partial predicates. DDL, DML, and planner coverage in
  testing/go/partial_expression_index_test.go; real dump proof in
  testing/go/import_dump_probe_test.go; upsert coverage in
  testing/go/insert_on_conflict_test.go.
- [x] Broaden partial-index planner implication beyond exact-shape matching.
  Simple btree lookup plans now use partial indexes when scalar query filters
  imply the stored predicate, for example `score > 10` proving an index
  predicate of `score > 0`, while non-implying filters such as `score >= 0`
  stay on the table-scan path. Coverage in
  testing/go/partial_expression_index_test.go. Tracked by dg-7ug.8.2.
- [x] Use `IS NOT NULL` partial btree indexes for strict lookup predicates.
  The implication helper now recognizes non-NULL equality / IN-list,
  numeric-range, and boolean predicates as proving a matching
  `expr IS NOT NULL` partial-index predicate, including simple
  `lower(text)` / `upper(text)` expression predicates, while unsafe
  `IS DISTINCT FROM` and null-only shapes stay unsupported. Coverage in
  server/indexpredicate/implication_test.go and
  testing/go/partial_expression_index_test.go. Tracked by dg-7ug.8.5.
- [x] Use exclusion-style partial btree predicates for disjoint literal
  lookups. Equality and IN-list query predicates now imply same-expression
  partial-index predicates of the form `expr != literal` and
  `expr NOT IN (literal, ...)` when the query literal set is disjoint from the
  excluded values, including simple `lower(text)` / `upper(text)` expressions.
  Null-sensitive shapes such as `IS DISTINCT FROM` remain outside this slice.
  Coverage in server/indexpredicate/implication_test.go and
  testing/go/partial_expression_index_test.go. Tracked by dg-7ug.8.6.
- [x] Use partial pattern-opclass btree indexes for prefix `LIKE` lookups.
  Partial `text_pattern_ops` indexes now remain hidden from generic costing
  but are opted back in when the query includes a safe fixed-prefix `LIKE`
  predicate on the indexed column and separate scalar filters imply the
  partial-index predicate. Non-implying filters and unsafe non-prefix `LIKE`
  patterns stay on the table-scan path. Coverage in
  testing/go/partial_expression_index_test.go. Tracked by dg-7ug.8.7.
- [x] Use `IS NULL` partial btree indexes for null-safe NULL predicates.
  Same-column and simple same-expression `expr IS NOT DISTINCT FROM NULL`
  filters now imply matching `expr IS NULL` partial-index predicates, while
  non-NULL, wrong-column, and `IS DISTINCT FROM NULL` predicates remain
  unsupported. Coverage in server/indexpredicate/implication_test.go and
  testing/go/partial_expression_index_test.go. Tracked by dg-7ug.8.8.
- [x] Use `IS DISTINCT FROM` partial btree indexes for safe same-expression
  lookups. Exact `expr IS DISTINCT FROM literal`, disjoint equality/IN-list
  filters, and NULL filters now imply matching null-inclusive
  `expr IS DISTINCT FROM literal` predicates; `expr IS DISTINCT FROM NULL`
  is treated as the supported `IS NOT NULL` predicate shape. Conflicting
  literals and wrong expressions still stay off the indexed path. Coverage in
  server/indexpredicate/implication_test.go and
  testing/go/partial_expression_index_test.go. Tracked by dg-7ug.8.11.1.
- [x] Use cross-column equality partial btree indexes when query conjuncts
  safely prove the equality predicate by binding both sides to the same
  singleton literal value. Null-safe equality predicates also accept paired
  `IS NULL` facts, while mismatched, incomplete, multi-value, and SQL-null
  equality shapes stay off the indexed path. Coverage in
  server/indexpredicate/implication_test.go and
  testing/go/partial_expression_index_test.go. Tracked by dg-7ug.8.9.1.
- [x] Use deterministic unary `ltrim`/`rtrim` predicates in partial-index
  implication paths. `ltrim(expr)` and `rtrim(expr)` now participate in the
  implication helper, partial unique predicate evaluation, planner predicate
  serialization, and `ON CONFLICT` arbiter inference, while non-equivalent
  expression shapes stay rejected. Coverage in
  server/indexpredicate/implication_test.go,
  testing/go/partial_expression_index_test.go, and
  testing/go/insert_on_conflict_test.go. Tracked by dg-7ug.8.10.1.
- [x] Use deterministic unary `btrim(expr)` predicates in partial-index
  implication paths. `btrim(expr)` now participates in the implication helper,
  partial unique predicate evaluation, planner predicate serialization, and
  `ON CONFLICT` arbiter inference, while non-matching literal predicates stay
  rejected. Coverage in server/indexpredicate/implication_test.go,
  testing/go/partial_expression_index_test.go, and
  testing/go/insert_on_conflict_test.go. Tracked by dg-7ug.8.10.2.
- [ ] Continue PostgreSQL-style partial-index predicate implication beyond the
  current conservative subset. Broader cross-column/equality-class proofs,
  broader expression-level semantic implication beyond the currently
  supported deterministic unary function families, and planner deparsing for
  additional predicate families remain open.
  Tracked by dg-7ug.8.9, dg-7ug.8.10, and dg-7ug.8.11 under dg-7ug.8.
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
  / op.drop_index(..., postgresql_concurrently=True).
- [x] CONCURRENTLY non-blocking writes during Phase 1 - a deterministic
  regression pauses Dolt's local `creation.CreateIndex` path after index
  metadata is registered in memory and before the secondary prolly tree is
  built, then proves a second session can insert, update, and delete rows
  through a timeout-bounded writer while session A remains in the Phase 1
  build. After session A resumes, the UNIQUE CONCURRENTLY build completes,
  `pg_index` is valid/ready, the planner uses `IndexedTableAccess`, and the
  final index sees the inserted row, updated row, and deleted-row absence.
  Pinned by testing/go/create_index_concurrently_contention_test.go's
  TestCreateIndexConcurrentlyAllowsWritersDuringPhase1, with the local Dolt
  pause point kept unexported and installed only by the SQL test binary through
  testing/go/create_index_concurrently_test_hooks_test.go.
- [x] Add large-table `CREATE INDEX CONCURRENTLY` performance guardrails. The
  guardrail builds a secondary btree index over an 8k-row table with a
  deterministic Phase 1 pause, proves insert/update/delete writers complete
  under a timeout while the build is in progress, bounds the post-release build
  path, and verifies the final planner uses `IndexedTableAccess` for a row
  written during the build. Pinned by
  testing/go/create_index_concurrently_contention_test.go. Tracked by
  dg-7ug.8.4.
- [~] CONCURRENTLY for metadata-backed and non-btree index shapes -
  btree `INCLUDE`, non-unique btree partial, supported JSONB GIN, and
  non-unique btree expression indexes now use the same two-phase
  `pg_index.indisready=false` / `pg_index.indisvalid=false` catalog
  visibility as plain btree `CREATE INDEX CONCURRENTLY`, then flip to
  ready/valid after the inter-phase commit; partial-unique btree indexes
  keep predicate-scoped uniqueness after the final flip. Unique single-
  expression btree indexes also expose the two-phase catalog state while
  preserving functional uniqueness semantics. Pinned by
  testing/go/create_index_concurrently_test.go and
  testing/go/create_index_concurrently_contention_test.go.
- [x] Route unique expression `CREATE INDEX CONCURRENTLY` through the two-phase
  catalog state machine. The unique expression path builds through the ordinary
  functional-index resolver, flips the PostgreSQL readiness metadata before the
  inter-phase commit, exposes `(indisready=false, indisvalid=false)` to another
  session, then flips back to ready/valid. Coverage in
  testing/go/create_index_concurrently_test.go verifies final catalog state,
  `lower(email)` uniqueness, and duplicate-data cleanup; coverage in
  testing/go/create_index_concurrently_contention_test.go verifies cross-session
  phase visibility for the unique expression shape.
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
- [x] GiST indexes - rejected with SQLSTATE 0A000 `index method
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
  is preserved through pg_index. The btree planner boundary now fences
  metadata-only DESC / NULLS sort options from sort-elision while still
  allowing predicate lookup through the same index. Pinned by
  testing/go/index_opclass_nulls_probe_test.go and
  testing/go/index_benchmark_test.go.
- [x] Fence metadata-only btree sort options from ordered-scan planning.
  Indexes with preserved PostgreSQL DESC / NULLS FIRST/LAST options now
  report `IndexOrderNone` to sort-elision rules until physical storage can
  honor those options; ordinary btree ordered scans and predicate lookup on
  the fenced index stay covered. Tracked by dg-7ug.8.3.1.
- [x] Enable ordered-scan planning for btree sort-option indexes whose affected
  key columns are real `NOT NULL` table columns. The planner can safely use
  these indexes for matching `ORDER BY ... DESC NULLS LAST` / explicit null
  option shapes because null placement cannot change the physical order, while
  nullable keys remain fenced. Pinned by testing/go/index_benchmark_test.go.
  Tracked by dg-7ug.8.3.2.
- [ ] Model physical descending and NULLS FIRST/LAST index scan ordering in
  index storage and PostgreSQL-style planner preference. Today those
  per-column scan choices are metadata-preserved but not stored as
  PostgreSQL-style physical index ordering. Tracked by dg-7ug.8.3.3,
  dg-7ug.8.3.4, and dg-7ug.8.3.5 under dg-7ug.8.3.
- [~] Materialized view indexes - ordinary and unique btree indexes can be
  created on table-backed materialized views, round-trip through
  `pg_indexes`, set `pg_class.relhasindex`, and flip
  `pg_matviews.hasindexes`. This covers indexed restore-time snapshots
  and read paths. Non-concurrent `REFRESH MATERIALIZED VIEW` preserves
  existing matview indexes, and unique-index violations leave the prior
  snapshot intact. `REFRESH MATERIALIZED VIEW CONCURRENTLY` now validates
  PostgreSQL-style unique-index eligibility for all-row unique btree
  column indexes and rejects missing or partial unique indexes. Pinned by
  testing/go/materialized_view_probe_test.go.

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
- [x] Reader/writer pool topology - Drizzle ORM's
  `withReplicas()` reader/writer routing runs against Doltgres with
  distinct primary and reader URLs. The harness seeds a primary
  database and a reader database differently, proves ordinary
  `SELECT` calls use the reader URL, proves `$primary` reads use the
  primary URL, and proves `INSERT`, `UPDATE`, and `DELETE` route to
  the primary. This defines the supported topology for ORM-level
  reader/writer split: Doltgres supplies PostgreSQL-compatible
  primary and reader endpoints, while physical replication or
  application-managed catch-up remains outside this routing proof.
  Pinned by testing/go/drizzle_read_replicas_test.go.
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
  commit, and rollback-on-error behavior. `node-postgres` (`pg`) runs a
  real `Pool` through startup parameters, pooled CRUD, typed parameters,
  JSONB and array binding, named prepared statements, concurrent reads, and
  explicit transaction clients. `ts-postgres` also runs through the real
  client package, including startup, explicit `application_name`,
  extended-protocol parameter binding, binary result decoding, explicit
  prepare/execute/close, transaction commit, and rollback behavior. Knex runs
  over the real `pg` driver and covers schema builder DDL, query-builder joins,
  typed inserts, JSONB/text[] values, raw `ANY` predicates, pooled concurrent
  reads, commit, and rollback behavior. `pg-promise` runs over the real `pg`
  driver with task/transaction helpers, prepared statements, typed parameters,
  JSONB/text[] values, pooled concurrent reads, commit, and rollback behavior.
  `psycopg` runs through the real psycopg3 pool, including startup `application_name`,
  parameter binding, JSONB and array adaptation, concurrent reads, transaction
  commit, and rollback behavior. `psycopg2` runs through the legacy Python
  client pool, including startup `application_name`, typed parameters, JSONB
  and text[] adaptation, prepared statements, concurrent reads, commit, and
  rollback behavior. Ruby `pg` runs through a real PG::Connection,
  including startup `application_name`, typed parameters, JSONB and text[]
  values, prepared statements, concurrent reads, commit, and rollback. libpq
  runs through a compiled C probe with startup `application_name`, typed
  parameters, JSONB and text[] values, prepared statements, multiple
  connections, commit, and rollback. Go `database/sql` with `github.com/lib/pq`
  runs through the real Go driver wrapper with startup `application_name`,
  prepared statements, typed parameters, JSONB/text[] values, `pq.Array`
  adaptation, pooled concurrent reads, commit, and rollback. Java JDBC runs
  through the upstream PostgreSQL JDBC driver with startup `application_name`,
  prepared statements, typed parameters, `createArrayOf` text[] adaptation,
  JSONB values, multiple connections, commit, and rollback. Rust `sqlx` runs
  through the existing async pool fixture with startup, parameter binding,
  catalog `EXISTS` queries, UUID binding/decoding, and chrono
  timestamptz/date decoding. TypeORM runs a
  real `DataSource` over the `pg` driver, including schema synchronization,
  repository CRUD, JSONB/text[] values, relation joins, commit, and rollback.
  Sequelize runs over the real `pg` driver too, covering startup timezone GUCs,
  `sync({ force: true })` schema management, model CRUD, associations,
  JSONB/text[] values, pooled reads, managed commit, and managed rollback.
  Pinned by
  testing/go/postgres_js_client_test.go,
  testing/go/node_postgres_client_test.go,
  testing/go/ts_postgres_client_test.go, testing/go/knex_client_test.go,
  testing/go/pg_promise_client_test.go, testing/go/psycopg_client_test.go,
  testing/go/psycopg2_client_test.go, testing/go/ruby_pg_client_test.go,
  testing/go/libpq_client_test.go, testing/go/go_sql_pq_client_test.go,
  testing/go/jdbc_client_test.go, testing/go/rust_sqlx_client_test.go,
  testing/go/typeorm_client_test.go, and testing/go/sequelize_client_test.go.
- [ ] Add other secondary-client smoke gates when workloads require those
  clients, rather than implying support from the existing Node harnesses alone.
  Tracked by dg-7ug.10.3 under dg-7ug.10.

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
- [x] Run Zero with `ZERO_UPSTREAM_DB`, `ZERO_CVR_DB`, `ZERO_CHANGE_DB`, and
  `ZERO_CHANGE_STREAMER_MODE=discover` against Doltgres. The Docker-backed
  smoke harness starts `rocicorp/zero:1.4.0` with a local `file://`
  Litestream backup target for the view-syncer, uses distinct Doltgres
  databases for the upstream source, CVR, and change state, waits for the
  Zero-created publication and active `pgoutput` slot, generates and applies
  Zero permissions SQL with `zero-deploy-permissions`, validates a direct
  permissions deploy against the published table/columns, inserts into a
  `REPLICA IDENTITY FULL` table, and asserts `confirmed_flush_lsn` advances
  past the insert LSN. Pinned by testing/go/zero_sync_test.go's
  TestZeroDiscoverModeSmoke; verified with
  `DOLTGRES_ZERO_SMOKE=1 go test ./testing/go -run
  '^TestZeroDiscoverModeSmoke$'` on 2026-05-09.
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
- [~] Pin and test exactly the slot, publication, LSN, and replication-stat
  catalog queries each consumer issues. Electric 1.6.2's compiled query
  literals are pinned through
  testing/go/logical_replication_source_test.go's
  TestLogicalReplicationElectricCatalogProbeQueries: publication owner and
  operation checks, publication relation discovery, replica identity lookup by
  `oid[]`, relation-drift detection through `UNNEST`, snapshot/current-LSN
  reads, and replication-slot telemetry based on `pg_wal_lsn_diff`. Verified
  with `go test ./testing/go -run
  '^TestLogicalReplicationElectricCatalogProbeQueries$'` on 2026-05-09. Zero
  1.4.0's startup and catalog surfaces are now covered by the live
  discover-mode smoke plus pinned compatibility probes for publication
  grammar, `pg_index.indclass = ANY(...)`, `int2vector` / `oidvector`
  slices, zero-shaped published-column catalog CTEs, exported snapshots,
  `pg_logical_emit_message`, `pg_column_size`, `starts_with`, array
  operators, array subscripts, and JSON output serialization. Debezium's
  source-derived startup and publication probes are pinned through
  TestLogicalReplicationDebeziumCatalogProbeQueries against Debezium main
  commit 09592e3f9a6e1fa518940343cb1d37a8ef8e76c6: slot lookup via
  `pg_replication_slots`, `pg_is_in_recovery()` / current-WAL-LSN discovery,
  `version()` / `current_user` / `current_database()` server info,
  `pg_roles` membership through `pg_has_role`, publication existence checks,
  and `pg_publication_tables` discovery/validation. Debezium's optional
  `pg_replication_slot_advance` seek path remains outside the claimed surface;
  upstream Debezium treats an undefined function there as a non-fatal old-server
  boundary.
- [x] Capture Debezium's `pgoutput` consumer query surface in this checklist.
  The executable probe uses the exact source query shapes with JDBC parameter
  markers translated to pgx `$n` markers for the Go harness. Source references:
  `PostgresConnection.java` for slot lookup, WAL location, server info, and
  role-membership discovery; `PostgresReplicationConnection.java` for
  publication existence, publication-table discovery, publication validation,
  optional `pg_replication_slot_advance`, and `CREATE_REPLICATION_SLOT`.
- [ ] Add executable probes for additional named `pgoutput` consumers when a
  real workload introduces them. This future matrix expansion is intentionally
  not part of the current Electric / Zero / Debezium boundary. Tracked by
  dg-7ug.22.
- [x] Document Doltgres as source-only unless live subscriber/apply behavior
  is implemented. docs/electric-compatibility.md now states that Doltgres is a
  logical replication source for Electric, does not run subscription apply
  workers, initial table sync, remote slot creation, or incoming `pgoutput`
  apply, and only supports metadata-only `CREATE SUBSCRIPTION` round trips with
  `connect=false`. Pinned by testing/go/publication_subscription_test.go's
  TestSubscriptionDDLAndCatalogs, which rejects a default publisher connection,
  rejects PostgreSQL-incompatible `connect=false` combinations
  (`create_slot=true`, `enabled=true`, `copy_data=true`), rejects `ENABLE` on
  `slot_name=NONE`, rejects disabled `REFRESH`, rejects enabled `REFRESH`
  because it requires a publisher connection, verifies metadata in
  `pg_subscription` and `pg_stat_subscription_stats`, and verifies no
  subscriber/apply-worker state is exposed through `pg_subscription_rel` or
  `pg_stat_subscription`. The subscription catalog boundary was checked
  against `postgres:15-alpine` on 2026-05-09 before updating the Doltgres
  tests.
- [x] Keep live subscription/apply behavior rejected with executable
  boundaries. Subscription apply workers, initial table synchronization from a
  remote publisher, remote slot creation, and incoming `pgoutput` apply into
  Doltgres are not part of the source-consumer compatibility target; the
  metadata-only `connect=false` boundary remains pinned by
  TestSubscriptionDDLAndCatalogs.
- [x] Cover or reject Aurora / RDS-specific assumptions
  (`rds.logical_replication`, `pglogical`, `track_commit_timestamp`, RDS
  Proxy) that real-world stacks expose. docs/replication-provider-boundaries.md
  records the boundary: `rds.logical_replication` is absent, optional
  `current_setting(..., true)` probes return NULL, `track_commit_timestamp` is
  off/read-only, `pglogical` is explicitly rejected because apply workers and
  subscriber-side synchronization are not implemented, and RDS Proxy remains an
  AWS control-plane/proxy layer outside the Doltgres engine. Pinned by
  testing/go/provider_replication_boundary_test.go's
  TestProviderSpecificReplicationBoundaries.
- [x] Cover the rest of the replication feature surface in
  `postgresql-parity-issues.md` once consumers exercise it. The
  consumer-exercised PostgreSQL 15 replication rows are now closed in
  docs/postgresql-parity-issues.md: the source-mode `pgoutput` subset has
  explicit boundaries for physical slots, non-`pgoutput` plugins, and
  in-progress stream requests; subscriptions are documented and tested as
  metadata-only `connect=false` without apply-worker state; and publication
  DDL covers database-qualified table rejection plus the publication options
  currently claimed through Electric, Zero, and direct `pgoutput` tests.
- [ ] Mirror newly exercised PostgreSQL replication features back into this app
  checklist when a real consumer needs them, rather than leaving them only in
  docs/postgresql-parity-issues.md. This remains the standing future-work rule
  for consumers beyond the current Electric / Zero / Debezium matrix. Tracked
  by dg-7ug.23.

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
  varying`) plus PostgreSQL's `ARRAY` / `_element` split for array
  columns, and `column_default` surfaces both literal and expression
  defaults (e.g. `CURRENT_TIMESTAMP`). Coverage in
  testing/go/info_schema_column_order_test.go pins the workload shapes;
  testing/go/prisma_db_pull_test.go verifies Prisma uses that array
  metadata to recover `String[]`, and
  testing/go/alembic_autogenerate_test.go verifies SQLAlchemy/Alembic
  can inspect the same column metadata without proposing spurious
  migrations.
- [x] `pg_matviews` - the catalog view exists for no-matview databases and
  reports materialized-view rows once table-backed matviews are created:
  schemaname, matviewname, definition, populated state, and index presence all
  round-trip for the currently supported materialized-view surface. Pinned by
  testing/go/pg_matviews_probe_test.go and testing/go/materialized_view_probe_test.go.
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
  a foreign key. Prisma `db pull --print` also runs end-to-end against
  a live Doltgres instance and recovers primary keys, a composite primary key,
  unique constraints, non-unique indexes, JSONB, text arrays, decimals, and
  relations. Alembic `revision --autogenerate` runs through the real
  Alembic + SQLAlchemy + psycopg path against a live Doltgres instance and
  emits an empty migration for the matching schema slice instead of spurious
  table, column, index, or foreign-key operations. Covered by
  testing/go/drizzle_kit_introspect_test.go and
  testing/go/prisma_db_pull_test.go, and
  testing/go/alembic_autogenerate_test.go.
- [x] Authorization-policy deployment - Zero `.permissions.sql` now loads and
  is interpreted through the real Zero 1.4.0 CLI path. The Docker-backed
  TestZeroDiscoverModeSmoke writes a Zero `schema.ts`/permissions module,
  runs `zero-deploy-permissions --output-file --output-format sql`, applies
  the generated `UPDATE dgzero.permissions ...` SQL to Doltgres, verifies the
  stored JSON contains row/cell rules with `authData` references, then runs a
  direct `zero-deploy-permissions --upstream-db` validation against the
  Zero-created publication and asserts the CLI reports "Permissions unchanged".
  The same slice pinned the PostgreSQL ordering requirement Zero relies on:
  omitted defaults are materialized before `BEFORE INSERT` triggers while
  explicit `NULL` values remain explicit.

## Wire protocol and catalog metadata

These items track the wire-protocol and catalog-correctness surfaces
that GUI editors (TablePlus, DataGrip, DBeaver, pgAdmin) and ORM
introspection tools (Drizzle Kit, Prisma db pull, Alembic
autogenerate) inspect to drive editable result grids, schema diffs,
typed-exception handling, and client-side query timeouts.

- [ ] Run actual GUI binaries, and any remaining migration binaries required by
  workloads, against a live Doltgres instance for wire-protocol and catalog
  metadata surfaces that are currently proven only through Go-level harnesses.
  Drizzle Kit, Prisma db pull, Alembic autogenerate, and TablePlus-bundled
  `pg_dump` 17.0 now have live binary harnesses. Tracked by dg-7ug.10.1 and
  dg-7ug.10.2 under dg-7ug.10.
- [x] Run the TablePlus-bundled PostgreSQL dump binary against live Doltgres.
  testing/go/tableplus_dump_test.go locates
  `/Applications/TablePlus.app/Contents/Resources/dump_pg_17.0`, sets the
  app resource library path, verifies the bundled PostgreSQL 17.0 binary, and
  runs `--schema-only` against a UUID/JSONB/FK/index/view schema.
  This covers one TablePlus-distributed PostgreSQL tooling binary; full GUI
  editor workflows remain tracked by dg-7ug.10.
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
  in_failed_sql_transaction, 40P01 deadlock_detected, 40001
  serialization_failure, and 22008 datetime_field_overflow.
  Implementation landed in server/connection_handler.go's
  `errorResponseCode` across three layers — GMS error-kind matchers,
  MySQL-errno fallback, and message-prefix sniffing for errors that
  share errno 1105, including Dolt's commit-time "Unique Key
  Constraint Violation" shape. Coverage by testing/go/sqlstate_test.go (pgx,
  with cases for each code above) and
  testing/go/sqlalchemy_sqlstate_test.go which installs SQLAlchemy
  + psycopg3 in a venv and asserts each shape surfaces the right
  SQLAlchemyError subclass with the matching underlying SQLSTATE.
- [x] Map the tracked remaining driver-visible SQLSTATEs called out by
  app/client retry paths: 40001 serialization_failure for Dolt commit
  conflicts (without remapping PostgreSQL-style row-lock deadlocks away
  from 40P01) and 22008 datetime_field_overflow for timestamp/date
  overflow functions. Coverage in testing/go/sqlstate_test.go via pgx
  protocol assertions; testing/go/select_for_update_deadlock_test.go
  continues to pin the separate 40P01 deadlock_detected path. Future
  app-specific XX000 fallthroughs should be added as new concrete entries
  rather than treating this as full errcode parity.
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
  tables created with `CREATE TABLE name OF composite_type`). Pinned by
  testing/go/pg_class_reloftype_test.go.
- [~] Typed tables - permanent `CREATE TABLE name OF composite_type` now derives
  the table schema from the referenced composite type, supports ordinary
  INSERT/SELECT, stores the referenced type in durable table metadata, reports
  nonzero `pg_class.reloftype`, and surfaces `information_schema.tables.is_typed`
  plus the user-defined type schema/name. Temporary `CREATE TABLE name OF
  composite_type` now derives the same schema for a session-local table,
  supports ordinary INSERT/SELECT, and treats `IF NOT EXISTS` as a no-op for an
  existing temporary typed table. PostgreSQL's typed-table options now support
  table-level `PRIMARY KEY (...)` plus column-level `WITH OPTIONS NULL`, `WITH
  OPTIONS NOT NULL`, and `WITH OPTIONS PRIMARY KEY`; those options drive
  nullable metadata, primary-key metadata, and insert-time enforcement.
  For permanent typed tables, table-level `UNIQUE (...)` and column-level `WITH
  OPTIONS UNIQUE` now create unique indexes, expose
  `information_schema.table_constraints` / `pg_indexes`, and enforce duplicate
  inserts. Temporary typed tables with the same UNIQUE options now enforce
  duplicate inserts and updates, including duplicates within one multi-row
  INSERT, while preserving PostgreSQL's ordinary UNIQUE behavior that permits
  repeated NULL values. Permanent and temporary typed tables also support
  table-level `UNIQUE NULLS NOT DISTINCT (...)` and column-level `WITH OPTIONS
  UNIQUE NULLS NOT DISTINCT`, treating NULLs as equal for duplicate insert/update
  enforcement; permanent indexes report `pg_index.indnullsnotdistinct`.
  Permanent and temporary typed tables now also support literal and expression
  column defaults declared with `WITH OPTIONS DEFAULT`, including insert-time
  materialization for omitted columns and
  `information_schema.columns` default visibility. Permanent and temporary
  typed tables also support named table-level `CHECK` constraints and
  column-level `WITH OPTIONS ... CHECK` constraints with insert/update
  enforcement; permanent typed-table CHECK constraints are also visible through
  `information_schema.table_constraints`. Permanent typed tables now support
  table-level `FOREIGN KEY (...) REFERENCES ...` constraints and column-level
  `WITH OPTIONS ... REFERENCES ...` constraints, including insert-time
  enforcement and `information_schema.table_constraints` visibility; temporary
  typed-table foreign keys remain rejected by the engine's temporary-table FK
  boundary. Permanent and temporary typed tables now support stored generated
  columns declared with `WITH OPTIONS GENERATED ALWAYS AS (...) STORED`,
  including insert-time materialization for omitted generated columns;
  permanent generated columns also report `ALWAYS` through
  `information_schema.columns`. Table-level typed-table `PRIMARY KEY` and
  `UNIQUE` constraints now accept column index-element options such as
  opclasses, collation, sort direction, and NULL ordering for column-only
  constraints; expression index elements and index parameters such as
  `INCLUDE`, storage parameters, and index tablespaces remain unsupported.
  Remaining typed-table DDL boundaries are explicit rejections for temporary
  foreign keys, `LIKE`, standalone table indexes, `EXCLUDE`, `PARTITION BY`,
  `PARTITION OF`, and the generic unsupported `CREATE TABLE` options that also
  apply outside typed tables (`UNLOGGED`, storage parameters, `ON COMMIT`,
  `USING`, and `TABLESPACE`). Pinned by
  testing/go/pg_class_reloftype_test.go. Remaining option parity is tracked by
  dg-7ug.12.
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
  surface, the node-postgres and postgres.js harnesses cover secondary Node
  pooled-client paths with CRUD, parameters, JSONB, arrays, concurrent reads,
  commit, and rollback, the ts-postgres harness covers a binary-result
  Node client with explicit prepared statements and transactions, the Knex
  harness covers Node query-builder schema DDL, joins, raw predicates, pooled
  reads, and transaction boundaries, the pg-promise harness covers Node
  task/transaction helpers and prepared statements, the
  psycopg harness covers the direct Python driver pool with parameters,
  JSONB/array adaptation, concurrent reads, and transaction boundaries, the
  psycopg2 harness covers the legacy Python driver pool with parameters,
  JSONB/array adaptation, prepared statements, concurrent reads, and
  transaction boundaries, and
  the Ruby `pg` harness covers the native Ruby client with parameters, prepared
  statements, JSONB/text[] values, concurrent reads, and transaction
  boundaries. The libpq harness compiles a C probe for typed parameters,
  prepared statements, JSONB/text[] values, multiple connections, and
  transaction boundaries. The Go `database/sql` + `github.com/lib/pq` harness
  covers the real Go driver wrapper with prepared statements, typed
  parameters, JSONB/text[] values, `pq.Array` adaptation, pooled reads, and
  transaction boundaries. The Java JDBC harness covers the upstream PostgreSQL
  JDBC driver with prepared statements, typed parameters, `createArrayOf`
  text[] adaptation, JSONB values, multiple connections, and transaction
  boundaries. The Rust `sqlx` harness covers async pool usage, parameters,
  UUIDs, and chrono timestamp/date decoding, and the TypeORM harness covers a
  real ORM `DataSource` over `pg` with schema synchronization, repository CRUD,
  JSONB/text[] values, relation joins, and transaction boundaries. The
  Sequelize harness covers another real ORM over `pg` with `sync({ force:
  true })`, model CRUD, associations, JSONB/text[] values, pooled reads, and
  managed transactions.
- [ ] Expand driver/ORM matrix proof beyond pgx, node-postgres,
  postgres.js, ts-postgres, Knex, pg-promise, TypeORM, Sequelize, psycopg,
  psycopg2, Ruby `pg`, libpq, Go `database/sql` with `github.com/lib/pq`, and
  Java JDBC, and Rust `sqlx`. Add runnable smoke gates for the advertised
  client and migration-tool matrix before claiming broad client compatibility.
  Tracked by dg-7ug.10.3 under dg-7ug.10.
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

No open app-compatibility blocker is currently known to require a Dolt storage
primitive. The repo now uses a local
`replace github.com/dolthub/dolt/go => ./third_party/dolt/go`, and the
remaining `CREATE INDEX CONCURRENTLY` writer-concurrency concern was closed
with executable evidence rather than a new storage primitive: writers can
commit while Phase 1 is paused, and Dolt's working-set merge reconciles those
row changes with the committing schema/index build.

- [x] CREATE INDEX CONCURRENTLY phase 4 — non-blocking writes during the index
  backfill. Pinned by
  testing/go/create_index_concurrently_contention_test.go's
  TestCreateIndexConcurrentlyAllowsWritersDuringPhase1, which pauses the local
  Dolt secondary-index build and verifies concurrent insert/update/delete
  writers complete before the build resumes and remain visible through the
  final valid/ready index.
- [x] Remove the exported local Dolt index-build pause setter. The pause point
  remains nil by default and unexported in the local Dolt submodule, while
  testing/go/create_index_concurrently_test_hooks_test.go installs it from the
  SQL test binary through a `_test.go` linkname shim for deterministic
  contention coverage.

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
