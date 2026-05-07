# PostgreSQL compatibility TODO

Last updated: 2026-05-06

This is the Doltgres compatibility TODO for PostgreSQL parity. It tracks known
gaps by the PostgreSQL major version that introduced or owns the behavior.
PostgreSQL 15 is the base because Doltgres currently reports
`server_version_num = 150017` and presents itself as PostgreSQL 15.17 through
`server/config/parameters_list.go`.

This file is not a compatibility claim. An item is open until the feature is
implemented or explicitly scoped out with executable rejection tests and the
required confidence gate below.

Primary upstream references:

- PostgreSQL 15 release notes: https://www.postgresql.org/docs/15/release-15.html
- PostgreSQL 16 release notes: https://www.postgresql.org/docs/16/release-16.html
- PostgreSQL 17 release notes: https://www.postgresql.org/docs/17/release-17.html
- PostgreSQL 18 release notes: https://www.postgresql.org/docs/18/release-18.html

## Done rules

Use these gates before checking off an item:

| Gate | Required evidence |
| --- | --- |
| L1 focused behavior | A focused Doltgres test fails before the fix and passes after it, including negative tests for unsupported shapes. |
| L2 PostgreSQL-paired parity | The same SQL runs against PostgreSQL and Doltgres, comparing rows, errors, command tags, notices where relevant, and catalog side effects. |
| L3 client compatibility | Real clients/ORMs/drivers exercise the behavior through pgwire, prepared statements, binary/text formats, transactions, and catalog introspection. |
| L4 workload compatibility | Realistic applications, dumps, and replication consumers survive the behavior through import/dump corpus runs, regression slices, PgDog/Electric or other harnesses, and at least one non-trivial app/ORM migration path where applicable. |
| L5 release guardrail | The relevant L1-L4 tests are automated in CI or a mandatory release gate, with support boundaries preserved in docs. |

Default gates:

- SQL semantics, DDL, DML, type behavior, expressions, privileges,
  transactions, COPY, catalogs, and planner correctness need L2.
- ORM, driver, `psql`, migration, and admin-tool surfaces need L3.
- Dump/restore, extensions, replication, prepared statements, wire protocol,
  broad schema features, and broad planner/index claims need L4.
- Advertised support should have an L5 guardrail.

## Global compatibility TODO

- [ ] Add PostgreSQL-paired SQL semantic tests for every closed SQL behavior row.
- [ ] Expand PostgreSQL regression-suite coverage and map every skipped file to a TODO or non-goal.
- [ ] Complete wire protocol gates for simple query, extended query, prepared statements, portals, text and binary formats, COPY, TLS, cancel, errors, notices, and command tags.
- [ ] Run client-library and ORM gates for the advertised matrix: C, Java, Node, Python, Ruby, Rust, PHP, Perl, Drizzle, psycopg2, SQLAlchemy, pgx-style clients, and migration tools as needed.
- [ ] Complete catalog and information-schema coverage for `psql`, ORMs, migration tools, BI tools, and replication consumers.
- [ ] Triage import/dump corpus failures into implemented, unsupported, or explicit non-goal buckets.
- [ ] Add representative migration-framework paths for Drizzle, SQLAlchemy/Alembic, Prisma, Rails, Django, Knex, TypeORM, Hibernate, and Liquibase as support claims require.
- [ ] Cover text and binary type encodings for drivers that request binary mode.
- [ ] Add paired correctness tests and benchmark guardrails for performance-sensitive planner and index claims.
- [ ] Complete role, ownership, privilege, ACL, and admin-tool introspection gates.
- [ ] Complete transaction/concurrency gates for isolation, retries, savepoints, locks, prepared transactions, temp objects, and error states.
- [ ] Validate replication and CDC claims through named integration harnesses, not catalog metadata alone.
- [ ] Add extension-specific L4 gates for each advertised extension.
- [ ] Build a real application/dump corpus before making any arbitrary-client compatibility claim.

## PostgreSQL 15 base TODO

These are baseline PostgreSQL 15 or long-standing PostgreSQL behaviors that
clients can hit even when they do not use newer PostgreSQL versions.

### SQL objects, routines, and security

- [ ] SQL `MERGE` - implement PostgreSQL 15 SQL-standard `MERGE` or add explicit rejection coverage; do not confuse this with Dolt version-control merge functions.
- [ ] Views - close search-path, nested, temporary, recursive, security-barrier, security-invoker, updatable-view, and rule-backed view gaps.
- [ ] Materialized views - implement or explicitly reject `CREATE MATERIALIZED VIEW`, `ALTER MATERIALIZED VIEW`, `REFRESH MATERIALIZED VIEW`, drop/rename semantics, and `pg_matviews`.
- [ ] Triggers - close constraint trigger, `FROM`, `DEFERRABLE`, transition table, statement trigger, `INSTEAD OF`, `UPDATE OF`, `TRUNCATE`, `ALTER TRIGGER`, execution edge-case, and `pg_trigger` gaps.
- [ ] Rewrite rules and event triggers - implement executable rule/event-trigger behavior or preserve explicit rejection plus complete catalog boundaries.
- [ ] PL/pgSQL - close alias opcode, `CASE` without `ELSE`, branch-qualified function creation, non-trigger composite/table types, `table.*`, anonymous `DO`, and dump-derived function gaps.
- [ ] SQL functions and procedures - close routine DDL, routine options, `ALTER/DROP ROUTINE`, multi-function drop, cascade behavior, `CALL` limitations, SQL-standard routine bodies, `pg_proc`, `pg_aggregate`, and helper function introspection.
- [ ] Procedural languages - implement or explicitly reject `CREATE/ALTER/DROP LANGUAGE` and populate `pg_language` consistently with routine support boundaries.
- [ ] Built-in functions and aggregate syntax - close `date_trunc`, `extract`, `to_char`, time-zone/TM formatting, transaction-local `set_config`, catalog helper variants, `pg_get_triggerdef`, `pg_get_userbyid`, function filters, `WITHIN GROUP`, and function-level `ORDER BY` gaps.
- [ ] Object ownership - implement owner semantics for object kinds, `REASSIGN OWNED`, `DROP OWNED`, and owner privilege behavior.
- [ ] Roles and privileges - close `ALTER DEFAULT PRIVILEGES`, broader `GRANT`/`REVOKE`, cross-database grants, role membership/admin variants, legacy role aliases, owner default privileges, sequence owner privileges, CTE visibility, routine/sequence privileges, exact errors, `SET ROLE`, and role/auth/ACL catalogs.
- [ ] PostgreSQL 15 public-schema security defaults - implement `pg_database_owner` and PostgreSQL 15 default public-schema ACL semantics or document the non-goal.
- [ ] Row-level security and security labels - implement RLS policy DDL/enforcement, RLS table modes, `pg_policy`/`pg_policies`, security labels, and security-label catalogs or reject them explicitly.
- [ ] Schemas and namespace behavior - close schema-qualified DDL, `search_path`, ALTER TABLE/FK schema handling, `dolt_` namespace reservation, `CREATE SCHEMA AUTHORIZATION`, and inline schema element execution.
- [ ] Database DDL - close `CREATE DATABASE` options, template/strategy/locale/tablespace/connection/OID behavior, `ALTER DATABASE`, `RENAME DATABASE`, and `DROP DATABASE ... WITH (FORCE)`.
- [ ] Collations, locales, and encodings - close database locale/collation options, `CREATE/DROP/ALTER COLLATION`, conversions, collation-aware string functions, timezone catalogs, and encoding/conversion catalogs.

### Tables, types, expressions, and queries

- [ ] CREATE TABLE and table constraints - close storage parameters, `ON COMMIT`, unlogged tables, access methods, tablespaces, `CREATE TABLE AS ... WITH NO DATA`, `PARTITION OF`, `LIKE` options, constraint naming/options, inline sequence options, multi-column CHECK, `NO INHERIT`, exclusion constraints, constraint storage/tablespace options, and constraint drop cascades.
- [ ] `SELECT INTO` table creation - close PostgreSQL-compatible table creation semantics for `SELECT INTO`.
- [ ] Typed tables - implement typed-table behavior or preserve explicit unsupported coverage.
- [ ] ALTER TABLE - close `NOT VALID`, `VALIDATE CONSTRAINT`, deferrable constraint alteration, column-level `IF [NOT] EXISTS`, drop behavior, `COLLATE`, `USING`, clustering flags, tablespace moves, trigger/rule enable-disable commands, partition operations, schema-qualified cases, generated-column changes, and cast/equality edge cases.
- [ ] TOAST, column storage, and compression - implement or explicitly scope out PostgreSQL storage/compression DDL and catalog behavior.
- [ ] Partitioning and inheritance - close partition DDL, partition catalogs, inheritance catalogs, query-planning behavior, and regression slices.
- [ ] Sequences and identity - close sequence options, identity-column behavior, serial/bigserial edge cases, sequence ownership, sequence catalogs, and privilege behavior.
- [ ] Foreign keys - close FK edge cases, schema-qualified references, deferrability, validation, cascade/drop behavior, and catalog introspection.
- [ ] DROP/ALTER object variants - close unsupported object-kind variants, cascade/restrict behavior, and parser-only object DDL.
- [ ] Parser-only statement classes - sweep parser statement nodes for missing converter/runtime paths and add rejection tests for non-goals.
- [ ] Comments and object descriptions - implement comment DDL and `pg_description`/object-description helpers or explicitly reject.
- [ ] Type system - close `pg_type`, regtype, pseudo-type, typmod, array, domain, enum, composite, and cast introspection gaps.
- [ ] Range and multirange types - implement range/multirange types, operators, functions, casts, indexes, and catalogs or reject as non-goals.
- [ ] XML type and XML functions - implement XML type/function behavior or keep explicit unsupported coverage.
- [ ] Network address types - close `inet`, `cidr`, `macaddr`, and related operator/function/index behavior.
- [ ] Geometric types - close point/line/box/path/polygon/circle behavior or reject with tests.
- [ ] Bit strings and money - close bit-string and money type semantics, casts, formatting, and operators.
- [ ] Scalar type index and ordering edge cases - close type-specific ordering, comparison, collation, opclass, and index behavior.
- [ ] Enum/domain/composite types - close enum, domain, composite, constraint, cast, dependency, and catalog behavior.
- [ ] Date/time, interval, and special numeric behavior - close date/time, interval, infinity, NaN, precision, timezone, and numeric edge cases.
- [ ] Arrays and records - close array/record construction, comparison, casts, functions, SRF, and binary/text encoding behavior.
- [ ] Expressions and operators - close operator precedence, row expressions, boolean/null behavior, casts, polymorphism, and custom operator boundaries.
- [ ] Pattern matching and regular expressions - close `LIKE`/`ILIKE`, regex, `SIMILAR TO`, collations, SRF placement, and error behavior.
- [ ] Casts, conversions, and polymorphism - close cast lookup, implicit/assignment casts, polymorphic function resolution, and conversion behavior.
- [ ] Query clauses, CTEs, locking, table functions, and TABLESAMPLE - close `WITH`, recursive CTE, locking clauses, table functions, SRF context, lateral behavior, and TABLESAMPLE gaps.
- [ ] Subqueries, set operations, and plan-cache semantics - close correlated subqueries, `EXISTS`, `ANY`/`ALL`, set operations, plan cache invalidation, and prepared-plan behavior.
- [ ] Grouping sets, rollup/cube, and advanced aggregate planning - close grouping sets, rollup, cube, aggregate filters/order, and advanced aggregate planning behavior.
- [ ] JSON and JSONB - close remaining JSON/JSONB functions, operators, casts, path behavior, SRFs, aggregates, containment, comparison, error, and binary/text behavior.

### Indexes, DML, planner, and maintenance

- [ ] Index DDL options and metadata - close index options, access methods, storage parameters, expressions, predicates, INCLUDE, collations, opclasses, tablespaces, comments, dependencies, and catalog metadata.
- [ ] GIN indexes - close the supported JSONB GIN subset to PostgreSQL-paired parity and preserve explicit unsupported boundaries for non-covered GIN behavior.
- [ ] Hash, GiST, SP-GiST, and BRIN indexes - implement these access methods or keep exact rejection tests and catalog boundaries.
- [ ] Btree planner and metadata - close mixed predicate planning, partial index behavior, INCLUDE behavior, null ordering, collation, opclass, and opfamily semantics.
- [ ] Concurrent index lifecycle, REINDEX, and `ALTER INDEX` - implement or explicitly reject concurrent create/drop, reindex variants, and alter-index behavior.
- [ ] DML table references, aliases, and mutation variants - close aliased inserts, insert table refs, overriding values, `ON CONFLICT` variants, targetless upserts, inheritance-targeting forms, `UPDATE ... FROM`, `DELETE ... USING`, cursor-positioned mutations, row assignment, and affected-row parity.
- [ ] TRUNCATE - close multi-table, cascade/restrict, and truncate-trigger behavior.
- [ ] EXPLAIN - close EXPLAIN output, options, plan node reporting, and version-specific options as a compatibility surface.
- [ ] Planner and executor nodes - close unsupported planner/executor node behavior and paired correctness/performance gates.
- [ ] VACUUM - implement or explicitly reject PostgreSQL VACUUM behavior and options.
- [ ] ANALYZE - close PostgreSQL ANALYZE semantics beyond Dolt statistics mapping.
- [ ] Storage maintenance and clustering - close CLUSTER, storage maintenance, reloptions, and table/index physical organization behavior.
- [ ] Extended statistics and aggregates - close extended statistics, statistic object DDL/catalogs, and planner use.
- [ ] Statistics and monitoring catalogs - close runtime stats views, progress views, and monitoring catalog shape/values.
- [ ] Administrative/runtime functions - close relation size, transaction ID, advisory lock, backend termination, postmaster lifecycle, snapshot, WAL/LSN, and recovery helper semantics.
- [ ] Heap system columns, TID, and MVCC metadata - close `ctid`, `xmin`, `xmax`, TID, and MVCC metadata behavior or document non-goals.

### Protocol, sessions, catalogs, extensions, and replication

- [ ] COPY - close query-form COPY, table COPY options, text/CSV/binary edge cases, server-side paths, privilege checks, dump/import behavior, and version-specific COPY options.
- [ ] Extended query protocol, SQL cursors, portals, and psql meta commands - close portal lifecycle, cursor commands, cancel keys, row-max suspension, named statement/portal cleanup, command tags, row descriptions, psql probes, and `pg_cursors`.
- [ ] Sessions and temporary objects - close temp schemas/objects, session cleanup, ON COMMIT behavior, and session-local state.
- [ ] LISTEN/NOTIFY asynchronous messaging - close notification delivery, transaction timing, payload/channel behavior, and client-visible semantics.
- [ ] `LOCK TABLE` and relation lock visibility - close lock modes, conflicts, wait behavior, and lock catalog visibility.
- [ ] Transactions, constraint modes, and session authorization - close isolation/error states, constraint modes, prepared transactions, retries, and session authorization semantics.
- [ ] Savepoints and transaction commands - close savepoint status and broader savepoint/transaction command parity.
- [ ] Configuration parameters - close `SET`, `SHOW`, `RESET`, transaction-local settings, startup parameters, custom GUCs, and catalog/config visibility.
- [ ] Server utility commands - close PostgreSQL utility statements or add explicit rejection coverage.
- [ ] Information schema and pg_catalog - close view information, column metadata, type/regtype metadata, function/trigger/user introspection, runtime views, dependency catalogs, descriptions, roles/ACL catalogs, FDW catalogs, full-text catalogs, stats/progress catalogs, large-object catalogs, and TODO row iterators.
- [ ] Object identity and dependency helper functions - close object-address, identity, dependency, description helpers, dependency catalogs, and description catalogs.
- [ ] Extensions - close `CREATE EXTENSION` options, `ALTER EXTENSION`, `DROP EXTENSION`, extension result checks, cross-platform extension loading, and extension availability catalogs.
- [ ] Custom operators, access methods, and transforms - implement or explicitly reject custom operator/operator class/operator family/access method/transform DDL and helper functions.
- [ ] Full text search - close `tsquery`, `tsvector`, dictionaries, configurations, parsers, templates, ranking/headline helpers, and full-text GIN behavior.
- [ ] Foreign data wrappers - implement or explicitly reject FDW/server/foreign table/user mapping/import foreign schema behavior and catalogs.
- [ ] Large objects - close large-object DDL, descriptors, read/write/seek/truncate, import/export, privilege, comment, ownership, helper, and catalog behavior.
- [ ] Tablespaces - close tablespace DDL, placement, owner/rename, reloptions, reindex moves, partition interactions, and `pg_tablespace`.
- [ ] Logical replication source - keep the supported `pgoutput` source-mode subset honest and add explicit boundaries for physical slots, non-`pgoutput` plugins, and in-progress stream messages.
- [ ] Subscriptions and apply workers - implement live subscriber behavior or preserve explicit metadata-only `connect=false` support with tests for missing publisher connections, remote slot creation, initial sync, apply workers, and `pg_subscription_rel`.
- [ ] Publications - close table database qualifiers and every publication option claimed through Electric or other consumers.
- [ ] Import/dump compatibility - unskip and triage the bulk dump-import suite and real-world dumps.

## PostgreSQL 16 TODO

- [ ] Version identity - decide whether PostgreSQL 16 behavior is globally out of scope while Doltgres reports PostgreSQL 15.17, or implement/version-gate each PostgreSQL 16 surface.
- [ ] SQL/JSON constructors - implement `JSON_ARRAY`, `JSON_ARRAYAGG`, `JSON_OBJECT`, and `JSON_OBJECTAGG`.
- [ ] Enhanced SQL/JSON path numeric literals - close PostgreSQL 16 JSONPath numeric literal syntax.
- [ ] Ordinary SQL numeric literal syntax - implement non-decimal integer literals and underscore separators.
- [ ] Logical decoding on standbys - implement recovery/standby mode support or explicitly reject standby decoding.
- [ ] Parallel logical replication apply - implement or explicitly reject PostgreSQL 16 parallel apply behavior.
- [ ] Binary initial table synchronization - implement binary initial sync for subscriptions or reject it with tests.
- [ ] `REPLICA IDENTITY FULL` apply optimization - implement apply-side btree lookup optimization or document source-only support.
- [ ] `pg_stat_io` - expose PostgreSQL 16 `pg_stat_io` parity or explicitly reject.
- [ ] COPY `DEFAULT` mapping - implement PostgreSQL 16 COPY default mapping behavior.
- [ ] Subscription privileges and password bypass - implement `pg_create_subscription` and `password_required=false` semantics or reject them.
- [ ] Catalog/stat shape changes - close PostgreSQL 16 additions such as `pg_prepared_statements.result_types`, copy progress reporting, and scan timestamp fields.
- [ ] New server variables/functions - implement or reject `SYSTEM_USER`, `pg_input_is_valid()`, `pg_input_error_info()`, `date_add()`, `date_subtract()`, `array_sample()`, `array_shuffle()`, `ANY_VALUE()`, and `random_normal()`.
- [ ] `VACUUM`/`ANALYZE BUFFER_USAGE_LIMIT` - implement or reject buffer-usage controls.
- [ ] Role inheritance and CREATEROLE changes - implement PostgreSQL 16 role inheritance and CREATEROLE behavior.
- [ ] Generated columns on inherited/partitioned tables - close PostgreSQL 16 restrictions and behavior.

## PostgreSQL 17 TODO

- [ ] Version identity - decide whether PostgreSQL 17 behavior is globally out of scope while Doltgres reports PostgreSQL 15.17, or implement/version-gate each PostgreSQL 17 surface.
- [ ] SQL/JSON additions beyond `JSON_TABLE` - implement `JSON`, `JSON_SCALAR`, `JSON_SERIALIZE`, `JSON_EXISTS`, `JSON_QUERY`, `JSON_VALUE`, and expanded jsonpath behavior.
- [ ] `MERGE RETURNING` and MERGE-on-views - close these after base SQL `MERGE` support exists.
- [ ] COPY error handling, logging, and force-null additions - implement `COPY ... ON_ERROR`, skipped-row reporting, `LOG_VERBOSITY`, and broader force-null/not-null controls.
- [ ] Partitioned-table identity columns and exclusion constraints - close identity/exclusion behavior on partitioned tables.
- [ ] Built-in immutable collation provider - implement provider behavior or explicitly reject.
- [ ] Logical slot failover and upgrade survival - implement failover/upgrade semantics for logical slots or document non-goal status.
- [ ] `pg_createsubscriber` workflows - implement physical-replica-to-logical-replica tooling semantics or reject.
- [ ] TLS direct negotiation and ALPN - implement PostgreSQL 17 direct TLS negotiation behavior.
- [ ] `pg_maintain` predefined role - implement the role and privileges or reject.
- [ ] Incremental backup tooling and pg_dump filters - implement or document non-goal status for `pg_basebackup` incremental backup, `pg_combinebackup`, and `pg_dump --filter`.
- [ ] New monitoring columns/views - close PostgreSQL 17 monitoring and EXPLAIN additions.
- [ ] Planner/index performance changes - validate btree `IN` scan and BRIN-related behavior against Doltgres index support boundaries.
- [ ] New non-JSON built-ins - implement or reject `uuid_extract_timestamp()`, `uuid_extract_version()`, `to_bin()`, `to_oct()`, Unicode information functions, `xmltext()`, `to_regtypemod()`, and `pg_basetype()`.

## PostgreSQL 18 TODO

- [ ] Version identity - decide whether PostgreSQL 18 behavior is globally out of scope while Doltgres reports PostgreSQL 15.17, or implement/version-gate each PostgreSQL 18 surface.
- [ ] Async I/O subsystem and `pg_aios` - implement or explicitly reject PostgreSQL storage-engine I/O parity and `pg_aios`.
- [ ] `pg_upgrade` retaining optimizer statistics - implement or reject statistics-retention semantics.
- [ ] Btree skip scan - implement or explicitly reject PostgreSQL 18 multicolumn btree skip scan behavior.
- [ ] `uuidv7()` - implement and test the PostgreSQL 18 UUID generator.
- [ ] New scalar, array, and JSON built-ins - implement or reject `array_sort()`, `array_reverse()`, bytea `reverse()`, integer-bytea casts, `casefold()`, `crc32()`/`crc32c()`, `gamma()`/`lgamma()`, JSON null cast behavior, and optional array-null stripping arguments.
- [ ] Virtual generated columns - implement virtual generated columns or document stored-only behavior.
- [ ] DML `OLD`/`NEW` in `RETURNING` - implement PostgreSQL 18 aliases for `INSERT`, `UPDATE`, `DELETE`, and `MERGE`.
- [ ] Temporal constraints - implement `WITHOUT OVERLAPS` and `PERIOD` constraints or reject them.
- [ ] Constraint inheritance and partition validation changes - implement `ALTER CONSTRAINT ... [NO] INHERIT`, `NOT VALID` FKs on partitioned tables, and dropping constraints `ONLY`.
- [ ] OAuth authentication - implement PostgreSQL 18 OAuth authentication or reject it.
- [ ] Wire protocol 3.2 negotiation and status - implement protocol 3.2, 256-bit cancel keys, and status-reporting changes.
- [ ] Generated columns in logical replication - implement end-to-end generated-column publication behavior or reject.
- [ ] Subscription default streaming and two-phase alteration - implement PostgreSQL 18 subscription streaming defaults and two-phase slot alteration behavior or reject.
- [ ] New privileges and ACL helpers - implement `pg_get_acl()`, large-object privilege helpers, large-object default privileges, and `pg_signal_autovacuum_worker`.
- [ ] Monitoring catalog shape changes - close PostgreSQL 18 monitoring catalog column changes.
- [ ] VACUUM/ANALYZE inheritance behavior - implement inheritance child handling and `ONLY` behavior or reject.
- [ ] COPY CSV EOF, reject limits, and logging options - implement PostgreSQL 18 CSV EOF behavior, `REJECT_LIMIT`, and `LOG_VERBOSITY silent`.
- [ ] Non-btree unique indexes as partition keys/materialized view unique indexes - implement or explicitly reject these expanded PostgreSQL 18 index uses.

## Maintenance TODO

- [ ] Put every new issue under the first PostgreSQL major version where the behavior is required.
- [ ] If an older base issue blocks a newer feature, link both tasks.
- [ ] Tie each task to executable tests, explicit unsupported code paths, release notes, or a current repo scan before closing it.
- [ ] Do not count parser/catalog-only support as semantic parity.
- [ ] Re-run a parser/converter sweep for `var _ Statement` declarations without a `case *tree.*` converter before claiming no parser-only SQL gaps remain.
- [ ] Re-run `rg "TODO: Implement .* row iter" server/tables/pgcatalog` before claiming catalog introspection parity.
- [ ] When a feature becomes supported, replace the task with the remaining narrower boundary or move it to a resolved changelog with proving tests.
