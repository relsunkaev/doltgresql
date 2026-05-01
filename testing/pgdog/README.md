# PgDog Compatibility Smoke

This directory contains the Doltgres compatibility boundary for PgDog.

PgDog support is currently scoped to a primary-only customer-shard smoke path: clients can connect to PgDog, PgDog can connect to two Doltgres primary shards, DDL can be broadcast to the shards, and ordinary sharded `INSERT` / `SELECT` statements can be routed by a supported shard key type.

The shared-data topology decision is checked in at [migration-topology.md](./migration-topology.md). Shared, non-customer tables stay on the main Aurora/PostgreSQL endpoint and are not accessed through PgDog. The Aurora-to-Doltgres apply-path decision is checked in at [apply-path.md](./apply-path.md): PgDog owns production migration apply for this lane.

Rollback proof and operator limits are checked in at [rollback.md](./rollback.md).

## Run

From the repository root:

```bash
testing/pgdog/run_pgdog_smoke.sh
```

For the customer migration contract:

```bash
testing/pgdog/run_customer_migration_harness.sh
```

The script builds a local `doltgres` binary unless `DOLTGRES_BIN` is set, starts a temporary main database plus two temporary Doltgres customer shards on the host, starts `ghcr.io/pgdogdev/pgdog:latest` in Docker, runs a shard-routing smoke test through PgDog, then checks supported compatibility lanes and explicit unsupported boundaries.

The customer migration harness starts a real `postgres:16-alpine` source container, two Doltgres customer shards, and PgDog. It creates matching customer schema, broadcasts a post-start `ALTER TABLE` through PgDog, copies one customer's rows through PgDog, restarts PgDog before post-copy traffic, applies transaction-scoped insert/update/delete traffic through the PgDog customer route, validates SQL prepared statements and pgx extended-protocol prepared statements, rejects missing-shard-key writes, validates source-to-destination row/checksum equality before cutover, proves post-cutover writes route to Doltgres, reverse-applies insert/update/delete changes back to the source rollback database across a Doltgres restart, and verifies shared tables remain source-only.

On Homebrew-based macOS setups, the script automatically uses `icu4c@78` for the local Go build when `CGO_CPPFLAGS` is not already set.

Useful overrides:

```bash
DOLTGRES_BIN=/path/to/doltgres \
PGDOG_IMAGE=ghcr.io/pgdogdev/pgdog:latest \
PGDOG_LOAD_SCHEMA=on \
PGDOG_PORT=16432 \
DOLTGRES_MAIN_PORT=15434 \
DOLTGRES_SHARD0_PORT=15432 \
DOLTGRES_SHARD1_PORT=15433 \
testing/pgdog/run_pgdog_smoke.sh
```

The customer migration harness also accepts `POSTGRES_IMAGE`, `SOURCE_POSTGRES_PORT`, `DOLTGRES_DATABASE`, and `CUSTOMER_ID`.

For CI, prefer pinning `PGDOG_IMAGE` to a digest rather than using `latest`.

## Supported Boundary

Use this PgDog configuration shape:

```toml
[general]
two_phase_commit = false
two_phase_commit_auto = false
prepared_statements = "extended"
read_write_split = "include_primary"
load_schema = "on"

[[databases]]
name = "pgdog"
host = "host.docker.internal"
port = 15432
database_name = "pgdog"
user = "postgres"
password = "password"
role = "primary"
shard = 0

[[databases]]
name = "pgdog"
host = "host.docker.internal"
port = 15433
database_name = "pgdog"
user = "postgres"
password = "password"
role = "primary"
shard = 1

[[sharded_tables]]
database = "pgdog"
name = "pgdog_items"
column = "tenant_id"
data_type = "bigint"
```

PgDog requires `pgdog.toml` and `users.toml`, uses `[[databases]]` entries for backend primaries, loads schema at startup, and routes configured sharded-table columns such as `bigint`, `varchar` / `text`, `uuid`, and `vector`. This smoke path keeps `bigint` as the distribution check and adds a routed `vector` lookup to cover pgvector-style shard keys. The separate main database in the harness stands in for Aurora shared data; it is deliberately not configured as a PgDog shard.

## Unsupported Paths

Keep these PgDog features disabled or out of scope for Doltgres until the corresponding PostgreSQL surface is implemented:

| PgDog lane | Doltgres status | Required configuration or behavior |
| --- | --- | --- |
| 2PC / prepared transactions | `PREPARE TRANSACTION`, `COMMIT PREPARED`, `ROLLBACK PREPARED`, and `pg_prepared_xacts` are supported. Prepared transaction metadata, transaction start root, and the prepared Dolt working set are recovered after a Doltgres restart. | PgDog 2PC can be smoke-tested across restart. Recovered `COMMIT PREPARED` merges non-conflicting working-set changes against the original transaction start root; conflicting changes surface as commit errors and leave the prepared transaction available for retry or rollback. |
| Resharding and cutover | PgDog owns Aurora-to-Doltgres production apply for this migration lane. Doltgres exposes PostgreSQL replication source mode for `IDENTIFY_SYSTEM`, logical slot create/drop, `START_REPLICATION`, keepalives, standby status updates, local sender/slot catalogs, and pgoutput `Relation` / `Insert` / `Update` / `Delete` / `Commit` messages for published table changes. The Doltgres-native `server/logrepl` consumer is not the supported Aurora apply engine for this flow. | Replication connection, health probes, and post-stream row-change movement can be smoke-tested. Full PgDog cutover orchestration still needs an end-to-end resharding harness before it should be treated as production-supported. |
| Publication and subscription DDL | `CREATE` / `ALTER` / `DROP PUBLICATION`, metadata-only `CREATE` / `ALTER` / `DROP SUBSCRIPTION`, and local publication/subscription catalogs are supported. Source-mode pgoutput honors publication column lists, row filters, `insert` / `update` / `delete` publish flags, `FOR ALL TABLES`, and schema publications. Subscriptions do not start remote apply workers; remote publisher connections are rejected unless `connect=false`. | PgDog setup can create local logical-replication metadata. Use `WITH (connect=false, enabled=false, create_slot=false, slot_name=NONE)` for subscriptions. `TRUNCATE` publication output is not implemented; use row-level DML during PgDog movement. |
| Replication slots and replication stats | `pg_replication_slots`, `pg_stat_replication`, and `pg_stat_replication_slots` reflect durable logical replication source slots, active senders, confirmed/restart LSNs, and per-slot decoded transaction/byte totals. Slot state and acknowledged LSNs survive Doltgres restart. | PgDog replication-health probes can use slot/sender catalogs and LSN lag to verify a stream reaches zero before cutover. |
| COPY movement | Text, CSV, and binary `COPY FROM` plus table `COPY TO STDOUT` are supported. The PgDog smoke routes `COPY FROM STDIN` and post-copy DML through PgDog for a customer-shaped table covering `bigint` shard keys, `uuid`, `boolean`, `numeric`, `json` / `jsonb`, `bytea`, `text[]`, `timestamp`, `timestamptz`, `vector`, defaults, generated columns, nulls, and multi-row batches. Query-form `COPY (SELECT ...) TO STDOUT` is not implemented yet. | Use table-based COPY movement only. Text `COPY FROM STDIN` is the broadest verified format for PgDog resharding; CSV and binary COPY remain covered by direct Doltgres protocol tests. |
| Vector shard keys | Doltgres provides a pgvector-compatible `vector` scalar with text/binary IO and equality for PgDog shard-key routing. Distance operators and ANN indexes are not implemented. | Use `vector` for equality-routed shard keys only. |
| Replica routing | `pg_is_in_recovery()` reports primary mode, `pg_current_wal_lsn()` reflects the local logical-replication source LSN after row-producing writes, and replay/receive LSNs are `NULL` because Doltgres still has no standby replay stream. | Configure only primary Doltgres entries. Use primary-side LSN and sender stats for health probes, not PgDog replica routing or standby lag checks. |

SQL-level `PREPARE`, `EXECUTE`, `DEALLOCATE`, and `pg_prepared_statements` are supported for non-sharded smoke coverage. Do not use SQL `PREPARE` / `EXECUTE` for sharded PgDog DML: upstream PgDog documents that `EXECUTE` for prepared statements requiring sharding is not supported and is sent to all shards. Use driver/client extended-protocol prepared statements for sharded customer DML.

PgDog schema loading is supported for the startup schema-cache queries used by the open-source PgDog image, including column, relation, and foreign-key introspection. The harness still configures sharded tables explicitly so shard-key type coverage remains deterministic. The customer migration harness also covers post-start customer-table DDL broadcast and subsequent routed DML through the refreshed PgDog schema state.

The customer migration harness runs `testing/pgdog/protocol_probe` through PgDog to exercise pgx extended-protocol `Parse` / `Bind` / `Execute` prepared statements and transaction boundaries in the same topology.

Relevant PgDog docs:

- https://docs.pgdog.dev/configuration/
- https://docs.pgdog.dev/configuration/pgdog.toml/databases/
- https://docs.pgdog.dev/configuration/pgdog.toml/sharded_tables/
- https://docs.pgdog.dev/features/sharding/2pc/
- https://docs.pgdog.dev/features/prepared-statements/
- https://docs.pgdog.dev/configuration/pgdog.toml/general/
