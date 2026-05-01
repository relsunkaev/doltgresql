# PgDog Migration Apply Path

This decision defines the supported Aurora/PostgreSQL to Doltgres apply path for PgDog-driven per-customer migration.

## Decision

PgDog owns the production apply path for this migration.

The supported path is:

1. The main Aurora/PostgreSQL database remains the shared-data endpoint.
2. PgDog fronts the Doltgres customer shard set.
3. PgDog performs customer-table data movement with its resharding/COPY and row-change apply pipeline.
4. Doltgres acts as a PostgreSQL-compatible destination shard for PgDog SQL, COPY, 2PC, schema loading, and replication-health probes.
5. Doltgres logical replication source mode is used for Doltgres-originated streams, rollback validation, and health/status coverage, not as the Aurora-to-Doltgres production apply worker.

## Unsupported Path

The existing `server/logrepl.LogicalReplicator` consumer is not on the critical path for this PgDog migration. Do not use it as the production Aurora-to-Doltgres apply engine for customer cutover.

That consumer still needs a deliberate hardening project before it can be considered for production migration apply:

- reconnect and resume behavior after network or publisher failure
- durable local progress that cannot acknowledge WAL ahead of persisted target state
- safe SQL generation with identifier quoting and parameters instead of string assembly
- primary-key-change handling
- unkeyed-table handling or explicit rejection
- table and schema mapping
- publication column lists and row filters
- production type-matrix coverage
- local tests that do not require only external GitHub Actions infrastructure

If the migration scope changes to require Doltgres-native Aurora apply, open a new implementation lane for those items instead of extending the PgDog migration beads implicitly.

## Operational Boundary

For the PgDog migration lane, verify apply readiness through the PgDog smoke and end-to-end migration harness:

- customer rows are copied into Doltgres shards through PgDog
- post-copy customer DML routes through PgDog by shard key
- logical replication source-mode catalogs expose slot and sender health for Doltgres-originated streams
- the main shared-data endpoint remains outside the PgDog shard cluster

