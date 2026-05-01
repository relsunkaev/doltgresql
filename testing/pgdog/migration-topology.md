# PgDog Customer Migration Topology

This is the supported topology for moving from one Aurora/PostgreSQL database to per-customer Doltgres shards behind PgDog.

## Supported Shape

Use two application database endpoints:

- **Main endpoint**: the existing Aurora/PostgreSQL database remains the source of truth for shared, non-customer tables.
- **Customer endpoint**: PgDog fronts only customer-scoped tables hosted by Doltgres shards.

Every customer-scoped table routed through PgDog must have a `customer_id` shard key, or another explicitly configured shard key, and must be listed in PgDog `[[sharded_tables]]`. The application must route shared-table SQL to the main endpoint and customer-table SQL to the PgDog endpoint.

## Query Boundary

Shared tables are not configured as PgDog shard tables and are not created on Doltgres customer shards. A shared-table read or write sent to PgDog is expected to fail because the table is absent from the customer shard cluster. The smoke harness creates a `shared_accounts` table on the main endpoint and asserts that the same table cannot be read or written through PgDog.

Customer-scoped reads, writes, COPY movement, and logical-replication source-mode probes use PgDog and Doltgres shards. Customer queries should include the shard key when the operation is intended to hit one customer. Sharded-table queries without a shard-key predicate may be broadcast or merged by PgDog and should be reserved for admin or migration tooling.

## Unsupported Alternatives

- Do not put the main Aurora database in the same PgDog shard set as Doltgres customer shards. PgDog's unsharded-table routing is not a source-of-truth routing model for shared data.
- Do not rely on PgDog fanout, round-robin, or omnisharded behavior for mutable shared tables.
- Do not run transactions that must atomically span the main endpoint and the customer endpoint. Split the workflow at the application layer or add an explicit compensation path.
- Do not depend on cross-endpoint joins. Query shared and customer data separately, or replicate immutable reference data deliberately.
- Do not create shared schemas or shared tables through PgDog.

Immutable reference tables may be replicated to all Doltgres shards only when the application treats them as read-only shard-local copies. That is a separate reference-data pattern, not the source-of-truth shared-data model.

