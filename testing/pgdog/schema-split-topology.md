# PgDog Schema-Split Customer Migration Topology

This is the target topology for running shared and customer-owned data behind one PgDog endpoint.

The older [migration-topology.md](./migration-topology.md) remains the proven two-endpoint boundary from `dg-jri`: applications use Aurora/PostgreSQL directly for shared tables and PgDog for Doltgres customer shards. This document defines the stricter one-endpoint topology tracked by `dg-u1h`.

## Supported Shape

Use one PgDog logical database with mixed backend shards:

- **Shard 0**: the existing Aurora/PostgreSQL database. It owns `shared.*` and remains the default home for unmigrated `customer.*` rows.
- **Shard 1..N**: Doltgres customer shards. Each shard owns migrated customer rows for `customer.*` tables.
- **PgDog endpoint**: the application connects to PgDog for both shared and customer SQL.

Customer-owned tables live in a dedicated schema such as `customer`. Shared, non-customer tables live in a separate schema such as `shared`.

The application query contract is schema-qualified SQL:

```sql
SELECT * FROM shared.accounts WHERE id = $1;
SELECT * FROM customer.orders WHERE customer_id = $1 AND order_id = $2;
```

Unqualified table names and `search_path`-only routing are not part of the supported production contract until a harness proves a narrower safe subset.

## PgDog Configuration Contract

Use `[[sharded_schemas]]` to route shared schema traffic to shard 0:

```toml
[[sharded_schemas]]
database = "prod"
name = "shared"
shard = 0
```

Do not add an unnamed default `[[sharded_schemas]]` entry while `customer.*` tables are expected to route by `customer_id`. PgDog resolves schema mappings before table/key mappings, so an unnamed default schema mapping would send the `customer` schema to shard 0 and prevent customer cutover mappings from taking effect.

Use `[[sharded_tables]]` for every customer-owned table, including the schema name and shard-key type:

```toml
[[sharded_tables]]
database = "prod"
schema = "customer"
name = "orders"
column = "customer_id"
data_type = "bigint"
```

Use `[[sharded_mappings]]` to map customer IDs. Before migration, unmatched customer IDs fall back to shard 0. During cutover, add or change list mappings for migrated customer IDs:

```toml
[[sharded_mappings]]
database = "prod"
schema = "customer"
table = "orders"
column = "customer_id"
kind = "list"
values = [42]
shard = 1

[[sharded_mappings]]
database = "prod"
schema = "customer"
table = "orders"
column = "customer_id"
kind = "default"
shard = 0
```

The `schema`, `table`, and `column` fields in mappings must match the corresponding `[[sharded_tables]]` entry.

## Migration Flow

For one customer:

1. Create `shared` and `customer` schemas on shard 0.
2. Create matching `customer.*` schema on the target Doltgres shard.
3. Keep the default customer mapping on shard 0 while the customer is unmigrated.
4. Copy that customer's `customer.*` rows from shard 0 to the target Doltgres shard. Use table `COPY ... FROM STDIN` into the Doltgres shard; when a source filter is required, stage only the selected customer's rows in a source-side temporary table and copy that table instead of copying all customers.
5. Apply source-side row changes to the Doltgres shard with row-level `INSERT`, `UPDATE`, and `DELETE` until validation reaches zero divergence.
6. Drain or pause that customer's writes.
7. Add or update list mappings for the migrated customer ID to point to the Doltgres shard.
8. Restart PgDog with the updated configuration, then wait for readiness and schema cache load.
9. Resume customer traffic through PgDog and verify post-cutover writes land only on Doltgres.
10. If rollback safety is required, create a Doltgres publication and durable logical slot before accepting rollback-sensitive writes, then stream the migrated customer's `customer.*` row changes back to shard 0 until rollback validation matches.

PgDog documents configuration hot reload and an admin `RELOAD` command, but this topology treats **PgDog restart** as the baseline supported mapping-cutover primitive. A restart recycles PgDog client and server connections, so operators must drain or pause the selected customer's writes and let clients reconnect before resuming traffic. If `RELOAD` is later proven safe for `[[sharded_mappings]]`, this document should be updated to make reload the preferred low-disruption primitive.

## Guardrails

- Customer DML must include the configured shard key in a PgDog-supported query shape.
- Default customer-ID mapping must route unmigrated customer IDs to shard 0. Do not use default schema routing as a production fallback for mutable application SQL.
- Use driver extended-protocol prepared statements for sharded customer DML. SQL `PREPARE` / `EXECUTE` for sharded DML is not supported by upstream PgDog and can fan out.
- Do not rely on mutable cross-schema joins between `shared.*` on Aurora and `customer.*` on Doltgres until explicitly proven.
- Do not rely on transactions that must atomically span shard 0 and Doltgres shards unless a dedicated test proves the exact shape.
- Keep `TRUNCATE` out of logical migration movement; use row-level DML.
- Keep Doltgres entries primary-only. Replica routing and standby replay lag are not part of this topology.
- Immutable reference data may be copied to all Doltgres shards only as a deliberate read-only pattern, not as the source-of-truth model for shared tables.

## Required Harness Proof

This topology is not considered supported until the schema-split harness proves:

- `shared.*` reads and writes route only to shard 0.
- Unmigrated `customer.*` rows route to shard 0 through the default mapping.
- A selected customer ID can be copied to Doltgres and cut over through list mapping.
- Post-copy source changes for the selected customer converge on Doltgres before cutover.
- Post-cutover writes for the migrated customer route only to Doltgres.
- Post-cutover insert/update/delete changes for the migrated customer reverse-apply back to shard 0 through a durable Doltgres logical slot, including after a Doltgres restart.
- Another customer remains on shard 0 after the migrated customer cuts over.
- PgDog reload or restart semantics for the mapping change are known and documented.
- Unsafe unqualified, cross-schema, and cross-shard shapes are either rejected or documented with tests.

## References

- https://docs.pgdog.dev/configuration/pgdog.toml/sharded_schemas/
- https://docs.pgdog.dev/configuration/pgdog.toml/sharded_tables/
- https://docs.pgdog.dev/features/sharding/manual-routing/
- https://docs.pgdog.dev/features/sharding/cross-shard-queries/
