# PgDog Schema-Split Migration Runbook

This runbook is the executable support boundary for moving one customer from a single Aurora/PostgreSQL database into Doltgres behind one PgDog endpoint.

Use it with:

- [schema-split-topology.md](./schema-split-topology.md) for the design contract.
- [rollback.md](./rollback.md) for the reverse-apply proof boundary.
- `testing/pgdog/run_schema_split_harness.sh` for the end-to-end local proof.
- `go test ./testing/pgdog/schema_config` for large config-shape generation coverage.

## Final Setup

The application connects to one PgDog logical database.

- Shard 0 is the current Aurora/PostgreSQL database.
- Shards 1..N are Doltgres primaries.
- `shared.*` tables live only on shard 0.
- `customer.*` tables exist on shard 0 and on Doltgres customer shards.
- Unmigrated customer IDs route to shard 0 through the default customer mapping.
- Migrated customer IDs route to Doltgres through list mappings.

The application does not need app-side per-customer connection routing. PgDog owns customer placement through `[[sharded_mappings]]`. The application does need to generate SQL that PgDog can route:

- Qualify tables as `shared.table_name` or `customer.table_name`.
- Include `customer_id` in mutable customer SQL.
- Use driver extended-protocol prepared statements, not SQL `PREPARE` / `EXECUTE`, for routed customer SQL.
- Fetch shared and customer data with separate statements; do not join `shared.*` to `customer.*` through PgDog.
- Do not put `shared.*` writes and migrated `customer.*` writes in the same transaction.

## PgDog Config Template

Use `cross_shard_disabled = true` to reject many route-free query shapes. This does not make cross-backend transactions safe; the harness proves PgDog can still accept a transaction that writes `shared.*` on shard 0 and `customer.*` on Doltgres.

```toml
[general]
host = "0.0.0.0"
port = 6432
prepared_statements = "extended"
read_write_split = "include_primary"
load_schema = "on"
cross_shard_disabled = true

[[databases]]
name = "prod"
host = "aurora-writer.internal"
port = 5432
database_name = "prod"
user = "postgres"
password = "..."
role = "primary"
shard = 0

[[databases]]
name = "prod"
host = "customer-42.doltgres.internal"
port = 5432
database_name = "postgres"
user = "postgres"
password = "..."
role = "primary"
shard = 1

[[sharded_schemas]]
database = "prod"
name = "shared"
shard = 0

[[sharded_tables]]
database = "prod"
schema = "customer"
name = "orders"
column = "customer_id"
data_type = "bigint"

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

Do not add an unnamed default `[[sharded_schemas]]` entry. It routes the `customer` schema to shard 0 before customer table mappings apply.

For many customers, generate one list mapping per Doltgres shard with many customer IDs in `values`, not one TOML block per customer. Use `testing/pgdog/schema_config` to validate the generated shape.

## Preflight

Before migrating a customer:

1. Confirm every customer-owned table is in the `customer` schema and has the chosen shard key.
2. Confirm non-customer tables are in `shared` or another schema that remains on shard 0.
3. Confirm application SQL is schema-qualified and includes `customer_id` in routed customer DML.
4. Confirm ORM-generated SQL uses quoted schema-qualified identifiers when quoting is enabled.
5. Create matching `customer.*` tables on the target Doltgres shard.
6. Keep the PgDog default customer mapping pointed at shard 0.
7. Run the local proof commands listed in this runbook after any relevant PgDog/Doltgres change.

## Migration

For customer `42`:

1. Keep PgDog routing customer `42` to shard 0 through the default mapping.
2. Stage only customer `42` rows on shard 0, then copy into Doltgres with table `COPY`.

   ```sql
   CREATE TEMP TABLE dg_customer_orders_copy AS
   SELECT customer_id, order_id, status, amount, note
   FROM customer.orders
   WHERE customer_id = 42
   ORDER BY order_id;

   COPY dg_customer_orders_copy (customer_id, order_id, status, amount, note) TO STDOUT;
   ```

   Pipe that output into:

   ```sql
   COPY customer.orders (customer_id, order_id, status, amount, note) FROM STDIN;
   ```

3. Apply source-side changes that happen after copy to Doltgres with row-level `INSERT`, `UPDATE`, and `DELETE`.
4. Validate source and Doltgres convergence for the selected customer.

   ```sql
   SELECT customer_id, order_id, status, amount, COALESCE(note, '')
   FROM customer.orders
   WHERE customer_id = 42
   ORDER BY order_id;

   SELECT count(*),
          COALESCE(sum(amount), 0),
          COALESCE(sum(length(COALESCE(note, ''))), 0)
   FROM customer.orders
   WHERE customer_id = 42;
   ```

5. If rollback safety is required, create a Doltgres publication and durable logical slot before accepting rollback-sensitive writes.
6. Drain or pause writes for customer `42`.
7. Add or update the PgDog list mapping for customer `42` to the Doltgres shard.
8. Restart PgDog and wait for readiness. Restart is the supported cutover primitive for this runbook.
9. Resume customer `42` traffic.
10. Verify post-cutover writes land only on Doltgres.
11. Verify another customer still routes to shard 0.
12. Verify `shared.*` remains source-only.

## Rollback

Rollback safety is proven with `testing/pgdog/reverse_apply`, a test helper. Production must use PgDog's production reverse replication task or another hardened applier with retries, monitoring, and alerting.

Rollback-ready mode:

1. Create a publication on the Doltgres shard for migrated `customer.*` tables.
2. Create a durable logical replication slot on Doltgres before post-cutover writes.
3. Stream `pgoutput` changes from Doltgres.
4. Apply inserts, updates, and deletes back to shard 0 `customer.*`.
5. Acknowledge LSNs only after the shard 0 target commit succeeds.
6. After Doltgres restart, reconnect to the same slot and resume.

To roll traffic back:

1. Pause the customer.
2. Wait for reverse apply to reach zero divergence.
3. Change the PgDog mapping for that customer back to shard 0 or remove its list mapping.
4. Restart PgDog.
5. Resume traffic and validate PgDog reads match shard 0 rows.

## Verification Commands

Run the direct Doltgres compatibility boundary:

```bash
go test ./testing/go -run 'TestPgDogCompatibilityBoundary' -count=1
```

Run the one-endpoint schema-split harness:

```bash
testing/pgdog/run_schema_split_harness.sh
```

Run the older two-endpoint customer migration harness after changes to shared PgDog helpers:

```bash
testing/pgdog/run_customer_migration_harness.sh
```

Run scale/config generation tests:

```bash
go test ./testing/pgdog/schema_config
PGDOG_SCHEMA_CONFIG_STRESS=1 go test ./testing/pgdog/schema_config -run TestGenerateStressShape
```

## Support Matrix

| Area | Status | Proof |
| --- | --- | --- |
| `shared.*` reads/writes through PgDog | Supported, shard 0 only | `run_schema_split_harness.sh`, `dg-u1h.2`, `dg-u1h.3` |
| Unmigrated `customer.*` rows | Supported, default mapping to shard 0 | `run_schema_split_harness.sh`, `dg-u1h.3` |
| Migrated `customer.*` rows | Supported, list mapping to Doltgres | `run_schema_split_harness.sh`, `dg-u1h.3` |
| COPY movement | Supported for selected customer rows into Doltgres with table `COPY FROM STDIN` | `dg-u1h.4` |
| Post-copy change apply | Supported with row-level DML until convergence | `dg-u1h.4` |
| PgDog mapping cutover | Supported by PgDog restart | `dg-u1h.9` |
| PgDog config reload for mappings | Not the supported baseline | Open follow-up only if low-disruption reload is needed |
| Reverse apply rollback proof | Proven with test helper; production needs hardened applier | `dg-u1h.5`, `rollback.md` |
| Driver extended-protocol prepared customer DML | Supported for schema-qualified SQL with `customer_id` parameters | `dg-u1h.8`, `protocol_probe` |
| SQL `PREPARE` / `EXECUTE` for sharded customer SQL | Unsupported | `dg-u1h.8`, `run_schema_split_harness.sh` |
| Unqualified customer SQL and `search_path` routing | Unsupported | `dg-u1h.6`, `run_schema_split_harness.sh` |
| Shared/customer joins | Unsupported; separate reads are supported | `dg-u1h.7`, `run_schema_split_harness.sh` |
| Cross-backend mutable transactions | Unsupported; harness proves PgDog can accept them without a supported atomicity guarantee | `dg-u1h.7`, `run_schema_split_harness.sh` |
| One Doltgres database per customer | Config shape covered, runtime fleet limits not measured by default | `dg-u1h.10`, `schema_config` |
| Batching customers per Doltgres shard | Preferred config shape until fleet limits are measured | `dg-u1h.10`, `schema_config` |

