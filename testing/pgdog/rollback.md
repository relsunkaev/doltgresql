# PgDog Cutover Rollback

This document records the rollback boundary proven by the local PgDog customer migration harness and the one-endpoint schema-split harness.

## Proven Flow

After customer traffic is routed to Doltgres through PgDog:

1. Create a publication on the Doltgres customer shard for the migrated customer table.
2. Create a durable logical replication slot on that Doltgres shard before accepting post-cutover writes that must remain rollback-safe.
3. Stream `pgoutput` from Doltgres and apply the row changes back to the original Postgres/Aurora-side rollback database.
4. Acknowledge the applied commit LSN only after the rollback target commit succeeds.
5. If the Doltgres shard restarts, reconnect to the same slot and continue streaming from the acknowledged position.
6. Validate the customer row set on the rollback target against the PgDog/Doltgres row set before declaring rollback readiness.

The harnesses implement this with `testing/pgdog/reverse_apply`, a test-only pgoutput applier for the customer orders table. The helper defaults to `public.customer_orders` for the two-endpoint harness and accepts `-schema customer -table orders` for the schema-split harness. It proves insert, update, and delete changes made through PgDog after cutover are applied back to the source database. It also restarts the Doltgres source shard between reverse-apply batches to verify that the durable slot survives restart and no reverse changes are lost.

## Operator Limits

- This is a rollback-safety proof for the PgDog migration lane, not a generic production replication worker.
- Reverse replication must be established before rollback-sensitive post-cutover writes.
- The local helper covers the representative customer table used by the harness. A production operator must use PgDog's reverse replication task or another hardened applier with schema mapping, retries, monitoring, and alerting.
- Only customer-owned tables belong in the reverse stream. Shared tables remain on the main endpoint and are not replicated from Doltgres.
- Validation must compare both row contents and aggregate checksums for the migrated customer before rollback is considered safe.
