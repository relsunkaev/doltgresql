# Electric compatibility

This document is the support boundary for Electric sync against Doltgres. The
short smoke path is not enough to claim broad compatibility; supported behavior
must be backed by the Docker-backed Electric tests and the direct pgoutput tests.

## Supported Electric versions

The pinned supported image is:

```bash
electricsql/electric:1.6.2
```

Run the supported-version suite with:

```bash
DOLTGRES_ELECTRIC_SMOKE=1 \
DOLTGRES_ELECTRIC_IMAGES=electricsql/electric:1.6.2 \
go test ./testing/go -run 'TestElectric(SyncSmoke|MultiShapeCatchupAndSchemaChange|QualifiedSchemaTablePublication|DropColumnShapeRefetch|CompatibilitySoak)$' -count=1 -v -timeout=20m
```

`DOLTGRES_ELECTRIC_IMAGES` is comma-separated so CI can run a matrix without
changing test code. `DOLTGRES_ELECTRIC_IMAGE` remains as a single-image fallback
for local runs. `latest` may be tested opportunistically, but it is not a support
claim unless it is added to the pinned image list.

The GitHub Actions lane in
`.github/workflows/electric-compatibility.yaml` runs the pinned Docker-backed
suite on pull requests. Ordinary non-Docker `go test` runs still skip the
Electric container tests unless `DOLTGRES_ELECTRIC_SMOKE=1` is set.

## Electric-backed coverage

| Area | Status | Evidence |
| --- | --- | --- |
| Startup and shape subscription | Supported for the pinned image | `TestElectricSyncSmoke` starts Electric, creates a shape, and waits for the default slot. |
| Initial sync plus insert/update/delete | Supported for published tables with `REPLICA IDENTITY FULL` | `TestElectricSyncSmoke` verifies all three operations through the shape API. |
| Doltgres restart while Electric is running | Supported for the pinned smoke path | `TestElectricSyncSmoke` restarts Doltgres with the same database directory and continues streaming. |
| Multiple tables and multiple shapes | Supported for independent full-table shapes | `TestElectricMultiShapeCatchupAndSchemaChange` verifies two concurrent table shapes with interleaved changes. |
| Electric-down and Doltgres-restart backlog recovery | Supported with persistent Electric shape storage and Doltgres durable slots | `TestElectricMultiShapeCatchupAndSchemaChange` stops Electric, writes backlog while the slot is inactive, restarts Doltgres from the same database directory, restarts Electric, verifies slot LSN advancement, and verifies final shape state. |
| Concurrent backlog writers | Supported for distinct-row DML | `TestElectricMultiShapeCatchupAndSchemaChange` writes backlog through multiple SQL connections while Electric is offline. |
| Add-column metadata refresh | Supported for additive columns with defaults | `TestElectricMultiShapeCatchupAndSchemaChange` handles Electric's `must-refetch` response, updates the new column, and verifies the refreshed shape contains it. |
| Drop-column metadata refresh | Supported for active shapes when Electric can refetch | `TestElectricDropColumnShapeRefetch` drops a column, updates a remaining column, and verifies the refreshed shape no longer exposes the dropped column. |
| Schema-qualified table shapes | Supported when the schema table is explicitly added to the publication | `TestElectricQualifiedSchemaTablePublication` publishes `electric_schema_pub.electric_schema_items`, requests the qualified table shape, and verifies insert/update delivery. |
| Bounded throughput guardrail | Supported as a smoke-level guard, not a benchmark | `TestElectricCompatibilitySoak` applies 340 mutations with concurrent shape reads, records mutations/sec, and verifies final shape state within a fixed local timeout. |

## Direct pgoutput coverage

The lower-level source-mode tests in `testing/go/logical_replication_source_test.go`
cover behavior Electric depends on but that is faster and more precise to assert
without the Electric container:

| Area | Status | Evidence |
| --- | --- | --- |
| Replication handshake | Supported | `IDENTIFY_SYSTEM`, logical `CREATE_REPLICATION_SLOT`, `START_REPLICATION`, keepalive, standby status, and `CopyDone` are covered by `TestLogicalReplicationSourceProtocolAndCatalogs`. |
| pgoutput row messages | Supported for `Relation`, `Begin`, `Commit`, `Insert`, `Update`, `Delete`, and `Truncate` | Covered by protocol, transaction, update/delete, and truncate tests. |
| `REPLICA IDENTITY FULL` old update tuples | Supported | `TestLogicalReplicationSourceUpdateIncludesOldTupleForReplicaIdentityFull`. |
| Publication filters and column lists | Supported in source-mode pgoutput | `TestLogicalReplicationSourceHonorsPublicationRowFilterAndColumnList` and `TestLogicalReplicationSourceHonorsPublicationUpdateDeleteFiltersAndColumnLists`. |
| Publication action flags | Supported for row-level DML | `TestLogicalReplicationSourceHonorsPublicationActionFlags`. |
| `FOR ALL TABLES` and schema publications | Supported | `TestLogicalReplicationSourcePublishesAllTablesAndSchemaPublications`. |
| Explicit transactions and prepared transactions | Supported for row messages | `TestLogicalReplicationSourcePublishesExplicitTransactionAsOnePgoutputTransaction`, `TestLogicalReplicationSourcePublishesPreparedStatementDMLInExplicitTransaction`, `TestLogicalReplicationSourcePublishesCommitPreparedAsOnePgoutputTransaction`, and `TestLogicalReplicationSourcePublishesRecoveredCommitPrepared`. |
| In-progress transaction streaming option | Accepted without stream messages | `TestLogicalReplicationSourceToleratesStreamingOptionWithoutStreamMessages` requests `proto_version '2'` and `streaming 'true'`, then verifies Doltgres still sends whole-transaction `Begin` / row / `Commit` messages. |
| Durable inactive-slot replay | Supported | Restart replay and acknowledged-backlog pruning tests cover insert, update, delete, and LSN advancement. |
| Slot catalogs and sender stats | Supported | Protocol and restart tests assert `pg_replication_slots`, `pg_stat_replication`, and `pg_stat_replication_slots`. |
| Active slot drop rejection | Supported | `TestLogicalReplicationSourceTerminateBackendDeactivatesSlot` rejects drop while active, then terminates the sender and observes inactive state. |
| Unsupported slot mode/plugin failures | Explicitly rejected | `TestLogicalReplicationSourceRejectsUnsupportedSlotModesAndPlugins`. |
| Corrupt persisted slot state | Explicitly rejected at startup | `server/replsource` storage tests reject corrupt JSON and directory state paths. |

## Unsupported or unclaimed behavior

These are not claimed as Electric-supported:

- Subscriber/apply-worker behavior. Doltgres is a logical replication source for
  Electric in the pinned suite; it does not run PostgreSQL subscription workers,
  perform initial table synchronization from a remote publisher, create remote
  subscription slots, or apply incoming `pgoutput` changes into local tables.
  `CREATE SUBSCRIPTION` is metadata-only and must use `connect=false` for any
  supported round trip. `TestSubscriptionDDLAndCatalogs` pins this by rejecting
  a default publisher connection, PostgreSQL-incompatible `connect=false`
  combinations (`create_slot=true`, `enabled=true`, `copy_data=true`),
  `ENABLE` on `slot_name=NONE`, disabled `REFRESH`, and enabled `REFRESH`
  because it requires a publisher connection, while allowing metadata-only
  `connect=false` rows to appear in `pg_subscription` and
  `pg_stat_subscription_stats`. It also verifies that no apply worker state is
  exposed through `pg_subscription_rel` or `pg_stat_subscription`.
- Emitting pgoutput stream-start/stream-commit messages for in-progress
  transaction streaming. Clients may request `streaming 'true'`, but Doltgres
  publishes complete transactions only.
- Publication row filters and publication column lists through Electric. These
  remain direct pgoutput claims. Electric has separate shape `where` and
  `columns` parameters, but this suite does not claim PostgreSQL publication
  filters or column lists as an Electric-supported configuration.
- Publication action subsets through Electric. Electric 1.6.2 requires the
  configured manual publication to publish `INSERT`, `UPDATE`, `DELETE`, and
  `TRUNCATE`, so `WITH (publish = ...)` subsets remain direct pgoutput claims.
- `FOR ALL TABLES` publications through Electric. Electric 1.6.2 validates
  explicit `pg_publication_rel` membership and treats all-table publications as
  missing when manual table publishing is enabled.
- `FOR TABLES IN SCHEMA` schema publications through Electric. Use explicit
  table membership for schema-qualified Electric shapes unless a future Electric
  version proves schema-publication support in the compatibility suite.
- Logical decoding plugins other than `pgoutput`.
- Physical replication slots.
- Live add-table publication refresh while Electric is already running. Doltgres
  catalog membership is covered elsewhere, but this Electric-backed support
  boundary only claims tables that are in the publication before the shape is
  created.
- Arbitrary Electric versions outside the pinned image list.

Any new support claim should add a test first, then update this document.
