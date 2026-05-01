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
go test ./testing/go -run 'TestElectric(SyncSmoke|MultiShapeCatchupAndSchemaChange|CompatibilitySoak)$' -count=1 -v
```

`DOLTGRES_ELECTRIC_IMAGES` is comma-separated so CI can run a matrix without
changing test code. `DOLTGRES_ELECTRIC_IMAGE` remains as a single-image fallback
for local runs. `latest` may be tested opportunistically, but it is not a support
claim unless it is added to the pinned image list.

## Electric-backed coverage

| Area | Status | Evidence |
| --- | --- | --- |
| Startup and shape subscription | Supported for the pinned image | `TestElectricSyncSmoke` starts Electric, creates a shape, and waits for the default slot. |
| Initial sync plus insert/update/delete | Supported for published tables with `REPLICA IDENTITY FULL` | `TestElectricSyncSmoke` verifies all three operations through the shape API. |
| Doltgres restart while Electric is running | Supported for the pinned smoke path | `TestElectricSyncSmoke` restarts Doltgres with the same database directory and continues streaming. |
| Multiple tables and multiple shapes | Supported for independent full-table shapes | `TestElectricMultiShapeCatchupAndSchemaChange` verifies two concurrent table shapes with interleaved changes. |
| Electric restart and durable slot catch-up | Supported with persistent Electric shape storage and Doltgres durable slots | `TestElectricMultiShapeCatchupAndSchemaChange` stops Electric, writes backlog while the slot is inactive, restarts Electric, and verifies catch-up. |
| Concurrent backlog writers | Supported for distinct-row DML | `TestElectricMultiShapeCatchupAndSchemaChange` writes backlog through multiple SQL connections while Electric is offline. |
| Add-column metadata refresh | Supported for additive columns with defaults | `TestElectricMultiShapeCatchupAndSchemaChange` handles Electric's `must-refetch` response, updates the new column, and verifies the refreshed shape contains it. |
| Bounded throughput guardrail | Supported as a smoke-level guard, not a benchmark | `TestElectricCompatibilitySoak` applies 115 mutations and verifies final shape state within a fixed local timeout. |

## Direct pgoutput coverage

The lower-level source-mode tests in `testing/go/logical_replication_source_test.go`
cover behavior Electric depends on but that is faster and more precise to assert
without the Electric container:

| Area | Status | Evidence |
| --- | --- | --- |
| Replication handshake | Supported | `IDENTIFY_SYSTEM`, logical `CREATE_REPLICATION_SLOT`, `START_REPLICATION`, keepalive, standby status, and `CopyDone` are covered by `TestLogicalReplicationSourceProtocolAndCatalogs`. |
| pgoutput row messages | Supported for `Relation`, `Begin`, `Commit`, `Insert`, `Update`, and `Delete` | Covered by protocol, transaction, and update/delete tests. |
| `REPLICA IDENTITY FULL` old update tuples | Supported | `TestLogicalReplicationSourceUpdateIncludesOldTupleForReplicaIdentityFull`. |
| Publication filters and column lists | Supported in source-mode pgoutput | `TestLogicalReplicationSourceHonorsPublicationRowFilterAndColumnList` and `TestLogicalReplicationSourceHonorsPublicationUpdateDeleteFiltersAndColumnLists`. |
| Publication action flags | Supported for row-level DML | `TestLogicalReplicationSourceHonorsPublicationActionFlags`. |
| `FOR ALL TABLES` and schema publications | Supported | `TestLogicalReplicationSourcePublishesAllTablesAndSchemaPublications`. |
| Explicit transactions and prepared transactions | Supported for row messages | `TestLogicalReplicationSourcePublishesExplicitTransactionAsOnePgoutputTransaction`, `TestLogicalReplicationSourcePublishesPreparedStatementDMLInExplicitTransaction`, `TestLogicalReplicationSourcePublishesCommitPreparedAsOnePgoutputTransaction`, and `TestLogicalReplicationSourcePublishesRecoveredCommitPrepared`. |
| Durable inactive-slot replay | Supported | Restart replay and acknowledged-backlog pruning tests cover insert, update, delete, and LSN advancement. |
| Slot catalogs and sender stats | Supported | Protocol and restart tests assert `pg_replication_slots`, `pg_stat_replication`, and `pg_stat_replication_slots`. |
| Active slot drop rejection | Supported | `TestLogicalReplicationSourceTerminateBackendDeactivatesSlot` rejects drop while active, then terminates the sender and observes inactive state. |
| Unsupported slot mode/plugin failures | Explicitly rejected | `TestLogicalReplicationSourceRejectsUnsupportedSlotModesAndPlugins`. |
| Corrupt persisted slot state | Explicitly rejected at startup | `server/replsource` storage tests reject corrupt JSON and directory state paths. |

## Unsupported or unclaimed behavior

These are not claimed as Electric-supported:

- `TRUNCATE` pgoutput messages.
- Streaming in-progress transactions using pgoutput stream-start/stream-commit
  messages.
- Logical decoding plugins other than `pgoutput`.
- Physical replication slots.
- Drop-column behavior while Electric has live shapes. Additive schema changes
  are covered; destructive schema changes need an explicit design before they
  become supported.
- Live add-table publication refresh while Electric is already running. Doltgres
  catalog membership is covered elsewhere, but this Electric-backed support
  boundary only claims tables that are in the publication before the shape is
  created.
- Arbitrary Electric versions outside the pinned image list.

Any new support claim should add a test first, then update this document.
