# Reproduced Bugs

This file tracks data consistency, correctness, persistence, and security bugs
that have a concrete regression test reproducer. These are reproduction
artifacts only; no fixes are included here.

## Data Consistency

### LOCK TABLE ACCESS EXCLUSIVE does not block readers

- Reproducer: `TestLockTableAccessExclusiveBlocksReadersRepro` in
  `testing/go/lock_table_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestLockTableAccessExclusiveBlocksReadersRepro -count=1`.
- Expected PostgreSQL behavior: while one transaction holds `LOCK TABLE ...
  IN ACCESS EXCLUSIVE MODE`, a concurrent `SELECT` on that table blocks until
  the lock is released.
- Observed Doltgres behavior: the reader completes immediately while the lock
  holder transaction is still open, so the accepted lock statement does not
  provide the advertised serialization boundary.

### LOCK TABLE ACCESS EXCLUSIVE does not block writers

- Reproducer: `TestLockTableAccessExclusiveBlocksWritersRepro` in
  `testing/go/lock_table_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestLockTableAccessExclusiveBlocksWritersRepro -count=1`.
- Expected PostgreSQL behavior: while one transaction holds `LOCK TABLE ...
  IN ACCESS EXCLUSIVE MODE`, a concurrent `INSERT` on that table blocks until
  the lock is released.
- Observed Doltgres behavior: the writer completes immediately while the lock
  holder transaction is still open, so the accepted lock statement does not
  provide the advertised write exclusion boundary.

### LOCK TABLE SHARE MODE does not block writers

- Reproducer: `TestLockTableShareModeBlocksWritersRepro` in
  `testing/go/lock_table_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLockTableShareModeBlocksWritersRepro -count=1`.
- Expected PostgreSQL behavior: while one transaction holds `LOCK TABLE ... IN
  SHARE MODE`, a concurrent writer that needs a row-exclusive table lock, such
  as an `INSERT`, blocks until the SHARE lock is released.
- Observed Doltgres behavior: the concurrent `INSERT` completes immediately
  while the SHARE lock holder transaction is still open, so accepted weaker
  table locks do not provide PostgreSQL's write exclusion semantics.

### LOCK TABLE is allowed outside transaction blocks

- Reproducer: `TestLockTableRequiresTransactionBlockRepro` in
  `testing/go/lock_table_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLockTableRequiresTransactionBlockRepro -count=1`.
- Expected PostgreSQL behavior: `LOCK TABLE name IN ACCESS EXCLUSIVE MODE`
  outside an explicit transaction block fails with `can only be used in
  transaction blocks`.
- Observed Doltgres behavior: the standalone `LOCK TABLE` succeeds, so callers
  can issue lock statements in a context where PostgreSQL would reject them and
  no durable transaction-scoped lock can exist.

### LOCK TABLE NOWAIT ignores conflicting locks

- Reproducer: `TestLockTableNowaitRejectsConflictingLocksRepro` in
  `testing/go/lock_table_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLockTableNowaitRejectsConflictingLocksRepro -count=1`.
- Expected PostgreSQL behavior: if one transaction holds `LOCK TABLE ... IN
  ACCESS EXCLUSIVE MODE`, a concurrent `LOCK TABLE ... IN ACCESS SHARE MODE
  NOWAIT` fails immediately with `could not obtain lock on relation`.
- Observed Doltgres behavior: the conflicting `NOWAIT` lock succeeds without an
  error while the incompatible lock is still held, so callers can incorrectly
  proceed after believing they acquired PostgreSQL-compatible lock protection.

### SELECT FOR UPDATE does not block concurrent writers

- Reproducer: `TestSelectForUpdateBlocksConcurrentWritersRepro` in
  `testing/go/select_for_update_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestSelectForUpdateBlocksConcurrentWritersRepro -count=1`.
- Expected PostgreSQL behavior: a transaction that selects a row `FOR UPDATE`
  holds a row lock, and a concurrent writer targeting the same row blocks until
  the first transaction releases that lock.
- Observed Doltgres behavior: the concurrent `UPDATE` completes immediately
  while the `SELECT FOR UPDATE` transaction is still open.

### SELECT FOR SHARE does not block concurrent writers

- Reproducer: `TestSelectForShareBlocksConcurrentWritersRepro` in
  `testing/go/select_for_update_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSelectForShareBlocksConcurrentWritersRepro -count=1`.
- Expected PostgreSQL behavior: a transaction that selects a row `FOR SHARE`
  holds a share-strength row lock that conflicts with writers, so a concurrent
  `UPDATE` targeting that row blocks until the first transaction releases the
  lock.
- Observed Doltgres behavior: the concurrent `UPDATE` completes immediately
  while the `SELECT FOR SHARE` transaction is still open, so callers can read a
  row under a PostgreSQL share lock without actually excluding concurrent
  changes.

### SELECT FOR NO KEY UPDATE does not block concurrent writers

- Reproducer: `TestSelectForNoKeyUpdateBlocksConcurrentWritersRepro` in
  `testing/go/select_for_update_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSelectForNoKeyUpdateBlocksConcurrentWritersRepro -count=1`.
- Expected PostgreSQL behavior: a transaction that selects a row `FOR NO KEY
  UPDATE` holds a row lock that conflicts with ordinary writers, so a
  concurrent `UPDATE` targeting that row blocks until the first transaction
  releases the lock.
- Observed Doltgres behavior: the concurrent `UPDATE` completes immediately
  while the `SELECT FOR NO KEY UPDATE` transaction is still open, so callers can
  believe they reserved the row for non-key updates while concurrent writes
  still proceed.

### SELECT FOR KEY SHARE does not block concurrent deletes

- Reproducer: `TestSelectForKeyShareBlocksConcurrentDeletesRepro` in
  `testing/go/select_for_update_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSelectForKeyShareBlocksConcurrentDeletesRepro -count=1`.
- Expected PostgreSQL behavior: a transaction that selects a row `FOR KEY
  SHARE` holds a key-share row lock that conflicts with deletes, so a
  concurrent `DELETE` targeting that row blocks until the first transaction
  releases the lock.
- Observed Doltgres behavior: the concurrent `DELETE` completes immediately
  while the `SELECT FOR KEY SHARE` transaction is still open, so callers can
  protect a referenced key in PostgreSQL but lose that protection in Doltgres.

### SELECT FOR UPDATE accepts non-lockable result rows

- Reproducer: `TestSelectForUpdateRejectsNonLockableQueryShapesRepro` in
  `testing/go/select_for_update_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestSelectForUpdateRejectsNonLockableQueryShapesRepro
  -count=1`.
- Expected PostgreSQL behavior: `FOR UPDATE` is rejected for aggregate,
  grouped, `DISTINCT`, and set-operation result rows because those rows cannot
  be directly locked in the underlying base relation.
- Observed Doltgres behavior: all four queries are accepted without an error,
  so callers can believe they acquired row locks for result rows that do not
  map to lockable base-table tuples.

### SERIALIZABLE transactions allow write skew

- Reproducer: `TestSerializableRejectsWriteSkewRepro` in
  `testing/go/transaction_isolation_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSerializableRejectsWriteSkewRepro -count=1`.
- Expected PostgreSQL behavior: two concurrent `SERIALIZABLE` transactions
  that both read a two-row invariant and then each clear a different row cannot
  both commit; PostgreSQL aborts one transaction with `could not serialize
  access`, leaving one row still satisfying the invariant.
- Observed Doltgres behavior: both transactions commit, leaving zero
  `on_call` rows, so `SERIALIZABLE` does not prevent write-skew corruption of
  cross-row invariants.

### SERIALIZABLE transactions allow stale same-row read-modify-write commits

- Reproducer: `TestSerializableRejectsStaleReadModifyWriteRepro` in
  `testing/go/transaction_isolation_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSerializableRejectsStaleReadModifyWriteRepro -count=1`.
- Expected PostgreSQL behavior: if two concurrent `SERIALIZABLE` transactions
  both read the same row and then write a value derived from that stale read,
  PostgreSQL aborts one transaction with a serialization failure.
- Observed Doltgres behavior: both transactions commit successfully. Each
  transaction reads `balance = 0`, writes `balance = 1`, and the final stored
  row remains `balance = 1`, so a serializable read-modify-write loses one
  transaction's increment instead of forcing a retry.

### SET TRANSACTION ISOLATION is allowed after queries

- Reproducer: `TestSetTransactionIsolationAfterQueryRejectedRepro` in
  `testing/go/transaction_isolation_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSetTransactionIsolationAfterQueryRejectedRepro -count=1`.
- Expected PostgreSQL behavior: `SET TRANSACTION ISOLATION LEVEL ...` must run
  before any query in the transaction; after `BEGIN; SELECT 1`, PostgreSQL
  rejects it with `must be called before any query`.
- Observed Doltgres behavior: the isolation-level change is accepted after a
  query has already executed, so transaction mode changes can take effect at a
  point PostgreSQL forbids.

### SET TRANSACTION READ WRITE is allowed after queries

- Reproducer: `TestSetTransactionReadWriteAfterQueryRejectedRepro` in
  `testing/go/transaction_mode_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSetTransactionReadWriteAfterQueryRejectedRepro -count=1`.
- Expected PostgreSQL behavior: after `START TRANSACTION READ ONLY; SELECT 1`,
  switching the same transaction to `READ WRITE` fails with `transaction
  read-write mode must be set before any query`.
- Observed Doltgres behavior: `SET TRANSACTION READ WRITE` succeeds after the
  transaction has already run a query, allowing transaction access mode to be
  changed at a point PostgreSQL rejects.

### SET TRANSACTION DEFERRABLE is allowed after queries

- Reproducer: `TestSetTransactionDeferrableAfterQueryRejectedRepro` in
  `testing/go/transaction_mode_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSetTransactionDeferrableAfterQueryRejectedRepro -count=1`.
- Expected PostgreSQL behavior: after `BEGIN ISOLATION LEVEL SERIALIZABLE;
  SELECT 1`, changing deferrable mode in either direction fails with `SET
  TRANSACTION [NOT] DEFERRABLE must be called before any query`.
- Observed Doltgres behavior: `SET TRANSACTION DEFERRABLE` and `SET
  TRANSACTION NOT DEFERRABLE` both succeed after the serializable transaction
  has already run a query, so serializable transaction mode can be changed at a
  point PostgreSQL rejects.

### SET TRANSACTION SNAPSHOT accepts invalid snapshot contracts

- Reproducer: `TestSetTransactionSnapshotValidationRepro` in
  `testing/go/transaction_isolation_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSetTransactionSnapshotValidationRepro -count=1`.
- Expected PostgreSQL behavior: `SET TRANSACTION SNAPSHOT` rejects invalid
  snapshot identifiers and also rejects snapshot import unless the transaction
  isolation level is `REPEATABLE READ` or `SERIALIZABLE`.
- Observed Doltgres behavior: both `SET TRANSACTION SNAPSHOT 'not-a-snapshot'`
  in a repeatable-read transaction and the same call in a default read-committed
  transaction succeed, so session setup can claim to import a snapshot that
  PostgreSQL would reject.

### Shared advisory lock functions are missing

- Reproducer: `TestSharedAdvisoryLocksCoexistAndBlockExclusiveRepro` in
  `testing/go/advisory_lock_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestSharedAdvisoryLocksCoexistAndBlockExclusiveRepro
  -count=1`.
- Expected PostgreSQL behavior: `pg_advisory_lock_shared` and
  `pg_try_advisory_lock_shared` allow multiple shared holders for the same key,
  while an exclusive `pg_try_advisory_lock` on that key returns `false` until
  the shared holders release it.
- Observed Doltgres behavior: the first shared-lock call fails with `ERROR:
  function: 'pg_advisory_lock_shared' not found (errno 1105) (sqlstate HY000)
  (SQLSTATE XX000)`, so workloads cannot use PostgreSQL's shared advisory-lock
  exclusion mode.

### Some read-only transaction modes do not prevent writes

- Reproducers: `TestSetTransactionReadOnlyPreventsWritesRepro`,
  `TestDefaultTransactionReadOnlyPreventsWritesRepro`,
  `TestSessionCharacteristicsReadOnlyPreventsWritesRepro`,
  `TestReadOnlyTransactionRejectsPersistentSequenceNextvalRepro`, and
  `TestReadOnlyTransactionRejectsPersistentSequenceSetvalRepro` in
  `testing/go/transaction_mode_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run
  'Test(SetTransactionReadOnlyPreventsWrites|DefaultTransactionReadOnlyPreventsWrites|SessionCharacteristicsReadOnlyPreventsWrites|ReadOnlyTransactionRejectsPersistentSequence(Nextval|Setval))Repro'
  -count=1`.
- Expected PostgreSQL behavior: after `BEGIN; SET TRANSACTION READ ONLY`, or
  after `SET default_transaction_read_only TO on; BEGIN`, ordinary table writes
  fail with a read-only transaction error. `SET SESSION CHARACTERISTICS AS
  TRANSACTION READ ONLY` succeeds and makes later transactions read-only by
  default. In a persistent read-only transaction, `nextval` and `setval` on
  non-temporary sequences also fail because they would advance persistent state.
- Observed Doltgres behavior: both read-only table-write reproducers accept the
  `INSERT`, `SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY` fails with
  `ERROR: SET SESSION CHARACTERISTICS is not yet supported (SQLSTATE 0A000)`,
  and `START TRANSACTION READ ONLY; SELECT nextval('read_only_persistent_seq')`
  and `SELECT setval('read_only_setval_seq', 50)` both succeed instead of
  rejecting the persistent sequence writes.

### COPY FROM rejection in a read-only transaction also rejects ROLLBACK

- Reproducer: `TestReadOnlyCopyFromFailureAllowsRollbackRepro` in
  `testing/go/transaction_mode_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestReadOnlyCopyFromFailureAllowsRollbackRepro -count=1`.
- Expected PostgreSQL behavior: `COPY ... FROM STDIN` into a persistent table
  inside `START TRANSACTION READ ONLY` fails, but transaction-control recovery
  is still allowed and `ROLLBACK` closes the transaction successfully.
- Observed Doltgres behavior: `COPY FROM` is rejected with `cannot execute
  statement in a READ ONLY transaction`, but the following `ROLLBACK` is also
  rejected with the same read-only transaction error.

### COPY FREEZE is rejected in valid post-TRUNCATE contexts

- Reproducer: `TestCopyFreezeAfterTruncateRepro` in
  `testing/go/copy_freeze_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCopyFreezeAfterTruncateRepro -count=1`.
- Expected PostgreSQL behavior: after a table is truncated in the current
  transaction, `COPY table FROM STDIN WITH (FREEZE)` is valid and persists the
  copied rows.
- Observed Doltgres behavior: the valid `COPY FREEZE` statement fails with `at
  or near "freeze": syntax error`, and the client sends COPY data messages
  after no COPY operation has started.

### Read-only transactions allow persistent DDL

- Reproducers: `TestStartTransactionReadOnlyRejectsCreateTableRepro`,
  `TestStartTransactionReadOnlyRejectsAlterTableRepro`,
  `TestStartTransactionReadOnlyRejectsDropTableRepro`,
  `TestStartTransactionReadOnlyRejectsCreateIndexRepro`,
  `TestStartTransactionReadOnlyRejectsDropIndexRepro`,
  `TestStartTransactionReadOnlyRejectsCreateSequenceRepro`, and
  `TestStartTransactionReadOnlyRejectsDropSequenceRepro` in
  `testing/go/transaction_mode_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run
  'TestStartTransactionReadOnlyRejects(CreateTable|AlterTable|DropTable|CreateIndex|DropIndex|CreateSequence|DropSequence)Repro'
  -count=1`.
- Expected PostgreSQL behavior: inside `START TRANSACTION READ ONLY`,
  persistent table, index, and sequence DDL all fail with a read-only
  transaction error; after `COMMIT`, the catalog remains unchanged.
- Observed Doltgres behavior: persistent table, index, and sequence DDL
  statements succeed inside the read-only transaction. `COMMIT` persists
  created objects, persists table/index changes, and removes dropped objects.

### Read-only transaction allows TRUNCATE and ROLLBACK does not restore rows

- Reproducer: `TestStartTransactionReadOnlyRejectsTruncateRepro` in
  `testing/go/transaction_mode_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestStartTransactionReadOnlyRejectsTruncateRepro -count=1`.
- Expected PostgreSQL behavior: inside `START TRANSACTION READ ONLY`,
  `TRUNCATE` of a persistent table fails with a read-only transaction error. If
  a transaction is rolled back, rows present before the transaction remain.
- Observed Doltgres behavior: `TRUNCATE read_only_truncate_target` succeeds
  inside the read-only transaction, and after `ROLLBACK`, the table remains
  empty instead of restoring the two pre-existing rows.

### CREATE TABLE PARTITION BY is accepted but ignored

- Reproducers: `TestCreateTablePartitionByPersistsPartitionMetadataRepro` and
  `TestPartitionedTableWithoutPartitionRejectsInsertRepro` in
  `testing/go/partition_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'Test(CreateTablePartitionByPersistsPartitionMetadata|PartitionedTableWithoutPartitionRejectsInsert)Repro'
  -count=1`.
- Expected PostgreSQL behavior: `CREATE TABLE ... PARTITION BY LIST (id)`
  creates a partitioned table with `pg_class.relkind = 'p'`,
  `pg_get_partkeydef(oid) = 'LIST (id)'`, and an insert with no matching
  partition fails.
- Observed Doltgres behavior: the table is created as an ordinary table with
  `relkind = 'r'`, `pg_get_partkeydef` returns an empty string, and rows insert
  successfully into the parent table.

### CREATE TABLE INHERITS is treated like a one-time LIKE copy

- Reproducers: `TestInheritedTableRowsVisibleThroughParentRepro` and
  `TestCreateTableInheritsPersistsPgInheritsMetadataRepro` in
  `testing/go/inheritance_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'Test(InheritedTableRowsVisibleThroughParent|CreateTableInheritsPersistsPgInheritsMetadata)Repro'
  -count=1`.
- Expected PostgreSQL behavior: rows inserted into an inherited child table are
  visible when scanning the parent table, and `pg_inherits` records the
  parent/child relationship.
- Observed Doltgres behavior: the parent scan returns no child rows, and
  `pg_inherits` has no row for the accepted inheritance declaration.

### Parent-table UPDATE does not update inherited child rows

- Reproducer: `TestInheritedChildRowsUpdatedThroughParentRepro` in
  `testing/go/inheritance_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestInheritedChildRowsUpdatedThroughParentRepro -count=1`.
- Expected PostgreSQL behavior: `UPDATE inherit_parent_update SET ... WHERE
  id = 1` scans inherited child rows by default and updates the matching
  child's inherited columns.
- Observed Doltgres behavior: the parent-table update succeeds but affects no
  inherited child rows, leaving `inherit_child_update.label` unchanged as
  `child`.

### Parent-table DELETE does not delete inherited child rows

- Reproducer: `TestInheritedChildRowsDeletedThroughParentRepro` in
  `testing/go/inheritance_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestInheritedChildRowsDeletedThroughParentRepro -count=1`.
- Expected PostgreSQL behavior: `DELETE FROM inherit_parent_delete WHERE id =
  1` scans inherited child rows by default and removes the matching child row.
- Observed Doltgres behavior: the parent-table delete succeeds but affects no
  inherited child rows, leaving the child row present.

### Parent-table TRUNCATE does not truncate inherited child rows

- Reproducer: `TestTruncateInheritedParentTruncatesChildRowsRepro` in
  `testing/go/inheritance_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTruncateInheritedParentTruncatesChildRowsRepro -count=1`.
- Expected PostgreSQL behavior: `TRUNCATE inherit_parent_truncate` truncates
  rows from the parent table and inherited child tables by default unless
  `ONLY` is specified.
- Observed Doltgres behavior: the parent table is truncated, but the inherited
  child table still contains its row, so truncating the parent leaves descendant
  data behind.

### Inherited generated columns accept default and identity overrides

- Reproducers: `TestInheritedGeneratedColumnRejectsDefaultOverrideRepro` and
  `TestInheritedGeneratedColumnRejectsIdentityOverrideRepro` in
  `testing/go/inheritance_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestInheritedGeneratedColumnRejects(Default|Identity)OverrideRepro'
  -count=1`.
- Expected PostgreSQL behavior: if a parent column is `GENERATED ALWAYS AS
  (...) STORED`, a child table cannot merge that inherited column with a regular
  default or with identity generation; `CREATE TABLE child (b INT DEFAULT 10)
  INHERITS (parent)` fails with `inherits from generated column but specifies
  default`, and `b INT GENERATED ALWAYS AS IDENTITY` fails with `inherits from
  generated column but specifies identity`.
- Observed Doltgres behavior: both child table definitions succeed, allowing
  schemas where an inherited generated column can be treated as a regular
  defaulted column or as an identity column.

### ALTER TABLE parent ADD COLUMN does not propagate to inherited child tables

- Reproducer: `TestAlterInheritedParentAddColumnPropagatesToChildRepro` in
  `testing/go/inheritance_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterInheritedParentAddColumnPropagatesToChildRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE parent ADD COLUMN marker TEXT
  DEFAULT 'added' NOT NULL` adds the inherited column to child tables and
  backfills existing child rows with the default.
- Observed Doltgres behavior: the parent add-column statement succeeds, but the
  child schema is unchanged; selecting `marker` from the child fails with
  `column "marker" could not be found in any table in scope`.

### ALTER TABLE parent RENAME COLUMN does not propagate to inherited child tables

- Reproducer: `TestAlterInheritedParentRenameColumnPropagatesToChildRepro` in
  `testing/go/inheritance_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterInheritedParentRenameColumnPropagatesToChildRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE parent RENAME COLUMN label TO
  title` renames the inherited child column as well, so child rows are readable
  through `title`.
- Observed Doltgres behavior: the parent rename succeeds, but the child schema
  keeps the old column name; selecting `title` from the child fails with
  `column "title" could not be found in any table in scope`.

### ALTER TABLE parent DROP COLUMN does not propagate to inherited child tables

- Reproducer: `TestAlterInheritedParentDropColumnPropagatesToChildRepro` in
  `testing/go/inheritance_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterInheritedParentDropColumnPropagatesToChildRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE parent DROP COLUMN label` drops
  the inherited child column too, leaving only `id` and the child's local
  `extra` column.
- Observed Doltgres behavior: the parent drop-column statement succeeds, but
  `information_schema.columns` for the child still lists `label`.

### ALTER TABLE parent ALTER COLUMN TYPE does not propagate to inherited child tables

- Reproducer: `TestAlterInheritedParentColumnTypePropagatesToChildRepro` in
  `testing/go/inheritance_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterInheritedParentColumnTypePropagatesToChildRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE parent ALTER COLUMN amount TYPE
  BIGINT` rewrites inherited child rows and updates the child column type, so
  the child reports `pg_typeof(amount) = bigint`.
- Observed Doltgres behavior: the parent type change succeeds, but the child
  column remains `integer`.

### ALTER TABLE parent ADD CHECK does not protect inherited child writes

- Reproducer: `TestAlterInheritedParentAddCheckPropagatesToChildRepro` in
  `testing/go/inheritance_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterInheritedParentAddCheckPropagatesToChildRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE parent ADD CONSTRAINT ... CHECK
  (id > 0)` propagates the inherited check to child tables and rejects later
  child rows with negative `id` values.
- Observed Doltgres behavior: the added parent check is not enforced on the
  child, so `INSERT INTO inherit_child_add_check VALUES (-1, 'bad', 'extra')`
  succeeds and persists an invalid child row.

### ALTER TABLE parent ADD CHECK does not validate existing inherited child rows

- Reproducer: `TestAlterInheritedParentAddCheckValidatesExistingChildRowsRepro`
  in `testing/go/inheritance_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterInheritedParentAddCheckValidatesExistingChildRowsRepro
  -count=1`.
- Expected PostgreSQL behavior: adding `CHECK (id > 0)` to a parent table
  scans inherited child tables and rejects the DDL if any existing child row
  violates the inherited constraint.
- Observed Doltgres behavior: the parent `ADD CHECK` succeeds even though
  `inherit_child_existing_check` already contains `id = -1`, leaving invalid
  child data under an accepted parent constraint.

### ALTER TABLE parent SET NOT NULL does not protect inherited child writes

- Reproducer: `TestAlterInheritedParentSetNotNullPropagatesToChildRepro` in
  `testing/go/inheritance_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterInheritedParentSetNotNullPropagatesToChildRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE parent ALTER COLUMN label SET NOT
  NULL` propagates the inherited not-null requirement to child tables and
  rejects later child rows with `NULL` in `label`.
- Observed Doltgres behavior: the parent `SET NOT NULL` succeeds, but the
  child still accepts and persists a row whose inherited `label` value is
  `NULL`.

### ALTER TABLE parent SET NOT NULL does not validate existing inherited child rows

- Reproducer:
  `TestAlterInheritedParentSetNotNullValidatesExistingChildRowsRepro` in
  `testing/go/inheritance_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterInheritedParentSetNotNullValidatesExistingChildRowsRepro
  -count=1`.
- Expected PostgreSQL behavior: setting `label NOT NULL` on a parent table
  scans inherited child tables and rejects the DDL if any existing child row has
  `NULL` in that inherited column.
- Observed Doltgres behavior: the parent `SET NOT NULL` succeeds even though
  `inherit_child_existing_not_null` already contains a child row with
  `label = NULL`, leaving invalid child data under an accepted parent
  constraint.

### DROP TABLE parent ignores inherited child dependencies

- Reproducer: `TestDropInheritedParentRequiresCascadeRepro` in
  `testing/go/inheritance_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDropInheritedParentRequiresCascadeRepro -count=1`.
- Expected PostgreSQL behavior: `DROP TABLE inherit_parent_drop_restrict`
  fails under default `RESTRICT` behavior because
  `inherit_child_drop_restrict` depends on the parent through table
  inheritance.
- Observed Doltgres behavior: the parent is dropped successfully while the
  inherited child remains, severing the inheritance dependency without
  `CASCADE`.

### DROP TABLE parent CASCADE leaves inherited child tables behind

- Reproducer: `TestDropInheritedParentCascadeDropsChildRepro` in
  `testing/go/inheritance_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDropInheritedParentCascadeDropsChildRepro -count=1`.
- Expected PostgreSQL behavior: `DROP TABLE inherit_parent_drop_cascade
  CASCADE` drops both the inherited parent and the dependent child table.
- Observed Doltgres behavior: the parent is dropped, but
  `inherit_child_drop_cascade` still exists after the cascade.

### ALTER TABLE parent SET DEFAULT does not propagate to inherited child tables

- Reproducer: `TestAlterInheritedParentSetDefaultPropagatesToChildRepro` in
  `testing/go/inheritance_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterInheritedParentSetDefaultPropagatesToChildRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE parent ALTER COLUMN label SET
  DEFAULT 'new-default'` propagates the default to inherited child tables, so a
  later child insert that omits `label` stores `new-default`.
- Observed Doltgres behavior: the parent default change succeeds, but the child
  keeps no default for the inherited column; the later child insert persists
  `label = NULL`.

### ALTER TABLE parent DROP DEFAULT does not propagate to inherited child tables

- Reproducer: `TestAlterInheritedParentDropDefaultPropagatesToChildRepro` in
  `testing/go/inheritance_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterInheritedParentDropDefaultPropagatesToChildRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE parent ALTER COLUMN label DROP
  DEFAULT` removes the inherited default from child tables, so a later child
  insert that omits `label` stores `NULL`.
- Observed Doltgres behavior: the parent default drop succeeds, but the child
  keeps the old inherited default and persists `label = 'parent-default'`.

### ALTER TABLE parent DROP NOT NULL does not propagate to inherited child tables

- Reproducer: `TestAlterInheritedParentDropNotNullPropagatesToChildRepro` in
  `testing/go/inheritance_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterInheritedParentDropNotNullPropagatesToChildRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE parent ALTER COLUMN label DROP
  NOT NULL` removes the inherited not-null requirement from child tables, so a
  later child insert with `label = NULL` succeeds.
- Observed Doltgres behavior: the parent not-null drop succeeds, but the child
  still rejects `NULL` in the inherited `label` column.

### ALTER TABLE INHERIT is rejected instead of attaching a child table

- Reproducer: `TestAlterTableInheritAttachesChildRepro` in
  `testing/go/inheritance_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterTableInheritAttachesChildRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE child INHERIT parent` establishes
  a live inheritance relationship for compatible tables, making child rows
  visible through parent scans and recording the edge in `pg_inherits`.
- Observed Doltgres behavior: the attach statement fails with `ALTER TABLE
  with unsupported command type *tree.AlterTableInherit`, so existing
  compatible tables cannot be attached through PostgreSQL inheritance.

### ALTER TABLE NO INHERIT is rejected instead of detaching a child table

- Reproducer: `TestAlterTableNoInheritDetachesChildRepro` in
  `testing/go/inheritance_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterTableNoInheritDetachesChildRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE child NO INHERIT parent` removes
  the inheritance relationship so the child no longer depends on or scans
  through the parent.
- Observed Doltgres behavior: the detach statement fails with `ALTER TABLE
  with unsupported command type *tree.AlterTableInherit`, so existing
  inheritance relationships cannot be detached.

### Zero-column `CREATE TABLE` panics

- Reproducer: `TestCreateZeroColumnTableRepro` in
  `testing/go/create_table_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateZeroColumnTableRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE TABLE name ()` creates a zero-column
  relation, `INSERT INTO name DEFAULT VALUES` stores a row, and
  `SELECT count(*) FROM name` reports that row.
- Observed Doltgres behavior: `CREATE TABLE zero_column_items ()` panics with
  a nil-pointer dereference in `planbuilder.(*Builder).buildIndexDefs`, so the
  valid relation cannot be created.

### CREATE TABLE reloptions are rejected

- Reproducer: `TestCreateTableReloptionsPersistRepro` in
  `testing/go/create_table_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateTableReloptionsPersistRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE TABLE name (...) WITH (fillfactor=30,
  autovacuum_enabled=false, autovacuum_analyze_scale_factor=0.2)` succeeds and
  stores those table reloptions in `pg_catalog.pg_class.reloptions`.
- Observed Doltgres behavior: setup fails with `storage parameters are not yet
  supported`, so table storage options cannot be declared or persisted.

### CREATE TABLE IF NOT EXISTS AS evaluates the skipped query

- Reproducer: `TestCreateTableAsIfNotExistsDoesNotEvaluateQueryRepro` in
  `testing/go/create_table_as_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateTableAsIfNotExistsDoesNotEvaluateQueryRepro -count=1`.
- Expected PostgreSQL behavior: when the destination relation already exists,
  `CREATE TABLE IF NOT EXISTS name AS SELECT ...` skips the CTAS query and
  leaves the existing table unchanged.
- Observed Doltgres behavior: the skipped query is still evaluated; a
  replacement query containing `SELECT 1 / 0 AS value` fails with `division by
  zero` even though the destination table already exists.

### CREATE TABLE AS with an explicit column list panics

- Reproducer: `TestCreateTableAsExplicitColumnNamesRepro` in
  `testing/go/create_table_as_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateTableAsExplicitColumnNamesRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE TABLE name (col1, col2) AS SELECT ...`
  creates the target table and uses the explicit column list to rename the
  query output columns.
- Observed Doltgres behavior: the statement panics during AST conversion with
  a nil-pointer dereference in `server/ast.nodeColumnTableDef`, and the target
  table is not created.

### CREATE TABLE AS EXECUTE is rejected

- Reproducer: `TestCreateTableAsExecutePreparedStatementRepro` in
  `testing/go/prepared_statement_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateTableAsExecutePreparedStatementRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE TABLE target AS EXECUTE
  prepared_statement(...)` executes the prepared statement and materializes its
  result rows into the new table.
- Observed Doltgres behavior: the CTAS statement fails with `at or near
  "execute": syntax error`, and the target table is not created.

### Prepared SELECT star silently changes result shape after DDL

- Reproducer: `TestPreparedSelectStarRejectsChangedResultShapeRepro` in
  `testing/go/prepared_statement_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPreparedSelectStarRejectsChangedResultShapeRepro -count=1`.
- Expected PostgreSQL behavior: after `PREPARE p AS SELECT * FROM table`,
  schema DDL that changes the prepared statement result row type causes a later
  `EXECUTE p` to fail with `cached plan must not change result type`.
- Observed Doltgres behavior: after `ALTER TABLE prepared_shape_items ADD
  COLUMN label TEXT DEFAULT 'new shape'`, `EXECUTE prepared_shape_plan` succeeds
  and returns fields `[id label]` with row `[1 new shape]`, silently changing
  the prepared statement result shape visible to the client.

### CREATE TABLE AS copies source column defaults

- Reproducer: `TestCreateTableAsDoesNotCopyDefaultsRepro` in
  `testing/go/create_table_as_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateTableAsDoesNotCopyDefaultsRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE TABLE copy AS SELECT ... FROM source`
  creates regular result columns and does not copy source-table column
  defaults, so an insert that omits the copied column stores `NULL`.
- Observed Doltgres behavior: the CTAS target retains the source column
  default; inserting a row that omits the copied `label` column stores `source
  default` instead of `NULL`.

### CREATE TABLE AS TIMETZ typmod output stores unrounded values

- Reproducer: `TestCreateTableAsTimetzTypmodMaterializesRoundedValueRepro` in
  `testing/go/create_table_as_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateTableAsTimetzTypmodMaterializesRoundedValueRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE TABLE name AS SELECT
  CAST('21:43:56.789+00'::timetz AS TIMETZ(0)) AS tz` materializes
  `21:43:57+00` and records the output column type as
  `time(0) with time zone`.
- Observed Doltgres behavior: the CTAS output column metadata reports
  `time(0) with time zone`, but the stored row is `21:43:56.789+00`, so CTAS
  can materialize data outside the declared result-column precision.

### CREATE TABLE AS timestamp typmod output stores unrounded values

- Reproducer: `TestCreateTableAsTimestampTypmodMaterializesRoundedValueRepro` in
  `testing/go/create_table_as_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateTableAsTimestampTypmodMaterializesRoundedValueRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE TABLE name AS SELECT
  CAST(TIMESTAMP '2021-09-15 21:43:56.789' AS TIMESTAMP(0)) AS ts` materializes
  `2021-09-15 21:43:57` and records the output column type as
  `timestamp(0) without time zone`.
- Observed Doltgres behavior: the CTAS target stores
  `2021-09-15 21:43:56.789`, so CTAS can persist timestamp values outside the
  declared result-column precision.

### CREATE TABLE AS timestamptz typmod output stores unrounded values

- Reproducer:
  `TestCreateTableAsTimestamptzTypmodMaterializesRoundedValueRepro` in
  `testing/go/create_table_as_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateTableAsTimestamptzTypmodMaterializesRoundedValueRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE TABLE name AS SELECT
  CAST(TIMESTAMPTZ '2021-09-15 21:43:56.789+00' AS TIMESTAMPTZ(0)) AS ts`
  materializes `2021-09-15 21:43:57+00` and records the output column type as
  `timestamp(0) with time zone`.
- Observed Doltgres behavior: the CTAS target stores
  `2021-09-15 21:43:56.789+00`, so CTAS can persist timestamptz values outside
  the declared result-column precision.

### CREATE TABLE AS interval typmod output stores unrestricted values

- Reproducer: `TestCreateTableAsIntervalTypmodMaterializesRestrictedValueRepro`
  in `testing/go/create_table_as_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateTableAsIntervalTypmodMaterializesRestrictedValueRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE TABLE name AS SELECT CAST(INTERVAL '3
  days 04:05:06.789' AS INTERVAL DAY TO SECOND(0)) AS ds` materializes
  `3 days 04:05:07` and records the output column type as
  `interval day to second(0)`.
- Observed Doltgres behavior: the CTAS target stores `3 days 04:05:06.789` and
  reports the output column type as plain `interval`, so CTAS can persist
  interval values outside the declared result-column precision and lose field
  restriction metadata.

### CREATE TABLE AS character typmod output stores unpadded values

- Reproducer: `TestCreateTableAsCharacterTypmodMaterializesPaddedValueRepro` in
  `testing/go/create_table_as_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateTableAsCharacterTypmodMaterializesPaddedValueRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE TABLE name AS SELECT CAST('ab' AS
  CHARACTER(3)) AS label` materializes a padded fixed-width value, so
  `octet_length(label)` is `3`, and records the output column type as
  `character(3)`.
- Observed Doltgres behavior: the CTAS output column metadata reports
  `character(3)`, but the stored row has `octet_length(label) = 2`, so CTAS can
  persist fixed-width character output without the required padding.

### CREATE TABLE LIKE INCLUDING DEFAULTS is rejected

- Reproducer: `TestCreateTableLikeIncludingDefaultsCopiesDefaultsRepro` in
  `testing/go/create_table_like_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestCreateTableLikeIncludingDefaultsCopiesDefaultsRepro
  -count=1`.
- Expected PostgreSQL behavior: `CREATE TABLE copy (LIKE source INCLUDING
  DEFAULTS)` creates a table with copied column defaults, so inserting a row
  that omits the copied column uses the source table's default expression.
- Observed Doltgres behavior: table creation fails with `ERROR: options for
  LIKE are not yet supported (SQLSTATE XX000)`, so valid schema-copy
  migrations cannot preserve defaults.

### CREATE TABLE LIKE INCLUDING IDENTITY is rejected

- Reproducer: `TestCreateTableLikeIncludingIdentityCopiesIdentityRepro` in
  `testing/go/create_table_like_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateTableLikeIncludingIdentityCopiesIdentityRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE TABLE copy (LIKE source INCLUDING
  IDENTITY)` copies identity generation into the new table, so inserts that omit
  the copied identity column receive generated values.
- Observed Doltgres behavior: table creation fails with `at or near "identity":
  syntax error: unimplemented: this syntax`, so schema-copy migrations cannot
  preserve identity generation.

### CREATE TABLE LIKE INCLUDING GENERATED is rejected

- Reproducer: `TestCreateTableLikeIncludingGeneratedCopiesGeneratedColumnsRepro`
  in `testing/go/create_table_like_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateTableLikeIncludingGeneratedCopiesGeneratedColumnsRepro
  -count=1`.
- Expected PostgreSQL behavior: `CREATE TABLE copy (LIKE source INCLUDING
  GENERATED)` copies stored generated column expressions into the new table, so
  writes to base columns recompute the generated values.
- Observed Doltgres behavior: table creation fails with `options for LIKE are
  not yet supported`, so schema-copy migrations cannot preserve generated-column
  semantics.

### CREATE TABLE LIKE copies identity generation by default

- Reproducer: `TestCreateTableLikeExcludesIdentityByDefaultRepro` in
  `testing/go/create_table_like_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateTableLikeExcludesIdentityByDefaultRepro -count=1`.
- Expected PostgreSQL behavior: plain `CREATE TABLE copy (LIKE source)` does
  not copy identity generation; an explicit value can be inserted into the
  copied column unless `INCLUDING IDENTITY` was requested.
- Observed Doltgres behavior: the copied column is still treated as a generated
  identity column, and inserting an explicit value fails with `The value
  specified for generated column "id" in table "like_no_identity_copy" is not
  allowed`.

### CREATE TABLE LIKE copies generated expressions by default

- Reproducer: `TestCreateTableLikeExcludesGeneratedByDefaultRepro` in
  `testing/go/create_table_like_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateTableLikeExcludesGeneratedByDefaultRepro -count=1`.
- Expected PostgreSQL behavior: plain `CREATE TABLE copy (LIKE source)` creates
  a regular column for a source generated column unless `INCLUDING GENERATED`
  was requested, so omitting the copied column stores `NULL`.
- Observed Doltgres behavior: the copied column remains generated and computes
  `doubled = 14` for `base_value = 7`, even though `INCLUDING GENERATED` was
  not requested.

### CREATE TABLE LIKE copies defaults, CHECK constraints, and indexes by default

- Reproducers: `TestCreateTableLikeExcludesDefaultsByDefaultRepro`,
  `TestCreateTableLikeExcludesCheckConstraintsByDefaultRepro`, and
  `TestCreateTableLikeExcludesIndexesByDefaultRepro` in
  `testing/go/create_table_like_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestCreateTableLikeExcludes(Defaults|CheckConstraints|Indexes)ByDefaultRepro'
  -count=1`.
- Expected PostgreSQL behavior: plain `CREATE TABLE copy (LIKE source)` copies
  column names and types, but it does not copy defaults, `CHECK` constraints,
  or indexes/unique constraints unless the corresponding `INCLUDING` option is
  requested.
- Observed Doltgres behavior: the copied table uses the source default
  (`source default`) when the column is omitted, rejects a row that violates the
  source `CHECK (amount > 0)`, and rejects duplicate copied `code` values via
  the source unique index.

### DISCARD ALL does not clear LISTEN registrations

- Reproducer: `TestDiscardAllUnlistensChannelsRepro` in
  `testing/go/session_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDiscardAllUnlistensChannelsRepro -count=1`.
- Expected PostgreSQL behavior: `DISCARD ALL` includes `UNLISTEN *`, so a
  session that was listening on a channel stops receiving notifications after
  the discard completes.
- Observed Doltgres behavior: after `LISTEN discard_all_channel` and
  `DISCARD ALL`, a later `NOTIFY discard_all_channel, 'after discard'` is still
  delivered to the discarded session.

### DISCARD TEMP is rejected before dropping temporary tables

- Reproducer: `TestDiscardTempDropsTemporaryTablesRepro` in
  `testing/go/session_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDiscardTempDropsTemporaryTablesRepro -count=1`.
- Expected PostgreSQL behavior: `DISCARD TEMP` succeeds and drops all temporary
  tables in the current session, so the discarded temp relation is no longer
  queryable afterward.
- Observed Doltgres behavior: `DISCARD TEMP` fails with `ERROR: at or near
  "temp": syntax error: unimplemented: this syntax (SQLSTATE 0A000)`, and the
  reproducer confirms the temporary table still contains one row.

### ALTER SYSTEM is rejected before transaction-block validation

- Reproducer: `TestAlterSystemInsideTransactionRejectedRepro` in
  `testing/go/alter_system_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterSystemInsideTransactionRejectedRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER SYSTEM SET work_mem = '64kB'` and
  `ALTER SYSTEM RESET work_mem` are parsed as supported PostgreSQL statements,
  but inside an explicit transaction block they fail with `ALTER SYSTEM cannot
  run inside a transaction block` before any `postgresql.auto.conf` write.
- Observed Doltgres behavior: both forms fail during parsing with `at or near
  "system": syntax error (SQLSTATE 42601)`, so clients see a syntax error
  instead of the PostgreSQL admin-command transaction-boundary error.

### SET SESSION AUTHORIZATION is rejected instead of switching session users

- Reproducer: `TestSetSessionAuthorizationChangesCurrentAndSessionUserRepro` in
  `testing/go/session_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSetSessionAuthorizationChangesCurrentAndSessionUserRepro -count=1`.
- Expected PostgreSQL behavior: an elevated session can run `SET SESSION
  AUTHORIZATION session_auth_target`, after which both `current_user` and
  `session_user` report `session_auth_target` until reset.
- Observed Doltgres behavior: `SET SESSION AUTHORIZATION` and `RESET SESSION
  AUTHORIZATION` both fail with `ERROR: SET SESSION AUTHORIZATION is not yet
  supported (SQLSTATE 0A000)`, and the session remains authorized as
  `postgres`.

### Database DDL is allowed inside transaction blocks

- Reproducers: `TestCreateDatabaseInsideTransactionRejectedRepro` and
  `TestDropDatabaseInsideTransactionRejectedRepro` in
  `testing/go/transaction_ddl_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'Test(Create|Drop)DatabaseInsideTransactionRejectedRepro' -count=1`.
- Expected PostgreSQL behavior: `CREATE DATABASE` and `DROP DATABASE` fail
  inside an explicit transaction block with `cannot run inside a transaction
  block`.
- Observed Doltgres behavior: both statements succeed after `BEGIN`.

### CREATE DATABASE accepts invalid encoding names

- Reproducer: `TestCreateDatabaseRejectsInvalidEncodingRepro` in
  `testing/go/create_database_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateDatabaseRejectsInvalidEncodingRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE DATABASE name ENCODING notexist` fails
  with `not a valid encoding name` and does not create the database.
- Observed Doltgres behavior: the statement succeeds, so invalid encoding
  metadata in schema setup or restore input is accepted instead of rejected.

### CREATE DATABASE catalog options are rejected

- Reproducer: `TestCreateDatabaseCatalogOptionsRepro` in
  `testing/go/create_database_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateDatabaseCatalogOptionsRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE DATABASE` accepts catalog options such
  as `ALLOW_CONNECTIONS false`, `CONNECTION LIMIT 0`, and `IS_TEMPLATE true`,
  and persists them in `pg_database.datallowconn`, `datconnlimit`, and
  `datistemplate`.
- Observed Doltgres behavior: each option is rejected as not yet supported, so
  valid database provisioning DDL cannot express connection policy, connection
  limits, or template-database state.

### ALTER DATABASE RENAME TO is rejected

- Reproducer: `TestAlterDatabaseRenameRepro` in
  `testing/go/alter_database_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterDatabaseRenameRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER DATABASE old_name RENAME TO new_name`
  succeeds for a database that is not the current connection target, and the
  renamed database can be connected to afterward.
- Observed Doltgres behavior: the statement fails with `RENAME DATABASE is not
  yet supported`, so valid database rename migrations cannot run.

### ALTER DATABASE catalog options are rejected

- Reproducer: `TestAlterDatabaseCatalogOptionsRepro` in
  `testing/go/alter_database_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterDatabaseCatalogOptionsRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER DATABASE ... WITH ALLOW_CONNECTIONS
  false`, `CONNECTION LIMIT 0`, and `IS_TEMPLATE true` succeed and persist the
  changed values in `pg_database.datallowconn`, `datconnlimit`, and
  `datistemplate`.
- Observed Doltgres behavior: each statement fails with `ALTER DATABASE is not
  yet supported Please file an issue at
  https://github.com/dolthub/doltgresql/issues`, and the catalog rows keep the
  default values `datallowconn = t`, `datconnlimit = -1`, and
  `datistemplate = f`.

### ALTER DATABASE SET does not persist pg_db_role_setting rows

- Reproducer: `TestAlterDatabaseSetPopulatesPgDbRoleSettingRepro` in
  `testing/go/alter_database_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterDatabaseSetPopulatesPgDbRoleSettingRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER DATABASE database_setting_catalog SET
  work_mem = '64kB'` succeeds and persists a database-scoped configuration row
  in `pg_catalog.pg_db_role_setting`.
- Observed Doltgres behavior: the statement fails with `ALTER DATABASE is not
  yet supported Please file an issue at
  https://github.com/dolthub/doltgresql/issues`, so the database-scoped
  configuration row is never recorded.

### Transaction errors do not abort later writes before commit

- Reproducer: `TestTransactionErrorRollsBackTransactionOnCommitRepro` in
  `testing/go/transaction_error_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTransactionErrorRollsBackTransactionOnCommitRepro -count=1`.
- Expected PostgreSQL behavior: after a statement error inside an explicit
  transaction, later statements fail with `current transaction is aborted`; a
  later `COMMIT` rolls the failed transaction back, so no rows persist.
- Observed Doltgres behavior: after a duplicate primary key error, a later
  insert in the same transaction succeeds, `COMMIT` persists the transaction,
  and the final table contains both `(1, 'before error')` and
  `(2, 'after error')`.

### Savepoint errors do not reject later statements before rollback recovery

- Reproducer: `TestSavepointErrorRequiresRollbackToSavepointRepro` in
  `testing/go/transaction_error_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSavepointErrorRequiresRollbackToSavepointRepro -count=1`.
- Expected PostgreSQL behavior: after a statement error following `SAVEPOINT
  sp`, later SQL statements fail with `current transaction is aborted` until
  the client runs `ROLLBACK TO SAVEPOINT sp`; after that rollback, the outer
  transaction can accept new work and commit.
- Observed Doltgres behavior: after a duplicate primary key error inside the
  savepoint, `INSERT INTO savepoint_error_items VALUES (2, 'before rollback to
  savepoint')` succeeds instead of being rejected. `ROLLBACK TO SAVEPOINT`
  later discards the accepted row, but Doltgres still exposes a writable state
  that PostgreSQL forbids before savepoint recovery.

### DROP SEQUENCE allows removing sequences still used by column defaults

- Reproducer: `TestDropSequenceReferencedByDefaultRequiresCascadeRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDropSequenceReferencedByDefaultRequiresCascadeRepro -count=1`.
- Expected PostgreSQL behavior: if a table column default calls
  `nextval('default_dependency_seq')`, `DROP SEQUENCE default_dependency_seq`
  fails without `CASCADE` because the default depends on the sequence.
- Observed Doltgres behavior: the sequence is dropped successfully, leaving the
  table default pointing at a removed sequence.

### DROP SEQUENCE CASCADE leaves dependent column defaults behind

- Reproducer: `TestDropSequenceCascadeRemovesColumnDefaultRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDropSequenceCascadeRemovesColumnDefaultRepro -count=1`.
- Expected PostgreSQL behavior: `DROP SEQUENCE cascade_default_seq CASCADE`
  removes column defaults that depend on the dropped sequence; later inserts
  into the table do not call the deleted sequence.
- Observed Doltgres behavior: `DROP SEQUENCE cascade_default_seq CASCADE`
  drops the sequence but leaves `cascade_default_items.id` with its stale
  `nextval('cascade_default_seq')` default. A later insert that omits `id`
  fails with `relation "cascade_default_seq" does not exist`.

### DROP SEQUENCE treats OWNED BY as a dependent default

- Reproducer: `TestDropOwnedSequenceWithoutDefaultRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDropOwnedSequenceWithoutDefaultRepro -count=1`.
- Expected PostgreSQL behavior: a sequence that is `OWNED BY` a table column
  but is not referenced by a column default can be dropped directly; `OWNED BY`
  makes the sequence depend on the column, not the column depend on the
  sequence.
- Observed Doltgres behavior: `DROP SEQUENCE
  owned_sequence_without_default_seq` is rejected with `cannot drop sequence
  owned_sequence_without_default_seq because other objects depend on it`,
  leaving the unused sequence in `pg_class`.

### DROP COLUMN leaves owned sequences behind

- Reproducer: `TestDropOwnedColumnDropsOwnedSequenceRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDropOwnedColumnDropsOwnedSequenceRepro -count=1`.
- Expected PostgreSQL behavior: dropping a table column drops sequences marked
  `OWNED BY` that column.
- Observed Doltgres behavior: `ALTER TABLE
  drop_owned_column_sequence_items DROP COLUMN id` succeeds, but
  `drop_owned_column_sequence_seq` remains in `pg_class` as a sequence.

### RENAME COLUMN does not update owned sequence dependency metadata

- Reproducer: `TestRenameOwnedColumnUpdatesOwnedSequenceRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRenameOwnedColumnUpdatesOwnedSequenceRepro -count=1`.
- Expected PostgreSQL behavior: after renaming a column that owns a sequence,
  `pg_get_serial_sequence` for the renamed column still returns the owned
  sequence.
- Observed Doltgres behavior: `ALTER TABLE
  rename_owned_column_sequence_items RENAME COLUMN id TO renamed_id` succeeds,
  but `pg_get_serial_sequence('rename_owned_column_sequence_items',
  'renamed_id')` returns `NULL`.

### ALTER SEQUENCE RENAME TO is rejected

- Reproducer: `TestAlterSequenceRenameToRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterSequenceRenameToRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER SEQUENCE rename_sequence_old_seq
  RENAME TO rename_sequence_new_seq` renames the sequence; `nextval` resolves
  through the new name and the old name stops resolving.
- Observed Doltgres behavior: the valid rename statement fails with `RENAME
  SEQUENCE is not yet supported`, leaving only the old sequence name.

### Schema-qualified sequence OWNED BY rejects same-schema unqualified tables

- Reproducers: `TestCreateQualifiedSequenceOwnedByUnqualifiedTableRepro` and
  `TestAlterQualifiedSequenceOwnedByUnqualifiedTableRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'Test(CreateQualifiedSequenceOwnedByUnqualifiedTable|AlterQualifiedSequenceOwnedByUnqualifiedTable)Repro'
  -count=1`.
- Expected PostgreSQL behavior: `OWNED BY table.column` resolves an
  unqualified table name through the search path before enforcing that the
  sequence and owning table are in the same schema, so `public.seq OWNED BY
  table.id` works when `table` resolves to `public.table`.
- Observed Doltgres behavior: `CREATE SEQUENCE
  public.create_qualified_sequence_owner_seq OWNED BY
  create_qualified_sequence_owner_items.id` fails with `CREATE SEQUENCE must
  use the same schema for the sequence and owned table`; `ALTER SEQUENCE
  public.alter_qualified_sequence_owner_seq OWNED BY
  alter_qualified_sequence_owner_items.id` fails with the equivalent `ALTER
  SEQUENCE` error. The sequence ownership metadata is never persisted, and
  `pg_get_serial_sequence(...)` returns `NULL`.

### DROP COLUMN allows removing columns still used by views

- Reproducer: `TestDropColumnUsedByViewRequiresCascadeRepro` in
  `testing/go/view_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDropColumnUsedByViewRequiresCascadeRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE view_dependency_source DROP COLUMN
  drop_value` fails under default `RESTRICT` behavior because
  `view_dependency_reader` depends on that column.
- Observed Doltgres behavior: the column is dropped successfully, leaving the
  dependent view definition stale.

### DROP COLUMN allows removing columns still used by materialized views

- Reproducer: `TestDropColumnUsedByMaterializedViewRequiresCascadeRepro` in
  `testing/go/view_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDropColumnUsedByMaterializedViewRequiresCascadeRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... DROP COLUMN drop_value` fails
  under default `RESTRICT` behavior because a materialized view depends on that
  base-table column.
- Observed Doltgres behavior: the column is dropped successfully, leaving the
  dependent materialized-view definition stale.

### DROP COLUMN CASCADE leaves dependent views behind

- Reproducers: `TestDropColumnCascadeDropsDependentViewRepro` and
  `TestDropColumnCascadeDropsDependentMaterializedViewRepro` in
  `testing/go/view_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestDropColumnCascadeDropsDependent(View|MaterializedView)Repro'
  -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... DROP COLUMN drop_value CASCADE`
  drops dependent views and materialized views, then removes the base-table
  column.
- Observed Doltgres behavior: the parser logs `CASCADE option on DROP COLUMN is
  not yet supported, ignoring`; the base-table column is removed, but
  `cascade_column_reader` and `cascade_mat_column_reader` remain registered as
  stale dependent relations.

### DROP TABLE allows removing tables still used by views

- Reproducer: `TestDropTableUsedByViewRequiresCascadeRepro` in
  `testing/go/view_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDropTableUsedByViewRequiresCascadeRepro -count=1`.
- Expected PostgreSQL behavior: `DROP TABLE view_table_dependency_source` fails
  under default `RESTRICT` behavior because `view_table_dependency_reader`
  depends on the table.
- Observed Doltgres behavior: the table is dropped successfully, leaving the
  dependent view definition stale.

### DROP TABLE allows removing tables still used by materialized views

- Reproducer: `TestDropTableUsedByMaterializedViewRequiresCascadeRepro` in
  `testing/go/view_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDropTableUsedByMaterializedViewRequiresCascadeRepro -count=1`.
- Expected PostgreSQL behavior: `DROP TABLE matview_table_dependency_source`
  fails under default `RESTRICT` behavior because
  `matview_table_dependency_reader` depends on the table.
- Observed Doltgres behavior: the table is dropped successfully, leaving the
  materialized view with a stale source dependency.

### DROP TABLE CASCADE leaves dependent views behind

- Reproducers: `TestDropTableCascadeDropsDependentViewRepro` and
  `TestDropTableCascadeDropsDependentMaterializedViewRepro` in
  `testing/go/view_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestDropTableCascadeDropsDependent(View|MaterializedView)Repro'
  -count=1`.
- Expected PostgreSQL behavior: `DROP TABLE ... CASCADE` drops dependent views
  and materialized views along with the base table.
- Observed Doltgres behavior: the base table is dropped, but the dependent
  normal view or materialized view remains registered, leaving stale relations
  that still reference the removed table.

### DROP VIEW allows removing views still used by other views

- Reproducer: `TestDropViewUsedByViewRequiresCascadeRepro` in
  `testing/go/view_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDropViewUsedByViewRequiresCascadeRepro -count=1`.
- Expected PostgreSQL behavior: `DROP VIEW view_chain_base` fails under default
  `RESTRICT` behavior because `view_chain_reader` depends on that view.
- Observed Doltgres behavior: the base view is dropped successfully, leaving the
  dependent view definition stale.

### DROP MATERIALIZED VIEW allows removing materialized views still used by views

- Reproducer: `TestDropMaterializedViewUsedByViewRequiresCascadeRepro` in
  `testing/go/view_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDropMaterializedViewUsedByViewRequiresCascadeRepro -count=1`.
- Expected PostgreSQL behavior: `DROP MATERIALIZED VIEW matview_chain_base`
  fails under default `RESTRICT` behavior because `matview_chain_reader`
  depends on that materialized view.
- Observed Doltgres behavior: the materialized view is dropped successfully,
  leaving the dependent view definition stale.

### DROP VIEW and DROP MATERIALIZED VIEW CASCADE are rejected

- Reproducers: `TestDropViewCascadeRepro` and
  `TestDropMaterializedViewCascadeRepro` in
  `testing/go/drop_restrict_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestDrop(View|MaterializedView)CascadeRepro' -count=1`.
- Expected PostgreSQL behavior: `DROP VIEW ... CASCADE` and `DROP MATERIALIZED
  VIEW ... CASCADE` remove dependent views before dropping the target relation.
- Observed Doltgres behavior: both statements fail with `CASCADE is not yet
  supported`, so valid dependency-dropping DDL cannot be replayed and dependent
  views are left behind.

### DROP FUNCTION CASCADE is rejected

- Reproducer: `TestDropFunctionCascadeRepro` in
  `testing/go/drop_restrict_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDropFunctionCascadeRepro -count=1`.
- Expected PostgreSQL behavior: `DROP FUNCTION name() CASCADE` drops the
  function and any dependent objects, such as triggers that execute that
  function.
- Observed Doltgres behavior: the statement fails with `DROP FUNCTION with
  CASCADE is not supported yet`, so valid dependency-dropping function DDL
  cannot be replayed.

### RENAME COLUMN leaves dependent views stale

- Reproducer: `TestRenameColumnUsedByViewKeepsViewUsableRepro` in
  `testing/go/view_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestRenameColumnUsedByViewKeepsViewUsableRepro -count=1`.
- Expected PostgreSQL behavior: renaming a base-table column rewrites dependent
  view definitions so the existing view remains queryable with its original
  output column names.
- Observed Doltgres behavior: the rename succeeds, but querying the dependent
  view fails because the stored view definition still references the old
  base-table column name.

### RENAME TABLE leaves dependent views stale

- Reproducer: `TestRenameTableUsedByViewKeepsViewUsableRepro` in
  `testing/go/view_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestRenameTableUsedByViewKeepsViewUsableRepro -count=1`.
- Expected PostgreSQL behavior: renaming a base table rewrites dependent view
  definitions so the existing view remains queryable.
- Observed Doltgres behavior: the rename succeeds, but querying the dependent
  view fails because the stored view definition still references the old
  base-table name.

### Schema-qualified ALTER TABLE RENAME loses the source schema

- Reproducer: `TestAlterTableQualifiedRenameStaysInSchemaRepro` in
  `testing/go/schema_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterTableQualifiedRenameStaysInSchemaRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE schema.table RENAME TO new_name`
  renames the relation inside the original schema, preserving its data.
- Observed Doltgres behavior: the statement resolves only the unqualified source
  table name, fails with `table not found: source_table`, and leaves the
  original schema table unchanged.

### CREATE SCHEMA ignores contained schema elements

- Reproducer: `TestCreateSchemaExecutesContainedTableRepro` in
  `testing/go/schema_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateSchemaExecutesContainedTableRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE SCHEMA name CREATE TABLE item (...)`
  creates both the schema and the contained table, and the table can be written
  and read afterward.
- Observed Doltgres behavior: the schema creation statement succeeds, but the
  contained `CREATE TABLE` is ignored; later writes to `schema.table` fail with
  `table not found`.

### ALTER SCHEMA RENAME TO is rejected

- Reproducer: `TestAlterSchemaRenameRepro` in
  `testing/go/schema_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterSchemaRenameRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER SCHEMA old_name RENAME TO new_name`
  renames the namespace and keeps contained objects accessible through the new
  schema name while removing the old schema name.
- Observed Doltgres behavior: the statement fails with `ERROR: ALTER SCHEMA
  is not yet supported Please file an issue at
  https://github.com/dolthub/doltgresql/issues (SQLSTATE 0A000)`. The
  contained table remains accessible through the old schema name and is missing
  from the new schema name.

### ALTER SET SCHEMA is rejected for relations, routines, sequences, types, and domains

- Reproducers: `TestAlterTableSetSchemaMovesRelationRepro`,
  `TestAlterViewSetSchemaMovesViewRepro`,
  `TestAlterMaterializedViewSetSchemaMovesViewRepro`,
  `TestAlterSequenceSetSchemaMovesSequenceRepro`,
  `TestAlterFunctionSetSchemaMovesFunctionRepro`,
  `TestAlterProcedureSetSchemaMovesProcedureRepro`,
  `TestAlterRoutineSetSchemaMovesFunctionRepro`,
  `TestAlterTypeSetSchemaMovesEnumRepro`, and
  `TestAlterDomainSetSchemaMovesDomainRepro` in
  `testing/go/schema_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestAlter(Table|View|MaterializedView|Sequence|Function|Procedure|Routine|Type|Domain)SetSchema.*Repro'
  -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... SET SCHEMA`, `ALTER VIEW ...
  SET SCHEMA`, `ALTER MATERIALIZED VIEW ... SET SCHEMA`, `ALTER SEQUENCE ...
  SET SCHEMA`, `ALTER FUNCTION ... SET SCHEMA`, `ALTER PROCEDURE ... SET
  SCHEMA`, generic `ALTER ROUTINE ... SET SCHEMA`, `ALTER TYPE ... SET SCHEMA`,
  and `ALTER DOMAIN ... SET SCHEMA` move the existing object into the target
  schema, preserving stored rows, view definitions, materialized contents,
  sequence state, routine behavior, or type semantics while removing the old
  `public`-schema name.
- Observed Doltgres behavior: `ALTER TABLE ... SET SCHEMA` fails with `ERROR:
  ALTER TABLE SET SCHEMA is not yet supported (SQLSTATE 0A000)`. `ALTER VIEW
  ... SET SCHEMA` fails with `ALTER VIEW is not yet supported`; `ALTER
  MATERIALIZED VIEW ... SET SCHEMA` and `ALTER SEQUENCE ... SET SCHEMA` also
  fail through the unsupported table SET SCHEMA path. `ALTER FUNCTION ... SET
  SCHEMA` and `ALTER PROCEDURE ... SET SCHEMA` fail with their broad
  not-supported errors; `ALTER ROUTINE ... SET SCHEMA` fails with a generic
  syntax error. `ALTER TYPE ... SET SCHEMA` and `ALTER DOMAIN ... SET SCHEMA`
  fail with their corresponding not-supported errors. The objects remain in
  `public` and cannot be moved to another schema.

### ALTER FUNCTION RENAME TO is rejected

- Reproducer: `TestAlterFunctionRenameRepro` in
  `testing/go/function_lookup_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterFunctionRenameRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER FUNCTION old_name(...) RENAME TO
  new_name` renames the existing function. Calls through the new name resolve
  to the original body, and calls through the old name fail as undefined.
- Observed Doltgres behavior: the rename statement fails with `ERROR: ALTER
  FUNCTION statement is not yet supported Please file an issue at
  https://github.com/dolthub/doltgresql/issues (SQLSTATE 0A000)`. The new
  function name cannot be resolved, while the old name remains callable.

### ALTER PROCEDURE RENAME TO is rejected

- Reproducer: `TestAlterProcedureRenameRepro` in
  `testing/go/procedure_argument_modes_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterProcedureRenameRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER PROCEDURE old_name(...) RENAME TO
  new_name` renames the existing procedure. Calls through the new name execute
  the original procedure body, and calls through the old name fail as
  undefined.
- Observed Doltgres behavior: the rename statement fails with `ERROR: ALTER
  PROCEDURE statement is not yet supported Please file an issue at
  https://github.com/dolthub/doltgresql/issues (SQLSTATE 0A000)`. The new
  procedure name cannot be resolved, while the old name remains callable.

### ALTER ROUTINE RENAME TO is rejected

- Reproducer: `TestAlterRoutineRenameFunctionRepro` in
  `testing/go/function_lookup_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterRoutineRenameFunctionRepro -count=1`.
- Expected PostgreSQL behavior: the generic `ALTER ROUTINE old_name(...)
  RENAME TO new_name` syntax can rename a function, after which the new name
  resolves to the original function body and the old name is undefined.
- Observed Doltgres behavior: the statement fails during parsing with `ERROR:
  at or near "routine": syntax error (SQLSTATE 42601)`. The new function name
  cannot be resolved, while the old name remains callable.

### DROP SCHEMA CASCADE is rejected

- Reproducer: `TestDropSchemaCascadeDropsContainedRelationsRepro` in
  `testing/go/schema_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDropSchemaCascadeDropsContainedRelationsRepro -count=1`.
- Expected PostgreSQL behavior: `DROP SCHEMA cascade_schema CASCADE` drops the
  schema and the relations contained within it.
- Observed Doltgres behavior: the statement fails with `ERROR: DROP SCHEMA with
  CASCADE behavior is not yet supported`, leaving the schema and contained
  table in place.

### ALTER COLUMN TYPE USING is rejected before converting stored rows

- Reproducer: `TestAlterColumnTypeUsingConvertsExistingRowsRepro` in
  `testing/go/alter_table_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterColumnTypeUsingConvertsExistingRowsRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... ALTER COLUMN amount_text TYPE
  INT USING amount_text::INT` rewrites existing stored `text` values such as
  `10` and `25` into integer values while changing the column type.
- Observed Doltgres behavior: the `ALTER TABLE` fails with `ERROR: ALTER TABLE
  with USING is not supported yet (SQLSTATE XX000)`, leaving the stored values
  and column type unchanged.

### ALTER COLUMN TYPE ignores row-type dependents when the type is unchanged

- Reproducer: `TestAlterTableSameTypeRejectsRowTypeDependentsRepro` in
  `testing/go/alter_table_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterTableSameTypeRejectsRowTypeDependentsRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE row_type_parent ALTER COLUMN a SET
  DATA TYPE INT` is rejected while another table has a `row_type_parent`
  column, even though the requested type is the same, with `cannot alter table
  "row_type_parent" because column "row_type_child.parent_row" uses its row
  type`.
- Observed Doltgres behavior: the statement succeeds, bypassing the row-type
  dependency guard that should prevent table row type rewrites while dependent
  columns exist.

### ALTER COLUMN TYPE typmod rewrites are incomplete and allow overflow

- Reproducer: `TestAlterColumnTypeAppliesTypmodsToExistingRowsRepro` in
  `testing/go/alter_table_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterColumnTypeAppliesTypmodsToExistingRowsRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... ALTER COLUMN ... TYPE`
  rewrites existing rows through the new column typmod. Converting a
  `timestamp` column to `timestamp(0)` rounds existing fractional seconds, and
  converting a `numeric` column to `numeric(5,2)` rejects rows such as
  `999.995` that overflow after rounding, leaving the stored value unchanged.
- Observed Doltgres behavior: converting `timestamp` to `timestamp(0)` leaves
  `2021-09-15 21:43:56.789` unchanged, and converting `999.995` to
  `numeric(5,2)` succeeds and stores `1000.00` instead of raising
  `numeric field overflow`, so `ALTER COLUMN TYPE` can persist values that
  violate the target PostgreSQL typmod.

### ALTER COLUMN TYPE varchar typmod rewrites truncate oversized values

- Reproducer: `TestAlterColumnTypeVarcharRejectsTypmodOverflowRepro` in
  `testing/go/alter_table_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterColumnTypeVarcharRejectsTypmodOverflowRepro -count=1`.
- Expected PostgreSQL behavior: converting a `text` column containing `abcd` to
  `varchar(3)` rejects the rewrite with `value too long for type character
  varying(3)` and leaves the stored row and column type unchanged.
- Observed Doltgres behavior: the `ALTER TABLE` succeeds, changes the column
  type to `character varying`, and rewrites the stored value from `abcd` to
  `abc`, so a schema change can silently truncate persisted data.

### ALTER COLUMN TYPE character typmod rewrites truncate or underpad values

- Reproducer: `TestAlterColumnTypeCharacterAppliesTypmodRepro` in
  `testing/go/alter_table_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterColumnTypeCharacterAppliesTypmodRepro -count=1`.
- Expected PostgreSQL behavior: converting a `text` column containing `abcd` to
  `character(3)` rejects the rewrite with `value too long for type
  character(3)`, while converting a `text` value `ab` to `character(3)` stores
  a padded three-byte value equal to `'ab '::character(3)`.
- Observed Doltgres behavior: the overlong rewrite succeeds and changes the
  stored value from `abcd` to a truncated `character` value, while the shorter
  rewrite stores `ab` with `octet_length = 2` and it does not compare equal to
  the padded `character(3)` value.

### ALTER COLUMN TYPE TIMETZ typmod rewrites store unrounded values

- Reproducer: `TestAlterColumnTypeAppliesTimetzTypmodToExistingRowsRepro` in
  `testing/go/alter_table_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterColumnTypeAppliesTimetzTypmodToExistingRowsRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... ALTER COLUMN tz TYPE
  TIMETZ(0)` rewrites existing rows through the new column typmod, so existing
  value `21:43:56.789+00` stores as `21:43:57+00`.
- Observed Doltgres behavior: the altered row keeps `21:43:56.789+00`, so
  `ALTER COLUMN TYPE` can persist existing timetz values that violate the
  target column's declared precision.

### ALTER COLUMN TYPE TIME typmod rewrites store unrounded values

- Reproducer: `TestAlterColumnTypeAppliesTimeTypmodToExistingRowsRepro` in
  `testing/go/alter_table_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterColumnTypeAppliesTimeTypmodToExistingRowsRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... ALTER COLUMN tm TYPE TIME(0)`
  rewrites existing rows through the new column typmod, so existing value
  `21:43:56.789` stores as `21:43:57`.
- Observed Doltgres behavior: the altered row keeps `21:43:56.789`, so
  `ALTER COLUMN TYPE` can persist existing time values that violate the target
  column's declared precision.

### ALTER COLUMN TYPE timestamptz typmod rewrites store unrounded values

- Reproducer:
  `TestAlterColumnTypeAppliesTimestamptzTypmodToExistingRowsRepro` in
  `testing/go/alter_table_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterColumnTypeAppliesTimestamptzTypmodToExistingRowsRepro
  -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... ALTER COLUMN tz TYPE
  TIMESTAMPTZ(0)` rewrites existing rows through the new column typmod, so
  existing value `2021-09-15 21:43:56.789+00` stores as
  `2021-09-15 21:43:57+00` when the session time zone is UTC.
- Observed Doltgres behavior: the altered row keeps
  `2021-09-15 21:43:56.789+00`, so `ALTER COLUMN TYPE` can persist existing
  timestamptz values that violate the target column's declared precision.

### ALTER COLUMN TYPE interval typmod rewrites store unrestricted values

- Reproducer: `TestAlterColumnTypeAppliesIntervalTypmodToExistingRowsRepro` in
  `testing/go/alter_table_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterColumnTypeAppliesIntervalTypmodToExistingRowsRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... ALTER COLUMN ds TYPE INTERVAL
  DAY TO SECOND(0)` rewrites existing rows through the new column typmod, so
  existing value `3 days 04:05:06.789` stores as `3 days 04:05:07`.
- Observed Doltgres behavior: the altered row keeps `3 days 04:05:06.789`, so
  `ALTER COLUMN TYPE` can persist existing interval values that violate the
  target column's declared field/precision.

### ALTER TABLE ADD COLUMN TIMETZ typmod defaults backfill unrounded values

- Reproducer:
  `TestAlterTableAddTimetzTypmodColumnDefaultBackfillsRoundedValueRepro` in
  `testing/go/alter_table_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterTableAddTimetzTypmodColumnDefaultBackfillsRoundedValueRepro
  -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... ADD COLUMN tz TIMETZ(0)
  DEFAULT '21:43:56.789+00'::timetz` backfills existing rows through the new
  column typmod, so every existing row stores `21:43:57+00`.
- Observed Doltgres behavior: existing rows are backfilled with
  `21:43:56.789+00`, so adding a timetz typmod column with a default can
  persist values outside the new column's declared precision.

### ALTER TABLE ADD COLUMN timestamp typmod defaults backfill unrounded values

- Reproducer:
  `TestAlterTableAddTimestampTypmodColumnDefaultBackfillsRoundedValueRepro` in
  `testing/go/alter_table_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterTableAddTimestampTypmodColumnDefaultBackfillsRoundedValueRepro
  -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... ADD COLUMN ts TIMESTAMP(0)
  DEFAULT '2021-09-15 21:43:56.789'::timestamp` backfills existing rows through
  the new column typmod, so every existing row stores `2021-09-15 21:43:57`.
- Observed Doltgres behavior: existing rows are backfilled with
  `2021-09-15 21:43:56.789`, so adding a timestamp typmod column with a default
  can persist values outside the new column's declared precision.

### ALTER TABLE ADD COLUMN timestamptz typmod defaults backfill unrounded values

- Reproducer:
  `TestAlterTableAddTimestamptzTypmodColumnDefaultBackfillsRoundedValueRepro` in
  `testing/go/alter_table_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterTableAddTimestamptzTypmodColumnDefaultBackfillsRoundedValueRepro
  -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... ADD COLUMN tz TIMESTAMPTZ(0)
  DEFAULT '2021-09-15 21:43:56.789+00'::timestamptz` backfills existing rows
  through the new column typmod, so every existing row stores
  `2021-09-15 21:43:57+00` when the session time zone is UTC.
- Observed Doltgres behavior: existing rows are backfilled with
  `2021-09-15 21:43:56.789+00`, so adding a timestamptz typmod column with a
  default can persist values outside the new column's declared precision.

### ALTER TABLE ADD COLUMN interval typmod defaults backfill unrestricted values

- Reproducer:
  `TestAlterTableAddIntervalTypmodColumnDefaultBackfillsRestrictedValueRepro` in
  `testing/go/alter_table_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterTableAddIntervalTypmodColumnDefaultBackfillsRestrictedValueRepro
  -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... ADD COLUMN ds INTERVAL DAY TO
  SECOND(0) DEFAULT '3 days 04:05:06.789'::interval` backfills existing rows
  through the new column typmod, so every existing row stores
  `3 days 04:05:07`.
- Observed Doltgres behavior: existing rows are backfilled with
  `3 days 04:05:06.789`, so adding an interval typmod column with a default can
  persist values outside the new column's declared field/precision.

### ALTER TABLE ADD COLUMN character typmod defaults backfill underpadded values

- Reproducer:
  `TestAlterTableAddCharacterTypmodColumnDefaultPadsBackfillRepro` in
  `testing/go/alter_table_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterTableAddCharacterTypmodColumnDefaultPadsBackfillRepro
  -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... ADD COLUMN label CHARACTER(3)
  DEFAULT 'ab'` backfills existing rows through the new fixed-width typmod, so
  every existing row stores a three-byte value equal to
  `'ab '::character(3)`.
- Observed Doltgres behavior: existing rows are backfilled with `ab` at
  `octet_length = 2`, and the stored value does not compare equal to the padded
  `character(3)` value, so adding fixed-width character defaults can persist
  underpadded data.

### ALTER TABLE reloptions are rejected

- Reproducer: `TestAlterTableReloptionsPersistRepro` in
  `testing/go/alter_table_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterTableReloptionsPersistRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE name SET (fillfactor=40,
  autovacuum_enabled=false)` succeeds and persists the table reloptions in
  `pg_catalog.pg_class.reloptions`.
- Observed Doltgres behavior: the `ALTER TABLE` fails with `ALTER TABLE with
  unsupported command type *tree.AlterTableSetStorage`, so table reloptions
  cannot be changed after creation.

### CREATE ACCESS METHOD is rejected before pg_am metadata is persisted

- Reproducer: `TestCreateAccessMethodPersistsPgAmRepro` in
  `testing/go/access_method_definition_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateAccessMethodPersistsPgAmRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE ACCESS METHOD heap_repro_am TYPE TABLE
  HANDLER heap_tableam_handler` succeeds and `pg_catalog.pg_am` exposes the
  new table access-method row.
- Observed Doltgres behavior: the setup statement fails with `at or near
  "access": syntax error (SQLSTATE 42601)`, so custom access methods cannot be
  defined and no `pg_am` metadata is persisted.

### DROP ACCESS METHOD IF EXISTS is rejected instead of no-oping

- Reproducer: `TestDropAccessMethodIfExistsMissingNoopsRepro` in
  `testing/go/access_method_definition_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDropAccessMethodIfExistsMissingNoopsRepro -count=1`.
- Expected PostgreSQL behavior: `DROP ACCESS METHOD IF EXISTS missing_repro_am`
  succeeds as a no-op when the access method does not exist.
- Observed Doltgres behavior: the statement fails with `at or near "access":
  syntax error (SQLSTATE 42601)`, so defensive migration cleanup DDL cannot
  run.

### RENAME TABLE leaves dependent materialized-view refresh definitions stale

- Reproducer: `TestRenameTableUsedByMaterializedViewKeepsRefreshUsableRepro`
  in `testing/go/view_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestRenameTableUsedByMaterializedViewKeepsRefreshUsableRepro -count=1`.
- Expected PostgreSQL behavior: renaming a base table preserves dependent
  materialized-view dependencies, and later `REFRESH MATERIALIZED VIEW` reads
  from the renamed table.
- Observed Doltgres behavior: the rename succeeds, but refresh still resolves
  the old table name and the materialized view retains its stale snapshot.

### RENAME VIEW leaves dependent views stale

- Reproducer: `TestRenameViewUsedByViewKeepsViewUsableRepro` in
  `testing/go/view_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestRenameViewUsedByViewKeepsViewUsableRepro -count=1`.
- Expected PostgreSQL behavior: renaming a view through `ALTER TABLE ... RENAME
  TO` preserves dependent views, which continue to resolve the renamed view by
  dependency identity.
- Observed Doltgres behavior: the rename succeeds, but querying the dependent
  view fails because the stored view definition still references the old view
  name.

### RENAME MATERIALIZED VIEW leaves dependent views stale

- Reproducer: `TestRenameMaterializedViewUsedByViewKeepsViewUsableRepro` in
  `testing/go/view_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestRenameMaterializedViewUsedByViewKeepsViewUsableRepro -count=1`.
- Expected PostgreSQL behavior: renaming a materialized view preserves
  dependent views, which continue to resolve the renamed relation by dependency
  identity.
- Observed Doltgres behavior: the rename succeeds, but querying the dependent
  view fails because the stored view definition still references the old
  materialized-view name.

### nextval in WHERE predicates is not evaluated per row

- Reproducer: `TestNextvalInWherePredicateEvaluatesPerRowRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestNextvalInWherePredicateEvaluatesPerRowRepro -count=1`.
- Expected PostgreSQL behavior: `nextval('volatile_filter_seq')` in a `WHERE`
  predicate is volatile and advances for each candidate row, so rows with
  markers `1`, `2`, and `3` match as the sequence advances.
- Observed Doltgres behavior: the filter returns only the first matching row,
  and the following `nextval` call returns `2`, showing that the predicate call
  advanced the sequence only once instead of once per row.

### nextval ignores later search_path schemas

- Reproducer: `TestNextvalUsesSecondSearchPathSchemaRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestNextvalUsesSecondSearchPathSchemaRepro -count=1`.
- Expected PostgreSQL behavior: unqualified sequence names resolve through each
  `search_path` schema in order, so with `search_path` set to
  `first_sequence_path_schema, second_sequence_path_schema, public`,
  `nextval('search_path_later_seq')` finds
  `second_sequence_path_schema.search_path_later_seq`.
- Observed Doltgres behavior: unqualified `nextval('search_path_later_seq')`
  only checks the current schema and fails with `relation
  "search_path_later_seq" does not exist`, even though the sequence exists in
  the next search-path schema.

### setval ignores later search_path schemas

- Reproducer: `TestSetvalUsesSecondSearchPathSchemaRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSetvalUsesSecondSearchPathSchemaRepro -count=1`.
- Expected PostgreSQL behavior: unqualified sequence names in `setval` resolve
  through each `search_path` schema in order, so `setval('search_path_setval_seq',
  90)` mutates `second_setval_path_schema.search_path_setval_seq` when it is in
  the second search-path schema.
- Observed Doltgres behavior: `setval('search_path_setval_seq', 90)` only
  checks the current schema and fails with `relation "search_path_setval_seq"
  does not exist`; the qualified sequence is not reseeded and the following
  qualified `nextval` returns its original start value `70`.

### nextval misparses quoted sequence names containing dots

- Reproducer: `TestNextvalHandlesQuotedSequenceNamesWithDotsRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestNextvalHandlesQuotedSequenceNamesWithDotsRepro -count=1`.
- Expected PostgreSQL behavior: dots inside double-quoted identifiers are part
  of the identifier, so `nextval('"quoted.sequence.name"')` and
  `nextval('public."quoted.sequence.name"')` resolve the sequence named
  `quoted.sequence.name`.
- Observed Doltgres behavior: `nextval('"quoted.sequence.name"')` is split on
  the quoted dots and fails with `relation "name" does not exist`;
  `nextval('public."quoted.sequence.name"')` fails with `cannot parse
  relation: public."quoted.sequence.name"`.

### setval misparses quoted sequence names containing dots

- Reproducer: `TestSetvalHandlesQuotedSequenceNamesWithDotsRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSetvalHandlesQuotedSequenceNamesWithDotsRepro -count=1`.
- Expected PostgreSQL behavior: dots inside double-quoted identifiers are part
  of the identifier, so `setval('"setval.quoted.sequence"', 42)` and
  `setval('public."setval.quoted.sequence"', 50)` resolve and mutate the
  sequence named `setval.quoted.sequence`.
- Observed Doltgres behavior: `setval('"setval.quoted.sequence"', 42)` is
  split on the quoted dots and fails with `relation "sequence" does not
  exist`; the schema-qualified form fails with `cannot parse relation:
  public."setval.quoted.sequence"`.

### nextval sequence allocations roll back with transactions

- Reproducer: `TestNextvalIsNotRolledBackRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestNextvalIsNotRolledBackRepro -count=1`.
- Expected PostgreSQL behavior: `nextval` sequence allocation is
  non-transactional; after `BEGIN; SELECT nextval(...); ROLLBACK`, the next
  `nextval` call returns the following sequence value.
- Observed Doltgres behavior: after rolling back the transaction, the next
  `nextval` call returns the same value again, reusing a value already handed
  out inside the rolled-back transaction.

### setval sequence changes roll back with transactions

- Reproducer: `TestSetvalIsNotRolledBackRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestSetvalIsNotRolledBackRepro -count=1`.
- Expected PostgreSQL behavior: sequence state changes made by `setval` are
  non-transactional; after `BEGIN; SELECT setval(..., 50); ROLLBACK`, the next
  `nextval` call returns `51`.
- Observed Doltgres behavior: rolling back the transaction restores the
  sequence to its initial state, so the next `nextval` call returns `1`.

### nextval sequence allocations roll back to savepoints

- Reproducer: `TestNextvalIsNotRolledBackToSavepointRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestNextvalIsNotRolledBackToSavepointRepro -count=1`.
- Expected PostgreSQL behavior: `nextval` sequence allocation is not rolled
  back by `ROLLBACK TO SAVEPOINT`; after values `1` and `2` are allocated,
  rolling back to the savepoint still leaves the next call at `3`.
- Observed Doltgres behavior: rolling back to the savepoint rewinds the
  sequence allocation, so the next `nextval` call returns `2` again.

### setval sequence changes roll back to savepoints

- Reproducer: `TestSetvalIsNotRolledBackToSavepointRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestSetvalIsNotRolledBackToSavepointRepro -count=1`.
- Expected PostgreSQL behavior: `setval` sequence state changes are not rolled
  back by `ROLLBACK TO SAVEPOINT`; after `setval(..., 50)`, the next `nextval`
  call returns `51`.
- Observed Doltgres behavior: rolling back to the savepoint restores the
  sequence to its initial state, so the next `nextval` call returns `1`.

### currval is not implemented for session sequence state

- Reproducer: `TestCurrvalReturnsSessionSequenceValueRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCurrvalReturnsSessionSequenceValueRepro -count=1`.
- Expected PostgreSQL behavior: after a session calls
  `nextval('currval_session_seq')`, `currval('currval_session_seq')` returns
  the latest value obtained for that sequence in the same session.
- Observed Doltgres behavior: `currval('currval_session_seq')` fails with
  `function: 'currval' not found`.

### lastval is not implemented for session sequence state

- Reproducer: `TestLastvalReturnsLatestSessionSequenceValueRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestLastvalReturnsLatestSessionSequenceValueRepro -count=1`.
- Expected PostgreSQL behavior: after a session obtains values from multiple
  sequences, `lastval()` returns the most recent value obtained by `nextval`
  in that session.
- Observed Doltgres behavior: `lastval()` fails with `function: 'lastval' not
  found`.

### nextval allocations are not visible across concurrent transactions

- Reproducer: `TestNextvalIsVisibleAcrossTransactionsRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestNextvalIsVisibleAcrossTransactionsRepro -count=1`.
- Expected PostgreSQL behavior: a sequence value allocated by `nextval` is
  immediately visible outside the allocating transaction, so a second session
  calling `nextval` receives the next value even before the first transaction
  commits.
- Observed Doltgres behavior: while the first transaction remains open after
  receiving `1`, a second session also receives `1`, so concurrent callers can
  be handed duplicate sequence values.

### DEFAULT nextval can persist duplicate ids across concurrent transactions

- Reproducer: `TestDefaultNextvalIsVisibleAcrossTransactionsRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDefaultNextvalIsVisibleAcrossTransactionsRepro -count=1`.
- Expected PostgreSQL behavior: concurrent transactions inserting rows whose
  `id` default calls `nextval` receive distinct generated ids, because sequence
  allocation is global rather than transaction-local.
- Observed Doltgres behavior: two concurrent transactions both insert and
  commit rows with generated id `1`, leaving duplicate generated ids persisted
  in the table.

### Identity columns can persist duplicate values across concurrent transactions

- Reproducer: `TestIdentityValuesAreVisibleAcrossTransactionsRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestIdentityValuesAreVisibleAcrossTransactionsRepro -count=1`.
- Expected PostgreSQL behavior: concurrent transactions inserting into a
  `GENERATED BY DEFAULT AS IDENTITY` column receive distinct generated values.
- Observed Doltgres behavior: two concurrent transactions both insert and
  commit rows with identity value `1`, leaving duplicate generated identity
  values persisted in the table.

### Identity sequence options are rejected

- Reproducer: `TestIdentitySequenceOptionsAffectGeneratedValuesRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestIdentitySequenceOptionsAffectGeneratedValuesRepro -count=1`.
- Expected PostgreSQL behavior: identity sequence options such as `START WITH
  100 INCREMENT BY 2` are accepted and determine generated identity values.
- Observed Doltgres behavior: table creation fails with `sequence options are
  not yet supported, create a sequence separately`, so valid identity schemas
  cannot be created and persisted generated values cannot follow the requested
  sequence.

### ADD COLUMN identity leaves generated values NULL

- Reproducer: `TestAddIdentityColumnBackfillsEachExistingRowRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAddIdentityColumnBackfillsEachExistingRowRepro -count=1`.
- Expected PostgreSQL behavior: adding `GENERATED BY DEFAULT AS IDENTITY` to a
  populated table assigns distinct generated values to existing rows, and later
  inserts that omit the identity column receive the next sequence value.
- Observed Doltgres behavior: the `ALTER TABLE ... ADD COLUMN ... AS IDENTITY`
  statement succeeds, but existing rows and a later inserted row all store
  `NULL` in the identity column instead of generated values.

### Identity columns can be nullable and store NULL values

- Reproducers: `TestIdentityColumnRejectsExplicitNullabilityRepro` and
  `TestIdentityColumnIsImplicitlyNotNullRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestIdentityColumn(RejectsExplicitNullability|IsImplicitlyNotNull)Repro'
  -count=1`.
- Expected PostgreSQL behavior: identity columns are always `NOT NULL`;
  explicit `NULL` declarations such as `GENERATED ALWAYS AS IDENTITY NULL` are
  rejected, and inserting `NULL` into an identity column fails even when the
  table definition does not spell out `NOT NULL`.
- Observed Doltgres behavior: `GENERATED ALWAYS AS IDENTITY NULL` is accepted,
  and a `GENERATED BY DEFAULT AS IDENTITY` column accepts and persists an
  explicit `NULL` value.

### Identity INSERT OVERRIDING clauses are rejected

- Reproducers: `TestGeneratedAlwaysIdentityOverridingSystemValueRepro`,
  `TestGeneratedByDefaultIdentityOverridingUserValueRepro`,
  `TestGeneratedAlwaysIdentityOverridingUserValueRepro`, and
  `TestGeneratedByDefaultIdentityOverridingSystemValueRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'TestGenerated(AlwaysIdentityOverriding(SystemValue|UserValue)|ByDefaultIdentityOverriding(UserValue|SystemValue))Repro'
  -count=1`.
- Expected PostgreSQL behavior: `OVERRIDING SYSTEM VALUE` allows explicit
  values for both `GENERATED ALWAYS` and `GENERATED BY DEFAULT` identity
  inserts, and `OVERRIDING USER VALUE` ignores caller-supplied identity values
  and uses the generated value.
- Observed Doltgres behavior: all four valid `INSERT ... OVERRIDING ... VALUE`
  forms fail during parsing with `at or near "overriding": syntax error`.

### ALTER COLUMN identity operations are rejected

- Reproducers: `TestAlterColumnDropIdentityRepro`,
  `TestAlterColumnSetGeneratedIdentityModeRepro`, and
  `TestAlterColumnIdentitySequenceOptionsRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestAlterColumn(DropIdentity|SetGeneratedIdentityMode|IdentitySequenceOptions)Repro'
  -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... ALTER COLUMN ... DROP
  IDENTITY` removes generation while preserving the column, `SET GENERATED BY
  DEFAULT` changes an identity column from `ALWAYS` to caller-supplied values,
  and identity sequence options such as `SET INCREMENT BY 2 SET START WITH 100
  RESTART` affect future generated values.
- Observed Doltgres behavior: `DROP IDENTITY` fails with `ALTER TABLE with
  unsupported command type *tree.AlterTableDropExprIden`, `SET GENERATED BY
  DEFAULT` fails with `ALTER TABLE variant is not yet supported`, and changing
  identity sequence options fails during parsing with `at or near ";": syntax
  error`.

### TRUNCATE RESTART IDENTITY is rejected

- Reproducer: `TestTruncateRestartIdentityResetsOwnedSequenceRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTruncateRestartIdentityResetsOwnedSequenceRepro -count=1`.
- Expected PostgreSQL behavior: `TRUNCATE ... RESTART IDENTITY` truncates the
  table and resets sequences owned by the truncated table, so the next inserted
  `SERIAL` value starts again at `1`.
- Observed Doltgres behavior: parsing fails at `restart`, leaving the original
  rows in place and allowing the next insert to continue the old sequence.

### setval changes are not visible across concurrent transactions

- Reproducer: `TestSetvalIsVisibleAcrossTransactionsRepro` in
  `testing/go/sequence_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestSetvalIsVisibleAcrossTransactionsRepro -count=1`.
- Expected PostgreSQL behavior: sequence state changed by `setval` is
  immediately visible outside the reseeding transaction, so another session
  calling `nextval` after `setval(..., 50)` receives `51`.
- Observed Doltgres behavior: while the `setval` transaction remains open,
  another session still receives `1`, so sequence reseeding is isolated like
  ordinary transactional data.

### ALTER SEQUENCE option changes are rejected

- Reproducer: `TestAlterSequenceOptionsAffectNextvalRepro` in
  `testing/go/sequence_alter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterSequenceOptionsAffectNextvalRepro -count=1`.
- Expected PostgreSQL behavior: ordinary `ALTER SEQUENCE` option changes such
  as `RESTART WITH 24`, `INCREMENT BY 4`, `MAXVALUE 36`, and `CYCLE` are
  accepted and affect later `nextval` calls, yielding `24`, `28`, `32`, `36`,
  and then cycling to `1`.
- Observed Doltgres behavior: `ALTER SEQUENCE ... RESTART WITH ...` fails with
  `RESTART is not yet supported`, so later `nextval` calls continue from the
  original sequence state instead of the requested restart, increment, bound,
  and cycle settings.

### ALTER SEQUENCE IF EXISTS rejects options before no-oping missing sequences

- Reproducer: `TestAlterSequenceIfExistsMissingWithOptionsNoopsRepro` in
  `testing/go/sequence_alter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterSequenceIfExistsMissingWithOptionsNoopsRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER SEQUENCE IF EXISTS missing_seq ...`
  no-ops when the target sequence does not exist, even when the statement
  includes otherwise valid sequence options such as `RESTART`, `INCREMENT`, and
  `CYCLE`.
- Observed Doltgres behavior: the missing-sequence statement is rejected during
  AST conversion with `RESTART is not yet supported` before the `IF EXISTS`
  no-op can apply.

### pg_get_constraintdef includes CHECK constraint names

- Reproducer: `TestPgGetConstraintdefCheckOmitsConstraintNameRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgGetConstraintdefCheckOmitsConstraintNameRepro -count=1`.
- Expected PostgreSQL behavior: `pg_get_constraintdef` returns the constraint
  definition body, so a named CHECK constraint deparses as
  `CHECK ((amount > 0))` without the constraint name.
- Observed Doltgres behavior: the deparsed CHECK definition is
  `amount_positive CHECK "amount" > 0 ENFORCED`, including the constraint name
  and an `ENFORCED` suffix instead of PostgreSQL's `pg_get_constraintdef` text.

### pg_get_constraintdef omits foreign-key actions

- Reproducer: `TestPgGetConstraintdefForeignKeyActionsRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgGetConstraintdefForeignKeyActionsRepro -count=1`.
- Expected PostgreSQL behavior: `pg_get_constraintdef` preserves referential
  actions, so a foreign key declared `ON UPDATE CASCADE ON DELETE SET NULL`
  deparses with those actions.
- Observed Doltgres behavior: the deparsed foreign key omits both actions and
  returns only `FOREIGN KEY (parent_id) REFERENCES constraintdef_fk_parent(id)`,
  which would reconstruct different referential behavior.

### pg_get_constraintdef does not quote constrained column names

- Reproducer: `TestPgGetConstraintdefQuotesColumnNamesRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgGetConstraintdefQuotesColumnNamesRepro -count=1`.
- Expected PostgreSQL behavior: constrained column names that require quoting
  are quoted in the deparsed definition, e.g. `PRIMARY KEY ("CaseColumn")`.
- Observed Doltgres behavior: the deparsed primary key emits
  `PRIMARY KEY (CaseColumn)` without quotes, so the definition no longer
  round-trips to the original mixed-case column.

### pg_get_indexdef does not quote identifiers that require quoting

- Reproducer: `TestPgGetIndexdefQuotesIdentifiersRepro` in
  `testing/go/index_metadata_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgGetIndexdefQuotesIdentifiersRepro -count=1`.
- Expected PostgreSQL behavior: `pg_get_indexdef` quotes index, table, and
  column identifiers that require quoting, e.g.
  `CREATE INDEX "IndexQuoteIdx" ON public."IndexQuoteItems" USING btree
  ("CaseColumn")`, and the single-attribute form returns `"CaseColumn"`.
- Observed Doltgres behavior: the deparsed index definition and attribute omit
  the required quotes, so the output references folded identifiers rather than
  the original mixed-case index, table, and column names.

### DEFERRABLE UNIQUE constraints are enforced immediately

- Reproducer: `TestDeferrableUniqueConstraintCanBeFixedBeforeCommitRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDeferrableUniqueConstraintCanBeFixedBeforeCommitRepro -count=1`.
- Expected PostgreSQL behavior: `UNIQUE DEFERRABLE INITIALLY DEFERRED` allows a
  transaction to introduce a temporary duplicate and repair it before `COMMIT`.
- Observed Doltgres behavior: the duplicate insert fails immediately, even
  though the unique constraint accepted `DEFERRABLE INITIALLY DEFERRED`.

### UNIQUE constraints with INCLUDE columns are rejected

- Reproducer: `TestUniqueConstraintIncludeColumnsRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestUniqueConstraintIncludeColumnsRepro -count=1`.
- Expected PostgreSQL behavior: `UNIQUE (code) INCLUDE (label)` in `CREATE
  TABLE` is accepted and creates a unique btree index whose uniqueness key is
  `code` and whose covering payload column is `label`.
- Observed Doltgres behavior: the table definition fails with `include columns
  is not yet supported`, so valid covering unique constraints cannot be restored
  or used.

### UNIQUE constraint storage parameters are rejected

- Reproducer: `TestUniqueConstraintStorageParamsRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestUniqueConstraintStorageParamsRepro -count=1`.
- Expected PostgreSQL behavior: `UNIQUE (code) WITH (fillfactor=70)` in
  `CREATE TABLE` is accepted and persists the backing unique index reloption in
  `pg_catalog.pg_indexes`.
- Observed Doltgres behavior: the table definition fails with `storage
  parameters are not yet supported for indexes`, so valid unique-constraint
  index storage options cannot be restored or used.

### UNIQUE constraints with USING INDEX TABLESPACE pg_default are rejected

- Reproducer: `TestUniqueConstraintDefaultTablespaceRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestUniqueConstraintDefaultTablespaceRepro -count=1`.
- Expected PostgreSQL behavior: `UNIQUE (code) USING INDEX TABLESPACE
  pg_default` in `CREATE TABLE` is accepted, creates the backing unique index in
  the default tablespace, and leaves the table usable.
- Observed Doltgres behavior: the table definition fails with `TABLESPACE is not
  yet supported for indexes`, so valid unique constraints that explicitly spell
  the default index tablespace cannot be restored or used.

### PRIMARY KEY constraints with INCLUDE columns are rejected

- Reproducer: `TestPrimaryKeyConstraintIncludeColumnsRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPrimaryKeyConstraintIncludeColumnsRepro -count=1`.
- Expected PostgreSQL behavior: `PRIMARY KEY (id) INCLUDE (label)` in `CREATE
  TABLE` is accepted and creates a unique btree index whose primary-key column
  is `id` and whose covering payload column is `label`.
- Observed Doltgres behavior: the table definition fails with `include columns
  is not yet supported`, so valid covering primary-key constraints cannot be
  restored or used.

### PRIMARY KEY constraint storage parameters are rejected

- Reproducer: `TestPrimaryKeyConstraintStorageParamsRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPrimaryKeyConstraintStorageParamsRepro -count=1`.
- Expected PostgreSQL behavior: `PRIMARY KEY (id) WITH (fillfactor=70)` in
  `CREATE TABLE` is accepted and persists the backing primary-key index
  reloption in `pg_catalog.pg_indexes`.
- Observed Doltgres behavior: the table definition fails with `storage
  parameters are not yet supported for indexes`, so valid primary-key index
  storage options cannot be restored or used.

### PRIMARY KEY constraints with USING INDEX TABLESPACE pg_default are rejected

- Reproducer: `TestPrimaryKeyConstraintDefaultTablespaceRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPrimaryKeyConstraintDefaultTablespaceRepro -count=1`.
- Expected PostgreSQL behavior: `PRIMARY KEY (id) USING INDEX TABLESPACE
  pg_default` in `CREATE TABLE` is accepted, creates the backing primary-key
  index in the default tablespace, and leaves the table usable.
- Observed Doltgres behavior: the table definition fails with `TABLESPACE is not
  yet supported for indexes`, so valid primary-key constraints that explicitly
  spell the default index tablespace cannot be restored or used.

### CREATE TABLE OF UNIQUE constraints with INCLUDE columns are rejected

- Reproducer: `TestTypedTableUniqueConstraintIncludeColumnsRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTypedTableUniqueConstraintIncludeColumnsRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE TABLE name OF composite_type (UNIQUE
  (code) INCLUDE (id))` is accepted and creates a typed table with a covering
  unique index.
- Observed Doltgres behavior: the typed-table definition fails with `CREATE
  TABLE OF unique constraint INCLUDE columns are not yet supported`, so valid
  typed tables with covering unique constraints cannot be restored or used.

### CREATE TABLE OF PRIMARY KEY constraints with INCLUDE columns are rejected

- Reproducer: `TestTypedTablePrimaryKeyConstraintIncludeColumnsRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTypedTablePrimaryKeyConstraintIncludeColumnsRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE TABLE name OF composite_type (PRIMARY
  KEY (id) INCLUDE (label))` is accepted and creates a typed table with a
  covering primary-key index.
- Observed Doltgres behavior: the typed-table definition fails with `CREATE
  TABLE OF primary key INCLUDE columns are not yet supported`, so valid typed
  tables with covering primary-key constraints cannot be restored or used.

### CREATE TABLE OF UNIQUE constraint storage parameters are rejected

- Reproducer: `TestTypedTableUniqueConstraintStorageParamsRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTypedTableUniqueConstraintStorageParamsRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE TABLE name OF composite_type (UNIQUE
  (code) WITH (fillfactor=70))` is accepted and persists the typed table's
  backing unique index storage parameter in `pg_catalog.pg_indexes`.
- Observed Doltgres behavior: the typed-table definition fails with `STORAGE
  parameters not yet supported for indexes`, so valid typed tables with unique
  index reloptions cannot be restored or used.

### CREATE TABLE OF PRIMARY KEY constraint storage parameters are rejected

- Reproducer: `TestTypedTablePrimaryKeyConstraintStorageParamsRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTypedTablePrimaryKeyConstraintStorageParamsRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE TABLE name OF composite_type (PRIMARY
  KEY (id) WITH (fillfactor=70))` is accepted and persists the typed table's
  backing primary-key index storage parameter in `pg_catalog.pg_indexes`.
- Observed Doltgres behavior: the typed-table definition fails with `STORAGE
  parameters not yet supported for indexes`, so valid typed tables with
  primary-key index reloptions cannot be restored or used.

### CREATE TABLE OF UNIQUE constraints with USING INDEX TABLESPACE pg_default are rejected

- Reproducer: `TestTypedTableUniqueConstraintDefaultTablespaceRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTypedTableUniqueConstraintDefaultTablespaceRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE TABLE name OF composite_type (UNIQUE
  (code) USING INDEX TABLESPACE pg_default)` is accepted, creates the typed
  table's backing unique index in the default tablespace, and leaves the table
  usable.
- Observed Doltgres behavior: the typed-table definition fails with `TABLESPACE
  is not yet supported`, so valid typed tables that explicitly spell the default
  unique-index tablespace cannot be restored or used.

### CREATE TABLE OF PRIMARY KEY constraints with USING INDEX TABLESPACE pg_default are rejected

- Reproducer: `TestTypedTablePrimaryKeyConstraintDefaultTablespaceRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTypedTablePrimaryKeyConstraintDefaultTablespaceRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE TABLE name OF composite_type (PRIMARY
  KEY (id) USING INDEX TABLESPACE pg_default)` is accepted, creates the typed
  table's backing primary-key index in the default tablespace, and leaves the
  table usable.
- Observed Doltgres behavior: the typed-table definition fails with `TABLESPACE
  is not yet supported`, so valid typed tables that explicitly spell the default
  primary-key index tablespace cannot be restored or used.

### DEFERRABLE PRIMARY KEY constraints are enforced immediately

- Reproducer:
  `TestDeferrablePrimaryKeyConstraintCanBeFixedBeforeCommitRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run
  TestDeferrablePrimaryKeyConstraintCanBeFixedBeforeCommitRepro -count=1`.
- Expected PostgreSQL behavior: `PRIMARY KEY DEFERRABLE INITIALLY DEFERRED`
  allows a transaction to introduce a temporary duplicate primary key and
  repair it before `COMMIT`.
- Observed Doltgres behavior: the duplicate insert fails immediately with
  `duplicate primary key given: [1]`, even though the primary-key constraint
  accepted `DEFERRABLE INITIALLY DEFERRED`.

### SET CONSTRAINTS does not defer DEFERRABLE UNIQUE constraints

- Reproducer: `TestSetConstraintsDefersUniqueConstraintRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestSetConstraintsDefersUniqueConstraintRepro -count=1`.
- Expected PostgreSQL behavior: after `SET CONSTRAINTS ALL DEFERRED`, a
  `UNIQUE DEFERRABLE INITIALLY IMMEDIATE` constraint is checked at commit time,
  allowing a temporary duplicate to be repaired before `COMMIT`.
- Observed Doltgres behavior: the duplicate insert fails immediately even after
  the transaction switches constraints to deferred mode.

### SET CONSTRAINTS accepts non-deferrable constraints

- Reproducer: `TestSetConstraintsRejectsNonDeferrableConstraintRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestSetConstraintsRejectsNonDeferrableConstraintRepro -count=1`.
- Expected PostgreSQL behavior: `SET CONSTRAINTS
  set_constraints_nondeferrable_code_key DEFERRED` fails because the named
  ordinary unique constraint is not deferrable.
- Observed Doltgres behavior: the statement succeeds and records no error.

### ALTER CONSTRAINT cannot change foreign-key deferrability

- Reproducer: `TestAlterForeignKeyConstraintDeferrabilityRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterForeignKeyConstraintDeferrabilityRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... ALTER CONSTRAINT` can change
  a foreign key to `DEFERRABLE INITIALLY DEFERRED`, after which temporary
  violations can be repaired before commit.
- Observed Doltgres behavior: the alter statement fails with
  `ALTER TABLE with unsupported command type *tree.AlterTableAlterConstraint`,
  so valid migrations cannot change foreign-key timing.

### Multiple column-level CHECK constraints are rejected

- Reproducer: `TestMultipleColumnCheckConstraintsAreEnforcedRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestMultipleColumnCheckConstraintsAreEnforcedRepro -count=1`.
- Expected PostgreSQL behavior: a single column may declare multiple `CHECK`
  constraints, and every stored constraint is enforced for subsequent writes.
- Observed Doltgres behavior: table creation fails with
  `column-declared multiple CHECK expressions are not yet supported`, rejecting
  a valid schema shape before either constraint can be enforced.

### ALTER TABLE ADD CONSTRAINT rejects CHECK constraints with NO INHERIT

- Reproducer: `TestAlterTableAddCheckConstraintNoInheritRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterTableAddCheckConstraintNoInheritRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... ADD CONSTRAINT ... CHECK (...)
  NO INHERIT` is accepted, stores `pg_constraint.connoinherit = true`, and
  enforces the check for writes to the table itself.
- Observed Doltgres behavior: the alter statement fails with `NO INHERIT is not
  yet supported for check constraints`, so valid migrations using explicit
  check-inheritance semantics cannot be restored or applied.

### CREATE TABLE OF rejects CHECK constraints with NO INHERIT

- Reproducer: `TestTypedTableCheckConstraintNoInheritRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTypedTableCheckConstraintNoInheritRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE TABLE name OF composite_type (CONSTRAINT
  ... CHECK (...) NO INHERIT)` is accepted, stores
  `pg_constraint.connoinherit = true`, and enforces the check for writes to the
  typed table itself.
- Observed Doltgres behavior: the typed-table definition fails with `NO INHERIT
  is not yet supported for check constraints`, so valid typed-table schemas with
  explicit check-inheritance semantics cannot be restored or used.

### CHECK constraints reject immutable function calls

- Reproducer: `TestCheckConstraintAcceptsImmutableFunctionRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCheckConstraintAcceptsImmutableFunctionRepro -count=1`.
- Expected PostgreSQL behavior: a `CHECK` constraint may call an immutable SQL
  function, accepts rows for which the function returns true, and rejects rows
  for which it returns false.
- Observed Doltgres behavior: table creation fails with `Invalid constraint
  expression, function not supported: check_function_positive(check_function_items.amount)`,
  so valid CHECK-based invariants cannot be declared.

### CHECK constraints accept set-returning functions

- Reproducer: `TestCheckConstraintsRejectNonScalarExpressionsRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCheckConstraintsRejectNonScalarExpressionsRepro -count=1`.
- Expected PostgreSQL behavior: `CHECK (generate_series(1, 2) > 0)` is rejected
  because set-returning functions are not allowed in check constraints.
- Observed Doltgres behavior: the table definition succeeds, allowing a
  PostgreSQL-invalid non-scalar expression to be persisted as a row-integrity
  constraint.

### ADD COLUMN NOT NULL synthesizes values for existing rows

- Reproducer: `TestAddNotNullColumnValidatesExistingRowsRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestAddNotNullColumnValidatesExistingRowsRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... ADD COLUMN required_value INT
  NOT NULL` on a non-empty table without a default fails because existing rows
  would contain nulls in the new constrained column.
- Observed Doltgres behavior: the alter statement succeeds and the pre-existing
  row is rewritten with synthesized `required_value = 0`, introducing data the
  schema never declared as a default.

### NOT VALID CHECK constraints are rejected and cannot be validated

- Reproducer: `TestNotValidCheckConstraintEnforcesNewRowsRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestNotValidCheckConstraintEnforcesNewRowsRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... ADD CONSTRAINT ... NOT VALID`
  succeeds without scanning existing rows, enforces the new `CHECK` constraint
  for future writes, and `VALIDATE CONSTRAINT` later fails until pre-existing
  violations are repaired.
- Observed Doltgres behavior: adding the `NOT VALID` check fails with
  `NOT VALID is not supported yet`, and `VALIDATE CONSTRAINT` fails with
  `ALTER TABLE with unsupported command type *tree.AlterTableValidateConstraint`.

### NOT VALID foreign keys are rejected and cannot be validated

- Reproducer: `TestNotValidForeignKeyConstraintEnforcesNewRowsRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestNotValidForeignKeyConstraintEnforcesNewRowsRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... ADD CONSTRAINT ... FOREIGN
  KEY ... NOT VALID` succeeds without scanning existing rows, enforces the new
  foreign key for future writes, and `VALIDATE CONSTRAINT` later succeeds after
  pre-existing violations are repaired.
- Observed Doltgres behavior: adding the `NOT VALID` foreign key fails with
  `NOT VALID is not supported yet`, and `VALIDATE CONSTRAINT` fails with
  `ALTER TABLE with unsupported command type *tree.AlterTableValidateConstraint`.

### EXCLUDE constraints are rejected before enforcing conflicts

- Reproducer: `TestExclusionConstraintRejectsConflictingRowsRepro` in
  `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestExclusionConstraintRejectsConflictingRowsRepro
  -count=1`.
- Expected PostgreSQL behavior: `EXCLUDE USING btree (resource_id WITH =)` is
  accepted, distinct values insert successfully, and conflicting equal values
  are rejected by the exclusion constraint.
- Observed Doltgres behavior: table creation fails with `ERROR: unknown table
  definition encountered (SQLSTATE XX000)`, so valid exclusion constraints
  cannot protect conflicting rows.

### ALTER DOMAIN ADD CONSTRAINT is rejected before validating existing rows

- Reproducer: `TestAlterDomainAddConstraintValidatesExistingRowsRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestAlterDomainAddConstraintValidatesExistingRowsRepro
  -count=1`.
- Expected PostgreSQL behavior: `ALTER DOMAIN ... ADD CONSTRAINT ... CHECK`
  validates existing columns that use the domain before accepting the new
  domain invariant.
- Observed Doltgres behavior: the statement fails with `ERROR: ALTER DOMAIN is
  not yet supported Please file an issue at
  https://github.com/dolthub/doltgresql/issues (SQLSTATE 0A000)`, so existing
  domains cannot be tightened with validated constraints.

### Domain CHECK constraints accept non-scalar expressions

- Reproducer: `TestDomainCheckRejectsNonScalarExpressionsRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDomainCheckRejectsNonScalarExpressionsRepro -count=1`.
- Expected PostgreSQL behavior: domain `CHECK` constraints reject subqueries,
  aggregate functions, window functions, and set-returning functions.
- Observed Doltgres behavior: `CREATE DOMAIN ... CHECK` accepts all four
  expression classes, allowing PostgreSQL-invalid non-row-local domain
  invariants to be persisted and reused by columns.

### Domain values cannot cast back to their base type

- Reproducer: `TestDomainValueCastsToBaseTypeRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDomainValueCastsToBaseTypeRepro -count=1`.
- Expected PostgreSQL behavior: a value cast to a domain can be explicitly cast
  back to that domain's base type, such as
  `7::base_cast_domain::integer`.
- Observed Doltgres behavior: the second cast fails with `EXPLICIT CAST: cast
  from base_cast_domain to integer does not exist`.

### Domain base-type typmods are not applied to stored values

- Reproducers: `TestTemporalDomainTypmodsRoundStoredValuesRepro` and
  `TestNumericDomainTypmodsRoundStoredValuesRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'Test(Temporal|Numeric)DomainTypmodsRoundStoredValuesRepro' -count=1`.
- Expected PostgreSQL behavior: a domain declared as `timestamp(0)` rounds
  assigned timestamp values to whole seconds, and a domain declared as
  `interval day to second(0)` rounds assigned interval values to whole seconds
  while preserving only the allowed interval fields. A domain declared as
  `numeric(5,2)` rounds assigned values to two fractional digits and rejects
  values such as `999.995` that overflow after rounding.
- Observed Doltgres behavior: domain-typed columns store
  `2021-09-15 21:43:56.789`, `3 days 04:05:06.789`, and `123.456` unchanged,
  and accept `999.995`, so typmods on a domain's base type are not enforced
  during column assignment.

### TIMETZ domain typmods are not applied to stored or cast values

- Reproducer: `TestTimetzDomainTypmodsRoundValuesRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimetzDomainTypmodsRoundValuesRepro -count=1`.
- Expected PostgreSQL behavior: a domain declared as `TIMETZ(0)` rounds
  assigned and explicitly cast timetz values to whole seconds, so
  `21:43:56.789+00` becomes `21:43:57+00`.
- Observed Doltgres behavior: both the stored domain value and the explicit
  domain cast return `21:43:56.789+00`, so timetz domain base-type typmods are
  not enforced before values reach storage or expression results.

### Domain typmod casts return uncoerced values

- Reproducer: `TestDomainTypmodCastsUseCoercedValueRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDomainTypmodCastsUseCoercedValueRepro -count=1`.
- Expected PostgreSQL behavior: explicit casts to a domain apply the typmod on
  the domain's base type. Casting `123.456` to a `numeric(5,2)` domain returns
  `123.46`, and casting timestamp `2021-09-15 21:43:56.789` to a
  `timestamp(0)` domain returns `2021-09-15 21:43:57`.
- Observed Doltgres behavior: both casts return the raw input values
  `123.456` and `2021-09-15 21:43:56.789`, so domain casts do not enforce
  base-type typmods even before values reach table storage.

### Domain typmod SQL function returns are uncoerced

- Reproducer: `TestDomainTypmodSqlFunctionReturnUsesCoercedValueRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDomainTypmodSqlFunctionReturnUsesCoercedValueRepro -count=1`.
- Expected PostgreSQL behavior: SQL functions declared to return a domain apply
  the typmod on the domain's base type before returning the value. A function
  returning a `numeric(5,2)` domain returns `123.46` for `123.456`, and a
  function returning a `timestamp(0)` domain returns `2021-09-15 21:43:57` for
  `2021-09-15 21:43:56.789`.
- Observed Doltgres behavior: both functions return the raw values `123.456`
  and `2021-09-15 21:43:56.789`, so SQL function return coercion bypasses
  domain base-type typmods.

### Domain typmod UNIQUE constraints use uncoerced values

- Reproducer: `TestDomainTypmodUniqueUsesCoercedValuesRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDomainTypmodUniqueUsesCoercedValuesRepro -count=1`.
- Expected PostgreSQL behavior: unique constraints over domain-typed columns are
  enforced after applying the domain base-type typmod. Two `numeric(5,2)`
  domain values that both round to `1.23` conflict, and two `timestamp(0)`
  domain values that both round to `2021-09-15 21:43:57` conflict.
- Observed Doltgres behavior: both pairs are accepted and stored with their
  original fractional precision, so unique keys over typmod-constrained domains
  can contain duplicate PostgreSQL-visible values.

### Domain typmod generated columns use uncoerced values

- Reproducer: `TestDomainTypmodGeneratedColumnUsesCoercedValueRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDomainTypmodGeneratedColumnUsesCoercedValueRepro -count=1`.
- Expected PostgreSQL behavior: a stored generated column over a
  `numeric(5,2)` domain value is computed from the coerced stored value, so
  inserting `123.456` stores `123.46` and the generated `amount::text` value is
  also `123.46`.
- Observed Doltgres behavior: both the domain column and the generated column
  keep `123.456`, so stored generated data can be derived from uncoerced domain
  values.

### Domain typmod defaults store uncoerced values

- Reproducer: `TestDomainTypmodDefaultUsesCoercedValueRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDomainTypmodDefaultUsesCoercedValueRepro -count=1`.
- Expected PostgreSQL behavior: a default declared on a `numeric(5,2)` domain is
  coerced through the domain base-type typmod before storage, so the default
  `123.456` stores as `123.46`.
- Observed Doltgres behavior: inserting a row that uses the domain default
  stores `123.456`, so implicit writes through domain defaults can persist
  values outside the domain base type's declared typmod.

### Domain defaults that call functions panic during insert

- Reproducer: `TestDomainDefaultFunctionEvaluatesOnInsertRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDomainDefaultFunctionEvaluatesOnInsertRepro -count=1`.
- Expected PostgreSQL behavior: a domain default declared as
  `DEFAULT (dg_domain_default_value())` evaluates when inserting into a
  domain-typed column, so `INSERT ... RETURNING value` returns `2`.
- Observed Doltgres behavior: the insert fails with a recovered panic,
  `column default function expressions must be enclosed in parentheses`, so
  domain defaults backed by functions cannot be applied.

### Domain typmod CHECK constraints see uncoerced values

- Reproducer: `TestDomainTypmodCheckUsesCoercedValueRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDomainTypmodCheckUsesCoercedValueRepro -count=1`.
- Expected PostgreSQL behavior: domain `CHECK` constraints are evaluated after
  the domain base-type typmod is applied. A `numeric(5,2)` domain with
  `CHECK (VALUE = 123.456)` rejects an inserted `123.456`, because the value
  visible to the check is `123.46`.
- Observed Doltgres behavior: the insert succeeds and the table contains one
  row, so domain checks can validate and persist values using the raw
  uncoerced input instead of the domain's declared base type.

### Domain typmod table CHECK constraints see uncoerced values

- Reproducer: `TestDomainTypmodTableCheckUsesCoercedValueRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDomainTypmodTableCheckUsesCoercedValueRepro -count=1`.
- Expected PostgreSQL behavior: table `CHECK` constraints over a domain-typed
  column are evaluated after the domain base-type typmod is applied. A
  `numeric(5,2)` domain column with `CHECK (amount = 123.456)` rejects inserted
  `123.456`, because the checked row value is `123.46`.
- Observed Doltgres behavior: the insert succeeds and the table contains one
  row, so table constraints can validate and persist values using raw
  uncoerced input instead of the domain's declared base type.

### Schema-qualified domain CHECK functions resolve through search_path

- Reproducer: `TestSchemaQualifiedDomainCheckFunctionUsesExplicitSchemaRepro`
  in `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSchemaQualifiedDomainCheckFunctionUsesExplicitSchemaRepro -count=1`.
- Expected PostgreSQL behavior: a domain `CHECK` declared as
  `dg_domain_lookup_b.is_valid(VALUE)` evaluates the validation function from
  the explicitly named schema; because that function returns false, inserting a
  value of the domain type is rejected.
- Observed Doltgres behavior: with `search_path` set to
  `dg_domain_lookup_a, public`, the insert succeeds and the table contains one
  row, so the domain validation runs the same-name search-path function instead
  of the schema-qualified function in the domain constraint.

### Domain typmod COPY FROM stores uncoerced values

- Reproducer: `TestDomainTypmodCopyFromUsesCoercedValueRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDomainTypmodCopyFromUsesCoercedValueRepro -count=1`.
- Expected PostgreSQL behavior: `COPY FROM STDIN` into a `numeric(5,2)` domain
  column applies the domain base-type typmod before storage, so input
  `123.456` stores as `123.46`.
- Observed Doltgres behavior: the copied row stores `123.456`, so bulk
  ingestion can persist values outside the domain base type's declared typmod.

### Domain typmod UPDATE stores uncoerced values

- Reproducer: `TestDomainTypmodUpdateUsesCoercedValueRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDomainTypmodUpdateUsesCoercedValueRepro -count=1`.
- Expected PostgreSQL behavior: `UPDATE` assignments to a `numeric(5,2)` domain
  column are coerced through the domain base-type typmod before storage, so
  updating a row to `123.456` stores `123.46`.
- Observed Doltgres behavior: the updated row stores `123.456`, so existing
  rows can be rewritten into values outside the domain base type's declared
  typmod.

### Domain typmod ON CONFLICT UPDATE stores uncoerced values

- Reproducer: `TestDomainTypmodOnConflictUpdateUsesCoercedValueRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDomainTypmodOnConflictUpdateUsesCoercedValueRepro -count=1`.
- Expected PostgreSQL behavior: `ON CONFLICT DO UPDATE` assignments to a
  `numeric(5,2)` domain column are coerced through the domain base-type typmod
  before storage, so the conflict update stores `123.46`.
- Observed Doltgres behavior: the conflict update stores `123.456`, so upsert
  writes can rewrite existing rows into values outside the domain base type's
  declared typmod.

### Domain typmod INSERT SELECT stores uncoerced values

- Reproducer: `TestDomainTypmodInsertSelectUsesCoercedValueRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDomainTypmodInsertSelectUsesCoercedValueRepro -count=1`.
- Expected PostgreSQL behavior: `INSERT ... SELECT` into a `numeric(5,2)`
  domain column coerces source values through the domain base-type typmod
  before storage, so selected `123.456` stores as `123.46`.
- Observed Doltgres behavior: the inserted row stores `123.456`, so set-based
  inserts can persist values outside the domain base type's declared typmod.

### Domain typmod UPDATE FROM stores uncoerced values

- Reproducer: `TestDomainTypmodUpdateFromUsesCoercedValueRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDomainTypmodUpdateFromUsesCoercedValueRepro -count=1`.
- Expected PostgreSQL behavior: `UPDATE ... FROM` assignments into a
  `numeric(5,2)` domain column coerce joined source values through the domain
  base-type typmod before storage, so source `123.456` stores as `123.46`.
- Observed Doltgres behavior: the updated row stores `123.456`, so joined
  updates can rewrite existing rows into values outside the domain base type's
  declared typmod.

### Array domains reject or panic on valid values

- Reproducer: `TestArrayDomainAcceptsValidValuesRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestArrayDomainAcceptsValidValuesRepro -count=1`.
- Expected PostgreSQL behavior: a domain over an array type accepts valid array
  values that satisfy the domain `CHECK`, explicit casts return the array value,
  table assignments store the value, and invalid arrays are rejected with the
  domain constraint error.
- Observed Doltgres behavior: `SELECT ARRAY[1, 2]::int_pair_domain` fails while
  scanning the valid array value with `failed to scan array element 0: invalid
  length for int4: 0`. Inserting the same valid value into a domain-typed table
  column panics with `receiveMessage recovered panic: interface conversion:
  interface {} is int32, not []interface {}`. Invalid arrays are still rejected
  with `int_pair_domain_check`.

### Arrays over domain element types panic during table creation

- Reproducer: `TestArrayOverDomainEnforcesElementConstraintsRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestArrayOverDomainEnforcesElementConstraintsRepro -count=1`.
- Expected PostgreSQL behavior: a column declared as an array of a domain type,
  such as `positive_array_element_domain[]`, can be created, valid arrays can be
  stored, and invalid array elements are rejected by the domain's element
  constraint.
- Observed Doltgres behavior: `CREATE TABLE array_over_domain_items (...,
  values_int positive_array_element_domain[])` panics during AST conversion with
  a nil-pointer dereference in `server/ast.nodeColumnTableDef`, so arrays over
  domain element types cannot be stored or validated.

### UPDATE aliases on domain-typed columns panic during constraint analysis

- Reproducers: `TestUpdateAliasDomainColumnValidAssignmentRepro`,
  `TestUpdateAliasEnforcesDomainConstraintsRepro`, and
  `TestUpdateFromEnforcesDomainConstraintsRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestUpdateAliasDomainColumnValidAssignmentRepro|TestUpdateAliasEnforcesDomainConstraintsRepro|TestUpdateFromEnforcesDomainConstraintsRepro'
  -count=1`.
- Expected PostgreSQL behavior: `UPDATE` statements using a target-table alias,
  including `UPDATE ... FROM`, can assign valid values to domain-typed target
  columns and enforce constraints declared on those domains. If any updated row
  assigns a value that violates the domain check, the statement fails with that
  domain constraint error and none of the statement's row changes persist.
- Observed Doltgres behavior: the aliased update panics during domain
  constraint analysis with `receiveMessage recovered panic: table not found:
  t`, so even valid aliased updates are rejected and invalid updates fail with
  an internal error instead of the domain check.

### Domain-typed column defaults are ignored or rejected

- Reproducers: `TestDomainTypedColumnAcceptsValidColumnDefaultRepro` and
  `TestAlterTableAddDomainColumnValidDefaultRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestDomainTypedColumnAcceptsValidColumnDefaultRepro|TestAlterTableAddDomainColumnValidDefaultRepro'
  -count=1`.
- Expected PostgreSQL behavior: a column whose type is a domain can declare a
  valid default using the domain's base type. `INSERT` statements that omit the
  column use that default, and `ALTER TABLE ... ADD COLUMN ... DEFAULT` accepts
  the default and backfills existing rows after validating the domain.
- Observed Doltgres behavior: `CREATE TABLE column_default_domain_items (...,
  amount column_default_positive_domain DEFAULT 5)` is accepted, but inserting
  a row that omits `amount` persists `NULL` instead of `5`. The equivalent
  `ALTER TABLE ... ADD COLUMN amount add_column_valid_default_domain DEFAULT 5`
  is rejected with `ASSIGNMENT_CAST: target is of type
  add_column_valid_default_domain but expression is of type integer: 5`.

### UPDATE SET DEFAULT rejects domain defaults

- Reproducer: `TestUpdateSetDefaultUsesDomainDefaultRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestUpdateSetDefaultUsesDomainDefaultRepro -count=1`.
- Expected PostgreSQL behavior: `UPDATE ... SET amount = DEFAULT` on a
  domain-typed column uses the domain default when the column does not declare
  its own default, then validates and stores that value.
- Observed Doltgres behavior: the update is rejected with `UPDATE:
  non-Doltgres type found in source: ()`, and the row keeps its previous value
  instead of storing the domain default.

### Function domain boundaries are not enforced or resolved

- Reproducers: `TestSqlFunctionReturnEnforcesDomainConstraintsRepro` and
  `TestPlpgsqlFunctionReturnDomainValueRepro` and
  `TestSqlFunctionArgumentResolvesDomainInputRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'Test(SqlFunctionReturnEnforcesDomainConstraints|PlpgsqlFunctionReturnDomainValue|SqlFunctionArgumentResolvesDomainInput)Repro'
  -count=1`.
- Expected PostgreSQL behavior: SQL functions returning a domain validate their
  returned value against the domain constraints at execution time. PL/pgSQL
  functions returning a domain can return valid values of the domain's base
  type. SQL functions declared with a domain-typed argument can be called with
  valid values of the domain's base type.
- Observed Doltgres behavior: a SQL function declared as
  `RETURNS function_return_positive_domain` can return `-1` without the
  `function_return_positive_domain_check` error. A PL/pgSQL function declared
  as `RETURNS plpgsql_return_positive_domain` fails for a valid `RETURN 7` with
  `no valid cast for return value`. A SQL function declared with `input_value
  function_arg_positive_domain` cannot be called as
  `function_arg_identity(7)` and fails with `function
  function_arg_identity(integer) does not exist`.

### ON CONFLICT DO UPDATE bypasses domain constraints

- Reproducers: `TestOnConflictUpdateEnforcesDomainConstraintsRepro` and
  `TestOnConflictUpdateEnforcesDomainNotNullRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestOnConflictUpdateEnforcesDomain(Constraints|NotNull)Repro'
  -count=1`.
- Expected PostgreSQL behavior: `ON CONFLICT DO UPDATE` enforces constraints
  declared on domain-typed target columns. If the conflict action assigns a
  value that violates the domain check or domain `NOT NULL` invariant, the
  statement fails atomically and the existing row remains unchanged.
- Observed Doltgres behavior: `INSERT INTO on_conflict_domain_items VALUES (1,
  2) ON CONFLICT (id) DO UPDATE SET amount = -1` succeeds and persists
  `amount = -1`, bypassing `CHECK (VALUE > 0)` on the domain. The same path
  accepts `amount = NULL` for a `NOT NULL` domain and persists the null value.

### ALTER TABLE ADD COLUMN allows NULL backfills for NOT NULL domains

- Reproducer: `TestAlterTableAddDomainNotNullColumnValidatesExistingRowsRepro`
  in `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterTableAddDomainNotNullColumnValidatesExistingRowsRepro
  -count=1`.
- Expected PostgreSQL behavior: adding a column whose type is a `NOT NULL`
  domain to a non-empty table validates the implicit NULL backfill and rejects
  the `ALTER TABLE` unless a valid non-null default is provided.
- Observed Doltgres behavior: `ALTER TABLE add_required_domain_items ADD COLUMN
  amount add_column_required_domain` succeeds, and the existing row is
  persisted as `(1, NULL)`, violating the domain's `NOT NULL` invariant.

### ALTER COLUMN TYPE to a domain does not validate existing rows

- Reproducers: `TestAlterColumnTypeToDomainValidatesExistingRowsRepro` and
  `TestAlterColumnTypeToNotNullDomainValidatesExistingRowsRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestAlterColumnTypeTo(NotNullDomain|Domain)ValidatesExistingRowsRepro'
  -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... ALTER COLUMN amount TYPE
  some_domain` validates existing stored values against the target domain before
  accepting the type change. If any row violates the target domain `CHECK` or
  `NOT NULL` invariant, the `ALTER TABLE` fails and the original table contents
  remain unchanged.
- Observed Doltgres behavior: converting an `INT` column containing `-1` to
  `alter_type_existing_positive_domain`, whose check requires `VALUE > 0`,
  succeeds instead of raising `alter_type_existing_positive_domain_check`,
  leaving the invalid value stored under the domain-typed column. Converting an
  `INT` column containing `NULL` to `alter_type_required_domain`, a `NOT NULL`
  domain, also succeeds and leaves the null value stored under the domain-typed
  column.

### Nested domains panic instead of enforcing domain constraints

- Reproducer: `TestNestedDomainEnforcesBaseDomainConstraintsRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestNestedDomainEnforcesBaseDomainConstraintsRepro
  -count=1`.
- Expected PostgreSQL behavior: a domain created on top of another domain can
  cast valid values and enforces both the base domain constraints and its own
  constraints.
- Observed Doltgres behavior: creating the nested domain succeeds, but casting
  to it panics with `unable to get DoltgresType from ID:
  {Type:["","nested_base_positive"]}` before valid values or constraint
  violations are handled.

### ALTER DOMAIN SET NOT NULL is rejected before validating existing rows

- Reproducer: `TestAlterDomainSetNotNullValidatesExistingRowsRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestAlterDomainSetNotNullValidatesExistingRowsRepro
  -count=1`.
- Expected PostgreSQL behavior: `ALTER DOMAIN ... SET NOT NULL` validates
  existing columns that use the domain before accepting the not-null invariant.
- Observed Doltgres behavior: the statement fails with `ERROR: ALTER DOMAIN is
  not yet supported Please file an issue at
  https://github.com/dolthub/doltgresql/issues (SQLSTATE 0A000)`, so nullable
  existing domains cannot be tightened to reject null values.

### RENAME COLUMN rejects columns used by CHECK constraints

- Reproducer: `TestRenameColumnUsedByCheckConstraintKeepsConstraintUsableRepro`
  in `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestRenameColumnUsedByCheckConstraintKeepsConstraintUsableRepro -count=1`.
- Expected PostgreSQL behavior: renaming a column used by a same-table `CHECK`
  constraint rewrites the constraint expression, and the renamed column remains
  constrained.
- Observed Doltgres behavior: the rename fails with `can't alter column
  "amount" because it would invalidate check constraint "amount_positive"`.

### ALTER TABLE RENAME CONSTRAINT is rejected

- Reproducer: `TestAlterTableRenameConstraintRepro` in
  `testing/go/rename_constraint_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterTableRenameConstraintRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... RENAME CONSTRAINT` renames the
  table constraint, updates `pg_catalog.pg_constraint.conname`, and future
  violations report the new constraint name.
- Observed Doltgres behavior: setup fails with `ERROR: ALTER TABLE with
  unsupported command type *tree.AlterTableRenameConstraint (SQLSTATE XX000)`,
  so constraint names cannot be corrected or migrated in place.

### UPDATE SET DEFAULT rejects columns with implicit NULL defaults

- Reproducer: `TestUpdateSetImplicitNullDefaultRepro` in
  `testing/go/update_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestUpdateSetImplicitNullDefaultRepro -count=1`.
- Expected PostgreSQL behavior: assigning `DEFAULT` to a column without an
  explicit default updates that column to its implicit `NULL` default.
- Observed Doltgres behavior: the update fails with `UPDATE: non-Doltgres type
  found in source: ()`, and the rows retain their old values.

### Column defaults accept references to other columns

- Reproducers: `TestCreateTableDefaultRejectsColumnReferencesRepro` and
  `TestAlterColumnDefaultRejectsColumnReferencesRepro` in
  `testing/go/default_expression_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'Test(CreateTableDefaultRejectsColumnReferences|AlterColumnDefaultRejectsColumnReferences)Repro'
  -count=1`.
- Expected PostgreSQL behavior: column default expressions cannot reference
  other columns from the row, so both `DEFAULT (source_value)` in `CREATE TABLE`
  and `ALTER COLUMN ... SET DEFAULT (source_value)` are rejected.
- Observed Doltgres behavior: both statements succeed, allowing persisted
  schema definitions with PostgreSQL-invalid row-dependent defaults.

### Column defaults accept aggregate, window, and set-returning expressions

- Reproducer: `TestDefaultExpressionsRejectNonScalarExpressionsRepro` in
  `testing/go/default_expression_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDefaultExpressionsRejectNonScalarExpressionsRepro -count=1`.
- Expected PostgreSQL behavior: `DEFAULT (avg(1))`, `DEFAULT (row_number()
  OVER ())`, and `DEFAULT (generate_series(1, 2))` are rejected because
  aggregate, window, and set-returning functions are not allowed in column
  defaults.
- Observed Doltgres behavior: all three table definitions succeed, allowing
  persisted schema definitions with PostgreSQL-invalid non-scalar defaults.

### INSERT duplicate target columns report the wrong error

- Reproducer: `TestInsertRejectsDuplicateTargetColumnsRepro` in
  `testing/go/insert_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestInsertRejectsDuplicateTargetColumnsRepro -count=1`.
- Expected PostgreSQL behavior: `INSERT INTO t (id, a, a) VALUES (...)` is
  rejected with `column "a" specified more than once`, and no row is inserted.
- Observed Doltgres behavior: the statement is rejected, but reports
  `column 'a' specified twice` with a MySQL errno and `SQLSTATE XX000`. Clients
  that rely on PostgreSQL's duplicate-column diagnostic see a non-PostgreSQL
  error shape.

### COPY FROM STDIN duplicate target columns report the wrong error

- Reproducer: `TestCopyFromStdinRejectsDuplicateTargetColumnsRepro` in
  `testing/go/copy_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCopyFromStdinRejectsDuplicateTargetColumnsRepro -count=1`.
- Expected PostgreSQL behavior: `COPY t (id, a, a) FROM STDIN` is rejected
  before ingesting rows with `column "a" specified more than once`.
- Observed Doltgres behavior: the copy is rejected, but reports
  `column 'a' specified twice` with `SQLSTATE XX000`. COPY clients see a
  non-PostgreSQL duplicate-column diagnostic.

### UPDATE SET expressions use earlier assignments from the same statement

- Reproducer: `TestUpdateAssignmentsUseOriginalRowValuesRepro` in
  `testing/go/update_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestUpdateAssignmentsUseOriginalRowValuesRepro -count=1`.
- Expected PostgreSQL behavior: right-hand expressions in an `UPDATE` `SET`
  list read the original row values, so `SET a = b, b = a` swaps `(10, 20)`
  to `(20, 10)`.
- Observed Doltgres behavior: the update persists `(20, 20)`, showing that
  the second assignment reads the value assigned by the first assignment.

### UPDATE row-valued SET from a subquery is rejected

- Reproducers: `TestUpdateMultiAssignmentFromSubqueryRepro` and
  `TestUpdateMultiAssignmentEmptySubquerySetsNullsRepro` in
  `testing/go/update_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run
  'TestUpdateMultiAssignment(FromSubquery|EmptySubquerySetsNulls)Repro'
  -count=1`.
- Expected PostgreSQL behavior: `UPDATE ... SET (a, b) = (SELECT ...)` can
  assign multiple target columns from a scalar subquery in one simultaneous
  row-valued assignment. If the scalar subquery returns no rows, PostgreSQL
  assigns `NULL` to each target column.
- Observed Doltgres behavior: the update fails with `UPDATE: non-Doltgres type
  found in source: Subquery(...)`, and the target row keeps its original
  values instead of applying either the selected values or the no-row `NULL`
  assignment.

### UPDATE row-valued SET reports an internal error for duplicate target columns

- Reproducer: `TestUpdateMultiAssignmentRejectsDuplicateColumnsRepro` in
  `testing/go/update_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestUpdateMultiAssignmentRejectsDuplicateColumnsRepro -count=1`.
- Expected PostgreSQL behavior: `UPDATE ... SET (a, a) = (1, 2)` is rejected
  with `multiple assignments to same column "a"` before changing the target
  row.
- Observed Doltgres behavior: the statement is rejected with the internal error
  `ASSIGNMENT_CAST: target is of type integer but expression is of type
  record: RECORD EXPR` instead of validating the duplicate target column.

### UPDATE scalar SET accepts duplicate target columns

- Reproducer: `TestUpdateScalarAssignmentRejectsDuplicateColumnsRepro` in
  `testing/go/update_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestUpdateScalarAssignmentRejectsDuplicateColumnsRepro -count=1`.
- Expected PostgreSQL behavior: `UPDATE ... SET a = 1, a = 2` is rejected with
  `multiple assignments to same column "a"`, and the row remains unchanged.
- Observed Doltgres behavior: the update succeeds and persists the last
  assignment, changing `a` from `10` to `2`. A statement PostgreSQL rejects can
  silently mutate stored data.

### ON CONFLICT DO UPDATE SET expressions use earlier assignments from the same statement

- Reproducer: `TestOnConflictUpdateAssignmentsUseOriginalRowValuesRepro` in
  `testing/go/update_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestOnConflictUpdateAssignmentsUseOriginalRowValuesRepro -count=1`.
- Expected PostgreSQL behavior: `ON CONFLICT DO UPDATE SET` expressions read
  the original target-row values unless they explicitly reference `EXCLUDED`,
  so `SET c1 = 'new-c1', c2 = c1` stores `c2 = 'old-c1'`.
- Observed Doltgres behavior: the row persists `c2 = 'new-c1'`, showing that
  the second assignment reads the value assigned earlier in the same `SET`
  list.

### ON CONFLICT DO UPDATE accepts duplicate target columns

- Reproducer: `TestOnConflictUpdateRejectsDuplicateTargetColumnsRepro` in
  `testing/go/update_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestOnConflictUpdateRejectsDuplicateTargetColumnsRepro -count=1`.
- Expected PostgreSQL behavior: `INSERT ... ON CONFLICT DO UPDATE SET a = 1,
  a = 2` is rejected with `multiple assignments to same column "a"`, and the
  conflicting target row remains unchanged.
- Observed Doltgres behavior: the upsert succeeds and persists the last
  assignment, changing `a` from `10` to `2`. A conflict-handling statement
  PostgreSQL rejects can silently mutate stored data.

### ON CONFLICT DO UPDATE WHERE predicates are evaluated once per assignment

- Reproducer: `TestOnConflictUpdateWhereVolatilePredicateEvaluatesOnceRepro`
  in `testing/go/update_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run
  TestOnConflictUpdateWhereVolatilePredicateEvaluatesOnceRepro -count=1`.
- Expected PostgreSQL behavior: the `ON CONFLICT DO UPDATE WHERE` predicate is
  evaluated once for the conflicting row. With
  `WHERE nextval('...') = 1`, both `SET` assignments use that accepted row and
  the next sequence value is `2`.
- Observed Doltgres behavior: the predicate is evaluated separately for each
  `SET` assignment. The first assignment stores `a = 10`, the second predicate
  evaluation consumes another sequence value and leaves `b = 0`, the
  `RETURNING` list emits no row, and the next sequence value is `3`.

### ON CONFLICT DO UPDATE cannot pass EXCLUDED columns to functions

- Reproducer: `TestOnConflictUpdateFunctionArgumentCanReferenceExcludedRepro`
  in `testing/go/update_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run
  TestOnConflictUpdateFunctionArgumentCanReferenceExcludedRepro -count=1`.
- Expected PostgreSQL behavior: `EXCLUDED` columns are visible inside function
  arguments in the `DO UPDATE` action, so
  `SET label = add_excluded_suffix(EXCLUDED.label)` stores the function result
  derived from the proposed row.
- Observed Doltgres behavior: the same upsert fails during planning with
  `table not found: excluded`, and the conflicting target row remains
  unchanged.

### ON CONFLICT DO UPDATE row-valued SET from a subquery is rejected

- Reproducer: `TestOnConflictUpdateMultiAssignmentFromSubqueryRepro` in
  `testing/go/update_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestOnConflictUpdateMultiAssignmentFromSubqueryRepro
  -count=1`.
- Expected PostgreSQL behavior: the `DO UPDATE` action can use `SET (a, b) =
  (SELECT ...)` to assign multiple target columns from a scalar subquery.
- Observed Doltgres behavior: the upsert fails with `UPDATE: non-Doltgres type
  found in source: Subquery(...)`, and the conflicting target row keeps its
  original values.

### ON CONFLICT DO UPDATE can update the same target row twice in one statement

- Reproducer: `TestOnConflictUpdateRejectsDuplicateTargetRowsRepro` in
  `testing/go/update_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestOnConflictUpdateRejectsDuplicateTargetRowsRepro -count=1`.
- Expected PostgreSQL behavior: if one `INSERT ... ON CONFLICT DO UPDATE`
  statement would update the same target row more than once, PostgreSQL rejects
  the statement with `ON CONFLICT DO UPDATE command cannot affect row a second
  time` and leaves the original row untouched.
- Observed Doltgres behavior: the statement succeeds and persists the second
  proposed row's value, silently applying an ambiguous multi-row upsert.

### ON CONFLICT cannot infer unique expression indexes

- Reproducer: `TestOnConflictUsesUniqueExpressionIndexRepro` in
  `testing/go/index_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestOnConflictUsesUniqueExpressionIndexRepro -count=1`.
- Expected PostgreSQL behavior: a unique expression index such as
  `CREATE UNIQUE INDEX ... ON expression_upsert_items ((lower(email)))` can be
  used as an `INSERT ... ON CONFLICT ((lower(email))) DO UPDATE` arbiter, so a
  case-insensitive duplicate email updates the existing row.
- Observed Doltgres behavior: the upsert fails during parsing with
  `at or near "(": syntax error`, leaving the original row unchanged even
  though the matching unique expression index exists and is enforced for plain
  inserts.

### ON CONFLICT cannot use NULLS NOT DISTINCT unique indexes for NULL keys

- Reproducer: `TestOnConflictUsesNullsNotDistinctUniqueIndexRepro` in
  `testing/go/index_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestOnConflictUsesNullsNotDistinctUniqueIndexRepro
  -count=1`.
- Expected PostgreSQL behavior: a `UNIQUE NULLS NOT DISTINCT` index can be
  inferred by `ON CONFLICT (code)`, so inserting a second `NULL` key routes to
  `DO UPDATE` and updates the existing row.
- Observed Doltgres behavior: the `NULLS NOT DISTINCT` index rejects the
  duplicate `NULL` as a unique violation, but `ON CONFLICT (code)` does not
  treat that conflict as its arbiter. The statement fails with `duplicate key
  value violates unique constraint`, and the original row remains unchanged.

### CREATE UNIQUE INDEX NULLS NOT DISTINCT accepts existing duplicate NULLs

- Reproducer:
  `TestCreateUniqueIndexNullsNotDistinctRejectsExistingDuplicateNullsRepro` in
  `testing/go/index_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run
  TestCreateUniqueIndexNullsNotDistinctRejectsExistingDuplicateNullsRepro
  -count=1`.
- Expected PostgreSQL behavior: building a `UNIQUE NULLS NOT DISTINCT` index
  scans existing rows and rejects duplicate `NULL` keys with a duplicate-key
  error, leaving no invalid index behind.
- Observed Doltgres behavior: `CREATE UNIQUE INDEX ... NULLS NOT DISTINCT`
  succeeds on a table that already contains two `NULL` keys, and
  `pg_indexes` reports the invalid unique index as created.

### ALTER TABLE ADD UNIQUE NULLS NOT DISTINCT accepts existing duplicate NULLs

- Reproducer:
  `TestAddUniqueConstraintNullsNotDistinctRejectsExistingDuplicateNullsRepro`
  in `testing/go/constraint_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run
  TestAddUniqueConstraintNullsNotDistinctRejectsExistingDuplicateNullsRepro
  -count=1`.
- Expected PostgreSQL behavior: adding a `UNIQUE NULLS NOT DISTINCT`
  constraint scans existing rows and rejects duplicate `NULL` keys, leaving no
  invalid constraint metadata behind.
- Observed Doltgres behavior: `ALTER TABLE ... ADD CONSTRAINT ... UNIQUE
  NULLS NOT DISTINCT` succeeds on a table that already contains two `NULL`
  keys, and `information_schema.table_constraints` reports the invalid
  constraint as created.

### Index definitions accept invalid expressions PostgreSQL rejects

- Reproducer: `TestIndexDefinitionsRejectInvalidExpressionsRepro` in
  `testing/go/index_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestIndexDefinitionsRejectInvalidExpressionsRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE INDEX ... ((generate_series(1, 2)))`,
  index expressions using built-in or user-defined `VOLATILE`/`STABLE`
  functions such as `random()`, `now()`, or a function declared `VOLATILE`,
  partial index predicates using set-returning, volatile, or stable functions,
  and partial index predicates with subqueries are rejected because persisted
  index definitions cannot depend on set-returning, volatile, stable, or
  subquery expressions.
- Observed Doltgres behavior: those index definitions all succeed, allowing
  PostgreSQL-invalid expression and partial-index metadata to be persisted.

### CREATE/DROP INDEX CONCURRENTLY are allowed inside transaction blocks

- Reproducers: `TestCreateIndexConcurrentlyRejectsTransactionBlockRepro` and
  `TestDropIndexConcurrentlyRejectsTransactionBlockRepro` in
  `testing/go/index_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'Test(Create|Drop)IndexConcurrentlyRejectsTransactionBlockRepro'
  -count=1`.
- Expected PostgreSQL behavior: `CREATE INDEX CONCURRENTLY` and `DROP INDEX
  CONCURRENTLY` are rejected inside an explicit transaction block with `cannot
  run inside a transaction block`. The rejected `CREATE` creates no index
  metadata, and the rejected `DROP` leaves the existing index in place.
- Observed Doltgres behavior: both statements succeed inside `BEGIN`.
  `CREATE INDEX CONCURRENTLY` still leaves the new index visible in
  `pg_catalog.pg_indexes` after `ROLLBACK`, so rolled-back DDL metadata
  persists. `DROP INDEX CONCURRENTLY` is also accepted inside the transaction,
  bypassing PostgreSQL's transaction-boundary rule.

### CLUSTER is rejected before marking clustered-index metadata

- Reproducer: `TestClusterMarksIndexClusteredRepro` in
  `testing/go/index_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestClusterMarksIndexClusteredRepro -count=1`.
- Expected PostgreSQL behavior: `CLUSTER index_name ON table_name` rewrites the
  table using the selected index and records that index as clustered in
  `pg_catalog.pg_index.indisclustered`.
- Observed Doltgres behavior: the `CLUSTER` statement fails during parsing with
  `at or near "cluster": syntax error`, and the selected index is absent from
  `pg_index` rows where `indisclustered` is true.

### SQL MERGE is rejected instead of applying matched and not-matched actions

- Reproducer: `TestSqlMergeUpdatesAndInsertsRowsRepro` in
  `testing/go/sql_merge_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestSqlMergeUpdatesAndInsertsRowsRepro -count=1`.
- Expected PostgreSQL behavior: `MERGE INTO ... USING ... WHEN MATCHED THEN
  UPDATE ... WHEN NOT MATCHED THEN INSERT ...` updates matching target rows and
  inserts missing source rows atomically.
- Observed Doltgres behavior: the statement fails with `ERROR: at or near
  "merge": syntax error (SQLSTATE 42601)`, leaving the target table unchanged.

### DELETE USING is rejected instead of deleting joined target rows

- Reproducer: `TestDeleteUsingDeletesJoinedRowsRepro` in
  `testing/go/delete_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDeleteUsingDeletesJoinedRowsRepro -count=1`.
- Expected PostgreSQL behavior: `DELETE FROM ... USING ... WHERE ...` joins the
  target table with the `USING` relations, deletes matching target rows, and
  can return the deleted target rows through `RETURNING`.
- Observed Doltgres behavior: the statement fails at parse/plan time with
  `at or near "where": syntax error: unimplemented: this syntax`, so no joined
  delete occurs.

### Data-modifying CTEs are rejected

- Reproducer: `TestDataModifyingCtesRepro` in
  `testing/go/data_modifying_cte_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestDataModifyingCtesRepro -count=1`.
- Expected PostgreSQL behavior: `WITH` queries can contain `INSERT`, `UPDATE`,
  or `DELETE` statements with `RETURNING`; the outer query can consume the
  returned rows while the data modification persists.
- Observed Doltgres behavior: each write-form CTE fails with
  `unsupported CTE statement type: *tree.Insert`, `*tree.Update`, or
  `*tree.Delete`, so no data-modifying CTE write is applied.

### DML RETURNING cannot project `tableoid`

- Reproducer: `TestDmlReturningCanProjectTableoidRepro` in
  `testing/go/dml_returning_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDmlReturningCanProjectTableoidRepro -count=1`.
- Expected PostgreSQL behavior: `INSERT`, `UPDATE`, and `DELETE` `RETURNING`
  clauses can project the affected row's system columns, including
  `tableoid::regclass::text`, which identifies the base table for the returned
  row.
- Observed Doltgres behavior: each statement fails during planning with
  `column "tableoid" could not be found in any table in scope`, so DML
  `RETURNING` cannot expose PostgreSQL system-column metadata.

### Simple views are not automatically updatable

- Reproducers: `TestSimpleViewInsertUpdatesBaseTableRepro`,
  `TestSimpleViewUpdateUpdatesBaseTableRepro`, and
  `TestSimpleViewDeleteDeletesBaseTableRepro` in
  `testing/go/updatable_view_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'TestSimpleView(Insert|Update|Delete).*BaseTableRepro' -count=1`.
- Expected PostgreSQL behavior: a simple view over one base table is
  automatically updatable, so `INSERT`, `UPDATE`, and `DELETE` against the view
  write through to the underlying table.
- Observed Doltgres behavior: `INSERT` through the view fails with `expected
  insert destination to be resolved or unresolved table`, `UPDATE` fails with
  `table doesn't support UPDATE`, and `DELETE` fails with `table doesn't
  support DELETE FROM`.

### ROLLBACK does not remove temporary tables created in the transaction

- Reproducer: `TestRollbackDropsTempTableCreatedInTransactionRepro` in
  `testing/go/transaction_ddl_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestRollbackDropsTempTableCreatedInTransactionRepro -count=1`.
- Expected PostgreSQL behavior: a temporary table created inside a transaction
  is dropped when that transaction rolls back, so the same temp-table name can
  be reused afterward.
- Observed Doltgres behavior: the temporary table remains visible after
  `ROLLBACK`, and recreating the same temp-table name fails because it still
  exists.

### Schema-qualified persistent table names resolve to same-name temp tables

- Reproducer: `TestTemporaryTableShadowsPersistentTableRepro` in
  `testing/go/temp_table_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTemporaryTableShadowsPersistentTableRepro -count=1`.
- Expected PostgreSQL behavior: a temporary table may shadow a persistent table
  of the same name for unqualified references in the creating session, but
  `public.table_name` still resolves to the persistent table.
- Observed Doltgres behavior: after creating `TEMPORARY TABLE temp_shadow_items`
  over a persistent `public.temp_shadow_items`, `SELECT ... FROM
  public.temp_shadow_items` returns the temporary row instead of the persistent
  row.

### Schema-qualified INSERT writes into same-name temp tables

- Reproducer: `TestSchemaQualifiedInsertIgnoresTemporaryTableShadowRepro` in
  `testing/go/temp_table_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSchemaQualifiedInsertIgnoresTemporaryTableShadowRepro -count=1`.
- Expected PostgreSQL behavior: `INSERT INTO public.table_name` targets the
  persistent table in `public`, even when the session has a same-name temporary
  table that shadows unqualified references.
- Observed Doltgres behavior: the schema-qualified insert is written into the
  temporary table instead; after dropping the temporary table, the persistent
  table is missing the inserted row.

### Schema-qualified UPDATE changes same-name temp tables

- Reproducer: `TestSchemaQualifiedUpdateIgnoresTemporaryTableShadowRepro` in
  `testing/go/temp_table_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSchemaQualifiedUpdateIgnoresTemporaryTableShadowRepro -count=1`.
- Expected PostgreSQL behavior: `UPDATE public.table_name ...` updates the
  persistent table in `public`, even when a same-name temporary table shadows
  unqualified references.
- Observed Doltgres behavior: the schema-qualified update changes the
  temporary table instead; after dropping the temporary table, the persistent
  table still has its old value.

### Schema-qualified DELETE ignores same-name persistent tables

- Reproducer: `TestSchemaQualifiedDeleteIgnoresTemporaryTableShadowRepro` in
  `testing/go/temp_table_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSchemaQualifiedDeleteIgnoresTemporaryTableShadowRepro -count=1`.
- Expected PostgreSQL behavior: `DELETE FROM public.table_name ...` deletes from
  the persistent table in `public`, not from a same-name temporary table.
- Observed Doltgres behavior: the schema-qualified delete is evaluated against
  the temporary table, so the matching persistent row remains after the temp
  table is dropped.

### Schema-qualified ALTER TABLE is blocked by same-name temp tables

- Reproducer: `TestSchemaQualifiedAlterIgnoresTemporaryTableShadowRepro` in
  `testing/go/temp_table_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSchemaQualifiedAlterIgnoresTemporaryTableShadowRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE public.table_name ...` alters the
  persistent table in `public`, even when a same-name temporary table shadows
  unqualified references.
- Observed Doltgres behavior: the schema-qualified alter resolves to the
  temporary table and fails with `table temp_shadow_alter_items cannot be
  altered`, leaving the persistent table unmodified.

### Schema-qualified TRUNCATE is blocked by same-name temp tables

- Reproducer: `TestSchemaQualifiedTruncateIgnoresTemporaryTableShadowRepro` in
  `testing/go/temp_table_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSchemaQualifiedTruncateIgnoresTemporaryTableShadowRepro -count=1`.
- Expected PostgreSQL behavior: `TRUNCATE TABLE public.table_name` truncates
  the persistent table in `public`, not a same-name temporary table.
- Observed Doltgres behavior: the schema-qualified truncate resolves to the
  temporary table and fails with `table doesn't support TRUNCATE`, leaving the
  persistent rows in place.

### Schema-qualified DROP TABLE drops same-name temp tables

- Reproducer: `TestSchemaQualifiedDropIgnoresTemporaryTableShadowRepro` in
  `testing/go/temp_table_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSchemaQualifiedDropIgnoresTemporaryTableShadowRepro -count=1`.
- Expected PostgreSQL behavior: `DROP TABLE public.table_name` drops the
  persistent table in `public`, leaving any same-name temporary table in the
  session available for unqualified references.
- Observed Doltgres behavior: `DROP TABLE public.temp_shadow_drop_items` drops
  the temporary table instead, leaving the persistent table in place until a
  later unqualified `DROP TABLE` removes it.

### Explicit pg_temp function calls resolve to same-name public functions

- Reproducer:
  `TestExplicitPgTempFunctionLookupResolvesTemporaryFunctionRepro` in
  `testing/go/temp_table_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestExplicitPgTempFunctionLookupResolvesTemporaryFunctionRepro -count=1`.
- Expected PostgreSQL behavior: unqualified function calls do not implicitly
  search `pg_temp`, even when `pg_temp` appears first in `search_path`, but
  explicitly qualified `pg_temp.function_name()` calls resolve to the temporary
  function.
- Observed Doltgres behavior: unqualified calls correctly resolve the public
  function, but `SELECT pg_temp.pgtemp_lookup_probe()` returns the public
  function result instead of the temporary function result, so the explicit
  `pg_temp` qualifier is ignored or misresolved.

### Schema-qualified function calls resolve to search-path functions

- Reproducer: `TestSchemaQualifiedFunctionLookupUsesExplicitSchemaRepro` in
  `testing/go/function_lookup_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSchemaQualifiedFunctionLookupUsesExplicitSchemaRepro -count=1`.
- Expected PostgreSQL behavior: `schema_name.function_name()` calls resolve the
  function from the explicitly named schema, even when a same-name function in a
  different schema is earlier in `search_path`.
- Observed Doltgres behavior: after `search_path` is set to
  `dg_fn_lookup_a, public`, `SELECT dg_fn_lookup_b.dg_lookup_probe()` returns
  the `dg_fn_lookup_a` result, so explicit schema qualification is ignored for
  user-defined function calls.

### Schema-qualified function calls execute search-path side effects

- Reproducer: `TestSchemaQualifiedFunctionSideEffectsUseExplicitSchemaRepro` in
  `testing/go/function_lookup_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSchemaQualifiedFunctionSideEffectsUseExplicitSchemaRepro -count=1`.
- Expected PostgreSQL behavior: `schema_name.function_name()` executes the
  explicitly named function, so a same-name function in an earlier
  `search_path` schema cannot perform different writes.
- Observed Doltgres behavior: after `search_path` is set to
  `dg_fn_effect_a, public`, `SELECT dg_fn_effect_b.lookup_mutator()` executes
  the function from `dg_fn_effect_a`; the audit table contains label `a`
  instead of `b`, proving the wrong mutating routine ran.

### Schema-qualified default functions persist search-path results

- Reproducer: `TestSchemaQualifiedDefaultFunctionUsesExplicitSchemaRepro` in
  `testing/go/function_lookup_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSchemaQualifiedDefaultFunctionUsesExplicitSchemaRepro -count=1`.
- Expected PostgreSQL behavior: a column default declared as
  `dg_default_fn_b.lookup_default()` stores the result from the explicitly named
  schema, even when `search_path` starts with `dg_default_fn_a`.
- Observed Doltgres behavior: inserting a row without the defaulted column
  stores `1` from `dg_default_fn_a.lookup_default()` instead of `2` from
  `dg_default_fn_b.lookup_default()`, so a schema-qualified default can persist
  corrupted values when another same-name function is earlier in `search_path`.

### Unqualified default functions rebind at insert time

- Reproducer: `TestUnqualifiedDefaultFunctionBindsAtCreateTimeRepro` in
  `testing/go/function_lookup_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestUnqualifiedDefaultFunctionBindsAtCreateTimeRepro -count=1`.
- Expected PostgreSQL behavior: an unqualified function in a stored default
  binds to the function visible when the default is created, so later changes to
  `search_path` do not redirect inserts to a same-name function in another
  schema.
- Observed Doltgres behavior: after creating the default while
  `dg_default_bind_a` is first in `search_path`, then inserting with
  `dg_default_bind_b` first, the row stores `2` from
  `dg_default_bind_b.bind_default()` instead of `1` from the create-time
  binding in `dg_default_bind_a`.

### CHECK constraints reject user-defined functions

- Reproducers: `TestCheckConstraintAllowsSchemaQualifiedFunctionRepro` and
  `TestCheckConstraintAllowsUnqualifiedFunctionRepro` in
  `testing/go/function_lookup_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestCheckConstraintAllows(SchemaQualified|Unqualified)FunctionRepro'
  -count=1`.
- Expected PostgreSQL behavior: `CREATE TABLE ... CHECK (...)` accepts
  user-defined function calls in the constraint expression, including both
  unqualified calls visible through `search_path` and explicitly schema-qualified
  calls.
- Observed Doltgres behavior: table creation fails with `Invalid constraint
  expression, function not supported`, so schemas that enforce integrity through
  reusable validation functions cannot be loaded.

### Schema-qualified unique expression indexes enforce search-path keys

- Reproducer:
  `TestSchemaQualifiedUniqueExpressionIndexUsesExplicitSchemaRepro` in
  `testing/go/function_lookup_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSchemaQualifiedUniqueExpressionIndexUsesExplicitSchemaRepro -count=1`.
- Expected PostgreSQL behavior: a unique expression index on
  `dg_unique_fn_b.unique_key(value)` computes keys with the function from
  `dg_unique_fn_b`; inserting values `1` and `11` conflicts because the explicit
  function maps both to key `1`.
- Observed Doltgres behavior: with `search_path` set to `dg_unique_fn_a, public`,
  the second insert succeeds and the table contains two rows, so the unique index
  is enforcing keys from the same-name search-path function instead of the
  schema-qualified function in the index definition.

### Unqualified unique expression indexes rebind at insert time

- Reproducer: `TestUnqualifiedUniqueExpressionIndexBindsAtCreateTimeRepro` in
  `testing/go/function_lookup_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestUnqualifiedUniqueExpressionIndexBindsAtCreateTimeRepro -count=1`.
- Expected PostgreSQL behavior: an unqualified function in an expression index
  binds to the function visible when the index is created. A unique index on
  `unique_bind_key(value)` created while `dg_unique_bind_a` is first in
  `search_path` should compute keys as the raw value, so values `1` and `11`
  are distinct.
- Observed Doltgres behavior: after `search_path` is changed to
  `dg_unique_bind_b`, inserting value `11` fails with `duplicate unique key
  given: [1]`, proving the unique index expression was recomputed with
  `dg_unique_bind_b.unique_bind_key(value)` at insert time.

### Partial unique index predicates cannot evaluate user-defined functions

- Reproducer: `TestPartialUniqueIndexPredicateFunctionEvaluatesOnInsertRepro` in
  `testing/go/function_lookup_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPartialUniqueIndexPredicateFunctionEvaluatesOnInsertRepro -count=1`.
- Expected PostgreSQL behavior: an insert into a table with a partial unique
  index whose predicate calls an immutable user-defined function evaluates the
  predicate and succeeds when no uniqueness conflict exists.
- Observed Doltgres behavior: the first insert fails with `partial unique index
  predicate function include_row does not support int64`, so valid partial
  indexes using reusable predicate functions cannot accept rows.

### Schema-qualified generated columns persist search-path function results

- Reproducer: `TestSchemaQualifiedGeneratedColumnFunctionUsesExplicitSchemaRepro`
  in `testing/go/function_lookup_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSchemaQualifiedGeneratedColumnFunctionUsesExplicitSchemaRepro
  -count=1`.
- Expected PostgreSQL behavior: a stored generated column declared as
  `dg_generated_fn_b.generated_value(value)` stores the result from the
  explicitly named schema; for input `5`, the generated value is `205`.
- Observed Doltgres behavior: with `search_path` set to
  `dg_generated_fn_a, public`, the inserted row stores generated value `105`, so
  persisted generated-column data is computed with the same-name search-path
  function instead of the schema-qualified function in the table definition.

### Unqualified generated-column functions rebind at insert time

- Reproducer:
  `TestUnqualifiedGeneratedColumnFunctionBindsAtCreateTimeRepro` in
  `testing/go/function_lookup_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestUnqualifiedGeneratedColumnFunctionBindsAtCreateTimeRepro -count=1`.
- Expected PostgreSQL behavior: an unqualified function in a stored generated
  column binds to the function visible when the table is created; later
  `search_path` changes do not redirect generated-value computation.
- Observed Doltgres behavior: after creating the generated column while
  `dg_generated_bind_a` is first in `search_path`, then inserting with
  `dg_generated_bind_b` first, the stored generated value is `205` from
  `dg_generated_bind_b.generated_bind_value(5)` instead of `105` from the
  create-time binding in `dg_generated_bind_a`.

### Schema-qualified view functions execute search-path functions

- Reproducer: `TestSchemaQualifiedViewFunctionUsesExplicitSchemaRepro` in
  `testing/go/function_lookup_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSchemaQualifiedViewFunctionUsesExplicitSchemaRepro -count=1`.
- Expected PostgreSQL behavior: a view expression declared as
  `dg_view_fn_b.view_value(value)` executes the function from the explicitly
  named schema; for input `5`, the view returns `205`.
- Observed Doltgres behavior: with `search_path` set to `dg_view_fn_a, public`,
  querying the view returns `105`, so schema-qualified function calls stored in
  view definitions are evaluated through the current search-path function.

### Unqualified view functions rebind at query time

- Reproducer: `TestUnqualifiedViewFunctionBindsAtCreateTimeRepro` in
  `testing/go/function_lookup_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestUnqualifiedViewFunctionBindsAtCreateTimeRepro -count=1`.
- Expected PostgreSQL behavior: an unqualified function in a stored view
  definition binds to the function visible when the view is created; later
  `search_path` changes do not redirect the view to a same-name function in
  another schema.
- Observed Doltgres behavior: after creating the view while `dg_view_bind_a` is
  first in `search_path`, then querying with `dg_view_bind_b` first, the view
  returns `205` from `dg_view_bind_b.view_bind_value(5)` instead of `105` from
  the create-time binding in `dg_view_bind_a`.

### Schema-qualified materialized view functions persist search-path results

- Reproducer:
  `TestSchemaQualifiedMaterializedViewFunctionUsesExplicitSchemaRepro` in
  `testing/go/function_lookup_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSchemaQualifiedMaterializedViewFunctionUsesExplicitSchemaRepro
  -count=1`.
- Expected PostgreSQL behavior: a materialized view query declared with
  `dg_matview_fn_b.mat_value(value)` materializes the function result from the
  explicitly named schema; for input `5`, the stored value is `205`.
- Observed Doltgres behavior: with `search_path` set to
  `dg_matview_fn_a, public`, the materialized view stores `105`, so
  materialized-view data is persisted from the same-name search-path function
  instead of the schema-qualified function in the view query.

### Unqualified materialized-view functions rebind on refresh

- Reproducer:
  `TestUnqualifiedMaterializedViewFunctionBindsAtCreateTimeRepro` in
  `testing/go/function_lookup_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestUnqualifiedMaterializedViewFunctionBindsAtCreateTimeRepro -count=1`.
- Expected PostgreSQL behavior: an unqualified function in a materialized-view
  definition binds to the function visible when the materialized view is
  created; `REFRESH MATERIALIZED VIEW` reuses that binding even if
  `search_path` later changes.
- Observed Doltgres behavior: after creating the materialized view while
  `dg_matview_bind_a` is first in `search_path`, then refreshing with
  `dg_matview_bind_b` first, the materialized row stores `206` from
  `dg_matview_bind_b.mat_bind_value(6)` instead of `106` from the create-time
  binding in `dg_matview_bind_a`.

### Temporary table ON COMMIT actions are rejected

- Reproducers: `TestTemporaryTableOnCommitDeleteRowsRepro` and
  `TestTemporaryTableOnCommitDropRepro` in
  `testing/go/temp_table_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestTemporaryTableOnCommit(DeleteRows|Drop)Repro' -count=1`.
- Expected PostgreSQL behavior: `CREATE TEMPORARY TABLE ... ON COMMIT DELETE
  ROWS` clears temporary table contents at commit, and `ON COMMIT DROP` drops
  the temporary table at commit.
- Observed Doltgres behavior: both table definitions fail during setup with
  `ERROR: ON COMMIT is not yet supported (SQLSTATE 0A000)`.

### Temporary tables can be created explicitly in persistent schemas

- Reproducer: `TestTemporaryTableRejectsPersistentSchemaRepro` in
  `testing/go/temp_table_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTemporaryTableRejectsPersistentSchemaRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE TEMPORARY TABLE public.name (...)` fails
  with `cannot create temporary relation in non-temporary schema`, because
  temporary relations must be created in a session-local temp schema.
- Observed Doltgres behavior: the schema-qualified temporary-table creation
  succeeds, so callers can request a temporary relation in `public` instead of
  being forced onto a temporary schema.

### PREPARE TRANSACTION allows transactions that used temporary tables

- Reproducer: `TestPrepareTransactionRejectsTemporaryTableUseRepro` in
  `testing/go/prepared_transaction_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPrepareTransactionRejectsTemporaryTableUseRepro -count=1`.
- Expected PostgreSQL behavior: `PREPARE TRANSACTION` rejects a transaction
  that created or used a temporary table, because prepared transactions must
  survive past the current session and cannot depend on session-local temporary
  state.
- Observed Doltgres behavior: the transaction prepares successfully after
  creating and inserting into a temporary table; the reproducer rolls back the
  prepared transaction and fails because the expected rejection never occurs.

### PL/pgSQL exception blocks do not roll back protected writes

- Reproducer: `TestPlpgsqlExceptionBlockRollsBackInnerWritesRepro` in
  `testing/go/statement_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestPlpgsqlExceptionBlockRollsBackInnerWritesRepro -count=1`.
- Expected PostgreSQL behavior: a PL/pgSQL block with an `EXCEPTION` handler
  runs the protected block as a subtransaction, so writes before the caught
  exception are rolled back while handler writes persist.
- Observed Doltgres behavior: both the pre-exception insert and the handler
  insert persist, so caught exceptions can leave protected-block side effects
  behind.

### PL/pgSQL unhandled exceptions do not roll back function writes

- Reproducer: `TestPlpgsqlUnhandledExceptionRollsBackFunctionWritesRepro` in
  `testing/go/statement_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPlpgsqlUnhandledExceptionRollsBackFunctionWritesRepro -count=1`.
- Expected PostgreSQL behavior: if a PL/pgSQL function writes a row and then
  raises an unhandled exception, the failed function call rolls back the row
  written inside the function.
- Observed Doltgres behavior: the function raises the expected exception, but
  the row written before `RAISE EXCEPTION` remains persisted.

### SQL function errors do not roll back function writes

- Reproducer: `TestSqlFunctionErrorRollsBackFunctionWritesRepro` in
  `testing/go/statement_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSqlFunctionErrorRollsBackFunctionWritesRepro -count=1`.
- Expected PostgreSQL behavior: if a SQL-language function writes a row and a
  later statement in the same function fails, the failed function call rolls
  back the earlier row write.
- Observed Doltgres behavior: the function reports the duplicate-key error
  from the second insert, but the first row inserted by the failed function
  remains persisted.

### Function side effects survive outer statement constraint failures

- Reproducer: `TestFunctionSideEffectsRollBackOnOuterStatementErrorRepro` in
  `testing/go/statement_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestFunctionSideEffectsRollBackOnOuterStatementErrorRepro -count=1`.
- Expected PostgreSQL behavior: if a function called by an `INSERT` expression
  writes an audit row and returns a value that later violates the target
  table's `CHECK` constraint, the failed outer statement rolls back both the
  target-row write and the function side effect.
- Observed Doltgres behavior: the target row is rejected by the `CHECK`
  constraint, but the audit row written by the function remains persisted.

### INSERT function side effects survive foreign-key failures

- Reproducer: `TestInsertFunctionSideEffectsRollBackOnForeignKeyErrorRepro` in
  `testing/go/statement_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestInsertFunctionSideEffectsRollBackOnForeignKeyErrorRepro -count=1`.
- Expected PostgreSQL behavior: if an `INSERT` expression calls a function that
  writes an audit row and returns a value that later violates a foreign key,
  the failed statement rolls back both the rejected target-row write and the
  function side effect.
- Observed Doltgres behavior: the target row is rejected by the foreign-key
  violation, but the audit row written by the function remains persisted.

### INSERT SELECT function side effects survive later CHECK failures

- Reproducer:
  `TestInsertSelectFunctionSideEffectsRollBackOnConstraintErrorRepro` in
  `testing/go/statement_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestInsertSelectFunctionSideEffectsRollBackOnConstraintErrorRepro
  -count=1`.
- Expected PostgreSQL behavior: if `INSERT ... SELECT` evaluates a
  side-effecting function for selected rows and a later selected row violates a
  target-table `CHECK` constraint, the failed statement rolls back all target
  rows and all function side effects.
- Observed Doltgres behavior: the statement reports the `CHECK` violation, but
  the first inserted target row remains persisted and both function audit rows
  remain persisted, including the audit row for the rejected selected row.

### INSERT SELECT function side effects survive later duplicate-key failures

- Reproducer:
  `TestInsertSelectFunctionSideEffectsRollBackOnDuplicateKeyErrorRepro` in
  `testing/go/statement_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestInsertSelectFunctionSideEffectsRollBackOnDuplicateKeyErrorRepro
  -count=1`.
- Expected PostgreSQL behavior: if `INSERT ... SELECT` evaluates a
  side-effecting function for selected rows and a later selected row violates
  the target table's primary key, the failed statement rolls back all target
  rows and all function side effects.
- Observed Doltgres behavior: the statement reports the duplicate-key error,
  but the first inserted target row remains persisted and both function audit
  rows remain persisted, including the audit row for the rejected selected row.

### SELECT expression errors do not roll back function side effects

- Reproducer: `TestSelectFunctionSideEffectsRollBackOnExpressionErrorRepro` in
  `testing/go/statement_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSelectFunctionSideEffectsRollBackOnExpressionErrorRepro -count=1`.
- Expected PostgreSQL behavior: if a `SELECT` list invokes a side-effecting
  function and another expression in the same statement errors, the failed
  statement rolls back the function's write.
- Observed Doltgres behavior: the statement reports `division by zero`, but the
  audit row written by the function remains persisted.

### RETURNING expression errors leave base writes and function side effects

- Reproducer: `TestReturningFunctionSideEffectsRollBackOnExpressionErrorRepro`
  in `testing/go/statement_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestReturningFunctionSideEffectsRollBackOnExpressionErrorRepro
  -count=1`.
- Expected PostgreSQL behavior: if an `INSERT ... RETURNING` expression invokes
  a side-effecting function and another `RETURNING` expression errors, the
  failed statement rolls back both the inserted target row and the function's
  audit-row write.
- Observed Doltgres behavior: the statement reports `division by zero`, but
  both the target row and the audit row remain persisted.

### UPDATE RETURNING PL/pgSQL functions panic after applying row changes

- Reproducer: `TestUpdateReturningFunctionSucceedsAfterRowChangeRepro` in
  `testing/go/statement_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestUpdateReturningFunctionSucceedsAfterRowChangeRepro -count=1`.
- Expected PostgreSQL behavior: an `UPDATE ... RETURNING` list may call a
  PL/pgSQL function; the statement succeeds, applies the row update, returns
  the function result, and commits the function's audit-row write.
- Observed Doltgres behavior: the row update is applied, but evaluating the
  `RETURNING` function raises `DoltgresHandler caught panic: runtime error:
  invalid memory address or nil pointer dereference`, and the function's audit
  row is not written.

### DELETE RETURNING PL/pgSQL functions panic after deleting rows

- Reproducer: `TestDeleteReturningFunctionSucceedsAfterDeleteRepro` in
  `testing/go/statement_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDeleteReturningFunctionSucceedsAfterDeleteRepro -count=1`.
- Expected PostgreSQL behavior: a `DELETE ... RETURNING` list may call a
  PL/pgSQL function; the statement succeeds, deletes the target row, returns
  the function result, and commits the function's audit-row write.
- Observed Doltgres behavior: the target row is deleted, but evaluating the
  `RETURNING` function raises `DoltgresHandler caught panic: runtime error:
  invalid memory address or nil pointer dereference`, and the function's audit
  row is not written.

### ON CONFLICT DO UPDATE function side effects survive CHECK failures

- Reproducer:
  `TestOnConflictDoUpdateFunctionSideEffectsRollBackOnConstraintErrorRepro` in
  `testing/go/statement_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestOnConflictDoUpdateFunctionSideEffectsRollBackOnConstraintErrorRepro
  -count=1`.
- Expected PostgreSQL behavior: if an `ON CONFLICT DO UPDATE` assignment calls
  a side-effecting function and the resulting update violates a target-table
  `CHECK` constraint, the failed statement leaves the original target row
  unchanged and rolls back the function's audit-row write.
- Observed Doltgres behavior: the conflicting target row remains unchanged, but
  the audit row written by the function survives after the statement reports
  the check-constraint error.

### ON CONFLICT DO NOTHING insert-expression side effects survive non-target unique failures

- Reproducer:
  `TestOnConflictDoNothingFunctionSideEffectsRollBackOnNonTargetUniqueErrorRepro`
  in `testing/go/statement_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestOnConflictDoNothingFunctionSideEffectsRollBackOnNonTargetUniqueErrorRepro
  -count=1`.
- Expected PostgreSQL behavior: if `INSERT ... ON CONFLICT DO NOTHING`
  evaluates side-effecting insert expressions and then fails a unique
  constraint that is not the conflict arbiter, the failed statement rolls back
  all function side effects.
- Observed Doltgres behavior: the target table retains only its pre-existing
  row, but both function audit rows from the failed insert remain persisted.

### ON CONFLICT DO UPDATE insert-expression side effects survive CHECK failures

- Reproducer:
  `TestOnConflictDoUpdateInsertFunctionSideEffectsRollBackOnConstraintErrorRepro`
  in `testing/go/statement_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestOnConflictDoUpdateInsertFunctionSideEffectsRollBackOnConstraintErrorRepro
  -count=1`.
- Expected PostgreSQL behavior: if `INSERT ... ON CONFLICT DO UPDATE`
  evaluates a side-effecting function in the proposed insert tuple and the
  conflict update later violates a `CHECK` constraint, the failed statement
  leaves the existing row unchanged and rolls back the function side effect.
- Observed Doltgres behavior: the existing row remains unchanged after the
  `CHECK` error, but the function audit row from the proposed insert tuple
  remains persisted.

### UPDATE expressions using PL/pgSQL functions panic instead of CHECK errors

- Reproducer: `TestUpdateFunctionConstraintViolationReportsCheckErrorRepro` in
  `testing/go/statement_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestUpdateFunctionConstraintViolationReportsCheckErrorRepro -count=1`.
- Expected PostgreSQL behavior: if an `UPDATE` expression calls a PL/pgSQL
  function that returns a value violating the target table's `CHECK`
  constraint, the statement reports that check-constraint violation, leaves the
  original target row unchanged, and rolls back function side effects.
- Observed Doltgres behavior: the same `UPDATE` recovers a nil-pointer panic
  from the PL/pgSQL function evaluation path and returns an internal
  `receiveMessage recovered panic: runtime error: invalid memory address or nil
  pointer dereference` error instead of the check-constraint error.

### UPDATE expressions using PL/pgSQL functions panic instead of foreign-key errors

- Reproducer:
  `TestUpdateFunctionForeignKeyViolationReportsForeignKeyErrorRepro` in
  `testing/go/statement_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestUpdateFunctionForeignKeyViolationReportsForeignKeyErrorRepro
  -count=1`.
- Expected PostgreSQL behavior: if an `UPDATE` expression calls a PL/pgSQL
  function that returns a value violating the target table's foreign key, the
  statement reports the foreign-key violation, leaves the original target row
  unchanged, and rolls back function side effects.
- Observed Doltgres behavior: the same `UPDATE` recovers a nil-pointer panic
  from the PL/pgSQL function evaluation path and returns an internal
  `receiveMessage recovered panic: runtime error: invalid memory address or nil
  pointer dereference` error instead of the foreign-key error.

### DELETE predicates using PL/pgSQL functions panic instead of foreign-key errors

- Reproducer:
  `TestDeleteFunctionPredicateForeignKeyViolationReportsForeignKeyErrorRepro`
  in `testing/go/statement_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDeleteFunctionPredicateForeignKeyViolationReportsForeignKeyErrorRepro
  -count=1`.
- Expected PostgreSQL behavior: if a `DELETE` predicate calls a PL/pgSQL
  function and the matching delete is blocked by a referencing foreign key, the
  statement reports the foreign-key violation, keeps the target row, and rolls
  back function side effects.
- Observed Doltgres behavior: the same `DELETE` recovers a nil-pointer panic
  from the PL/pgSQL function evaluation path and returns an internal
  `receiveMessage recovered panic: runtime error: invalid memory address or nil
  pointer dereference` error instead of the foreign-key error.

### CREATE TABLE AS query errors leave target relations and function side effects

- Reproducer:
  `TestCreateTableAsRollsBackFunctionSideEffectsOnQueryErrorRepro` in
  `testing/go/statement_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateTableAsRollsBackFunctionSideEffectsOnQueryErrorRepro
  -count=1`.
- Expected PostgreSQL behavior: if a `CREATE TABLE AS SELECT` source query
  errors after invoking a side-effecting function, the failed statement rolls
  back the new target relation and the function's side-effect writes.
- Observed Doltgres behavior: the statement reports `division by zero`, but
  `to_regclass('ctas_side_effect_target')` shows the target relation still
  exists and the function's audit row remains persisted.

### CREATE MATERIALIZED VIEW query errors leave target relations and function side effects

- Reproducer:
  `TestCreateMaterializedViewRollsBackFunctionSideEffectsOnQueryErrorRepro` in
  `testing/go/statement_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateMaterializedViewRollsBackFunctionSideEffectsOnQueryErrorRepro
  -count=1`.
- Expected PostgreSQL behavior: if a `CREATE MATERIALIZED VIEW AS SELECT`
  source query errors after invoking a side-effecting function, the failed
  statement rolls back the new materialized-view relation and the function's
  side-effect writes.
- Observed Doltgres behavior: the statement reports `division by zero`, but
  `to_regclass('matview_side_effect_target')` shows the materialized view still
  exists and the function's audit row remains persisted.

### PL/pgSQL CASE without ELSE does not raise case_not_found

- Reproducer: `TestPlpgsqlCaseWithoutElseRaisesCaseNotFoundRepro` in
  `testing/go/plpgsql_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPlpgsqlCaseWithoutElseRaisesCaseNotFoundRepro -count=1`.
- Expected PostgreSQL behavior: if a PL/pgSQL `CASE` statement has no `ELSE`
  and no `WHEN` branch matches, the function raises `case not found`.
- Observed Doltgres behavior: the no-match call returns normally instead of
  raising, so control flow that relies on PostgreSQL's exception semantics can
  silently continue.

### PL/pgSQL RAISE accepts duplicate options

- Reproducers: `TestPlpgsqlRaiseRejectsDuplicateMessageOptionRepro` and
  `TestPlpgsqlRaiseRejectsDuplicateDetailOptionRepro` in
  `testing/go/plpgsql_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestPlpgsqlRaiseRejectsDuplicate(Message|Detail)OptionRepro' -count=1`.
- Expected PostgreSQL behavior: a PL/pgSQL `RAISE` statement that supplies a
  format string and also specifies `USING MESSAGE = ...` raises
  `RAISE option already specified: MESSAGE`; a statement that repeats
  `USING DETAIL = ...` raises `RAISE option already specified: DETAIL`.
- Observed Doltgres behavior: the duplicate-`MESSAGE` function call returns
  successfully instead of raising, and the duplicate-`DETAIL` function raises
  the user message instead of the PostgreSQL duplicate-option error. Invalid
  PL/pgSQL exception statements can be silently accepted or report the wrong
  failure.

### PL/pgSQL ALIAS variables are not resolved

- Reproducer: `TestPlpgsqlAliasVariablesResolveRepro` in
  `testing/go/plpgsql_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPlpgsqlAliasVariablesResolveRepro -count=1`.
- Expected PostgreSQL behavior: PL/pgSQL `ALIAS FOR` declarations create
  alternate names for local variables and function arguments, so assignments
  through nested aliases update the original variable.
- Observed Doltgres behavior: the function is created, but executing it fails
  with `ERROR: variable nested_alias could not be found`, so valid PL/pgSQL
  alias-based code cannot run.

### PL/pgSQL functions cannot return table-typed composite variables

- Reproducer: `TestPlpgsqlReturnsTableCompositeVariableRepro` in
  `testing/go/plpgsql_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPlpgsqlReturnsTableCompositeVariableRepro -count=1`.
- Expected PostgreSQL behavior: a PL/pgSQL function can declare a variable
  using a table row type, `SELECT * INTO` that variable, and return it as the
  function's composite result; the repro returns `(1,apple,3,2.5)`.
- Observed Doltgres behavior: executing the function fails with
  `type "plpgsql_composite_return_items" does not exist`, so valid row-typed
  PL/pgSQL functions cannot return stored composite rows.

### Row-level INSERT triggers cannot see earlier rows from the same statement

- Reproducer: `TestBeforeInsertTriggerSeesEarlierRowsInSameStatementRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestBeforeInsertTriggerSeesEarlierRowsInSameStatementRepro -count=1`.
- Expected PostgreSQL behavior: during a multi-row `INSERT`, a row-level
  `BEFORE INSERT` trigger for a later row can see rows already inserted earlier
  in that same statement, so trigger-driven updates to those earlier rows take
  effect.
- Observed Doltgres behavior: the later trigger does not see the earlier row,
  so the statement commits without the trigger-driven update and persists stale
  target-table data.

### UPDATE FROM does not fire row-level UPDATE triggers

- Reproducer: `TestUpdateFromFiresRowTriggersRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestUpdateFromFiresRowTriggersRepro -count=1`.
- Expected PostgreSQL behavior: `UPDATE ... FROM` fires row-level `UPDATE`
  triggers once for each updated target row, so trigger-maintained audit tables
  or derived data reflect the changes.
- Observed Doltgres behavior: the target rows are updated, but the row-level
  trigger does not fire and the trigger-maintained audit table remains empty.

### Trigger exceptions do not roll back base-row or trigger side-effect writes

- Reproducer: `TestAfterInsertTriggerErrorRollsBackStatementRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAfterInsertTriggerErrorRollsBackStatementRepro -count=1`.
- Expected PostgreSQL behavior: if an `AFTER INSERT` trigger raises an
  exception after writing an audit row, the failed statement rolls back both
  the inserted base row and the trigger's audit-table write.
- Observed Doltgres behavior: the trigger raises the expected error, but the
  base row and the audit row both remain persisted, leaving data from a failed
  statement visible.

### AFTER UPDATE trigger exceptions do not roll back base-row or trigger side-effect writes

- Reproducer: `TestAfterUpdateTriggerErrorRollsBackStatementRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAfterUpdateTriggerErrorRollsBackStatementRepro -count=1`.
- Expected PostgreSQL behavior: if an `AFTER UPDATE` trigger raises an
  exception after writing an audit row, the failed statement rolls back both
  the row update and the trigger's audit-table write.
- Observed Doltgres behavior: the trigger raises the expected error, but the
  target row remains updated and the audit row remains persisted.

### AFTER DELETE trigger exceptions do not roll back base-row or trigger side-effect writes

- Reproducer: `TestAfterDeleteTriggerErrorRollsBackStatementRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAfterDeleteTriggerErrorRollsBackStatementRepro -count=1`.
- Expected PostgreSQL behavior: if an `AFTER DELETE` trigger raises an
  exception after writing an audit row, the failed statement rolls back both
  the row delete and the trigger's audit-table write.
- Observed Doltgres behavior: the trigger raises the expected error, but the
  target row remains deleted and the audit row remains persisted.

### Statement-level AFTER INSERT trigger exceptions do not roll back base-row or trigger side-effect writes

- Reproducer: `TestAfterStatementInsertTriggerErrorRollsBackStatementRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAfterStatementInsertTriggerErrorRollsBackStatementRepro -count=1`.
- Expected PostgreSQL behavior: if a statement-level `AFTER INSERT` trigger
  raises an exception after writing an audit row, the failed statement rolls
  back both the inserted target row and the trigger's audit-table write.
- Observed Doltgres behavior: the trigger raises the expected error, but the
  inserted target row and the audit row both remain persisted.

### Statement-level AFTER UPDATE trigger exceptions do not roll back base-row or trigger side-effect writes

- Reproducer: `TestAfterStatementUpdateTriggerErrorRollsBackStatementRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAfterStatementUpdateTriggerErrorRollsBackStatementRepro -count=1`.
- Expected PostgreSQL behavior: if a statement-level `AFTER UPDATE` trigger
  raises an exception after writing an audit row, the failed statement rolls
  back both the row update and the trigger's audit-table write, and the client
  sees the trigger exception.
- Observed Doltgres behavior: the statement reports the internal error
  `result max1Row iterator returned more than one row`, while the target row
  remains updated and the audit row remains persisted.

### Statement-level AFTER DELETE trigger exceptions do not roll back base-row or trigger side-effect writes

- Reproducer: `TestAfterStatementDeleteTriggerErrorRollsBackStatementRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAfterStatementDeleteTriggerErrorRollsBackStatementRepro -count=1`.
- Expected PostgreSQL behavior: if a statement-level `AFTER DELETE` trigger
  raises an exception after writing an audit row, the failed statement rolls
  back both the row delete and the trigger's audit-table write, and the client
  sees the trigger exception.
- Observed Doltgres behavior: the statement reports the internal error
  `result max1Row iterator returned more than one row`, while the target row
  remains deleted and the audit row remains persisted.

### BEFORE TRUNCATE trigger exceptions do not stop or roll back the truncate

- Reproducer: `TestBeforeTruncateTriggerErrorRollsBackSideEffectsRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestBeforeTruncateTriggerErrorRollsBackSideEffectsRepro -count=1`.
- Expected PostgreSQL behavior: if a `BEFORE TRUNCATE` trigger writes an audit
  row and then raises an exception, the failed statement keeps the target rows
  and rolls back the trigger's audit-table write.
- Observed Doltgres behavior: the trigger raises the expected error, but the
  target table is still truncated and the audit row remains persisted.

### AFTER TRUNCATE trigger exceptions do not roll back the truncate or trigger side effects

- Reproducer: `TestAfterTruncateTriggerErrorRollsBackStatementRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAfterTruncateTriggerErrorRollsBackStatementRepro -count=1`.
- Expected PostgreSQL behavior: if an `AFTER TRUNCATE` trigger raises an
  exception after writing an audit row, the failed statement restores the
  truncated target rows, rolls back the audit-table write, and reports the
  trigger exception.
- Observed Doltgres behavior: the statement reports the internal error
  `result schema iterator returned more than one row`, while the target table
  remains truncated and the audit row remains persisted.

### COPY FROM BEFORE INSERT trigger exceptions do not roll back trigger side effects

- Reproducer:
  `TestCopyFromStdinBeforeInsertTriggerErrorRollsBackSideEffectsRepro` in
  `testing/go/copy_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCopyFromStdinBeforeInsertTriggerErrorRollsBackSideEffectsRepro
  -count=1`.
- Expected PostgreSQL behavior: if `COPY FROM STDIN` fires a row-level
  `BEFORE INSERT` trigger that writes an audit row and then raises an
  exception, the failed `COPY` inserts no target row and rolls back the
  trigger side effect.
- Observed Doltgres behavior: `COPY` reports the trigger exception and leaves
  the target table empty, but the audit row written by the trigger remains
  persisted.

### COPY FROM AFTER INSERT trigger exceptions do not roll back copied rows or trigger side effects

- Reproducer:
  `TestCopyFromStdinAfterInsertTriggerErrorRollsBackStatementRepro` in
  `testing/go/copy_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCopyFromStdinAfterInsertTriggerErrorRollsBackStatementRepro
  -count=1`.
- Expected PostgreSQL behavior: if `COPY FROM STDIN` fires a row-level
  `AFTER INSERT` trigger that writes an audit row and then raises an
  exception, the failed `COPY` rolls back both the copied target row and the
  trigger side effect.
- Observed Doltgres behavior: `COPY` reports the trigger exception, but the
  copied target row and the audit row written by the trigger remain persisted.

### COPY FROM statement-level BEFORE INSERT trigger exceptions do not roll back trigger side effects

- Reproducer:
  `TestCopyFromStdinBeforeStatementTriggerErrorRollsBackSideEffectsRepro` in
  `testing/go/copy_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCopyFromStdinBeforeStatementTriggerErrorRollsBackSideEffectsRepro
  -count=1`.
- Expected PostgreSQL behavior: if `COPY FROM STDIN` fires a statement-level
  `BEFORE INSERT` trigger that writes an audit row and then raises an
  exception, the failed `COPY` inserts no target rows and rolls back the
  trigger side effect.
- Observed Doltgres behavior: `COPY` reports the trigger exception and leaves
  the target table empty, but the audit row written by the statement trigger
  remains persisted.

### COPY FROM statement-level AFTER INSERT trigger exceptions do not roll back copied rows or trigger side effects

- Reproducer:
  `TestCopyFromStdinAfterStatementTriggerErrorRollsBackStatementRepro` in
  `testing/go/copy_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCopyFromStdinAfterStatementTriggerErrorRollsBackStatementRepro
  -count=1`.
- Expected PostgreSQL behavior: if `COPY FROM STDIN` fires a statement-level
  `AFTER INSERT` trigger that writes an audit row and then raises an exception,
  the failed `COPY` rolls back both copied target rows and the trigger side
  effect.
- Observed Doltgres behavior: `COPY` reports the trigger exception, but the
  copied target row and the audit row written by the statement trigger remain
  persisted.

### COPY FROM row-level BEFORE INSERT trigger side effects survive later CHECK failures

- Reproducer:
  `TestCopyFromStdinBeforeInsertTriggerSideEffectsRollBackOnConstraintErrorRepro`
  in `testing/go/copy_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCopyFromStdinBeforeInsertTriggerSideEffectsRollBackOnConstraintErrorRepro
  -count=1`.
- Expected PostgreSQL behavior: if `COPY FROM STDIN` fires row-level
  `BEFORE INSERT` triggers and a later copied row violates a `CHECK`
  constraint, the failed `COPY` rolls back all copied target rows and all
  trigger side effects.
- Observed Doltgres behavior: `COPY` reports the `CHECK` violation, but the
  first copied target row remains persisted and both trigger audit rows remain
  persisted, including the audit row for the rejected input row.

### COPY FROM row-level AFTER INSERT trigger side effects survive later CHECK failures

- Reproducer:
  `TestCopyFromStdinAfterInsertTriggerSideEffectsRollBackOnConstraintErrorRepro`
  in `testing/go/copy_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCopyFromStdinAfterInsertTriggerSideEffectsRollBackOnConstraintErrorRepro
  -count=1`.
- Expected PostgreSQL behavior: if `COPY FROM STDIN` fires row-level
  `AFTER INSERT` triggers and a later copied row violates a `CHECK`
  constraint, the failed `COPY` rolls back all copied target rows and all
  trigger side effects.
- Observed Doltgres behavior: `COPY` reports the `CHECK` violation, but the
  first copied target row and its `AFTER INSERT` trigger audit row remain
  persisted.

### COPY FROM statement-level BEFORE INSERT trigger side effects survive later CHECK failures

- Reproducer:
  `TestCopyFromStdinBeforeStatementTriggerSideEffectsRollBackOnConstraintErrorRepro`
  in `testing/go/copy_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCopyFromStdinBeforeStatementTriggerSideEffectsRollBackOnConstraintErrorRepro
  -count=1`.
- Expected PostgreSQL behavior: if `COPY FROM STDIN` fires a statement-level
  `BEFORE INSERT` trigger and a later copied row violates a `CHECK`
  constraint, the failed `COPY` rolls back both copied target rows and the
  statement trigger side effect.
- Observed Doltgres behavior: `COPY` reports the `CHECK` violation and leaves
  the target table empty, but the statement trigger's audit row remains
  persisted.

### BEFORE trigger exceptions do not roll back trigger side effects

- Reproducer: `TestBeforeInsertTriggerErrorRollsBackSideEffectsRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestBeforeInsertTriggerErrorRollsBackSideEffectsRepro -count=1`.
- Expected PostgreSQL behavior: if a `BEFORE INSERT` trigger writes an audit
  row and then raises an exception, the failed statement rolls back that
  trigger side effect and inserts no base row.
- Observed Doltgres behavior: the base row is not inserted, but the audit row
  written before `RAISE EXCEPTION` remains persisted even though the statement
  failed.

### BEFORE UPDATE trigger exceptions do not roll back trigger side effects

- Reproducer: `TestBeforeUpdateTriggerErrorRollsBackSideEffectsRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestBeforeUpdateTriggerErrorRollsBackSideEffectsRepro -count=1`.
- Expected PostgreSQL behavior: if a `BEFORE UPDATE` trigger writes an audit
  row and then raises an exception, the failed statement rolls back that
  trigger side effect and leaves the target row unchanged.
- Observed Doltgres behavior: the target row remains unchanged, but the audit
  row written before `RAISE EXCEPTION` remains persisted even though the
  statement failed.

### BEFORE DELETE trigger exceptions do not roll back trigger side effects

- Reproducer: `TestBeforeDeleteTriggerErrorRollsBackSideEffectsRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestBeforeDeleteTriggerErrorRollsBackSideEffectsRepro -count=1`.
- Expected PostgreSQL behavior: if a `BEFORE DELETE` trigger writes an audit
  row and then raises an exception, the failed statement rolls back that
  trigger side effect and keeps the target row.
- Observed Doltgres behavior: the target row remains present, but the audit row
  written before `RAISE EXCEPTION` remains persisted even though the statement
  failed.

### Statement-level INSERT trigger exceptions do not roll back trigger side effects

- Reproducer: `TestStatementInsertTriggerErrorRollsBackSideEffectsRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestStatementInsertTriggerErrorRollsBackSideEffectsRepro -count=1`.
- Expected PostgreSQL behavior: if a statement-level `BEFORE INSERT` trigger
  writes an audit row and then raises an exception, the failed statement rolls
  back the trigger side effect and inserts no target row.
- Observed Doltgres behavior: the target row is not inserted, but the
  statement-trigger audit row remains persisted even though the statement
  failed.

### Statement-level UPDATE trigger exceptions do not roll back trigger side effects

- Reproducer: `TestStatementUpdateTriggerErrorRollsBackSideEffectsRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestStatementUpdateTriggerErrorRollsBackSideEffectsRepro -count=1`.
- Expected PostgreSQL behavior: if a statement-level `BEFORE UPDATE` trigger
  writes an audit row and then raises an exception, the failed statement leaves
  the target row unchanged and rolls back the trigger side effect.
- Observed Doltgres behavior: the target row remains unchanged, but the
  statement-trigger audit row remains persisted even though the statement
  failed.

### Statement-level DELETE trigger exceptions do not roll back trigger side effects

- Reproducer: `TestStatementDeleteTriggerErrorRollsBackSideEffectsRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestStatementDeleteTriggerErrorRollsBackSideEffectsRepro -count=1`.
- Expected PostgreSQL behavior: if a statement-level `BEFORE DELETE` trigger
  writes an audit row and then raises an exception, the failed statement keeps
  the target row and rolls back the trigger side effect.
- Observed Doltgres behavior: the target row remains present, but the
  statement-trigger audit row remains persisted even though the statement
  failed.

### BEFORE trigger side effects survive target-row constraint failures

- Reproducer:
  `TestBeforeInsertTriggerSideEffectsRollBackOnConstraintErrorRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestBeforeInsertTriggerSideEffectsRollBackOnConstraintErrorRepro
  -count=1`.
- Expected PostgreSQL behavior: if a `BEFORE INSERT` trigger writes an audit
  row and the target row later fails a table `CHECK` constraint, the failed
  statement rolls back both the rejected base-row write and the trigger's audit
  write.
- Observed Doltgres behavior: the target row is rejected by the `CHECK`
  constraint, but the audit row written by the `BEFORE` trigger remains
  persisted.

### BEFORE UPDATE trigger side effects survive target-row constraint failures

- Reproducer:
  `TestBeforeUpdateTriggerSideEffectsRollBackOnConstraintErrorRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestBeforeUpdateTriggerSideEffectsRollBackOnConstraintErrorRepro
  -count=1`.
- Expected PostgreSQL behavior: if a `BEFORE UPDATE` trigger writes an audit
  row and the updated target row later fails a table `CHECK` constraint, the
  failed statement rolls back the row update and the trigger's audit write.
- Observed Doltgres behavior: the target row remains unchanged after the
  `CHECK` failure, but the audit row written by the `BEFORE UPDATE` trigger
  remains persisted.

### Statement-level trigger side effects survive target-row constraint failures

- Reproducer:
  `TestStatementTriggerSideEffectsRollBackOnConstraintErrorRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestStatementTriggerSideEffectsRollBackOnConstraintErrorRepro -count=1`.
- Expected PostgreSQL behavior: if a statement-level `BEFORE INSERT` trigger
  writes an audit row and the triggering insert later fails a table `CHECK`
  constraint, the failed statement rolls back the trigger side effect.
- Observed Doltgres behavior: the target row is rejected by the `CHECK`
  constraint, but the statement-trigger audit row remains persisted.

### Statement-level UPDATE trigger side effects survive target-row constraint failures

- Reproducer:
  `TestStatementUpdateTriggerSideEffectsRollBackOnConstraintErrorRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestStatementUpdateTriggerSideEffectsRollBackOnConstraintErrorRepro
  -count=1`.
- Expected PostgreSQL behavior: if a statement-level `BEFORE UPDATE` trigger
  writes an audit row and the triggering update later fails a table `CHECK`
  constraint, the failed statement leaves the target row unchanged and rolls
  back the trigger side effect.
- Observed Doltgres behavior: the target row remains unchanged after the
  `CHECK` failure, but the statement-trigger audit row remains persisted.

### Statement-level DELETE trigger side effects survive foreign-key failures

- Reproducer:
  `TestStatementDeleteTriggerSideEffectsRollBackOnForeignKeyErrorRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestStatementDeleteTriggerSideEffectsRollBackOnForeignKeyErrorRepro
  -count=1`.
- Expected PostgreSQL behavior: if a statement-level `BEFORE DELETE` trigger
  writes an audit row and the triggering delete later fails a foreign-key
  constraint, the failed statement keeps the parent row and rolls back the
  trigger side effect.
- Observed Doltgres behavior: the parent row remains after the foreign-key
  error, but the statement-trigger audit row remains persisted.

### BEFORE DELETE row trigger side effects survive foreign-key failures

- Reproducer:
  `TestBeforeDeleteRowTriggerSideEffectsRollBackOnForeignKeyErrorRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestBeforeDeleteRowTriggerSideEffectsRollBackOnForeignKeyErrorRepro
  -count=1`.
- Expected PostgreSQL behavior: if a row-level `BEFORE DELETE` trigger writes
  an audit row and the delete later fails a foreign-key constraint, the failed
  statement keeps the parent row and rolls back the trigger side effect.
- Observed Doltgres behavior: the parent row remains after the foreign-key
  error, but the row-trigger audit row remains persisted.

### BEFORE UPDATE row trigger side effects survive foreign-key failures

- Reproducer:
  `TestBeforeUpdateRowTriggerSideEffectsRollBackOnForeignKeyErrorRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestBeforeUpdateRowTriggerSideEffectsRollBackOnForeignKeyErrorRepro
  -count=1`.
- Expected PostgreSQL behavior: if a row-level `BEFORE UPDATE` trigger writes
  an audit row and the update later fails a foreign-key constraint, the failed
  statement leaves the parent row unchanged and rolls back the trigger side
  effect.
- Observed Doltgres behavior: the parent row remains unchanged after the
  foreign-key error, but the row-trigger audit row remains persisted.

### UPDATE FROM bypasses target CHECK constraints

- Reproducer: `TestUpdateFromCheckConstraintIsStatementAtomicRepro` in
  `testing/go/statement_atomicity_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestUpdateFromCheckConstraintIsStatementAtomicRepro -count=1`.
- Expected PostgreSQL behavior: `UPDATE ... FROM` enforces target-table
  `CHECK` constraints for every updated row; if any joined row would violate a
  check, the statement fails atomically and none of its earlier updates persist.
- Observed Doltgres behavior: `UPDATE atomic_update_from_items AS t SET qty =
  s.new_qty FROM atomic_update_from_source AS s WHERE t.id = s.id` succeeds and
  persists `qty = -1`, bypassing `CHECK (qty > 0)`.

### PL/pgSQL table-typed trigger variables cannot access row fields

- Reproducer: `TestBeforeInsertTriggerTableTypedRecordAssignmentRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestBeforeInsertTriggerTableTypedRecordAssignmentRepro -count=1`.
- Expected PostgreSQL behavior: a trigger function can declare a variable with
  the table's row type, assign `NEW` into it, access fields such as `rec.qty`,
  assign the record back to `NEW`, and persist the modified row.
- Observed Doltgres behavior: creating the trigger function fails with
  `"rec.qty" is not a known variable`, so accepted PostgreSQL trigger code that
  normalizes inserted data cannot be installed.

### PL/pgSQL trigger arguments are not available through TG_ARGV

- Reproducer: `TestTriggerArgumentsPopulateTgArgvRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTriggerArgumentsPopulateTgArgvRepro -count=1`.
- Expected PostgreSQL behavior: trigger arguments passed in `CREATE TRIGGER ...
  EXECUTE FUNCTION fn('alpha', 'beta')` are visible inside the trigger
  function as `TG_NARGS = 2`, `TG_ARGV[0] = 'alpha'`, and
  `TG_ARGV[1] = 'beta'`.
- Observed Doltgres behavior: the trigger is accepted and `TG_NARGS` is
  populated, but executing the trigger fails with `column "tg_argv" could not
  be found in any table in scope`, so argument-driven trigger logic cannot run.

### INSTEAD OF triggers on views are rejected

- Reproducer: `TestInsteadOfInsertTriggerOnViewRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestInsteadOfInsertTriggerOnViewRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE TRIGGER ... INSTEAD OF INSERT ON
  <view>` is accepted for a row-level view trigger, and inserting into the view
  runs the trigger body to route or transform the write.
- Observed Doltgres behavior: trigger creation fails with `ERROR: INSTEAD OF is
  not yet supported for CREATE TRIGGER Please file an issue at
  https://github.com/dolthub/doltgresql/issues (SQLSTATE 0A000)`, so explicit
  view write routing cannot be installed.

### ALTER TABLE DISABLE/ENABLE TRIGGER is rejected

- Reproducer: `TestAlterTableDisableEnableTriggerRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestAlterTableDisableEnableTriggerRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... DISABLE TRIGGER <name>`
  disables an existing user trigger, and `ALTER TABLE ... ENABLE TRIGGER
  <name>` re-enables it.
- Observed Doltgres behavior: both statements fail with `ERROR: ALTER TABLE
  with unsupported command type *tree.AlterTableTrigger (SQLSTATE XX000)`, so
  a trigger that should be disabled still fires and writes audit rows.

### ALTER TRIGGER RENAME TO is rejected

- Reproducer: `TestAlterTriggerRenameRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestAlterTriggerRenameRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TRIGGER old_name ON table_name RENAME
  TO new_name` renames the existing trigger, persists the new name in
  `pg_catalog.pg_trigger`, and leaves the trigger behavior attached to the
  table.
- Observed Doltgres behavior: the statement fails with `ERROR: unknown
  statement type encountered: *tree.AlterTrigger (SQLSTATE XX000)`, and
  `pg_trigger` continues reporting the old trigger name. Trigger-renaming
  migrations therefore cannot run.

### Column-specific UPDATE triggers are rejected

- Reproducer: `TestUpdateOfGeneratedColumnFiresForBaseColumnChangeRepro` in
  `testing/go/generated_column_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestUpdateOfGeneratedColumnFiresForBaseColumnChangeRepro
  -count=1`.
- Expected PostgreSQL behavior: `CREATE TRIGGER ... UPDATE OF <column>` is
  accepted, and for stored generated columns the trigger fires when an update
  to a base column changes the generated value.
- Observed Doltgres behavior: trigger creation fails with `UPDATE specific
  columns are not yet supported for CREATE TRIGGER`, so column-specific update
  audit hooks cannot be installed.

### Direct trigger-function calls panic instead of returning a trigger-context error

- Reproducer: `TestTriggerFunctionCannotBeCalledDirectlyRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestTriggerFunctionCannotBeCalledDirectlyRepro -count=1`.
- Expected PostgreSQL behavior: a function declared `RETURNS TRIGGER` cannot be
  invoked as a scalar function; `SELECT trigger_func()` fails with `trigger
  functions can only be called as triggers`.
- Observed Doltgres behavior: the direct call enters the PL/pgSQL trigger
  execution path without trigger context and fails with `DoltgresHandler caught
  panic: runtime error: invalid memory address or nil pointer dereference`
  from `server/plpgsql/interpreter_logic.go`, returning an internal error
  instead of PostgreSQL's trigger-context validation error.

### Deferrable constraint triggers are rejected

- Reproducer: `TestDeferrableConstraintTriggerFiresAtCommitRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestDeferrableConstraintTriggerFiresAtCommitRepro
  -count=1`.
- Expected PostgreSQL behavior: `CREATE CONSTRAINT TRIGGER ... DEFERRABLE
  INITIALLY DEFERRED` installs an `AFTER ROW` constraint trigger whose action
  can be deferred until transaction commit, so constraint/audit logic observes
  PostgreSQL's commit-time trigger boundary.
- Observed Doltgres behavior: setup fails with `CREATE CONSTRAINT TRIGGER is
  not yet supported`, so applications cannot define deferrable trigger-backed
  integrity checks or audit hooks.

### Whole-row trigger WHEN predicates cannot compare OLD.* and NEW.*

- Reproducer: `TestUpdateTriggerWhenWholeRowDistinctRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestUpdateTriggerWhenWholeRowDistinctRepro -count=1`.
- Expected PostgreSQL behavior: an update trigger can use `WHEN (OLD.* IS
  DISTINCT FROM NEW.*)` to run a generic audit trigger only when the full row
  actually changes.
- Observed Doltgres behavior: trigger creation succeeds, but both matching
  updates fail at execution with ``variable `OLD.*` could not be found``.
  The generic audit predicate cannot be evaluated, so the changed-row audit
  trigger never records the update.

### Event trigger audit functions cannot be created

- Reproducer: `TestEventTriggerAuditsDdlCommandRepro` in
  `testing/go/trigger_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestEventTriggerAuditsDdlCommandRepro -count=1`.
- Expected PostgreSQL behavior: a function returning `event_trigger` can be
  installed and attached to `ddl_command_end`, allowing DDL such as
  `CREATE TABLE` to be audited or guarded.
- Observed Doltgres behavior: creating the event-trigger function fails with
  `type "event_trigger" does not exist`, so DDL audit hooks cannot be installed
  before `CREATE EVENT TRIGGER` itself is reached.

### Foreign keys accept incompatible float-to-integer references

- Reproducer: `TestForeignKeyRejectsFloatReferencingIntegerRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestForeignKeyRejectsFloatReferencingIntegerRepro -count=1`.
- Expected PostgreSQL behavior: a `FLOAT` referencing column cannot form a
  foreign key to an `INT` referenced column because the types are incompatible
  for referential integrity.
- Observed Doltgres behavior: the invalid foreign key is accepted.

### Typmod foreign keys use uncoerced values

- Reproducer: `TestTypmodForeignKeyUsesCoercedValuesRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTypmodForeignKeyUsesCoercedValuesRepro -count=1`.
- Expected PostgreSQL behavior: foreign keys are checked after applying the
  declared typmods. A child `TIMESTAMP(0)` value of
  `2021-09-15 21:43:56.700` can reference a parent value inserted as
  `2021-09-15 21:43:56.600` because both store as `2021-09-15 21:43:57`. The
  same applies to domain values over `numeric(5,2)` that both round to `1.23`.
- Observed Doltgres behavior: both child inserts fail with foreign-key
  violations because Doltgres probes the parent key using the uncoerced child
  value, rejecting referentially-valid PostgreSQL rows.

### Domain foreign-key columns cannot reference base-type keys

- Reproducer: `TestForeignKeyDomainColumnReferencesBaseTypeRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestForeignKeyDomainColumnReferencesBaseTypeRepro -count=1`.
- Expected PostgreSQL behavior: a child foreign-key column declared as a domain
  over `INT` may reference a parent `INT` primary key, and valid domain values
  can be inserted and read back.
- Observed Doltgres behavior: `CREATE TABLE domain_fk_base_child (...)` fails
  with `Key columns "parent_id" and "id" are of incompatible types:
  positive_fk_child_domain and integer`, so valid foreign keys between domains
  and their base types cannot be created.

### Composite foreign keys reject MATCH FULL

- Reproducer: `TestCompositeForeignKeyMatchFullRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCompositeForeignKeyMatchFullRepro -count=1`.
- Expected PostgreSQL behavior: composite foreign keys support `MATCH FULL`,
  which allows all referencing columns to be `NULL` together but rejects
  partially-null referencing rows.
- Observed Doltgres behavior: creating the composite foreign key fails with
  `MATCH FULL on composite foreign keys is not yet supported`, so valid
  PostgreSQL schemas that rely on full-null referential integrity are rejected.

### ON DELETE SET NULL column lists are rejected

- Reproducer: `TestForeignKeyOnDeleteSetNullColumnListRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestForeignKeyOnDeleteSetNullColumnListRepro -count=1`.
- Expected PostgreSQL behavior: a composite foreign key can use `ON DELETE SET
  NULL (parent_id)` to clear only the nullable referencing column while
  preserving a separate non-null tenant or partition key.
- Observed Doltgres behavior: child-table creation fails with `ERROR: SET NULL
  <columns> is not yet supported (SQLSTATE 0A000)`, so schemas cannot express
  selective nulling referential actions.

### ON UPDATE SET NULL column lists are rejected

- Reproducer: `TestForeignKeyOnUpdateSetNullColumnListRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestForeignKeyOnUpdateSetNullColumnListRepro -count=1`.
- Expected PostgreSQL behavior: a composite foreign key can use `ON UPDATE SET
  NULL (parent_id)` to clear only the nullable referencing column while
  preserving a separate non-null tenant or partition key.
- Observed Doltgres behavior: child-table creation fails with `ERROR: SET NULL
  <columns> is not yet supported (SQLSTATE 0A000)`, so schemas cannot express
  selective nulling update actions.

### ON DELETE SET DEFAULT column lists are rejected

- Reproducer: `TestForeignKeyOnDeleteSetDefaultColumnListRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestForeignKeyOnDeleteSetDefaultColumnListRepro -count=1`.
- Expected PostgreSQL behavior: a composite foreign key can use `ON DELETE SET
  DEFAULT (parent_id)` to default only the selected referencing column while
  preserving a separate non-null tenant or partition key.
- Observed Doltgres behavior: child-table creation fails with `ERROR: SET
  DEFAULT <columns> is not yet supported (SQLSTATE 0A000)`, so schemas cannot
  express selective defaulting delete actions.

### ON UPDATE SET DEFAULT column lists are rejected

- Reproducer: `TestForeignKeyOnUpdateSetDefaultColumnListRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestForeignKeyOnUpdateSetDefaultColumnListRepro -count=1`.
- Expected PostgreSQL behavior: a composite foreign key can use `ON UPDATE SET
  DEFAULT (parent_id)` to default only the selected referencing column while
  preserving a separate non-null tenant or partition key.
- Observed Doltgres behavior: child-table creation fails with `ERROR: SET
  DEFAULT <columns> is not yet supported (SQLSTATE 0A000)`, so schemas cannot
  express selective defaulting update actions.

### ON DELETE SET NULL bypasses child CHECK constraints

- Reproducer: `TestForeignKeyOnDeleteSetNullValidatesCheckConstraintRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run
  TestForeignKeyOnDeleteSetNullValidatesCheckConstraintRepro -count=1`.
- Expected PostgreSQL behavior: referential `ON DELETE SET NULL` rewrites still
  validate child-table constraints; deleting parent `1` fails when the child
  row would violate `CHECK (parent_id IS NOT NULL)`.
- Observed Doltgres behavior: deleting the parent succeeds and rewrites the
  child row to `parent_id = NULL`, bypassing the child `CHECK` constraint and
  persisting an invalid child row.

### ON UPDATE SET NULL bypasses child CHECK constraints

- Reproducer: `TestForeignKeyOnUpdateSetNullValidatesCheckConstraintRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run
  TestForeignKeyOnUpdateSetNullValidatesCheckConstraintRepro -count=1`.
- Expected PostgreSQL behavior: referential `ON UPDATE SET NULL` rewrites still
  validate child-table constraints; updating parent key `1` fails when the
  child row would violate `CHECK (parent_id IS NOT NULL)`.
- Observed Doltgres behavior: updating the parent key succeeds and rewrites the
  child row to `parent_id = NULL`, bypassing the child `CHECK` constraint and
  persisting an invalid child row.

### ON DELETE SET NULL leaves stored generated columns stale

- Reproducer: `TestForeignKeyOnDeleteSetNullRecomputesGeneratedColumnsRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestForeignKeyOnDeleteSetNullRecomputesGeneratedColumnsRepro
  -count=1`.
- Expected PostgreSQL behavior: referential `ON DELETE SET NULL` rewrites are
  ordinary child-row updates for stored generated columns; after `parent_id`
  becomes `NULL`, `parent_marker GENERATED ALWAYS AS (parent_id + 10)` becomes
  `NULL`.
- Observed Doltgres behavior: `parent_id` is set to `NULL`, but the stored
  generated column remains at the old value `11`.

### ON UPDATE SET NULL leaves stored generated columns stale

- Reproducer: `TestForeignKeyOnUpdateSetNullRecomputesGeneratedColumnsRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestForeignKeyOnUpdateSetNullRecomputesGeneratedColumnsRepro
  -count=1`.
- Expected PostgreSQL behavior: referential `ON UPDATE SET NULL` rewrites are
  ordinary child-row updates for stored generated columns; after `parent_id`
  becomes `NULL`, `parent_marker GENERATED ALWAYS AS (parent_id + 10)` becomes
  `NULL`.
- Observed Doltgres behavior: `parent_id` is set to `NULL`, but the stored
  generated column remains at the old value `11`.

### ON DELETE SET DEFAULT can persist invalid foreign-key values

- Reproducer: `TestForeignKeyOnDeleteSetDefaultValidatesDefaultRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestForeignKeyOnDeleteSetDefaultValidatesDefaultRepro
  -count=1`.
- Expected PostgreSQL behavior: `ON DELETE SET DEFAULT` still enforces the
  foreign key after applying the default; deleting parent `1` fails when the
  child default is `0` and no parent `0` exists.
- Observed Doltgres behavior: deleting the parent succeeds and rewrites the
  child row to `parent_id = 0`, leaving a persisted child reference with no
  matching parent row.

### ON UPDATE SET DEFAULT can persist invalid foreign-key values

- Reproducer: `TestForeignKeyOnUpdateSetDefaultValidatesDefaultRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestForeignKeyOnUpdateSetDefaultValidatesDefaultRepro
  -count=1`.
- Expected PostgreSQL behavior: `ON UPDATE SET DEFAULT` still enforces the
  foreign key after applying the default; updating parent key `1` fails when
  the child default is `0` and no parent `0` exists.
- Observed Doltgres behavior: updating the parent key succeeds and rewrites the
  child row to `parent_id = 0`, leaving a persisted child reference with no
  matching parent row.

### ON DELETE SET DEFAULT bypasses child CHECK constraints

- Reproducer: `TestForeignKeyOnDeleteSetDefaultValidatesCheckConstraintRepro`
  in `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run
  TestForeignKeyOnDeleteSetDefaultValidatesCheckConstraintRepro -count=1`.
- Expected PostgreSQL behavior: referential `ON DELETE SET DEFAULT` rewrites
  still validate child-table constraints; deleting parent `1` fails when the
  default `0` has a matching parent row but violates child `CHECK (parent_id >
  0)`.
- Observed Doltgres behavior: deleting the parent succeeds and rewrites the
  child row to `parent_id = 0`, bypassing the child `CHECK` constraint and
  persisting an invalid child row.

### ON UPDATE SET DEFAULT bypasses child CHECK constraints

- Reproducer: `TestForeignKeyOnUpdateSetDefaultValidatesCheckConstraintRepro`
  in `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run
  TestForeignKeyOnUpdateSetDefaultValidatesCheckConstraintRepro -count=1`.
- Expected PostgreSQL behavior: referential `ON UPDATE SET DEFAULT` rewrites
  still validate child-table constraints; updating parent key `1` fails when
  the default `0` has a matching parent row but violates child `CHECK
  (parent_id > 0)`.
- Observed Doltgres behavior: updating the parent key succeeds and rewrites the
  child row to `parent_id = 0`, bypassing the child `CHECK` constraint and
  persisting an invalid child row.

### ON DELETE SET DEFAULT leaves stored generated columns stale

- Reproducer:
  `TestForeignKeyOnDeleteSetDefaultRecomputesGeneratedColumnsRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run
  TestForeignKeyOnDeleteSetDefaultRecomputesGeneratedColumnsRepro -count=1`.
- Expected PostgreSQL behavior: referential `ON DELETE SET DEFAULT` rewrites
  are ordinary child-row updates for stored generated columns; after
  `parent_id` becomes its default `0`, `parent_marker GENERATED ALWAYS AS
  (parent_id + 10)` becomes `10`.
- Observed Doltgres behavior: `parent_id` is set to `0`, but the stored
  generated column remains at the old value `11`.

### ON UPDATE SET DEFAULT leaves stored generated columns stale

- Reproducer:
  `TestForeignKeyOnUpdateSetDefaultRecomputesGeneratedColumnsRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run
  TestForeignKeyOnUpdateSetDefaultRecomputesGeneratedColumnsRepro -count=1`.
- Expected PostgreSQL behavior: referential `ON UPDATE SET DEFAULT` rewrites
  are ordinary child-row updates for stored generated columns; after
  `parent_id` becomes its default `0`, `parent_marker GENERATED ALWAYS AS
  (parent_id + 10)` becomes `10`.
- Observed Doltgres behavior: `parent_id` is set to `0`, but the stored
  generated column remains at the old value `11`.

### ON UPDATE CASCADE bypasses child CHECK constraints

- Reproducer: `TestForeignKeyOnUpdateCascadeValidatesCheckConstraintRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run
  TestForeignKeyOnUpdateCascadeValidatesCheckConstraintRepro -count=1`.
- Expected PostgreSQL behavior: referential `ON UPDATE CASCADE` rewrites still
  validate child-table constraints; updating parent key `1` to `11` fails when
  the child row would violate `CHECK (parent_id < 10)`.
- Observed Doltgres behavior: updating the parent key succeeds and cascades the
  child row to `parent_id = 11`, bypassing the child `CHECK` constraint and
  persisting an invalid child row.

### ON UPDATE CASCADE leaves stored generated columns stale

- Reproducer: `TestForeignKeyOnUpdateCascadeRecomputesGeneratedColumnsRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestForeignKeyOnUpdateCascadeRecomputesGeneratedColumnsRepro
  -count=1`.
- Expected PostgreSQL behavior: referential `ON UPDATE CASCADE` rewrites are
  ordinary child-row updates for stored generated columns; after `parent_id`
  cascades from `1` to `3`, `parent_marker GENERATED ALWAYS AS (parent_id +
  10)` becomes `13`.
- Observed Doltgres behavior: `parent_id` is updated to `3`, but the stored
  generated column remains at the old value `11`.

### ON UPDATE CASCADE does not fire child UPDATE triggers

- Reproducer: `TestForeignKeyOnUpdateCascadeFiresChildUpdateTriggersRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run
  TestForeignKeyOnUpdateCascadeFiresChildUpdateTriggersRepro -count=1`.
- Expected PostgreSQL behavior: referential `ON UPDATE CASCADE` rewrites are
  ordinary child-row updates for row-level triggers; updating parent key `1` to
  `3` fires the child table's `AFTER UPDATE` trigger with old parent `1` and
  new parent `3`.
- Observed Doltgres behavior: the child row is rewritten to `parent_id = 3`,
  but the audit table remains empty because the child `AFTER UPDATE` trigger
  does not fire.

### ON DELETE CASCADE does not fire child DELETE triggers

- Reproducer: `TestForeignKeyOnDeleteCascadeFiresChildDeleteTriggersRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run
  TestForeignKeyOnDeleteCascadeFiresChildDeleteTriggersRepro -count=1`.
- Expected PostgreSQL behavior: referential `ON DELETE CASCADE` child-row
  deletes fire row-level `DELETE` triggers on the child table; deleting parent
  key `1` logs the deleted child row with old parent `1`.
- Observed Doltgres behavior: the child row is deleted, but the audit table
  remains empty because the child `AFTER DELETE` trigger does not fire.

### ON DELETE SET NULL does not fire child UPDATE triggers

- Reproducer: `TestForeignKeyOnDeleteSetNullFiresChildUpdateTriggersRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run
  TestForeignKeyOnDeleteSetNullFiresChildUpdateTriggersRepro -count=1`.
- Expected PostgreSQL behavior: referential `ON DELETE SET NULL` rewrites are
  ordinary child-row updates for row-level triggers; deleting parent key `1`
  fires the child table's `AFTER UPDATE` trigger with old parent `1` and new
  parent `NULL`.
- Observed Doltgres behavior: the child row is rewritten to `parent_id = NULL`,
  but the audit table remains empty because the child `AFTER UPDATE` trigger
  does not fire.

### ON DELETE SET DEFAULT does not fire child UPDATE triggers

- Reproducer:
  `TestForeignKeyOnDeleteSetDefaultFiresChildUpdateTriggersRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run
  TestForeignKeyOnDeleteSetDefaultFiresChildUpdateTriggersRepro -count=1`.
- Expected PostgreSQL behavior: referential `ON DELETE SET DEFAULT` rewrites
  are ordinary child-row updates for row-level triggers; deleting parent key
  `1` fires the child table's `AFTER UPDATE` trigger with old parent `1` and
  new parent `0`.
- Observed Doltgres behavior: the child row is rewritten to `parent_id = 0`,
  but the audit table remains empty because the child `AFTER UPDATE` trigger
  does not fire.

### ON UPDATE SET DEFAULT does not fire child UPDATE triggers

- Reproducer:
  `TestForeignKeyOnUpdateSetDefaultFiresChildUpdateTriggersRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run
  TestForeignKeyOnUpdateSetDefaultFiresChildUpdateTriggersRepro -count=1`.
- Expected PostgreSQL behavior: referential `ON UPDATE SET DEFAULT` rewrites
  are ordinary child-row updates for row-level triggers; updating parent key
  `1` fires the child table's `AFTER UPDATE` trigger with old parent `1` and
  new parent `0`.
- Observed Doltgres behavior: the child row is rewritten to `parent_id = 0`,
  but the audit table remains empty because the child `AFTER UPDATE` trigger
  does not fire.

### TRUNCATE CASCADE does not truncate referencing tables

- Reproducer: `TestTruncateReferencedTableCascadeTruncatesChildrenRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestTruncateReferencedTableCascadeTruncatesChildrenRepro -count=1`.
- Expected PostgreSQL behavior: `TRUNCATE fk_truncate_cascade_parent CASCADE`
  truncates the referenced parent table and the dependent child table in the
  same operation.
- Observed Doltgres behavior: the statement fails with `CASCADE is not yet
  supported`, and both parent and child rows remain.

### DROP COLUMN allows removing referenced foreign-key columns

- Reproducer: `TestDropReferencedColumnRequiresCascadeRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDropReferencedColumnRequiresCascadeRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE fk_drop_column_parent DROP COLUMN
  id` fails under default `RESTRICT` behavior because a child-table foreign key
  references that column.
- Observed Doltgres behavior: the referenced key column is dropped
  successfully, leaving the child foreign-key dependency unprotected.

### DROP TABLE CASCADE does not remove dependent foreign keys

- Reproducer: `TestDropReferencedTableCascadeDropsForeignKeyRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDropReferencedTableCascadeDropsForeignKeyRepro -count=1`.
- Expected PostgreSQL behavior: `DROP TABLE fk_drop_cascade_parent CASCADE`
  drops the referenced parent table and removes the dependent foreign-key
  constraint, leaving the referencing table usable.
- Observed Doltgres behavior: the statement is rejected as though `CASCADE` had
  not been supplied, the parent table and foreign-key constraint remain, and a
  subsequent insert into the child table is still checked against the old
  parent.

### DROP CONSTRAINT allows removing primary keys still referenced by foreign keys

- Reproducer: `TestDropReferencedPrimaryKeyConstraintRequiresCascadeRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDropReferencedPrimaryKeyConstraintRequiresCascadeRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... DROP CONSTRAINT` fails under
  default `RESTRICT` behavior when the primary-key constraint is still
  referenced by a child-table foreign key.
- Observed Doltgres behavior: the referenced primary-key constraint is dropped
  successfully, leaving the child foreign-key dependency unprotected.

### DROP CONSTRAINT CASCADE does not remove dependent foreign keys

- Reproducers:
  `TestDropReferencedPrimaryKeyConstraintCascadeDropsForeignKeyRepro` and
  `TestDropReferencedUniqueConstraintCascadeDropsForeignKeyRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'TestDropReferenced(PrimaryKey|Unique)ConstraintCascadeDropsForeignKeyRepro'
  -count=1`.
- Expected PostgreSQL behavior: dropping a referenced primary-key or unique
  constraint with `CASCADE` removes dependent foreign-key constraints and leaves
  the child table usable.
- Observed Doltgres behavior: the statement fails with `CASCADE is not yet
  supported for drop constraint`, the dependent foreign key remains, and a later
  child insert is still rejected against the old reference.

### DROP INDEX allows removing standalone unique indexes still referenced by foreign keys

- Reproducer: `TestDropReferencedStandaloneUniqueIndexRequiresCascadeRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDropReferencedStandaloneUniqueIndexRequiresCascadeRepro -count=1`.
- Expected PostgreSQL behavior: `DROP INDEX referenced_unique_index` fails under
  default `RESTRICT` behavior when a child-table foreign key depends on that
  standalone unique index; the index remains and continues rejecting duplicate
  parent keys.
- Observed Doltgres behavior: the referenced unique index is dropped while the
  dependent foreign-key constraint remains, so duplicate parent key values can be
  inserted even though the foreign key still exists.

### DROP INDEX CASCADE does not remove dependent foreign keys

- Reproducer: `TestDropReferencedUniqueIndexCascadeDropsForeignKeyRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDropReferencedUniqueIndexCascadeDropsForeignKeyRepro -count=1`.
- Expected PostgreSQL behavior: `DROP INDEX referenced_unique_index CASCADE`
  drops the standalone referenced unique index and removes dependent foreign-key
  constraints, leaving the child table usable without the old reference check.
- Observed Doltgres behavior: the referenced unique index is dropped, but the
  dependent foreign-key constraint remains in `pg_constraint` and continues
  rejecting child rows against the old reference.

### DROP CONSTRAINT loses explicit schema for foreign-key child tables

- Reproducer: `TestDropForeignKeyConstraintWithExplicitSchemaRepro` in
  `testing/go/foreign_key_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDropForeignKeyConstraintWithExplicitSchemaRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE explicit_fk_child.child_items DROP
  CONSTRAINT child_items_parent_id_fkey` resolves the schema-qualified child
  table and removes the foreign-key constraint.
- Observed Doltgres behavior: the statement fails with `table not found:
  child_items`, indicating the explicit schema was lost during resolution, and
  the foreign key continues rejecting rows.

### Generated columns accept prohibited expressions

- Reproducers: `TestGeneratedColumnRejectsVolatileFunctionsRepro`,
  `TestGeneratedColumnRejectsSelfReferenceRepro`,
  `TestGeneratedColumnRejectsGeneratedColumnReferenceRepro`,
  `TestGeneratedColumnRejectsAggregateExpressionsRepro`,
  `TestGeneratedColumnRejectsWindowExpressionsRepro`, and
  `TestGeneratedColumnRejectsSetReturningExpressionsRepro` in
  `testing/go/generated_column_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'TestGeneratedColumnRejects(VolatileFunctions|SelfReference|GeneratedColumnReference|AggregateExpressions|WindowExpressions|SetReturningExpressions)Repro'
  -count=1`.
- Expected PostgreSQL behavior: stored generated column expressions reject
  volatile functions, self references, references to another generated column,
  aggregate functions, window functions, and set-returning functions.
- Observed Doltgres behavior: each invalid table definition is accepted,
  allowing prohibited expressions to define persisted generated data.

### Generated columns accept duplicate generation clauses

- Reproducer: `TestGeneratedColumnRejectsMalformedReferencesGuard` in
  `testing/go/generated_column_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestGeneratedColumnRejectsMalformedReferencesGuard -count=1`.
- Expected PostgreSQL behavior: a column cannot specify more than one stored
  generation clause; `GENERATED ALWAYS AS (...) STORED GENERATED ALWAYS AS
  (...) STORED` fails with `multiple generation clauses specified`.
- Observed Doltgres behavior: the duplicate generation-clause table definition
  succeeds, allowing ambiguous persisted generated-column metadata.

### Generated columns reject allowed `tableoid` references

- Reproducer: `TestGeneratedColumnAllowsTableoidReferenceRepro` in
  `testing/go/generated_column_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestGeneratedColumnAllowsTableoidReferenceRepro -count=1`.
- Expected PostgreSQL behavior: stored generated columns may reference
  `tableoid`; a generated boolean can compare `tableoid` to the table's
  `regclass`, and a generated `regclass` column can store `tableoid`.
- Observed Doltgres behavior: creating the table fails with `column "tableoid"
  could not be found in any table in scope`, so valid generated-column schemas
  using PostgreSQL's allowed system-column exception cannot be restored.

### Generated columns accept identity plus generation expressions

- Reproducer: `TestGeneratedColumnRejectsConflictingGenerationClausesRepro` in
  `testing/go/generated_column_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestGeneratedColumnRejectsConflictingGenerationClausesRepro -count=1`.
- Expected PostgreSQL behavior: a column cannot be both an identity column and
  a stored generated column; `GENERATED ALWAYS AS IDENTITY GENERATED ALWAYS AS
  (...) STORED` fails with `both identity and generation expression specified`.
- Observed Doltgres behavior: the invalid table definition succeeds, allowing a
  persisted column definition with conflicting identity and stored generated
  semantics.

### Generated columns accept invalid foreign-key referential actions

- Reproducers: `TestGeneratedColumnRejectsOnUpdateCascadeReferenceRepro` and
  `TestGeneratedColumnRejectsOnDeleteSetNullReferenceRepro` in
  `testing/go/generated_column_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'TestGeneratedColumnRejectsOn(UpdateCascade|DeleteSetNull)ReferenceRepro'
  -count=1`.
- Expected PostgreSQL behavior: a generated column cannot be the referencing
  column of a foreign key with `ON UPDATE CASCADE` or `ON DELETE SET NULL`,
  because those actions would try to modify a generated value.
- Observed Doltgres behavior: both invalid table definitions are accepted.

### COPY FROM generated-column rejection poisons the session

- Reproducer: `TestCopyFromGeneratedColumnErrorKeepsSessionUsableRepro` in
  `testing/go/generated_column_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCopyFromGeneratedColumnErrorKeepsSessionUsableRepro -count=1`.
- Expected PostgreSQL behavior: `COPY table (base_col, generated_col) FROM
  STDIN` is rejected because generated columns cannot be loaded directly, but
  the same session remains usable and a follow-up `SELECT count(*)` returns
  zero persisted rows.
- Observed Doltgres behavior: `COPY FROM STDIN` reports the generated-column
  error, but the following query on the same session fails with the same
  generated-column error/read failure instead of returning the table contents.

### ALTER COLUMN SET/DROP NOT NULL corrupts generated column expressions

- Reproducers: `TestAlterGeneratedColumnSetNotNullEnforcesGeneratedValueRepro`
  and `TestAlterGeneratedColumnDropNotNullPreservesGeneratedValueRepro` in
  `testing/go/generated_column_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestAlterGeneratedColumn(SetNotNullEnforces|DropNotNullPreserves)GeneratedValueRepro'
  -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... ALTER COLUMN generated_col SET
  NOT NULL` and `DROP NOT NULL` change only nullability while preserving the
  stored generation expression; later inserts whose generated value is valid
  continue to compute and persist generated values.
- Observed Doltgres behavior: both `ALTER TABLE` variants succeed, but later
  valid inserts fail with `Invalid default value for
  '(nullif(...base_value,0 as __doltgres_anon__?column?...))': at or near
  "as": syntax error`, indicating the generated expression metadata was
  rewritten into invalid SQL.

### BEFORE triggers can persist explicit values into stored generated columns

- Reproducer: `TestBeforeTriggerGeneratedColumnAssignmentIsIgnoredRepro` in
  `testing/go/generated_column_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestBeforeTriggerGeneratedColumnAssignmentIsIgnoredRepro
  -count=1`.
- Expected PostgreSQL behavior: stored generated columns are computed after
  `BEFORE` triggers run, and an assignment to `NEW.<generated_column>` inside
  the trigger does not propagate to storage.
- Observed Doltgres behavior: the trigger assigns `NEW.base_value := 10` and
  `NEW.doubled := 999`; the row persists `(base_value, doubled) = (10, 999)`
  instead of recomputing the generated value as `20`.

### BEFORE trigger WHEN clauses can reference NEW generated columns

- Reproducer: `TestBeforeTriggerWhenCannotReferenceNewGeneratedColumnRepro` in
  `testing/go/generated_column_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestBeforeTriggerWhenCannotReferenceNewGeneratedColumnRepro
  -count=1`.
- Expected PostgreSQL behavior: a `BEFORE INSERT` or `BEFORE UPDATE` trigger's
  `WHEN` predicate cannot reference `NEW` generated columns because generated
  values are not computed until after `BEFORE` triggers.
- Observed Doltgres behavior: `CREATE TRIGGER ... BEFORE INSERT ... WHEN
  (NEW.doubled > 10)` succeeds, so the trigger timing rule for generated
  columns is not enforced.

### DROP COLUMN allows removing columns still used by generated columns

- Reproducer: `TestDropColumnUsedByGeneratedColumnRequiresCascadeRepro` in
  `testing/go/generated_column_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDropColumnUsedByGeneratedColumnRequiresCascadeRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... DROP COLUMN base_value` fails
  under default `RESTRICT` behavior when a stored generated column expression
  depends on that base column.
- Observed Doltgres behavior: the base column is dropped successfully, leaving
  the generated column dependency unprotected.

### DROP FUNCTION allows removing functions still used by defaults, views, materialized views, triggers, generated columns, or expression indexes

- Reproducers: `TestDropFunctionUsedByColumnDefaultRequiresCascadeRepro` and
  `TestDropFunctionUsedByViewRequiresCascadeRepro`,
  `TestDropFunctionUsedByMaterializedViewRequiresCascadeRepro`,
  `TestDropFunctionUsedByTriggerRequiresCascadeRepro`, and
  `TestDropFunctionUsedByGeneratedColumnRequiresCascadeRepro`, and
  `TestDropFunctionUsedByExpressionIndexRequiresCascadeRepro` in
  `testing/go/function_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'TestDropFunctionUsedBy(ColumnDefault|View|MaterializedView|Trigger|GeneratedColumn|ExpressionIndex)RequiresCascadeRepro'
  -count=1`.
- Expected PostgreSQL behavior: `DROP FUNCTION` fails under default `RESTRICT`
  behavior when a column default, view expression, materialized-view
  expression, trigger, stored generated column, or expression index depends on
  that function.
- Observed Doltgres behavior: all six functions are dropped successfully,
  leaving dependent schema objects with stale expressions, trigger function
  references, materialized-view definitions, generated-column expressions, or
  expression-index definitions.

### DROP TYPE and DROP DOMAIN allow removing types still used by views or functions

- Reproducers: `TestDropEnumTypeUsedByViewRequiresCascadeRepro` in
  `testing/go/enum_metadata_repro_test.go`,
  `TestDropEnumTypeUsedByFunctionRequiresCascadeRepro` in
  `testing/go/enum_metadata_repro_test.go`,
  `TestDropDomainUsedByViewRequiresCascadeRepro` in
  `testing/go/domain_correctness_repro_test.go`, and
  `TestDropDomainUsedByFunctionRequiresCascadeRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestDrop(EnumType|Domain)UsedBy(Function|View)RequiresCascadeRepro'
  -count=1`.
- Expected PostgreSQL behavior: `DROP TYPE` and `DROP DOMAIN` fail under
  default `RESTRICT` behavior when a view expression or function signature
  depends on that type or domain.
- Observed Doltgres behavior: all four drops succeed, leaving dependent view
  definitions or function signatures with stale type references.

### DROP TYPE dependency checks ignore type schema

- Reproducer: `TestDropTypeDependencyChecksSchemaQualifiedTypeRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDropTypeDependencyChecksSchemaQualifiedTypeRepro -count=1`.
- Expected PostgreSQL behavior: dropping
  `drop_type_schema_a.same_named_enum` succeeds when only
  `drop_type_schema_b.same_named_enum` is used by a table column, because the
  schema-qualified types are distinct objects.
- Observed Doltgres behavior: `DROP TYPE
  drop_type_schema_a.same_named_enum` is rejected because a table column uses a
  same-named type from `drop_type_schema_b`.

### DROP DOMAIN dependency checks ignore domain schema

- Reproducer: `TestDropDomainDependencyChecksSchemaQualifiedDomainRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDropDomainDependencyChecksSchemaQualifiedDomainRepro -count=1`.
- Expected PostgreSQL behavior: dropping
  `drop_domain_schema_a.same_named_domain` succeeds when only
  `drop_domain_schema_b.same_named_domain` is used by a table column, because
  the schema-qualified domains are distinct objects.
- Observed Doltgres behavior: `DROP DOMAIN
  drop_domain_schema_a.same_named_domain` is rejected because a table column
  uses a same-named domain from `drop_domain_schema_b`.

### DROP TYPE and DROP DOMAIN reject CASCADE even without dependents

- Reproducers: `TestDropTypeCascadeWithoutDependentsRepro` in
  `testing/go/type_correctness_repro_test.go` and
  `TestDropDomainCascadeWithoutDependentsRepro` in
  `testing/go/domain_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestDrop(Type|Domain)CascadeWithoutDependentsRepro' -count=1`.
- Expected PostgreSQL behavior: `DROP TYPE ... CASCADE` and `DROP DOMAIN ...
  CASCADE` are valid and drop the target object, even when no dependent
  objects need to be removed.
- Observed Doltgres behavior: both statements are rejected up front with
  `cascading type drops are not yet supported` or `cascading domain drops are
  not yet supported`, leaving the unused type/domain catalog entries in place.

### search_path does not resolve views in later schemas

- Reproducer: `TestSearchPathResolvesViewsInLaterSchemasRepro` in
  `testing/go/view_search_path_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestSearchPathResolvesViewsInLaterSchemasRepro -count=1`.
- Expected PostgreSQL behavior: unqualified relation lookup resolves a view in
  the first schema in `search_path` that contains a matching view name.
- Observed Doltgres behavior: the same unqualified view query fails with
  `table not found` when the matching view is in a later schema.

### Scalar ANY/ALL with a multi-column subquery reports an internal shape error

- Reproducers: `TestAnySubqueryRejectsMultipleColumnsRepro` and
  `TestAllSubqueryRejectsMultipleColumnsRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'Test(Any|All)SubqueryRejectsMultipleColumnsRepro' -count=1`.
- Expected PostgreSQL behavior: scalar `ANY` and `ALL` subqueries with more
  than one output column fail during semantic validation with
  `subquery has too many columns`.
- Observed Doltgres behavior: both queries reach execution and fail with
  `*expression.subqueryAnyExpr: expected right child to return \`2\` values
  but returned \`1\``, leaking an internal expression-shape error instead of
  PostgreSQL's semantic error.

### Scalar IN with a multi-column subquery reports a MySQL-shaped error

- Reproducer: `TestInSubqueryRejectsMultipleColumnsRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestInSubqueryRejectsMultipleColumnsRepro -count=1`.
- Expected PostgreSQL behavior: `SELECT 1 IN (SELECT 1, 2)` fails during
  semantic validation with `subquery has too many columns`.
- Observed Doltgres behavior: the query fails with
  `operand should have 1 columns, but has 2`, exposing a MySQL-style diagnostic
  instead of PostgreSQL's semantic error.

### Row IN with a matching multi-column subquery fails internally

- Reproducer: `TestRowInSubqueryAcceptsMultipleColumnsRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRowInSubqueryAcceptsMultipleColumnsRepro -count=1`.
- Expected PostgreSQL behavior: row-valued `IN` accepts a subquery with the same
  number of columns as the row constructor, so `ROW(1, 2) IN (SELECT 1, 2)`
  returns `true` and `ROW(1, 3) IN (SELECT 1, 2)` returns `false`.
- Observed Doltgres behavior: both queries fail during planning with
  `*expression.InSubquery: found equality comparison that does not return a
  bool`, so valid composite-key membership predicates cannot run.

### Row constructors reject table-alias star expansion

- Reproducer: `TestRowConstructorExpandsTableAliasStarRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRowConstructorExpandsTableAliasStarRepro -count=1`.
- Expected PostgreSQL behavior: inside a row constructor, `table_alias.*`
  expands to the current row's fields, so `SELECT ROW(p.*, 99)::text FROM
  row_alias_users p ORDER BY name` returns `(jason,SEA,42,99)` and
  `(max,SFO,31,99)`.
- Observed Doltgres behavior: the query fails with `* syntax is not yet
  supported in this context`, so valid row-constructor projection over table
  aliases cannot run.

### Row IS DISTINCT comparisons with NULL fields return NULL

- Reproducer: `TestRowIsNotDistinctFromHandlesNullsRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRowIsNotDistinctFromHandlesNullsRepro -count=1`.
- Expected PostgreSQL behavior: row-valued `IS NOT DISTINCT FROM` treats
  corresponding `NULL` fields as equal, and row-valued `IS DISTINCT FROM`
  returns `false` when all fields are not distinct.
- Observed Doltgres behavior: both `ROW(1, NULL) IS NOT DISTINCT FROM ROW(1,
  NULL)` and `ROW(NULL, 4) IS DISTINCT FROM ROW(NULL, 4)` return SQL `NULL`
  instead of deterministic booleans.

### Row equality stops at NULL before checking later unequal fields

- Reproducer: `TestRowValueComparisonsHandleNullsRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestRowValueComparisonsHandleNullsRepro -count=1`.
- Expected PostgreSQL behavior: row equality returns `false` when any later
  non-null field pair is unequal, even if an earlier field comparison is
  `NULL`; for example, `ROW(1, 2, 3) = ROW(1, NULL, 4)` returns `false`.
- Observed Doltgres behavior: the same equality expression returns SQL `NULL`,
  so row equality can report unknown even after a decisive unequal field exists.

### GREATEST and LEAST reject ordinary scalar arguments

- Reproducer: `TestGreatestLeastEvaluateScalarArgumentsRepro` in
  `testing/go/greatest_least_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestGreatestLeastEvaluateScalarArgumentsRepro -count=1`.
- Expected PostgreSQL behavior: `GREATEST(1, 2, 3)` returns `3`,
  `LEAST(1, 2, 3)` returns `1`, and `GREATEST`/`LEAST` ignore `NULL`
  arguments unless all arguments are `NULL`.
- Observed Doltgres behavior: the same expressions fail during planning with
  `unsupported type for greatest/least argument: *types.DoltgresType`, so
  standard PostgreSQL conditional expressions cannot run.

### ILIKE pattern matching is rejected

- Reproducer: `TestILikePatternMatchRepro` in
  `testing/go/expression_operator_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestILikePatternMatchRepro -count=1`.
- Expected PostgreSQL behavior: `ILIKE` and `NOT ILIKE` evaluate
  case-insensitive pattern predicates, so `Alpha ILIKE 'a%'` and `Alpha NOT
  ILIKE 'b%'` both return true.
- Observed Doltgres behavior: the query fails with `ERROR: ILIKE is not yet
  supported`, so PostgreSQL case-insensitive LIKE predicates cannot run.

### SIMILAR TO pattern matching is rejected

- Reproducer: `TestSimilarToPatternMatchRepro` in
  `testing/go/expression_operator_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSimilarToPatternMatchRepro -count=1`.
- Expected PostgreSQL behavior: `SIMILAR TO` evaluates SQL regular expression
  patterns, so strings such as `abc` and `adc` match `a%(b|c)` while `abx`
  does not.
- Observed Doltgres behavior: the query fails with `ERROR: similar to is not
  yet supported (SQLSTATE 0A000)`, so standard SQL pattern predicates cannot
  run.

### Case-insensitive regular-expression operators are rejected

- Reproducer: `TestCaseInsensitiveRegexMatchRepro` in
  `testing/go/expression_operator_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCaseInsensitiveRegexMatchRepro -count=1`.
- Expected PostgreSQL behavior: `~*` and `!~*` evaluate case-insensitive
  regular-expression matches.
- Observed Doltgres behavior: the query fails with `ERROR: ~* is not yet
  supported`, so PostgreSQL case-insensitive regex predicates cannot run.

### Power operator is rejected

- Reproducer: `TestPowerOperatorRepro` in
  `testing/go/expression_operator_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPowerOperatorRepro -count=1`.
- Expected PostgreSQL behavior: the `^` operator evaluates numeric
  exponentiation, so `2 ^ 3` returns `8` and `4 ^ 0.5` returns `2`.
- Observed Doltgres behavior: the query fails with `ERROR: the power operator
  is not yet supported`, so PostgreSQL numeric exponentiation cannot run.

### Unary numeric root and absolute-value operators are rejected

- Reproducer: `TestUnaryNumericOperatorsRepro` in
  `testing/go/expression_operator_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestUnaryNumericOperatorsRepro -count=1`.
- Expected PostgreSQL behavior: unary `|/`, `||/`, and `@` evaluate square
  root, cube root, and absolute value respectively.
- Observed Doltgres behavior: the query fails with `ERROR: square root
  operator is not yet supported`, so PostgreSQL unary numeric operators cannot
  run.

### Temporal OVERLAPS expressions are rejected

- Reproducer: `TestTemporalOverlapsOperatorRepro` in
  `testing/go/expression_operator_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTemporalOverlapsOperatorRepro -count=1`.
- Expected PostgreSQL behavior: temporal period expressions using `OVERLAPS`
  return whether two date, timestamp, or interval-backed periods intersect.
- Observed Doltgres behavior: valid `OVERLAPS` expressions fail during parsing
  with `at or near ")": syntax error: unimplemented: this syntax`, so period
  intersection predicates cannot run.

### power(numeric, numeric) loses precision for fractional exponents

- Reproducer: `TestNumericPowerFractionalExponentRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestNumericPowerFractionalExponentRepro -count=1`.
- Expected PostgreSQL behavior: `power(2::numeric, 0.5::numeric)::float8`
  returns `1.4142135623730951`.
- Observed Doltgres behavior: the same query returns `1.4142136`, losing
  precision before the result is cast to `float8`.

### sqrt(numeric) leaks float64 precision into numeric output

- Reproducer: `TestSqrtNumericMatchesPostgresPrecisionRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestSqrtNumericMatchesPostgresPrecisionRepro -count=1`.
- Expected PostgreSQL behavior: `sqrt(2::numeric)::text` returns
  `1.414213562373095`.
- Observed Doltgres behavior: the same query returns `1.4142135623730951`,
  exposing the float64 approximation used internally for a PostgreSQL `numeric`
  result.

### numeric logarithms collapse small deltas through float64 conversion

- Reproducer: `TestNumericLogarithmsPreserveSmallDeltasRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestNumericLogarithmsPreserveSmallDeltasRepro -count=1`.
- Expected PostgreSQL behavior: `ln(1.0000000000000000000001::numeric)::text`
  returns `0.00000000000000000000010000000000000000`,
  `log(1.0000000000000000000001::numeric)::text` returns
  `0.00000000000000000000004342944819032518`, and
  `log(1.0000000000000000000001::numeric,
  1.0000000000000000000003::numeric)::text` returns
  `2.9999999999999999999997`.
- Observed Doltgres behavior: the one-argument `ln` and `log` calls both return
  `0`, and the two-argument `log` call fails with `division by zero`, showing
  that exact numeric values near one are collapsed to `float64(1)`.

### bytea array casts to text arrays drop PostgreSQL escape layers

- Reproducer: `TestByteaArrayCastToTextUsesPostgresEscapingRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestByteaArrayCastToTextUsesPostgresEscapingRepro -count=1`.
- Expected PostgreSQL behavior: casting `bytea[]` to `text[]` preserves the
  extra escaping required to represent bytea values inside text array output.
- Observed Doltgres behavior: the cast returns values with one fewer escape
  layer, so the text array does not match PostgreSQL's bytea array rendering.

### `substring(string for count)` resolves to the wrong function signature

- Reproducer: `TestSubstringForCountSyntaxRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubstringForCountSyntaxRepro -count=1`.
- Expected PostgreSQL behavior: `substring('hello' for 3)` returns `hel`.
- Observed Doltgres behavior: the query fails with
  `function substring(unknown, bigint, integer) does not exist`, so a valid
  SQL-standard substring form cannot be used.

### `substring ... similar ... escape` syntax is rejected

- Reproducer: `TestSubstringSimilarEscapeSyntaxRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubstringSimilarEscapeSyntaxRepro -count=1`.
- Expected PostgreSQL behavior: `substring('hello.' similar 'hello#.' escape
  '#')` returns `hello.`, and a quoted capture marker such as
  `substring('Thomas' similar '%#"o_a#"_' escape '#')` returns `oma`.
- Observed Doltgres behavior: both queries fail during parsing with syntax
  errors at the pattern literals, so SQL-standard SIMILAR substring extraction
  cannot run.

### `regexp_matches` rejects supported PostgreSQL regex flags

- Reproducer: `TestRegexpMatchesSupportedFlagsRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRegexpMatchesSupportedFlagsRepro -count=1`.
- Expected PostgreSQL behavior: `regexp_matches('ab', 'a b', 'x')` treats the
  pattern as expanded syntax and returns `{ab}`, while
  `regexp_matches(E'a\nb', '^b', 'n')` uses newline-sensitive matching and
  returns `{b}`.
- Observed Doltgres behavior: both queries fail with unsupported-flag errors
  such as `regex flag "x" not supported` and `regex flag "n" not supported`.

### `regexp_replace` with flags resolves to the wrong overload

- Reproducer: `TestRegexpReplaceReplacesMatchesRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRegexpReplaceReplacesMatchesRepro -count=1`.
- Expected PostgreSQL behavior: `regexp_replace('foobarbaz', 'b..', 'X')`
  returns `fooXbaz`, and the four-argument global form
  `regexp_replace('foobarbaz', 'b..', 'X', 'g')` returns `fooXX`.
- Observed Doltgres behavior: the query fails with `Truncated incorrect int
  value: g`, indicating that the four-argument text-flags overload is not
  resolved with PostgreSQL semantics.

### `regexp_split_to_array` is missing

- Reproducer: `TestRegexpSplitToArraySplitsTextRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRegexpSplitToArraySplitsTextRepro -count=1`.
- Expected PostgreSQL behavior: `regexp_split_to_array('a,b,c', ',')` returns
  `{a,b,c}`.
- Observed Doltgres behavior: planning fails with
  `function: 'regexp_split_to_array' not found`, so regex-based splitting into
  arrays cannot be used.

### `regexp_like` returns text instead of boolean

- Reproducer: `TestRegexpLikeReturnsBooleanRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRegexpLikeReturnsBooleanRepro -count=1`.
- Expected PostgreSQL behavior: `regexp_like('abc', '^a')` returns the boolean
  value `true`.
- Observed Doltgres behavior: the function returns the text value `t`, so
  clients see the wrong result type for a PostgreSQL boolean predicate.

### `regexp_count` is missing

- Reproducer: `TestRegexpCountCountsMatchesRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRegexpCountCountsMatchesRepro -count=1`.
- Expected PostgreSQL behavior: `regexp_count('abcabc', 'a')` returns `2`.
- Observed Doltgres behavior: planning fails with
  `function: 'regexp_count' not found`, so regex match counts cannot be used.

### `concat_ws(NULL, ...) IS NULL` returns text instead of boolean

- Reproducer: `TestConcatWsSkipsNullsRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestConcatWsSkipsNullsRepro -count=1`.
- Expected PostgreSQL behavior: `concat_ws(',', 10, 20, NULL, 30)` returns
  `10,20,30`, `concat_ws('', 10, 20, NULL, 30)` returns `102030`, and
  `concat_ws(NULL, 10, 20) IS NULL` returns the boolean value `true`.
- Observed Doltgres behavior: the string results match, but the NULL-separator
  predicate returns the text value `t`, so clients see the wrong result type
  for a PostgreSQL boolean expression.

### `format` rejects dynamic width arguments

- Reproducer: `TestFormatDynamicWidthRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestFormatDynamicWidthRepro -count=1`.
- Expected PostgreSQL behavior: `format('%*s|%*s', 5, 'x', -5, 'y')` returns
  `    x|y    `, using the supplied positive and negative width arguments for
  right- and left-justification.
- Observed Doltgres behavior: execution fails with
  `format() width from arguments is not yet supported`, so PostgreSQL dynamic
  width formatting cannot be used.

### `parse_ident` is missing for SQL identifier parsing

- Reproducer: `TestParseIdentSplitsQualifiedNamesRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestParseIdentSplitsQualifiedNamesRepro -count=1`.
- Expected PostgreSQL behavior: `parse_ident('Schemax.Tabley')::text[]`
  returns `{schemax,tabley}`, `parse_ident('"SchemaX"."TableY"')::text[]`
  preserves quoted case as `{SchemaX,TableY}`, and
  `parse_ident('foo.boo[]', false)::text[]` returns `{foo,boo}`.
- Observed Doltgres behavior: planning fails with
  `function: 'parse_ident' not found`, so clients cannot use PostgreSQL's
  identifier parser.

### `string_to_table` cannot split text into rows

- Reproducer: `TestStringToTableSplitsRowsRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestStringToTableSplitsRowsRepro -count=1`.
- Expected PostgreSQL behavior: `string_to_table('1|2|3', '|')` can be used in
  `FROM` and returns three rows: `1`, `2`, and `3`.
- Observed Doltgres behavior: planning fails with
  `unsupported syntax: values row(string_to_table(...))`, so standard
  text-splitting set-returning queries cannot run.

### Function named-argument notation is rejected

- Reproducer: `TestFunctionNamedArgumentNotationRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestFunctionNamedArgumentNotationRepro -count=1`.
- Expected PostgreSQL behavior: calls such as
  `make_date(year => 2026, month => 5, day => 10)` and
  `make_date(2026, day => 10, month => 5)` both return `2026-05-10`.
- Observed Doltgres behavior: parsing fails with
  `at or near ">": syntax error`, so PostgreSQL named and mixed named
  function-argument notation cannot be used.

### `current_catalog` reports the wrong result column name

- Reproducer: `TestCurrentCatalogColumnNameRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCurrentCatalogColumnNameRepro -count=1`.
- Expected PostgreSQL behavior: `SELECT current_catalog` returns the current
  database name in a column named `current_catalog`.
- Observed Doltgres behavior: the value is returned under the column name
  `current_database`, so clients that inspect result metadata see the wrong
  PostgreSQL column label.

### `current_schema()` reports a quoted result column name

- Reproducer: `TestCurrentSchemaColumnNameRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCurrentSchemaColumnNameRepro -count=1`.
- Expected PostgreSQL behavior: `SELECT current_schema()` returns `public` in
  a column named `current_schema`.
- Observed Doltgres behavior: the result column name is `"current_schema"`,
  including the quote characters, so clients see incorrect result metadata.

### XML well-formedness predicate functions are missing

- Reproducer: `TestXmlWellFormedFunctionsRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestXmlWellFormedFunctionsRepro -count=1`.
- Expected PostgreSQL behavior: `xml_is_well_formed('<a/>')` and
  `xml_is_well_formed_document('<a/>')` return `true`,
  `xml_is_well_formed('<a>')` returns `false`, and
  `xml_is_well_formed_content('plain text')` returns `true`.
- Observed Doltgres behavior: planning fails with
  `function: 'xml_is_well_formed' not found`, so text-based XML validation
  helpers cannot be used.

### name columns accept implicit integer assignments

- Reproducer: `TestNameTypeRejectsIntegerAssignmentRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestNameTypeRejectsIntegerAssignmentRepro -count=1`.
- Expected PostgreSQL behavior: assigning an integer expression to a `name`
  column without an explicit cast is rejected by type checking.
- Observed Doltgres behavior: `INSERT INTO name_assignment_items VALUES (1,
  12345)` succeeds, storing a PostgreSQL-invalid implicit cast.

### User-defined CREATE CAST is rejected

- Reproducer: `TestCreateCastFunctionIsUsedByExplicitCastRepro` in
  `testing/go/cast_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateCastFunctionIsUsedByExplicitCastRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE CAST (cast_color AS INT) WITH FUNCTION
  cast_color_to_int(cast_color)` succeeds, and an explicit cast such as
  `('green'::cast_color)::INT` calls the user-defined SQL cast function.
- Observed Doltgres behavior: setup fails with `ERROR: at or near "(": syntax
  error: unimplemented: this syntax (SQLSTATE 0A000)` at the `CREATE CAST`
  statement.

### User-defined CREATE OPERATOR is rejected

- Reproducer: `TestCreateOperatorInstallsCallableOperatorRepro` in
  `testing/go/operator_definition_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateOperatorInstallsCallableOperatorRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE OPERATOR === (...)` can bind a SQL
  boolean function as an infix operator, and `SELECT 2 === 4, 2 === 3` returns
  `true, false`.
- Observed Doltgres behavior: setup fails with `ERROR: at or near "=": syntax
  error: unimplemented: this syntax (SQLSTATE 0A000)` at the `CREATE OPERATOR`
  statement.

### xid columns accept invalid transaction ID input

- Reproducer: `TestXidRejectsInvalidInputRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestXidRejectsInvalidInputRepro -count=1`.
- Expected PostgreSQL behavior: `xid` input rejects out-of-range or malformed
  values such as `4294967296`, `-1`, and `abc`.
- Observed Doltgres behavior: all three values insert successfully into an
  `xid` column, allowing invalid transaction ID values to be persisted.

### xid ORDER BY is allowed despite lacking an ordering operator

- Reproducer: `TestXidOrderingRequiresOrderingOperatorRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestXidOrderingRequiresOrderingOperatorRepro -count=1`.
- Expected PostgreSQL behavior: `ORDER BY` on an `xid` column fails because
  PostgreSQL cannot identify an ordering operator for type `xid`.
- Observed Doltgres behavior: `SELECT id, x FROM xid_order_items ORDER BY x`
  succeeds, applying ordering semantics PostgreSQL does not define for `xid`.

### `ORDER BY ... USING` is rejected

- Reproducer: `TestOrderByUsingOperatorRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestOrderByUsingOperatorRepro -count=1`.
- Expected PostgreSQL behavior: `ORDER BY v USING > NULLS LAST` sorts by the
  named greater-than ordering operator, returning non-null values in descending
  order followed by `NULL`; `ORDER BY v USING < NULLS FIRST` sorts by the
  named less-than ordering operator with `NULL` first.
- Observed Doltgres behavior: both valid queries fail during parsing with
  `at or near "using": syntax error`, so PostgreSQL queries that name an
  ordering operator cannot run.

### ICU collations cannot enforce collation-aware uniqueness

- Reproducer: `TestIcuCollationUniqueConstraintUsesCollationEqualityRepro` in
  `testing/go/collation_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestIcuCollationUniqueConstraintUsesCollationEqualityRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE COLLATION ... provider = icu,
  deterministic = false` succeeds, and a `UNIQUE` text column using a
  case-insensitive collation rejects a second value such as `ABC` after `abc`.
- Observed Doltgres behavior: setup fails immediately with `ERROR: CREATE
  COLLATION is not yet supported (SQLSTATE 0A000)`, so Doltgres cannot express
  or enforce that collation-aware uniqueness rule.

### array_agg over array inputs cannot return higher-dimensional arrays

- Reproducer: `TestArrayAggOverArrayColumnReturnsHigherDimensionalArrayRepro`
  in `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestArrayAggOverArrayColumnReturnsHigherDimensionalArrayRepro -count=1`.
- Expected PostgreSQL behavior: `array_agg(vals)` over a `FLOAT[]` column
  returns a two-dimensional array such as
  `{{1.0,2.0},{3.0,4.0},{5.0,6.0}}`.
- Observed Doltgres behavior: the query fails while returning the result with
  `arrays with 2 dimensions are not yet supported using the binary format`.

### User-defined CREATE AGGREGATE is rejected

- Reproducer: `TestCreateAggregateSqlTransitionFunctionRepro` in
  `testing/go/aggregate_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateAggregateSqlTransitionFunctionRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE AGGREGATE custom_sum(INT)` with a SQL
  transition function succeeds, and `SELECT custom_sum(v)` returns grouped
  aggregate results such as `3` and `10`.
- Observed Doltgres behavior: setup fails with `ERROR: CREATE AGGREGATE is not
  yet supported ... (SQLSTATE 0A000)`.

### GROUP BY GROUPING SETS is rejected

- Reproducer: `TestGroupByGroupingSetsRepro` in
  `testing/go/aggregate_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestGroupByGroupingSetsRepro -count=1`.
- Expected PostgreSQL behavior: `GROUP BY GROUPING SETS ((region, product),
  (region), ())` emits detail rows, per-region subtotal rows, and a grand total
  in a single aggregate query.
- Observed Doltgres behavior: the query fails during parsing with `at or near
  "sets": syntax error`.

### GROUP BY ROLLUP is treated as a missing function

- Reproducer: `TestGroupByRollupRepro` in
  `testing/go/aggregate_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestGroupByRollupRepro -count=1`.
- Expected PostgreSQL behavior: `GROUP BY ROLLUP (region, product)` computes
  hierarchical subtotal rows equivalent to grouped detail rows, per-region
  subtotals, and a grand total.
- Observed Doltgres behavior: the query is parsed as a call to `rollup` and
  fails with `function: 'rollup' not found`.

### GROUP BY CUBE is treated as a missing function

- Reproducer: `TestGroupByCubeRepro` in
  `testing/go/aggregate_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestGroupByCubeRepro -count=1`.
- Expected PostgreSQL behavior: `GROUP BY CUBE (region, product)` computes all
  subtotal combinations for the listed grouping keys, including product totals
  and the grand total.
- Observed Doltgres behavior: the query is parsed as a call to `cube` and fails
  with `function: 'cube' not found`.

### Negative-scale numeric typmods are rejected

- Reproducer: `TestNumericNegativeScaleRoundsStoredValuesRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestNumericNegativeScaleRoundsStoredValuesRepro -count=1`.
- Expected PostgreSQL behavior: `NUMERIC(2, -3)` is a valid column type. Values
  such as `12345` and `99499` are rounded to `12000` and `99000` before
  precision enforcement, while `99500` overflows after rounding.
- Observed Doltgres behavior: `CREATE TABLE ... amount NUMERIC(2, -3)` fails
  during parsing with `at or near "-": syntax error`, so schemas that rely on
  PostgreSQL's negative-scale numeric storage semantics cannot be represented.

### Numeric typmods with scale greater than precision are rejected

- Reproducer: `TestNumericScaleGreaterThanPrecisionRoundsStoredValuesRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestNumericScaleGreaterThanPrecisionRoundsStoredValuesRepro -count=1`.
- Expected PostgreSQL behavior: `NUMERIC(3, 5)` is a valid column type for
  fractional values whose absolute rounded value is less than `10^-2`. Values
  such as `0.001234` and `0.009994` are stored as `0.00123` and `0.00999`,
  while `0.09999` overflows.
- Observed Doltgres behavior: `CREATE TABLE ... amount NUMERIC(3, 5)` fails
  with `scale (5) must be between 0 and precision (3)`, so valid PostgreSQL
  schemas with scale-greater-than-precision numeric columns cannot be loaded.

### Numeric special values are rejected

- Reproducer: `TestNumericSpecialValuesRoundTripRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestNumericSpecialValuesRoundTripRepro -count=1`.
- Expected PostgreSQL behavior: a `NUMERIC` column can store `NaN`,
  `Infinity`, and `-Infinity`. PostgreSQL orders them as `-Infinity`, finite
  numbers, `Infinity`, then `NaN`, and comparisons such as
  `'NaN'::numeric = 'NaN'::numeric`,
  `'Infinity'::numeric > 999999999999999999999999999999::numeric`, and
  `'-Infinity'::numeric < -999999999999999999999999999999::numeric` all return
  true.
- Observed Doltgres behavior: inserting `'NaN'` into a `NUMERIC` column fails
  with `invalid input syntax for type numeric: "NaN"`, so PostgreSQL numeric
  data containing special values cannot be loaded or round-tripped.

### Multidimensional array columns are rejected

- Reproducer: `TestMultidimensionalArrayColumnRoundTripRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestMultidimensionalArrayColumnRoundTripRepro -count=1`.
- Expected PostgreSQL behavior: a column declared as `VARCHAR[][]` can store and
  return a two-dimensional array value such as `{{abc,def},{ghi,jkl}}`.
- Observed Doltgres behavior: `CREATE TABLE ... labels VARCHAR[][]` fails
  during parsing with `at or near "]": syntax error: unimplemented: this
  syntax`.

### Common PostgreSQL built-in scalar types cannot round trip

- Reproducer: `TestCommonBuiltinTypesRoundTripRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCommonBuiltinTypesRoundTripRepro -count=1`.
- Expected PostgreSQL behavior: common built-in data types such as `cidr`,
  `inet`, `macaddr`, `money`, geometric types, `xml`, `tsquery`, and
  `tsvector` can be declared in tables, inserted into, and selected back.
- Observed Doltgres behavior: `cidr`, `inet`, `macaddr`, `money`, `box`,
  `circle`, `line`, `lseg`, `path`, `xml`, `tsquery`, and `tsvector` table
  creation fails with `type "..." does not exist` errors, while `POINT` and
  `POLYGON` table creation fail with `at or near ")": syntax error:
  unimplemented: this syntax`.

### Large-object bytea round trips are missing

- Reproducer: `TestLargeObjectByteaRoundTripRepro` in
  `testing/go/large_object_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLargeObjectByteaRoundTripRepro -count=1`.
- Expected PostgreSQL behavior: `lo_from_bytea(0, decode('deadbeef', 'hex'))`
  creates a large object and returns its OID; `lo_get(oid)` then reads back
  the persisted bytes, so `encode(lo_get(oid), 'hex')` returns `deadbeef`.
- Observed Doltgres behavior: the setup insert fails with `function:
  'lo_from_bytea' not found`, so callers cannot create or round-trip
  PostgreSQL large-object data through the built-in large-object APIs.

### Large-object creation does not persist metadata

- Reproducer: `TestLargeObjectCreatePersistsMetadataRepro` in
  `testing/go/large_object_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLargeObjectCreatePersistsMetadataRepro -count=1`.
- Expected PostgreSQL behavior: `lo_create(424242)` creates a large object with
  that OID, `pg_catalog.pg_largeobject_metadata` exposes the metadata row, and
  `lo_unlink(424242)` removes the object.
- Observed Doltgres behavior: `lo_create` and `lo_unlink` both fail with
  `function: '...' not found`, and `pg_largeobject_metadata` remains empty for
  the requested OID.

### table.* composite function arguments panic during planning

- Reproducer: `TestCompositeStarArgumentToFunctionRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCompositeStarArgumentToFunctionRepro -count=1`.
- Expected PostgreSQL behavior: when a function expects a table's row type,
  `table_name.*` may be passed as a single composite argument and the function
  receives each input row.
- Observed Doltgres behavior: planning the query panics with `star is just a
  placeholder node, but Type was called`, so valid row-typed function calls can
  abort the connection instead of returning results.

### timestamp and timestamptz - interval ignore day and month components

- Reproducers: `TestTimestampMinusIntervalSubtractsDayComponentsRepro` and
  `TestTimestampMinusIntervalSubtractsMonthComponentsRepro`, plus
  `TestTimestamptzMinusIntervalSubtractsDayComponentsRepro` and
  `TestTimestamptzMinusIntervalSubtractsMonthComponentsRepro`, in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'Test(Timestamp|Timestamptz)MinusIntervalSubtracts(Day|Month)ComponentsRepro'
  -count=1`.
- Expected PostgreSQL behavior: subtracting `interval '2 days'` from
  `timestamp '2025-07-21 04:05:06'` returns `2025-07-19 04:05:06`, and
  subtracting `interval '1 month'` from `timestamp '2025-03-31 12:00:00'`
  returns `2025-02-28 12:00:00`. The same day/month subtraction applies to
  `timestamp with time zone`.
- Observed Doltgres behavior: the subtraction queries return the original
  timestamp/timestamptz value unchanged, so day and month interval components
  are ignored.

### date, timestamp, and timestamptz + interval use fixed-duration months instead of calendar months

- Reproducers: `TestDatePlusIntervalUsesCalendarMonthsRepro` and
  `TestTimestampPlusIntervalUsesCalendarMonthsRepro`, plus
  `TestTimestamptzPlusIntervalUsesCalendarMonthsRepro`,
  `TestIntervalPlusDateUsesCalendarMonthsRepro`,
  `TestIntervalPlusTimestampUsesCalendarMonthsRepro`, and
  `TestIntervalPlusTimestamptzUsesCalendarMonthsRepro`, in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'Test((Date|Timestamp|Timestamptz)PlusInterval|IntervalPlus(Date|Timestamp|Timestamptz))UsesCalendarMonthsRepro'
  -count=1`.
- Expected PostgreSQL behavior: adding `interval '1 month'` to `timestamp
  '2025-01-31 12:00:00'` uses calendar-month semantics and returns
  `2025-02-28 12:00:00`; adding the same interval to `date '2025-01-31'` or
  `timestamp with time zone '2025-01-31 12:00:00+00'` returns February 28 as
  well. The commuted `interval + date/timestamp/timestamptz` operators have the
  same behavior.
- Observed Doltgres behavior: all covered month-addition queries land on March
  2, treating the month as a fixed 30-day duration.

### date - interval uses fixed-duration months instead of calendar months

- Reproducer: `TestDateMinusIntervalUsesCalendarMonthsRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDateMinusIntervalUsesCalendarMonthsRepro -count=1`.
- Expected PostgreSQL behavior: subtracting `interval '1 month'` from `date
  '2025-03-31'` uses calendar-month semantics and returns
  `2025-02-28 00:00:00`.
- Observed Doltgres behavior: the query returns `2025-03-01 00:00:00`, treating
  the month as a fixed 30-day duration.

### extract/date_part timestamp Julian fields drop the fractional day

- Reproducers: `TestExtractTimestampJulianIncludesFractionalDayRepro` and
  `TestDatePartTimestampJulianIncludesFractionalDayRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'Test(Extract|DatePart)TimestampJulianIncludesFractionalDayRepro' -count=1`.
- Expected PostgreSQL behavior: extracting Julian from `timestamp
  '2001-02-18 20:38:40'` or `timestamptz '2001-02-18 20:38:40+00'` includes
  the time-of-day fraction, returning `2451959.86018518518518518519` for
  `extract` and `2451959.860185185` for `date_part`.
- Observed Doltgres behavior: both functions return `2451959`, dropping the
  fractional day entirely.

### date_trunc timestamptz with a named zone uses the input instant's offset

- Reproducer: `TestDateTruncTimestamptzNamedZoneUsesTruncatedOffsetRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDateTruncTimestamptzNamedZoneUsesTruncatedOffsetRepro -count=1`.
- Expected PostgreSQL behavior: `date_trunc('day', timestamptz ...,
  'America/New_York')` truncates in the named zone and applies the offset that
  is valid at the truncated wall time. For 2021 DST transition days, the
  expected UTC epochs are `1615698000` for March 14 and `1636257600` for
  November 7.
- Observed Doltgres behavior: the March case returns `1615694400` and the
  November case returns `1636261200`, showing that it keeps the offset from the
  input instant instead of recalculating the offset for local midnight.

### date_bin overflows outside Go's nanosecond timestamp window

- Reproducer: `TestDateBinUsesPostgresTimestampRangeRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDateBinUsesPostgresTimestampRangeRepro -count=1`.
- Expected PostgreSQL behavior: `date_bin` works for timestamp and timestamptz
  values across PostgreSQL's timestamp range, including years 1500 and 2300.
  Binning the covered examples returns `1500-01-02 00:00:00`,
  `2300-01-01 00:05:00`, and the corresponding `+00` timestamptz values.
- Observed Doltgres behavior: the same bins wrap through `UnixNano`, returning
  values such as `2084-07-21 23:34:33.709551` for the year-1500 case and
  `1715-06-13 00:30:26.290448` for the year-2300 case.

### age uses a fixed 30-day borrow instead of calendar month lengths

- Reproducer: `TestAgeUsesCalendarMonthBorrowingRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAgeUsesCalendarMonthBorrowingRepro -count=1`.
- Expected PostgreSQL behavior: `age(timestamp '2001-02-28', timestamp
  '2001-01-31')` returns `28 days`, and `age(timestamp '2004-03-01',
  timestamp '2004-01-31')` returns `1 mon 1 day`, borrowing with calendar
  month lengths.
- Observed Doltgres behavior: the same calls return `27 days` and `1 mon`,
  showing that `age()` borrows a fixed 30 days.

### timezone(text, timestamptz) applies the target offset with the wrong sign

- Reproducer: `TestTimezoneTextTimestamptzUsesTargetOffsetRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestTimezoneTextTimestamptzUsesTargetOffsetRepro -count=1`.
- Expected PostgreSQL behavior: converting `timestamptz '2021-03-14
  12:00:00+00'` to `America/New_York` returns local timestamp `2021-03-14
  08:00:00`; converting `timestamptz '2001-02-16 20:38:40.12-05'` to fixed
  zone `-04:45` returns `2001-02-17 06:23:40.12`.
- Observed Doltgres behavior: the named-zone conversion returns
  `2021-03-14 16:00:00`, and the fixed-offset conversion returns
  `2001-02-16 20:53:40.12`, showing that the offset is applied in the wrong
  direction.

### timezone(text, timestamp) uses the UTC instant instead of local wall time for named-zone offsets

- Reproducer: `TestTimezoneTextTimestampUsesWallTimeOffsetRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestTimezoneTextTimestampUsesWallTimeOffsetRepro -count=1`.
- Expected PostgreSQL behavior: `timestamp AT TIME ZONE 'America/New_York'`
  interprets the timestamp as local wall time in New York. Around the 2021 DST
  transitions, `2021-03-14 03:30:00` maps to UTC epoch `1615707000`, and
  `2021-11-07 03:30:00` maps to UTC epoch `1636273800`.
- Observed Doltgres behavior: the same conversions return `1615710600` and
  `1636270200`, showing that the named-zone offset is chosen as if the
  timestamp were already a UTC instant.

### to_char timezone fields use named-zone offsets for plain timestamps and location names for TZ

- Reproducer: `TestToCharTimezoneFieldsMatchPostgresSessionZoneRepro` in
  `testing/go/to_char_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestToCharTimezoneFieldsMatchPostgresSessionZoneRepro -count=1`.
- Expected PostgreSQL behavior: after `SET TIME ZONE 'America/New_York'`,
  formatting a plain `timestamp` with `TZ OF TZH:TZM` leaves `TZ` empty and
  reports zero offset fields, while formatting `timestamptz '2021-03-14
  12:00:00+00'` returns `2021-03-14 08:00 EDT -04 -04:00`.
- Observed Doltgres behavior: the plain timestamp formats as
  `2021-03-14 12:00 AMERICA/NEW_YORK -04:00 -04:00`, and the timestamptz
  formats as `2021-03-14 08:00 AMERICA/NEW_YORK -04:00 -04:00`.

### date constructors accept impossible calendar dates

- Reproducer: `TestMakeDateTimestampRejectInvalidCalendarDateRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestMakeDateTimestampRejectInvalidCalendarDateRepro -count=1`.
- Expected PostgreSQL behavior: `make_date(2021, 2, 29)`,
  `make_timestamp(2021, 2, 29, 0, 0, 0)`, and
  `make_timestamptz(2021, 2, 29, 0, 0, 0, 'UTC')` all fail with
  `date field value out of range`.
- Observed Doltgres behavior: all three calls complete without an error,
  accepting an impossible calendar date instead of rejecting it.

### to_timestamp(float8) rejects valid PostgreSQL timestamps and NULLs infinities

- Reproducer: `TestToTimestampFloatSupportsPostgresRangeAndInfinityRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestToTimestampFloatSupportsPostgresRangeAndInfinityRepro -count=1`.
- Expected PostgreSQL behavior: `to_timestamp(10413792000)` returns
  `2300-01-01 00:00:00+00`, while `to_timestamp('Infinity'::float8)` and
  `to_timestamp('-Infinity'::float8)` return `infinity` and `-infinity`.
- Observed Doltgres behavior: the year-2300 timestamp fails with `timestamp out
  of range`, and both infinity inputs return SQL `NULL`.

### to_timestamp(text, text) accepts the output-only OF timezone pattern

- Reproducer: `TestToTimestampRejectsOutputOnlyOFPatternRepro` in
  `testing/go/to_timestamp_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestToTimestampRejectsOutputOnlyOFPatternRepro -count=1`.
- Expected PostgreSQL behavior: `to_timestamp('2011-12-18 11:38 +05',
  'YYYY-MM-DD HH12:MI OF')` fails with `formatting field "OF" is only supported
  in to_char` because `OF` is not a valid input pattern.
- Observed Doltgres behavior: the query succeeds, accepting an output-only
  timezone formatting token as an input offset.

### to_date(text, text) accepts the output-only OF timezone pattern

- Reproducer: `TestToDateRejectsOutputOnlyOFPatternRepro` in
  `testing/go/to_date_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestToDateRejectsOutputOnlyOFPatternRepro -count=1`.
- Expected PostgreSQL behavior: `to_date('2011-12-18 +05', 'YYYY-MM-DD OF')`
  fails with `formatting field "OF" is only supported in to_char` because `OF`
  is not a valid input pattern.
- Observed Doltgres behavior: the query succeeds and returns a date, accepting
  an output-only timezone formatting token in `to_date`.

### to_timestamp/to_date ISO weekday parsing maps Monday to Sunday

- Reproducers: `TestToTimestampIsoWeekdayParsesMondayRepro` in
  `testing/go/to_timestamp_correctness_repro_test.go` and
  `TestToDateIsoWeekdayParsesMondayRepro` in
  `testing/go/to_date_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'TestTo(Timestamp|Date)IsoWeekdayParsesMondayRepro' -count=1`.
- Expected PostgreSQL behavior: parsing `2005521` with `IYYYIWID` treats
  ISO weekday `1` as Monday, returning `2005-12-26` for `to_date` and
  `2005-12-26 00:00:00+00` for `to_timestamp` in UTC.
- Observed Doltgres behavior: both parsers return `2006-01-01`, the Sunday at
  the end of the ISO week.

### extract/date_part timestamptz timezone fields use a fixed offset

- Reproducers: `TestExtractTimestamptzTimezoneUsesSessionTimeZoneRepro` and
  `TestDatePartTimestamptzTimezoneUsesSessionTimeZoneRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'Test(Extract|DatePart)TimestamptzTimezoneUsesSessionTimeZoneRepro' -count=1`.
- Expected PostgreSQL behavior: after `SET TIME ZONE 'UTC'`, extracting
  `timezone`, `timezone_hour`, or `timezone_minute` from a `timestamptz` returns
  `0`.
- Observed Doltgres behavior: `timezone` returns `-28800` and `timezone_hour`
  returns `-8`, showing that both functions use a fixed offset instead of the
  active session time zone.

### Timestamp column typmods are not applied to stored values

- Reproducer: `TestTimestampColumnTypmodsRoundStoredFractionalSecondsRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimestampColumnTypmodsRoundStoredFractionalSecondsRepro -count=1`.
- Expected PostgreSQL behavior: assigning `2021-09-15 21:43:56.789` to
  `TIMESTAMP(0)` stores `2021-09-15 21:43:57`, assigning it to `TIMESTAMP(2)`
  stores `2021-09-15 21:43:56.79`, and assigning the same instant to
  `TIMESTAMPTZ(0)` stores `2021-09-15 21:43:57+00` when the session time zone
  is UTC.
- Observed Doltgres behavior: the stored timestamp and timestamptz values retain
  `.789` fractional seconds, so column assignment does not enforce timestamp
  typmod precision.

### Timestamp typmod defaults store unrounded values

- Reproducer: `TestTimestampTypmodDefaultRoundsStoredValueRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimestampTypmodDefaultRoundsStoredValueRepro -count=1`.
- Expected PostgreSQL behavior: a default used for a `TIMESTAMP(0)` column is
  coerced through the target column typmod before storage, so default
  `2021-09-15 21:43:56.789` stores as `2021-09-15 21:43:57`.
- Observed Doltgres behavior: inserting a row that uses the column default
  stores `2021-09-15 21:43:56.789`, so implicit writes through defaults can
  persist timestamp values outside the target column's declared precision.

### Timestamp typmod COPY FROM stores unrounded values

- Reproducer: `TestTimestampTypmodCopyFromRoundsStoredValueRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimestampTypmodCopyFromRoundsStoredValueRepro -count=1`.
- Expected PostgreSQL behavior: `COPY FROM STDIN` into a `TIMESTAMP(0)` column
  applies the target column typmod before storage, so copied
  `2021-09-15 21:43:56.789` stores as `2021-09-15 21:43:57`.
- Observed Doltgres behavior: the copied row stores
  `2021-09-15 21:43:56.789`, so bulk ingestion can persist timestamp values
  outside the target column's declared precision.

### Timestamp typmod UPDATE stores unrounded values

- Reproducer: `TestTimestampTypmodUpdateRoundsStoredValueRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimestampTypmodUpdateRoundsStoredValueRepro -count=1`.
- Expected PostgreSQL behavior: `UPDATE` assignments into a `TIMESTAMP(0)`
  column apply the target column typmod before storage, so updating a row to
  `2021-09-15 21:43:56.789` stores as `2021-09-15 21:43:57`.
- Observed Doltgres behavior: the updated row stores
  `2021-09-15 21:43:56.789`, so ordinary updates can rewrite existing rows into
  values outside the target column's declared precision.

### Timestamp typmod ON CONFLICT UPDATE stores unrounded values

- Reproducer: `TestTimestampTypmodOnConflictUpdateRoundsStoredValueRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimestampTypmodOnConflictUpdateRoundsStoredValueRepro -count=1`.
- Expected PostgreSQL behavior: `ON CONFLICT DO UPDATE` assignments into a
  `TIMESTAMP(0)` column apply the target column typmod before storage, so the
  conflict update stores `2021-09-15 21:43:57`.
- Observed Doltgres behavior: the conflict update stores
  `2021-09-15 21:43:56.789`, so upsert writes can rewrite existing rows into
  values outside the target column's declared precision.

### Timestamp typmod INSERT SELECT stores unrounded values

- Reproducer: `TestTimestampTypmodInsertSelectRoundsStoredValuesRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimestampTypmodInsertSelectRoundsStoredValuesRepro -count=1`.
- Expected PostgreSQL behavior: `INSERT ... SELECT` into a `TIMESTAMP(0)`
  column applies the target column typmod before storage, so source
  `2021-09-15 21:43:56.789` stores as `2021-09-15 21:43:57`.
- Observed Doltgres behavior: the inserted row stores
  `2021-09-15 21:43:56.789`, so set-based inserts can persist timestamp values
  outside the target column's declared precision.

### Timestamp typmod UPDATE FROM stores unrounded values

- Reproducer: `TestTimestampTypmodUpdateFromRoundsStoredValuesRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimestampTypmodUpdateFromRoundsStoredValuesRepro -count=1`.
- Expected PostgreSQL behavior: `UPDATE ... FROM` assignments into a
  `TIMESTAMP(0)` column apply the target column typmod before storage, so joined
  source `2021-09-15 21:43:56.789` stores as `2021-09-15 21:43:57`.
- Observed Doltgres behavior: the updated row stores
  `2021-09-15 21:43:56.789`, so joined updates can rewrite existing rows into
  values outside the target column's declared precision.

### Timestamptz typmod defaults store unrounded values

- Reproducer: `TestTimestamptzTypmodDefaultRoundsStoredValueRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimestamptzTypmodDefaultRoundsStoredValueRepro -count=1`.
- Expected PostgreSQL behavior: a default used for a `TIMESTAMPTZ(0)` column is
  coerced through the target column typmod before storage, so default
  `2021-09-15 21:43:56.789+00` stores as `2021-09-15 21:43:57+00` when the
  session time zone is UTC.
- Observed Doltgres behavior: inserting a row that uses the column default
  stores `2021-09-15 21:43:56.789+00`, so implicit writes through defaults can
  persist timestamptz values outside the target column's declared precision.

### Timestamptz typmod COPY FROM stores unrounded values

- Reproducer: `TestTimestamptzTypmodCopyFromRoundsStoredValueRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimestamptzTypmodCopyFromRoundsStoredValueRepro -count=1`.
- Expected PostgreSQL behavior: `COPY FROM STDIN` into a `TIMESTAMPTZ(0)`
  column applies the target column typmod before storage, so copied
  `2021-09-15 21:43:56.789+00` stores as `2021-09-15 21:43:57+00` when the
  session time zone is UTC.
- Observed Doltgres behavior: the copied row stores
  `2021-09-15 21:43:56.789+00`, so bulk ingestion can persist timestamptz
  values outside the target column's declared precision.

### Timestamptz typmod UPDATE stores unrounded values

- Reproducer: `TestTimestamptzTypmodUpdateRoundsStoredValueRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimestamptzTypmodUpdateRoundsStoredValueRepro -count=1`.
- Expected PostgreSQL behavior: `UPDATE` assignments into a `TIMESTAMPTZ(0)`
  column apply the target column typmod before storage, so updating a row to
  `2021-09-15 21:43:56.789+00` stores as `2021-09-15 21:43:57+00` when the
  session time zone is UTC.
- Observed Doltgres behavior: the updated row stores
  `2021-09-15 21:43:56.789+00`, so ordinary updates can rewrite existing rows
  into timestamptz values outside the target column's declared precision.

### Timestamptz typmod ON CONFLICT UPDATE stores unrounded values

- Reproducer: `TestTimestamptzTypmodOnConflictUpdateRoundsStoredValueRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimestamptzTypmodOnConflictUpdateRoundsStoredValueRepro -count=1`.
- Expected PostgreSQL behavior: `ON CONFLICT DO UPDATE` assignments into a
  `TIMESTAMPTZ(0)` column apply the target column typmod before storage, so the
  conflict update stores `2021-09-15 21:43:57+00` when the session time zone is
  UTC.
- Observed Doltgres behavior: the conflict update stores
  `2021-09-15 21:43:56.789+00`, so upsert writes can rewrite existing rows into
  timestamptz values outside the target column's declared precision.

### Timestamptz typmod INSERT SELECT stores unrounded values

- Reproducer: `TestTimestamptzTypmodInsertSelectRoundsStoredValuesRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimestamptzTypmodInsertSelectRoundsStoredValuesRepro -count=1`.
- Expected PostgreSQL behavior: `INSERT ... SELECT` into a `TIMESTAMPTZ(0)`
  column applies the target column typmod before storage, so source
  `2021-09-15 21:43:56.789+00` stores as `2021-09-15 21:43:57+00` when the
  session time zone is UTC.
- Observed Doltgres behavior: the inserted row stores
  `2021-09-15 21:43:56.789+00`, so set-based inserts can persist timestamptz
  values outside the target column's declared precision.

### Timestamptz typmod UPDATE FROM stores unrounded values

- Reproducer: `TestTimestamptzTypmodUpdateFromRoundsStoredValuesRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimestamptzTypmodUpdateFromRoundsStoredValuesRepro -count=1`.
- Expected PostgreSQL behavior: `UPDATE ... FROM` assignments into a
  `TIMESTAMPTZ(0)` column apply the target column typmod before storage, so
  joined source `2021-09-15 21:43:56.789+00` stores as
  `2021-09-15 21:43:57+00` when the session time zone is UTC.
- Observed Doltgres behavior: the updated row stores
  `2021-09-15 21:43:56.789+00`, so joined updates can rewrite existing rows
  into timestamptz values outside the target column's declared precision.

### Timestamp typmod table CHECK constraints see unrounded values

- Reproducer: `TestTimestampTypmodTableCheckUsesRoundedValueRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimestampTypmodTableCheckUsesRoundedValueRepro -count=1`.
- Expected PostgreSQL behavior: table `CHECK` constraints over a
  `TIMESTAMP(0)` column are evaluated after the target column typmod is applied.
  A check comparing to `2021-09-15 21:43:56.789` rejects an inserted
  `2021-09-15 21:43:56.789`, because the checked row value is
  `2021-09-15 21:43:57`.
- Observed Doltgres behavior: the insert succeeds and the table contains one
  row, so table constraints can validate and persist timestamp values using raw
  unrounded input instead of the target column's declared precision.

### Timestamptz typmod table CHECK constraints see unrounded values

- Reproducer: `TestTimestamptzTypmodTableCheckUsesRoundedValueRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimestamptzTypmodTableCheckUsesRoundedValueRepro -count=1`.
- Expected PostgreSQL behavior: table `CHECK` constraints over a
  `TIMESTAMPTZ(0)` column are evaluated after the target column typmod is
  applied. A check comparing to `2021-09-15 21:43:56.789+00` rejects an inserted
  `2021-09-15 21:43:56.789+00`, because the checked row value is
  `2021-09-15 21:43:57+00` when the session time zone is UTC.
- Observed Doltgres behavior: the insert succeeds and the table contains one
  row, so table constraints can validate and persist timestamptz values using
  raw unrounded input instead of the target column's declared precision.

### Timestamp typmod generated columns store unrounded values

- Reproducer: `TestTimestampTypmodGeneratedColumnRoundsStoredValueRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimestampTypmodGeneratedColumnRoundsStoredValueRepro -count=1`.
- Expected PostgreSQL behavior: stored generated columns declared as
  `TIMESTAMP(0)` apply the generated column's typmod before storage, so
  generated value `2021-09-15 21:43:56.789` stores as
  `2021-09-15 21:43:57`.
- Observed Doltgres behavior: the generated column stores
  `2021-09-15 21:43:56.789`, so generated timestamp columns can persist values
  outside their declared precision.

### Timestamptz typmod generated columns store unrounded values

- Reproducer: `TestTimestamptzTypmodGeneratedColumnRoundsStoredValueRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimestamptzTypmodGeneratedColumnRoundsStoredValueRepro -count=1`.
- Expected PostgreSQL behavior: stored generated columns declared as
  `TIMESTAMPTZ(0)` apply the generated column's typmod before storage, so
  generated value `2021-09-15 21:43:56.789+00` stores as
  `2021-09-15 21:43:57+00` when the session time zone is UTC.
- Observed Doltgres behavior: the generated column stores
  `2021-09-15 21:43:56.789+00`, so generated timestamptz columns can persist
  values outside their declared precision.

### Timestamp typmod UNIQUE constraints use unrounded values

- Reproducer: `TestTimestampTypmodUniqueUsesRoundedValuesRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimestampTypmodUniqueUsesRoundedValuesRepro -count=1`.
- Expected PostgreSQL behavior: unique constraints are enforced after timestamp
  typmod coercion. Inserting `2021-09-15 21:43:56.600` and then
  `2021-09-15 21:43:56.700` into a `TIMESTAMP(0) UNIQUE` column rejects the
  second row because both values round to `2021-09-15 21:43:57`.
- Observed Doltgres behavior: both rows are accepted and stored as
  `2021-09-15 21:43:56.6` and `2021-09-15 21:43:56.7`, so a unique timestamp
  key can contain duplicate PostgreSQL-visible values.

### Timestamptz typmod UNIQUE constraints use unrounded values

- Reproducer: `TestTimestamptzTypmodUniqueUsesRoundedValuesRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimestamptzTypmodUniqueUsesRoundedValuesRepro -count=1`.
- Expected PostgreSQL behavior: unique constraints are enforced after
  timestamptz typmod coercion. Inserting `2021-09-15 21:43:56.600+00` and then
  `2021-09-15 21:43:56.700+00` into a `TIMESTAMPTZ(0) UNIQUE` column rejects
  the second row because both values round to `2021-09-15 21:43:57+00` when the
  session time zone is UTC.
- Observed Doltgres behavior: both rows are accepted and stored as
  `2021-09-15 21:43:56.6+00` and `2021-09-15 21:43:56.7+00`, so a unique
  timestamptz key can contain duplicate PostgreSQL-visible values.

### Timestamp typmod foreign keys use unrounded values

- Reproducer: `TestTimestampTypmodForeignKeyUsesRoundedValuesRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimestampTypmodForeignKeyUsesRoundedValuesRepro -count=1`.
- Expected PostgreSQL behavior: foreign-key comparisons use values after
  timestamp typmod coercion. A parent value inserted as
  `2021-09-15 21:43:56.600` and a child value inserted as
  `2021-09-15 21:43:56.700` both round to `2021-09-15 21:43:57`, so the child
  row is accepted.
- Observed Doltgres behavior: the child insert is rejected as a foreign-key
  violation using raw key `2021-09-15 21:43:56.7`, so referential checks can
  disagree with PostgreSQL-visible timestamp values.

### Timestamptz typmod foreign keys use unrounded values

- Reproducer: `TestTimestamptzTypmodForeignKeyUsesRoundedValuesRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimestamptzTypmodForeignKeyUsesRoundedValuesRepro -count=1`.
- Expected PostgreSQL behavior: foreign-key comparisons use values after
  timestamptz typmod coercion. A parent value inserted as
  `2021-09-15 21:43:56.600+00` and a child value inserted as
  `2021-09-15 21:43:56.700+00` both round to `2021-09-15 21:43:57+00`, so the
  child row is accepted when the session time zone is UTC.
- Observed Doltgres behavior: the child insert is rejected as a foreign-key
  violation using raw key `2021-09-15 21:43:56.7+00`, so referential checks can
  disagree with PostgreSQL-visible timestamptz values.

### Timestamp array element typmods are not applied to stored values

- Reproducer: `TestTimestampArrayTypmodsRoundStoredElementsRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimestampArrayTypmodsRoundStoredElementsRepro -count=1`.
- Expected PostgreSQL behavior: assigning values to `TIMESTAMP(0)[]` rounds
  every timestamp element to whole seconds, and assigning values to
  `TIMESTAMPTZ(2)[]` rounds every timestamptz element to two fractional digits.
- Observed Doltgres behavior: the stored array elements retain `.789` and
  `.123` fractional seconds, so timestamp array element typmods are not enforced
  on assignment.

### TIMETZ column typmods are not applied to stored values

- Reproducer: `TestTimeColumnTypmodsRoundStoredFractionalSecondsRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimeColumnTypmodsRoundStoredFractionalSecondsRepro -count=1`.
- Expected PostgreSQL behavior: assigning `21:43:56.789+00` to a `TIMETZ(0)`
  column stores `21:43:57+00`.
- Observed Doltgres behavior: the stored `TIMETZ(0)` value remains
  `21:43:56.789+00`, so timetz column typmods are not enforced on ordinary
  inserts.

### TIMETZ array element typmods are not applied to stored values

- Reproducer: `TestTimeArrayTypmodsRoundStoredElementsRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimeArrayTypmodsRoundStoredElementsRepro -count=1`.
- Expected PostgreSQL behavior: assigning values to `TIMETZ(0)[]` rounds every
  timetz element to whole seconds, so `21:43:56.789+00` stores as
  `21:43:57+00`.
- Observed Doltgres behavior: the stored array elements retain
  `21:43:56.789+00` and `21:43:57.123+00`, so timetz array element typmods are
  not enforced on assignment.

### TIMETZ typmod defaults store unrounded values

- Reproducer: `TestTimeTypmodDefaultRoundsStoredValueRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimeTypmodDefaultRoundsStoredValueRepro -count=1`.
- Expected PostgreSQL behavior: a default used for a `TIMETZ(0)` column is
  coerced through the target column typmod before storage, so default
  `21:43:56.789+00` stores as `21:43:57+00`.
- Observed Doltgres behavior: inserting a row that uses the column default
  stores `21:43:56.789+00`, so implicit writes through defaults can persist
  timetz values outside the target column's declared precision.

### TIMETZ typmod UPDATE stores unrounded values

- Reproducer: `TestTimeTypmodUpdateRoundsStoredValueRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimeTypmodUpdateRoundsStoredValueRepro -count=1`.
- Expected PostgreSQL behavior: `UPDATE` assignments into a `TIMETZ(0)` column
  apply the target column typmod before storage, so updating a row to
  `21:43:56.789+00` stores as `21:43:57+00`.
- Observed Doltgres behavior: the updated row stores `21:43:56.789+00`, so
  ordinary updates can rewrite existing rows into timetz values outside the
  target column's declared precision.

### TIMETZ typmod ON CONFLICT UPDATE stores unrounded values

- Reproducer: `TestTimeTypmodOnConflictUpdateRoundsStoredValueRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimeTypmodOnConflictUpdateRoundsStoredValueRepro -count=1`.
- Expected PostgreSQL behavior: `ON CONFLICT DO UPDATE` assignments into a
  `TIMETZ(0)` column apply the target column typmod before storage, so the
  conflict update stores `21:43:57+00`.
- Observed Doltgres behavior: the conflict update stores `21:43:56.789+00`, so
  upsert writes can rewrite existing rows into timetz values outside the target
  column's declared precision.

### TIMETZ typmod INSERT SELECT stores unrounded values

- Reproducer: `TestTimeTypmodInsertSelectRoundsStoredValuesRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimeTypmodInsertSelectRoundsStoredValuesRepro -count=1`.
- Expected PostgreSQL behavior: `INSERT ... SELECT` into a `TIMETZ(0)` column
  applies the target column typmod before storage, so selected
  `21:43:56.789+00` stores as `21:43:57+00`.
- Observed Doltgres behavior: the inserted row stores `21:43:56.789+00`, so
  set-based inserts can persist timetz values outside the target column's
  declared precision.

### TIMETZ typmod UPDATE FROM stores unrounded values

- Reproducer: `TestTimeTypmodUpdateFromRoundsStoredValuesRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimeTypmodUpdateFromRoundsStoredValuesRepro -count=1`.
- Expected PostgreSQL behavior: `UPDATE ... FROM` assignments into a
  `TIMETZ(0)` column apply the target column typmod before storage, so joined
  source `21:43:56.789+00` stores as `21:43:57+00`.
- Observed Doltgres behavior: the updated row stores `21:43:56.789+00`, so
  joined updates can rewrite existing rows into timetz values outside the target
  column's declared precision.

### TIMETZ typmod table CHECK constraints see unrounded values

- Reproducer: `TestTimeTypmodTableCheckUsesRoundedValueRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimeTypmodTableCheckUsesRoundedValueRepro -count=1`.
- Expected PostgreSQL behavior: table `CHECK` constraints over a `TIMETZ(0)`
  column are evaluated after the target column typmod is applied. A check
  comparing to `21:43:56.789+00` rejects inserted `21:43:56.789+00`, because
  the checked row value is `21:43:57+00`.
- Observed Doltgres behavior: the insert succeeds and the table contains one
  row, so table constraints can validate and persist timetz values using raw
  unrounded input instead of the target column's declared precision.

### TIMETZ typmod generated columns store unrounded values

- Reproducer: `TestTimeTypmodGeneratedColumnUsesRoundedValueRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimeTypmodGeneratedColumnUsesRoundedValueRepro -count=1`.
- Expected PostgreSQL behavior: stored generated columns declared as
  `TIMETZ(0)` apply the generated column's typmod before storage, so generated
  value `21:43:56.789+00` stores as `21:43:57+00`.
- Observed Doltgres behavior: the generated column stores `21:43:56.789+00`,
  so generated timetz columns can persist values outside their declared
  precision.

### TIMETZ typmod UNIQUE constraints use unrounded values

- Reproducer: `TestTimeTypmodUniqueUsesRoundedValuesRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimeTypmodUniqueUsesRoundedValuesRepro -count=1`.
- Expected PostgreSQL behavior: unique constraints are enforced after timetz
  typmod coercion. Inserting `21:43:56.600+00` and then `21:43:56.700+00`
  into a `TIMETZ(0) UNIQUE` column rejects the second row because both values
  round to `21:43:57+00`.
- Observed Doltgres behavior: both rows are accepted, so a unique timetz key can
  contain duplicate PostgreSQL-visible values.

### TIMETZ typmod foreign keys use unrounded values

- Reproducer: `TestTimeTypmodForeignKeyUsesRoundedValuesRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimeTypmodForeignKeyUsesRoundedValuesRepro -count=1`.
- Expected PostgreSQL behavior: foreign-key comparisons use values after timetz
  typmod coercion. A parent value inserted as `21:43:56.600+00` and a child
  value inserted as `21:43:56.700+00` both round to `21:43:57+00`, so the child
  row is accepted.
- Observed Doltgres behavior: the child insert is rejected as a foreign-key
  violation, so referential checks can disagree with PostgreSQL-visible timetz
  values.

### TIMETZ typmod casts return unrounded values

- Reproducer: `TestTimeTypmodCastsUseRoundedValueRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTimeTypmodCastsUseRoundedValueRepro -count=1`.
- Expected PostgreSQL behavior: explicit casts to timetz typmod types enforce
  the requested precision. Casting `21:43:56.789+00` to `TIMETZ(0)` returns
  `21:43:57+00`.
- Observed Doltgres behavior: the cast returns `21:43:56.789+00`, so
  expression-level casts can leak precision that PostgreSQL removes.

### Interval field typmods are not applied to stored values

- Reproducer: `TestIntervalFieldTypmodsRestrictStoredFieldsRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestIntervalFieldTypmodsRestrictStoredFieldsRepro -count=1`.
- Expected PostgreSQL behavior: assigning
  `'1 year 2 months 3 days 04:05:06.789'` to an
  `INTERVAL YEAR TO MONTH` column stores `1 year 2 mons`, and assigning
  `'3 days 04:05:06.789'` to an `INTERVAL DAY TO SECOND(0)` column stores
  `3 days 04:05:07`.
- Observed Doltgres behavior: both columns retain the unrestricted input fields,
  storing `1 year 2 mons 3 days 04:05:06.789` and `3 days 04:05:06.789`, so
  interval typmods are not enforced on column assignment.

### Interval typmod defaults store unrestricted values

- Reproducer: `TestIntervalTypmodDefaultRestrictsStoredValueRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestIntervalTypmodDefaultRestrictsStoredValueRepro -count=1`.
- Expected PostgreSQL behavior: a default used for an
  `INTERVAL DAY TO SECOND(0)` column is coerced through the target column typmod
  before storage, so default `3 days 04:05:06.789` stores as
  `3 days 04:05:07`.
- Observed Doltgres behavior: inserting a row that uses the column default
  stores `3 days 04:05:06.789`, so implicit writes through defaults can persist
  interval values outside the target column's declared field/precision.

### Explicit DEFAULT inserts store uncoerced temporal typmod defaults

- Reproducer: `TestTemporalTypmodExplicitDefaultCoercesStoredValuesRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTemporalTypmodExplicitDefaultCoercesStoredValuesRepro -count=1`.
- Expected PostgreSQL behavior: `INSERT ... VALUES (..., DEFAULT, ...)`
  coerces each temporal default through the target column's declared typmod
  before storage: `timestamp(0)` and `timestamptz(0)` defaults round to
  `2021-09-15 21:43:57`, `time(0)` and `timetz(0)` defaults round to
  `21:43:57`, and `interval day to second(0)` defaults restrict to
  `3 days 04:05:07`.
- Observed Doltgres behavior: explicit `DEFAULT` inserts store unrounded
  `timestamp`, `timestamptz`, `timetz`, and unrestricted `interval` defaults
  (`2021-09-15 21:43:56.789`, `2021-09-15 21:43:56.789+00`,
  `21:43:56.789+00`, and `3 days 04:05:06.789`). The plain `time(0)` default
  is rounded, so explicit default insertion applies temporal typmods
  inconsistently and can persist values outside declared precision.

### ALTER COLUMN SET DEFAULT stores uncoerced temporal typmod defaults

- Reproducer: `TestTemporalTypmodAlterSetDefaultCoercesStoredValuesRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTemporalTypmodAlterSetDefaultCoercesStoredValuesRepro -count=1`.
- Expected PostgreSQL behavior: defaults installed with `ALTER TABLE ... ALTER
  COLUMN ... SET DEFAULT` are coerced through the target column's declared
  typmod on future writes: `timestamp(0)` and `timestamptz(0)` defaults round
  to `2021-09-15 21:43:57`, `time(0)` and `timetz(0)` defaults round to
  `21:43:57`, and `interval day to second(0)` defaults restrict to
  `3 days 04:05:07`.
- Observed Doltgres behavior: future inserts using defaults installed by
  `ALTER COLUMN SET DEFAULT` store unrounded `timestamp`, `timestamptz`,
  `timetz`, and unrestricted `interval` values (`2021-09-15 21:43:56.789`,
  `2021-09-15 21:43:56.789+00`, `21:43:56.789+00`, and
  `3 days 04:05:06.789`). The plain `time(0)` default is rounded, so altered
  defaults can persist values outside declared temporal precision.

### UPDATE SET DEFAULT stores uncoerced temporal typmod defaults

- Reproducer: `TestTemporalTypmodUpdateSetDefaultCoercesStoredValuesRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTemporalTypmodUpdateSetDefaultCoercesStoredValuesRepro -count=1`.
- Expected PostgreSQL behavior: `UPDATE ... SET column = DEFAULT` coerces each
  temporal default through the target column's declared typmod before storage:
  `timestamp(0)` and `timestamptz(0)` defaults round to
  `2021-09-15 21:43:57`, `time(0)` and `timetz(0)` defaults round to
  `21:43:57`, and `interval day to second(0)` defaults restrict to
  `3 days 04:05:07`.
- Observed Doltgres behavior: `UPDATE SET DEFAULT` stores unrounded
  `timestamp`, `timestamptz`, `timetz`, and unrestricted `interval` defaults
  (`2021-09-15 21:43:56.789`, `2021-09-15 21:43:56.789+00`,
  `21:43:56.789+00`, and `3 days 04:05:06.789`). The plain `time(0)` default
  is rounded, so this path applies temporal typmods inconsistently and can
  persist values outside declared precision.

### ON CONFLICT SET DEFAULT panics before applying temporal defaults

- Reproducer: `TestTemporalTypmodOnConflictSetDefaultCoercesStoredValuesRepro`
  in `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTemporalTypmodOnConflictSetDefaultCoercesStoredValuesRepro -count=1`.
- Expected PostgreSQL behavior: `INSERT ... ON CONFLICT DO UPDATE SET column =
  DEFAULT` writes each column default through the target column's declared
  typmod, so temporal defaults round or restrict to the same stored values as
  ordinary `UPDATE SET DEFAULT`.
- Observed Doltgres behavior: the conflict update fails before writing the row
  with `receiveMessage recovered panic: default column is a placeholder node,
  but Type was called`, from `assignUpdateFieldCasts` while analyzing the
  `DEFAULT` assignments. The existing row remains unchanged instead of being
  updated to the PostgreSQL-compatible default values.

### Interval typmod COPY FROM stores unrestricted values

- Reproducer: `TestIntervalTypmodCopyFromRestrictsStoredValueRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestIntervalTypmodCopyFromRestrictsStoredValueRepro -count=1`.
- Expected PostgreSQL behavior: `COPY FROM STDIN` into an
  `INTERVAL DAY TO SECOND(0)` column applies the target column typmod before
  storage, so copied `3 days 04:05:06.789` stores as `3 days 04:05:07`.
- Observed Doltgres behavior: the copied row stores `3 days 04:05:06.789`, so
  bulk ingestion can persist interval values outside the target column's
  declared field/precision.

### Interval typmod UPDATE stores unrestricted values

- Reproducer: `TestIntervalTypmodUpdateRestrictsStoredValueRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestIntervalTypmodUpdateRestrictsStoredValueRepro -count=1`.
- Expected PostgreSQL behavior: `UPDATE` assignments into an
  `INTERVAL DAY TO SECOND(0)` column apply the target column typmod before
  storage, so updating a row to `3 days 04:05:06.789` stores as
  `3 days 04:05:07`.
- Observed Doltgres behavior: the updated row stores `3 days 04:05:06.789`, so
  ordinary updates can rewrite existing rows into interval values outside the
  target column's declared field/precision.

### Interval typmod ON CONFLICT UPDATE stores unrestricted values

- Reproducer: `TestIntervalTypmodOnConflictUpdateRestrictsStoredValueRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestIntervalTypmodOnConflictUpdateRestrictsStoredValueRepro -count=1`.
- Expected PostgreSQL behavior: `ON CONFLICT DO UPDATE` assignments into an
  `INTERVAL DAY TO SECOND(0)` column apply the target column typmod before
  storage, so the conflict update stores `3 days 04:05:07`.
- Observed Doltgres behavior: the conflict update stores
  `3 days 04:05:06.789`, so upsert writes can rewrite existing rows into
  interval values outside the target column's declared field/precision.

### Interval typmod INSERT SELECT stores unrestricted values

- Reproducer: `TestIntervalTypmodInsertSelectRestrictsStoredValueRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestIntervalTypmodInsertSelectRestrictsStoredValueRepro -count=1`.
- Expected PostgreSQL behavior: `INSERT ... SELECT` into an
  `INTERVAL DAY TO SECOND(0)` column applies the target column typmod before
  storage, so selected `3 days 04:05:06.789` stores as `3 days 04:05:07`.
- Observed Doltgres behavior: the inserted row stores
  `3 days 04:05:06.789`, so set-based inserts can persist interval values
  outside the target column's declared field/precision.

### Interval typmod UPDATE FROM stores unrestricted values

- Reproducer: `TestIntervalTypmodUpdateFromRestrictsStoredValueRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestIntervalTypmodUpdateFromRestrictsStoredValueRepro -count=1`.
- Expected PostgreSQL behavior: `UPDATE ... FROM` assignments into an
  `INTERVAL DAY TO SECOND(0)` column apply the target column typmod before
  storage, so joined source `3 days 04:05:06.789` stores as
  `3 days 04:05:07`.
- Observed Doltgres behavior: the updated row stores
  `3 days 04:05:06.789`, so joined updates can rewrite existing rows into
  interval values outside the target column's declared field/precision.

### Interval typmod table CHECK constraints see unrestricted values

- Reproducer: `TestIntervalTypmodTableCheckUsesRestrictedValueRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestIntervalTypmodTableCheckUsesRestrictedValueRepro -count=1`.
- Expected PostgreSQL behavior: table `CHECK` constraints over an
  `INTERVAL DAY TO SECOND(0)` column are evaluated after the target column
  typmod is applied. A check comparing to `3 days 04:05:06.789` rejects inserted
  `3 days 04:05:06.789`, because the checked row value is
  `3 days 04:05:07`.
- Observed Doltgres behavior: the insert succeeds and the table contains one
  row, so table constraints can validate and persist interval values using raw
  unrestricted input instead of the target column's declared field/precision.

### Interval typmod generated columns store unrestricted values

- Reproducer: `TestIntervalTypmodGeneratedColumnUsesRestrictedValueRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestIntervalTypmodGeneratedColumnUsesRestrictedValueRepro -count=1`.
- Expected PostgreSQL behavior: stored generated columns declared as
  `INTERVAL DAY TO SECOND(0)` apply the generated column's typmod before
  storage, so generated value `3 days 04:05:06.789` stores as
  `3 days 04:05:07`.
- Observed Doltgres behavior: the generated column stores
  `3 days 04:05:06.789`, so generated interval columns can persist values
  outside their declared field/precision.

### Interval typmod UNIQUE constraints use unrestricted values

- Reproducer: `TestIntervalTypmodUniqueUsesRestrictedValuesRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestIntervalTypmodUniqueUsesRestrictedValuesRepro -count=1`.
- Expected PostgreSQL behavior: unique constraints are enforced after interval
  typmod coercion. Inserting `3 days 04:05:06.600` and then
  `3 days 04:05:06.700` into an `INTERVAL DAY TO SECOND(0) UNIQUE` column
  rejects the second row because both values round to `3 days 04:05:07`.
- Observed Doltgres behavior: both rows are accepted, so a unique interval key
  can contain duplicate PostgreSQL-visible values.

### Interval typmod foreign keys use unrestricted values

- Reproducer: `TestIntervalTypmodForeignKeyUsesRestrictedValuesRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestIntervalTypmodForeignKeyUsesRestrictedValuesRepro -count=1`.
- Expected PostgreSQL behavior: foreign-key comparisons use values after
  interval typmod coercion. A parent value inserted as `3 days 04:05:06.600`
  and a child value inserted as `3 days 04:05:06.700` both round to
  `3 days 04:05:07`, so the child row is accepted.
- Observed Doltgres behavior: the child insert is rejected as a foreign-key
  violation, so referential checks can disagree with PostgreSQL-visible
  interval values.

### Interval typmod casts return unrestricted values

- Reproducer: `TestIntervalTypmodCastsUseRestrictedValueRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestIntervalTypmodCastsUseRestrictedValueRepro -count=1`.
- Expected PostgreSQL behavior: explicit casts to interval typmod types enforce
  the requested fields and precision. Casting
  `1 year 2 months 3 days 04:05:06.789` to `INTERVAL YEAR TO MONTH` returns
  `1 year 2 mons`, and casting `3 days 04:05:06.789` to
  `INTERVAL DAY TO SECOND(0)` returns `3 days 04:05:07`.
- Observed Doltgres behavior: the casts return unrestricted interval values, so
  expression-level casts can leak fields and precision that PostgreSQL removes.

### Interval array element typmods are not applied to stored values

- Reproducer: `TestIntervalArrayTypmodsRestrictStoredElementsRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestIntervalArrayTypmodsRestrictStoredElementsRepro -count=1`.
- Expected PostgreSQL behavior: assigning values to
  `INTERVAL YEAR TO MONTH[]` drops day-time fields from each array element, and
  assigning values to `INTERVAL DAY TO SECOND(0)[]` rounds each element to whole
  seconds.
- Observed Doltgres behavior: the stored array elements retain unrestricted
  fields and fractional seconds, so interval array element typmods are not
  enforced on assignment.

### extract/date_part interval quarter uses the wrong month bucket

- Reproducers: `TestExtractIntervalQuarterUsesPostgresMonthBucketRepro` and
  `TestDatePartIntervalQuarterUsesPostgresMonthBucketRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'Test(Extract|DatePart)IntervalQuarterUsesPostgresMonthBucketRepro' -count=1`.
- Expected PostgreSQL behavior: `extract(quarter FROM interval '3 months')` and
  `date_part('quarter', interval '3 months')` both return `2`.
- Observed Doltgres behavior: both functions return `1`, putting an interval
  month field of three months in the previous quarter bucket.

### extract/date_part negative interval fields use floored totals

- Reproducers: `TestExtractNegativeIntervalFieldsUsePostgresNormalizationRepro`
  and `TestDatePartNegativeIntervalFieldsUsePostgresNormalizationRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'Test(Extract|DatePart)NegativeIntervalFieldsUsePostgresNormalizationRepro'
  -count=1`.
- Expected PostgreSQL behavior: extracting hour/minute/second from `interval
  '-65 minutes 10 seconds'` returns normalized fields `-1`, `-4`, and
  `-50.000000` respectively; extracting year/decade from `interval '-13
  months'` returns `-1` and `0`.
- Observed Doltgres behavior: hour and minute extraction return `-2` and `-5`,
  and year/decade extraction returns `-2` and `-1`, flooring totals rather than
  using PostgreSQL's normalized negative interval fields.

### to_char(interval) treats nanoseconds as microseconds and drops fractional seconds

- Reproducer: `TestToCharIntervalPreservesFractionalSecondsRepro` in
  `testing/go/to_char_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestToCharIntervalPreservesFractionalSecondsRepro -count=1`.
- Expected PostgreSQL behavior: `to_char(interval '1.234 seconds',
  'HH24:MI:SS.US')` returns `00:00:01.234000`, and formatting `interval '1 hour
  2 minutes 3.456 seconds'` with `HH24:MI:SS.MS.US SSSS` returns
  `01:02:03.456.456000 3723`.
- Observed Doltgres behavior: the same calls return `00:20:34.000000` and
  `34:17:36.000.000000 3723456`, showing that interval nanoseconds are scaled as
  microseconds and the fractional remainder is lost.

### to_char FF1 through FF5 use the wrong fractional-second digits

- Reproducer: `TestToCharFractionalSecondPrecisionTokensRepro` in
  `testing/go/to_char_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestToCharFractionalSecondPrecisionTokensRepro -count=1`.
- Expected PostgreSQL behavior: formatting `timestamp '2021-09-15
  21:43:56.123456'` with `FF1 FF2 FF3 FF4 FF5 FF6` returns
  `1 12 123 1234 12345 123456`.
- Observed Doltgres behavior: the same call returns
  `3 23 123 0123 00123 123456`, showing that shorter `FF` tokens are derived
  from truncated milliseconds instead of the leading fractional-second digits.

### to_char drops January and Sunday names

- Reproducer: `TestToCharFirstMonthAndWeekdayNamesRepro` in
  `testing/go/to_char_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestToCharFirstMonthAndWeekdayNamesRepro -count=1`.
- Expected PostgreSQL behavior: formatting `timestamp '2021-01-03 12:00:00'`
  with `MONTH Month month MON Mon mon MM DAY Day day DY Dy dy D` returns
  the full January and Sunday names, including `JANUARY`, `January`, `january`,
  `JAN`, `Jan`, `jan`, `SUNDAY`, `Sunday`, `sunday`, `SUN`, `Sun`, and `sun`.
- Observed Doltgres behavior: the same call returns only `      01       1`,
  dropping every January and Sunday text token.

### to_char ordinal suffixes mishandle 11, 12, and 13

- Reproducer: `TestToCharOrdinalSuffixTeenDatesRepro` in
  `testing/go/to_char_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestToCharOrdinalSuffixTeenDatesRepro -count=1`.
- Expected PostgreSQL behavior: formatting dates 11, 12, and 13 with `DDTH DDth`
  returns `11TH 11th`, `12TH 12th`, and `13TH 13th`.
- Observed Doltgres behavior: the same calls return `11ST 11st`, `12ND 12nd`,
  and `13RD 13rd`, applying suffixes based only on the last digit.

### to_char(numeric, text) is registered but always returns not supported

- Reproducer: `TestToCharNumericFormatsPostgresPatternsRepro` in
  `testing/go/to_char_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestToCharNumericFormatsPostgresPatternsRepro -count=1`.
- Expected PostgreSQL behavior: `to_char(1234.5::numeric, 'FM9,999.00')` returns
  `1,234.50`, and `to_char((-42.5)::numeric, 'S999.9')` returns ` -42.5`.
- Observed Doltgres behavior: both queries fail with
  `to_char(numeric,text) is not supported yet` even though the PostgreSQL
  function is registered.

### time with time zone - interval has reversed operator registration

- Reproducers: `TestTimetzMinusIntervalSubtractsIntervalRepro` and
  `TestIntervalMinusTimetzIsRejectedRepro` in
  `testing/go/datetime_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'Test(TimetzMinusIntervalSubtractsInterval|IntervalMinusTimetzIsRejected)Repro'
  -count=1`.
- Expected PostgreSQL behavior: `time with time zone '04:05:06+00' - interval
  '2 minutes'` returns `04:03:06+00`, while `interval '2 minutes' - time with
  time zone '04:05:06+00'` fails because that operator does not exist.
- Observed Doltgres behavior: the valid `timetz - interval` operator fails with
  `function internal_binary_operator_func_-(timetz, interval) does not exist`,
  while the invalid `interval - timetz` expression succeeds.

### JSON DISTINCT aggregates do not require a json equality operator

- Reproducers: `TestJsonAggDistinctRequiresJsonEqualityOperatorRepro` and
  `TestJsonObjectAggDistinctRequiresJsonEqualityOperatorRepro` in
  `testing/go/json_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'TestJson(Agg|ObjectAgg)DistinctRequiresJsonEqualityOperatorRepro' -count=1`.
- Expected PostgreSQL behavior: `json_agg(DISTINCT doc)` and
  `json_object_agg(DISTINCT key, doc)` over `json` values fail because
  PostgreSQL's plain `json` type has no equality operator.
- Observed Doltgres behavior: both queries succeed and aggregate the `json`
  values instead of reporting the missing equality operator.

### JSON extraction operators canonicalize json subdocuments as jsonb

- Reproducers: `TestJsonExtractionPreservesJsonSubdocumentTextRepro` and
  `TestJsonExtractionPreservesJsonObjectKeyOrderRepro` in
  `testing/go/json_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestJsonExtractionPreservesJson(SubdocumentText|ObjectKeyOrder)Repro'
  -count=1`.
- Expected PostgreSQL behavior: `json` extraction operators and functions such
  as `->`, `#>`, `json_extract_path`, and `json_extract_path_text` preserve the
  extracted subdocument's original JSON text, including spacing and object key
  order.
- Observed Doltgres behavior: extracted `json` subdocuments are parsed and
  re-emitted through the canonical JSONB representation, so whitespace is
  normalized and keys are reordered, changing visible `json` values.

### Plain JSON object iterators canonicalize object order and duplicate keys

- Reproducers: `TestJsonEachPreservesJsonObjectOrderAndDuplicatesRepro` and
  `TestJsonObjectKeysPreservesJsonObjectOrderAndDuplicatesRepro` in
  `testing/go/json_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestJson(Each|ObjectKeys)PreservesJsonObjectOrderAndDuplicatesRepro'
  -count=1`.
- Expected PostgreSQL behavior: plain `json` object iterators preserve object
  field order and duplicate keys, so `json_each('{"b":1,"a":2}'::json)` emits
  `b:1,a:2`, `json_each('{"a":1,"a":2}'::json)` emits both `a` entries, and
  `json_object_keys` follows the same ordered duplicate-key behavior.
- Observed Doltgres behavior: the same calls emit sorted keys such as
  `a:2,b:1` / `a,b` and collapse duplicates to only `a:2` / `a`, showing that
  plain `json` is parsed through a JSONB-style sorted/deduplicated object
  representation before object iterators run.

### JSON array element functions canonicalize plain json element text

- Reproducer: `TestJsonArrayElementsPreservesJsonElementTextRepro` in
  `testing/go/json_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestJsonArrayElementsPreservesJsonElementTextRepro -count=1`.
- Expected PostgreSQL behavior: `json_array_elements` and
  `json_array_elements_text` preserve each plain `json` element's text, so
  object key order, duplicate keys, and whitespace survive when array elements
  are emitted.
- Observed Doltgres behavior: the same functions emit JSONB-style canonicalized
  elements such as `{"a": 2, "b": 1}`, collapse duplicate keys to `{"a": 2}`,
  and normalize whitespace, changing visible plain `json` values.

### JSON object builders accept non-scalar keys

- Reproducers: `TestJsonBuildObjectRejectsNonScalarKeysRepro` and
  `TestJsonbBuildObjectRejectsNonScalarKeysRepro` in
  `testing/go/json_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestJsonb?BuildObjectRejectsNonScalarKeysRepro' -count=1`.
- Expected PostgreSQL behavior: `json_build_object` and `jsonb_build_object`
  reject JSON, array, and composite key arguments with `key value must be
  scalar, not array, composite, or json`.
- Observed Doltgres behavior: the same object builders accept non-scalar key
  arguments and stringify them into object keys, so invalid JSON object
  construction succeeds instead of failing.

### JSON object aggregates accept non-scalar keys

- Reproducers: `TestJsonObjectAggRejectsNonScalarKeysRepro` and
  `TestJsonbObjectAggRejectsNonScalarKeysRepro` in
  `testing/go/json_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestJsonb?ObjectAggRejectsNonScalarKeysRepro' -count=1`.
- Expected PostgreSQL behavior: `json_object_agg` and `jsonb_object_agg`
  reject JSON, array, and composite key arguments with `key value must be
  scalar, not array, composite, or json`.
- Observed Doltgres behavior: the same object aggregates accept non-scalar key
  arguments and stringify them into object keys, so invalid aggregate object
  construction succeeds instead of failing.

### Plain JSON aggregates canonicalize json input values

- Reproducer: `TestJsonAggregatesPreserveJsonInputTextRepro` in
  `testing/go/json_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestJsonAggregatesPreserveJsonInputTextRepro -count=1`.
- Expected PostgreSQL behavior: `json_agg` and `json_object_agg` over plain
  `json` values preserve each value's visible JSON text, including object key
  order, duplicate keys, and whitespace.
- Observed Doltgres behavior: the same aggregate outputs canonicalize nested
  plain `json` values, reordering object keys, dropping duplicate keys, and
  removing whitespace, so aggregate JSON output differs from PostgreSQL and can
  persist altered JSON text.

### json_to_record canonicalizes nested plain json fields

- Reproducer: `TestJsonToRecordPreservesNestedJsonTextRepro` in
  `testing/go/json_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestJsonToRecordPreservesNestedJsonTextRepro -count=1`.
- Expected PostgreSQL behavior: `json_to_record` preserves nested fields
  declared as plain `json`, including nested object key order and duplicate
  keys.
- Observed Doltgres behavior: nested `json` record fields are returned through
  a JSONB-style representation, reordering keys such as `{"b":1,"a":2}` to
  `{"a":2,"b":1}` and collapsing duplicate-key objects to only the last key.

### json_populate_record is missing for plain json

- Reproducer: `TestJsonPopulateRecordPreservesNestedJsonTextRepro` in
  `testing/go/json_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestJsonPopulateRecordPreservesNestedJsonTextRepro -count=1`.
- Expected PostgreSQL behavior: `json_populate_record` accepts a plain `json`
  object and populates a composite row, preserving nested fields declared as
  plain `json`.
- Observed Doltgres behavior: the equivalent `jsonb_populate_record` exists,
  but the plain `json_populate_record` function is missing and valid queries
  fail with `function: 'json_populate_record' not found`.

### JSON object text-array builders mis-handle two-dimensional arrays

- Reproducers: `TestJsonObjectAcceptsTwoDimensionalTextArrayRepro` and
  `TestJsonObjectTwoDimensionalArrayRequiresTwoColumnsRepro` in
  `testing/go/json_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestJsonObject(AcceptsTwoDimensionalTextArray|TwoDimensionalArrayRequiresTwoColumns)Repro'
  -count=1`.
- Expected PostgreSQL behavior: `json_object(text[])` and
  `jsonb_object(text[])` accept a two-dimensional text array only when each row
  has exactly two columns, interpreting each row as one key/value pair.
- Observed Doltgres behavior: valid two-column arrays are flattened and parsed
  into incorrect keys such as `"{a"`, while one-column and three-column arrays
  are accepted instead of failing with `array must have two columns`.

### JSON object two-array form reports the wrong multidimensional-array error

- Reproducer: `TestJsonObjectTwoArrayFormMultidimensionalInputsErrorRepro` in
  `testing/go/json_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestJsonObjectTwoArrayFormMultidimensionalInputsErrorRepro -count=1`.
- Expected PostgreSQL behavior: `json_object(text[], text[])` and
  `jsonb_object(text[], text[])` reject multidimensional key or value arrays
  with `wrong number of array subscripts`.
- Observed Doltgres behavior: the invalid inputs are rejected with
  `mismatched array dimensions` instead, which does not match PostgreSQL's
  error contract for multidimensional arguments.

### JSON conversion panics on non-finite float values

- Reproducers: `TestToJsonFloatNonFiniteValuesBecomeStringsRepro` and
  `TestJsonBuildersFloatNonFiniteValuesBecomeStringsRepro` in
  `testing/go/json_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'Test(ToJson|JsonBuilders)FloatNonFiniteValuesBecomeStringsRepro'
  -count=1`.
- Expected PostgreSQL behavior: `to_json`, `to_jsonb`, `json_build_array`,
  `jsonb_build_array`, `json_build_object`, and `jsonb_build_object` serialize
  non-finite `float8` values as JSON strings such as `"NaN"`, `"Infinity"`,
  and `"-Infinity"`.
- Observed Doltgres behavior: the same conversions panic through
  `shopspring/decimal` with `Cannot create a Decimal from NaN`, returning an
  internal server error instead of JSON string values.

### Plain JSON float conversion loses negative zero spelling

- Reproducer: `TestJsonFloatNegativeZeroPreservesJsonSpellingRepro` in
  `testing/go/json_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestJsonFloatNegativeZeroPreservesJsonSpellingRepro -count=1`.
- Expected PostgreSQL behavior: conversion to plain `json` preserves `float8`
  negative zero spelling, so `to_json('-0'::float8)::text` returns `-0` and
  `json_build_array('-0'::float8)::text` returns `[-0]`; `jsonb` canonicalizes
  the same value to `0`.
- Observed Doltgres behavior: both plain `json` conversions return `0` / `[0]`,
  so the plain JSON spelling distinction is lost and plain `json` is
  canonicalized like `jsonb`.

### JSON conversion rejects accepted multidimensional array literals

- Reproducer: `TestToJsonMultidimensionalArrayPreservesNestingRepro` in
  `testing/go/json_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestToJsonMultidimensionalArrayPreservesNestingRepro -count=1`.
- Expected PostgreSQL behavior: `to_json('{{1,2},{3,4}}'::int[])::text`
  returns `[[1,2],[3,4]]`, and `to_jsonb` returns `[[1, 2], [3, 4]]`,
  preserving the SQL array's two-dimensional structure.
- Observed Doltgres behavior: the accepted PostgreSQL multidimensional array
  literal fails before JSON conversion with `invalid input syntax for type int4:
  "{1"`, so valid multidimensional array inputs cannot be converted to JSON.

### JSON conversion uses timestamp-with-time-zone formatting for date and timestamp values

- Reproducers: `TestToJsonDateTimestampUsePostgresFormattingRepro` and
  `TestJsonBuildersDateTimestampUsePostgresFormattingRepro` in
  `testing/go/json_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'Test(ToJson|JsonBuilders)DateTimestampUsePostgresFormattingRepro'
  -count=1`.
- Expected PostgreSQL behavior: `to_json`, `to_jsonb`, `json_build_array`,
  `jsonb_build_array`, `json_build_object`, and `jsonb_build_object` serialize
  `date` values as strings such as `"2020-01-02"` and `timestamp without time
  zone` values as strings such as `"2020-01-02T03:04:05.123456"`.
- Observed Doltgres behavior: the same conversions emit generic
  timestamp-with-zone-looking strings such as `"2020-01-02T00:00:00Z"` and
  `"2020-01-02T03:04:05.123456Z"`, changing both scalar JSON conversion output
  and persisted JSON document contents built from date/timestamp values.

### to_json(record) sorts object fields like jsonb

- Reproducer: `TestToJsonRecordPreservesFieldOrderRepro` in
  `testing/go/json_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestToJsonRecordPreservesFieldOrderRepro -count=1`.
- Expected PostgreSQL behavior: `to_json(r)::text` preserves record field order
  just like `row_to_json(r)::text`, while `to_jsonb(r)::text` canonicalizes the
  object key order.
- Observed Doltgres behavior: `to_json(r)::text` returns `{"a":2,"b":1}` for a
  record whose fields are ordered as `b, a`, so plain JSON record conversion is
  being sorted like JSONB.

### JSONB comparison uses the wrong array/null type precedence

- Reproducer: `TestJsonbComparisonTypePrecedenceMatchesPostgresRepro` in
  `testing/go/json_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestJsonbComparisonTypePrecedenceMatchesPostgresRepro -count=1`.
- Expected PostgreSQL behavior: JSONB cross-type comparison orders arrays before
  JSON null, then strings, numbers, booleans, and objects; for example
  `'[]'::jsonb < 'null'::jsonb` returns true.
- Observed Doltgres behavior: `'[]'::jsonb < 'null'::jsonb` returns false,
  indicating the internal JSONB comparator uses a different type precedence and
  can misorder JSONB values in comparisons and ordering operations.

### Whole-row references reject duplicate field names as ambiguous

- Reproducer: `TestWholeRowReferenceAllowsDuplicateFieldNamesRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestWholeRowReferenceAllowsDuplicateFieldNamesRepro -count=1`.
- Expected PostgreSQL behavior: a whole-row reference to a subquery with
  duplicate output field names is allowed, so `r::text` returns `(1,2)` and
  `row_to_json(r)::text` preserves both duplicate `a` fields.
- Observed Doltgres behavior: preparing the same whole-row reference fails with
  `ambiguous column name "a", it's present in all these tables: [r r]`, so
  duplicate field names prevent valid whole-row record access.

### JSONB existence operators compare escaped strings without decoding

- Reproducer: `TestJsonbExistsDecodesEscapedStringElementsRepro` in
  `testing/go/json_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestJsonbExistsDecodesEscapedStringElementsRepro -count=1`.
- Expected PostgreSQL behavior: JSONB existence operators `?`, `?|`, and `?&`
  compare object keys, string array elements, and scalar string values against
  the decoded SQL text probe, so a JSON value containing `"a\nb"` matches
  `E'a\nb'`.
- Observed Doltgres behavior: the same probes all return false because the
  stored escaped JSON string representation is compared directly against the
  decoded SQL text value.

### jsonb_agg(DISTINCT jsonb) keeps duplicate values

- Reproducer: `TestJsonbAggDistinctDeduplicatesJsonbValuesRepro` in
  `testing/go/json_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestJsonbAggDistinctDeduplicatesJsonbValuesRepro -count=1`.
- Expected PostgreSQL behavior: `jsonb_agg(DISTINCT doc)` over duplicate
  `jsonb` values returns a single JSONB element.
- Observed Doltgres behavior: duplicate `jsonb` values both appear in the
  aggregate result.

### jsonb_set with an empty path returns the original document

- Reproducers: `TestJsonbSetEmptyPathReplacesWholeDocumentRepro`,
  `TestJsonbSetEmptyPathUpdateReplacesStoredDocumentRepro`, and
  `TestJsonbSetLaxEmptyPathReplacesWholeDocumentRepro` in
  `testing/go/json_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestJsonbSet(Lax)?EmptyPath(ReplacesWholeDocument|UpdateReplacesStoredDocument)Repro'
  -count=1`.
- Expected PostgreSQL behavior: `jsonb_set('{"a":1}'::jsonb, '{}',
  '2'::jsonb)` treats the empty path as the whole target document and returns
  `2`; `jsonb_set_lax` with a non-null replacement value follows the same
  semantics; and the same expression inside an `UPDATE` replaces the stored
  JSONB value.
- Observed Doltgres behavior: the function returns the original document,
  `{"a": 1}`, `jsonb_set_lax` does the same, and an `UPDATE` using that
  expression leaves the stored document unchanged, so empty-path JSONB
  replacement silently fails.

### JSONB subscripting reads and assignments are rejected

- Reproducer: `TestJsonbSubscriptUpdatePersistsNestedDocumentRepro` in
  `testing/go/json_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestJsonbSubscriptUpdatePersistsNestedDocumentRepro -count=1`.
- Expected PostgreSQL behavior: `doc['a']['b']` reads the nested JSONB value,
  and `UPDATE jsonb_subscript_update_items SET doc['a']['b'] = '2'::jsonb
  WHERE id = 1` rewrites the nested value and persists `{"a": {"b": 2},
  "keep": true}`.
- Observed Doltgres behavior: the read form fails with `ERROR: multi
  dimensional array subscripts are not yet supported (SQLSTATE XX000)`, and
  the `UPDATE` form fails during parsing with `ERROR: at or near "[": syntax
  error (SQLSTATE 42601)`. The stored document remains `{"a": {"b": 1},
  "keep": true}`, so JSONB subscripting cannot be used for nested document
  access or mutation.

### jsonb_set accepts invalid path targets instead of raising PostgreSQL path errors

- Reproducers: `TestJsonbSetRejectsScalarTargetRepro` and
  `TestJsonbSetArrayPathRequiresIntegerRepro` in
  `testing/go/json_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestJsonb(SetRejectsScalarTarget|SetArrayPathRequiresInteger)Repro'
  -count=1`.
- Expected PostgreSQL behavior: `jsonb_set('"a"'::jsonb, '{a}',
  '"b"'::jsonb)` fails with `cannot set path in scalar`, and
  `jsonb_set('{"a":[1,2,3]}'::jsonb, '{a,not_an_int}',
  '"new_value"'::jsonb)` fails because the second path element is not an
  integer array index.
- Observed Doltgres behavior: both statements succeed instead of raising an
  error, so invalid JSONB path updates can appear to have executed normally.

### jsonb_insert accepts scalar targets instead of raising a PostgreSQL path error

- Reproducer: `TestJsonbInsertRejectsScalarTargetRepro` in
  `testing/go/json_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestJsonbInsertRejectsScalarTargetRepro -count=1`.
- Expected PostgreSQL behavior: `jsonb_insert('"a"'::jsonb, '{a}',
  '"b"'::jsonb)` fails with `cannot set path in scalar`.
- Observed Doltgres behavior: the statement succeeds instead of raising an
  error, so an invalid JSONB insert path is silently accepted.

### jsonb_insert silently accepts existing object keys

- Reproducer: `TestJsonbInsertRejectsExistingObjectKeyRepro` in
  `testing/go/json_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestJsonbInsertRejectsExistingObjectKeyRepro -count=1`.
- Expected PostgreSQL behavior: `jsonb_insert('{"a":1}'::jsonb, '{a}',
  '2'::jsonb)` rejects the existing key with `cannot replace existing key` and
  hints that `jsonb_set` should be used for replacement.
- Observed Doltgres behavior: the same statement succeeds instead of raising an
  error, so an invalid insert into an existing object key is silently accepted.

### jsonb_path_match coerces non-boolean jsonpath results to false

- Reproducers: `TestJsonbPathMatchJsonNullReturnsSqlNullRepro` and
  `TestJsonbPathMatchRequiresSingleBooleanResultRepro` in
  `testing/go/json_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'TestJsonbPathMatch(JsonNullReturnsSqlNull|RequiresSingleBooleanResult)Repro'
  -count=1`.
- Expected PostgreSQL behavior: `jsonb_path_match('null'::jsonb, '$')`
  returns SQL `NULL`, and `jsonb_path_match('[true]'::jsonb, '$')` fails
  because the jsonpath result is not a single boolean.
- Observed Doltgres behavior: the JSON null result is returned as `false`, and
  the array result succeeds instead of reporting `single boolean result is
  expected`.

### jsonb_path_query_array rejects supported jsonpath filter predicates

- Reproducer: `TestJsonbPathQueryArrayFilterPredicateRepro` in
  `testing/go/json_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestJsonbPathQueryArrayFilterPredicateRepro -count=1`.
- Expected PostgreSQL behavior: `jsonb_path_query_array('[{"a": 1}, {"a":
  2}]'::jsonb, '$[*].a ? (@ > 1)')` applies the filter predicate and returns
  `[2]`.
- Observed Doltgres behavior: the accepted PostgreSQL jsonpath expression fails
  with `unsupported jsonpath syntax near "? (@ > 1)"`.

### Quoted VALUES aliases that differ only by case resolve to the wrong aggregate columns

- Reproducer: `TestValuesQuotedCaseDistinctAggregateColumnsRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestValuesQuotedCaseDistinctAggregateColumnsRepro -count=1`.
- Expected PostgreSQL behavior: quoted aliases `"Val"` and `"val"` remain
  distinct, so `SUM("Val")` aggregates `1 + 2.5` and `SUM("val")` aggregates
  `10 + 20`.
- Observed Doltgres behavior: the query returns `3.5` for both aggregates,
  showing that the lowercase quoted alias resolves to the mixed-case column
  instead of the distinct lowercase column.

### FETCH FIRST WITH TIES is rejected

- Reproducer: `TestFetchFirstWithTiesIncludesPeerRowsRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestFetchFirstWithTiesIncludesPeerRowsRepro -count=1`.
- Expected PostgreSQL behavior: `ORDER BY score FETCH FIRST 2 ROWS WITH TIES`
  returns the first two ordered rows plus any later rows tied with the last row
  in that limited prefix.
- Observed Doltgres behavior: the valid query fails during parsing with `at or
  near "with": syntax error`, so peer-preserving top-N queries cannot be used.

### WITH HOLD cursor state cannot persist across COMMIT

- Reproducer: `TestCursorWithHoldSurvivesCommitRepro` in
  `testing/go/cursor_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCursorWithHoldSurvivesCommitRepro -count=1`.
- Expected PostgreSQL behavior: `DECLARE cursor CURSOR WITH HOLD FOR ...`
  creates a holdable cursor whose materialized result survives `COMMIT`; after
  fetching the first row before commit, a later `FETCH NEXT` after commit
  returns the second row.
- Observed Doltgres behavior: the holdable cursor declaration fails with `at
  or near "declare": syntax error: unimplemented: this syntax`, and later
  `FETCH`/`CLOSE` statements fail as syntax errors, so SQL cursor state cannot
  persist across transaction boundaries.

### TABLESAMPLE SYSTEM is rejected

- Reproducer: `TestTableSampleSystemHundredReturnsAllRowsRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTableSampleSystemHundredReturnsAllRowsRepro -count=1`.
- Expected PostgreSQL behavior: `FROM table TABLESAMPLE SYSTEM (100)` is a
  valid sampled table reference, and a 100 percent sample returns all rows.
- Observed Doltgres behavior: the query fails during parsing with `at or near
  "system": syntax error`, so SQL-standard sampled table references cannot be
  used.

### SELECT cannot project `tableoid`

- Reproducer: `TestSelectCanProjectTableoidRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSelectCanProjectTableoidRepro -count=1`.
- Expected PostgreSQL behavior: ordinary table scans expose `tableoid` as a
  system column, and `tableoid::regclass::text` identifies the relation that
  supplied each row.
- Observed Doltgres behavior: planning fails with `column "tableoid" could not
  be found in any table in scope`, so queries cannot project this PostgreSQL
  system column.

### ROWS FROM cannot combine multiple set-returning functions

- Reproducer: `TestRowsFromMultipleSetReturningFunctionsRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRowsFromMultipleSetReturningFunctionsRepro -count=1`.
- Expected PostgreSQL behavior: `ROWS FROM (generate_series(...),
  unnest(...))` zips the set-returning functions and pads shorter result sets
  with `NULL`.
- Observed Doltgres behavior: the query fails during planning with
  `unsupported syntax: values row(generate_series(...)), row(unnest(...))`, so
  valid multi-SRF table functions cannot be used.

### Multi-array `unnest` is rejected instead of null-padding shorter inputs

- Reproducer: `TestUnnestMultipleArraysPadsShorterInputsRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestUnnestMultipleArraysPadsShorterInputsRepro -count=1`.
- Expected PostgreSQL behavior: `unnest(ARRAY[10,20],
  ARRAY['foo','bar','baz'])` zips the arrays in `FROM` and pads the shorter
  integer array with `NULL` for the final row.
- Observed Doltgres behavior: planning fails with `unsupported syntax: values
  row(unnest(ARRAY[], ARRAY[]))`, so valid multi-array `unnest` table
  functions cannot be used.

### `generate_series` rejects `WITH ORDINALITY`

- Reproducer: `TestGenerateSeriesWithOrdinalityRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestGenerateSeriesWithOrdinalityRepro -count=1`.
- Expected PostgreSQL behavior: table functions such as
  `generate_series(4, 8, 2) WITH ORDINALITY` append a one-based ordinality
  column to the generated rows.
- Observed Doltgres behavior: the query fails with `WITH ORDINALITY is only
  supported for unnest`, so valid ordinality queries over `generate_series`
  cannot run.

### Window frame EXCLUDE CURRENT ROW is ignored

- Reproducer: `TestWindowFrameExcludeCurrentRowRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestWindowFrameExcludeCurrentRowRepro -count=1`.
- Expected PostgreSQL behavior: `EXCLUDE CURRENT ROW` removes the current row
  from the active window frame, so a full-frame sum over values `10, 20, 30`
  returns `50`, `40`, and `30` for the three rows.
- Observed Doltgres behavior: the clause is accepted but ignored; every row
  receives the unexcluded full-frame sum `60`.

### GROUPS window frames are rejected

- Reproducer: `TestWindowFrameGroupsRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestWindowFrameGroupsRepro -count=1`.
- Expected PostgreSQL behavior: `GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW`
  is a valid window frame mode and computes frames by peer groups.
- Observed Doltgres behavior: the query fails with `GROUPS is not yet
  supported`, so valid PostgreSQL window-frame queries cannot run.

### Window frame EXCLUDE GROUP is ignored

- Reproducer: `TestWindowFrameExcludeGroupRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestWindowFrameExcludeGroupRepro -count=1`.
- Expected PostgreSQL behavior: `EXCLUDE GROUP` removes the current row and
  its ordering peers from the active window frame.
- Observed Doltgres behavior: the clause is accepted but ignored; a full-frame
  sum over `1, 2, 3, 4` returns `10` for every row instead of excluding the
  current peer group.

### Window frame EXCLUDE TIES is ignored

- Reproducer: `TestWindowFrameExcludeTiesRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestWindowFrameExcludeTiesRepro -count=1`.
- Expected PostgreSQL behavior: `EXCLUDE TIES` removes ordering peers from the
  active window frame while keeping the current row.
- Observed Doltgres behavior: the clause is accepted but ignored; rows with a
  tied ordering value receive the unexcluded full-frame sum instead of sums
  that exclude the other peer rows.

### RANGE offset window frames are rejected

- Reproducer: `TestWindowFrameRangeOffsetRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestWindowFrameRangeOffsetRepro -count=1`.
- Expected PostgreSQL behavior: `RANGE BETWEEN 1 PRECEDING AND CURRENT ROW`
  is valid with a single numeric `ORDER BY` expression and computes a frame by
  value distance rather than row count.
- Observed Doltgres behavior: the query fails with `a range's order by must be
  one expression; found: 2` even though the window has one `ORDER BY`
  expression, so valid PostgreSQL range-offset frames cannot run.

### `cume_dist` window function is missing

- Reproducer: `TestCumeDistWindowFunctionRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCumeDistWindowFunctionRepro -count=1`.
- Expected PostgreSQL behavior: `cume_dist() OVER (ORDER BY score)` computes
  cumulative distribution values for each ordered row, including shared values
  for peer rows.
- Observed Doltgres behavior: planning fails with `function: 'cume_dist' not
  found`, so standard cumulative-distribution reporting queries cannot run.

### `nth_value` window function is missing

- Reproducer: `TestNthValueWindowFunctionRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestNthValueWindowFunctionRepro -count=1`.
- Expected PostgreSQL behavior: `nth_value(v, 2) OVER (...)` returns the
  second value in the active window frame.
- Observed Doltgres behavior: planning fails with `function: 'nth_value' not
  found`, so valid PostgreSQL nth-value window queries cannot run.

### Filtered window SUM returns zero for an empty filtered frame

- Reproducer: `TestWindowAggregateFilterReturnsNullForEmptyFrameRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestWindowAggregateFilterReturnsNullForEmptyFrameRepro -count=1`.
- Expected PostgreSQL behavior: `sum(v) FILTER (WHERE paid) OVER (...)`
  returns `NULL` when no row in the active frame satisfies the filter, matching
  ordinary aggregate semantics.
- Observed Doltgres behavior: the filtered window sum returns `0` for an empty
  filtered frame, which can silently change reports that distinguish no input
  from a numeric zero.

### Filtered window AVG errors instead of returning NULL for an empty filtered frame

- Reproducer: `TestAvgWindowAggregateFilterReturnsNullForEmptyFrameRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAvgWindowAggregateFilterReturnsNullForEmptyFrameRepro -count=1`.
- Expected PostgreSQL behavior: `avg(v) FILTER (WHERE paid) OVER (...)`
  returns `NULL` when no row in the active frame satisfies the filter, matching
  ordinary aggregate semantics.
- Observed Doltgres behavior: the filtered window average fails with
  `value NaN is not a valid Decimal`, so reports using filtered moving averages
  can error when a partition has no matching inputs yet.

### Boolean aggregates are rejected as window functions

- Reproducer: `TestBooleanAggregatesCanBeWindowFunctionsRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestBooleanAggregatesCanBeWindowFunctionsRepro -count=1`.
- Expected PostgreSQL behavior: `bool_or(paid) OVER (...)` and
  `bool_and(paid) OVER (...)` compute running boolean aggregates over each
  window frame.
- Observed Doltgres behavior: planning leaves the aggregate outside a window
  node and fails with `an aggregation remained in the expression`, so valid
  boolean window aggregate queries cannot run.

### `array_agg` is rejected as a window function

- Reproducer: `TestArrayAggCanBeWindowFunctionRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestArrayAggCanBeWindowFunctionRepro -count=1`.
- Expected PostgreSQL behavior: `array_agg(label) OVER (...)` computes the
  array of labels in the active window frame without requiring `GROUP BY`.
- Observed Doltgres behavior: planning treats the expression as a grouped
  aggregate and fails with `in aggregated query without GROUP BY`, so valid
  array-valued window aggregate queries cannot run.

### `percentile_cont` ordered-set aggregate is rejected

- Reproducer: `TestPercentileContWithinGroupRepro` in
  `testing/go/aggregate_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPercentileContWithinGroupRepro -count=1`.
- Expected PostgreSQL behavior: `percentile_cont(0.5) WITHIN GROUP (ORDER BY
  v)` computes an interpolated percentile; for values `10, 20, 30, 40`, the
  median is `25`.
- Observed Doltgres behavior: the valid ordered-set aggregate fails with
  `WITHIN GROUP is not yet supported`, so continuous-percentile reports cannot
  run.

### `percentile_disc` ordered-set aggregate is rejected

- Reproducer: `TestPercentileDiscWithinGroupRepro` in
  `testing/go/aggregate_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPercentileDiscWithinGroupRepro -count=1`.
- Expected PostgreSQL behavior: `percentile_disc(0.5) WITHIN GROUP (ORDER BY
  v)` chooses the first discrete value whose cumulative distribution reaches
  the requested percentile; for values `10, 20, 30, 40`, the result is `20`.
- Observed Doltgres behavior: the valid ordered-set aggregate fails with
  `WITHIN GROUP is not yet supported`, so discrete-percentile reports cannot
  run.

### `mode` ordered-set aggregate is rejected

- Reproducer: `TestModeWithinGroupRepro` in
  `testing/go/aggregate_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestModeWithinGroupRepro -count=1`.
- Expected PostgreSQL behavior: `mode() WITHIN GROUP (ORDER BY v)` returns the
  most frequent ordered input value.
- Observed Doltgres behavior: the valid ordered-set aggregate fails with
  `WITHIN GROUP is not yet supported`, so standard mode calculations cannot
  run.

### Hypothetical-set `rank` aggregate is rejected

- Reproducer: `TestHypotheticalRankWithinGroupRepro` in
  `testing/go/aggregate_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestHypotheticalRankWithinGroupRepro -count=1`.
- Expected PostgreSQL behavior: `rank(25) WITHIN GROUP (ORDER BY v)` computes
  the rank a hypothetical value would have in the ordered aggregate input; for
  values `10, 20, 30, 40`, the result is `3`.
- Observed Doltgres behavior: the valid hypothetical-set aggregate fails with
  `WITHIN GROUP is not yet supported`, so hypothetical rank calculations cannot
  run.

### `lag` and `lead` reject constant offsets and defaults

- Reproducer: `TestLagLeadConstantOffsetAndDefaultRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLagLeadConstantOffsetAndDefaultRepro -count=1`.
- Expected PostgreSQL behavior: `lag(v, 2, 0)` and `lead(v, 2, 99)` use the
  requested two-row offset and return the supplied default when the target row
  is outside the partition.
- Observed Doltgres behavior: the query fails during planning with `offset
  must be a non-negative integer; found: 2 as ...`, so valid offset/default
  variants of `lag` and `lead` cannot run.

### `lag` and `lead` reject dynamic row offsets

- Reproducer: `TestLagLeadDynamicOffsetRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLagLeadDynamicOffsetRepro -count=1`.
- Expected PostgreSQL behavior: the offset argument to `lag` and `lead` is
  evaluated from the current row, so a column expression can determine how far
  backward or forward to read.
- Observed Doltgres behavior: the query fails during planning with `offset
  must be a non-negative integer; found: lag_lead_dynamic_items.off`, so
  row-dependent offset queries cannot run.

### `ntile` rejects dynamic bucket-count expressions

- Reproducer: `TestNtileDynamicBucketCountRepro` in
  `testing/go/query_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestNtileDynamicBucketCountRepro -count=1`.
- Expected PostgreSQL behavior: `ntile(buckets) OVER (ORDER BY id)` accepts a
  bucket-count expression evaluated from the current row.
- Observed Doltgres behavior: the query fails with `unable to find field with
  index 2 in row of 0 columns`, so valid row-expression bucket counts cannot
  run.

## Persistence

### COMMENT ON COLUMN does not persist descriptions

- Reproducer: `TestCommentOnColumnPersistsDescriptionRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCommentOnColumnPersistsDescriptionRepro -count=1`.
- Expected PostgreSQL behavior: after `COMMENT ON COLUMN
  comment_target.label IS 'visible label comment'`, `col_description` returns
  that comment for the column.
- Observed Doltgres behavior: `COMMENT ON COLUMN` is accepted, but
  `col_description('comment_target'::regclass, 2)` returns `NULL`.

### COMMENT ON TABLE does not persist descriptions

- Reproducer: `TestCommentOnTablePersistsDescriptionRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCommentOnTablePersistsDescriptionRepro -count=1`.
- Expected PostgreSQL behavior: after `COMMENT ON TABLE table_comment_target IS
  'visible table comment'`, `obj_description` returns that comment for the
  relation.
- Observed Doltgres behavior: `COMMENT ON TABLE` is accepted, but
  `obj_description('table_comment_target'::regclass)` returns `NULL`.

### COMMENT ON view, materialized view, and sequence does not persist descriptions

- Reproducer: `TestCommentOnRelationKindsPersistsDescriptionRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCommentOnRelationKindsPersistsDescriptionRepro -count=1`.
- Expected PostgreSQL behavior: accepted `COMMENT ON VIEW`, `COMMENT ON
  MATERIALIZED VIEW`, and `COMMENT ON SEQUENCE` statements persist relation
  descriptions visible through `obj_description`.
- Observed Doltgres behavior: all three statements are accepted, but
  `obj_description` returns `NULL` for the view, materialized view, and
  sequence.

### COMMENT ON index, constraint, and trigger does not persist descriptions

- Reproducer: `TestCommentOnIndexConstraintTriggerPersistsDescriptionRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCommentOnIndexConstraintTriggerPersistsDescriptionRepro -count=1`.
- Expected PostgreSQL behavior: accepted `COMMENT ON INDEX`, `COMMENT ON
  CONSTRAINT`, and `COMMENT ON TRIGGER` statements persist descriptions visible
  through `obj_description`.
- Observed Doltgres behavior: all three statements are accepted, but
  `obj_description` returns `NULL` for the index, constraint, and trigger.

### COMMENT ON non-table objects does not persist descriptions

- Reproducers: `TestCommentOnSchemaPersistsDescriptionRepro`,
  `TestCommentOnDatabasePersistsDescriptionRepro`,
  `TestCommentOnFunctionPersistsDescriptionRepro`, and
  `TestCommentOnTypePersistsDescriptionRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'TestCommentOn(Schema|Database|Function|Type)PersistsDescriptionRepro'
  -count=1`.
- Expected PostgreSQL behavior: accepted `COMMENT ON SCHEMA`, `COMMENT ON
  DATABASE`, `COMMENT ON FUNCTION`, and `COMMENT ON TYPE` statements persist
  descriptions visible through `obj_description` or `shobj_description`.
- Observed Doltgres behavior: all four statements are accepted, but the
  corresponding description function returns `NULL`.

### COMMENT ON role and extension does not persist descriptions

- Reproducer: `TestCommentOnRoleAndExtensionPersistsDescriptionRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCommentOnRoleAndExtensionPersistsDescriptionRepro -count=1`.
- Expected PostgreSQL behavior: accepted `COMMENT ON ROLE` and `COMMENT ON
  EXTENSION` statements persist descriptions visible through
  `shobj_description` and `obj_description`.
- Observed Doltgres behavior: both statements are accepted, but the
  corresponding description functions return `NULL`.

### COMMENT ON procedure, routine, domain, and language does not persist descriptions

- Reproducer:
  `TestCommentOnProcedureRoutineDomainLanguagePersistsDescriptionRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCommentOnProcedureRoutineDomainLanguagePersistsDescriptionRepro
  -count=1`.
- Expected PostgreSQL behavior: accepted `COMMENT ON PROCEDURE`, `COMMENT ON
  ROUTINE`, `COMMENT ON DOMAIN`, and `COMMENT ON LANGUAGE` statements persist
  descriptions visible through `obj_description`.
- Observed Doltgres behavior: all four statements are accepted, but the
  corresponding description functions return `NULL`.

### COMMENT ON collation and operator does not persist descriptions

- Reproducer: `TestCommentOnCollationAndOperatorPersistsDescriptionRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCommentOnCollationAndOperatorPersistsDescriptionRepro -count=1`.
- Expected PostgreSQL behavior: accepted `COMMENT ON COLLATION` and `COMMENT ON
  OPERATOR` statements persist descriptions visible through `obj_description`.
- Observed Doltgres behavior: both statements are accepted, but the
  corresponding description functions return `NULL`.

### COMMENT ON access method does not persist descriptions

- Reproducer: `TestCommentOnAccessMethodPersistsDescriptionRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCommentOnAccessMethodPersistsDescriptionRepro -count=1`.
- Expected PostgreSQL behavior: accepted `COMMENT ON ACCESS METHOD btree`
  persists a description visible through `obj_description(..., 'pg_am')`.
- Observed Doltgres behavior: the statement is accepted, but
  `obj_description(..., 'pg_am')` returns `NULL`.

### COMMENT ON publication does not persist descriptions

- Reproducer: `TestCommentOnPublicationPersistsDescriptionRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCommentOnPublicationPersistsDescriptionRepro -count=1`.
- Expected PostgreSQL behavior: accepted `COMMENT ON PUBLICATION
  comment_publication_target` persists a description visible through
  `obj_description(..., 'pg_publication')`.
- Observed Doltgres behavior: the statement is accepted, but
  `obj_description(..., 'pg_publication')` returns `NULL`.

### COMMENT ON subscription does not persist descriptions

- Reproducer: `TestCommentOnSubscriptionPersistsDescriptionRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCommentOnSubscriptionPersistsDescriptionRepro -count=1`.
- Expected PostgreSQL behavior: accepted `COMMENT ON SUBSCRIPTION
  comment_subscription_target` persists a description visible through
  `obj_description(..., 'pg_subscription')`.
- Observed Doltgres behavior: the statement is accepted, but
  `obj_description(..., 'pg_subscription')` returns `NULL`.

### COMMENT ON text-search objects does not persist descriptions

- Reproducer: `TestCommentOnTextSearchObjectsPersistsDescriptionRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCommentOnTextSearchObjectsPersistsDescriptionRepro -count=1`.
- Expected PostgreSQL behavior: accepted `COMMENT ON TEXT SEARCH
  CONFIGURATION`, `COMMENT ON TEXT SEARCH DICTIONARY`, `COMMENT ON TEXT SEARCH
  PARSER`, and `COMMENT ON TEXT SEARCH TEMPLATE` statements persist
  descriptions visible through `obj_description`.
- Observed Doltgres behavior: all four statements are accepted, but the
  corresponding description functions return `NULL`.

### COMMENT ON missing targets is accepted

- Reproducer: `TestCommentOnMissingTargetsRequiresExistingObjectRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCommentOnMissingTargetsRequiresExistingObjectRepro -count=1`.
- Expected PostgreSQL behavior: `COMMENT ON TABLE`, `COMMENT ON COLUMN`,
  `COMMENT ON FUNCTION`, `COMMENT ON ROLE`, `COMMENT ON EXTENSION`, `COMMENT
  ON PROCEDURE`, `COMMENT ON ROUTINE`, `COMMENT ON DOMAIN`, and `COMMENT ON
  LANGUAGE`, `COMMENT ON COLLATION`, `COMMENT ON OPERATOR`, and text-search
  `COMMENT ON` forms validate that the target exists and fail for missing
  objects.
- Observed Doltgres behavior: comments on a missing table, a column of a
  missing table, a missing function, a missing role, a missing extension, a
  missing procedure, a missing routine, a missing domain, a missing language, a
  missing collation, a missing operator, and missing text-search objects all
  succeed, because `COMMENT ON` is accepted before resolving the target.

### COMMENT ON missing access method is accepted

- Reproducer: `TestCommentOnMissingAccessMethodRequiresExistingObjectRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCommentOnMissingAccessMethodRequiresExistingObjectRepro -count=1`.
- Expected PostgreSQL behavior: `COMMENT ON ACCESS METHOD missing_comment_am`
  validates that the access method exists and fails for a missing target.
- Observed Doltgres behavior: the statement succeeds without resolving the
  missing access method.

### COMMENT ON missing publication is accepted

- Reproducer: `TestCommentOnMissingPublicationRequiresExistingObjectRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCommentOnMissingPublicationRequiresExistingObjectRepro -count=1`.
- Expected PostgreSQL behavior: `COMMENT ON PUBLICATION
  missing_comment_publication` validates that the publication exists and fails
  for a missing target.
- Observed Doltgres behavior: the statement succeeds without resolving the
  missing publication.

### COMMENT ON missing subscription is accepted

- Reproducer: `TestCommentOnMissingSubscriptionRequiresExistingObjectRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCommentOnMissingSubscriptionRequiresExistingObjectRepro -count=1`.
- Expected PostgreSQL behavior: `COMMENT ON SUBSCRIPTION
  missing_comment_subscription` validates that the subscription exists and
  fails for a missing target.
- Observed Doltgres behavior: the statement succeeds without resolving the
  missing subscription.

### COMMENT ON missing policy is accepted

- Reproducer: `TestCommentOnMissingPolicyRequiresExistingObjectRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCommentOnMissingPolicyRequiresExistingObjectRepro -count=1`.
- Expected PostgreSQL behavior: `COMMENT ON POLICY missing_comment_policy ON
  comment_policy_target` validates that the policy exists on the target table
  and fails for a missing policy.
- Observed Doltgres behavior: the statement succeeds without resolving the
  missing policy.

### COMMENT ON missing large object is accepted

- Reproducer: `TestCommentOnMissingLargeObjectRequiresExistingObjectRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCommentOnMissingLargeObjectRequiresExistingObjectRepro -count=1`.
- Expected PostgreSQL behavior: `COMMENT ON LARGE OBJECT 987654321` validates
  that the large object exists and fails for a missing object.
- Observed Doltgres behavior: the statement succeeds without resolving the
  missing large object.

### COMMENT ON missing tablespace is accepted

- Reproducer: `TestCommentOnMissingTablespaceRequiresExistingObjectRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCommentOnMissingTablespaceRequiresExistingObjectRepro -count=1`.
- Expected PostgreSQL behavior: `COMMENT ON TABLESPACE
  missing_comment_tablespace` validates that the tablespace exists and fails
  for a missing target.
- Observed Doltgres behavior: the statement succeeds without resolving the
  missing tablespace.

### COMMENT ON TABLE does not require table ownership

- Reproducer: `TestCommentOnTableRequiresOwnershipRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCommentOnTableRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: commenting on a table requires ownership of the
  table.
- Observed Doltgres behavior: a role with only schema `USAGE` can run `COMMENT
  ON TABLE comment_private IS 'unauthorized comment'`.

### COMMENT ON COLUMN does not require table ownership

- Reproducer: `TestCommentOnColumnRequiresOwnershipRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCommentOnColumnRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: commenting on a column requires ownership of
  the table containing that column.
- Observed Doltgres behavior: a role with only schema `USAGE` can run `COMMENT
  ON COLUMN comment_column_private.secret IS 'unauthorized column comment'`.

### COMMENT ON view, materialized view, and sequence does not require ownership

- Reproducer: `TestCommentOnRelationKindsRequiresOwnershipRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCommentOnRelationKindsRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: commenting on a view, materialized view, or
  sequence requires ownership of the target relation.
- Observed Doltgres behavior: a role with only schema `USAGE` can run `COMMENT
  ON VIEW`, `COMMENT ON MATERIALIZED VIEW`, and `COMMENT ON SEQUENCE` against
  relations owned by another role.

### COMMENT ON index, constraint, and trigger does not require ownership

- Reproducer: `TestCommentOnIndexConstraintTriggerRequiresOwnershipRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCommentOnIndexConstraintTriggerRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: commenting on an index, constraint, or trigger
  requires ownership of the underlying relation.
- Observed Doltgres behavior: a role with only schema `USAGE` can run `COMMENT
  ON INDEX`, `COMMENT ON CONSTRAINT`, and `COMMENT ON TRIGGER` against objects
  attached to another role's table.

### COMMENT ON non-table objects does not require ownership

- Reproducers: `TestCommentOnSchemaRequiresOwnershipRepro`,
  `TestCommentOnDatabaseRequiresOwnershipRepro`,
  `TestCommentOnFunctionRequiresOwnershipRepro`, and
  `TestCommentOnTypeRequiresOwnershipRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'TestCommentOn(Schema|Database|Function|Type)RequiresOwnershipRepro'
  -count=1`.
- Expected PostgreSQL behavior: commenting on schemas, databases, functions,
  and types requires ownership of the target object.
- Observed Doltgres behavior: ordinary roles can run `COMMENT ON` for a schema,
  database, function, or type they do not own.

### COMMENT ON role and extension does not require privileges

- Reproducer: `TestCommentOnRoleAndExtensionRequiresPrivilegeRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCommentOnRoleAndExtensionRequiresPrivilegeRepro -count=1`.
- Expected PostgreSQL behavior: commenting on a role requires the `CREATEROLE`
  attribute, and commenting on an extension requires ownership of that
  extension.
- Observed Doltgres behavior: ordinary login roles can run `COMMENT ON ROLE`
  and `COMMENT ON EXTENSION` without those privileges.

### COMMENT ON procedure, routine, domain, and language does not require ownership

- Reproducer:
  `TestCommentOnProcedureRoutineDomainLanguageRequiresOwnershipRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCommentOnProcedureRoutineDomainLanguageRequiresOwnershipRepro
  -count=1`.
- Expected PostgreSQL behavior: commenting on procedures, routines, domains,
  and procedural languages requires ownership of the target object.
- Observed Doltgres behavior: an ordinary role with only schema `USAGE` can run
  `COMMENT ON PROCEDURE`, `COMMENT ON ROUTINE`, `COMMENT ON DOMAIN`, and
  `COMMENT ON LANGUAGE` against objects it does not own.

### COMMENT ON collation and operator does not require ownership

- Reproducer: `TestCommentOnCollationAndOperatorRequiresOwnershipRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCommentOnCollationAndOperatorRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: commenting on a collation or operator requires
  ownership of the target object.
- Observed Doltgres behavior: an ordinary role can run `COMMENT ON COLLATION`
  and `COMMENT ON OPERATOR` against `pg_catalog` objects it does not own.

### COMMENT ON text-search objects does not require ownership

- Reproducer: `TestCommentOnTextSearchObjectsRequiresOwnershipRepro` in
  `testing/go/comment_persistence_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCommentOnTextSearchObjectsRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: commenting on text-search configurations and
  dictionaries requires ownership, while commenting on text-search parsers and
  templates requires superuser privileges.
- Observed Doltgres behavior: an ordinary role can run `COMMENT ON TEXT SEARCH
  CONFIGURATION`, `COMMENT ON TEXT SEARCH DICTIONARY`, `COMMENT ON TEXT SEARCH
  PARSER`, and `COMMENT ON TEXT SEARCH TEMPLATE` against `pg_catalog` objects
  without those privileges.

### CREATE TYPE AS ENUM does not populate pg_enum

- Reproducer: `TestCreateEnumPopulatesPgEnumRepro` in
  `testing/go/enum_metadata_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateEnumPopulatesPgEnumRepro -count=1`.
- Expected PostgreSQL behavior: after `CREATE TYPE enum_catalog_target AS ENUM
  ('sad', 'ok', 'happy')`, `pg_catalog.pg_enum` exposes one row per enum label
  with the corresponding sort order.
- Observed Doltgres behavior: the enum type is usable, but `pg_enum` returns no
  rows for the created type.

### ALTER TYPE ADD VALUE is rejected for enum types

- Reproducer: `TestAlterEnumAddValuePersistsUsableLabelRepro` in
  `testing/go/enum_metadata_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestAlterEnumAddValuePersistsUsableLabelRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TYPE enum_alter_status ADD VALUE
  'archived' AFTER 'new'` persists the new enum label and accepts it in future
  enum-typed rows.
- Observed Doltgres behavior: the alter statement fails with `ERROR: ALTER
  TYPE is not yet supported Please file an issue at
  https://github.com/dolthub/doltgresql/issues (SQLSTATE 0A000)`, so persisted
  enum types cannot be evolved with new values.

### ALTER TYPE RENAME VALUE is rejected for enum types

- Reproducer: `TestAlterEnumRenameValueRepro` in
  `testing/go/alter_type_rename_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterEnumRenameValueRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TYPE enum RENAME VALUE old TO new`
  updates the persisted enum label without recreating the type.
- Observed Doltgres behavior: the alter statement fails with `ERROR: ALTER
  TYPE is not yet supported Please file an issue at
  https://github.com/dolthub/doltgresql/issues (SQLSTATE 0A000)`, so enum
  labels cannot be renamed in place.

### ALTER TYPE and DOMAIN RENAME TO are rejected

- Reproducer: `TestAlterTypeAndDomainRenameToRepro` in
  `testing/go/alter_type_rename_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterTypeAndDomainRenameToRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TYPE rename_enum_object RENAME TO
  renamed_enum_object` and `ALTER DOMAIN rename_domain_object RENAME TO
  renamed_domain_object` rename the persisted type objects in place; the old
  names stop resolving through `to_regtype`, and the new names resolve.
- Observed Doltgres behavior: `ALTER TYPE ... RENAME TO` fails with `ERROR:
  ALTER TYPE is not yet supported Please file an issue at
  https://github.com/dolthub/doltgresql/issues (SQLSTATE 0A000)`, and `ALTER
  DOMAIN ... RENAME TO` fails with `ERROR: ALTER DOMAIN is not yet supported
  Please file an issue at https://github.com/dolthub/doltgresql/issues
  (SQLSTATE 0A000)`, so persisted type and domain objects cannot be renamed.

### ORDER BY on enum columns uses label text instead of declaration order

- Reproducer: `TestEnumOrderingUsesDeclarationOrderRepro` in
  `testing/go/enum_metadata_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestEnumOrderingUsesDeclarationOrderRepro -count=1 -v`.
- Expected PostgreSQL behavior: enum values sort by their declaration order.
  For `CREATE TYPE enum_declared_order AS ENUM ('beta', 'alpha', 'gamma')`,
  `ORDER BY status` returns `beta`, then `alpha`, then `gamma`.
- Observed Doltgres behavior: direct enum comparisons report declaration-order
  results, but `ORDER BY status` returns rows in label-text order:
  `alpha`, `beta`, `gamma`. Queries that rely on enum sort order can produce
  incorrectly ordered result sets.

### Arrays over enum element types panic during table creation

- Reproducer: `TestEnumArrayColumnRoundTripsValuesRepro` in
  `testing/go/enum_metadata_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestEnumArrayColumnRoundTripsValuesRepro -count=1`.
- Expected PostgreSQL behavior: a column declared as an array of an enum type,
  such as `enum_array_mood[]`, can be created, can store arrays of enum values,
  and supports ordinary array subscripting and membership predicates.
- Observed Doltgres behavior: `CREATE TABLE enum_array_items (..., moods
  enum_array_mood[])` panics during AST conversion with a nil-pointer
  dereference in `server/ast.nodeColumnTableDef`, so arrays over enum element
  types cannot be stored or queried.

### Composite types are missing pg_class metadata

- Reproducer: `TestCompositeTypeCatalogRelidRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCompositeTypeCatalogRelidRepro -count=1`.
- Expected PostgreSQL behavior: after `CREATE TYPE composite_catalog_type AS
  (id integer, label text)`, `pg_type.typrelid` points at a companion
  `pg_class` row with `relkind = 'c'`.
- Observed Doltgres behavior: the composite type exists, but
  `pg_type.typrelid` is zero and no matching `pg_class` row is exposed.

### Composite attribute typmods are not applied to stored values

- Reproducer: `TestCompositeAttributeTypmodsRoundStoredValuesRepro` in
  `testing/go/composite_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCompositeAttributeTypmodsRoundStoredValuesRepro -count=1`.
- Expected PostgreSQL behavior: attributes declared as `numeric(5,2)` and
  `timestamp(0)` inside a composite type are rounded when `ROW(...)` values are
  assigned to a column of that composite type, and numeric attribute values that
  overflow after rounding are rejected.
- Observed Doltgres behavior: stored composite attributes retain `123.456` and
  `2021-09-15 21:43:56.789`, and `999.995` is accepted instead of raising
  `numeric field overflow`, so composite attribute typmods are not enforced on
  assignment.

### TIMETZ composite attribute typmods are not applied to stored values

- Reproducer: `TestCompositeTimetzAttributeTypmodsRoundStoredValuesRepro` in
  `testing/go/composite_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCompositeTimetzAttributeTypmodsRoundStoredValuesRepro -count=1`.
- Expected PostgreSQL behavior: attributes declared as `TIMETZ(0)` inside a
  composite type are rounded when `ROW(...)` values are assigned to a column of
  that composite type, so `21:43:56.789+00` stores as `21:43:57+00`.
- Observed Doltgres behavior: the stored composite attribute retains
  `21:43:56.789+00`, so timetz composite attribute typmods are not enforced on
  assignment.

### Arrays over composite element types panic during table creation

- Reproducer: `TestCompositeArrayColumnRoundTripsValuesRepro` in
  `testing/go/composite_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCompositeArrayColumnRoundTripsValuesRepro -count=1`.
- Expected PostgreSQL behavior: a column declared as an array of a composite
  type, such as `composite_array_line[]`, can be created, can store arrays of
  `ROW(...)::composite_array_line` values, and can read fields through array
  subscripting.
- Observed Doltgres behavior: `CREATE TABLE composite_array_orders (..., lines
  composite_array_line[])` panics during AST conversion with a nil-pointer
  dereference in `server/ast.nodeColumnTableDef`, so arrays over composite
  element types cannot be stored or queried.

### ALTER TYPE cannot add or drop composite attributes

- Reproducers: `TestAlterCompositeTypeAddAttributeRepro` and
  `TestAlterCompositeTypeDropAttributeRepro` in
  `testing/go/composite_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestAlterCompositeType(Add|Drop)AttributeRepro' -count=1`.
- Expected PostgreSQL behavior: `ALTER TYPE composite ADD ATTRIBUTE ...`
  changes the composite row shape so values can include the new field, and
  `ALTER TYPE composite DROP ATTRIBUTE ...` removes the field from the row
  shape.
- Observed Doltgres behavior: both statements fail with `ALTER TYPE is not yet
  supported Please file an issue at https://github.com/dolthub/doltgresql/issues`,
  so composite types cannot be evolved after creation.

### CREATE SEQUENCE does not populate pg_sequences

- Reproducer: `TestCreateSequencePopulatesPgSequencesRepro` in
  `testing/go/sequence_metadata_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateSequencePopulatesPgSequencesRepro -count=1`.
- Expected PostgreSQL behavior: after `CREATE SEQUENCE
  sequence_catalog_target`, `pg_catalog.pg_sequences` exposes that sequence and
  its metadata.
- Observed Doltgres behavior: the sequence is usable, but `pg_sequences`
  returns no row for the created sequence.

### CREATE SEQUENCE does not populate pg_statio sequence views

- Reproducer: `TestCreateSequencePopulatesPgStatioSequenceViewsRepro` in
  `testing/go/sequence_metadata_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateSequencePopulatesPgStatioSequenceViewsRepro -count=1`.
- Expected PostgreSQL behavior: after `CREATE SEQUENCE
  statio_sequence_catalog_target`, `pg_catalog.pg_statio_user_sequences` and
  `pg_catalog.pg_statio_all_sequences` expose that sequence with numeric I/O
  statistic counters.
- Observed Doltgres behavior: the sequence is usable, but both
  `pg_statio_user_sequences` and `pg_statio_all_sequences` are empty stubs, so
  sequence I/O statistics consumers cannot see the created sequence.

### CREATE TEMPORARY SEQUENCE is rejected

- Reproducer: `TestCreateTemporarySequenceRepro` in
  `testing/go/sequence_metadata_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateTemporarySequenceRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE TEMPORARY SEQUENCE name` creates a
  session-local sequence, and `nextval('name')` can allocate values from it in
  that session.
- Observed Doltgres behavior: `CREATE TEMPORARY SEQUENCE
  temp_sequence_target` fails with `temporary sequences are not yet supported`,
  so workloads cannot use session-local sequences.

### Sequence relations do not expose `last_value` and `is_called`

- Reproducer: `TestSequenceRelationTracksIsCalledRepro` in
  `testing/go/sequence_metadata_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSequenceRelationTracksIsCalledRepro -count=1`.
- Expected PostgreSQL behavior: selecting from a sequence relation exposes
  persistent sequence state. Before the first `nextval`, `last_value` is the
  start value and `is_called` is false; after the first `nextval`, `is_called`
  is true.
- Observed Doltgres behavior: `SELECT last_value, is_called FROM
  sequence_is_called_target` fails with `table not found:
  sequence_is_called_target`, so clients cannot inspect sequence relation state
  or distinguish uncalled sequences from consumed ones.

### ALTER COLUMN SET STORAGE does not persist attstorage

- Reproducer: `TestAlterColumnSetStoragePersistsCatalogRepro` in
  `testing/go/column_storage_metadata_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAlterColumnSetStoragePersistsCatalogRepro -count=1`.
- Expected PostgreSQL behavior: after `ALTER TABLE storage_catalog_target ALTER
  COLUMN payload SET STORAGE EXTERNAL`, `pg_attribute.attstorage` for the
  column is `e`.
- Observed Doltgres behavior: the statement is accepted, but `attstorage`
  remains `p`.

### ALTER COLUMN SET COMPRESSION does not persist attcompression

- Reproducer: `TestAlterColumnSetCompressionPersistsCatalogRepro` in
  `testing/go/column_storage_metadata_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAlterColumnSetCompressionPersistsCatalogRepro -count=1`.
- Expected PostgreSQL behavior: after `ALTER TABLE compression_catalog_target
  ALTER COLUMN payload SET COMPRESSION pglz`, `pg_attribute.attcompression` for
  the column is `p`.
- Observed Doltgres behavior: the statement is accepted, but `attcompression`
  remains empty.

### ALTER COLUMN SET STATISTICS does not persist attstattarget

- Reproducer: `TestAlterColumnSetStatisticsPersistsCatalogRepro` in
  `testing/go/column_storage_metadata_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAlterColumnSetStatisticsPersistsCatalogRepro -count=1`.
- Expected PostgreSQL behavior: after `ALTER TABLE statistics_catalog_target
  ALTER COLUMN payload SET STATISTICS 42`, `pg_attribute.attstattarget` for the
  column is `42`.
- Observed Doltgres behavior: the statement is accepted, but `attstattarget`
  remains `-1`.

### ALTER COLUMN SET options are rejected before persisting attoptions

- Reproducer: `TestPgAttributeColumnOptionsMetadataRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgAttributeColumnOptionsMetadataRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... ALTER COLUMN category SET
  (n_distinct = 100, n_distinct_inherited = 200)` succeeds and persists those
  per-column planner options in `pg_catalog.pg_attribute.attoptions`.
- Observed Doltgres behavior: the valid `ALTER TABLE` form fails with
  `ALTER TABLE with unsupported command type *tree.AlterTableSetAttribution`,
  and `pg_attribute.attoptions` remains `NULL` for the column.

### ANALYZE does not populate pg_stats

- Reproducer: `TestAnalyzePopulatesPgStatsRepro` in
  `testing/go/statistics_catalog_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAnalyzePopulatesPgStatsRepro -count=1`.
- Expected PostgreSQL behavior: after `ANALYZE analyze_catalog_target`,
  `pg_catalog.pg_stats` exposes column statistics rows for analyzed columns,
  including `category`.
- Observed Doltgres behavior: `ANALYZE` succeeds and Dolt statistics may be
  populated, but `pg_stats` returns no row for the analyzed column.

### CREATE STATISTICS is rejected before pg_statistic_ext metadata is persisted

- Reproducer: `TestCreateStatisticsPopulatesPgStatisticExtRepro` in
  `testing/go/statistics_catalog_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateStatisticsPopulatesPgStatisticExtRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE STATISTICS name (dependencies) ON a, b
  FROM table` succeeds, `ANALYZE` can collect statistics, and
  `pg_catalog.pg_statistic_ext` exposes the extended-statistics definition.
- Observed Doltgres behavior: the valid `CREATE STATISTICS` statement fails
  with a syntax error near `(`, and `pg_statistic_ext` has no row for the
  statistics object.

### pg_get_expr default metadata omits PostgreSQL cast details

- Reproducer: `TestPgAttrdefDefaultExpressionsRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgAttrdefDefaultExpressionsRepro -count=1`.
- Expected PostgreSQL behavior: ordinary column defaults are exposed through
  `pg_catalog.pg_attrdef`, and `pg_get_expr(adbin, adrelid)` renders defaults
  with PostgreSQL's resolved expression text, such as `lower('ABC'::text)`.
- Observed Doltgres behavior: the `pg_attrdef` row exists, but
  `pg_get_expr` renders the same default as `lower('ABC')`, losing the
  PostgreSQL cast annotation that catalog consumers use when reconstructing
  definitions.

### Generated column expressions are missing from pg_attrdef

- Reproducer: `TestPgGetExprReturnsGeneratedColumnExpressionRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestPgGetExprReturnsGeneratedColumnExpressionRepro -count=1`.
- Expected PostgreSQL behavior: a stored generated column has a `pg_attrdef`
  row whose `adbin` can be rendered by `pg_get_expr(adbin, adrelid)`.
- Observed Doltgres behavior: `pg_attrdef` returns no row for the generated
  column expression, so catalog consumers cannot reconstruct the expression.

### Generated column expressions are missing from information_schema

- Reproducer: `TestInformationSchemaGeneratedColumnExpressionRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestInformationSchemaGeneratedColumnExpressionRepro -count=1`.
- Expected PostgreSQL behavior: `information_schema.columns` exposes a
  non-null `generation_expression` for stored generated columns.
- Observed Doltgres behavior: the generated column is flagged with
  `is_generated = 'ALWAYS'`, but `generation_expression` is null.

### Identity column metadata is missing from catalogs

- Reproducer: `TestIdentityColumnCatalogMetadataRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestIdentityColumnCatalogMetadataRepro -count=1`.
- Expected PostgreSQL behavior: identity columns expose `attidentity = 'a'` or
  `'d'` in `pg_attribute`, and `information_schema.columns` reports
  `is_identity = 'YES'` with the matching `identity_generation`.
- Observed Doltgres behavior: identity columns are reported with empty
  `attidentity`, `is_identity = 'NO'`, and no `identity_generation`, even
  though identity DML works.

### Domain column metadata is missing from information_schema

- Reproducer: `TestInformationSchemaDomainColumnMetadataRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestInformationSchemaDomainColumnMetadataRepro -count=1`.
- Expected PostgreSQL behavior: for a column declared with a domain type,
  `information_schema.columns` exposes the underlying `data_type` and the
  `domain_catalog`, `domain_schema`, and `domain_name` fields.
- Observed Doltgres behavior: the domain column row reports empty `data_type`
  and null domain metadata fields.

### View updatability metadata is missing from information_schema

- Reproducer: `TestInformationSchemaViewUpdatabilityMetadataRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestInformationSchemaViewUpdatabilityMetadataRepro -count=1`.
- Expected PostgreSQL behavior: `information_schema.views` reports `YES` or
  `NO` for `is_updatable`, `is_insertable_into`, and the trigger-related
  updatability columns.
- Observed Doltgres behavior: `check_option` is populated, but all five
  updatability columns are null.

### pg_views leaves viewowner empty

- Reproducer: `TestPgViewsViewownerMetadataRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgViewsViewownerMetadataRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_views.viewowner` reports the
  owner name for each view; a view created by the default superuser is owned by
  `postgres`.
- Observed Doltgres behavior: the `pg_views` row exists, but `viewowner` is the
  empty string instead of `postgres`, so catalog consumers cannot recover view
  ownership from `pg_views`.

### pg_get_viewdef(oid, integer) errors instead of returning a definition

- Reproducer: `TestPgGetViewdefWrapColumnOverloadRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgGetViewdefWrapColumnOverloadRepro -count=1`.
- Expected PostgreSQL behavior: `pg_get_viewdef(view_oid, wrap_column)` returns
  the view definition text, with optional wrapping controlled by the integer
  argument.
- Observed Doltgres behavior: `pg_get_viewdef('view'::regclass, 0)` fails with
  `not yet supported` instead of returning the stored view definition.

### pg_get_triggerdef(oid, true) errors instead of returning a definition

- Reproducer: `TestPgGetTriggerdefPrettyOverloadRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgGetTriggerdefPrettyOverloadRepro -count=1`.
- Expected PostgreSQL behavior: `pg_get_triggerdef(trigger_oid, true)` returns
  the trigger definition text, using pretty output when requested.
- Observed Doltgres behavior: the same call fails with `pretty printing is not
  yet supported`, so catalog consumers that request pretty trigger definitions
  cannot introspect valid triggers.

### Trigger WHEN conditions are missing from catalog metadata

- Reproducer: `TestTriggerConditionMetadataRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTriggerConditionMetadataRepro -count=1`.
- Expected PostgreSQL behavior: a trigger created with a `WHEN` condition stores
  that condition in `pg_catalog.pg_trigger.tgqual` and exposes it through
  `information_schema.triggers.action_condition`.
- Observed Doltgres behavior: the `WHEN` trigger is accepted, but both
  `tgqual IS NOT NULL` and `action_condition IS NOT NULL` return `f`, so
  introspection loses the trigger condition.

### Simple updatable-view INSERT does not write the base table

- Reproducer: `TestSimpleUpdatableViewInsertWritesBaseTableRepro` in
  `testing/go/updatable_view_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSimpleUpdatableViewInsertWritesBaseTableRepro -count=1`.
- Expected PostgreSQL behavior: inserting into a simple automatically
  updatable view forwards the row into the underlying base table.
- Observed Doltgres behavior: `INSERT INTO updatable_view_insert_reader`
  fails with `expected insert destination to be resolved or unresolved table`,
  and the base table remains empty.

### Simple updatable-view UPDATE does not write the base table

- Reproducer: `TestSimpleUpdatableViewUpdateWritesBaseTableRepro` in
  `testing/go/updatable_view_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSimpleUpdatableViewUpdateWritesBaseTableRepro -count=1`.
- Expected PostgreSQL behavior: updating a simple automatically updatable view
  forwards the update to matching rows in the underlying base table.
- Observed Doltgres behavior: `UPDATE updatable_view_update_reader` fails with
  `table doesn't support UPDATE`, and the base-table row keeps its old value.

### Simple updatable-view DELETE does not write the base table

- Reproducer: `TestSimpleUpdatableViewDeleteWritesBaseTableRepro` in
  `testing/go/updatable_view_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSimpleUpdatableViewDeleteWritesBaseTableRepro -count=1`.
- Expected PostgreSQL behavior: deleting from a simple automatically updatable
  view forwards the delete to matching rows in the underlying base table.
- Observed Doltgres behavior: `DELETE FROM updatable_view_delete_reader` fails
  with `table doesn't support DELETE FROM`, and the matching base-table row
  remains present.

### CREATE VIEW security_barrier option is rejected

- Reproducer: `TestCreateViewSecurityBarrierPersistsReloptionRepro` in
  `testing/go/view_option_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestCreateViewSecurityBarrierPersistsReloptionRepro
  -count=1`.
- Expected PostgreSQL behavior: `CREATE VIEW ... WITH (security_barrier =
  true) AS ...` is accepted, and `pg_catalog.pg_class.reloptions` persists
  `{security_barrier=true}` for the view.
- Observed Doltgres behavior: the DDL is rejected with
  `ERROR: CREATE VIEW 'security_barrier' option is not yet supported (SQLSTATE
  0A000)`.

### CREATE VIEW security_invoker option loses reloptions metadata

- Reproducer: `TestCreateViewSecurityInvokerPersistsReloptionRepro` in
  `testing/go/view_option_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestCreateViewSecurityInvokerPersistsReloptionRepro
  -count=1`.
- Expected PostgreSQL behavior: `CREATE VIEW ... WITH (security_invoker =
  true) AS ...` is accepted, and `pg_catalog.pg_class.reloptions` persists
  `{security_invoker=true}` for the view.
- Observed Doltgres behavior: the view is accepted, but
  `pg_catalog.pg_class.reloptions` is `NULL`, so catalog consumers cannot see
  that the view was declared with an invoker-security boundary.

### CREATE OR REPLACE VIEW security_invoker option loses reloptions metadata

- Reproducer: `TestCreateOrReplaceViewSecurityInvokerPersistsReloptionRepro`
  in `testing/go/view_option_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateOrReplaceViewSecurityInvokerPersistsReloptionRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE OR REPLACE VIEW ... WITH
  (security_invoker = true) AS ...` persists `{security_invoker=true}` in
  `pg_catalog.pg_class.reloptions`.
- Observed Doltgres behavior: the replacement is accepted, but
  `pg_catalog.pg_class.reloptions` is `NULL`, so replacing a view can create an
  invoker-security view whose security option is invisible in relation
  metadata.

### CREATE VIEW WITH LOCAL CHECK OPTION is rejected

- Reproducer: `TestCreateViewCheckOptionPersistsMetadataRepro` in
  `testing/go/view_option_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestCreateViewCheckOptionPersistsMetadataRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE VIEW ... WITH LOCAL CHECK OPTION` is
  accepted for an updatable view, and `information_schema.views.check_option`
  reports `LOCAL`.
- Observed Doltgres behavior: the DDL is rejected with `ERROR: unsupported
  syntax: WITH LOCAL CHECK OPTION (errno 1105) (sqlstate HY000) (SQLSTATE
  XX000)`.

### CREATE VIEW check_option reloption loses metadata

- Reproducer: `TestCreateViewCheckOptionReloptionPersistsMetadataRepro` in
  `testing/go/view_option_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestCreateViewCheckOptionReloptionPersistsMetadataRepro
  -count=1`.
- Expected PostgreSQL behavior: `CREATE VIEW ... WITH (check_option = 'local')
  AS ...` is accepted for an updatable view, and
  `information_schema.views.check_option` reports `LOCAL`.
- Observed Doltgres behavior: the view is accepted, but
  `information_schema.views.check_option` reports `NONE`.

### CREATE TEMPORARY VIEW is rejected

- Reproducer: `TestCreateTemporaryViewRoundTripRepro` in
  `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestCreateTemporaryViewRoundTripRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE TEMPORARY VIEW ... AS ...` is accepted,
  the view can be queried in the creating session, and it is recorded in a
  `pg_temp_%` schema.
- Observed Doltgres behavior: the DDL is rejected with `ERROR: CREATE
  TEMPORARY VIEW is not yet supported (SQLSTATE 0A000)`.

### CREATE RECURSIVE VIEW is rejected

- Reproducer: `TestCreateRecursiveViewRoundTripRepro` in
  `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestCreateRecursiveViewRoundTripRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE RECURSIVE VIEW name(cols) AS ...` is
  accepted and evaluates the recursive view definition.
- Observed Doltgres behavior: the DDL is rejected with `ERROR: CREATE
  RECURSIVE VIEW is not yet supported (SQLSTATE 0A000)`.

### ALTER VIEW RENAME TO is rejected

- Reproducer: `TestAlterViewRenameToRepro` in
  `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterViewRenameToRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER VIEW name RENAME TO new_name` succeeds,
  the view is queryable under the new name, and the old name no longer resolves.
- Observed Doltgres behavior: the statement fails with `ALTER VIEW is not yet
  supported`, so valid view rename DDL cannot run.

### CREATE OR REPLACE VIEW accepts an existing column rename

- Reproducer: `TestCreateOrReplaceViewRejectsColumnRenameRepro` in
  `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateOrReplaceViewRejectsColumnRenameRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE OR REPLACE VIEW` rejects replacement
  definitions that would rename an existing output column, with an error like
  `cannot change name of view column`.
- Observed Doltgres behavior: the replacement succeeds, so the existing view no
  longer exposes the original `label` column and its stored definition is
  mutated to the incompatible `renamed_label` contract.

### CREATE OR REPLACE VIEW accepts an existing column type change

- Reproducer: `TestCreateOrReplaceViewRejectsColumnTypeChangeRepro` in
  `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateOrReplaceViewRejectsColumnTypeChangeRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE OR REPLACE VIEW` rejects replacement
  definitions that would change the data type of an existing output column,
  with an error like `cannot change data type of view column`.
- Observed Doltgres behavior: the replacement succeeds and querying the
  original `label` column returns integer `7` instead of the original text
  value `old`, so an existing view contract can be silently changed.

### CREATE OR REPLACE VIEW accepts dropping an existing column

- Reproducer: `TestCreateOrReplaceViewRejectsColumnDropRepro` in
  `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateOrReplaceViewRejectsColumnDropRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE OR REPLACE VIEW` rejects replacement
  definitions that would remove an existing output column, with an error like
  `cannot drop columns from view`.
- Observed Doltgres behavior: the replacement succeeds and the original
  `label` column can no longer be selected from the view, so a replacement can
  silently narrow an existing view contract.

### CREATE OR REPLACE VIEW accepts reordering existing columns

- Reproducer: `TestCreateOrReplaceViewRejectsColumnReorderRepro` in
  `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateOrReplaceViewRejectsColumnReorderRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE OR REPLACE VIEW` rejects replacement
  definitions that would reorder existing output columns, with an error like
  `cannot change name of view column`.
- Observed Doltgres behavior: the replacement succeeds, so the stored view
  definition can change the ordinal contract of existing output columns instead
  of requiring an append-only compatible replacement.

### CREATE MATERIALIZED VIEW TIMETZ typmod output stores unrounded values

- Reproducer:
  `TestCreateMaterializedViewTimetzTypmodMaterializesRoundedValueRepro` in
  `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateMaterializedViewTimetzTypmodMaterializesRoundedValueRepro
  -count=1`.
- Expected PostgreSQL behavior: `CREATE MATERIALIZED VIEW name AS SELECT
  CAST('21:43:56.789+00'::timetz AS TIMETZ(0)) AS tz` materializes
  `21:43:57+00` and records the output column type as
  `time(0) with time zone`.
- Observed Doltgres behavior: the materialized-view output column metadata
  reports `time(0) with time zone`, but the stored row is `21:43:56.789+00`,
  so materialized views can persist data outside the declared result-column
  precision.

### CREATE MATERIALIZED VIEW timestamp typmod output stores unrounded values

- Reproducer:
  `TestCreateMaterializedViewTimestampTypmodMaterializesRoundedValueRepro` in
  `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateMaterializedViewTimestampTypmodMaterializesRoundedValueRepro
  -count=1`.
- Expected PostgreSQL behavior: `CREATE MATERIALIZED VIEW name AS SELECT
  CAST(TIMESTAMP '2021-09-15 21:43:56.789' AS TIMESTAMP(0)) AS ts` materializes
  `2021-09-15 21:43:57` and records the output column type as
  `timestamp(0) without time zone`.
- Observed Doltgres behavior: the materialized view stores
  `2021-09-15 21:43:56.789`, so materialized views can persist timestamp values
  outside the declared result-column precision.

### CREATE MATERIALIZED VIEW timestamptz typmod output stores unrounded values

- Reproducer:
  `TestCreateMaterializedViewTimestamptzTypmodMaterializesRoundedValueRepro` in
  `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateMaterializedViewTimestamptzTypmodMaterializesRoundedValueRepro
  -count=1`.
- Expected PostgreSQL behavior: `CREATE MATERIALIZED VIEW name AS SELECT
  CAST(TIMESTAMPTZ '2021-09-15 21:43:56.789+00' AS TIMESTAMPTZ(0)) AS ts`
  materializes `2021-09-15 21:43:57+00` and records the output column type as
  `timestamp(0) with time zone`.
- Observed Doltgres behavior: the materialized view stores
  `2021-09-15 21:43:56.789+00`, so materialized views can persist timestamptz
  values outside the declared result-column precision.

### CREATE MATERIALIZED VIEW interval typmod output stores unrestricted values

- Reproducer:
  `TestCreateMaterializedViewIntervalTypmodMaterializesRestrictedValueRepro` in
  `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateMaterializedViewIntervalTypmodMaterializesRestrictedValueRepro
  -count=1`.
- Expected PostgreSQL behavior: `CREATE MATERIALIZED VIEW name AS SELECT
  CAST(INTERVAL '3 days 04:05:06.789' AS INTERVAL DAY TO SECOND(0)) AS ds`
  materializes `3 days 04:05:07` and records the output column type as
  `interval day to second(0)`.
- Observed Doltgres behavior: the materialized view stores
  `3 days 04:05:06.789` and reports the output column type as plain `interval`,
  so materialized views can persist interval values outside the declared
  result-column precision and lose field restriction metadata.

### CREATE MATERIALIZED VIEW character typmod output stores unpadded values

- Reproducer:
  `TestCreateMaterializedViewCharacterTypmodMaterializesPaddedValueRepro` in
  `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateMaterializedViewCharacterTypmodMaterializesPaddedValueRepro
  -count=1`.
- Expected PostgreSQL behavior: `CREATE MATERIALIZED VIEW name AS SELECT
  CAST('ab' AS CHARACTER(3)) AS label` materializes a padded fixed-width value,
  so `octet_length(label)` is `3`, and records the output column type as
  `character(3)`.
- Observed Doltgres behavior: the materialized-view output column metadata
  reports `character(3)`, but the stored row has `octet_length(label) = 2`, so
  materialized views can persist fixed-width character output without the
  required padding.

### REFRESH MATERIALIZED VIEW TIMETZ typmod output stores unrounded values

- Reproducer:
  `TestRefreshMaterializedViewTimetzTypmodMaterializesRoundedValueRepro` in
  `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRefreshMaterializedViewTimetzTypmodMaterializesRoundedValueRepro
  -count=1`.
- Expected PostgreSQL behavior: `REFRESH MATERIALIZED VIEW` on a
  `WITH NO DATA` materialized view whose query returns
  `CAST(tz AS TIMETZ(0))` materializes the rounded value `21:43:57+00` and
  records the output column type as `time(0) with time zone`.
- Observed Doltgres behavior: the refreshed materialized-view output column
  metadata reports `time(0) with time zone`, but the stored row is
  `21:43:56.789+00`, so refresh can persist data outside the declared
  result-column precision.

### REFRESH MATERIALIZED VIEW timestamp typmod output stores unrounded values

- Reproducer:
  `TestRefreshMaterializedViewTimestampTypmodMaterializesRoundedValueRepro` in
  `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRefreshMaterializedViewTimestampTypmodMaterializesRoundedValueRepro
  -count=1`.
- Expected PostgreSQL behavior: `REFRESH MATERIALIZED VIEW` on a `WITH NO DATA`
  materialized view whose query returns `CAST(ts AS TIMESTAMP(0))` materializes
  the rounded value `2021-09-15 21:43:57` and records the output column type as
  `timestamp(0) without time zone`.
- Observed Doltgres behavior: the refreshed materialized view stores
  `2021-09-15 21:43:56.789`, so refresh can persist timestamp data outside the
  declared result-column precision.

### REFRESH MATERIALIZED VIEW timestamptz typmod output stores unrounded values

- Reproducer:
  `TestRefreshMaterializedViewTimestamptzTypmodMaterializesRoundedValueRepro` in
  `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRefreshMaterializedViewTimestamptzTypmodMaterializesRoundedValueRepro
  -count=1`.
- Expected PostgreSQL behavior: `REFRESH MATERIALIZED VIEW` on a `WITH NO DATA`
  materialized view whose query returns `CAST(ts AS TIMESTAMPTZ(0))`
  materializes `2021-09-15 21:43:57+00` and records the output column type as
  `timestamp(0) with time zone`.
- Observed Doltgres behavior: the refreshed materialized view stores
  `2021-09-15 21:43:56.789+00`, so refresh can persist timestamptz data outside
  the declared result-column precision.

### REFRESH MATERIALIZED VIEW interval typmod output stores unrestricted values

- Reproducer:
  `TestRefreshMaterializedViewIntervalTypmodMaterializesRestrictedValueRepro` in
  `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRefreshMaterializedViewIntervalTypmodMaterializesRestrictedValueRepro
  -count=1`.
- Expected PostgreSQL behavior: `REFRESH MATERIALIZED VIEW` on a `WITH NO DATA`
  materialized view whose query returns `CAST(ds AS INTERVAL DAY TO SECOND(0))`
  materializes `3 days 04:05:07` and records the output column type as
  `interval day to second(0)`.
- Observed Doltgres behavior: the refreshed materialized view stores
  `3 days 04:05:06.789` and reports the output column type as plain `interval`,
  so refresh can persist interval values outside the declared result-column
  precision and lose field restriction metadata.

### REFRESH MATERIALIZED VIEW character typmod output stores overpadded values

- Reproducer:
  `TestRefreshMaterializedViewCharacterTypmodMaterializesPaddedValueRepro` in
  `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRefreshMaterializedViewCharacterTypmodMaterializesPaddedValueRepro
  -count=1`.
- Expected PostgreSQL behavior: `REFRESH MATERIALIZED VIEW` on a `WITH NO DATA`
  materialized view whose query returns `CAST(label AS CHARACTER(3))`
  materializes a padded fixed-width value, so `octet_length(label)` is `3`, and
  records the output column type as `character(3)`.
- Observed Doltgres behavior: the refreshed materialized-view output column
  metadata reports `character(3)`, but the stored row has
  `octet_length(label) = 11`, so refresh can persist fixed-width character
  output with the wrong physical width.

### REFRESH MATERIALIZED VIEW CONCURRENTLY TIMETZ typmod output stores unrounded values

- Reproducer:
  `TestRefreshMaterializedViewConcurrentlyTimetzTypmodMaterializesRoundedValueRepro`
  in `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRefreshMaterializedViewConcurrentlyTimetzTypmodMaterializesRoundedValueRepro
  -count=1`.
- Expected PostgreSQL behavior: `REFRESH MATERIALIZED VIEW CONCURRENTLY`
  refreshes a materialized view whose query returns `CAST(tz AS TIMETZ(0))`
  by materializing the rounded value `21:43:57+00` and preserving the output
  column type as `time(0) with time zone`.
- Observed Doltgres behavior: the concurrent refresh path leaves the
  materialized-view row as `21:43:56.789+00` while the output column metadata
  reports `time(0) with time zone`, so concurrent refresh can persist data
  outside the declared result-column precision.

### REFRESH MATERIALIZED VIEW CONCURRENTLY timestamp typmod output stores unrounded values

- Reproducer:
  `TestRefreshMaterializedViewConcurrentlyTimestampTypmodMaterializesRoundedValueRepro`
  in `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRefreshMaterializedViewConcurrentlyTimestampTypmodMaterializesRoundedValueRepro
  -count=1`.
- Expected PostgreSQL behavior: `REFRESH MATERIALIZED VIEW CONCURRENTLY`
  refreshes a materialized view whose query returns `CAST(ts AS TIMESTAMP(0))`
  by materializing the rounded value `2021-09-15 21:43:57` and preserving the
  output column type as `timestamp(0) without time zone`.
- Observed Doltgres behavior: the concurrent refresh path leaves the
  materialized-view row as `2021-09-15 21:43:56.789`, so concurrent refresh can
  persist timestamp data outside the declared result-column precision.

### REFRESH MATERIALIZED VIEW CONCURRENTLY timestamptz typmod output stores unrounded values

- Reproducer:
  `TestRefreshMaterializedViewConcurrentlyTimestamptzTypmodMaterializesRoundedValueRepro`
  in `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRefreshMaterializedViewConcurrentlyTimestamptzTypmodMaterializesRoundedValueRepro
  -count=1`.
- Expected PostgreSQL behavior: `REFRESH MATERIALIZED VIEW CONCURRENTLY`
  refreshes a materialized view whose query returns `CAST(ts AS TIMESTAMPTZ(0))`
  by materializing `2021-09-15 21:43:57+00` and preserving the output column type
  as `timestamp(0) with time zone`.
- Observed Doltgres behavior: the concurrent refresh path leaves the
  materialized-view row as `2021-09-15 21:43:56.789+00`, so concurrent refresh
  can persist timestamptz data outside the declared result-column precision.

### REFRESH MATERIALIZED VIEW CONCURRENTLY interval typmod output stores unrestricted values

- Reproducer:
  `TestRefreshMaterializedViewConcurrentlyIntervalTypmodMaterializesRestrictedValueRepro`
  in `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRefreshMaterializedViewConcurrentlyIntervalTypmodMaterializesRestrictedValueRepro
  -count=1`.
- Expected PostgreSQL behavior: `REFRESH MATERIALIZED VIEW CONCURRENTLY`
  refreshes a materialized view whose query returns
  `CAST(ds AS INTERVAL DAY TO SECOND(0))` by materializing `3 days 04:05:07`
  and preserving the output column type as `interval day to second(0)`.
- Observed Doltgres behavior: the concurrent refresh path leaves the
  materialized-view row as `3 days 04:05:06.789` and reports the output column
  type as plain `interval`, so concurrent refresh can persist interval values
  outside the declared result-column precision and lose field restriction
  metadata.

### REFRESH MATERIALIZED VIEW CONCURRENTLY character typmod output stores overpadded values

- Reproducer:
  `TestRefreshMaterializedViewConcurrentlyCharacterTypmodMaterializesPaddedValueRepro`
  in `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRefreshMaterializedViewConcurrentlyCharacterTypmodMaterializesPaddedValueRepro
  -count=1`.
- Expected PostgreSQL behavior: `REFRESH MATERIALIZED VIEW CONCURRENTLY`
  refreshes a materialized view whose query returns `CAST(label AS
  CHARACTER(3))` by materializing a padded fixed-width value with
  `octet_length(label) = 3` and preserving the output column type as
  `character(3)`.
- Observed Doltgres behavior: the concurrent refresh path stores a row with
  `octet_length(label) = 11` while the output column metadata reports
  `character(3)`, so concurrent refresh can persist fixed-width character output
  with the wrong physical width.

### CREATE MATERIALIZED VIEW TABLESPACE pg_default is rejected

- Reproducer: `TestCreateMaterializedViewDefaultTablespaceRepro` in
  `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateMaterializedViewDefaultTablespaceRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE MATERIALIZED VIEW name TABLESPACE
  pg_default AS ...` succeeds, populates the materialized view by default, and
  the materialized view can be queried.
- Observed Doltgres behavior: the DDL fails with `CREATE MATERIALIZED VIEW
  TABLESPACE is not yet supported`, so valid dump or migration DDL that spells
  out the default materialized-view placement cannot run.

### CREATE MATERIALIZED VIEW USING heap is rejected

- Reproducer: `TestCreateMaterializedViewUsingHeapRepro` in
  `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateMaterializedViewUsingHeapRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE MATERIALIZED VIEW name USING heap AS
  ...` succeeds, populates the materialized view by default, and the
  materialized view can be queried.
- Observed Doltgres behavior: the DDL fails with `CREATE MATERIALIZED VIEW
  USING is not yet supported`, so valid dump or migration DDL that explicitly
  spells the default materialized-view access method cannot run.

### CREATE MATERIALIZED VIEW storage parameters are rejected

- Reproducer: `TestCreateMaterializedViewStorageParamsRepro` in
  `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateMaterializedViewStorageParamsRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE MATERIALIZED VIEW name WITH
  (fillfactor=70, autovacuum_enabled=false) AS ...` succeeds and persists those
  reloptions in `pg_catalog.pg_class.reloptions`.
- Observed Doltgres behavior: the DDL fails with `CREATE MATERIALIZED VIEW
  storage parameters are not yet supported`, so valid materialized-view storage
  options cannot be restored or used.

### ALTER MATERIALIZED VIEW SET TABLESPACE pg_default is rejected

- Reproducer: `TestAlterMaterializedViewSetDefaultTablespaceRepro` in
  `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterMaterializedViewSetDefaultTablespaceRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER MATERIALIZED VIEW name SET TABLESPACE
  pg_default` succeeds and leaves the materialized view queryable.
- Observed Doltgres behavior: the statement fails with `ALTER MATERIALIZED VIEW
  command is not yet supported`, so valid materialized-view migration DDL that
  spells out default placement cannot run.

### ALTER MATERIALIZED VIEW SET ACCESS METHOD heap is rejected

- Reproducer: `TestAlterMaterializedViewSetHeapAccessMethodRepro` in
  `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterMaterializedViewSetHeapAccessMethodRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER MATERIALIZED VIEW name SET ACCESS METHOD
  heap` succeeds and leaves the materialized view queryable.
- Observed Doltgres behavior: the statement fails with `ALTER MATERIALIZED VIEW
  command is not yet supported`, so valid materialized-view migration DDL that
  explicitly spells the default access method cannot run.

### ALTER MATERIALIZED VIEW reloptions are rejected

- Reproducer: `TestAlterMaterializedViewReloptionsPersistRepro` in
  `testing/go/view_ddl_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterMaterializedViewReloptionsPersistRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER MATERIALIZED VIEW name SET
  (fillfactor=80, autovacuum_enabled=false)` succeeds and persists those
  reloptions in `pg_catalog.pg_class.reloptions`.
- Observed Doltgres behavior: the statement fails with `ALTER MATERIALIZED VIEW
  command is not yet supported`, and the materialized view keeps empty
  reloptions.

### pg_get_function metadata helpers return empty strings

- Reproducer: `TestPgGetFunctionCatalogIntrospectionRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestPgGetFunctionCatalogIntrospectionRepro -count=1`.
- Expected PostgreSQL behavior: for built-in function OID `31` (`byteaout`),
  `pg_get_function_result` returns `cstring`,
  `pg_get_function_identity_arguments` returns `bytea`, and
  `pg_get_functiondef` renders a `CREATE OR REPLACE FUNCTION` definition.
- Observed Doltgres behavior: the result and identity-argument helpers return
  empty strings, and `pg_get_functiondef` does not render the function
  definition.

### pg_encoding_to_char does not map SQL_ASCII

- Reproducer: `TestPgEncodingToCharMapsKnownEncodingIdsRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgEncodingToCharMapsKnownEncodingIdsRepro -count=1`.
- Expected PostgreSQL behavior: known encoding IDs map to canonical names, so
  `pg_encoding_to_char(0)` returns `SQL_ASCII` and
  `pg_encoding_to_char(6)` returns `UTF8`.
- Observed Doltgres behavior: `pg_encoding_to_char(0)` returns an empty string,
  while `pg_encoding_to_char(6)` returns `UTF8`.

### Relation size helpers report zero for populated relations

- Reproducer: `TestRelationSizeHelpersReportStoredDataRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRelationSizeHelpersReportStoredDataRepro -count=1`.
- Expected PostgreSQL behavior: after inserting rows and creating an index,
  `pg_relation_size`, `pg_table_size`, `pg_indexes_size`, and
  `pg_total_relation_size` report nonzero byte counts for the populated
  relation or its index data.
- Observed Doltgres behavior: all five size checks are false because the
  registered size helpers return `0`, so admin and introspection workloads see
  populated relations as empty.

### pg_backend_memory_contexts does not report TopMemoryContext

- Reproducer: `TestPgBackendMemoryContextsReportsTopContextRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgBackendMemoryContextsReportsTopContextRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_backend_memory_contexts`
  exposes the current backend's top-level `TopMemoryContext` row with
  consistent byte counters.
- Observed Doltgres behavior: `pg_backend_memory_contexts` is an empty
  placeholder, so backend memory introspection cannot see the top-level memory
  context.

### pg_hba_file_rules does not report parsed HBA rules

- Reproducer: `TestPgHbaFileRulesReportsParsedRulesRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgHbaFileRulesReportsParsedRulesRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_hba_file_rules` exposes at
  least one parsed HBA rule, and parsed rows do not report errors when the
  active HBA file is valid.
- Observed Doltgres behavior: `pg_hba_file_rules` is an empty placeholder, so
  administrators cannot inspect effective HBA rules through the catalog view.

### pg_shmem_allocations does not report shared-memory allocations

- Reproducer: `TestPgShmemAllocationsReportsAllocationRowsRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgShmemAllocationsReportsAllocationRowsRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_shmem_allocations` exposes one
  or more shared-memory allocation rows with non-negative sizes and
  `allocated_size >= size`.
- Observed Doltgres behavior: `pg_shmem_allocations` is an empty placeholder,
  so `count(*) > 0` returns false and shared-memory allocation introspection
  sees no rows.

### pg_stat_activity does not report the current backend

- Reproducer: `TestPgStatActivityReportsCurrentBackendRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgStatActivityReportsCurrentBackendRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_stat_activity` exposes a row for
  the current backend, so filtering by `pid = pg_backend_pid()` returns at
  least one row.
- Observed Doltgres behavior: `pg_stat_activity` is an empty placeholder, so
  the same filter returns no rows.

### pg_stat_ssl does not report the current backend

- Reproducer: `TestPgStatSslReportsCurrentBackendRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgStatSslReportsCurrentBackendRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_stat_ssl` exposes one row per
  backend, so filtering by `pid = pg_backend_pid()` returns the current
  connection's SSL metadata row.
- Observed Doltgres behavior: `pg_stat_ssl` is an empty placeholder, so the
  current backend is absent from the SSL stats view.

### pg_stat_gssapi does not report the current backend

- Reproducer: `TestPgStatGssapiReportsCurrentBackendRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgStatGssapiReportsCurrentBackendRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_stat_gssapi` exposes one row per
  backend, so filtering by `pid = pg_backend_pid()` returns the current
  connection's GSSAPI metadata row.
- Observed Doltgres behavior: `pg_stat_gssapi` is an empty placeholder, so the
  current backend is absent from the GSSAPI stats view.

### pg_stat_user_tables does not report user tables

- Reproducer: `TestPgStatUserTablesReportsUserRelationsRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgStatUserTablesReportsUserRelationsRepro -count=1`.
- Expected PostgreSQL behavior: after creating a user table,
  `pg_catalog.pg_stat_user_tables` exposes a row for that table with its schema
  and relation name, and statistic counters are numeric values.
- Observed Doltgres behavior: `pg_stat_user_tables` is an empty placeholder, so
  the created user table is absent from the stats view.

### pg_stat_all_tables does not report user tables

- Reproducer: `TestPgStatAllTablesReportsUserRelationsRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgStatAllTablesReportsUserRelationsRepro -count=1`.
- Expected PostgreSQL behavior: after creating a user table,
  `pg_catalog.pg_stat_all_tables` exposes a row for that table with its schema
  and relation name, and statistic counters are numeric values.
- Observed Doltgres behavior: `pg_stat_all_tables` is an empty placeholder, so
  the created user table is absent from the all-table stats view.

### pg_stat_sys_tables does not report system tables

- Reproducer: `TestPgStatSysTablesReportsSystemRelationsRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgStatSysTablesReportsSystemRelationsRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_stat_sys_tables` exposes rows
  for system relations such as `pg_catalog.pg_class`.
- Observed Doltgres behavior: `pg_stat_sys_tables` is an empty placeholder, so
  system-table statistics consumers see no system relations.

### pg_statio_user_tables does not report user tables

- Reproducer: `TestPgStatioUserTablesReportsUserRelationsRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgStatioUserTablesReportsUserRelationsRepro -count=1`.
- Expected PostgreSQL behavior: after creating a user table,
  `pg_catalog.pg_statio_user_tables` exposes a row for that table with its
  schema and relation name, and I/O statistic counters are numeric values.
- Observed Doltgres behavior: `pg_statio_user_tables` is an empty placeholder,
  so the created user table is absent from the user-table I/O stats view.

### pg_statio_all_tables does not report user tables

- Reproducer: `TestPgStatioAllTablesReportsUserRelationsRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgStatioAllTablesReportsUserRelationsRepro -count=1`.
- Expected PostgreSQL behavior: after creating a user table,
  `pg_catalog.pg_statio_all_tables` exposes a row for that table with its
  schema and relation name, and I/O statistic counters are numeric values.
- Observed Doltgres behavior: `pg_statio_all_tables` is an empty placeholder,
  so the created user table is absent from the all-table I/O stats view.

### pg_statio_sys_tables does not report system tables

- Reproducer: `TestPgStatioSysTablesReportsSystemRelationsRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgStatioSysTablesReportsSystemRelationsRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_statio_sys_tables` exposes rows
  for system relations such as `pg_catalog.pg_class`.
- Observed Doltgres behavior: `pg_statio_sys_tables` is an empty placeholder,
  so system-table I/O statistics consumers see no system relations.

### pg_stat_xact_user_tables does not report user tables

- Reproducer: `TestPgStatXactUserTablesReportsUserRelationsRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgStatXactUserTablesReportsUserRelationsRepro -count=1`.
- Expected PostgreSQL behavior: after creating a user table,
  `pg_catalog.pg_stat_xact_user_tables` exposes a current-transaction stats
  row for that table with its schema and relation name, and statistic counters
  are numeric values.
- Observed Doltgres behavior: `pg_stat_xact_user_tables` is an empty
  placeholder, so the created user table is absent from the xact user-table
  stats view.

### pg_stat_xact_all_tables does not report user tables

- Reproducer: `TestPgStatXactAllTablesReportsUserRelationsRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgStatXactAllTablesReportsUserRelationsRepro -count=1`.
- Expected PostgreSQL behavior: after creating a user table,
  `pg_catalog.pg_stat_xact_all_tables` exposes a current-transaction stats row
  for that table with its schema and relation name, and statistic counters are
  numeric values.
- Observed Doltgres behavior: `pg_stat_xact_all_tables` is an empty
  placeholder, so the created user table is absent from the xact all-table
  stats view.

### pg_stat_xact_sys_tables does not report system tables

- Reproducer: `TestPgStatXactSysTablesReportsSystemRelationsRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgStatXactSysTablesReportsSystemRelationsRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_stat_xact_sys_tables` exposes
  current-transaction stats rows for system relations such as
  `pg_catalog.pg_class`.
- Observed Doltgres behavior: `pg_stat_xact_sys_tables` is an empty
  placeholder, so system-table transaction statistics consumers see no system
  relations.

### pg_stat_user_functions does not track called user functions

- Reproducer: `TestPgStatUserFunctionsTracksUserFunctionCallsRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgStatUserFunctionsTracksUserFunctionCallsRepro -count=1`.
- Expected PostgreSQL behavior: with `track_functions = 'all'`, calling a user
  function creates or updates a `pg_catalog.pg_stat_user_functions` row for the
  function with positive call counts and non-negative timing counters.
- Observed Doltgres behavior: `pg_stat_user_functions` is an empty placeholder,
  so the called `public.stat_user_function_target` function is absent even
  after enabling function tracking.

### pg_stat_xact_user_functions does not track current transaction calls

- Reproducer: `TestPgStatXactUserFunctionsTracksCurrentTransactionCallsRepro`
  in `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgStatXactUserFunctionsTracksCurrentTransactionCallsRepro -count=1`.
- Expected PostgreSQL behavior: with `track_functions = 'all'`, calling a user
  function inside a transaction creates or updates a
  `pg_catalog.pg_stat_xact_user_functions` row for the current transaction.
- Observed Doltgres behavior: `pg_stat_xact_user_functions` is an empty
  placeholder, so the called `public.stat_xact_user_function_target` function
  is absent from the current-transaction function stats view.

### pg_stat_database does not report the current database

- Reproducer: `TestPgStatDatabaseReportsCurrentDatabaseRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgStatDatabaseReportsCurrentDatabaseRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_stat_database` exposes a stats
  row for the current database, so filtering by `datname = current_database()`
  returns at least one row.
- Observed Doltgres behavior: `pg_stat_database` is an empty placeholder, so
  the current database is absent from the stats view.

### pg_stat_archiver does not report the cluster-wide archiver row

- Reproducer: `TestPgStatArchiverReportsClusterRowRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgStatArchiverReportsClusterRowRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_stat_archiver` contains exactly
  one cluster-wide archiver statistics row, so `count(*) = 1`.
- Observed Doltgres behavior: `pg_stat_archiver` is an empty placeholder, so
  `count(*) = 1` returns false and archiver statistics consumers see no row.

### pg_stat_bgwriter does not report the cluster-wide bgwriter row

- Reproducer: `TestPgStatBgwriterReportsClusterRowRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgStatBgwriterReportsClusterRowRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_stat_bgwriter` contains exactly
  one cluster-wide background-writer statistics row, so `count(*) = 1`.
- Observed Doltgres behavior: `pg_stat_bgwriter` is an empty placeholder, so
  `count(*) = 1` returns false and background-writer statistics consumers see
  no row.

### pg_stat_database_conflicts does not report the current database

- Reproducer: `TestPgStatDatabaseConflictsReportsCurrentDatabaseRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgStatDatabaseConflictsReportsCurrentDatabaseRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_stat_database_conflicts`
  exposes a stats row for the current database, so filtering by
  `datname = current_database()` returns at least one row.
- Observed Doltgres behavior: `pg_stat_database_conflicts` is an empty
  placeholder, so the current database is absent from the conflict stats view.

### pg_stat_wal does not report the cluster-wide WAL row

- Reproducer: `TestPgStatWalReportsClusterRowRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgStatWalReportsClusterRowRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_stat_wal` contains exactly one
  cluster-wide WAL statistics row, so `count(*) = 1`.
- Observed Doltgres behavior: `pg_stat_wal` is an empty placeholder, so
  `count(*) = 1` returns false and WAL statistics consumers see no row.

### pg_stat_slru does not report SLRU cache rows

- Reproducer: `TestPgStatSlruReportsCacheRowsRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgStatSlruReportsCacheRowsRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_stat_slru` contains one or more
  rows describing SLRU cache statistics, so `count(*) > 0`.
- Observed Doltgres behavior: `pg_stat_slru` is an empty placeholder, so
  `count(*) > 0` returns false and SLRU statistics consumers see no cache
  rows.

### pg_table_is_visible ignores search-path shadowing

- Reproducer: `TestPgTableIsVisibleHonorsSearchPathShadowingRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgTableIsVisibleHonorsSearchPathShadowingRepro -count=1`.
- Expected PostgreSQL behavior: with `search_path = visible_first,
  visible_second`, `pg_table_is_visible('visible_second.shadowed_table')`
  returns `false` when `visible_first.shadowed_table` has the same relation
  name and shadows it.
- Observed Doltgres behavior: `pg_table_is_visible` returns `true` for both
  relations because it only checks whether the relation's schema appears
  anywhere on the search path.

### pg_type_is_visible ignores search-path shadowing

- Reproducer: `TestPgTypeIsVisibleHonorsSearchPathShadowingRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgTypeIsVisibleHonorsSearchPathShadowingRepro -count=1`.
- Expected PostgreSQL behavior: with `search_path = visible_type_first,
  visible_type_second`, `pg_type_is_visible` returns `false` for
  `visible_type_second.shadowed_domain` when `visible_type_first` contains a
  same-named domain that shadows it.
- Observed Doltgres behavior: `pg_type_is_visible` returns `true` for both
  domains because it only checks whether the type's schema appears anywhere on
  the search path.

### pg_proc does not expose built-in functions

- Reproducer: `TestPgProcExposesBuiltinFunctionsRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestPgProcExposesBuiltinFunctionsRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_proc` exposes built-in function
  rows, including overloads for `abs`.
- Observed Doltgres behavior: `count(*) > 0` is false for `proname = 'abs'`
  because the catalog handler is still an empty stub.

### pg_cast does not expose built-in casts

- Reproducer: `TestPgCastExposesBuiltinCastsRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestPgCastExposesBuiltinCastsRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_cast` contains built-in cast
  rows, including the implicit function-backed cast from `integer` to
  `bigint`.
- Observed Doltgres behavior: `pg_cast` returns no row for that built-in cast
  because the catalog handler is still an empty stub.

### pg_operator reports equality operators as neither mergeable nor hashable

- Reproducer: `TestPgOperatorEqualityMergeHashFlagsRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestPgOperatorEqualityMergeHashFlagsRepro -count=1`.
- Expected PostgreSQL behavior: the built-in integer equality operator has
  `oprcanmerge = true` and `oprcanhash = true` in
  `pg_catalog.pg_operator`.
- Observed Doltgres behavior: the operator row exists, but both flags are
  reported as false because the catalog handler hard-codes them for every
  operator.

### pg_language does not expose installed languages

- Reproducer: `TestPgLanguageExposesBuiltinLanguagesRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestPgLanguageExposesBuiltinLanguagesRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_language` exposes installed
  languages, including `sql` and `plpgsql`.
- Observed Doltgres behavior: functions can be created with those languages,
  but `pg_language` returns no rows because the catalog handler is still an
  empty stub.

### pg_tablespace does not expose built-in tablespaces

- Reproducer: `TestPgTablespaceExposesBuiltinTablespacesRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestPgTablespaceExposesBuiltinTablespacesRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_tablespace` exposes the built-in
  `pg_default` and `pg_global` tablespaces.
- Observed Doltgres behavior: `pg_tablespace` returns no rows because the
  catalog handler is still an empty stub.

### CREATE FOREIGN DATA WRAPPER is rejected before populating pg_foreign_data_wrapper

- Reproducer: `TestCreateForeignDataWrapperPersistsCatalogRepro` in
  `testing/go/foreign_data_wrapper_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateForeignDataWrapperPersistsCatalogRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE FOREIGN DATA WRAPPER fdw_catalog_repro`
  succeeds for a superuser and persists a row in
  `pg_catalog.pg_foreign_data_wrapper`.
- Observed Doltgres behavior: the statement fails during parsing with `at or
  near "data": syntax error (SQLSTATE 42601)`, and
  `pg_foreign_data_wrapper` remains empty.

### CREATE SERVER is rejected before validating the referenced foreign-data wrapper

- Reproducer: `TestCreateForeignServerRequiresExistingWrapperRepro` in
  `testing/go/foreign_data_wrapper_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateForeignServerRequiresExistingWrapperRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE SERVER server_missing_fdw_repro FOREIGN
  DATA WRAPPER missing_server_fdw_repro` is a supported DDL form; when the
  referenced foreign-data wrapper is missing, PostgreSQL fails with an
  object-not-found error.
- Observed Doltgres behavior: the statement fails with `at or near
  "server_missing_fdw_repro": syntax error: unimplemented: this syntax
  (SQLSTATE 0A000)` before validating the referenced wrapper or persisting any
  `pg_foreign_server` metadata.

### ALTER and DROP SERVER are rejected before validating the target server

- Reproducer: `TestAlterAndDropForeignServerRequireExistingServerRepro` in
  `testing/go/foreign_data_wrapper_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterAndDropForeignServerRequireExistingServerRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER SERVER missing_alter_server_repro
  VERSION '2'` and `DROP SERVER missing_drop_server_repro` are supported DDL
  forms; when the target server is missing, PostgreSQL fails with an
  object-not-found error.
- Observed Doltgres behavior: `ALTER SERVER` fails with `at or near "server":
  syntax error (SQLSTATE 42601)`, and `DROP SERVER` fails with `at or near
  "missing_drop_server_repro": syntax error: unimplemented: this syntax
  (SQLSTATE 0A000)`.

### CREATE USER MAPPING is rejected before validating the referenced foreign server

- Reproducer: `TestCreateUserMappingRequiresExistingServerRepro` in
  `testing/go/foreign_data_wrapper_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateUserMappingRequiresExistingServerRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE USER MAPPING FOR CURRENT_USER SERVER
  missing_mapping_server_repro` is a supported DDL form; when the referenced
  server is missing, PostgreSQL fails with an object-not-found error.
- Observed Doltgres behavior: the statement fails with `at or near "for":
  syntax error (SQLSTATE 42601)` before validating the referenced server or
  persisting any `pg_user_mapping` metadata.

### CREATE FOREIGN TABLE is rejected before validating the referenced foreign server

- Reproducer: `TestCreateForeignTableRequiresExistingServerRepro` in
  `testing/go/foreign_data_wrapper_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateForeignTableRequiresExistingServerRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE FOREIGN TABLE
  foreign_table_missing_server_repro (id integer) SERVER
  missing_foreign_table_server_repro` is a supported DDL form; when the
  referenced foreign server is missing, PostgreSQL fails with an
  object-not-found error.
- Observed Doltgres behavior: the statement fails with `at or near
  "foreign_table_missing_server_repro": syntax error: unimplemented: this
  syntax (SQLSTATE 0A000)` before validating the referenced server or
  persisting any `pg_foreign_table` metadata.

### DROP FOREIGN TABLE is rejected before validating the target relation

- Reproducer: `TestDropForeignTableRequiresExistingRelationRepro` in
  `testing/go/foreign_data_wrapper_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDropForeignTableRequiresExistingRelationRepro -count=1`.
- Expected PostgreSQL behavior: `DROP FOREIGN TABLE
  missing_foreign_table_repro` is a supported DDL form; when the target
  relation is missing, PostgreSQL fails with an object-not-found error.
- Observed Doltgres behavior: the statement fails with `at or near
  "missing_foreign_table_repro": syntax error: unimplemented: this syntax
  (SQLSTATE 0A000)`.

### IMPORT FOREIGN SCHEMA is rejected before validating the referenced foreign server

- Reproducer: `TestImportForeignSchemaRequiresExistingServerRepro` in
  `testing/go/foreign_data_wrapper_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestImportForeignSchemaRequiresExistingServerRepro -count=1`.
- Expected PostgreSQL behavior: `IMPORT FOREIGN SCHEMA remote_schema FROM
  SERVER missing_import_schema_server_repro INTO public` is a supported DDL
  form; when the referenced foreign server is missing, PostgreSQL fails with an
  object-not-found error.
- Observed Doltgres behavior: the statement fails with `at or near "foreign":
  syntax error (SQLSTATE 42601)` before validating the referenced server.

### ALTER and DROP FOREIGN DATA WRAPPER are rejected before validating the target wrapper

- Reproducer: `TestAlterAndDropForeignDataWrapperRequireExistingWrapperRepro`
  in `testing/go/foreign_data_wrapper_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterAndDropForeignDataWrapperRequireExistingWrapperRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER FOREIGN DATA WRAPPER
  missing_alter_fdw_repro OPTIONS (ADD host 'localhost')` and `DROP FOREIGN
  DATA WRAPPER missing_drop_fdw_repro` are supported DDL forms; when the target
  wrapper is missing, PostgreSQL fails with an object-not-found error.
- Observed Doltgres behavior: `ALTER FOREIGN DATA WRAPPER` fails with `at or
  near "foreign": syntax error (SQLSTATE 42601)`, and `DROP FOREIGN DATA
  WRAPPER` fails with `at or near "wrapper": syntax error: unimplemented: this
  syntax (SQLSTATE 0A000)`.

### ALTER and DROP USER MAPPING are rejected before validating the referenced foreign server

- Reproducer: `TestAlterAndDropUserMappingRequireExistingServerRepro` in
  `testing/go/foreign_data_wrapper_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterAndDropUserMappingRequireExistingServerRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER USER MAPPING FOR CURRENT_USER SERVER
  missing_alter_mapping_server_repro OPTIONS (ADD user 'u')` and `DROP USER
  MAPPING FOR CURRENT_USER SERVER missing_drop_mapping_server_repro` are
  supported DDL forms; when the referenced server is missing, PostgreSQL fails
  with an object-not-found error.
- Observed Doltgres behavior: both statements fail with `at or near "for":
  syntax error (SQLSTATE 42601)` before validating the referenced server.

### ALTER FOREIGN TABLE is rejected before validating the target relation

- Reproducer: `TestAlterForeignTableRequiresExistingRelationRepro` in
  `testing/go/foreign_data_wrapper_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterForeignTableRequiresExistingRelationRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER FOREIGN TABLE
  missing_alter_foreign_table_repro OPTIONS (ADD host 'localhost')` is a
  supported DDL form; when the target relation is missing, PostgreSQL fails
  with an object-not-found error.
- Observed Doltgres behavior: the statement fails with `at or near "foreign":
  syntax error (SQLSTATE 42601)` before validating the target relation.

### SECURITY LABEL is rejected before provider validation

- Reproducer: `TestSecurityLabelReachesProviderValidationRepro` in
  `testing/go/security_label_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSecurityLabelReachesProviderValidationRepro -count=1`.
- Expected PostgreSQL behavior: `SECURITY LABEL ON TABLE ...`, `SECURITY LABEL
  FOR 'dummy' ON TABLE ...`, and `SECURITY LABEL ON ROLE ...` are parsed as
  supported PostgreSQL statements and then rejected at security-label provider
  validation when no provider is loaded.
- Observed Doltgres behavior: all three forms fail during parsing with `at or
  near "security": syntax error (SQLSTATE 42601)` before provider validation or
  any `pg_seclabel` / `pg_shseclabel` catalog path.

### Timezone catalog views do not expose built-in UTC metadata

- Reproducer: `TestPgTimezoneCatalogsExposeUtcRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestPgTimezoneCatalogsExposeUtcRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_timezone_names` exposes a `UTC`
  row with abbreviation `UTC`, and `pg_catalog.pg_timezone_abbrevs` exposes the
  `UTC` abbreviation.
- Observed Doltgres behavior: both timezone catalog views return no rows
  because their handlers are still empty stubs.

### pg_range does not expose built-in range types

- Reproducer: `TestPgRangeExposesBuiltinRangesRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestPgRangeExposesBuiltinRangesRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_range` exposes built-in range
  type metadata, including `int4range` with subtype `integer`.
- Observed Doltgres behavior: `pg_range` returns no rows because the catalog
  handler is still an empty stub.

### Text-search catalogs do not expose built-in parser, config, dictionary, or template rows

- Reproducer: `TestTextSearchCatalogsExposeBuiltinsRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestTextSearchCatalogsExposeBuiltinsRepro -count=1`.
- Expected PostgreSQL behavior: text-search catalogs expose built-in metadata,
  including `pg_ts_config.english`, `pg_ts_dict.english_stem`,
  `pg_ts_parser.default`, and `pg_ts_template.simple`.
- Observed Doltgres behavior: all four catalog tables return no rows because
  their handlers are still empty stubs.

### Built-in full-text search functions are missing

- Reproducer: `TestBuiltInTextSearchFunctionsMatchTermsRepro` in
  `testing/go/text_search_definition_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestBuiltInTextSearchFunctionsMatchTermsRepro -count=1`.
- Expected PostgreSQL behavior: `to_tsvector('simple'::regconfig, 'jumped
  cats')` returns `'cats':2 'jumped':1`, `to_tsquery` parses a tsquery, and the
  `@@` operator can match the generated vector against the query.
- Observed Doltgres behavior: planning fails with `function: 'to_tsvector' not
  found`, so built-in full-text search expressions cannot run even against the
  built-in `simple` configuration.

### User-defined text-search configurations are rejected

- Reproducer: `TestCreateTextSearchConfigurationCopyIsUsableRepro` in
  `testing/go/text_search_definition_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateTextSearchConfigurationCopyIsUsableRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE TEXT SEARCH CONFIGURATION ... (COPY =
  pg_catalog.simple)` succeeds, the new configuration appears in
  `pg_catalog.pg_ts_config`, and functions such as `to_tsvector` can use it.
- Observed Doltgres behavior: setup fails with `ERROR: at or near "search":
  syntax error: unimplemented: this syntax (SQLSTATE 0A000)` at the `CREATE
  TEXT SEARCH CONFIGURATION` statement.

### pg_config, pg_conversion, and pg_aggregate do not expose built-in rows

- Reproducer: `TestMiscBuiltinCatalogsExposeRowsRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestMiscBuiltinCatalogsExposeRowsRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_config` exposes a `BINDIR`
  setting, `pg_catalog.pg_conversion` exposes built-in conversions such as
  `utf8_to_iso_8859_1`, and `pg_catalog.pg_aggregate` contains aggregate
  metadata rows.
- Observed Doltgres behavior: `pg_config` and `pg_conversion` return no rows,
  and `count(*) > 0` is false for `pg_aggregate`, because all three handlers
  are still empty stubs.

### pg_init_privs does not expose built-in initial privileges

- Reproducer: `TestPgInitPrivsExposesBuiltinInitialPrivilegesRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestPgInitPrivsExposesBuiltinInitialPrivilegesRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_init_privs` contains built-in
  initial privilege metadata rows.
- Observed Doltgres behavior: `count(*) > 0` is false because the
  `pg_init_privs` handler is still an empty stub.

### ALTER ROLE SET does not persist role configuration in pg_db_role_setting

- Reproducer: `TestAlterRoleSetPopulatesPgDbRoleSettingRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAlterRoleSetPopulatesPgDbRoleSettingRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER ROLE role_setting_catalog SET work_mem =
  '64kB'` succeeds and `pg_catalog.pg_db_role_setting` exposes a row with
  `setrole = 'role_setting_catalog'::regrole`, `setdatabase = 0`, and
  `setconfig = {work_mem=64kB}`.
- Observed Doltgres behavior: the setup statement fails with a syntax error at
  `SET`, so role-level default configuration cannot be persisted or exposed
  through `pg_db_role_setting`.

### format_type renders InvalidOid as an unknown type

- Reproducer: `TestFormatTypeInvalidOidRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestFormatTypeInvalidOidRepro -count=1`.
- Expected PostgreSQL behavior: `format_type(0::oid, NULL)` and
  `format_type(0::oid, 20)` render `-` for `InvalidOid`.
- Observed Doltgres behavior: both calls render `???`, the marker PostgreSQL
  uses for arbitrary unknown type OIDs.

### regrole type is missing

- Reproducer: `TestRegroleTypeResolvesRolesRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestRegroleTypeResolvesRolesRepro -count=1`.
- Expected PostgreSQL behavior: `regrole` resolves role names to role OIDs and
  renders them back to names; `0::regrole::text` renders `-` for `InvalidOid`.
- Observed Doltgres behavior: casts to `regrole` fail during planning with
  `type "regrole" does not exist`.

### Additional reg* catalog OID types are missing

- Reproducer: `TestAdditionalRegTypesResolveBuiltinsRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAdditionalRegTypesResolveBuiltinsRepro -count=1`.
- Expected PostgreSQL behavior: `regprocedure`, `regoperator`, `regconfig`,
  and `regdictionary` resolve built-in procedures, operators, text-search
  configurations, and text-search dictionaries and render them back as text.
- Observed Doltgres behavior: casts to all four types fail during planning
  with `type "..." does not exist` errors because they are still registered as
  unknown types.

### regtype input does not resolve schema-qualified user-defined domains

- Reproducer: `TestRegtypeResolvesSchemaQualifiedDomainsRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRegtypeResolvesSchemaQualifiedDomainsRepro -count=1`.
- Expected PostgreSQL behavior: after creating domains in explicit schemas,
  literals such as `'regtype_schema_first.lookup_domain'::regtype` and
  `'regtype_schema_second.lookup_domain'::regtype` resolve to those domain type
  OIDs.
- Observed Doltgres behavior: the cast fails during planning with
  `type "regtype_schema_first.lookup_domain" does not exist`, so catalog
  lookups cannot address schema-qualified user-defined domains through
  `regtype`.

### to_regnamespace helper is missing

- Reproducer: `TestToRegnamespaceResolvesSchemaNamesRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestToRegnamespaceResolvesSchemaNamesRepro -count=1`.
- Expected PostgreSQL behavior: `to_regnamespace('pg_catalog')::text` resolves
  to `pg_catalog`, while `to_regnamespace('missing_schema')` returns `NULL`
  instead of throwing.
- Observed Doltgres behavior: planning fails with
  `function: 'to_regnamespace' not found`, even though `regnamespace` itself is
  available for schema OID lookup.

### to_regprocedure helper is missing

- Reproducer: `TestToRegprocedureResolvesFunctionSignaturesRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestToRegprocedureResolvesFunctionSignaturesRepro -count=1`.
- Expected PostgreSQL behavior:
  `to_regprocedure('array_in(cstring,oid,integer)')::text` resolves to
  `array_in(cstring,oid,integer)`, while a missing function signature returns
  `NULL`.
- Observed Doltgres behavior: planning fails with
  `function: 'to_regprocedure' not found`, even though `regprocedure` itself is
  available for function signature OID lookup.

### Interval typmods are missing from catalog metadata

- Reproducer: `TestIntervalTypmodCatalogMetadataRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestIntervalTypmodCatalogMetadataRepro -count=1`.
- Expected PostgreSQL behavior: interval field restrictions and fractional
  precision survive in `pg_attribute.atttypmod`, so `format_type` renders
  `interval year to month`, `interval day to second(3)`, and `interval(2)`.
- Observed Doltgres behavior: all three interval columns store `atttypmod =
  -1` and render as plain `interval`, losing the declared catalog metadata.

### Dropped columns are removed from pg_attribute instead of marked dropped

- Reproducer: `TestDroppedColumnRemainsInPgAttributeRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDroppedColumnRemainsInPgAttributeRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... DROP COLUMN` preserves the
  dropped column's attribute slot in `pg_catalog.pg_attribute` with
  `attisdropped = true`, keeping later columns at their original attribute
  numbers.
- Observed Doltgres behavior: the dropped column's `pg_attribute` row
  disappears and the following column is renumbered into its slot with
  `attisdropped = false`, so catalog consumers see unstable attribute numbers.

### pg_attribute physical type metadata is hardcoded

- Reproducer: `TestPgAttributePhysicalTypeMetadataRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgAttributePhysicalTypeMetadataRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_attribute` exposes
  type-specific physical metadata such as `attlen`, `attbyval`, `attalign`, and
  `attstorage`; for example, `int4` has length `4` and is passed by value,
  `text` is varlena with extended storage, `bool` is char-aligned, `numeric`
  uses main storage, and `timestamptz` is double-aligned.
- Observed Doltgres behavior: every tested column reports `attlen = 0`,
  `attbyval = false`, `attalign = 'i'`, and `attstorage = 'p'`, so catalog
  consumers cannot rely on PostgreSQL-compatible type layout metadata.

### ADD COLUMN default missing-value metadata is not persisted

- Reproducer: `TestPgAttributeMissingValueMetadataRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgAttributeMissingValueMetadataRepro -count=1`.
- Expected PostgreSQL behavior: when `ALTER TABLE ... ADD COLUMN marker INT
  DEFAULT 7` runs on a populated table, `pg_catalog.pg_attribute` records
  `atthasmissing = true` and `attmissingval = '{7}'` for the synthesized
  default value.
- Observed Doltgres behavior: the row value is readable as `7`, but
  `pg_attribute` reports `atthasmissing = false` and `attmissingval = NULL`,
  losing PostgreSQL's catalog metadata for missing column values.

### Temporary tables are missing from pg_class

- Reproducer: `TestTemporaryTableRelpersistenceCatalogMetadataRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestTemporaryTableRelpersistenceCatalogMetadataRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE TEMPORARY TABLE
  temp_rel_persistence` creates a `pg_class` row whose `relpersistence` is
  `t`.
- Observed Doltgres behavior: the temporary table exists for queries, but
  `pg_class` returns no row for it, so catalog consumers cannot identify the
  relation or its temporary persistence.

### Unlogged relations are rejected

- Reproducers: `TestUnloggedTableRelpersistenceCatalogMetadataRepro` and
  `TestUnloggedSequenceRelpersistenceCatalogMetadataRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestUnlogged(Table|Sequence)RelpersistenceCatalogMetadataRepro'
  -count=1`.
- Expected PostgreSQL behavior: `CREATE UNLOGGED TABLE` and `CREATE UNLOGGED
  SEQUENCE` create `pg_class` rows whose `relpersistence` is `u`.
- Observed Doltgres behavior: `CREATE UNLOGGED TABLE` fails with `UNLOGGED is
  not yet supported`, and `CREATE UNLOGGED SEQUENCE` fails with `unlogged
  sequences are not yet supported`, so valid unlogged persistence declarations
  cannot be created.

### pg_class column and check counts are always zero

- Reproducer: `TestPgClassColumnAndCheckCountsRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestPgClassColumnAndCheckCountsRepro -count=1`.
- Expected PostgreSQL behavior: a table with three user columns and one check
  constraint reports `pg_class.relnatts = 3` and `pg_class.relchecks = 1`.
- Observed Doltgres behavior: the same table reports both catalog fields as
  `0`, losing basic relation metadata.

### pg_class view rule metadata is missing

- Reproducer: `TestPgClassViewRuleMetadataRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestPgClassViewRuleMetadataRepro -count=1`.
- Expected PostgreSQL behavior: a view exposing two columns reports
  `pg_class.relnatts = 2` and `pg_class.relhasrules = true`.
- Observed Doltgres behavior: the same view reports `relnatts = 0` and
  `relhasrules = false`.

### CREATE VIEW does not populate pg_rewrite

- Reproducer: `TestCreateViewPopulatesPgRewriteRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateViewPopulatesPgRewriteRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE VIEW rewrite_catalog_view AS ...`
  creates a `pg_catalog.pg_rewrite` row named `_RETURN` for the view.
- Observed Doltgres behavior: the view is accepted, but `pg_rewrite` returns no
  row for the view's rewrite rule.

### CREATE RULE is rejected before rewrite side effects can run

- Reproducer: `TestCreateRuleDoAlsoExecutesAuditInsertRepro` in
  `testing/go/rule_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestCreateRuleDoAlsoExecutesAuditInsertRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE RULE ... ON INSERT ... DO ALSO` is
  accepted, and inserting into the source table also inserts the `NEW` row
  values into the audit table.
- Observed Doltgres behavior: rule creation fails with `ERROR: at or near
  "rule_source_items_audit": syntax error: unimplemented: this syntax
  (SQLSTATE 0A000)`, so rewrite-rule side effects cannot protect audit or
  redirect-write invariants.

### Column defaults that reference sequences do not populate pg_depend

- Reproducer: `TestColumnDefaultSequenceDependencyPopulatesPgDependRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestColumnDefaultSequenceDependencyPopulatesPgDependRepro -count=1`.
- Expected PostgreSQL behavior: a column default that calls
  `nextval('depend_catalog_seq')` creates a normal `pg_depend` row linking the
  default expression to the referenced sequence.
- Observed Doltgres behavior: the default and sequence are accepted, but
  `pg_depend` returns no dependency row for the sequence reference.

### Relation ownership does not populate pg_shdepend

- Reproducer: `TestTableOwnershipPopulatesPgShdependRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestTableOwnershipPopulatesPgShdependRepro -count=1`.
- Expected PostgreSQL behavior: after `ALTER TABLE shdepend_catalog_items OWNER
  TO shdepend_catalog_owner`, `pg_catalog.pg_shdepend` records an owner
  dependency (`deptype = 'o'`) from the relation to the owner role.
- Observed Doltgres behavior: the ownership change is accepted, but
  `pg_shdepend` returns no ownership dependency rows.

## Security

### information_schema privilege-filtered views are not readable by ordinary users

- Reproducers: `TestInformationSchemaTablesHidesUngrantableTablesRepro` and
  `TestInformationSchemaColumnsHidesUngrantableColumnsRepro`,
  `TestInformationSchemaSchemataHidesUngrantableSchemasRepro`,
  `TestInformationSchemaViewsHidesUngrantableViewsRepro`,
  `TestInformationSchemaTriggersHidesUngrantableTriggersRepro`, and
  `TestInformationSchemaTablePrivilegesHidesUngrantableTablesRepro` in
  `testing/go/information_schema_visibility_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'TestInformationSchema(Tables|Columns|Schemata|Views|Triggers|TablePrivileges)HidesUngrantable'
  -count=1`.
- Expected PostgreSQL behavior: ordinary users can query
  `information_schema.tables`, `information_schema.columns`,
  `information_schema.schemata`, `information_schema.views`,
  `information_schema.triggers`, and `information_schema.table_privileges`;
  rows for objects, schemas, columns, triggers, or privileges they cannot see
  are filtered out of the result.
- Observed Doltgres behavior: the ordinary user receives `permission denied`
  against the information-schema backing table (`tables`, `columns`,
  `schemata`, `views`, `triggers`, or `table_privileges`) instead of receiving
  an empty filtered result.

### CREATE DATABASE does not require CREATEDB

- Reproducer: `TestCreateDatabaseRequiresCreatedbPrivilegeRepro` in
  `testing/go/create_database_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateDatabaseRequiresCreatedbPrivilegeRepro -count=1`.
- Expected PostgreSQL behavior: a normal login role without `CREATEDB` cannot
  create databases and receives a permission error.
- Observed Doltgres behavior: `CREATE DATABASE unauthorized_db` succeeds for a
  role created without `CREATEDB`.

### DROP DATABASE does not require ownership

- Reproducer: `TestDropDatabaseRequiresOwnershipRepro` in
  `testing/go/create_database_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDropDatabaseRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: a normal login role cannot drop a database owned
  by another role.
- Observed Doltgres behavior: `DROP DATABASE protected_db` succeeds for a role
  that neither owns the database nor has elevated privileges.

### Revoked database CONNECT does not prevent new sessions

- Reproducer: `TestRevokedDatabaseConnectPreventsNewSessionRepro` in
  `testing/go/database_connect_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestRevokedDatabaseConnectPreventsNewSessionRepro -count=1`.
- Expected PostgreSQL behavior: after `REVOKE CONNECT ON DATABASE postgres FROM
  PUBLIC`, a normal user without an explicit `CONNECT` grant cannot open a new
  session to `postgres`.
- Observed Doltgres behavior: the user connects and runs `SELECT 1` without an
  error.

### Revoked database TEMPORARY does not prevent temporary table creation

- Reproducers: `TestCreateTemporaryTableRequiresDatabaseTemporaryPrivilegeRepro`
  and `TestCreateTemporaryTableAsRequiresDatabaseTemporaryPrivilegeRepro` in
  `testing/go/database_temp_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestCreateTemporaryTable(As)?RequiresDatabaseTemporaryPrivilegeRepro'
  -count=1`.
- Expected PostgreSQL behavior: after `REVOKE TEMPORARY ON DATABASE postgres
  FROM PUBLIC`, a normal role without an explicit `TEMPORARY` grant cannot
  create temporary tables, including `CREATE TEMPORARY TABLE ... AS SELECT`.
- Observed Doltgres behavior: when the role has schema `USAGE, CREATE`, both
  `CREATE TEMPORARY TABLE temp_privilege_denied (id INT)` and `CREATE
  TEMPORARY TABLE temp_ctas_privilege_denied AS SELECT 1 AS id` succeed.

### LOCK TABLE does not require privileges on the locked table

- Reproducer: `TestLockTableRequiresTablePrivilegeRepro` in
  `testing/go/lock_table_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestLockTableRequiresTablePrivilegeRepro -count=1`.
- Expected PostgreSQL behavior: a role needs privileges on a relation before it
  can explicitly lock that relation.
- Observed Doltgres behavior: a role with only schema `USAGE` successfully runs
  `LOCK TABLE lock_table_private IN ACCESS EXCLUSIVE MODE`.

### SELECT FOR UPDATE does not require UPDATE privilege

- Reproducer: `TestSelectForUpdateRequiresUpdatePrivilegeRepro` in
  `testing/go/select_for_update_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSelectForUpdateRequiresUpdatePrivilegeRepro -count=1`.
- Expected PostgreSQL behavior: `SELECT ... FOR UPDATE` requires `UPDATE`
  privilege because it takes update-strength row locks on the selected rows.
- Observed Doltgres behavior: a role with only schema `USAGE` and table
  `SELECT` can run `SELECT id, balance FROM for_update_privilege_private WHERE
  id = 1 FOR UPDATE` without any `UPDATE` grant.

### COMMIT PREPARED does not require prepared transaction ownership

- Reproducer: `TestCommitPreparedRequiresTransactionOwnerRepro` in
  `testing/go/prepared_transaction_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCommitPreparedRequiresTransactionOwnerRepro -count=1`.
- Expected PostgreSQL behavior: a prepared transaction can only be committed by
  the role that prepared it or by an elevated role.
- Observed Doltgres behavior: a different normal role can run `COMMIT PREPARED
  'dg_commit_requires_owner'` and commit another role's prepared transaction.

### ROLLBACK PREPARED does not require prepared transaction ownership

- Reproducer: `TestRollbackPreparedRequiresTransactionOwnerRepro` in
  `testing/go/prepared_transaction_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestRollbackPreparedRequiresTransactionOwnerRepro -count=1`.
- Expected PostgreSQL behavior: a prepared transaction can only be rolled back
  by the role that prepared it or by an elevated role.
- Observed Doltgres behavior: a different normal role can run `ROLLBACK
  PREPARED 'dg_rollback_requires_owner'` and discard another role's prepared
  transaction.

### DROP ROLE allows dropping roles that own tables

- Reproducer: `TestDropRoleOwningTableRepro` in
  `testing/go/drop_role_ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDropRoleOwningTableRepro -count=1`.
- Expected PostgreSQL behavior: `DROP ROLE` fails while the role owns database
  objects, requiring reassignment or object removal first.
- Observed Doltgres behavior: `DROP ROLE doomed_owner` succeeds even after that
  role creates `doomed_owned_table`.

### DROP ROLE allows dropping roles with table privileges

- Reproducer: `TestDropRoleWithGrantedTablePrivilegesRepro` in
  `testing/go/drop_role_ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDropRoleWithGrantedTablePrivilegesRepro -count=1`.
- Expected PostgreSQL behavior: `DROP ROLE` fails while explicit table
  privileges still depend on that role, reporting that objects depend on it.
- Observed Doltgres behavior: after `GRANT SELECT ON doomed_acl_items TO
  doomed_acl_role`, `DROP ROLE doomed_acl_role` succeeds and leaves the
  privilege dependency unblocked.

### DROP ROLE allows dropping roles with non-table ACL dependencies

- Reproducers: `TestDropRoleWithGrantedSchemaPrivilegesRepro`,
  `TestDropRoleWithGrantedSequencePrivilegesRepro`,
  `TestDropRoleWithGrantedRoutinePrivilegesRepro`, and
  `TestDropRoleWithGrantedDatabasePrivilegesRepro` in
  `testing/go/drop_role_ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestDropRoleWithGranted(Schema|Sequence|Routine|Database)PrivilegesRepro'
  -count=1`.
- Expected PostgreSQL behavior: `DROP ROLE` fails while explicit schema,
  sequence, routine, or database privileges still depend on that role,
  reporting that objects depend on it.
- Observed Doltgres behavior: after explicit `GRANT` statements on those
  object classes, `DROP ROLE` succeeds for each grantee role and leaves the
  privilege dependencies unblocked.

### DROP ROLE allows dropping role-membership grantors

- Reproducer: `TestDropRoleWithGrantedRoleMembershipRepro` in
  `testing/go/drop_role_ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDropRoleWithGrantedRoleMembershipRepro -count=1`.
- Expected PostgreSQL behavior: `DROP ROLE` fails while the role is the grantor
  of an active role membership, reporting that dependent membership privileges
  still exist.
- Observed Doltgres behavior: after `doomed_membership_grantor` grants
  `doomed_membership_group` to `doomed_membership_member`, `DROP ROLE
  doomed_membership_grantor` succeeds and leaves the membership grantor
  dependency unblocked.

### Normal roles can grant themselves CREATEDB

- Reproducer: `TestAlterRoleSelfCreatedbPrivilegeEscalationRepro` in
  `testing/go/role_privilege_escalation_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAlterRoleSelfCreatedbPrivilegeEscalationRepro -count=1`.
- Expected PostgreSQL behavior: a normal role cannot grant itself `CREATEDB`.
- Observed Doltgres behavior: `ALTER ROLE self_createdb CREATEDB` succeeds when
  run as `self_createdb`.

### Normal roles can grant themselves CREATEROLE

- Reproducer: `TestAlterRoleSelfCreateRolePrivilegeEscalationRepro` in
  `testing/go/role_privilege_escalation_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAlterRoleSelfCreateRolePrivilegeEscalationRepro -count=1`.
- Expected PostgreSQL behavior: a normal role cannot grant itself `CREATEROLE`.
- Observed Doltgres behavior: `ALTER ROLE self_createrole CREATEROLE` succeeds
  when run as `self_createrole`.

### CREATEROLE users can create BYPASSRLS roles

- Reproducer: `TestCreateRoleBypassRLSRequiresSuperuserRepro` in
  `testing/go/role_privilege_escalation_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateRoleBypassRLSRequiresSuperuserRepro -count=1`.
- Expected PostgreSQL behavior: creating a role with `BYPASSRLS` requires
  superuser privileges.
- Observed Doltgres behavior: a non-superuser role with `CREATEROLE` can run
  `CREATE ROLE bypass_created BYPASSRLS`.

### CREATEROLE users can create REPLICATION roles

- Reproducer: `TestCreateRoleReplicationRequiresSuperuserRepro` in
  `testing/go/role_privilege_escalation_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateRoleReplicationRequiresSuperuserRepro -count=1`.
- Expected PostgreSQL behavior: creating a role with `REPLICATION` requires
  superuser privileges.
- Observed Doltgres behavior: a non-superuser role with `CREATEROLE` can run
  `CREATE ROLE replication_created REPLICATION`.

### CREATEROLE users without CREATEDB can create CREATEDB roles

- Reproducer: `TestCreateRoleCreatedbRequiresCreatedbRepro` in
  `testing/go/role_privilege_escalation_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateRoleCreatedbRequiresCreatedbRepro -count=1`.
- Expected PostgreSQL behavior: creating a role with the `CREATEDB` attribute
  requires the actor to have `CREATEDB`, not only `CREATEROLE`.
- Observed Doltgres behavior: a non-superuser role with `CREATEROLE` but
  without `CREATEDB` can run `CREATE ROLE createdb_created CREATEDB`.

### CREATEROLE users without CREATEDB can grant CREATEDB

- Reproducer: `TestAlterRoleCreatedbRequiresCreatedbRepro` in
  `testing/go/role_privilege_escalation_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterRoleCreatedbRequiresCreatedbRepro -count=1`.
- Expected PostgreSQL behavior: changing another role's `CREATEDB` attribute
  requires the actor to have `CREATEDB`, not only `CREATEROLE`.
- Observed Doltgres behavior: a non-superuser role with `CREATEROLE` but
  without `CREATEDB` can run `ALTER ROLE createdb_altered CREATEDB`.

### Circular and self role memberships cause recovered server panics

- Reproducers: `TestGrantRoleRejectsCircularMembershipRepro` and
  `TestGrantRoleRejectsSelfMembershipRepro` in
  `testing/go/role_membership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'TestGrantRoleRejects(Self|Circular)MembershipRepro' -count=1`.
- Expected PostgreSQL behavior: after `GRANT circular_role_a TO
  circular_role_b`, attempting `GRANT circular_role_b TO circular_role_a`
  fails with a normal SQL error because role memberships cannot be circular;
  `GRANT self_member_role TO self_member_role` similarly fails because a role
  cannot be a member of itself.
- Observed Doltgres behavior: both invalid grants reach
  `auth.AddMemberToGroup`, and the connection handler reports a recovered
  panic: `missing validation to prevent circular role relationships`.

### GRANT role membership does not populate pg_auth_members

- Reproducer: `TestGrantRolePopulatesPgAuthMembersRepro` in
  `testing/go/role_membership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestGrantRolePopulatesPgAuthMembersRepro -count=1`.
- Expected PostgreSQL behavior: after `GRANT catalog_parent_role TO
  catalog_child_role`, `pg_catalog.pg_auth_members` exposes a membership row
  linking the granted role to the member role.
- Observed Doltgres behavior: the grant is accepted and can affect
  authorization, but `pg_auth_members` returns no row.

### GRANT role membership does not populate pg_group

- Reproducer: `TestGrantRolePopulatesPgGroupRepro` in
  `testing/go/role_membership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestGrantRolePopulatesPgGroupRepro -count=1`.
- Expected PostgreSQL behavior: after `GRANT group_catalog_role TO
  group_catalog_member`, the legacy `pg_catalog.pg_group` compatibility view
  exposes the granted role and a non-null `grolist`.
- Observed Doltgres behavior: the grant is accepted, but `pg_group` returns no
  rows for the granted role because the catalog handler is empty.

### REVOKE role removes other grantors' memberships

- Reproducers: `TestRevokeRoleOnlyRemovesNamedGrantorMembershipRepro` and
  `TestRevokeAdminOptionOnlyRemovesNamedGrantorMembershipRepro` in
  `testing/go/role_membership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestRevoke(Role|AdminOption)OnlyRemovesNamedGrantorMembershipRepro'
  -count=1`.
- Expected PostgreSQL behavior: when two grantors independently grant the same
  role membership, revoking one grantor's membership or admin option leaves the
  other grantor's edge in place, so the member remains a member and retains
  delegation ability when the remaining edge has admin option.
- Observed Doltgres behavior: role membership is stored as one member/group
  edge. `REVOKE role_multi_group FROM role_multi_member` removes the membership
  entirely, and `REVOKE ADMIN OPTION FOR role_multi_option_group` strips
  delegation even though another grantor also granted the admin option.

### CREATE ROLE IN ROLE is rejected

- Reproducer: `TestCreateRoleInRoleGrantsMembershipRepro` in
  `testing/go/role_membership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateRoleInRoleGrantsMembershipRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE ROLE child IN ROLE parent` succeeds and
  grants the new role membership in the existing parent role.
- Observed Doltgres behavior: parsing fails with `at or near "in": syntax
  error (SQLSTATE 42601)`.

### CREATE ROLE ROLE membership option is rejected

- Reproducer: `TestCreateRoleRoleOptionAddsMembersRepro` in
  `testing/go/role_membership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateRoleRoleOptionAddsMembersRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE ROLE group_role ROLE member_role`
  succeeds and adds existing roles as members of the newly-created role.
- Observed Doltgres behavior: parsing fails with `at or near "role": syntax
  error (SQLSTATE 42601)`.

### CREATE ROLE ADMIN membership option is rejected

- Reproducer: `TestCreateRoleAdminOptionGrantsAdminMembershipRepro` in
  `testing/go/role_membership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateRoleAdminOptionGrantsAdminMembershipRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE ROLE group_role ADMIN member_role`
  succeeds and grants membership with the admin option, allowing the member to
  delegate `group_role` onward.
- Observed Doltgres behavior: parsing fails with `at or near "admin": syntax
  error (SQLSTATE 42601)`.

### REVOKE ADMIN OPTION ignores dependent role grants

- Reproducers: `TestRevokeAdminOptionRestrictRejectsDependentRoleGrantRepro`
  and `TestRevokeAdminOptionCascadeRemovesDependentRoleGrantRepro` in
  `testing/go/role_membership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestRevokeAdminOption(RestrictRejects|CascadeRemoves)DependentRoleGrantRepro'
  -count=1`.
- Expected PostgreSQL behavior: `REVOKE ADMIN OPTION FOR role FROM member
  RESTRICT` fails with `dependent privileges exist` when `member` used that
  admin option to grant the role onward; the `CASCADE` form succeeds, removes
  dependent downstream memberships, and leaves `member` as a non-admin member.
- Observed Doltgres behavior: the `RESTRICT` form succeeds and strips
  `revoke_role_restrict_middle`'s admin option even though
  `revoke_role_restrict_leaf` depends on it. The `CASCADE` form fails with
  `REVOKE does not yet support CASCADE`, leaving the dependent membership and
  delegation power in place.

### REVOKE role ignores dependent role grants

- Reproducers: `TestRevokeRoleRestrictRejectsDependentRoleGrantRepro` and
  `TestRevokeRoleCascadeRemovesDependentRoleGrantRepro` in
  `testing/go/role_membership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestRevokeRole(RestrictRejects|CascadeRemoves)DependentRoleGrantRepro'
  -count=1`.
- Expected PostgreSQL behavior: `REVOKE role FROM member RESTRICT` fails with
  `dependent privileges exist` when `member` used that membership to grant the
  role onward; the `CASCADE` form succeeds and removes both the direct
  membership and dependent downstream memberships.
- Observed Doltgres behavior: the `RESTRICT` form succeeds and removes
  `revoke_role_full_restrict_middle`'s membership despite the dependent
  `revoke_role_full_restrict_leaf` grant. The `CASCADE` form fails with
  `REVOKE does not yet support CASCADE`, leaving both the direct and downstream
  memberships in place.

### pg_has_role is missing PostgreSQL role-name argument forms

- Reproducer: `TestPgHasRoleSupportsRoleNameArgumentsRepro` in
  `testing/go/role_membership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestPgHasRoleSupportsRoleNameArgumentsRepro -count=1`.
- Expected PostgreSQL behavior: `pg_has_role` supports role-name and `regrole`
  arguments, including `pg_has_role('member', 'role', 'member')`,
  `pg_has_role('member'::regrole, 'role'::regrole, 'member')`, and the
  two-argument current-role form.
- Observed Doltgres behavior: the role-name form tries to coerce the target
  role name to `oid`, the `regrole` form fails because `regrole` is missing,
  and the two-argument form reports that no matching function exists.

### pg_has_role usage ignores intermediate NOINHERIT roles

- Reproducer: `TestPgHasRoleUsageHonorsIntermediateNoInheritRepro` in
  `testing/go/role_membership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgHasRoleUsageHonorsIntermediateNoInheritRepro -count=1`.
- Expected PostgreSQL behavior: if `usage_chain_leaf` is a member of
  `usage_chain_middle`, `usage_chain_middle` is `NOINHERIT`, and
  `usage_chain_middle` is a member of `usage_chain_top`,
  `pg_has_role('usage_chain_leaf', usage_chain_top_oid, 'member')` returns
  true but `pg_has_role(..., 'usage')` returns false.
- Observed Doltgres behavior: `pg_has_role(..., 'usage')` returns true through
  the `NOINHERIT` intermediate role, overstating automatically usable
  privileges in role introspection.

### SET ROLE is rejected instead of assuming granted role privileges

- Reproducer: `TestSetRoleUsesGrantedRolePrivilegesRepro` in
  `testing/go/role_membership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSetRoleUsesGrantedRolePrivilegesRepro -count=1`.
- Expected PostgreSQL behavior: a `NOINHERIT` login role that has been granted
  `set_role_reader` can run `SET ROLE set_role_reader` and then use privileges
  granted to that role, such as `SELECT` on `set_role_private_items`.
- Observed Doltgres behavior: `SET ROLE set_role_reader` fails with `ERROR:
  unknown statement type encountered: *tree.SetRole (SQLSTATE XX000)`, and the
  follow-up `SELECT` remains denied.

### PostgreSQL 15 predefined pg_database_owner role is missing

- Reproducer: `TestPgDatabaseOwnerRoleExistsRepro` in
  `testing/go/role_catalog_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgDatabaseOwnerRoleExistsRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_roles` exposes the predefined
  non-login, non-superuser `pg_database_owner` role. PostgreSQL 15 dump output
  uses that role for public-schema ownership and database-owner privilege
  modeling.
- Observed Doltgres behavior: `pg_roles` returns no row for
  `pg_database_owner`, so PostgreSQL 15 role/catalog introspection and dumps
  that reference that predefined role cannot round-trip faithfully.

### Predefined server-file access roles are missing

- Reproducer: `TestServerFilePredefinedRolesExistRepro` in
  `testing/go/role_catalog_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestServerFilePredefinedRolesExistRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_roles` exposes the predefined
  non-login, non-superuser `pg_read_server_files`, `pg_write_server_files`, and
  `pg_execute_server_program` roles so administrators can grant controlled
  server-file and server-program access.
- Observed Doltgres behavior: `pg_roles` returns zero rows for those three
  predefined roles, so administrators cannot grant PostgreSQL-compatible
  server-file access roles and catalog introspection of those security
  principals fails.

### Predefined data-access and monitoring roles are missing

- Reproducer: `TestDataAndMonitoringPredefinedRolesExistRepro` in
  `testing/go/role_catalog_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDataAndMonitoringPredefinedRolesExistRepro -count=1`.
- Expected PostgreSQL behavior: `pg_catalog.pg_roles` exposes the predefined
  non-login, non-superuser `pg_read_all_data`, `pg_write_all_data`,
  `pg_monitor`, `pg_read_all_settings`, `pg_read_all_stats`,
  `pg_stat_scan_tables`, and `pg_signal_backend` roles so administrators can
  grant cluster-wide data access and monitoring permissions.
- Observed Doltgres behavior: `pg_roles` returns zero rows for those seven
  predefined roles, so PostgreSQL-compatible grants and catalog introspection
  for those principals fail.

### CREATE USER does not populate pg_shadow

- Reproducer: `TestCreateUserPopulatesPgShadowRepro` in
  `testing/go/role_catalog_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateUserPopulatesPgShadowRepro -count=1`.
- Expected PostgreSQL behavior: after `CREATE USER shadow_catalog_user
  PASSWORD 'pw'`, `pg_catalog.pg_shadow` exposes a row for that login role,
  including role flags such as `usesuper` and `usecreatedb`.
- Observed Doltgres behavior: the role is created, but `pg_shadow` returns no
  row because the catalog handler is still an empty stub.

### CREATE USER does not populate pg_authid.rolpassword

- Reproducer: `TestCreateUserPopulatesPgAuthidPasswordRepro` in
  `testing/go/role_catalog_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateUserPopulatesPgAuthidPasswordRepro -count=1`.
- Expected PostgreSQL behavior: after `CREATE USER authid_password_user
  PASSWORD 'pw'`, `pg_catalog.pg_authid.rolpassword` is non-null for that role
  when queried by a superuser.
- Observed Doltgres behavior: the role row exists and password authentication
  can work, but `pg_authid.rolpassword` is always null.

### CREATE USER does not populate pg_user.passwd with the password mask

- Reproducer: `TestCreateUserPopulatesPgUserPasswordMaskRepro` in
  `testing/go/role_catalog_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateUserPopulatesPgUserPasswordMaskRepro -count=1`.
- Expected PostgreSQL behavior: after `CREATE USER pguser_password_user
  PASSWORD 'pw'`, `pg_catalog.pg_user.passwd` exposes the masked value
  `********`.
- Observed Doltgres behavior: the role row exists, but `pg_user.passwd` is
  null.

### Expired role VALID UNTIL timestamps are not enforced at login

- Reproducer: `TestExpiredRoleValidUntilPreventsLoginRepro` in
  `testing/go/role_login_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestExpiredRoleValidUntilPreventsLoginRepro -count=1`.
- Expected PostgreSQL behavior: a role whose password validity timestamp is in
  the past cannot authenticate with that password.
- Observed Doltgres behavior: after the role's `ValidUntil` metadata is set to
  `2000-01-01 00:00:00+00`, authentication still succeeds.

### Role connection limits are not enforced at login

- Reproducer: `TestRoleConnectionLimitPreventsLoginRepro` in
  `testing/go/role_login_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestRoleConnectionLimitPreventsLoginRepro -count=1`.
- Expected PostgreSQL behavior: a role with `CONNECTION LIMIT 0` cannot open a
  new session and receives a "too many connections for role" error.
- Observed Doltgres behavior: `CREATE USER connection_limited ... CONNECTION
  LIMIT 0` is accepted, but that role can still authenticate and open a
  session.

### CREATE ROLE accepts invalid negative connection limits

- Reproducer: `TestCreateRoleRejectsInvalidConnectionLimitRepro` in
  `testing/go/role_option_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateRoleRejectsInvalidConnectionLimitRepro -count=1`.
- Expected PostgreSQL behavior: `CONNECTION LIMIT` accepts `-1` for unlimited
  or any non-negative value, and rejects lower values with
  `invalid connection limit`.
- Observed Doltgres behavior: `CREATE ROLE invalid_create_conn_limit
  CONNECTION LIMIT -2` succeeds and persists invalid role metadata.

### ALTER ROLE accepts invalid negative connection limits

- Reproducer: `TestAlterRoleRejectsInvalidConnectionLimitRepro` in
  `testing/go/role_option_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterRoleRejectsInvalidConnectionLimitRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER ROLE ... CONNECTION LIMIT` rejects
  values below `-1` with `invalid connection limit`.
- Observed Doltgres behavior: `ALTER ROLE invalid_alter_conn_limit CONNECTION
  LIMIT -2` succeeds and persists invalid role metadata.

### Normal roles can set session_replication_role and bypass foreign keys

- Reproducer: `TestSessionReplicationRoleRequiresSuperuserRepro` in
  `testing/go/session_replication_role_security_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestSessionReplicationRoleRequiresSuperuserRepro -count=1`.
- Expected PostgreSQL behavior: `session_replication_role` is a superuser-only
  parameter, so a normal login role cannot set it to `replica`.
- Observed Doltgres behavior: a normal role can set
  `session_replication_role = 'replica'`, then insert a child row that violates
  a foreign key because replica mode suppresses foreign key enforcement.

### ALTER DEFAULT PRIVILEGES grants are not applied to future tables

- Reproducer: `TestAlterDefaultPrivilegesGrantAppliesToFutureTablesRepro` in
  `testing/go/default_privileges_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAlterDefaultPrivilegesGrantAppliesToFutureTablesRepro -count=1`.
- Expected PostgreSQL behavior: after `ALTER DEFAULT PRIVILEGES IN SCHEMA
  public GRANT SELECT ON TABLES TO default_reader`, tables created later in
  that schema grant `SELECT` to `default_reader`.
- Observed Doltgres behavior: the default-privileges statement is accepted, but
  a later table still denies `SELECT` to the grantee with `permission denied for
  table default_priv_items`.

### ALTER DEFAULT PRIVILEGES grants are not applied to future sequences

- Reproducer: `TestAlterDefaultPrivilegesGrantAppliesToFutureSequencesRepro`
  in `testing/go/default_privileges_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAlterDefaultPrivilegesGrantAppliesToFutureSequencesRepro -count=1`.
- Expected PostgreSQL behavior: after `ALTER DEFAULT PRIVILEGES IN SCHEMA
  public GRANT USAGE ON SEQUENCES TO default_sequence_user`, sequences created
  later in that schema grant `USAGE` to `default_sequence_user`.
- Observed Doltgres behavior: the default-privileges statement is accepted, but
  a later sequence still denies `nextval` to the grantee with `permission
  denied for sequence default_priv_sequence`.

### setval does not require sequence UPDATE privilege

- Reproducer: `TestSetvalRequiresUpdatePrivilegeRepro` in
  `testing/go/sequence_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestSetvalRequiresUpdatePrivilegeRepro -count=1`.
- Expected PostgreSQL behavior: a role with only `USAGE` on a sequence can call
  `nextval`, but cannot call `setval` unless it also has `UPDATE` on that
  sequence.
- Observed Doltgres behavior: a role with only schema `USAGE` and sequence
  `USAGE` successfully runs `setval('setval_private_seq', 100)`.

### nextval does not honor sequence UPDATE privilege

- Reproducer: `TestNextvalAllowsUpdatePrivilegeRepro` in
  `testing/go/sequence_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestNextvalAllowsUpdatePrivilegeRepro -count=1`.
- Expected PostgreSQL behavior: `nextval` may be called by a role with either
  `USAGE` or `UPDATE` privilege on the sequence.
- Observed Doltgres behavior: a role with schema `USAGE` and explicit
  sequence `UPDATE` still receives `permission denied for sequence
  nextval_update_seq`.

### Sequence SELECT privilege does not allow sequence relation reads

- Reproducer: `TestSequenceRelationSelectAllowsSelectPrivilegeRepro` in
  `testing/go/sequence_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSequenceRelationSelectAllowsSelectPrivilegeRepro -count=1`.
- Expected PostgreSQL behavior: granting `SELECT` on a sequence allows the
  grantee to read its sequence relation state, such as `last_value`.
- Observed Doltgres behavior: even after `GRANT SELECT ON SEQUENCE
  relation_select_seq TO sequence_relation_select_user`, reading
  `relation_select_seq` fails with `permission denied for table
  relation_select_seq`.

### Sequence owners cannot use sequences they created

- Reproducer: `TestSequenceOwnerCanUseCreatedSequenceRepro` in
  `testing/go/sequence_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSequenceOwnerCanUseCreatedSequenceRepro -count=1`.
- Expected PostgreSQL behavior: a role that creates a sequence owns it and has
  implicit privileges to call `nextval` on that sequence.
- Observed Doltgres behavior: the creating role receives `permission denied for
  sequence owner_created_seq` when it calls `nextval`.

### ALTER DEFAULT PRIVILEGES grants are not applied to future functions

- Reproducer: `TestAlterDefaultPrivilegesGrantAppliesToFutureFunctionsRepro`
  in `testing/go/default_privileges_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAlterDefaultPrivilegesGrantAppliesToFutureFunctionsRepro -count=1`.
- Expected PostgreSQL behavior: after `ALTER DEFAULT PRIVILEGES IN SCHEMA
  public GRANT EXECUTE ON FUNCTIONS TO default_function_user`, functions
  created later in that schema grant `EXECUTE` to `default_function_user`.
- Observed Doltgres behavior: the default-privileges statement is accepted, but
  a later function still denies execution to the grantee with `permission denied
  for routine default_priv_function`.

### ALTER DEFAULT PRIVILEGES does not populate pg_default_acl

- Reproducer: `TestAlterDefaultPrivilegesPopulatesPgDefaultAclRepro` in
  `testing/go/default_privileges_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAlterDefaultPrivilegesPopulatesPgDefaultAclRepro -count=1`.
- Expected PostgreSQL behavior: after `ALTER DEFAULT PRIVILEGES IN SCHEMA
  public GRANT SELECT ON TABLES TO default_acl_catalog_user`,
  `pg_catalog.pg_default_acl` exposes a default ACL row for table privileges in
  that namespace.
- Observed Doltgres behavior: the statement is accepted, but `pg_default_acl`
  returns no row.

### Explicit table grants do not populate pg_class.relacl

- Reproducer: `TestGrantTablePrivilegesPopulatePgClassRelaclRepro` in
  `testing/go/acl_catalog_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestGrantTablePrivilegesPopulatePgClassRelaclRepro -count=1`.
- Expected PostgreSQL behavior: after `GRANT SELECT ON acl_catalog_target TO
  acl_catalog_reader`, `pg_class.relacl` for that table records an explicit ACL
  entry.
- Observed Doltgres behavior: the grant is accepted, but `pg_class.relacl`
  remains null for the table.

### Explicit column grants do not populate pg_attribute.attacl

- Reproducer: `TestPgAttributeColumnAclMetadataRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgAttributeColumnAclMetadataRepro -count=1`.
- Expected PostgreSQL behavior: after `GRANT SELECT (secret) ON table TO
  role`, `pg_catalog.pg_attribute.attacl` for the granted column contains a
  column ACL entry while ungranted columns keep `attacl = NULL`.
- Observed Doltgres behavior: the grant is accepted, but the granted column's
  `attacl` remains `NULL`, so catalog ACL metadata omits column-level grants.

### Explicit schema grants do not populate pg_namespace.nspacl

- Reproducer: `TestGrantSchemaPrivilegesPopulatePgNamespaceNspaclRepro` in
  `testing/go/acl_catalog_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestGrantSchemaPrivilegesPopulatePgNamespaceNspaclRepro -count=1`.
- Expected PostgreSQL behavior: after `GRANT USAGE ON SCHEMA
  acl_schema_target TO acl_schema_reader`, `pg_namespace.nspacl` for that
  schema records an explicit ACL entry.
- Observed Doltgres behavior: the grant is accepted, but
  `pg_namespace.nspacl` remains null for the schema.

### Relation access does not require schema USAGE

- Reproducer: `TestRelationAccessRequiresSchemaUsageRepro` in
  `testing/go/schema_privilege_guard_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRelationAccessRequiresSchemaUsageRepro -count=1`.
- Expected PostgreSQL behavior: a role with `SELECT` on
  `schema_usage_private.items` but no `USAGE` privilege on
  `schema_usage_private` cannot read the table and receives
  `permission denied for schema`.
- Observed Doltgres behavior: the `SELECT` succeeds, so an explicit table grant
  bypasses the schema `USAGE` boundary.

### Materialized view access does not require schema USAGE

- Reproducer: `TestMaterializedViewAccessRequiresSchemaUsageRepro` in
  `testing/go/schema_privilege_guard_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestMaterializedViewAccessRequiresSchemaUsageRepro -count=1`.
- Expected PostgreSQL behavior: a role with `SELECT` on
  `schema_view_private.visible_matview` but no `USAGE` privilege on
  `schema_view_private` cannot read the materialized view and receives
  `permission denied for schema`.
- Observed Doltgres behavior: the `SELECT` succeeds, so an explicit
  materialized-view grant bypasses the schema `USAGE` boundary.

### Procedure and type access do not require schema USAGE

- Reproducer: `TestProcedureAndTypeAccessRequiresSchemaUsageRepro` in
  `testing/go/schema_privilege_guard_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestProcedureAndTypeAccessRequiresSchemaUsageRepro -count=1`.
- Expected PostgreSQL behavior: a role with `EXECUTE` on
  `schema_object_private.hidden_procedure` but no `USAGE` privilege on
  `schema_object_private` cannot call the procedure, and a role without schema
  `USAGE` cannot cast to `schema_object_private.hidden_type`.
- Observed Doltgres behavior: both the `CALL
  schema_object_private.hidden_procedure()` statement and the
  `schema_object_private.hidden_type` cast succeed, so procedure and type
  access bypass the schema `USAGE` boundary.

### Explicit database grants do not populate pg_database.datacl

- Reproducer: `TestGrantDatabasePrivilegesPopulatePgDatabaseDataclRepro` in
  `testing/go/acl_catalog_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestGrantDatabasePrivilegesPopulatePgDatabaseDataclRepro -count=1`.
- Expected PostgreSQL behavior: after `GRANT CONNECT ON DATABASE postgres TO
  acl_database_reader`, `pg_database.datacl` for that database records an
  explicit ACL entry.
- Observed Doltgres behavior: the grant is accepted, but `pg_database.datacl`
  remains null for the database.

### Explicit sequence grants do not populate pg_class.relacl

- Reproducer: `TestGrantSequencePrivilegesPopulatePgClassRelaclRepro` in
  `testing/go/acl_catalog_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestGrantSequencePrivilegesPopulatePgClassRelaclRepro -count=1`.
- Expected PostgreSQL behavior: after `GRANT USAGE ON SEQUENCE
  acl_catalog_sequence TO acl_sequence_reader`, `pg_class.relacl` for that
  sequence records an explicit ACL entry.
- Observed Doltgres behavior: the grant is accepted, but `pg_class.relacl`
  remains null for the sequence.

### Privilege inquiry functions are missing for ACL checks

- Reproducers: `TestPrivilegeInquiryFunctionsReflectColumnGrantsRepro` and
  `TestPrivilegeInquiryFunctionsReflectObjectGrantsRepro` in
  `testing/go/acl_catalog_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestPrivilegeInquiryFunctionsReflect(Column|Object)GrantsRepro'
  -count=1`.
- Expected PostgreSQL behavior: after `GRANT SELECT (id) ON
  privilege_inquiry_private TO privilege_inquiry_reader`,
  `has_table_privilege(..., 'SELECT')` returns `false`,
  `has_column_privilege(..., 'id', 'SELECT')` returns `true`, and
  `has_column_privilege(..., 'secret', 'SELECT')` returns `false`. Explicit
  database, schema, sequence, and function grants are likewise reported by
  `has_database_privilege`, `has_schema_privilege`, `has_sequence_privilege`,
  and `has_function_privilege`.
- Observed Doltgres behavior: planning fails with
  `function: 'has_table_privilege' not found` for table/column checks and
  `function: 'has_database_privilege' not found` for object checks, so clients
  cannot inspect ACL state through PostgreSQL's privilege inquiry helpers.

### Type USAGE privileges cannot be revoked or granted

- Reproducer: `TestTypeUsagePrivilegeCanBeRevokedAndGrantedRepro` in
  `testing/go/type_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTypeUsagePrivilegeCanBeRevokedAndGrantedRepro -count=1`.
- Expected PostgreSQL behavior: a type owner can `REVOKE USAGE ON TYPE
  type_usage_acl_mood FROM PUBLIC`, non-granted roles are denied use of the
  type, `GRANT USAGE ON TYPE` restores access for selected roles, and a later
  revoke removes that access.
- Observed Doltgres behavior: both `REVOKE USAGE ON TYPE` statements fail with
  `this form of REVOKE is not yet supported`, `GRANT USAGE ON TYPE` fails with
  `this form of GRANT is not yet supported`, and the non-owner can still cast
  values to the type after the failed public revoke.

### Language USAGE privileges cannot be revoked or granted

- Reproducer: `TestLanguageUsagePrivilegeCanBeRevokedAndGrantedRepro` in
  `testing/go/language_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLanguageUsagePrivilegeCanBeRevokedAndGrantedRepro -count=1`.
- Expected PostgreSQL behavior: a language owner can `REVOKE USAGE ON LANGUAGE
  plpgsql FROM PUBLIC`, non-granted roles are denied `CREATE FUNCTION ...
  LANGUAGE plpgsql`, `GRANT USAGE ON LANGUAGE` restores access for selected
  roles, and a later revoke removes that access.
- Observed Doltgres behavior: both `REVOKE USAGE ON LANGUAGE` statements fail
  with `this form of REVOKE is not yet supported`, `GRANT USAGE ON LANGUAGE`
  fails with `this form of GRANT is not yet supported`, and the non-owner can
  still create `plpgsql` functions after the failed public revoke.

### Tablespace CREATE privileges cannot be granted or revoked

- Reproducer: `TestTablespaceCreatePrivilegeCanBeGrantedAndRevokedRepro` in
  `testing/go/tablespace_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTablespaceCreatePrivilegeCanBeGrantedAndRevokedRepro -count=1`.
- Expected PostgreSQL behavior: `GRANT CREATE ON TABLESPACE pg_default TO
  tablespace_create_acl_user` and the matching `REVOKE CREATE ON TABLESPACE
  pg_default` are supported ACL statements.
- Observed Doltgres behavior: the grant fails with `this form of GRANT is not
  yet supported`, and the revoke fails with `this form of REVOKE is not yet
  supported`.

### REVOKE GRANTED BY removes other grantors' table privileges

- Reproducer: `TestRevokeGrantedByOnlyRemovesNamedGrantorRepro` in
  `testing/go/grant_revoke_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRevokeGrantedByOnlyRemovesNamedGrantorRepro -count=1`.
- Expected PostgreSQL behavior: when two grantors independently grant `SELECT`
  on the same table to a grantee, `REVOKE SELECT ... GRANTED BY grantor_one`
  removes only `grantor_one`'s ACL entry and the grantee keeps access through
  `grantor_two`.
- Observed Doltgres behavior: after `revoke_grantor_one` runs `REVOKE ...
  GRANTED BY revoke_grantor_one`, `revoke_grantee` loses access to
  `revoke_granted_by_items`, so the revoke removed more ACL state than the
  named grantor's entry.

### REVOKE GRANTED BY removes other grantors' non-table privileges

- Reproducers: `TestRevokeSchemaGrantedByOnlyRemovesNamedGrantorRepro`,
  `TestRevokeSequenceGrantedByOnlyRemovesNamedGrantorRepro`, and
  `TestRevokeRoutineGrantedByOnlyRemovesNamedGrantorRepro` in
  `testing/go/grant_revoke_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestRevoke(Schema|Sequence|Routine)GrantedByOnlyRemovesNamedGrantorRepro'
  -count=1`.
- Expected PostgreSQL behavior: when two grantors independently grant the same
  schema, sequence, or routine privilege to a grantee, `REVOKE ... GRANTED BY
  grantor_one` removes only `grantor_one`'s ACL entry and the grantee keeps
  access through `grantor_two`.
- Observed Doltgres behavior: after `grantor_one` runs `REVOKE ... GRANTED BY
  grantor_one`, the grantee loses schema `CREATE`, sequence `USAGE`, and
  routine `EXECUTE` access, so the revoke removed more ACL state than the named
  grantor's entry.

### REVOKE GRANT OPTION GRANTED BY removes other grantors' grant options

- Reproducer:
  `TestRevokeGrantOptionForGrantedByOnlyRemovesNamedGrantorRepro` in
  `testing/go/grant_revoke_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRevokeGrantOptionForGrantedByOnlyRemovesNamedGrantorRepro -count=1`.
- Expected PostgreSQL behavior: when two grantors independently grant `SELECT`
  with grant option on the same table to a grantee, `REVOKE GRANT OPTION FOR
  SELECT ... GRANTED BY grantor_one` removes only `grantor_one`'s grant option
  entry and the grantee can still delegate through `grantor_two`.
- Observed Doltgres behavior: after `revoke_option_grantor_one` runs `REVOKE
  GRANT OPTION FOR ... GRANTED BY revoke_option_grantor_one`,
  `revoke_option_grantee` can no longer grant `SELECT` to
  `revoke_option_delegate`, so the revoke removed more grant-option state than
  the named grantor's entry.

### REVOKE GRANTED BY accepts inherited roles as grantor

- Reproducer: `TestRevokeGrantedByRequiresCurrentUserRepro` in
  `testing/go/grant_revoke_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRevokeGrantedByRequiresCurrentUserRepro -count=1`.
- Expected PostgreSQL behavior: `REVOKE ... GRANTED BY some_role` requires
  `some_role` to be the current user; being an inheriting member of
  `some_role` is not enough, and PostgreSQL rejects the revoke with `grantor
  must be current user`.
- Observed Doltgres behavior: `revoke_by_member`, which only inherits
  `revoke_by_group`, can run `REVOKE ... GRANTED BY revoke_by_group`; the
  statement succeeds and removes `revoke_by_grantee`'s access to
  `revoke_by_group_items`.

### REVOKE GRANT OPTION CASCADE is rejected before removing dependent grants

- Reproducer: `TestRevokeGrantOptionCascadeRemovesDependentTableGrantRepro` in
  `testing/go/grant_revoke_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRevokeGrantOptionCascadeRemovesDependentTableGrantRepro -count=1`.
- Expected PostgreSQL behavior: `REVOKE GRANT OPTION FOR SELECT ON table FROM
  middle CASCADE` succeeds and removes dependent grants made by `middle`, so a
  downstream grantee can no longer read the table.
- Observed Doltgres behavior: the revoke fails with `REVOKE does not yet
  support CASCADE`, and the dependent `cascade_leaf` grantee keeps `SELECT`
  access to `cascade_items`.

### REVOKE CASCADE is rejected before removing dependent table grants

- Reproducer: `TestRevokeCascadeRemovesDependentTableGrantRepro` in
  `testing/go/grant_revoke_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRevokeCascadeRemovesDependentTableGrantRepro -count=1`.
- Expected PostgreSQL behavior: `REVOKE SELECT ON table FROM middle CASCADE`
  succeeds, removes `middle`'s direct table privilege, and removes dependent
  table grants made by `middle`.
- Observed Doltgres behavior: the revoke fails with `REVOKE does not yet
  support CASCADE`, and both `full_cascade_middle` and `full_cascade_leaf` keep
  `SELECT` access to `full_cascade_items`.

### REVOKE RESTRICT ignores dependent table grants

- Reproducers: `TestRevokeRestrictRejectsDependentTableGrantRepro` and
  `TestRevokeGrantOptionRestrictRejectsDependentTableGrantRepro` in
  `testing/go/grant_revoke_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestRevoke(GrantOption)?RestrictRejectsDependentTableGrantRepro'
  -count=1`.
- Expected PostgreSQL behavior: `REVOKE SELECT ... RESTRICT` and `REVOKE GRANT
  OPTION FOR SELECT ... RESTRICT` both fail with `dependent privileges exist`
  when the target role has made downstream grants, and the original privilege
  graph remains unchanged.
- Observed Doltgres behavior: both revokes succeed instead of rejecting the
  dependent grant chain. The full revoke removes `restrict_middle`'s table
  access while leaving the downstream grant state orphaned, and the grant-option
  revoke strips `restrict_option_middle`'s ability to delegate after PostgreSQL
  would have preserved it.

### GRANT without grant option errors instead of warning and granting nothing

- Reproducer: `TestGrantWithoutGrantOptionDoesNotErrorRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestGrantWithoutGrantOptionDoesNotErrorRepro -count=1`.
- Expected PostgreSQL behavior: when a role with plain `SELECT` but without
  grant option runs `GRANT SELECT ON table TO another_role`, the statement
  completes with a warning and grants no privilege onward; the recipient still
  cannot read the table.
- Observed Doltgres behavior: the `GRANT` statement fails with
  `role "grant_option_plain_user" does not have permission to grant this
  privilege`, so PostgreSQL-compatible delegation attempts that rely on warning
  semantics are rejected.

### GRANT and REVOKE on a missing table succeed

- Reproducers: `TestGrantOnMissingTableRequiresExistingRelationRepro` and
  `TestRevokeOnMissingTableRequiresExistingRelationRepro` in
  `testing/go/grant_missing_object_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'Test(Grant|Revoke)OnMissingTableRequiresExistingRelationRepro' -count=1`.
- Expected PostgreSQL behavior: `GRANT SELECT ON TABLE missing_grant_table TO
  missing_table_grantee` and `REVOKE SELECT ON TABLE missing_revoke_table FROM
  missing_table_revokee` fail because the relation does not exist.
- Observed Doltgres behavior: both statements succeed against missing tables.

### GRANT and REVOKE on a missing schema succeed

- Reproducers: `TestGrantOnMissingSchemaRequiresExistingSchemaRepro` and
  `TestRevokeOnMissingSchemaRequiresExistingSchemaRepro` in
  `testing/go/grant_missing_object_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'Test(Grant|Revoke)OnMissingSchemaRequiresExistingSchemaRepro' -count=1`.
- Expected PostgreSQL behavior: `GRANT USAGE ON SCHEMA missing_grant_schema TO
  missing_schema_grantee` and `REVOKE USAGE ON SCHEMA missing_revoke_schema
  FROM missing_schema_revokee` fail because the schema does not exist.
- Observed Doltgres behavior: both statements succeed against missing schemas.

### GRANT and REVOKE on a missing database succeed

- Reproducers: `TestGrantOnMissingDatabaseRequiresExistingDatabaseRepro` and
  `TestRevokeOnMissingDatabaseRequiresExistingDatabaseRepro` in
  `testing/go/grant_missing_object_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'Test(Grant|Revoke)OnMissingDatabaseRequiresExistingDatabaseRepro' -count=1`.
- Expected PostgreSQL behavior: `GRANT CONNECT ON DATABASE
  missing_grant_database TO missing_database_grantee` and `REVOKE CONNECT ON
  DATABASE missing_revoke_database FROM missing_database_revokee` fail because
  the database does not exist.
- Observed Doltgres behavior: both statements succeed against missing
  databases.

### GRANT and REVOKE on a missing sequence succeed

- Reproducers: `TestGrantOnMissingSequenceRequiresExistingSequenceRepro` and
  `TestRevokeOnMissingSequenceRequiresExistingSequenceRepro` in
  `testing/go/grant_missing_object_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'Test(Grant|Revoke)OnMissingSequenceRequiresExistingSequenceRepro' -count=1`.
- Expected PostgreSQL behavior: `GRANT USAGE ON SEQUENCE
  missing_grant_sequence TO missing_sequence_grantee` and `REVOKE USAGE ON
  SEQUENCE missing_revoke_sequence FROM missing_sequence_revokee` fail because
  the relation does not exist.
- Observed Doltgres behavior: both statements succeed against missing
  sequences.

### GRANT and REVOKE on missing routines succeed

- Reproducers: `TestGrantOnMissingFunctionRequiresExistingRoutineRepro`,
  `TestGrantOnMissingProcedureRequiresExistingRoutineRepro`,
  `TestGrantOnMissingRoutineRequiresExistingRoutineRepro`,
  `TestRevokeOnMissingFunctionRequiresExistingRoutineRepro`,
  `TestRevokeOnMissingProcedureRequiresExistingRoutineRepro`, and
  `TestRevokeOnMissingRoutineRequiresExistingRoutineRepro` in
  `testing/go/grant_missing_object_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'Test(Grant|Revoke)OnMissing(Function|Procedure|Routine)RequiresExistingRoutineRepro'
  -count=1`.
- Expected PostgreSQL behavior: `GRANT EXECUTE ON FUNCTION
  missing_grant_function() TO missing_function_grantee`, `GRANT EXECUTE ON
  PROCEDURE missing_grant_procedure() TO missing_procedure_grantee`, `GRANT
  EXECUTE ON ROUTINE missing_grant_routine() TO missing_routine_grantee`, and
  the matching `REVOKE` statements fail because the routine does not exist.
- Observed Doltgres behavior: all six statements succeed against missing
  functions, procedures, or routines.

### GRANT and REVOKE on foreign data wrappers and foreign servers are rejected

- Reproducers:
  `TestGrantOnMissingForeignDataWrapperRequiresExistingWrapperRepro`,
  `TestGrantOnMissingForeignServerRequiresExistingServerRepro`,
  `TestRevokeOnMissingForeignDataWrapperRequiresExistingWrapperRepro`, and
  `TestRevokeOnMissingForeignServerRequiresExistingServerRepro` in
  `testing/go/grant_missing_object_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'Test(Grant|Revoke)OnMissingForeign(DataWrapper|Server)RequiresExisting(Wrapper|Server)Repro'
  -count=1`.
- Expected PostgreSQL behavior: `GRANT USAGE` and `REVOKE USAGE` on a foreign
  data wrapper or foreign server are supported ACL statements; when the named
  target is missing, they fail with an object-not-found error.
- Observed Doltgres behavior: the FDW and foreign-server `GRANT` statements
  fail with `this form of GRANT is not yet supported (SQLSTATE 0A000)`, and
  the matching `REVOKE` statements fail with `this form of REVOKE is not yet
  supported (SQLSTATE 0A000)` before target-object validation.

### GRANT and REVOKE on configuration parameters are rejected

- Reproducers: `TestGrantOnConfigurationParameterRepro` and
  `TestRevokeOnConfigurationParameterRepro` in
  `testing/go/parameter_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'Test(Grant|Revoke)OnConfigurationParameterRepro' -count=1`.
- Expected PostgreSQL behavior: `GRANT SET ON PARAMETER work_mem TO role`,
  `GRANT ALTER SYSTEM ON PARAMETER work_mem TO role`, and the matching
  `REVOKE` statements are valid configuration-parameter ACL statements.
- Observed Doltgres behavior: both parameter `GRANT` forms fail with
  `this form of GRANT is not yet supported (SQLSTATE 0A000)`, and both
  parameter `REVOKE` forms fail with
  `this form of REVOKE is not yet supported (SQLSTATE 0A000)`.

### GRANT and REVOKE on all objects in a missing schema succeed

- Reproducers:
  `TestGrantOnAllTablesInMissingSchemaRequiresExistingSchemaRepro`,
  `TestGrantOnAllSequencesInMissingSchemaRequiresExistingSchemaRepro`,
  `TestGrantOnAllFunctionsInMissingSchemaRequiresExistingSchemaRepro`,
  `TestRevokeOnAllTablesInMissingSchemaRequiresExistingSchemaRepro`,
  `TestRevokeOnAllSequencesInMissingSchemaRequiresExistingSchemaRepro`, and
  `TestRevokeOnAllFunctionsInMissingSchemaRequiresExistingSchemaRepro` in
  `testing/go/grant_missing_object_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'Test(Grant|Revoke)OnAll(Tables|Sequences|Functions)InMissingSchemaRequiresExistingSchemaRepro'
  -count=1`.
- Expected PostgreSQL behavior: `GRANT` or `REVOKE` on all tables, sequences,
  or functions in a named schema fails when that schema does not exist.
- Observed Doltgres behavior: all six `GRANT`/`REVOKE ... ON ALL ... IN
  SCHEMA missing_schema` statements succeed.

### REVOKE ON ALL TABLES IN SCHEMA revokes another schema

- Reproducer: `TestRevokeAllTablesInSchemaDoesNotAffectOtherSchemasRepro` in
  `testing/go/grant_missing_object_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestRevokeAllTablesInSchemaDoesNotAffectOtherSchemasRepro -count=1`.
- Expected PostgreSQL behavior: after granting `SELECT ON ALL TABLES IN SCHEMA
  public` and `SELECT ON ALL TABLES IN SCHEMA revoke_other_schema` to a user,
  `REVOKE SELECT ON ALL TABLES IN SCHEMA revoke_other_schema FROM user` removes
  only the `revoke_other_schema` privilege.
- Observed Doltgres behavior: the revoke also removes the user's ability to
  select from `public.revoke_public_acl`, so revoking one schema's all-tables
  privilege affects another schema.

### CREATE TABLE foreign keys do not require REFERENCES privilege

- Reproducer: `TestCreateTableForeignKeyRequiresReferencesPrivilegeRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateTableForeignKeyRequiresReferencesPrivilegeRepro -count=1`.
- Expected PostgreSQL behavior: creating a foreign key requires `REFERENCES`
  privilege on the referenced table or columns.
- Observed Doltgres behavior: a role with only `USAGE, CREATE` on the schema
  can create `fk_child_private` referencing `fk_parent_private`.

### CREATE TABLE foreign keys ignore column-scoped REFERENCES privileges

- Reproducer:
  `TestCreateTableForeignKeyRequiresReferencesOnReferencedColumnRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run
  TestCreateTableForeignKeyRequiresReferencesOnReferencedColumnRepro -count=1`.
- Expected PostgreSQL behavior: creating a foreign key requires `REFERENCES`
  privilege on the referenced table or specifically on the referenced parent
  column. A grant on a different parent column is insufficient, so
  `REFERENCES (other_id)` must not authorize a foreign key that references
  `id`.
- Observed Doltgres behavior: a role granted `REFERENCES (other_id)` on
  `fk_column_scope_parent_private` can create
  `fk_column_scope_child_private` referencing `fk_column_scope_parent_private(id)`,
  and the child table is durably created.

### ALTER TABLE ADD FOREIGN KEY does not require REFERENCES privilege

- Reproducer: `TestAlterTableAddForeignKeyRequiresReferencesPrivilegeRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run
  TestAlterTableAddForeignKeyRequiresReferencesPrivilegeRepro -count=1`.
- Expected PostgreSQL behavior: adding a foreign key to an existing table
  requires `REFERENCES` privilege on the referenced table or columns.
- Observed Doltgres behavior: a role with only `USAGE, CREATE` on the schema
  can create `alter_fk_child_private` and then add a foreign key referencing
  `alter_fk_parent_private`, even though it has no `REFERENCES` privilege on
  the parent table.

### ALTER TABLE ADD FOREIGN KEY ignores column-scoped REFERENCES privileges

- Reproducer:
  `TestAlterTableAddForeignKeyRequiresReferencesOnReferencedColumnRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run
  TestAlterTableAddForeignKeyRequiresReferencesOnReferencedColumnRepro
  -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... ADD FOREIGN KEY` requires
  `REFERENCES` privilege on the referenced table or specifically on the
  referenced parent column. A grant on `other_id` must not authorize a foreign
  key that references `id`.
- Observed Doltgres behavior: a role granted only `REFERENCES (other_id)` on
  `alter_fk_column_scope_parent_private` can add a foreign key from
  `alter_fk_column_scope_child_private(parent_id)` to
  `alter_fk_column_scope_parent_private(id)`, and the constraint metadata is
  durably created.

### ALTER TABLE ADD COLUMN REFERENCES does not require REFERENCES privilege

- Reproducer:
  `TestAlterTableAddColumnReferencesRequiresReferencesPrivilegeRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run
  TestAlterTableAddColumnReferencesRequiresReferencesPrivilegeRepro -count=1`.
- Expected PostgreSQL behavior: adding a column with an inline `REFERENCES`
  clause requires `REFERENCES` privilege on the referenced table or columns.
- Observed Doltgres behavior: a role with only `USAGE, CREATE` on the schema
  can create `alter_column_fk_child_private` and then add
  `parent_id INT REFERENCES alter_column_fk_parent_private(id)`, even though it
  has no `REFERENCES` privilege on the parent table.

### ALTER TABLE ADD COLUMN REFERENCES ignores column-scoped REFERENCES privileges

- Reproducer:
  `TestAlterTableAddColumnReferencesRequiresReferencesOnReferencedColumnRepro`
  in `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run
  TestAlterTableAddColumnReferencesRequiresReferencesOnReferencedColumnRepro
  -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... ADD COLUMN ... REFERENCES`
  requires `REFERENCES` privilege on the referenced table or specifically on
  the referenced parent column. A grant on `other_id` must not authorize an
  inline foreign key that references `id`.
- Observed Doltgres behavior: a role granted only `REFERENCES (other_id)` on
  `alter_column_fk_scope_parent_private` can add
  `parent_id INT REFERENCES alter_column_fk_scope_parent_private(id)` to
  `alter_column_fk_scope_child_private`, and the unauthorized column is durably
  created.

### ALTER TABLE ADD COLUMN does not require table ownership

- Reproducer: `TestAlterTableAddColumnRequiresOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAlterTableAddColumnRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: adding a column requires ownership of the table.
- Observed Doltgres behavior: a role with only schema `USAGE` can run `ALTER
  TABLE alter_add_column_private ADD COLUMN label TEXT`.

### ALTER TABLE DROP COLUMN does not require table ownership

- Reproducer: `TestAlterTableDropColumnRequiresOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAlterTableDropColumnRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: dropping a column requires ownership of the
  table.
- Observed Doltgres behavior: a role with only schema `USAGE` can run `ALTER
  TABLE alter_drop_column_private DROP COLUMN label`.

### ALTER TABLE ALTER COLUMN mutations do not require table ownership

- Reproducers: `TestAlterTableAlterColumnTypeRequiresOwnershipRepro`,
  `TestAlterTableAlterColumnSetDefaultRequiresOwnershipRepro`,
  `TestAlterTableAlterColumnDropDefaultRequiresOwnershipRepro`,
  `TestAlterTableAlterColumnSetNotNullRequiresOwnershipRepro`, and
  `TestAlterTableAlterColumnDropNotNullRequiresOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestAlterTableAlterColumn(Type|SetDefault|DropDefault|SetNotNull|DropNotNull)RequiresOwnershipRepro'
  -count=1`.
- Expected PostgreSQL behavior: changing a column's type, default, or
  nullability through `ALTER TABLE ... ALTER COLUMN` requires ownership of the
  table.
- Observed Doltgres behavior: a role with only schema `USAGE` can run `ALTER
  TABLE alter_column_type_private ALTER COLUMN amount TYPE BIGINT`, and
  `information_schema.columns` shows the persisted type changed to `bigint`.
  The same level of access can set `amount DEFAULT 7`, causing later inserts
  that omit `amount` to persist `7`, or drop an existing default, causing later
  omitted inserts to persist `NULL`. It can also run `ALTER COLUMN amount SET
  NOT NULL`, causing later null inserts to fail, or `DROP NOT NULL`, causing
  later null inserts to succeed.

### ALTER TABLE RENAME COLUMN does not require table ownership

- Reproducer: `TestAlterTableRenameColumnRequiresOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAlterTableRenameColumnRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: renaming a column requires ownership of the
  table.
- Observed Doltgres behavior: a role with only schema `USAGE` can run `ALTER
  TABLE alter_rename_column_private RENAME COLUMN label TO renamed_label`.

### ALTER TABLE ADD CONSTRAINT does not require table ownership

- Reproducer: `TestAlterTableAddConstraintRequiresOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAlterTableAddConstraintRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: adding a table constraint requires ownership of
  the table.
- Observed Doltgres behavior: a role with only schema `USAGE` can run `ALTER
  TABLE alter_add_constraint_private ADD CONSTRAINT value_positive CHECK (value
  > 0)`.

### ALTER TABLE DROP CONSTRAINT does not require table ownership

- Reproducer: `TestAlterTableDropConstraintRequiresOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAlterTableDropConstraintRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: dropping a table constraint requires ownership
  of the table.
- Observed Doltgres behavior: a role with only schema `USAGE` can run `ALTER
  TABLE alter_drop_constraint_private DROP CONSTRAINT value_positive`.

### ALTER TABLE REPLICA IDENTITY does not require table ownership

- Reproducer: `TestAlterTableReplicaIdentityRequiresOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterTableReplicaIdentityRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: changing a table's replica identity requires
  ownership of that table; a non-owner receives `must be owner of table`, and
  `pg_class.relreplident` remains at the default `d`.
- Observed Doltgres behavior: a role with only schema `USAGE` can run `ALTER
  TABLE replica_identity_owner_private REPLICA IDENTITY FULL`, changing the
  table's `relreplident` to `f`.

### ALTER TABLE RENAME TO does not require table ownership

- Reproducer: `TestRenameTableRequiresOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestRenameTableRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: renaming a table requires ownership of the
  table.
- Observed Doltgres behavior: a role with only schema `USAGE` can run `ALTER
  TABLE rename_table_private RENAME TO rename_table_private_new`.

### CREATE TRIGGER does not require TRIGGER privilege

- Reproducer: `TestCreateTriggerRequiresTriggerPrivilegeRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateTriggerRequiresTriggerPrivilegeRepro -count=1`.
- Expected PostgreSQL behavior: creating a trigger requires `TRIGGER` privilege
  on the target table, in addition to `EXECUTE` on the trigger function.
- Observed Doltgres behavior: a role with `EXECUTE` on the trigger function but
  no table privileges can create a trigger on `trigger_private`.

### DROP TRIGGER does not require table ownership

- Reproducer: `TestDropTriggerRequiresTableOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDropTriggerRequiresTableOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: dropping a trigger requires ownership of the
  table the trigger is defined on.
- Observed Doltgres behavior: a role that only has schema `USAGE` can drop
  `drop_trigger_private_before_insert` from `drop_trigger_private`.

### DROP FUNCTION does not require function ownership

- Reproducer: `TestDropFunctionRequiresOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDropFunctionRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: dropping a function requires ownership of that
  function.
- Observed Doltgres behavior: a role that only has schema `USAGE` can drop
  `drop_function_private()`.

### CREATE OR REPLACE FUNCTION does not require existing function ownership

- Reproducer: `TestCreateOrReplaceFunctionRequiresOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateOrReplaceFunctionRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: replacing an existing function requires
  ownership of that function.
- Observed Doltgres behavior: a role with schema `USAGE, CREATE` can run
  `CREATE OR REPLACE FUNCTION replace_function_private()` and replace a
  function owned by another role.

### Function EXECUTE grants apply to other overloads

- Reproducer: `TestFunctionExecuteGrantDoesNotApplyToOtherOverloadsRepro` in
  `testing/go/routine_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestFunctionExecuteGrantDoesNotApplyToOtherOverloadsRepro -count=1`.
- Expected PostgreSQL behavior: `GRANT EXECUTE ON FUNCTION name(INT)` grants
  only that overload, not other overloads with the same name.
- Observed Doltgres behavior: after granting `EXECUTE` on
  `overload_secret(INT)`, the grantee can execute
  `overload_secret('hidden'::TEXT)`.

### DROP PROCEDURE does not require procedure ownership

- Reproducer: `TestDropProcedureRequiresOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDropProcedureRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: dropping a procedure requires ownership of that
  procedure.
- Observed Doltgres behavior: a role that only has schema `USAGE` can drop
  `drop_procedure_private()`.

### CREATE OR REPLACE PROCEDURE does not require existing procedure ownership

- Reproducer: `TestCreateOrReplaceProcedureRequiresOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateOrReplaceProcedureRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: replacing an existing procedure requires
  ownership of that procedure.
- Observed Doltgres behavior: a role with schema `USAGE, CREATE` can run
  `CREATE OR REPLACE PROCEDURE replace_procedure_private()` and replace a
  procedure owned by another role.

### Procedure EXECUTE grants apply to other overloads

- Reproducer: `TestProcedureExecuteGrantDoesNotApplyToOtherOverloadsRepro` in
  `testing/go/routine_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestProcedureExecuteGrantDoesNotApplyToOtherOverloadsRepro -count=1`.
- Expected PostgreSQL behavior: `GRANT EXECUTE ON PROCEDURE name(INT)` grants
  only that overload, not other overloads with the same name.
- Observed Doltgres behavior: after granting `EXECUTE` on
  `overload_secret_proc(INT)`, the grantee can call
  `overload_secret_proc('hidden'::TEXT)`.

### DROP SEQUENCE does not require sequence ownership

- Reproducer: `TestDropSequenceRequiresOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDropSequenceRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: dropping a sequence requires ownership of that
  sequence.
- Observed Doltgres behavior: a role that only has schema `USAGE` can drop
  `drop_sequence_private`.

### CREATE SEQUENCE OWNED BY does not require table ownership

- Reproducer: `TestCreateSequenceOwnedByRequiresTableOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateSequenceOwnedByRequiresTableOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: creating a sequence owned by a table column
  requires ownership of the referenced table.
- Observed Doltgres behavior: a role with schema `USAGE, CREATE` but no table
  privileges can run `CREATE SEQUENCE sequence_owner_hijacked OWNED BY
  sequence_owner_private.id`.

### ALTER SEQUENCE OWNED BY does not require table ownership

- Reproducer: `TestAlterSequenceOwnedByRequiresTableOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAlterSequenceOwnedByRequiresTableOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: changing a sequence's `OWNED BY` dependency
  requires ownership of the referenced table.
- Observed Doltgres behavior: a role with only schema `USAGE` and sequence
  `UPDATE` can run `ALTER SEQUENCE alter_sequence_owned OWNED BY
  alter_sequence_owner_private.id`.

### ALTER SEQUENCE does not require sequence ownership

- Reproducer: `TestAlterSequenceRequiresSequenceOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterSequenceRequiresSequenceOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: changing a sequence's ownership dependency
  metadata with `ALTER SEQUENCE ... OWNED BY NONE` requires ownership of the
  sequence.
- Observed Doltgres behavior: a role with only schema `USAGE` and sequence
  `UPDATE` can run `ALTER SEQUENCE alter_sequence_private OWNED BY NONE`,
  clearing the sequence's ownership dependency.

### DROP TYPE does not require type ownership

- Reproducer: `TestDropTypeRequiresOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDropTypeRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: dropping a type requires ownership of that type.
- Observed Doltgres behavior: a role that only has schema `USAGE` can drop
  `drop_type_private`.

### DROP DOMAIN does not require domain ownership

- Reproducer: `TestDropDomainRequiresOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDropDomainRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: dropping a domain requires ownership of that
  domain.
- Observed Doltgres behavior: a role that only has schema `USAGE` can drop
  `drop_domain_private`.

### DROP INDEX does not require table ownership

- Reproducer: `TestDropIndexRequiresTableOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDropIndexRequiresTableOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: dropping an index requires ownership of the
  indexed relation.
- Observed Doltgres behavior: a role that only has schema `USAGE` can drop
  `drop_index_private_label_idx`.

### CREATE EXTENSION does not require database CREATE privilege

- Reproducer: `TestCreateExtensionRequiresCreatePrivilegeRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateExtensionRequiresCreatePrivilegeRepro -count=1`.
- Expected PostgreSQL behavior: creating an extension requires database-level
  authority, including `CREATE` privilege for trusted extensions.
- Observed Doltgres behavior: a normal role can create `"uuid-ossp"` without an
  explicit database `CREATE` grant.

### CREATE EXTENSION ignores fixed schema for non-relocatable extensions

- Reproducer:
  `TestCreateExtensionRejectsSchemaForNonRelocatableExtensionRepro` in
  `testing/go/extension_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateExtensionRejectsSchemaForNonRelocatableExtensionRepro
  -count=1`.
- Expected PostgreSQL behavior: a non-relocatable extension with a fixed
  control-file schema, such as `plpgsql` in `pg_catalog`, rejects an explicit
  conflicting target schema.
- Observed Doltgres behavior: `CREATE EXTENSION plpgsql WITH SCHEMA public`
  succeeds and records `plpgsql` in `pg_extension` with `public` as its
  namespace.

### DROP EXTENSION does not require extension ownership

- Reproducer: `TestDropExtensionRequiresOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDropExtensionRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: dropping an extension requires ownership of that
  extension.
- Observed Doltgres behavior: a normal role can drop `hstore` after another
  role creates it.

### DROP EXTENSION ignores dependent objects

- Reproducer: `TestDropExtensionRestrictRejectsDependentObjectsRepro` in
  `testing/go/extension_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDropExtensionRestrictRejectsDependentObjectsRepro -count=1`.
- Expected PostgreSQL behavior: `DROP EXTENSION hstore` uses `RESTRICT` by
  default and fails while a user table still depends on the extension-provided
  `public.hstore` type, leaving the extension installed.
- Observed Doltgres behavior: the drop succeeds even though
  `hstore_extension_dependents.payload` was declared with `public.hstore`,
  removes the `pg_extension` row, and makes `to_regtype('public.hstore')`
  return `NULL` instead of preserving the dependency-protected extension
  objects.

### DROP EXTENSION CASCADE leaves dependent columns behind

- Reproducer: `TestDropExtensionCascadeRemovesDependentColumnsRepro` in
  `testing/go/extension_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDropExtensionCascadeRemovesDependentColumnsRepro -count=1`.
- Expected PostgreSQL behavior: `DROP EXTENSION hstore CASCADE` removes
  dependent objects that use extension-provided objects, including a table
  column declared with `public.hstore`.
- Observed Doltgres behavior: the extension and `public.hstore` type are
  removed, but `information_schema.columns` still reports the dependent
  `payload` column on `hstore_extension_cascade_dependents`.

### DROP TYPE can remove an installed extension member type

- Reproducer: `TestDropExtensionMemberTypeRequiresDropExtensionRepro` in
  `testing/go/extension_dependency_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDropExtensionMemberTypeRequiresDropExtensionRepro -count=1`.
- Expected PostgreSQL behavior: an extension member object such as
  `public.hstore` cannot be dropped directly while `hstore` is installed; the
  user must drop the extension instead.
- Observed Doltgres behavior: `DROP TYPE public.hstore` succeeds, leaving the
  `pg_extension` row for `hstore` installed while `to_regtype('public.hstore')`
  returns `NULL`.

### CREATE EXTENSION WITH SCHEMA does not require schema CREATE privilege

- Reproducer:
  `TestCreateExtensionWithSchemaRequiresSchemaCreatePrivilegeRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateExtensionWithSchemaRequiresSchemaCreatePrivilegeRepro
  -count=1`.
- Expected PostgreSQL behavior: installing a trusted extension into an explicit
  target schema requires both database `CREATE` authority and `CREATE`
  privilege on that schema, because extension objects are created there.
- Observed Doltgres behavior: a role with database `CREATE` and only schema
  `USAGE` can create `hstore` inside `extension_private_schema`.

### CREATE VIEW does not require schema CREATE privilege

- Reproducer: `TestCreateViewRequiresSchemaCreatePrivilegeRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateViewRequiresSchemaCreatePrivilegeRepro -count=1`.
- Expected PostgreSQL behavior: creating a view requires `CREATE` privilege on
  the target schema.
- Observed Doltgres behavior: a role with only `USAGE` on
  `view_private_schema` can create `created_without_create` there.

### CREATE TYPE and DOMAIN do not require schema CREATE privilege

- Reproducer: `TestCreateTypeAndDomainRequireSchemaCreatePrivilegeRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateTypeAndDomainRequireSchemaCreatePrivilegeRepro -count=1`.
- Expected PostgreSQL behavior: creating a type or domain in a schema requires
  `CREATE` privilege on that target schema; a role with only `USAGE` on
  `create_object_private` cannot create `created_without_create_type` or
  `created_without_create_domain` there.
- Observed Doltgres behavior: both `CREATE TYPE
  create_object_private.created_without_create_type AS ENUM ('one')` and
  `CREATE DOMAIN create_object_private.created_without_create_domain AS INT`
  succeed, so type and domain creation bypass schema `CREATE`.

### DROP VIEW does not require view ownership

- Reproducer: `TestDropViewRequiresOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDropViewRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: dropping a view requires ownership of that view.
- Observed Doltgres behavior: a role that only has schema `USAGE` can drop
  `drop_view_private`.

### REFRESH MATERIALIZED VIEW does not require materialized-view ownership

- Reproducer: `TestRefreshMaterializedViewRequiresOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestRefreshMaterializedViewRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: refreshing a materialized view requires
  ownership of that materialized view.
- Observed Doltgres behavior: a role with schema `USAGE`, source-table
  `SELECT`, and ordinary privileges on `refresh_mv_private` can run `REFRESH
  MATERIALIZED VIEW refresh_mv_private`.

### ALTER MATERIALIZED VIEW RENAME COLUMN does not require ownership

- Reproducer: `TestAlterMaterializedViewRenameColumnRequiresOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAlterMaterializedViewRenameColumnRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: renaming a materialized-view column requires
  ownership of that materialized view.
- Observed Doltgres behavior: a role with schema `USAGE` and ordinary
  privileges on `rename_mv_private` can run `ALTER MATERIALIZED VIEW
  rename_mv_private RENAME COLUMN label TO renamed_label`.

### ALTER MATERIALIZED VIEW RENAME TO does not require ownership

- Reproducer: `TestAlterMaterializedViewRenameToRequiresOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterMaterializedViewRenameToRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: renaming a materialized view requires ownership
  of that materialized view.
- Observed Doltgres behavior: a role with schema `USAGE` and ordinary
  privileges on `rename_to_mv_private` can run `ALTER MATERIALIZED VIEW
  rename_to_mv_private RENAME TO renamed_by_non_owner`.

### CREATE INDEX does not require table ownership

- Reproducer: `TestCreateIndexRequiresTableOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateIndexRequiresTableOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: creating an index requires ownership of the
  indexed table.
- Observed Doltgres behavior: a role that only has schema `USAGE` can create
  `create_index_private_label_idx` on `create_index_private`.

### ALTER INDEX does not require table ownership

- Reproducer: `TestAlterIndexRequiresTableOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAlterIndexRequiresTableOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: altering index storage options requires
  ownership of the indexed table.
- Observed Doltgres behavior: a role that only has schema `USAGE` can run
  `ALTER INDEX alter_index_private_label_idx SET (fillfactor = 80)`.

### ALTER INDEX RENAME does not require table ownership

- Reproducer: `TestRenameIndexRequiresTableOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestRenameIndexRequiresTableOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: renaming an index requires ownership of the
  indexed table.
- Observed Doltgres behavior: a role that only has schema `USAGE` can rename
  `rename_index_private_label_idx`.

### REINDEX TABLE does not require table ownership

- Reproducer: `TestReindexTableRequiresOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestReindexTableRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: reindexing a table requires ownership of that
  table or elevated privilege.
- Observed Doltgres behavior: a role that only has schema `USAGE` can run
  `REINDEX TABLE reindex_table_private`.

### REINDEX INDEX does not require table ownership

- Reproducer: `TestReindexIndexRequiresTableOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestReindexIndexRequiresTableOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: reindexing an index requires ownership of the
  indexed table or elevated privilege.
- Observed Doltgres behavior: a role that only has schema `USAGE` can run
  `REINDEX INDEX reindex_index_private_label_idx`.

### CREATE PUBLICATION does not require table ownership

- Reproducer: `TestCreatePublicationRequiresTableOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreatePublicationRequiresTableOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: publishing a specific table requires ownership
  of that table.
- Observed Doltgres behavior: a role that only has schema `USAGE` can create
  `publication_private_pub FOR TABLE publication_private`.

### ALTER PUBLICATION ADD TABLE does not require table ownership

- Reproducer: `TestAlterPublicationAddTableRequiresTableOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterPublicationAddTableRequiresTableOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: even when a role owns a publication, adding a
  table to that publication requires ownership of the table being added.
- Observed Doltgres behavior: the publication owner can add a table owned by
  another role, and the table appears in `pg_publication_tables`.

### ALTER PUBLICATION SET TABLE does not require table ownership

- Reproducer: `TestAlterPublicationSetTableRequiresTableOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterPublicationSetTableRequiresTableOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: replacing publication membership with `ALTER
  PUBLICATION ... SET TABLE` requires ownership of each table being published.
- Observed Doltgres behavior: the publication owner can replace membership with
  a table owned by another role, and the table appears in
  `pg_publication_tables`.

### CREATE PUBLICATION FOR ALL TABLES does not require superuser

- Reproducer: `TestCreatePublicationForAllTablesRequiresSuperuserRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreatePublicationForAllTablesRequiresSuperuserRepro -count=1`.
- Expected PostgreSQL behavior: only superusers can create a `FOR ALL TABLES`
  publication.
- Observed Doltgres behavior: a normal role with database `CREATE` privilege can
  create a publication that includes all current and future tables.

### CREATE PUBLICATION FOR TABLES IN SCHEMA does not require superuser

- Reproducer: `TestCreatePublicationForTablesInSchemaRequiresSuperuserRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreatePublicationForTablesInSchemaRequiresSuperuserRepro -count=1`.
- Expected PostgreSQL behavior: only superusers can create a `FOR TABLES IN
  SCHEMA` publication, because it publishes all current and future tables in the
  named schema.
- Observed Doltgres behavior: a normal role with database `CREATE` privilege and
  schema `USAGE` can create the schema-wide publication, and the row persists in
  `pg_publication`.

### ALTER PUBLICATION ADD TABLES IN SCHEMA does not require superuser

- Reproducer:
  `TestAlterPublicationAddTablesInSchemaRequiresSuperuserRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterPublicationAddTablesInSchemaRequiresSuperuserRepro -count=1`.
- Expected PostgreSQL behavior: adding `TABLES IN SCHEMA` membership to a
  publication requires superuser privilege, because it publishes all current and
  future tables in the named schema.
- Observed Doltgres behavior: a normal publication owner with database `CREATE`
  privilege and schema `USAGE` can add the schema-wide membership, and the row
  persists in `pg_publication_namespace`.

### ALTER PUBLICATION SET TABLES IN SCHEMA does not require superuser

- Reproducer:
  `TestAlterPublicationSetTablesInSchemaRequiresSuperuserRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterPublicationSetTablesInSchemaRequiresSuperuserRepro -count=1`.
- Expected PostgreSQL behavior: replacing publication membership with `TABLES
  IN SCHEMA` requires superuser privilege, because it publishes all current and
  future tables in the named schema.
- Observed Doltgres behavior: a normal publication owner with database `CREATE`
  privilege and schema `USAGE` can replace membership with the schema-wide
  entry, and the row persists in `pg_publication_namespace`.

### ALTER PUBLICATION does not require publication ownership

- Reproducer: `TestAlterPublicationRequiresOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAlterPublicationRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: altering a publication requires ownership of
  that publication.
- Observed Doltgres behavior: a normal role can rename
  `publication_alter_pub`.

### DROP PUBLICATION does not require publication ownership

- Reproducer: `TestDropPublicationRequiresOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDropPublicationRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: dropping a publication requires ownership of
  that publication.
- Observed Doltgres behavior: a normal role can drop
  `publication_drop_pub`.

### CREATE SUBSCRIPTION does not require elevated privilege

- Reproducer: `TestCreateSubscriptionRequiresSuperuserRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateSubscriptionRequiresSuperuserRepro -count=1`.
- Expected PostgreSQL behavior: creating a subscription requires elevated
  privilege.
- Observed Doltgres behavior: a normal role can create metadata-only
  subscription `subscription_created`.

### CREATE SUBSCRIPTION does not set subscription ownership

- Reproducer: `TestCreateSubscriptionSetsOwnerCatalogRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateSubscriptionSetsOwnerCatalogRepro -count=1`.
- Expected PostgreSQL behavior: a subscription created by a superuser records
  that creating role in `pg_subscription.subowner`.
- Observed Doltgres behavior: the subscription is created, but
  `pg_subscription.subowner` resolves to `postgres` instead of the creating
  role.

### ALTER SUBSCRIPTION OWNER TO does not update subscription ownership

- Reproducer: `TestAlterSubscriptionOwnerUpdatesCatalogRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterSubscriptionOwnerUpdatesCatalogRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER SUBSCRIPTION ... OWNER TO role` updates
  `pg_subscription.subowner` to the target role.
- Observed Doltgres behavior: the statement is accepted, but
  `pg_subscription.subowner` remains `postgres`.

### ALTER SUBSCRIPTION OWNER TO accepts missing roles

- Reproducer: `TestAlterSubscriptionOwnerRejectsMissingRoleRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterSubscriptionOwnerRejectsMissingRoleRepro -count=1`.
- Expected PostgreSQL behavior: changing a subscription owner validates that
  the target role exists and rejects missing roles.
- Observed Doltgres behavior: `ALTER SUBSCRIPTION ... OWNER TO
  missing_subscription_owner` succeeds even though no such role exists.

### ALTER SUBSCRIPTION ADD PUBLICATION matches publication names case-insensitively

- Reproducer: `TestSubscriptionAddPublicationNamesAreCaseSensitiveRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionAddPublicationNamesAreCaseSensitiveRepro -count=1`.
- Expected PostgreSQL behavior: subscription publication names are
  case-sensitive identifiers, so adding unquoted `casepublication` to a
  subscription that already has quoted `"CasePublication"` appends a distinct
  publication name.
- Observed Doltgres behavior: `ALTER SUBSCRIPTION ... ADD PUBLICATION
  casepublication` treats `"CasePublication"` as a duplicate and rejects the
  valid metadata change.

### ALTER SUBSCRIPTION ADD PUBLICATION rejects refresh=false

- Reproducer: `TestSubscriptionAddPublicationRefreshFalseRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionAddPublicationRefreshFalseRepro -count=1`.
- Expected PostgreSQL behavior: for a disabled metadata-only subscription,
  `ALTER SUBSCRIPTION ... ADD PUBLICATION ... WITH (refresh = false)` updates
  the stored publication list without refreshing remote table state.
- Observed Doltgres behavior: the valid `refresh` option is rejected as an
  unrecognized subscription option, so the metadata update cannot run.

### ALTER SUBSCRIPTION SET PUBLICATION rejects refresh=false

- Reproducer: `TestSubscriptionSetPublicationRefreshFalseRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionSetPublicationRefreshFalseRepro -count=1`.
- Expected PostgreSQL behavior: for a disabled metadata-only subscription,
  `ALTER SUBSCRIPTION ... SET PUBLICATION ... WITH (refresh = false)` replaces
  the stored publication list without refreshing remote table state.
- Observed Doltgres behavior: the valid `refresh` option is rejected as an
  unrecognized subscription option, so the metadata update cannot run.

### ALTER SUBSCRIPTION DROP PUBLICATION rejects refresh=false

- Reproducer: `TestSubscriptionDropPublicationRefreshFalseRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionDropPublicationRefreshFalseRepro -count=1`.
- Expected PostgreSQL behavior: for a disabled metadata-only subscription,
  `ALTER SUBSCRIPTION ... DROP PUBLICATION ... WITH (refresh = false)` updates
  the stored publication list without trying to refresh remote table state.
- Observed Doltgres behavior: the valid `refresh` option is rejected as an
  unrecognized subscription option, so the metadata update cannot run.

### ALTER SUBSCRIPTION SET slot_name=NONE is allowed while enabled

- Reproducer: `TestSubscriptionSetSlotNameNoneRequiresDisabledRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionSetSlotNameNoneRequiresDisabledRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER SUBSCRIPTION ... SET (slot_name = NONE)`
  is rejected while the subscription is enabled, preserving its replication slot
  association.
- Observed Doltgres behavior: the enabled subscription accepts the metadata
  change and clears the slot association.

### CREATE SUBSCRIPTION reorders publication names

- Reproducer: `TestSubscriptionCreatePreservesPublicationOrderRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionCreatePreservesPublicationOrderRepro -count=1`.
- Expected PostgreSQL behavior: `pg_subscription.subpublications` preserves the
  publication order supplied to `CREATE SUBSCRIPTION`.
- Observed Doltgres behavior: persisted subscription metadata is normalized by
  sorting the publication names, so `z_pub_order, a_pub_order` is stored as
  `a_pub_order,z_pub_order`.

### CREATE SUBSCRIPTION silently compacts duplicate publication names

- Reproducer: `TestSubscriptionCreateRejectsDuplicatePublicationRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionCreateRejectsDuplicatePublicationRepro -count=1`.
- Expected PostgreSQL behavior: duplicate publication names in a subscription
  definition are rejected with `publication name ... used more than once`.
- Observed Doltgres behavior: the subscription is accepted and duplicate
  publication names are compacted during metadata normalization.

### CREATE SUBSCRIPTION accepts duplicate connect options

- Reproducer: `TestSubscriptionCreateRejectsDuplicateConnectOptionRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionCreateRejectsDuplicateConnectOptionRepro -count=1`.
- Expected PostgreSQL behavior: duplicate option names in `CREATE
  SUBSCRIPTION` are rejected with `conflicting or redundant options` before
  subscription metadata is created.
- Observed Doltgres behavior: the duplicate `connect` option list is accepted
  after one value is silently kept, and the subscription row is persisted.

### CREATE SUBSCRIPTION accepts duplicate enabled options

- Reproducer: `TestSubscriptionCreateRejectsDuplicateEnabledOptionRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionCreateRejectsDuplicateEnabledOptionRepro -count=1`.
- Expected PostgreSQL behavior: duplicate `enabled` options in `CREATE
  SUBSCRIPTION` are rejected with `conflicting or redundant options` before
  subscription metadata is created.
- Observed Doltgres behavior: the duplicate `enabled` option list is accepted
  after one value is silently kept, and the subscription row is persisted.

### ALTER SUBSCRIPTION accepts duplicate synchronous_commit options

- Reproducer:
  `TestSubscriptionAlterRejectsDuplicateSynchronousCommitOptionRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionAlterRejectsDuplicateSynchronousCommitOptionRepro
  -count=1`.
- Expected PostgreSQL behavior: duplicate options in `ALTER SUBSCRIPTION SET`
  are rejected with `conflicting or redundant options`, preserving the previous
  subscription metadata.
- Observed Doltgres behavior: the duplicate `synchronous_commit` option list is
  accepted and `pg_subscription.subsynccommit` is rewritten from `off` to
  `local`.

### CREATE SUBSCRIPTION accepts duplicate slot_name options

- Reproducer: `TestSubscriptionCreateRejectsDuplicateSlotNameOptionRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionCreateRejectsDuplicateSlotNameOptionRepro -count=1`.
- Expected PostgreSQL behavior: duplicate `slot_name` options in `CREATE
  SUBSCRIPTION` are rejected with `conflicting or redundant options` before
  subscription metadata is created.
- Observed Doltgres behavior: the duplicate `slot_name` option list is accepted
  after one value is silently kept, and the subscription row is persisted.

### ALTER SUBSCRIPTION accepts duplicate slot_name options

- Reproducer: `TestSubscriptionAlterRejectsDuplicateSlotNameOptionRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionAlterRejectsDuplicateSlotNameOptionRepro -count=1`.
- Expected PostgreSQL behavior: duplicate `slot_name` options in `ALTER
  SUBSCRIPTION SET` are rejected with `conflicting or redundant options`,
  preserving the previous replication slot name.
- Observed Doltgres behavior: the duplicate `slot_name` option list is accepted
  and clears `pg_subscription.subslotname`.

### CREATE SUBSCRIPTION accepts duplicate synchronous_commit options

- Reproducer:
  `TestSubscriptionCreateRejectsDuplicateSynchronousCommitOptionRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionCreateRejectsDuplicateSynchronousCommitOptionRepro
  -count=1`.
- Expected PostgreSQL behavior: duplicate `synchronous_commit` options in
  `CREATE SUBSCRIPTION` are rejected with `conflicting or redundant options`
  before subscription metadata is created.
- Observed Doltgres behavior: the duplicate `synchronous_commit` option list is
  accepted after one value is silently kept, and the subscription row is
  persisted.

### ALTER SUBSCRIPTION accepts duplicate binary options

- Reproducer: `TestSubscriptionAlterRejectsDuplicateBinaryOptionRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionAlterRejectsDuplicateBinaryOptionRepro -count=1`.
- Expected PostgreSQL behavior: duplicate `binary` options in `ALTER
  SUBSCRIPTION SET` are rejected with `conflicting or redundant options`,
  preserving the previous binary mode flag.
- Observed Doltgres behavior: the duplicate `binary` option list is accepted and
  `pg_subscription.subbinary` is rewritten from false to true.

### CREATE SUBSCRIPTION accepts an empty slot_name

- Reproducer: `TestSubscriptionCreateRejectsEmptySlotNameRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionCreateRejectsEmptySlotNameRepro -count=1`.
- Expected PostgreSQL behavior: `slot_name = ''` is rejected because a
  replication slot name cannot be empty.
- Observed Doltgres behavior: the metadata-only subscription is accepted with an
  empty slot name.

### ALTER SUBSCRIPTION accepts an empty slot_name

- Reproducer: `TestSubscriptionAlterRejectsEmptySlotNameRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionAlterRejectsEmptySlotNameRepro -count=1`.
- Expected PostgreSQL behavior: changing a subscription to `slot_name = ''` is
  rejected and preserves the existing slot association.
- Observed Doltgres behavior: the invalid metadata change is accepted and clears
  the stored slot name.

### CREATE SUBSCRIPTION accepts invalid streaming values

- Reproducer: `TestSubscriptionCreateRejectsInvalidStreamingOptionRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionCreateRejectsInvalidStreamingOptionRepro -count=1`.
- Expected PostgreSQL behavior: `streaming` must be boolean or `parallel`.
- Observed Doltgres behavior: invalid streaming values are accepted and coerced
  to false.

### ALTER SUBSCRIPTION accepts invalid streaming values

- Reproducer: `TestSubscriptionAlterRejectsInvalidStreamingOptionRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionAlterRejectsInvalidStreamingOptionRepro -count=1`.
- Expected PostgreSQL behavior: invalid `streaming` values are rejected and the
  previous `substream` value is preserved.
- Observed Doltgres behavior: the invalid value is accepted and the stored
  streaming flag is changed to false.

### ALTER SUBSCRIPTION accepts invalid synchronous_commit values

- Reproducer: `TestSubscriptionAlterRejectsInvalidSynchronousCommitRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionAlterRejectsInvalidSynchronousCommitRepro -count=1`.
- Expected PostgreSQL behavior: `synchronous_commit` is validated against the
  allowed values `local`, `remote_write`, `remote_apply`, `on`, and `off`.
- Observed Doltgres behavior: an arbitrary value is accepted and persisted to
  `pg_subscription.subsynccommit`.

### CREATE SUBSCRIPTION accepts invalid synchronous_commit values

- Reproducer: `TestSubscriptionCreateRejectsInvalidSynchronousCommitRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionCreateRejectsInvalidSynchronousCommitRepro -count=1`.
- Expected PostgreSQL behavior: create-time `synchronous_commit` values are
  validated against the same allowed enum as alter-time values.
- Observed Doltgres behavior: an arbitrary value is accepted for a
  metadata-only subscription.

### ALTER SUBSCRIPTION accepts unsupported two_phase changes

- Reproducer: `TestSubscriptionAlterRejectsTwoPhaseOptionRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionAlterRejectsTwoPhaseOptionRepro -count=1`.
- Expected PostgreSQL behavior: altering the `two_phase` subscription parameter
  is rejected on this PostgreSQL baseline.
- Observed Doltgres behavior: `ALTER SUBSCRIPTION ... SET (two_phase = false)`
  succeeds and changes `subtwophasestate`.

### ALTER SUBSCRIPTION SKIP rejects lsn=NONE

- Reproducer: `TestSubscriptionSkipLsnNoneClearsSkipLsnRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionSkipLsnNoneClearsSkipLsnRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER SUBSCRIPTION ... SKIP (lsn = NONE)`
  clears `pg_subscription.subskiplsn` back to `0/0`.
- Observed Doltgres behavior: the valid `NONE` form fails with a syntax error
  and leaves the previous skip LSN unchanged.

### ALTER SUBSCRIPTION ADD PUBLICATION accepts invalid copy_data values

- Reproducer: `TestSubscriptionAddPublicationRejectsInvalidCopyDataRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionAddPublicationRejectsInvalidCopyDataRepro -count=1`.
- Expected PostgreSQL behavior: non-boolean `copy_data` values are rejected and
  the publication list is unchanged.
- Observed Doltgres behavior: the invalid value is ignored and the new
  publication is added.

### ALTER SUBSCRIPTION SET PUBLICATION accepts invalid copy_data values

- Reproducer: `TestSubscriptionSetPublicationRejectsInvalidCopyDataRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionSetPublicationRejectsInvalidCopyDataRepro -count=1`.
- Expected PostgreSQL behavior: non-boolean `copy_data` values are rejected and
  the existing publication list is preserved.
- Observed Doltgres behavior: the invalid value is ignored and the publication
  list is replaced.

### ALTER SUBSCRIPTION DROP PUBLICATION accepts invalid copy_data values

- Reproducer: `TestSubscriptionDropPublicationRejectsInvalidCopyDataRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionDropPublicationRejectsInvalidCopyDataRepro -count=1`.
- Expected PostgreSQL behavior: non-boolean `copy_data` values are rejected and
  the existing publication list is preserved.
- Observed Doltgres behavior: the invalid value is ignored and the publication
  is removed.

### ALTER SUBSCRIPTION SET PUBLICATION compacts duplicate publication names

- Reproducer: `TestSubscriptionSetPublicationRejectsDuplicatePublicationRepro`
  in `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionSetPublicationRejectsDuplicatePublicationRepro -count=1`.
- Expected PostgreSQL behavior: duplicate publication names in `ALTER
  SUBSCRIPTION ... SET PUBLICATION` are rejected and the previous publication
  list is preserved.
- Observed Doltgres behavior: the duplicate list is accepted, compacted, and
  persisted over the previous publication list.

### CREATE SUBSCRIPTION accepts invalid origin values

- Reproducer: `TestSubscriptionCreateRejectsInvalidOriginOptionRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionCreateRejectsInvalidOriginOptionRepro -count=1`.
- Expected PostgreSQL behavior: `origin` values are validated and unknown values
  are rejected.
- Observed Doltgres behavior: invalid `origin` values are accepted for
  metadata-only subscriptions.

### CREATE SUBSCRIPTION accepts invalid password_required values

- Reproducer: `TestSubscriptionCreateRejectsInvalidPasswordRequiredOptionRepro`
  in `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionCreateRejectsInvalidPasswordRequiredOptionRepro -count=1`.
- Expected PostgreSQL behavior: `password_required` is parsed as a boolean
  subscription option and invalid values are rejected.
- Observed Doltgres behavior: invalid `password_required` values are accepted
  for metadata-only subscriptions.

### CREATE SUBSCRIPTION accepts invalid run_as_owner values

- Reproducer: `TestSubscriptionCreateRejectsInvalidRunAsOwnerOptionRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSubscriptionCreateRejectsInvalidRunAsOwnerOptionRepro -count=1`.
- Expected PostgreSQL behavior: `run_as_owner` is parsed as a boolean
  subscription option and invalid values are rejected.
- Observed Doltgres behavior: invalid `run_as_owner` values are accepted for
  metadata-only subscriptions.

### DROP SUBSCRIPTION with a slot is allowed inside a transaction

- Reproducer: `TestDropSubscriptionWithSlotInsideTransactionRejectedRepro` in
  `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDropSubscriptionWithSlotInsideTransactionRejectedRepro -count=1`.
- Expected PostgreSQL behavior: dropping a subscription that still has a
  replication slot name is rejected inside a transaction block.
- Observed Doltgres behavior: the slot-backed subscription can be dropped
  inside a transaction.

### DROP SUBSCRIPTION removes catalog metadata when remote slot cleanup fails

- Reproducer: `TestDropSubscriptionWithMissingRemoteSlotPreservesCatalogRepro`
  in `testing/go/subscription_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDropSubscriptionWithMissingRemoteSlotPreservesCatalogRepro -count=1`.
- Expected PostgreSQL behavior: if dropping the publisher replication slot
  fails, `DROP SUBSCRIPTION` errors and preserves the `pg_subscription` row.
- Observed Doltgres behavior: the slot-backed subscription is removed from the
  local catalog without any remote slot cleanup.

### ALTER SUBSCRIPTION does not require subscription ownership

- Reproducer: `TestAlterSubscriptionRequiresOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAlterSubscriptionRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: altering a subscription requires ownership of
  that subscription.
- Observed Doltgres behavior: a normal role can rename
  `subscription_alter_sub`.

### DROP SUBSCRIPTION does not require subscription ownership

- Reproducer: `TestDropSubscriptionRequiresOwnershipRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestDropSubscriptionRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: dropping a subscription requires ownership of
  that subscription.
- Observed Doltgres behavior: a normal role can drop
  `subscription_drop_sub`.

### CREATE TABLE by non-superuser does not set relation ownership

- Reproducer: `TestCreateTableOwnerUpdatesCatalogRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateTableOwnerUpdatesCatalogRepro -count=1`.
- Expected PostgreSQL behavior: when `table_catalog_creator` creates
  `created_table_catalog`, `pg_class.relowner` resolves to
  `table_catalog_creator`.
- Observed Doltgres behavior: the table is created, but relation ownership
  remains `postgres`.

### Table owners cannot use tables they created

- Reproducer: `TestTableOwnerCanUseCreatedTableRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTableOwnerCanUseCreatedTableRepro -count=1`.
- Expected PostgreSQL behavior: when a role creates a table, it owns that table
  and can insert into and select from it without explicit table grants.
- Observed Doltgres behavior: the creating role can create
  `owner_created_table`, but `INSERT` and `SELECT` on that table both fail with
  `permission denied for table owner_created_table`.

### CREATE SEQUENCE by non-superuser does not set relation ownership

- Reproducer: `TestCreateSequenceOwnerUpdatesCatalogRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateSequenceOwnerUpdatesCatalogRepro -count=1`.
- Expected PostgreSQL behavior: when `sequence_catalog_creator` creates
  `created_sequence_catalog`, `pg_class.relowner` resolves to
  `sequence_catalog_creator`.
- Observed Doltgres behavior: the sequence is created, but relation ownership
  remains `postgres`.

### CREATE VIEW by non-superuser does not set relation ownership

- Reproducer: `TestCreateViewOwnerUpdatesCatalogRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateViewOwnerUpdatesCatalogRepro -count=1`.
- Expected PostgreSQL behavior: when `view_catalog_creator` creates
  `created_view_catalog`, `pg_class.relowner` resolves to
  `view_catalog_creator`.
- Observed Doltgres behavior: the view is created, but relation ownership
  remains `postgres`.

### View owners cannot use views they created

- Reproducer: `TestViewOwnerCanUseCreatedViewRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestViewOwnerCanUseCreatedViewRepro -count=1`.
- Expected PostgreSQL behavior: when a role creates a view, it owns that view
  and can select from it without an explicit view grant.
- Observed Doltgres behavior: the creating role can create
  `owner_created_view`, but `SELECT` from that view fails with
  `permission denied for table owner_created_view`.

### CREATE MATERIALIZED VIEW by non-superuser does not set relation ownership

- Reproducer: `TestCreateMaterializedViewOwnerUpdatesCatalogRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateMaterializedViewOwnerUpdatesCatalogRepro -count=1`.
- Expected PostgreSQL behavior: when `materialized_view_catalog_creator`
  creates `created_materialized_view_catalog`, `pg_class.relowner` resolves to
  `materialized_view_catalog_creator`.
- Observed Doltgres behavior: the materialized view is created, but relation
  ownership remains `postgres`.

### Materialized view owners cannot use materialized views they created

- Reproducer: `TestMaterializedViewOwnerCanUseCreatedMaterializedViewRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestMaterializedViewOwnerCanUseCreatedMaterializedViewRepro -count=1`.
- Expected PostgreSQL behavior: when a role creates a materialized view, it owns
  that materialized view and can read or refresh it without explicit grants on
  the materialized view itself.
- Observed Doltgres behavior: the creating role can create
  `owner_created_materialized_view`, but both `SELECT` and
  `REFRESH MATERIALIZED VIEW` fail with `permission denied for table
  owner_created_materialized_view`.

### CREATE SCHEMA by non-superuser does not set schema ownership

- Reproducer: `TestCreateSchemaOwnerUpdatesCatalogRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateSchemaOwnerUpdatesCatalogRepro -count=1`.
- Expected PostgreSQL behavior: when `schema_catalog_creator` creates
  `created_schema_catalog`, `pg_namespace.nspowner` resolves to
  `schema_catalog_creator`.
- Observed Doltgres behavior: the schema is created, but schema ownership
  remains `postgres`.

### Schema owners cannot use schemas they created

- Reproducer: `TestSchemaOwnerCanUseCreatedSchemaRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSchemaOwnerCanUseCreatedSchemaRepro -count=1`.
- Expected PostgreSQL behavior: when a role creates a schema, it owns that
  schema and can create objects inside it.
- Observed Doltgres behavior: the creating role can create
  `owner_created_schema`, but creating a table inside that schema fails with
  `permission denied for schema owner_created_schema`.

### CREATE TYPE by non-superuser does not set type ownership

- Reproducer: `TestCreateTypeOwnerUpdatesCatalogRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateTypeOwnerUpdatesCatalogRepro -count=1`.
- Expected PostgreSQL behavior: when `type_catalog_creator` creates
  `created_enum_catalog`, `pg_type.typowner` resolves to
  `type_catalog_creator`.
- Observed Doltgres behavior: the type is created, but type ownership remains
  `postgres`.

### CREATE DOMAIN by non-superuser does not set domain ownership

- Reproducer: `TestCreateDomainOwnerUpdatesCatalogRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateDomainOwnerUpdatesCatalogRepro -count=1`.
- Expected PostgreSQL behavior: when `domain_catalog_creator` creates
  `created_domain_catalog`, `pg_type.typowner` resolves to
  `domain_catalog_creator`.
- Observed Doltgres behavior: the domain is created, but domain ownership
  remains `postgres`.

### ALTER TABLE OWNER TO does not update relation ownership

- Reproducer: `TestAlterTableOwnerUpdatesCatalogRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAlterTableOwnerUpdatesCatalogRepro -count=1`.
- Expected PostgreSQL behavior: after `ALTER TABLE owned_catalog OWNER TO
  new_owner`, `pg_class.relowner` resolves to `new_owner`.
- Observed Doltgres behavior: the statement is accepted but relation ownership
  remains `postgres`, so ownership-sensitive authorization and catalog
  introspection are inconsistent with the accepted DDL.

### ALTER TABLE OWNER TO does not transfer table owner privileges

- Reproducer: `TestAlterTableOwnerCanUseTransferredTableRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestAlterTableOwnerCanUseTransferredTableRepro -count=1`.
- Expected PostgreSQL behavior: after `ALTER TABLE transferred_table_runtime
  OWNER TO transferred_table_owner`, `transferred_table_owner` can insert into
  and select from the table without an explicit table grant.
- Observed Doltgres behavior: the accepted owner transfer does not grant owner
  authority at runtime; `INSERT INTO transferred_table_runtime` and `SELECT ...
  FROM transferred_table_runtime` fail with `permission denied for table
  transferred_table_runtime`.

### CREATE DATABASE OWNER does not update database ownership

- Reproducer: `TestCreateDatabaseOwnerUpdatesCatalogRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateDatabaseOwnerUpdatesCatalogRepro -count=1`.
- Expected PostgreSQL behavior: after `CREATE DATABASE owned_database_catalog
  OWNER = db_catalog_owner`, `pg_database.datdba` resolves to
  `db_catalog_owner`.
- Observed Doltgres behavior: the statement is accepted, but
  `pg_get_userbyid(datdba)` returns `unknown (OID=0)`.

### CREATE DATABASE OWNER does not require an existing owner role

- Reproducer: `TestCreateDatabaseOwnerRequiresExistingRoleRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateDatabaseOwnerRequiresExistingRoleRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE DATABASE ... OWNER = role` fails when
  `role` does not exist.
- Observed Doltgres behavior: `CREATE DATABASE missing_owner_database OWNER =
  missing_database_owner` succeeds even though the requested owner role does
  not exist.

### ALTER DATABASE OWNER TO does not update database ownership

- Reproducer: `TestAlterDatabaseOwnerUpdatesCatalogRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAlterDatabaseOwnerUpdatesCatalogRepro -count=1`.
- Expected PostgreSQL behavior: after `ALTER DATABASE
  alter_owned_database_catalog OWNER TO db_alter_catalog_owner`,
  `pg_database.datdba` resolves to `db_alter_catalog_owner`.
- Observed Doltgres behavior: the statement is accepted, but
  `pg_get_userbyid(datdba)` returns `unknown (OID=0)`.

### ALTER OWNER TO accepts missing roles for relations, sequences, and schemas

- Reproducer: `TestAlterOwnerRequiresExistingRoleRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterOwnerRequiresExistingRoleRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE`, `ALTER VIEW`, `ALTER SEQUENCE`,
  and `ALTER SCHEMA ... OWNER TO role` reject missing target roles before
  changing ownership metadata.
- Observed Doltgres behavior: all four statements succeed with no error for
  `missing_alter_owner`, even though that role does not exist.

### DROP OWNED BY is rejected before revoking granted privileges

- Reproducer: `TestDropOwnedRevokesGrantedPrivilegesRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDropOwnedRevokesGrantedPrivilegesRepro -count=1`.
- Expected PostgreSQL behavior: `DROP OWNED BY drop_owned_grantee` revokes
  privileges granted to that role, including table `SELECT` privileges on
  objects the role does not own.
- Observed Doltgres behavior: `DROP OWNED BY drop_owned_grantee` fails during
  parsing with `at or near "owned": syntax error (SQLSTATE 42601)`, and the
  grantee can still read `drop_owned_grants`.

### REASSIGN OWNED BY is rejected

- Reproducer: `TestReassignOwnedByEmptyRoleRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestReassignOwnedByEmptyRoleRepro -count=1`.
- Expected PostgreSQL behavior: `REASSIGN OWNED BY reassign_empty_old TO
  reassign_empty_new` succeeds even if the source role currently owns no
  objects.
- Observed Doltgres behavior: the statement fails during parsing with `at or
  near "reassign": syntax error (SQLSTATE 42601)`.

### ALTER VIEW OWNER TO does not update relation ownership

- Reproducer: `TestAlterViewOwnerUpdatesCatalogRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAlterViewOwnerUpdatesCatalogRepro -count=1`.
- Expected PostgreSQL behavior: after `ALTER VIEW owned_view_catalog OWNER TO
  view_owner`, `pg_class.relowner` resolves to `view_owner`.
- Observed Doltgres behavior: the statement is accepted, but relation ownership
  remains `postgres`.

### ALTER VIEW OWNER TO does not transfer view owner privileges

- Reproducer: `TestAlterViewOwnerCanUseTransferredViewRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestAlterViewOwnerCanUseTransferredViewRepro -count=1`.
- Expected PostgreSQL behavior: after `ALTER VIEW transferred_view_runtime
  OWNER TO transferred_view_owner`, `transferred_view_owner` can select from
  the view without an explicit view grant.
- Observed Doltgres behavior: the accepted owner transfer does not grant owner
  authority at runtime; selecting from `transferred_view_runtime` fails with
  `permission denied for table transferred_view_runtime`.

### ALTER MATERIALIZED VIEW OWNER TO does not update relation ownership

- Reproducer: `TestAlterMaterializedViewOwnerUpdatesCatalogRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestAlterMaterializedViewOwnerUpdatesCatalogRepro -count=1`.
- Expected PostgreSQL behavior: after `ALTER MATERIALIZED VIEW
  owned_materialized_view_catalog OWNER TO materialized_view_owner`,
  `pg_class.relowner` resolves to `materialized_view_owner`.
- Observed Doltgres behavior: the statement fails with `ALTER MATERIALIZED VIEW
  command is not yet supported`, so valid ownership-transfer DDL cannot update
  materialized-view ownership.

### ALTER SEQUENCE OWNER TO does not update relation ownership

- Reproducer: `TestAlterSequenceOwnerUpdatesCatalogRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAlterSequenceOwnerUpdatesCatalogRepro -count=1`.
- Expected PostgreSQL behavior: after `ALTER SEQUENCE owned_sequence_catalog
  OWNER TO sequence_owner`, `pg_class.relowner` resolves to `sequence_owner`.
- Observed Doltgres behavior: the statement is accepted, but sequence ownership
  remains `postgres`.

### ALTER SEQUENCE OWNER TO does not transfer sequence owner privileges

- Reproducer: `TestAlterSequenceOwnerCanUseTransferredSequenceRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestAlterSequenceOwnerCanUseTransferredSequenceRepro
  -count=1`.
- Expected PostgreSQL behavior: after `ALTER SEQUENCE
  transferred_sequence_runtime OWNER TO transferred_sequence_owner`,
  `transferred_sequence_owner` can call `nextval` without an explicit sequence
  grant.
- Observed Doltgres behavior: the accepted owner transfer does not grant owner
  authority at runtime; `SELECT nextval('transferred_sequence_runtime')` fails
  with `permission denied for sequence transferred_sequence_runtime`.

### ALTER SCHEMA OWNER TO does not update schema ownership

- Reproducer: `TestAlterSchemaOwnerUpdatesCatalogRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAlterSchemaOwnerUpdatesCatalogRepro -count=1`.
- Expected PostgreSQL behavior: after `ALTER SCHEMA owned_schema_catalog OWNER
  TO schema_owner`, `pg_namespace.nspowner` resolves to `schema_owner`.
- Observed Doltgres behavior: the statement is accepted, but schema ownership
  remains `postgres`.

### ALTER SCHEMA OWNER TO does not transfer schema owner privileges

- Reproducer: `TestAlterSchemaOwnerCanUseTransferredSchemaRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestAlterSchemaOwnerCanUseTransferredSchemaRepro -count=1`.
- Expected PostgreSQL behavior: after `ALTER SCHEMA transferred_schema_runtime
  OWNER TO transferred_schema_owner`, `transferred_schema_owner` can create
  objects in that schema without an explicit schema grant.
- Observed Doltgres behavior: the accepted owner transfer does not grant owner
  authority at runtime; `CREATE TABLE
  transferred_schema_runtime.owned_schema_table` fails with `permission denied
  for schema transferred_schema_runtime`.

### CREATE SCHEMA AUTHORIZATION does not set schema ownership

- Reproducer: `TestCreateSchemaAuthorizationUpdatesCatalogRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateSchemaAuthorizationUpdatesCatalogRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE SCHEMA AUTHORIZATION schema_auth_owner`
  creates a schema owned by `schema_auth_owner`.
- Observed Doltgres behavior: the schema is created, but
  `pg_namespace.nspowner` still resolves to `postgres`.

### CREATE SCHEMA AUTHORIZATION does not require an existing role

- Reproducer: `TestCreateSchemaAuthorizationRequiresExistingRoleRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateSchemaAuthorizationRequiresExistingRoleRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE SCHEMA AUTHORIZATION role` fails when
  `role` does not exist.
- Observed Doltgres behavior: `CREATE SCHEMA AUTHORIZATION missing_schema_owner`
  succeeds and creates a schema named for the nonexistent role.

### CREATE SCHEMA AUTHORIZATION does not require target-role membership

- Reproducer: `TestCreateSchemaAuthorizationRequiresTargetRoleMembershipRepro`
  in `testing/go/schema_privilege_guard_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateSchemaAuthorizationRequiresTargetRoleMembershipRepro
  -count=1`.
- Expected PostgreSQL behavior: a normal role with database `CREATE` privilege
  cannot create a schema owned by a different target role unless it is allowed
  to act as that target role.
- Observed Doltgres behavior: `schema_auth_actor` can run `CREATE SCHEMA
  unauthorized_schema_auth AUTHORIZATION schema_auth_target` even though it is
  not a member of, or otherwise allowed to act as, `schema_auth_target`.

### CREATE SCHEMA allows reserved `dolt_` namespace names

- Reproducer: `TestCreateSchemaRejectsDoltReservedNamespaceRepro` in
  `testing/go/schema_privilege_guard_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateSchemaRejectsDoltReservedNamespaceRepro -count=1`.
- Expected Doltgres behavior: user-created schemas must not occupy the
  reserved `dolt_` namespace; `CREATE SCHEMA dolt_reserved_schema` should fail
  with `invalid schema name`.
- Observed Doltgres behavior: the statement succeeds, allowing ordinary schema
  metadata to use names reserved for Dolt system/internal namespaces.

### ALTER TYPE OWNER TO does not update type ownership

- Reproducer: `TestAlterTypeOwnerUpdatesCatalogRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAlterTypeOwnerUpdatesCatalogRepro -count=1`.
- Expected PostgreSQL behavior: after `ALTER TYPE owned_enum_catalog OWNER TO
  type_owner`, `pg_type.typowner` resolves to `type_owner`.
- Observed Doltgres behavior: the statement is accepted, but type ownership
  remains `postgres`.

### ALTER DOMAIN OWNER TO does not update domain ownership

- Reproducer: `TestAlterDomainOwnerUpdatesCatalogRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAlterDomainOwnerUpdatesCatalogRepro -count=1`.
- Expected PostgreSQL behavior: after `ALTER DOMAIN owned_domain_catalog OWNER
  TO domain_owner`, `pg_type.typowner` resolves to `domain_owner`.
- Observed Doltgres behavior: the statement is accepted, but domain ownership
  remains `postgres`.

### Created functions lack visible pg_proc ownership rows

- Reproducer: `TestFunctionOwnerCatalogEntryRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestFunctionOwnerCatalogEntryRepro -count=1`.
- Expected PostgreSQL behavior: after creating a SQL function and altering its
  owner, `pg_proc.proowner` resolves to the requested owner.
- Observed Doltgres behavior: the function is accepted, but the `pg_proc` query
  for that function returns no row.

### Function owners cannot execute functions they created

- Reproducer: `TestFunctionOwnerCanExecuteCreatedFunctionRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestFunctionOwnerCanExecuteCreatedFunctionRepro -count=1`.
- Expected PostgreSQL behavior: a role that creates a function owns it and can
  execute it without an explicit `GRANT EXECUTE`.
- Observed Doltgres behavior: the creating role can create
  `owner_created_function`, but `SELECT owner_created_function()` fails with
  `permission denied for routine owner_created_function`.

### Created procedures lack visible pg_proc ownership rows

- Reproducer: `TestProcedureOwnerCatalogEntryRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestProcedureOwnerCatalogEntryRepro -count=1`.
- Expected PostgreSQL behavior: after creating a procedure and altering its
  owner, `pg_proc.proowner` resolves to the requested owner.
- Observed Doltgres behavior: the procedure is accepted, but the `pg_proc`
  query for that procedure returns no row.

### Procedure owners cannot call procedures they created

- Reproducer: `TestProcedureOwnerCanCallCreatedProcedureRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestProcedureOwnerCanCallCreatedProcedureRepro -count=1`.
- Expected PostgreSQL behavior: a role that creates a procedure owns it and can
  call it without an explicit `GRANT EXECUTE`.
- Observed Doltgres behavior: the creating role can create
  `owner_created_procedure`, but `CALL owner_created_procedure()` fails with
  `permission denied for routine owner_created_procedure`.

### Created extensions record postgres as owner

- Reproducer: `TestCreateExtensionOwnerUpdatesCatalogRepro` in
  `testing/go/ownership_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateExtensionOwnerUpdatesCatalogRepro -count=1`.
- Expected PostgreSQL behavior: when a non-superuser with the required database
  and schema privileges installs a trusted extension, `pg_extension.extowner`
  resolves to the installing role.
- Observed Doltgres behavior: `extension_catalog_creator` can create `hstore`,
  but `pg_get_userbyid(extowner)` reports `postgres`.

### CALL drops OUT and INOUT procedure result rows

- Reproducer: `TestProcedureOutArgumentsReturnRowsRepro` in
  `testing/go/procedure_argument_modes_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestProcedureOutArgumentsReturnRowsRepro -count=1`.
- Expected PostgreSQL behavior: procedures declared with `INOUT` or `OUT`
  arguments return a result row from `CALL`; for example
  `CALL proc_inout_value(5)` returns `12`, and
  `CALL proc_out_value(7, NULL)` returns `14`.
- Observed Doltgres behavior: both `CALL` statements execute without an error
  but return an empty result set, so callers cannot observe procedure output
  arguments.

### Row-level security default-deny is not enforced

- Reproducer: `TestRowLevelSecurityDefaultDenyRepro` in
  `testing/go/row_level_security_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestRowLevelSecurityDefaultDenyRepro -count=1`.
- Expected PostgreSQL behavior: after `ALTER TABLE ... ENABLE ROW LEVEL
  SECURITY`, a non-owner with table `SELECT` privilege but no matching policy
  sees no rows.
- Observed Doltgres behavior: the non-owner sees all rows, which means accepting
  the RLS DDL silently bypasses the default-deny security boundary.

### Row-level security policies cannot be created or enforced

- Reproducer: `TestRowLevelSecuritySelectPolicyFiltersRowsRepro` in
  `testing/go/row_level_security_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestRowLevelSecuritySelectPolicyFiltersRowsRepro
  -count=1`.
- Expected PostgreSQL behavior: `CREATE POLICY ... FOR SELECT USING (...)`
  is accepted, and after `ALTER TABLE ... ENABLE ROW LEVEL SECURITY`, a granted
  non-owner only sees rows allowed by the policy expression.
- Observed Doltgres behavior: policy creation is rejected with `ERROR: at or
  near "policy": syntax error (SQLSTATE 42601)`, so explicit RLS policies
  cannot define or enforce row visibility.

### Row-level security default-deny does not block INSERT

- Reproducer: `TestRowLevelSecurityDefaultDenyInsertRepro` in
  `testing/go/row_level_security_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRowLevelSecurityDefaultDenyInsertRepro -count=1`.
- Expected PostgreSQL behavior: after `ALTER TABLE ... ENABLE ROW LEVEL
  SECURITY`, a non-owner with table `INSERT` privilege but no matching INSERT
  policy cannot insert rows.
- Observed Doltgres behavior: the granted non-owner inserts a row successfully,
  and the row persists in the table.

### Row-level security default-deny does not block UPDATE

- Reproducer: `TestRowLevelSecurityDefaultDenyUpdateRepro` in
  `testing/go/row_level_security_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRowLevelSecurityDefaultDenyUpdateRepro -count=1`.
- Expected PostgreSQL behavior: after `ALTER TABLE ... ENABLE ROW LEVEL
  SECURITY`, a non-owner with table `UPDATE` privilege but no matching UPDATE
  policy cannot see target rows for update, so no row is changed.
- Observed Doltgres behavior: the granted non-owner updates the protected row,
  and the changed value persists.

### Row-level security default-deny does not block DELETE

- Reproducer: `TestRowLevelSecurityDefaultDenyDeleteRepro` in
  `testing/go/row_level_security_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRowLevelSecurityDefaultDenyDeleteRepro -count=1`.
- Expected PostgreSQL behavior: after `ALTER TABLE ... ENABLE ROW LEVEL
  SECURITY`, a non-owner with table `DELETE` privilege but no matching DELETE
  policy cannot see target rows for deletion, so no row is removed.
- Observed Doltgres behavior: the granted non-owner deletes the protected row.

### COPY FROM bypasses row-level security

- Reproducer: `TestRowLevelSecurityBlocksCopyFromRepro` in
  `testing/go/row_level_security_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRowLevelSecurityBlocksCopyFromRepro -count=1`.
- Expected PostgreSQL behavior: `COPY table FROM STDIN` is rejected when row
  level security is enabled for the target table; callers must use `INSERT`
  instead so RLS policies can be applied.
- Observed Doltgres behavior: a non-owner with `INSERT` privilege runs `COPY
  rls_copy_secrets (id, label) FROM STDIN`, receives `COPY 1`, and the copied
  row persists.

### COPY TO ignores row-level security

- Reproducer: `TestRowLevelSecurityFiltersCopyToRepro` in
  `testing/go/row_level_security_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRowLevelSecurityFiltersCopyToRepro -count=1`.
- Expected PostgreSQL behavior: `COPY table TO STDOUT` applies row-level
  security visibility rules just like `SELECT`; with no matching policy, a
  granted non-owner copies zero rows.
- Observed Doltgres behavior: the granted non-owner receives `COPY 1` and the
  protected row is emitted through `COPY TO STDOUT`.

### Row-level security settings do not update pg_class metadata

- Reproducer: `TestRowLevelSecurityPgClassMetadataRepro` in
  `testing/go/row_level_security_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestRowLevelSecurityPgClassMetadataRepro -count=1`.
- Expected PostgreSQL behavior: after `ALTER TABLE rls_catalog_target ENABLE
  ROW LEVEL SECURITY` and `FORCE ROW LEVEL SECURITY`, `pg_class.relrowsecurity`
  and `pg_class.relforcerowsecurity` are both true.
- Observed Doltgres behavior: both statements are accepted, but both
  `pg_class` flags remain false.

### `row_security_active` cannot report active RLS state

- Reproducer: `TestRowSecurityActiveReportsPolicyStateRepro` in
  `testing/go/row_level_security_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRowSecurityActiveReportsPolicyStateRepro -count=1`.
- Expected PostgreSQL behavior: `row_security_active('rls_active_docs'::regclass)`
  returns `false` for the table owner and `true` for a granted non-owner when
  row-level security is enabled.
- Observed Doltgres behavior: the owner query fails with
  `function: 'row_security_active' not found`, while the non-owner query fails
  with `permission denied for routine row_security_active`, so clients cannot
  inspect active RLS state through the PostgreSQL helper.

### ANALYZE does not require table ownership

- Reproducer: `TestAnalyzeTableRequiresOwnershipRepro` in
  `testing/go/analyze_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestAnalyzeTableRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: running `ANALYZE` on a table requires ownership
  of that table or elevated privileges.
- Observed Doltgres behavior: a role with only schema `USAGE` can run
  `ANALYZE analyze_private` on a table owned by another role.

### VACUUM does not require table ownership

- Reproducer: `TestVacuumTableRequiresOwnershipRepro` in
  `testing/go/maintenance_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestVacuumTableRequiresOwnershipRepro -count=1`.
- Expected PostgreSQL behavior: running `VACUUM` on a table requires ownership
  of that table or an equivalent maintenance privilege.
- Observed Doltgres behavior: a role with only schema `USAGE` can run
  `VACUUM vacuum_private` on a table owned by another role.

### COPY FROM STDIN does not require INSERT privilege

- Reproducer: `TestCopyFromStdinRequiresInsertPrivilegeRepro` in
  `testing/go/copy_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCopyFromStdinRequiresInsertPrivilegeRepro -count=1`.
- Expected PostgreSQL behavior: `COPY table FROM STDIN` requires insert access
  to the copied columns and should fail for a role with only schema `USAGE`.
- Observed Doltgres behavior: `COPY copy_from_private (id, label) FROM STDIN`
  succeeds as that role and returns `COPY 1`.

### COPY FROM server files does not require server-file privilege

- Reproducer: `TestCopyFromServerFileRequiresPrivilegeRepro` in
  `testing/go/copy_server_file_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCopyFromServerFileRequiresPrivilegeRepro -count=1`.
- Expected PostgreSQL behavior: `COPY table FROM '/server/path'` requires
  superuser privileges or membership in a server-file role such as
  `pg_read_server_files`.
- Observed Doltgres behavior: a normal role with table `INSERT` can read a
  server-side file through `COPY FROM` and load its contents into a table.

### COPY TO server files is rejected before server-file privilege enforcement

- Reproducer: `TestCopyToServerFileRequiresPrivilegeRepro` in
  `testing/go/copy_server_file_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCopyToServerFileRequiresPrivilegeRepro -count=1`.
- Expected PostgreSQL behavior: `COPY table TO '/server/path'` is valid syntax
  and, for an ordinary role, fails with a server-file privilege error requiring
  superuser privileges or membership in `pg_write_server_files`.
- Observed Doltgres behavior: parsing fails at the server file path with a
  syntax error before privilege enforcement, so the server-file write path does
  not match PostgreSQL semantics.

### COPY FROM PROGRAM is rejected before server-program privilege enforcement

- Reproducer: `TestCopyFromProgramRequiresPrivilegeRepro` in
  `testing/go/copy_server_file_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCopyFromProgramRequiresPrivilegeRepro -count=1`.
- Expected PostgreSQL behavior: `COPY table FROM PROGRAM '...'` is valid syntax
  and, for an ordinary role, fails with a server-program privilege error
  requiring superuser privileges or membership in `pg_execute_server_program`.
- Observed Doltgres behavior: parsing fails at `PROGRAM` with a syntax error
  before privilege enforcement.

### COPY TO PROGRAM is rejected before server-program privilege enforcement

- Reproducer: `TestCopyToProgramRequiresPrivilegeRepro` in
  `testing/go/copy_server_file_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCopyToProgramRequiresPrivilegeRepro -count=1`.
- Expected PostgreSQL behavior: `COPY table TO PROGRAM '...'` is valid syntax
  and, for an ordinary role, fails with a server-program privilege error
  requiring superuser privileges or membership in `pg_execute_server_program`.
- Observed Doltgres behavior: parsing fails at `PROGRAM` with a syntax error
  before privilege enforcement.

### SECURITY DEFINER functions do not run with owner privileges

- Reproducer: `TestSecurityDefinerFunctionUsesOwnerPrivilegesRepro` in
  `testing/go/security_definer_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestSecurityDefinerFunctionUsesOwnerPrivilegesRepro -count=1`.
- Expected PostgreSQL behavior: a `SECURITY DEFINER` SQL function executes with
  the privileges of the function owner, so a grantee with `EXECUTE` can read
  data that only the owner can access through the function body.
- Observed Doltgres behavior: the function and `GRANT EXECUTE` statements are
  accepted, but the grantee still receives `permission denied for table
  definer_private`.

### SECURITY DEFINER procedures do not run with owner privileges

- Reproducer: `TestSecurityDefinerProcedureUsesOwnerPrivilegesRepro` in
  `testing/go/security_definer_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestSecurityDefinerProcedureUsesOwnerPrivilegesRepro -count=1`.
- Expected PostgreSQL behavior: a `SECURITY DEFINER` procedure executes SQL
  statements with the privileges of the procedure owner, so a grantee with
  `EXECUTE` can perform the procedure body's write without direct table
  privileges.
- Observed Doltgres behavior: the procedure and `GRANT EXECUTE` statements are
  accepted, but `CALL definer_proc_insert()` still receives `permission denied
  for table definer_proc_private`.

### CTE SELECT checks privileges on the CTE alias

- Reproducer: `TestCteSelectUsesUnderlyingTablePrivilegesRepro` in
  `testing/go/column_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCteSelectUsesUnderlyingTablePrivilegesRepro -count=1`.
- Expected PostgreSQL behavior: a role with `SELECT` on the underlying table can
  read that table through a non-recursive CTE.
- Observed Doltgres behavior: the granted role receives `permission denied for
  table visible_items`, where `visible_items` is the CTE alias rather than a
  grantable relation.

### Default security-definer views require invoker base-table privileges

- Reproducer: `TestDefaultViewGrantUsesViewOwnerPrivilegesRepro` in
  `testing/go/view_security_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run TestDefaultViewGrantUsesViewOwnerPrivilegesRepro -count=1`.
- Expected PostgreSQL behavior: a normal view uses the view owner's privileges
  for underlying relations, so a role with `USAGE` on the schema and `SELECT`
  on the view can read through it without direct `SELECT` on the base table.
- Observed Doltgres behavior: selecting from the granted view fails with
  `permission denied for table default_view_grant_base`, so default views behave
  like invoker-security views and deny valid view-grantee reads.

### ALTER VIEW cannot enable security_invoker

- Reproducer: `TestAlterViewSecurityInvokerRequiresBaseTablePrivilegesRepro`
  in `testing/go/view_security_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off
  ./testing/go -run
  TestAlterViewSecurityInvokerRequiresBaseTablePrivilegesRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER VIEW name SET (security_invoker = true)`
  is accepted and changes later reads to use the invoker's base-table
  privileges.
- Observed Doltgres behavior: `ALTER VIEW alter_invoker_view SET
  (security_invoker = true)` fails with `ALTER VIEW is not yet supported`, so a
  view cannot be converted to invoker-security mode after creation.

## Security Regression Guards

These tests cover suspicious security surfaces that currently behave correctly.
They are worth keeping, but they are not counted as found bugs.

### COPY TO STDOUT requires table SELECT privileges

- Guard: `TestCopyToStdoutRequiresSelectPrivilegeGuard` in
  `testing/go/copy_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCopyToStdoutRequiresSelectPrivilegeGuard -count=1`.
- Expected PostgreSQL behavior: `COPY table TO STDOUT` requires read access to
  every copied column and should fail for a role with only schema `USAGE`.
- Observed Doltgres behavior: the test passes and receives `permission denied`.

### COPY TO STDOUT does not honor column SELECT grants

- Reproducer: `TestCopyToStdoutAllowsColumnSelectGrantRepro` in
  `testing/go/copy_column_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCopyToStdoutAllowsColumnSelectGrantRepro -count=1`.
- Expected PostgreSQL behavior: `COPY table (id, public_value) TO STDOUT`
  succeeds when the role has `SELECT (id, public_value)` on that table, while
  unlisted columns remain protected.
- Observed Doltgres behavior: the COPY path rewrites to a `SELECT` over the
  copied columns but rejects it with `permission denied for table
  copy_column_select_private`, so column-level `SELECT` grants are not honored
  for COPY output.

### SECURITY INVOKER views require base-table privileges

- Guard: `TestSecurityInvokerViewRequiresBaseTablePrivilegesGuard` in
  `testing/go/view_security_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestSecurityInvokerViewRequiresBaseTablePrivilegesGuard -count=1`.
- Expected PostgreSQL behavior: a role with `SELECT` on a
  `security_invoker = true` view still needs `SELECT` on the underlying table.
- Observed Doltgres behavior: the test passes and receives `permission denied`.

### CREATE OR REPLACE VIEW preserves granted view privileges

- Guard: `TestCreateOrReplaceViewPreservesGrantedPrivilegesGuard` in
  `testing/go/view_security_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateOrReplaceViewPreservesGrantedPrivilegesGuard -count=1`.
- Expected PostgreSQL behavior: `CREATE OR REPLACE VIEW` keeps existing
  compatible view grants while updating the view definition.
- Observed Doltgres behavior: the test passes; a role with existing view and
  base-table privileges can read the replaced view definition.

### CREATE VIEW requires source-table SELECT privileges

- Guard: `TestCreateViewRequiresSelectOnSourceTableGuard` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateViewRequiresSelectOnSourceTableGuard -count=1`.
- Expected PostgreSQL behavior: creating a view over a table requires `SELECT`
  privilege on that source table.
- Observed Doltgres behavior: the test passes and receives `permission denied`.

### CREATE TABLE AS requires source-table SELECT privileges

- Guard: `TestCreateTableAsRequiresSelectOnSourceTableGuard` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestCreateTableAsRequiresSelectOnSourceTableGuard -count=1`.
- Expected PostgreSQL behavior: `CREATE TABLE AS SELECT ... FROM source`
  requires `SELECT` privilege on the source table, in addition to permission to
  create the destination table.
- Observed Doltgres behavior: the test passes and receives `permission denied`.

### Persistent query definitions do not honor column-level SELECT grants

- Reproducers: `TestCreateTableAsAllowsGrantedSourceColumnsRepro`,
  `TestCreateViewAllowsGrantedSourceColumnsRepro`, and
  `TestCreateMaterializedViewAllowsGrantedSourceColumnsRepro` in
  `testing/go/column_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestCreate(TableAs|View|MaterializedView)AllowsGrantedSourceColumnsRepro'
  -count=1`.
- Expected PostgreSQL behavior: `GRANT SELECT (id, public_value) ON source TO
  role` lets that role define persistent query-backed objects whose defining
  query only reads `id` and `public_value`, including `CREATE TABLE ... AS`,
  `CREATE VIEW`, and `CREATE MATERIALIZED VIEW`, while still denying ungranted
  source columns.
- Observed Doltgres behavior: the CTAS, view, and materialized-view statements
  are rejected with `permission denied for table ...` even though each query
  only reads columns covered by the role's column-level `SELECT` grant.
- Guard: `TestCreateTableAsRejectsUngrantedSourceColumnsGuard` verifies that
  ungranted source columns are still rejected and no destination table is
  created.

### SELECT INTO table creation is rejected

- Reproducer: `TestSelectIntoCreatesTableRepro` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSelectIntoCreatesTableRepro -count=1`.
- Expected PostgreSQL behavior: `SELECT ... INTO new_table FROM source` creates
  `new_table` from the query result, equivalent to a table-creation form of the
  query.
- Observed Doltgres behavior: the statement fails during parsing with `at or
  near "into": syntax error`, and the destination table is never created.

### Public schema CREATE requires an explicit grant

- Guard: `TestPublicSchemaDefaultCreateRequiresExplicitGrantGuard` in
  `testing/go/ddl_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestPublicSchemaDefaultCreateRequiresExplicitGrantGuard -count=1`.
- Expected PostgreSQL 15 behavior: ordinary login roles do not have `CREATE`
  privilege on the `public` schema by default.
- Observed Doltgres behavior: the test passes and receives `permission denied
  for schema public`.

### Column SELECT grants do not allow granted columns

- Reproducer: `TestColumnSelectGrantAllowsGrantedColumnsRepro` in
  `testing/go/column_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestColumnSelectGrantAllowsGrantedColumnsRepro -count=1`.
- Expected PostgreSQL behavior: `GRANT SELECT (id, public_value) ON table TO
  role` allows that role to read `id` and `public_value` while still denying
  ungranted columns.
- Observed Doltgres behavior: selecting only the granted columns is rejected
  with `permission denied for table column_select_allowed_private`, so
  column-level `SELECT` grants are too restrictive even for the columns they
  explicitly grant.

### Column INSERT grants do not cover other columns

- Guard: `TestColumnInsertGrantDoesNotAllowOtherColumnsGuard` in
  `testing/go/column_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  TestColumnInsertGrantDoesNotAllowOtherColumnsGuard -count=1`.
- Expected PostgreSQL behavior: a column-level `INSERT` grant permits writes
  only to the granted columns, not to every column on the table.
- Observed Doltgres behavior: the test passes and receives `permission denied`.

### Column INSERT grants do not allow granted columns

- Reproducer: `TestColumnInsertGrantAllowsGrantedColumnsRepro` in
  `testing/go/column_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestColumnInsertGrantAllowsGrantedColumnsRepro -count=1`.
- Expected PostgreSQL behavior: `GRANT INSERT (id, public_value) ON table TO
  role` allows that role to insert rows when the statement names only `id` and
  `public_value`; unmentioned columns receive their defaults or `NULL`.
- Observed Doltgres behavior: the insert is rejected with `permission denied
  for table column_insert_allowed_private`, so column-level `INSERT` grants are
  too restrictive even for the columns they explicitly grant.

### Column UPDATE grants do not allow granted columns

- Reproducer: `TestColumnUpdateGrantAllowsGrantedColumnsRepro` in
  `testing/go/column_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestColumnUpdateGrantAllowsGrantedColumnsRepro -count=1`.
- Expected PostgreSQL behavior: `GRANT UPDATE (public_value) ON table TO role`
  allows that role to update `public_value` when it also has the needed `SELECT`
  privilege for the `WHERE` columns.
- Observed Doltgres behavior: the update is rejected with `permission denied
  for table column_update_allowed_private`, so column-level `UPDATE` grants are
  too restrictive even for the columns they explicitly grant.

### DELETE predicates can read columns without SELECT privilege

- Reproducer: `TestColumnDeleteRequiresSelectForWhereColumnsRepro` in
  `testing/go/column_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestColumnDeleteRequiresSelectForWhereColumnsRepro -count=1`.
- Expected PostgreSQL behavior: a role with `DELETE` and `SELECT` only on `id`
  cannot evaluate `WHERE private_value = 'private'`, and the target row remains
  unchanged.
- Observed Doltgres behavior: the `DELETE` succeeds without `SELECT` on
  `private_value`, and the row is deleted.

### DELETE RETURNING can expose columns without SELECT privilege

- Reproducer: `TestColumnDeleteReturningRequiresSelectPrivilegeRepro` in
  `testing/go/column_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestColumnDeleteReturningRequiresSelectPrivilegeRepro -count=1`.
- Expected PostgreSQL behavior: a role with `DELETE` and `SELECT` only on `id`
  cannot run `DELETE ... RETURNING private_value`, and the statement does not
  delete the row.
- Observed Doltgres behavior: the `DELETE ... RETURNING private_value`
  succeeds without `SELECT` on `private_value`, exposing the value and deleting
  the row.

### Schema DDL enforces create/drop privileges

- Guards: `TestCreateSchemaRequiresCreatePrivilegeGuard` and
  `TestDropSchemaRequiresOwnershipGuard` in
  `testing/go/schema_privilege_guard_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run
  'Test(CreateSchemaRequiresCreatePrivilegeGuard|DropSchemaRequiresOwnershipGuard)'
  -count=1`.
- Expected PostgreSQL behavior: a normal login role cannot create schemas
  without database `CREATE` privilege and cannot drop schemas owned by another
  role.
- Observed Doltgres behavior: both tests pass and receive `permission denied`.

### Logical replication omits the old key for primary-key updates

- Reproducer: `TestLogicalReplicationSourcePrimaryKeyUpdateIncludesOldKeyTupleRepro`
  in `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationSourcePrimaryKeyUpdateIncludesOldKeyTupleRepro
  -count=1`.
- Expected PostgreSQL behavior: under default replica identity, a logical
  replication UPDATE that changes the primary key includes an old-key tuple
  (`UpdateMessageTupleTypeKey`) with the previous key value, plus the new row
  tuple. Consumers need the old key to move or delete the prior downstream row.
- Observed Doltgres behavior: the UPDATE message has `OldTupleType` set to
  `0x0` instead of `0x4b` (`K`), so the old key tuple is absent while the new
  tuple contains the changed key.

### Publication column lists can omit replica identity for updates

- Reproducer: `TestPublicationColumnListRequiresReplicaIdentityForUpdatesRepro`
  in `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationColumnListRequiresReplicaIdentityForUpdatesRepro
  -count=1`.
- Expected PostgreSQL behavior: insert-only publications may use a column list
  that omits replica-identity columns, but publications that include UPDATE or
  DELETE must include the replica identity so logical replication consumers can
  locate changed or removed rows.
- Observed Doltgres behavior: the insert-only publication is accepted, but
  `CREATE PUBLICATION ... FOR TABLE publication_identity_columns (label)
  WITH (publish = 'update')` also succeeds even though `label` omits the
  primary-key replica identity column `id`.

### Publication column lists accept duplicate columns

- Reproducer: `TestPublicationColumnListRejectsDuplicateColumnsRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationColumnListRejectsDuplicateColumnsRepro -count=1`.
- Expected PostgreSQL behavior: duplicate column names in a publication column
  list are rejected.
- Observed Doltgres behavior: `CREATE PUBLICATION ... FOR TABLE t (id, id)`
  succeeds and stores invalid publication metadata.

### ALTER PUBLICATION ADD TABLE accepts duplicate column-list entries

- Reproducer: `TestPublicationAddTableColumnListRejectsDuplicateColumnsRepro`
  in `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationAddTableColumnListRejectsDuplicateColumnsRepro -count=1`.
- Expected PostgreSQL behavior: duplicate column names are rejected and the
  table is not added to the publication.
- Observed Doltgres behavior: the invalid table membership is accepted and
  persisted.

### ALTER PUBLICATION SET TABLE accepts duplicate column-list entries

- Reproducer: `TestPublicationSetTableColumnListRejectsDuplicateColumnsRepro`
  in `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationSetTableColumnListRejectsDuplicateColumnsRepro -count=1`.
- Expected PostgreSQL behavior: duplicate column names are rejected and the
  publication's table membership is preserved.
- Observed Doltgres behavior: the invalid table membership is accepted and
  persisted.

### CREATE PUBLICATION rejects duplicate plain table entries

- Reproducer: `TestPublicationAllowsDuplicatePlainTablesRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationAllowsDuplicatePlainTablesRepro -count=1`.
- Expected PostgreSQL behavior: redundant duplicate table entries without row
  filters or column lists are accepted and normalized to one publication
  relation.
- Observed Doltgres behavior: `CREATE PUBLICATION ... FOR TABLE t, t` is
  rejected before the publication is created.

### ALTER PUBLICATION ADD TABLE rejects duplicate plain table entries

- Reproducer: `TestPublicationAddDuplicatePlainTablesRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationAddDuplicatePlainTablesRepro -count=1`.
- Expected PostgreSQL behavior: redundant duplicate table entries without row
  filters or column lists are accepted by `ALTER PUBLICATION ... ADD TABLE` and
  normalized to one relation membership.
- Observed Doltgres behavior: the duplicate table list is rejected and no
  relation membership is added.

### ALTER PUBLICATION SET TABLE rejects duplicate plain table entries

- Reproducer: `TestPublicationSetDuplicatePlainTablesRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationSetDuplicatePlainTablesRepro -count=1`.
- Expected PostgreSQL behavior: redundant duplicate table entries without row
  filters or column lists are accepted by `ALTER PUBLICATION ... SET TABLE` and
  normalized to one relation membership.
- Observed Doltgres behavior: the duplicate table list is rejected and the
  publication remains without relation membership.

### CREATE PUBLICATION accepts duplicate publish options

- Reproducer: `TestPublicationCreateRejectsDuplicatePublishOptionRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationCreateRejectsDuplicatePublishOptionRepro -count=1`.
- Expected PostgreSQL behavior: duplicate publication option names are rejected
  with `conflicting or redundant options` and no publication is created.
- Observed Doltgres behavior: `CREATE PUBLICATION ... WITH (publish='insert',
  publish='update')` succeeds after the duplicate option list is collapsed.

### CREATE PUBLICATION accepts duplicate publish_via_partition_root options

- Reproducer:
  `TestPublicationCreateRejectsDuplicatePublishViaRootOptionRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationCreateRejectsDuplicatePublishViaRootOptionRepro
  -count=1`.
- Expected PostgreSQL behavior: duplicate `publish_via_partition_root` options
  are rejected and no publication is created.
- Observed Doltgres behavior: the duplicate option list is accepted after one
  value is silently kept.

### ALTER PUBLICATION accepts duplicate publish options

- Reproducer: `TestPublicationAlterRejectsDuplicatePublishOptionRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationAlterRejectsDuplicatePublishOptionRepro -count=1`.
- Expected PostgreSQL behavior: duplicate `publish` options in `ALTER
  PUBLICATION ... SET` are rejected and the previous publish flags are
  preserved.
- Observed Doltgres behavior: the duplicate option list is accepted and the
  publication flags are rewritten using the silently kept value.

### ALTER PUBLICATION accepts duplicate publish_via_partition_root options

- Reproducer:
  `TestPublicationAlterRejectsDuplicatePublishViaRootOptionRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationAlterRejectsDuplicatePublishViaRootOptionRepro
  -count=1`.
- Expected PostgreSQL behavior: duplicate `publish_via_partition_root` options
  in `ALTER PUBLICATION ... SET` are rejected.
- Observed Doltgres behavior: the duplicate option list is accepted after one
  value is silently kept.

### CREATE PUBLICATION rejects an empty publish action list

- Reproducer: `TestPublicationAllowsEmptyPublishOptionRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationAllowsEmptyPublishOptionRepro -count=1`.
- Expected PostgreSQL behavior: `WITH (publish = '')` is accepted and stores a
  publication with all publish action flags false.
- Observed Doltgres behavior: the valid empty action list is rejected with
  `publication option "publish" requires a comma-separated action list`.

### CREATE PUBLICATION treats CURRENT_SCHEMA as a literal schema name

- Reproducer: `TestPublicationCreateSchemaCurrentSchemaResolvesSearchPathRepro`
  in `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationCreateSchemaCurrentSchemaResolvesSearchPathRepro
  -count=1`.
- Expected PostgreSQL behavior: `FOR TABLES IN SCHEMA CURRENT_SCHEMA` resolves
  `CURRENT_SCHEMA` to the active search-path schema and stores that namespace in
  `pg_publication_namespace`.
- Observed Doltgres behavior: `CURRENT_SCHEMA` is treated as a literal schema
  name, so the valid publication definition is rejected when no schema named
  `current_schema` exists.

### ALTER PUBLICATION ADD SCHEMA treats CURRENT_SCHEMA as a literal schema name

- Reproducer: `TestPublicationAddSchemaCurrentSchemaResolvesSearchPathRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationAddSchemaCurrentSchemaResolvesSearchPathRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER PUBLICATION ... ADD TABLES IN SCHEMA
  CURRENT_SCHEMA` adds the active search-path schema to the publication.
- Observed Doltgres behavior: the valid ALTER statement is rejected because
  `CURRENT_SCHEMA` is resolved as a literal schema name.

### ALTER PUBLICATION SET SCHEMA treats CURRENT_SCHEMA as a literal schema name

- Reproducer: `TestPublicationSetSchemaCurrentSchemaResolvesSearchPathRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationSetSchemaCurrentSchemaResolvesSearchPathRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER PUBLICATION ... SET TABLES IN SCHEMA
  CURRENT_SCHEMA` replaces schema membership with the active search-path schema.
- Observed Doltgres behavior: the valid ALTER statement is rejected because
  `CURRENT_SCHEMA` is resolved as a literal schema name.

### ALTER PUBLICATION DROP SCHEMA treats CURRENT_SCHEMA as a literal schema name

- Reproducer: `TestPublicationDropSchemaCurrentSchemaResolvesSearchPathRepro`
  in `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationDropSchemaCurrentSchemaResolvesSearchPathRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER PUBLICATION ... DROP TABLES IN SCHEMA
  CURRENT_SCHEMA` removes the active search-path schema from publication
  membership.
- Observed Doltgres behavior: the valid ALTER statement is rejected, leaving
  the original schema membership in place.

### CREATE PUBLICATION accepts column lists with schema publications

- Reproducer: `TestPublicationCreateRejectsColumnListWithSchemaRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationCreateRejectsColumnListWithSchemaRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE PUBLICATION` rejects definitions that
  mix `FOR TABLES IN SCHEMA` membership with a table column list, and no
  publication is created.
- Observed Doltgres behavior: the invalid definition succeeds and stores both
  schema membership and table column-list membership.

### ALTER PUBLICATION ADD TABLE accepts column lists on schema publications

- Reproducer:
  `TestPublicationAddTableRejectsColumnListWithSchemaPublicationRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationAddTableRejectsColumnListWithSchemaPublicationRepro
  -count=1`.
- Expected PostgreSQL behavior: adding a table with a publication column list
  to a publication that already contains schema membership is rejected; the
  schema membership remains and no relation membership is added.
- Observed Doltgres behavior: the invalid `ALTER PUBLICATION ... ADD TABLE`
  succeeds and persists an explicit relation membership with a column list.

### ALTER PUBLICATION ADD SCHEMA accepts schema membership beside column lists

- Reproducer: `TestPublicationAddSchemaRejectsExistingColumnListRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationAddSchemaRejectsExistingColumnListRepro -count=1`.
- Expected PostgreSQL behavior: adding schema membership to a publication that
  already contains a table column list is rejected; the existing relation
  membership remains and no schema membership is added.
- Observed Doltgres behavior: the invalid `ALTER PUBLICATION ... ADD TABLES IN
  SCHEMA` succeeds and persists schema membership beside the column-list table
  membership.

### ALTER PUBLICATION SET accepts column lists with schema publications

- Reproducer: `TestPublicationSetRejectsColumnListWithSchemaRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationSetRejectsColumnListWithSchemaRepro -count=1`.
- Expected PostgreSQL behavior: replacing publication membership with a mixed
  `FOR TABLES IN SCHEMA` and table column-list definition is rejected, leaving
  the publication empty.
- Observed Doltgres behavior: the invalid `ALTER PUBLICATION ... SET` succeeds
  and persists both schema membership and explicit table column-list membership.

### Publication column lists match quoted column names case-insensitively

- Reproducer: `TestPublicationColumnListRespectsQuotedColumnCaseRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationColumnListRespectsQuotedColumnCaseRepro -count=1`.
- Expected PostgreSQL behavior: unquoted `casecolumn` does not match a quoted
  mixed-case `"CaseColumn"` column in a publication column list.
- Observed Doltgres behavior: the unquoted lower-case name is accepted and
  resolved to the quoted mixed-case column.

### ALTER PUBLICATION SET TABLE matches quoted column names case-insensitively

- Reproducer: `TestPublicationSetTableColumnListRespectsQuotedColumnCaseRepro`
  in `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationSetTableColumnListRespectsQuotedColumnCaseRepro -count=1`.
- Expected PostgreSQL behavior: the invalid column list is rejected and the
  publication's table membership stays empty.
- Observed Doltgres behavior: the invalid column list is accepted and persisted.

### Publication column lists accept generated columns

- Reproducer: `TestPublicationColumnListRejectsGeneratedColumnsRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationColumnListRejectsGeneratedColumnsRepro -count=1`.
- Expected PostgreSQL behavior: generated columns are rejected in publication
  column lists and no publication is created.
- Observed Doltgres behavior: `CREATE PUBLICATION ... FOR TABLE t
  (id, generated_value)` succeeds, so generated columns can be stored in
  publication metadata.

### CREATE PUBLICATION accepts pg_catalog as a published schema

- Reproducer: `TestPublicationRejectsSystemSchemaRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationRejectsSystemSchemaRepro -count=1`.
- Expected PostgreSQL behavior: system schemas such as `pg_catalog` cannot be
  added to publications.
- Observed Doltgres behavior: `CREATE PUBLICATION ... FOR TABLES IN SCHEMA
  pg_catalog` succeeds and stores invalid publication schema membership.

### ALTER PUBLICATION ADD SCHEMA accepts pg_catalog as a published schema

- Reproducer: `TestPublicationAddSchemaRejectsSystemSchemaRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationAddSchemaRejectsSystemSchemaRepro -count=1`.
- Expected PostgreSQL behavior: adding `pg_catalog` to an existing publication
  is rejected and leaves publication namespace membership empty.
- Observed Doltgres behavior: the invalid system-schema membership is accepted
  and persisted.

### ALTER PUBLICATION SET SCHEMA accepts pg_catalog as a published schema

- Reproducer: `TestPublicationSetSchemaRejectsSystemSchemaRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationSetSchemaRejectsSystemSchemaRepro -count=1`.
- Expected PostgreSQL behavior: replacing schema membership with `pg_catalog` is
  rejected and leaves publication namespace membership empty.
- Observed Doltgres behavior: the invalid system-schema membership is accepted
  and persisted.

### CREATE PUBLICATION accepts pg_catalog system tables

- Reproducer: `TestPublicationRejectsSystemTableRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationRejectsSystemTableRepro -count=1`.
- Expected PostgreSQL behavior: system tables such as `pg_catalog.pg_class`
  cannot be added to publications.
- Observed Doltgres behavior: `CREATE PUBLICATION ... FOR TABLE
  pg_catalog.pg_class` succeeds and stores invalid publication table
  membership.

### ALTER PUBLICATION ADD TABLE accepts pg_catalog system tables

- Reproducer: `TestPublicationAddTableRejectsSystemTableRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationAddTableRejectsSystemTableRepro -count=1`.
- Expected PostgreSQL behavior: adding `pg_catalog.pg_class` to an existing
  publication is rejected and leaves publication relation membership empty.
- Observed Doltgres behavior: the invalid system-table membership is accepted
  and persisted.

### ALTER PUBLICATION SET TABLE accepts pg_catalog system tables

- Reproducer: `TestPublicationSetTableRejectsSystemTableRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationSetTableRejectsSystemTableRepro -count=1`.
- Expected PostgreSQL behavior: replacing table membership with a system table
  is rejected and leaves publication relation membership empty.
- Observed Doltgres behavior: the invalid system-table membership is accepted
  and persisted.

### ALTER PUBLICATION ADD TABLE accepts explicit tables on FOR ALL TABLES publications

- Reproducer: `TestPublicationAllTablesRejectsAddTableRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationAllTablesRejectsAddTableRepro -count=1`.
- Expected PostgreSQL behavior: publications defined as `FOR ALL TABLES` reject
  explicit table membership changes and keep `pg_publication_rel` empty.
- Observed Doltgres behavior: `ALTER PUBLICATION ... ADD TABLE` succeeds and
  stores explicit table membership on a `FOR ALL TABLES` publication.

### ALTER PUBLICATION SET TABLE rewrites FOR ALL TABLES publications

- Reproducer: `TestPublicationAllTablesRejectsSetTableRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationAllTablesRejectsSetTableRepro -count=1`.
- Expected PostgreSQL behavior: publications defined as `FOR ALL TABLES` reject
  replacement with an explicit table list and keep `puballtables` true.
- Observed Doltgres behavior: `ALTER PUBLICATION ... SET TABLE` succeeds,
  flips `puballtables` false, and persists explicit table membership.

### ALTER PUBLICATION ADD SCHEMA accepts explicit schemas on FOR ALL TABLES publications

- Reproducer: `TestPublicationAllTablesRejectsAddSchemaRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationAllTablesRejectsAddSchemaRepro -count=1`.
- Expected PostgreSQL behavior: publications defined as `FOR ALL TABLES` reject
  explicit schema membership changes and keep `pg_publication_namespace` empty.
- Observed Doltgres behavior: `ALTER PUBLICATION ... ADD TABLES IN SCHEMA`
  succeeds and stores explicit schema membership on a `FOR ALL TABLES`
  publication.

### ALTER PUBLICATION SET SCHEMA rewrites FOR ALL TABLES publications

- Reproducer: `TestPublicationAllTablesRejectsSetSchemaRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationAllTablesRejectsSetSchemaRepro -count=1`.
- Expected PostgreSQL behavior: publications defined as `FOR ALL TABLES` reject
  replacement with an explicit schema list and keep `puballtables` true.
- Observed Doltgres behavior: `ALTER PUBLICATION ... SET TABLES IN SCHEMA`
  succeeds, flips `puballtables` false, and persists explicit schema
  membership.

### ALTER PUBLICATION DROP TABLE accepts WHERE clauses and removes membership

- Reproducer: `TestPublicationDropTableRejectsWhereClauseRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationDropTableRejectsWhereClauseRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER PUBLICATION ... DROP TABLE ... WHERE`
  is rejected because WHERE clauses are not valid when removing a table from a
  publication, and the original relation membership is preserved.
- Observed Doltgres behavior: the invalid DROP TABLE statement succeeds and
  removes the table from `pg_publication_rel`.

### Publication table membership is not updated after table rename

- Reproducer: `TestPublicationMembershipSurvivesTableRenameRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationMembershipSurvivesTableRenameRepro -count=1`.
- Expected PostgreSQL behavior: explicit publication membership follows the
  relation through `ALTER TABLE ... RENAME TO`, so `pg_publication_tables`
  exposes the renamed table and its current columns.
- Observed Doltgres behavior: the publication remains keyed to the old table
  name, so `pg_publication_tables` still reports the pre-rename relation name.

### Dropped publication table membership is inherited by a new table with the same name

- Reproducer: `TestPublicationMembershipClearedWhenTableDroppedRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationMembershipClearedWhenTableDroppedRepro -count=1`.
- Expected PostgreSQL behavior: dropping a table removes its explicit
  publication membership; a later table with the same name is not automatically
  present in `pg_publication_tables` or `pg_publication_rel`.
- Observed Doltgres behavior: the publication membership survives the drop by
  name, so a newly created same-name table is still reported as part of the
  publication.

### Dropped publication schema membership is inherited by a new schema with the same name

- Reproducer: `TestPublicationSchemaMembershipClearedWhenSchemaDroppedRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationSchemaMembershipClearedWhenSchemaDroppedRepro -count=1`.
- Expected PostgreSQL behavior: dropping a schema removes its `FOR TABLES IN
  SCHEMA` publication membership; a later schema with the same name is not
  present in `pg_publication_namespace` and its tables are not published through
  the old publication.
- Observed Doltgres behavior: the schema publication membership survives the
  drop by schema name, so a recreated schema and its tables are still reported
  as part of the publication.

### Publication column lists are not updated after column rename

- Reproducer: `TestPublicationColumnListSurvivesColumnRenameRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationColumnListSurvivesColumnRenameRepro -count=1`.
- Expected PostgreSQL behavior: publication column lists are tied to attribute
  numbers, so `ALTER TABLE ... RENAME COLUMN` exposes the renamed column in
  `pg_publication_tables.attnames` while `pg_publication_rel.prattrs` still
  points at the same attribute numbers.
- Observed Doltgres behavior: the publication column list stores the old column
  name, so after the rename `pg_publication_tables.attnames` is stale and
  `pg_publication_rel.prattrs` no longer resolves the column list.

### DROP COLUMN allows removing columns used by publication column lists

- Reproducer: `TestDropColumnUsedByPublicationColumnListRequiresCascadeRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDropColumnUsedByPublicationColumnListRequiresCascadeRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... DROP COLUMN` rejects a column
  that an explicit publication column list depends on with a publication
  dependency error, leaving both the column and publication column list intact.
- Observed Doltgres behavior: the column drop succeeds, removing the table
  column while the publication metadata still names the dropped column.

### DROP COLUMN allows removing columns used by publication row filters

- Reproducer: `TestDropColumnUsedByPublicationRowFilterRequiresCascadeRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDropColumnUsedByPublicationRowFilterRequiresCascadeRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... DROP COLUMN` rejects a column
  referenced by a publication row filter with a publication dependency error,
  leaving both the column and row filter intact.
- Observed Doltgres behavior: the column drop succeeds, removing the table
  column while the publication row filter still references the dropped column.

### Publication row filters are not updated after column rename

- Reproducer: `TestPublicationRowFilterSurvivesColumnRenameRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationRowFilterSurvivesColumnRenameRepro -count=1`.
- Expected PostgreSQL behavior: publication row filters are stored as table
  attribute expressions, so `ALTER TABLE ... RENAME COLUMN` updates the visible
  `pg_publication_tables.rowfilter` expression to the new column name.
- Observed Doltgres behavior: the row filter stores the old column name as text,
  so `pg_publication_tables.rowfilter` remains stale after the column rename.

### Publication row filters accept missing columns

- Reproducer: `TestPublicationRowFilterRejectsUnknownColumnRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationRowFilterRejectsUnknownColumnRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE PUBLICATION ... WHERE (...)` validates
  the row-filter expression against the target table and rejects references to
  columns that do not exist, leaving no publication behind.
- Observed Doltgres behavior: the publication is created with a row filter that
  references a missing column, so the invalid filter persists into publication
  metadata.

### Publication row filters match quoted columns case-insensitively

- Reproducer: `TestPublicationRowFilterRespectsQuotedColumnCaseRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationRowFilterRespectsQuotedColumnCaseRepro -count=1`.
- Expected PostgreSQL behavior: quoted identifiers preserve case in
  publication row filters, so an unquoted `casecolumn` reference does not match
  a table column declared as `"CaseColumn"` and the publication is not created.
- Observed Doltgres behavior: `CREATE PUBLICATION ... WHERE (casecolumn =
  'visible')` succeeds for a table that only has `"CaseColumn"`, and the
  invalid row filter persists into publication metadata.

### ALTER PUBLICATION ADD TABLE row filters accept missing columns

- Reproducer: `TestPublicationAddTableRowFilterRejectsUnknownColumnRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationAddTableRowFilterRejectsUnknownColumnRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER PUBLICATION ... ADD TABLE ... WHERE`
  validates the row filter against the target table, rejects missing columns,
  and leaves the publication without relation membership.
- Observed Doltgres behavior: the invalid row filter is accepted and relation
  membership is persisted.

### ALTER PUBLICATION SET TABLE row filters accept missing columns

- Reproducer: `TestPublicationSetTableRowFilterRejectsUnknownColumnRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationSetTableRowFilterRejectsUnknownColumnRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER PUBLICATION ... SET TABLE ... WHERE`
  validates the replacement row filter, rejects missing columns, and preserves
  empty publication relation membership.
- Observed Doltgres behavior: the invalid row filter is accepted and replaces
  the publication's table membership.

### Publication row filters accept volatile functions

- Reproducer: `TestPublicationRowFilterRejectsVolatileFunctionRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationRowFilterRejectsVolatileFunctionRepro -count=1`.
- Expected PostgreSQL behavior: publication row filters reject mutable functions
  such as `random()` because they make replication routing nondeterministic, and
  no publication is created.
- Observed Doltgres behavior: the volatile row filter is accepted and persisted
  in publication metadata.

### Publication row filters accept non-boolean expressions

- Reproducer: `TestPublicationRowFilterRejectsNonBooleanExpressionRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationRowFilterRejectsNonBooleanExpressionRepro -count=1`.
- Expected PostgreSQL behavior: publication row filters must be boolean
  expressions, so `WHERE (1234)` is rejected and no publication is created.
- Observed Doltgres behavior: the non-boolean row filter is accepted and
  persisted in publication metadata.

### Publication row filters accept system columns

- Reproducer: `TestPublicationRowFilterRejectsSystemColumnRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationRowFilterRejectsSystemColumnRepro -count=1`.
- Expected PostgreSQL behavior: publication row filters reject system columns
  such as `ctid` as invalid publication WHERE expressions.
- Observed Doltgres behavior: the system-column row filter is accepted and
  persisted in publication metadata.

### Publication row filters accept subqueries

- Reproducer: `TestPublicationRowFilterRejectsSubqueryRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationRowFilterRejectsSubqueryRepro -count=1`.
- Expected PostgreSQL behavior: row filters reject subqueries as invalid
  publication WHERE expressions and leave no publication behind.
- Observed Doltgres behavior: a row filter containing a subquery is accepted and
  persisted in publication metadata.

### Publication row filters accept aggregate functions

- Reproducer: `TestPublicationRowFilterRejectsAggregateRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationRowFilterRejectsAggregateRepro -count=1`.
- Expected PostgreSQL behavior: aggregate functions are rejected in publication
  row filters and no publication is created.
- Observed Doltgres behavior: a row filter containing `count(*)` is accepted and
  persisted in publication metadata.

### Publication row filters accept window functions

- Reproducer: `TestPublicationRowFilterRejectsWindowFunctionRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationRowFilterRejectsWindowFunctionRepro -count=1`.
- Expected PostgreSQL behavior: window functions are rejected in publication row
  filters and no publication is created.
- Observed Doltgres behavior: a row filter containing `row_number() OVER ()` is
  accepted and persisted in publication metadata.

### REPLICA IDENTITY USING INDEX accepts deferrable unique indexes

- Reproducer: `TestReplicaIdentityRejectsDeferrableUniqueIndexRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestReplicaIdentityRejectsDeferrableUniqueIndexRepro -count=1`.
- Expected PostgreSQL behavior: `ALTER TABLE ... REPLICA IDENTITY USING INDEX`
  rejects a deferrable unique index with `cannot use non-immediate index`, keeps
  the table at default replica identity, and does not mark the index as
  `indisreplident`.
- Observed Doltgres behavior: the deferrable unique index is accepted as replica
  identity, setting `pg_class.relreplident = 'i'` and marking the index as
  `pg_index.indisreplident`.

### REPLICA IDENTITY USING INDEX columns can be made nullable afterward

- Reproducer: `TestReplicaIdentityIndexColumnDropNotNullRejectedRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestReplicaIdentityIndexColumnDropNotNullRejectedRepro -count=1`.
- Expected PostgreSQL behavior: after `REPLICA IDENTITY USING INDEX` selects a
  unique index, `ALTER TABLE ... ALTER COLUMN indexed_column DROP NOT NULL`
  fails with `column ... is in index used as replica identity`, leaving the
  column marked `NOT NULL` and the index marked as replica identity.
- Observed Doltgres behavior: the `DROP NOT NULL` succeeds, making the replica
  identity index column nullable while the table remains configured with
  `relreplident = 'i'`.

### REPLICA IDENTITY USING INDEX is lost after table rename

- Reproducer: `TestReplicaIdentityUsingIndexSurvivesTableRenameRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestReplicaIdentityUsingIndexSurvivesTableRenameRepro -count=1`.
- Expected PostgreSQL behavior: after `ALTER TABLE ... REPLICA IDENTITY USING
  INDEX` selects a unique index, renaming the table preserves
  `pg_class.relreplident = 'i'` for the renamed relation and keeps the selected
  index marked as `pg_index.indisreplident`.
- Observed Doltgres behavior: renaming the table drops the visible replica
  identity metadata for the renamed relation; `pg_class.relreplident` falls back
  to `d` and no index is marked as `indisreplident`.

### Dropped table replica identity is inherited by a new table with the same name

- Reproducer: `TestReplicaIdentityClearedWhenTableDroppedRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestReplicaIdentityClearedWhenTableDroppedRepro -count=1`.
- Expected PostgreSQL behavior: dropping a table removes its replica identity
  metadata with the table, so a later `CREATE TABLE` with the same name starts
  with default `pg_class.relreplident = 'd'`.
- Observed Doltgres behavior: after a table is set to `REPLICA IDENTITY FULL`,
  dropped, and recreated with the same name, the new relation still reports
  `pg_class.relreplident = 'f'`, inheriting stale metadata from the dropped
  table.

### REPLICA IDENTITY USING INDEX is detached after index rename

- Reproducer: `TestReplicaIdentityUsingIndexSurvivesIndexRenameRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestReplicaIdentityUsingIndexSurvivesIndexRenameRepro -count=1`.
- Expected PostgreSQL behavior: after `REPLICA IDENTITY USING INDEX` selects a
  unique index, `ALTER INDEX ... RENAME TO ...` preserves
  `pg_class.relreplident = 'i'` and keeps `pg_index.indisreplident` attached to
  the renamed index.
- Observed Doltgres behavior: the table still reports `relreplident = 'i'`,
  but the renamed index is not marked as `indisreplident`, leaving the table's
  replica identity catalog state internally inconsistent.

### pg_relation_is_publishable returns NULL for publishable tables

- Reproducer: `TestPgRelationIsPublishableClassifiesRelationsRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgRelationIsPublishableClassifiesRelationsRepro -count=1`.
- Expected PostgreSQL behavior: `pg_relation_is_publishable` returns `true`
  for ordinary persistent tables and `false` for relations that cannot be
  published, such as views.
- Observed Doltgres behavior: the function returns `NULL` for both relation
  kinds, so publication/catalog tooling cannot distinguish a publishable table
  from excluded relations.

### Logical replication row filters omit DELETE when an UPDATE leaves the filter

- Reproducer:
  `TestLogicalReplicationSourcePublishesDeleteWhenUpdateLeavesRowFilterRepro`
  in `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationSourcePublishesDeleteWhenUpdateLeavesRowFilterRepro
  -count=1`.
- Expected PostgreSQL behavior: when a publication has a row filter such as
  `WHERE (customer_id = 42)` and an UPDATE changes a previously matching row so
  it no longer matches, logical replication publishes a DELETE for the old row
  if DELETE is included in the publication actions.
- Observed Doltgres behavior: the UPDATE succeeds, but the replication stream
  emits no DELETE message; the test times out waiting for the relation, DELETE,
  and commit messages. A subscriber would keep the previously visible row.

### Logical replication row filters omit INSERT when an UPDATE enters the filter

- Reproducer:
  `TestLogicalReplicationSourcePublishesInsertWhenUpdateEntersRowFilterRepro`
  in `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationSourcePublishesInsertWhenUpdateEntersRowFilterRepro
  -count=1`.
- Expected PostgreSQL behavior: when a publication has a row filter such as
  `WHERE (customer_id = 42)` and an UPDATE changes a previously non-matching
  row so it now matches, logical replication publishes an INSERT for the newly
  visible row if INSERT is included in the publication actions.
- Observed Doltgres behavior: the UPDATE succeeds, but the replication stream
  emits no INSERT message; the test times out waiting for the relation, INSERT,
  and commit messages. A subscriber would never learn about the newly visible
  row.

### Logical replication row filters reject range predicates during DML

- Reproducer:
  `TestLogicalReplicationSourceSupportsRangePublicationRowFilterRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationSourceSupportsRangePublicationRowFilterRepro
  -count=1`.
- Expected PostgreSQL behavior: publication row filters are ordinary immutable
  WHERE expressions; a publication such as `WHERE (customer_id > 40)` accepts
  matching publisher DML and publishes the matching row.
- Observed Doltgres behavior: `CREATE PUBLICATION ... WHERE (customer_id > 40)`
  succeeds, but the first matching `INSERT` fails with
  `ERROR: publication row filter comparison operator ">" is not supported
  (SQLSTATE XX000)`. Publisher DML can fail after a user creates a PostgreSQL
  valid publication row filter.

### Logical replication row filters do not apply PostgreSQL type coercion

- Reproducer:
  `TestLogicalReplicationSourceTypeCoercesPublicationRowFilterRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationSourceTypeCoercesPublicationRowFilterRepro
  -count=1`.
- Expected PostgreSQL behavior: publication row filters are SQL expressions;
  `WHERE (customer_id = 42.0)` matches a `BIGINT` `customer_id` value of `42`
  and publishes the row.
- Observed Doltgres behavior: the `INSERT` succeeds, but the replication stream
  emits no INSERT message; the test times out waiting for the relation, INSERT,
  and commit messages. The row-filter evaluator appears to compare textual row
  bytes (`"42"`) to the literal bytes (`"42.0"`) instead of applying
  PostgreSQL equality semantics, so a subscriber silently misses matching rows.

### Logical replication preserves publication column-list order

- Reproducer: `TestLogicalReplicationSourceColumnListUsesTableOrderRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationSourceColumnListUsesTableOrderRepro -count=1`.
- Expected PostgreSQL behavior: publication column-list order is not
  preserved. A table declared as `(id, alpha, beta)` and published as
  `(beta, alpha)` exposes and streams the published columns in table order,
  `alpha, beta`.
- Observed Doltgres behavior: the relation message preserves the textual
  publication list order and reports `beta, alpha`; the emitted tuple also
  follows that order. A subscriber can receive a row shape that does not match
  PostgreSQL's pgoutput column ordering.

### Logical replication duplicates changes for overlapping publications

- Reproducer:
  `TestLogicalReplicationSourceDoesNotDuplicateOverlappingPublicationsRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationSourceDoesNotDuplicateOverlappingPublicationsRepro
  -count=1`.
- Expected PostgreSQL behavior: when a subscriber requests two publications
  that both include the same table, one table change is published once. A
  subscriber should not receive duplicate INSERT/UPDATE/DELETE messages for the
  same row just because the table is in multiple requested publications.
- Observed Doltgres behavior: after the first expected INSERT change is read,
  another WAL `CopyData` message for the same transaction remains queued. A
  downstream consumer subscribed to both publications can apply the same change
  twice, causing duplicate inserts or repeated mutations.

### Logical replication DELETE sends a full old tuple under default replica identity

- Reproducer:
  `TestLogicalReplicationSourceDeleteUsesOldKeyForDefaultReplicaIdentityRepro`
  in `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationSourceDeleteUsesOldKeyForDefaultReplicaIdentityRepro
  -count=1`.
- Expected PostgreSQL behavior: under default replica identity, a DELETE message
  for a table with a primary key uses tuple type `K`
  (`DeleteMessageTupleTypeKey`) and includes only the replica-identity key
  columns.
- Observed Doltgres behavior: the DELETE message uses tuple type `O`
  (`DeleteMessageTupleTypeOld`) and includes the full deleted row. This does
  not match pgoutput's default replica-identity tuple shape and can expose
  non-key deleted column values to logical replication consumers.

### DELETE is allowed with REPLICA IDENTITY NOTHING in a delete publication

- Reproducer: `TestPublicationDeleteRequiresReplicaIdentityRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationDeleteRequiresReplicaIdentityRepro -count=1`.
- Expected PostgreSQL behavior: if a table has `REPLICA IDENTITY NOTHING` and
  belongs to a publication that publishes DELETE, a DELETE against that table
  fails with a replica-identity error and leaves the row intact.
- Observed Doltgres behavior: the DELETE succeeds without error and removes the
  row; the follow-up `SELECT count(*)` returns `0` instead of `1`. A publisher
  can delete rows that a logical replication subscriber cannot identify.

### UPDATE is allowed with REPLICA IDENTITY NOTHING in an update publication

- Reproducer: `TestPublicationUpdateRequiresReplicaIdentityRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationUpdateRequiresReplicaIdentityRepro -count=1`.
- Expected PostgreSQL behavior: if a table has `REPLICA IDENTITY NOTHING` and
  belongs to a publication that publishes UPDATE, an UPDATE against that table
  fails with a replica-identity error and leaves the row unchanged.
- Observed Doltgres behavior: the UPDATE succeeds without error and persists
  the changed value; the follow-up query returns `"after-update"` instead of
  `"before-update"`. A publisher can mutate rows that a logical replication
  subscriber cannot reliably identify.

### Logical replication DELETE sends a full old tuple for REPLICA IDENTITY USING INDEX

- Reproducer:
  `TestLogicalReplicationSourceDeleteUsesIndexKeyForReplicaIdentityUsingIndexRepro`
  in `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationSourceDeleteUsesIndexKeyForReplicaIdentityUsingIndexRepro
  -count=1`.
- Expected PostgreSQL behavior: for `REPLICA IDENTITY USING INDEX`, a DELETE
  message uses tuple type `K` and includes only the configured replica-identity
  index key columns.
- Observed Doltgres behavior: the DELETE message uses tuple type `O` and sends
  the full deleted row. A table configured to identify deletes by a narrow
  unique index still exposes non-identity deleted column values to logical
  replication consumers.

### Logical replication UPDATE omits the old key for REPLICA IDENTITY USING INDEX

- Reproducer:
  `TestLogicalReplicationSourceUpdateIncludesOldIndexKeyForReplicaIdentityUsingIndexRepro`
  in `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationSourceUpdateIncludesOldIndexKeyForReplicaIdentityUsingIndexRepro
  -count=1`.
- Expected PostgreSQL behavior: when `REPLICA IDENTITY USING INDEX` is set and
  an UPDATE changes that index key, the pgoutput UPDATE message includes an old
  key tuple (`K`) with the previous index value, plus the new row tuple.
- Observed Doltgres behavior: the UPDATE message has `OldTupleType` set to
  `0x0`, so the previous index key is absent while the new row contains the
  changed key. A subscriber cannot reliably locate and update the old row.

### Publication column lists can omit REPLICA IDENTITY USING INDEX columns

- Reproducer:
  `TestPublicationColumnListRequiresReplicaIdentityIndexForUpdatesRepro` in
  `testing/go/publication_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPublicationColumnListRequiresReplicaIdentityIndexForUpdatesRepro
  -count=1`.
- Expected PostgreSQL behavior: insert-only publications may project away
  replica-identity columns, but a publication that includes UPDATE or DELETE
  must include the configured `REPLICA IDENTITY USING INDEX` columns in its
  column list.
- Observed Doltgres behavior: the insert-only publication is accepted, but
  `CREATE PUBLICATION ... FOR TABLE publication_identity_index_columns (label)
  WITH (publish = 'update')` also succeeds even though `label` omits the
  configured replica identity index column `external_id`.

### Logical replication omits UPDATE ... FROM changes

- Reproducer: `TestLogicalReplicationSourcePublishesUpdateFromRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationSourcePublishesUpdateFromRepro -count=1`.
- Expected PostgreSQL behavior: `UPDATE target ... FROM source ...` mutates the
  target table and publishes an UPDATE for each changed target row when the
  target table belongs to the requested publication.
- Observed Doltgres behavior: the publisher-side UPDATE succeeds and the target
  row changes from `"before-update-from"` to `"after-update-from"`, but the
  replication stream emits no UPDATE message; the test times out waiting for
  relation, UPDATE, and commit messages. A subscriber silently misses joined
  updates.

### Logical replication publishes ON CONFLICT DO UPDATE as INSERT

- Reproducer:
  `TestLogicalReplicationSourcePublishesUpdateForOnConflictDoUpdateRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationSourcePublishesUpdateForOnConflictDoUpdateRepro
  -count=1`.
- Expected PostgreSQL behavior: `INSERT ... ON CONFLICT DO UPDATE` publishes an
  UPDATE when it updates an existing row, with the updated row tuple in the
  logical replication stream.
- Observed Doltgres behavior: the publisher row changes to `"after-upsert"`,
  but the first logical replication DML message is an `InsertMessageV2` instead
  of an `UpdateMessageV2`. A subscriber can try to insert a row that already
  exists instead of applying the intended update.

### Logical replication omits COPY FROM inserted rows

- Reproducer: `TestLogicalReplicationSourcePublishesCopyFromRowsRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationSourcePublishesCopyFromRowsRepro -count=1`.
- Expected PostgreSQL behavior: `COPY ... FROM STDIN` inserts publisher rows
  and logical replication publishes INSERT messages for rows inserted into a
  published table.
- Observed Doltgres behavior: `COPY FROM STDIN` reports one affected row and
  the publisher table contains `"copied-from-stdin"`, but the replication
  stream emits no INSERT message; the test times out waiting for relation,
  INSERT, and commit messages. Bulk-loaded rows can be missing from subscribers.

### TRUNCATE rejects multiple target tables

- Reproducer: `TestTruncateMultipleTablesRepro` in
  `testing/go/truncate_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTruncateMultipleTablesRepro -count=1`.
- Expected PostgreSQL behavior: `TRUNCATE table_a, table_b` is valid and
  truncates all listed tables in one statement.
- Observed Doltgres behavior: the statement fails with
  `ERROR: truncating multiple tables at once is not yet supported
  (SQLSTATE 0A000)`, and both tables retain their rows.

### Logical replication publishes rows rolled back to a savepoint

- Reproducer:
  `TestLogicalReplicationSourceSavepointRollbackDropsBufferedChangesRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationSourceSavepointRollbackDropsBufferedChangesRepro
  -count=1`.
- Expected PostgreSQL behavior: a row inserted after `SAVEPOINT` and then
  removed by `ROLLBACK TO SAVEPOINT` is absent from both the committed table
  state and the logical replication transaction emitted on COMMIT.
- Observed Doltgres behavior: the publisher table does not retain the rolled
  back row, but the logical replication transaction still includes
  `"51:rolled-back-savepoint"` between the two committed rows. A subscriber can
  ingest data that no longer exists in the publisher transaction.

### Logical replication publishes UPDATEs rolled back to a savepoint

- Reproducer:
  `TestLogicalReplicationSourceSavepointRollbackDropsBufferedUpdateRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationSourceSavepointRollbackDropsBufferedUpdateRepro
  -count=1`.
- Expected PostgreSQL behavior: an UPDATE performed after `SAVEPOINT` and then
  removed by `ROLLBACK TO SAVEPOINT` is absent from both the committed table
  state and the logical replication transaction emitted on COMMIT.
- Observed Doltgres behavior: the publisher row is restored to `"before-51"`,
  but the logical replication transaction still includes
  `"51:rolled-back-savepoint"` between the two committed UPDATE rows. A
  subscriber can apply an update that was rolled back on the publisher.

### Logical replication publishes DELETEs rolled back to a savepoint

- Reproducer:
  `TestLogicalReplicationSourceSavepointRollbackDropsBufferedDeleteRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationSourceSavepointRollbackDropsBufferedDeleteRepro
  -count=1`.
- Expected PostgreSQL behavior: a DELETE performed after `SAVEPOINT` and then
  removed by `ROLLBACK TO SAVEPOINT` is absent from both the committed table
  state and the logical replication transaction emitted on COMMIT.
- Observed Doltgres behavior: the publisher retains row `51`, but the logical
  replication transaction still includes `"51:rolled-back-delete"` between the
  two committed DELETE rows. A subscriber can delete a row that still exists on
  the publisher.

### Logical replication slot creation does not require a REPLICATION role

- Reproducer: `TestLogicalReplicationRequiresReplicationRoleForCreateSlotRepro`
  in `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRequiresReplicationRoleForCreateSlotRepro
  -count=1`.
- Expected PostgreSQL behavior: a connection using `replication=database` cannot
  create a logical replication slot unless the authenticated role is superuser
  or has the `REPLICATION` attribute.
- Observed Doltgres behavior: `ordinary_slot_user`, created only with `LOGIN`,
  connects with `replication=database` and successfully runs
  `CREATE_REPLICATION_SLOT ordinary_slot_user_slot LOGICAL pgoutput`; the test
  expects an error and receives nil.

### Logical replication streaming does not require a REPLICATION role

- Reproducer:
  `TestLogicalReplicationRequiresReplicationRoleForStartReplicationRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRequiresReplicationRoleForStartReplicationRepro
  -count=1`.
- Expected PostgreSQL behavior: `START_REPLICATION SLOT ... LOGICAL` requires a
  superuser or `REPLICATION` role.
- Observed Doltgres behavior: `ordinary_repl_reader`, created only with
  `LOGIN`, connects with `replication=database`, starts an existing pgoutput
  slot, and receives `public.ordinary_start_replication_items` row
  `1:visible-to-ordinary-role`.

### Logical replication slot drop does not require a REPLICATION role

- Reproducer:
  `TestLogicalReplicationRequiresReplicationRoleForDropSlotRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRequiresReplicationRoleForDropSlotRepro -count=1`.
- Expected PostgreSQL behavior: `DROP_REPLICATION_SLOT` requires a superuser or
  a role with the `REPLICATION` attribute.
- Observed Doltgres behavior: `ordinary_slot_dropper_protocol`, created only
  with `LOGIN`, connects with `replication=database` and successfully drops a
  slot created by `postgres`; `pg_replication_slots` reports a remaining count
  of zero.

### pg_terminate_backend does not require signal privileges for replication senders

- Reproducer: `TestPgTerminateBackendRequiresPrivilegeForReplicationSenderRepro`
  in `testing/go/logical_replication_security_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgTerminateBackendRequiresPrivilegeForReplicationSenderRepro
  -count=1`.
- Expected PostgreSQL behavior: terminating another user's backend requires
  superuser privileges or `pg_signal_backend` membership; a normal login role
  cannot terminate a superuser-owned replication sender.
- Observed Doltgres behavior: a normal role created only with `LOGIN` can run
  `pg_terminate_backend(active_pid)` against the active logical replication
  sender. The call returns without a privilege error, allowing the role to stop
  another user's replication stream.

### Transactional logical decoding messages survive ROLLBACK

- Reproducer: `TestPgLogicalEmitTransactionalMessageRollsBackRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgLogicalEmitTransactionalMessageRollsBackRepro -count=1`.
- Expected PostgreSQL behavior: `pg_logical_emit_message(true, ...)` emits a
  transactional logical-decoding message that is discarded if the surrounding
  transaction rolls back.
- Observed Doltgres behavior: after `BEGIN; SELECT
  pg_logical_emit_message(true, 'rolled/back', 'discard me'); ROLLBACK;`, the
  replication connection still receives a `CopyData` logical-decoding message
  containing the rolled-back prefix and payload.

### Transactional logical decoding messages are visible before COMMIT

- Reproducer: `TestPgLogicalEmitTransactionalMessageWaitsForCommitRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgLogicalEmitTransactionalMessageWaitsForCommitRepro -count=1`.
- Expected PostgreSQL behavior: `pg_logical_emit_message(true, ...)` is queued
  with the surrounding transaction and is not visible to logical replication
  clients until the transaction commits.
- Observed Doltgres behavior: after `BEGIN; SELECT
  pg_logical_emit_message(true, 'commit/only', 'after commit');`, the
  replication connection immediately receives a `CopyData` logical-decoding
  message before `COMMIT` runs. Subscribers can observe transactional logical
  messages before the publisher transaction is durable.

### Logical-replication TRUNCATE path is not rolled back to savepoint

- Reproducer:
  `TestLogicalReplicationSourceSavepointRollbackRestoresTruncatedRowsRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationSourceSavepointRollbackRestoresTruncatedRowsRepro
  -count=1`.
- Expected PostgreSQL behavior: with an active logical replication slot,
  `BEGIN; SAVEPOINT sp; TRUNCATE table; ROLLBACK TO SAVEPOINT sp; COMMIT;`
  restores the truncated publisher rows, and no TRUNCATE change is emitted for
  the rolled-back operation.
- Observed Doltgres behavior: the same TRUNCATE/savepoint flow passes without a
  replication slot, but with a pgoutput slot active through the logical
  replication capture path, the publisher table remains empty after COMMIT. The
  focused repro expects two restored rows and observes zero.

### Logical-replication TRUNCATE path is not restored by ROLLBACK

- Reproducer: `TestLogicalReplicationSourceRollbackRestoresTruncatedRowsRepro`
  in `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationSourceRollbackRestoresTruncatedRowsRepro
  -count=1`.
- Expected PostgreSQL behavior: with an active logical replication slot,
  `BEGIN; TRUNCATE table; ROLLBACK;` restores the truncated publisher rows and
  emits no TRUNCATE change for the aborted transaction.
- Observed Doltgres behavior: with a pgoutput slot active through the logical
  replication capture path, `ROLLBACK` does not restore the truncated publisher
  rows. The focused repro expects the two original rows after rollback and
  observes zero.

### `pg_drop_replication_slot` does not require a REPLICATION role

- Reproducer: `TestPgDropReplicationSlotRequiresReplicationRoleRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPgDropReplicationSlotRequiresReplicationRoleRepro -count=1`.
- Expected PostgreSQL behavior: dropping a replication slot through
  `pg_drop_replication_slot` requires a superuser or a role with the
  `REPLICATION` attribute.
- Observed Doltgres behavior: `ordinary_slot_dropper`, created only with
  `LOGIN`, successfully executes `SELECT pg_drop_replication_slot(...)` against
  a slot created by `postgres`; the slot is removed and
  `pg_replication_slots` reports a remaining count of zero.

### Temporary logical replication slots survive session close

- Reproducer:
  `TestLogicalReplicationTemporarySlotDropsOnSessionCloseRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationTemporarySlotDropsOnSessionCloseRepro -count=1`.
- Expected PostgreSQL behavior: a temporary logical replication slot is scoped
  to the session that created it and is dropped automatically when that session
  closes.
- Observed Doltgres behavior: after creating a temporary pgoutput slot and
  closing the creating replication connection without starting replication,
  `pg_replication_slots` still reports one row for that slot. The stale slot can
  continue blocking the slot name and misrepresent active replication state.

### Logical replication accepts invalid slot names

- Reproducer: `TestLogicalReplicationRejectsInvalidSlotNameRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRejectsInvalidSlotNameRepro -count=1`.
- Expected PostgreSQL behavior: replication slot names must use PostgreSQL's
  valid slot-name character set, and `CREATE_REPLICATION_SLOT` rejects names
  such as `invalid-slot-name`.
- Observed Doltgres behavior: `CREATE_REPLICATION_SLOT invalid-slot-name
  LOGICAL pgoutput` succeeds, and `pg_replication_slots` contains one row for
  that invalid slot name. Invalid replication metadata can be persisted and
  exposed through the catalog.

### Logical replication ignores `CREATE_REPLICATION_SLOT ... TWO_PHASE`

- Reproducer: `TestLogicalReplicationCreateSlotTwoPhaseSetsCatalogFlagRepro`
  in `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationCreateSlotTwoPhaseSetsCatalogFlagRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE_REPLICATION_SLOT name LOGICAL pgoutput
  TWO_PHASE` creates a logical replication slot enabled for decoding prepared
  transactions, and `pg_replication_slots.two_phase` reports true for that slot.
- Observed Doltgres behavior: the protocol command succeeds, but
  `pg_replication_slots.two_phase` remains false for `two_phase_catalog_slot`.
  The catalog reports a slot state that does not match the creation command, so
  prepared-transaction decoding capability is misrepresented.

### Logical replication allows `USE_SNAPSHOT` slot creation outside a transaction

- Reproducer:
  `TestLogicalReplicationCreateSlotUseSnapshotRequiresTransactionRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationCreateSlotUseSnapshotRequiresTransactionRepro
  -count=1`.
- Expected PostgreSQL behavior: `CREATE_REPLICATION_SLOT ... USE_SNAPSHOT`
  requires an explicit transaction and must use that transaction's snapshot.
  Running it outside a transaction should fail before creating a slot.
- Observed Doltgres behavior: `CREATE_REPLICATION_SLOT
  use_snapshot_outside_tx_slot LOGICAL pgoutput USE_SNAPSHOT` succeeds outside
  a transaction, and `pg_replication_slots` reports one row for the slot. The
  server records a slot whose snapshot mode contract was not satisfied.

### Logical replication slot creation ignores unknown options

- Reproducer: `TestLogicalReplicationCreateSlotRejectsUnknownOptionRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationCreateSlotRejectsUnknownOptionRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE_REPLICATION_SLOT` validates its option
  list and rejects unsupported options.
- Observed Doltgres behavior: `CREATE_REPLICATION_SLOT
  unknown_create_slot_option LOGICAL pgoutput UNKNOWN_OPTION` succeeds, and
  `pg_replication_slots` reports one row for that slot. Unsupported slot
  creation options are silently ignored while persistent slot metadata is
  created.

### Logical replication streams changes when `publication_names` is omitted

- Reproducer: `TestLogicalReplicationRequiresPublicationNamesOptionRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRequiresPublicationNamesOptionRepro -count=1`.
- Expected PostgreSQL behavior: the `pgoutput` plugin requires a
  `publication_names` option for `START_REPLICATION`; a client that omits it
  should not enter a replication stream.
- Observed Doltgres behavior: `START_REPLICATION` succeeds with only
  `"proto_version" '1'`, and the connection receives
  `public.missing_publication_names_items` row
  `1:visible-without-publication-names`. An empty sender publication list acts
  as a wildcard and can leak or corrupt replication data for an incorrectly
  configured subscriber.

### Logical replication ignores missing publications in `publication_names`

- Reproducer: `TestLogicalReplicationRejectsMissingPublicationNameRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRejectsMissingPublicationNameRepro -count=1`.
- Expected PostgreSQL behavior: `START_REPLICATION` with `pgoutput` rejects a
  `publication_names` list that names a publication that does not exist.
- Observed Doltgres behavior: a stream requested with
  `existing_publication_name_pub,missing_publication_name_pub` starts
  successfully and receives `public.missing_publication_name_items` row
  `1:visible-with-missing-publication`. A subscriber can run with a partially
  valid publication set instead of failing fast on configuration drift.

### Logical replication matches publication names case-insensitively

- Reproducer: `TestLogicalReplicationPublicationNamesAreCaseSensitiveRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationPublicationNamesAreCaseSensitiveRepro -count=1`.
- Expected PostgreSQL behavior: quoted publication names are case-sensitive, so
  a stream requesting `casesensitivepublication` should not match publication
  `"CaseSensitivePublication"`.
- Observed Doltgres behavior: the lower-case `publication_names` value starts a
  stream and receives `public.case_sensitive_publication_items` row
  `1:visible-through-wrong-case` from the quoted mixed-case publication. A
  subscriber can receive data for a publication name it did not exactly request.

### Logical replication splits quoted publication names at commas

- Reproducer: `TestLogicalReplicationPublicationNamesAllowQuotedCommasRepro`
  in `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationPublicationNamesAllowQuotedCommasRepro -count=1`.
- Expected PostgreSQL behavior: `publication_names` entries are publication
  identifiers and quoted identifiers may contain commas, so
  `"publication_names" '"Quoted,Publication"'` subscribes to publication
  `"Quoted,Publication"`.
- Observed Doltgres behavior: the stream starts, but an INSERT into
  `public.quoted_comma_publication_items` emits no relation, INSERT, or commit
  messages for the quoted-comma publication; the repro times out waiting for
  them. A valid quoted publication name can silently miss all changes.

### Logical replication streams changes when `proto_version` is omitted

- Reproducer: `TestLogicalReplicationRequiresProtoVersionOptionRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRequiresProtoVersionOptionRepro -count=1`.
- Expected PostgreSQL behavior: the `pgoutput` plugin requires a
  `proto_version` startup option so the output protocol is explicitly
  negotiated.
- Observed Doltgres behavior: `START_REPLICATION` succeeds with only
  `"publication_names" 'missing_proto_version_pub'`, and the connection
  receives `public.missing_proto_version_items` row
  `1:visible-without-proto-version`. A subscriber can receive pgoutput bytes
  without a negotiated protocol version.

### Logical replication streams changes with an invalid `proto_version`

- Reproducer: `TestLogicalReplicationRejectsInvalidProtoVersionOptionRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRejectsInvalidProtoVersionOptionRepro -count=1`.
- Expected PostgreSQL behavior: the `pgoutput` plugin validates
  `proto_version`; malformed values such as `not-a-number` must reject
  `START_REPLICATION` before a stream starts.
- Observed Doltgres behavior: `START_REPLICATION` succeeds with
  `"proto_version" 'not-a-number'`, and the connection receives
  `public.invalid_proto_version_items` row
  `1:visible-with-invalid-proto-version`. A subscriber can receive pgoutput
  bytes after advertising an invalid protocol version.

### Logical replication streams changes with unknown pgoutput options

- Reproducer: `TestLogicalReplicationRejectsUnknownPgoutputOptionRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRejectsUnknownPgoutputOptionRepro -count=1`.
- Expected PostgreSQL behavior: the `pgoutput` plugin rejects unrecognized
  startup options instead of silently ignoring them.
- Observed Doltgres behavior: `START_REPLICATION` succeeds with
  `"unknown_pgoutput_option" 'true'`, and the connection receives
  `public.unknown_pgoutput_option_items` row
  `1:visible-with-unknown-option`. A misconfigured subscriber can unknowingly
  run with options that the server ignored.

### Logical replication row filters reject bare boolean columns at write time

- Reproducer: `TestLogicalReplicationRowFilterBooleanColumnRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterBooleanColumnRepro -count=1`.
- Expected PostgreSQL behavior: a publication row filter such as
  `WHERE (visible)` is valid when `visible` is a boolean column; matching
  writes commit normally and are streamed to subscribers.
- Observed Doltgres behavior: the publication is created, but an insert that
  should match the filter fails with
  `publication row filter expression *sqlparser.ColName is not supported`.
  A valid row-filtered publication can turn ordinary writes into internal
  errors instead of streaming the changed row.

### Logical replication row filters reject IS FALSE at write time

- Reproducer: `TestLogicalReplicationRowFilterIsFalseRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterIsFalseRepro -count=1`.
- Expected PostgreSQL behavior: a publication row filter such as
  `WHERE (visible IS FALSE)` is valid for a boolean column; matching writes
  commit normally and are streamed to subscribers.
- Observed Doltgres behavior: the publication is created, but an insert that
  should match the filter fails with
  `publication row filter operator "is false" is not supported`. A valid
  row-filtered publication can make ordinary publisher DML fail.

### Logical replication row filters reject IS TRUE at write time

- Reproducer: `TestLogicalReplicationRowFilterIsTrueRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterIsTrueRepro -count=1`.
- Expected PostgreSQL behavior: a publication row filter such as
  `WHERE (visible IS TRUE)` is valid for a boolean column; matching writes
  commit normally and are streamed to subscribers.
- Observed Doltgres behavior: the publication is created, but an insert that
  should match the filter fails with
  `publication row filter operator "is true" is not supported`. A valid
  row-filtered publication can make ordinary publisher DML fail.

### Logical replication row filters reject IS NOT TRUE at write time

- Reproducer: `TestLogicalReplicationRowFilterIsNotTrueRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterIsNotTrueRepro -count=1`.
- Expected PostgreSQL behavior: a publication row filter such as
  `WHERE (visible IS NOT TRUE)` is valid; `FALSE` and `NULL` values can match,
  and matching writes commit normally and are streamed to subscribers.
- Observed Doltgres behavior: the publication is created, but an insert that
  should match the filter fails with
  `publication row filter operator "is not true" is not supported`. A valid
  row-filtered publication can make ordinary publisher DML fail.

### Logical replication row filters reject IS NOT FALSE at write time

- Reproducer: `TestLogicalReplicationRowFilterIsNotFalseRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterIsNotFalseRepro -count=1`.
- Expected PostgreSQL behavior: a publication row filter such as
  `WHERE (visible IS NOT FALSE)` is valid; `TRUE` and `NULL` values can match,
  and matching writes commit normally and are streamed to subscribers.
- Observed Doltgres behavior: the publication is created, but an insert that
  should match the filter fails with
  `publication row filter operator "is not false" is not supported`. A valid
  row-filtered publication can make ordinary publisher DML fail.

### Logical replication row filters reject IS UNKNOWN at write time

- Reproducer: `TestLogicalReplicationRowFilterIsUnknownRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterIsUnknownRepro -count=1`.
- Expected PostgreSQL behavior: a publication row filter such as
  `WHERE (visible IS UNKNOWN)` is valid; `NULL` values match, and matching
  writes commit normally and are streamed to subscribers.
- Observed Doltgres behavior: the publication is created, but an insert that
  should match the filter fails when replication re-parses the stored filter
  with `syntax error at position 40 near 'DISTINCT'`. A valid row-filtered
  publication can make ordinary publisher DML fail.

### Logical replication row filters reject IS NOT UNKNOWN at write time

- Reproducer: `TestLogicalReplicationRowFilterIsNotUnknownRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterIsNotUnknownRepro -count=1`.
- Expected PostgreSQL behavior: a publication row filter such as
  `WHERE (visible IS NOT UNKNOWN)` is valid; non-`NULL` boolean values match,
  and matching writes commit normally and are streamed to subscribers.
- Observed Doltgres behavior: the publication is created, but an insert that
  should match the filter fails when replication re-parses the stored filter
  with `syntax error at position 36 near 'DISTINCT'`. A valid row-filtered
  publication can make ordinary publisher DML fail.

### Logical replication row filters reject boolean true literals at write time

- Reproducer: `TestLogicalReplicationRowFilterBooleanTrueEqualityRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterBooleanTrueEqualityRepro -count=1`.
- Expected PostgreSQL behavior: a publication row filter such as
  `WHERE (visible = true)` is valid for a boolean column; matching writes
  commit normally and are streamed to subscribers.
- Observed Doltgres behavior: the publication is created, but an insert that
  should match the filter fails with
  `publication row filter scalar expression sqlparser.BoolVal is not supported`.
  A valid row-filtered publication can make ordinary publisher DML fail.

### Logical replication row filters reject boolean false literals at write time

- Reproducer: `TestLogicalReplicationRowFilterBooleanFalseEqualityRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterBooleanFalseEqualityRepro -count=1`.
- Expected PostgreSQL behavior: a publication row filter such as
  `WHERE (visible = false)` is valid for a boolean column; matching writes
  commit normally and are streamed to subscribers.
- Observed Doltgres behavior: the publication is created, but an insert that
  should match the filter fails with
  `publication row filter scalar expression sqlparser.BoolVal is not supported`.
  A valid row-filtered publication can make ordinary publisher DML fail.

### Logical replication row filters do not coerce boolean string literals

- Reproducer: `TestLogicalReplicationRowFilterBooleanStringLiteralRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterBooleanStringLiteralRepro -count=1`.
- Expected PostgreSQL behavior: a row filter such as `WHERE (visible =
  'true')` coerces the unknown literal to boolean, so a row with
  `visible = TRUE` matches and is streamed.
- Observed Doltgres behavior: the insert commits, but no replication message is
  emitted and the test times out waiting for the row. The row-filter evaluator
  appears to compare serialized row bytes to the literal bytes instead of using
  PostgreSQL boolean coercion semantics.

### Logical replication row filters publish boolean rows filtered by string literal inequality

- Reproducer:
  `TestLogicalReplicationRowFilterBooleanStringInequalitySuppressesRowsRepro`
  in `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterBooleanStringInequalitySuppressesRowsRepro
  -count=1`.
- Expected PostgreSQL behavior: a row filter such as `WHERE (visible <>
  'true')` coerces the unknown string literal to boolean, so a row with
  `visible = TRUE` does not match and should not be streamed.
- Observed Doltgres behavior: the insert commits, but logical replication emits
  a `CopyData` message instead of timing out with no row. A subscriber can
  receive a row that PostgreSQL would have filtered out.

### Logical replication row filters reject IS DISTINCT FROM at write time

- Reproducer: `TestLogicalReplicationRowFilterIsDistinctFromRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterIsDistinctFromRepro -count=1`.
- Expected PostgreSQL behavior: a publication row filter such as
  `WHERE (customer_id IS DISTINCT FROM 5)` is valid; matching writes commit
  normally and are streamed to subscribers.
- Observed Doltgres behavior: the publication is created, but an insert that
  should match the filter fails when replication re-parses the filter with
  `syntax error at position 40 near 'DISTINCT'`. A PostgreSQL-valid row filter
  can turn publisher writes into runtime errors.

### Logical replication row filters reject COALESCE at write time

- Reproducer: `TestLogicalReplicationRowFilterCoalesceRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterCoalesceRepro -count=1`.
- Expected PostgreSQL behavior: a publication row filter such as
  `WHERE (COALESCE(label, 'fallback') = 'shown')` is valid; matching writes
  commit normally and are streamed to subscribers.
- Observed Doltgres behavior: the publication is created, but an insert that
  should match the filter fails with
  `publication row filter scalar expression *sqlparser.FuncExpr is not supported`.
  A PostgreSQL-valid row filter can make ordinary publisher DML fail at write
  time instead of streaming the changed row.

### Logical replication row filters reject casts at write time

- Reproducer: `TestLogicalReplicationRowFilterCastRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterCastRepro -count=1`.
- Expected PostgreSQL behavior: a publication row filter such as
  `WHERE (code::varchar < 'm')` is valid; matching writes commit normally and
  are streamed to subscribers.
- Observed Doltgres behavior: the publication is created, but an insert that
  should match the filter fails when replication re-parses the filter with
  `syntax error at position 30 near '::VARCHAR'`. A PostgreSQL-valid row filter
  can turn publisher writes into runtime errors.

### Logical replication row filters reject LIKE at write time

- Reproducer: `TestLogicalReplicationRowFilterLikeRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterLikeRepro -count=1`.
- Expected PostgreSQL behavior: a publication row filter such as
  `WHERE (label LIKE 'show%')` is valid; matching writes commit normally and
  are streamed to subscribers.
- Observed Doltgres behavior: the publication is created, but an insert that
  should match the filter fails with
  `publication row filter comparison operator "like" is not supported`. A
  PostgreSQL-valid row filter can turn publisher writes into runtime errors.

### Logical replication row filters reject BETWEEN at write time

- Reproducer: `TestLogicalReplicationRowFilterBetweenRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterBetweenRepro -count=1`.
- Expected PostgreSQL behavior: a publication row filter such as
  `WHERE (score BETWEEN 10 AND 20)` is valid; matching writes commit normally
  and are streamed to subscribers.
- Observed Doltgres behavior: the publication is created, but an insert that
  should match the filter fails with
  `publication row filter expression *sqlparser.RangeCond is not supported`. A
  PostgreSQL-valid row filter can turn publisher writes into runtime errors.

### Logical replication row filters reject arithmetic expressions at write time

- Reproducer: `TestLogicalReplicationRowFilterArithmeticRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterArithmeticRepro -count=1`.
- Expected PostgreSQL behavior: a publication row filter such as
  `WHERE (score + 1 = 2)` is valid; matching writes commit normally and are
  streamed to subscribers.
- Observed Doltgres behavior: the publication is created, but an insert that
  should match the filter fails with
  `publication row filter scalar expression *sqlparser.BinaryExpr is not supported`.
  A PostgreSQL-valid row filter can turn publisher writes into runtime errors.

### Logical replication misses CHAR(n) rows by comparing padded bytes

- Reproducer: `TestLogicalReplicationRowFilterBpcharPaddingRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterBpcharPaddingRepro -count=1`.
- Expected PostgreSQL behavior: `CHAR(n)` equality uses `bpchar` comparison
  semantics, so a publication row filter such as `WHERE (code = 'a')` matches
  a `CHAR(3)` row inserted as `'a'` and streams the row.
- Observed Doltgres behavior: the insert commits, but no replication message is
  emitted and the test times out waiting for the row. The row-filter evaluator
  appears to compare the stored padded bytes with the unpadded literal, so
  subscribers can miss rows PostgreSQL would publish.

### Logical replication publishes CHAR(n) rows that should be filtered

- Reproducer:
  `TestLogicalReplicationRowFilterBpcharInequalitySuppressesRowsRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterBpcharInequalitySuppressesRowsRepro
  -count=1`.
- Expected PostgreSQL behavior: `CHAR(n)` inequality uses `bpchar` comparison
  semantics, so a `CHAR(3)` value inserted as `'a'` is not distinct from
  literal `'a'`; `WHERE (code <> 'a')` should not stream the row.
- Observed Doltgres behavior: the insert commits, and the replication
  connection receives a `CopyData` change for the row. The row-filter evaluator
  appears to compare the padded stored value with the unpadded literal as raw
  bytes, so subscribers can receive rows PostgreSQL would filter out.

### Logical replication treats numeric-looking text values as equal

- Reproducer:
  `TestLogicalReplicationRowFilterTextNumericEqualitySuppressesRowsRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterTextNumericEqualitySuppressesRowsRepro
  -count=1`.
- Expected PostgreSQL behavior: for a `TEXT` column, `label = '1.0'` uses text
  equality, so an inserted row with `label = '1'` does not match and should not
  be streamed by the publication.
- Observed Doltgres behavior: the insert commits, and the replication
  connection receives a `CopyData` change for the row. Doltgres parses both
  byte strings as numbers and treats text values `'1'` and `'1.0'` as equal,
  so subscribers can receive rows PostgreSQL would filter out.

### Logical replication orders numeric-looking text values as numbers

- Reproducer: `TestLogicalReplicationRowFilterTextNumericOrderingRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterTextNumericOrderingRepro -count=1`.
- Expected PostgreSQL behavior: for a `TEXT` column, `label < '2'` uses text
  ordering, so an inserted row with `label = '10'` matches under the baseline
  PostgreSQL collation and should be streamed by the publication.
- Observed Doltgres behavior: the insert commits, but no replication message is
  emitted and the test times out waiting for the row. Doltgres parses both byte
  strings as numbers and treats `'10' < '2'` as false, so subscribers can miss
  rows PostgreSQL would publish.

### Logical replication streams numeric-looking text values using numeric ordering

- Reproducer:
  `TestLogicalReplicationRowFilterTextNumericGreaterThanSuppressesRowsRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterTextNumericGreaterThanSuppressesRowsRepro
  -count=1`.
- Expected PostgreSQL behavior: for a `TEXT` column, `label > '2'` uses text
  ordering, so an inserted row with `label = '10'` does not match and should
  not be streamed by the publication.
- Observed Doltgres behavior: the insert commits, and the replication
  connection receives a `CopyData` change for the row. Doltgres parses both
  byte strings as numbers and treats `'10' > '2'` as true, so subscribers can
  receive rows PostgreSQL would filter out.

### Logical replication treats numeric-looking text inequality as numeric inequality

- Reproducer: `TestLogicalReplicationRowFilterTextNumericInequalityRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterTextNumericInequalityRepro -count=1`.
- Expected PostgreSQL behavior: for a `TEXT` column, `label <> '1.0'` uses text
  inequality, so an inserted row with `label = '1'` matches and should be
  streamed by the publication.
- Observed Doltgres behavior: the insert commits, but no replication message is
  emitted and the test times out waiting for the row. Doltgres parses both byte
  strings as numbers and treats text values `'1'` and `'1.0'` as equal, so
  subscribers can miss rows PostgreSQL would publish.

### Logical replication reparses numeric-looking text IN filters incorrectly

- Reproducer:
  `TestLogicalReplicationRowFilterTextNumericInSuppressesRowsRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterTextNumericInSuppressesRowsRepro
  -count=1`.
- Expected PostgreSQL behavior: for a `TEXT` column, `label IN ('1.0')` uses
  text equality; an inserted row with `label = '1'` commits normally and is not
  streamed by the publication.
- Observed Doltgres behavior: the publication is created, but the insert fails
  when replication re-parses the stored filter with
  `syntax error at position 34 near '1.0'`. A valid row-filtered publication can
  make ordinary publisher DML fail.

### Logical replication reparses numeric-looking text NOT IN filters incorrectly

- Reproducer: `TestLogicalReplicationRowFilterTextNumericNotInRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterTextNumericNotInRepro -count=1`.
- Expected PostgreSQL behavior: for a `TEXT` column, `label NOT IN ('1.0')`
  uses text equality; an inserted row with `label = '1'` matches, commits
  normally, and is streamed by the publication.
- Observed Doltgres behavior: the publication is created, but the insert fails
  when replication re-parses the stored filter with
  `syntax error at position 38 near '1.0'`. A valid row-filtered publication can
  make ordinary publisher DML fail.

### Logical replication reparses escaped text row filters incorrectly

- Reproducer: `TestLogicalReplicationRowFilterEscapedTextLiteralRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterEscapedTextLiteralRepro -count=1`.
- Expected PostgreSQL behavior: a publication row filter such as
  `WHERE (label = 'can''t')` is valid; a matching row commits normally and is
  streamed to subscribers.
- Observed Doltgres behavior: the publication is created, but the insert fails
  when replication re-parses the stored filter with
  `syntax error at position 34 near 'can't'`. A valid row-filtered publication
  can make ordinary publisher DML fail.

### Logical replication misses BYTEA rows by comparing rendered text

- Reproducer: `TestLogicalReplicationRowFilterByteaTextLiteralRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterByteaTextLiteralRepro -count=1`.
- Expected PostgreSQL behavior: `BYTEA` comparisons coerce string literals to
  typed byte strings, so a row filter literal spelled `'abc'` matches the same
  stored bytes even though row output renders as `\x616263`.
- Observed Doltgres behavior: the insert commits, but no replication message is
  emitted and the test times out waiting for the row. The row-filter evaluator
  appears to compare the rendered `\x616263` bytea text to the literal source
  text `abc`, so subscribers can miss rows PostgreSQL would publish.

### Logical replication publishes BYTEA rows that should be filtered

- Reproducer:
  `TestLogicalReplicationRowFilterByteaInequalitySuppressesRowsRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterByteaInequalitySuppressesRowsRepro
  -count=1`.
- Expected PostgreSQL behavior: `BYTEA` inequality compares typed byte strings,
  so a row storing the bytes for `abc` is not distinct from a row filter literal
  spelled `'abc'`; `WHERE (payload <> 'abc')` should not stream the row.
- Observed Doltgres behavior: the insert commits, and the replication connection
  receives a `CopyData` change for the row. The row-filter evaluator appears to
  compare rendered bytea text byte-for-byte, so subscribers can receive rows
  PostgreSQL would filter out.

### Logical replication misses JSONB rows by comparing serialized object text

- Reproducer: `TestLogicalReplicationRowFilterJsonbObjectCanonicalizationRepro`
  in `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterJsonbObjectCanonicalizationRepro
  -count=1`.
- Expected PostgreSQL behavior: `JSONB` equality compares typed canonical JSONB
  values, so object key order in a row filter literal is insignificant. A stored
  value `{"a":2,"b":1}` matches a row filter literal spelled `{"b":1,"a":2}`.
- Observed Doltgres behavior: the insert commits, but no replication message is
  emitted and the test times out waiting for the row. The row-filter evaluator
  appears to compare serialized JSONB object text instead of typed JSONB values,
  so subscribers can miss rows PostgreSQL would publish.

### Logical replication publishes JSONB rows that should be filtered

- Reproducer:
  `TestLogicalReplicationRowFilterJsonbInequalitySuppressesRowsRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterJsonbInequalitySuppressesRowsRepro
  -count=1`.
- Expected PostgreSQL behavior: `JSONB` inequality compares typed canonical
  JSONB values, so `{"a":2,"b":1}` is not distinct from a row filter literal
  spelled `{"b":1,"a":2}`; `WHERE (doc <> '{"b":1,"a":2}')` should not stream
  the row.
- Observed Doltgres behavior: the insert commits, and the replication connection
  receives a `CopyData` change for the row. The row-filter evaluator appears to
  compare serialized JSONB object text byte-for-byte, so subscribers can receive
  rows PostgreSQL would filter out.

### Logical replication misses JSONB numeric-equivalent rows

- Reproducer: `TestLogicalReplicationRowFilterJsonbNumericCanonicalizationRepro`
  in `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterJsonbNumericCanonicalizationRepro
  -count=1`.
- Expected PostgreSQL behavior: `JSONB` equality compares semantic JSONB numeric
  values, so a stored value `{"n":1}` matches a row filter literal spelled
  `{"n":1.0}`.
- Observed Doltgres behavior: the insert commits, but no replication message is
  emitted and the test times out waiting for the row. The row-filter evaluator
  appears to compare serialized JSONB numeric spelling instead of typed JSONB
  values, so subscribers can miss rows PostgreSQL would publish.

### Logical replication publishes JSONB numeric-equivalent rows that should be filtered

- Reproducer:
  `TestLogicalReplicationRowFilterJsonbNumericInequalitySuppressesRowsRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterJsonbNumericInequalitySuppressesRowsRepro
  -count=1`.
- Expected PostgreSQL behavior: `JSONB` inequality compares semantic JSONB
  numeric values, so `{"n":1}` is not distinct from a row filter literal spelled
  `{"n":1.0}`; `WHERE (doc <> '{"n":1.0}')` should not stream the row.
- Observed Doltgres behavior: the insert commits, and the replication connection
  receives a `CopyData` change for the row. The row-filter evaluator appears to
  compare serialized JSONB numeric spelling byte-for-byte, so subscribers can
  receive rows PostgreSQL would filter out.

### Logical replication misses UUID rows by comparing literal case

- Reproducer: `TestLogicalReplicationRowFilterUuidLiteralCaseRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterUuidLiteralCaseRepro -count=1`.
- Expected PostgreSQL behavior: UUID comparisons coerce unknown string
  literals to typed UUID values, so an uppercase UUID literal in a publication
  row filter matches the same UUID value stored in canonical lowercase form.
- Observed Doltgres behavior: the insert commits, but no replication message is
  emitted and the test times out waiting for the row. The row-filter evaluator
  appears to compare the UUID's serialized lowercase bytes to the literal's
  uppercase source bytes, so subscribers can miss rows PostgreSQL would publish.

### Logical replication publishes UUID rows that should be filtered

- Reproducer:
  `TestLogicalReplicationRowFilterUuidInequalitySuppressesRowsRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterUuidInequalitySuppressesRowsRepro
  -count=1`.
- Expected PostgreSQL behavior: UUID inequality compares typed UUID values, so
  an uppercase UUID literal is not distinct from the same stored UUID value in
  canonical lowercase form; `WHERE (external_id <> '...')` should not stream
  the row.
- Observed Doltgres behavior: the insert commits, and the replication
  connection receives a `CopyData` change for the row. The row-filter evaluator
  appears to compare UUID source spelling case-sensitively, so subscribers can
  receive rows PostgreSQL would filter out.

### Logical replication misses DATE rows by comparing literal spelling

- Reproducer:
  `TestLogicalReplicationRowFilterDateLiteralCanonicalizationRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterDateLiteralCanonicalizationRepro
  -count=1`.
- Expected PostgreSQL behavior: date comparisons coerce unknown string
  literals to typed `DATE` values, so a row filter literal spelled
  `'2026-5-1'` matches a stored `DATE` value output as `2026-05-01`.
- Observed Doltgres behavior: the insert commits, but no replication message is
  emitted and the test times out waiting for the row. The row-filter evaluator
  appears to compare the date's canonical serialized bytes to the literal's
  noncanonical source bytes, so subscribers can miss rows PostgreSQL would
  publish.

### Logical replication publishes DATE rows that should be filtered

- Reproducer:
  `TestLogicalReplicationRowFilterDateInequalitySuppressesRowsRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterDateInequalitySuppressesRowsRepro
  -count=1`.
- Expected PostgreSQL behavior: date inequality compares typed `DATE` values,
  so `DATE '2026-05-01'` is not distinct from a row filter literal spelled
  `'2026-5-1'`; `WHERE (event_date <> '2026-5-1')` should not stream the row.
- Observed Doltgres behavior: the insert commits, and the replication
  connection receives a `CopyData` change for the row. The row-filter evaluator
  appears to compare date source spelling byte-for-byte, so subscribers can
  receive rows PostgreSQL would filter out.

### Logical replication misses TIMESTAMP rows by comparing literal spelling

- Reproducer:
  `TestLogicalReplicationRowFilterTimestampLiteralCanonicalizationRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterTimestampLiteralCanonicalizationRepro
  -count=1`.
- Expected PostgreSQL behavior: timestamp comparisons coerce unknown string
  literals to typed `TIMESTAMP` values, so a row filter literal spelled
  `'2026-5-1 1:2:3'` matches a stored timestamp output as
  `2026-05-01 01:02:03`.
- Observed Doltgres behavior: the insert commits, but no replication message is
  emitted and the test times out waiting for the row. The row-filter evaluator
  appears to compare the timestamp's canonical serialized bytes to the literal's
  noncanonical source bytes, so subscribers can miss rows PostgreSQL would
  publish.

### Logical replication publishes TIMESTAMP rows that should be filtered

- Reproducer:
  `TestLogicalReplicationRowFilterTimestampInequalitySuppressesRowsRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterTimestampInequalitySuppressesRowsRepro
  -count=1`.
- Expected PostgreSQL behavior: timestamp inequality compares typed
  `TIMESTAMP` values, so `TIMESTAMP '2026-05-01 01:02:03'` is not distinct from
  a row filter literal spelled `'2026-5-1 1:2:3'`; `WHERE (event_ts <>
  '2026-5-1 1:2:3')` should not stream the row.
- Observed Doltgres behavior: the insert commits, and the replication
  connection receives a `CopyData` change for the row. The row-filter evaluator
  appears to compare timestamp source spelling byte-for-byte, so subscribers can
  receive rows PostgreSQL would filter out.

### Logical replication misses TIMESTAMPTZ rows by comparing rendered text

- Reproducer:
  `TestLogicalReplicationRowFilterTimestamptzLiteralCanonicalizationRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterTimestamptzLiteralCanonicalizationRepro
  -count=1`.
- Expected PostgreSQL behavior: `TIMESTAMPTZ` comparisons compare instants, so a
  row filter literal spelled `2026-05-01 01:02:03+00` matches a stored value
  inserted as `2026-04-30 18:02:03-07`, even when the replication session renders
  the row in the `America/Phoenix` time zone.
- Observed Doltgres behavior: the insert commits, but no replication message is
  emitted and the test times out waiting for the row. The row-filter evaluator
  appears to compare the session-rendered timestamp text to the literal source
  text, so subscribers can miss rows PostgreSQL would publish.

### Logical replication publishes TIMESTAMPTZ rows that should be filtered

- Reproducer:
  `TestLogicalReplicationRowFilterTimestamptzInequalitySuppressesRowsRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterTimestamptzInequalitySuppressesRowsRepro
  -count=1`.
- Expected PostgreSQL behavior: `TIMESTAMPTZ` inequality compares typed instants,
  so `2026-04-30 18:02:03-07` is not distinct from a row filter literal spelled
  `2026-05-01 01:02:03+00`; `WHERE (event_tz <> '2026-05-01 01:02:03+00')`
  should not stream the row.
- Observed Doltgres behavior: the insert commits, and the replication connection
  receives a `CopyData` change for the row. The row-filter evaluator appears to
  compare session-rendered timestamp text byte-for-byte, so subscribers can
  receive rows PostgreSQL would filter out.

### Logical replication misses TIME rows by comparing literal spelling

- Reproducer:
  `TestLogicalReplicationRowFilterTimeLiteralCanonicalizationRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterTimeLiteralCanonicalizationRepro
  -count=1`.
- Expected PostgreSQL behavior: time comparisons coerce unknown string literals
  to typed `TIME` values, so a row filter literal spelled `'1:2:3'` matches a
  stored `TIME` value output as `01:02:03`.
- Observed Doltgres behavior: the insert commits, but no replication message is
  emitted and the test times out waiting for the row. The row-filter evaluator
  appears to compare the time's canonical serialized bytes to the literal's
  noncanonical source bytes, so subscribers can miss rows PostgreSQL would
  publish.

### Logical replication publishes TIME rows that should be filtered

- Reproducer:
  `TestLogicalReplicationRowFilterTimeInequalitySuppressesRowsRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterTimeInequalitySuppressesRowsRepro
  -count=1`.
- Expected PostgreSQL behavior: time inequality compares typed `TIME` values,
  so `TIME '01:02:03'` is not distinct from a row filter literal spelled
  `'1:2:3'`; `WHERE (event_time <> '1:2:3')` should not stream the row.
- Observed Doltgres behavior: the insert commits, and the replication
  connection receives a `CopyData` change for the row. The row-filter evaluator
  appears to compare time source spelling byte-for-byte, so subscribers can
  receive rows PostgreSQL would filter out.

### Logical replication misses INTERVAL rows by comparing literal spelling

- Reproducer:
  `TestLogicalReplicationRowFilterIntervalLiteralCanonicalizationRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterIntervalLiteralCanonicalizationRepro
  -count=1`.
- Expected PostgreSQL behavior: interval comparisons coerce unknown string
  literals to typed `INTERVAL` values, so a row filter literal spelled
  `'1 day 2 hours'` matches a stored interval inserted as `'26 hours'`.
- Observed Doltgres behavior: the insert commits, but no replication message is
  emitted and the test times out waiting for the row. The row-filter evaluator
  appears to compare the interval's canonical serialized bytes to the literal's
  noncanonical source bytes, so subscribers can miss rows PostgreSQL would
  publish.

### Logical replication publishes INTERVAL rows that should be filtered

- Reproducer:
  `TestLogicalReplicationRowFilterIntervalInequalitySuppressesRowsRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterIntervalInequalitySuppressesRowsRepro
  -count=1`.
- Expected PostgreSQL behavior: interval inequality compares typed `INTERVAL`
  values, so `INTERVAL '26 hours'` is not distinct from a row filter literal
  spelled `'1 day 2 hours'`; `WHERE (span <> '1 day 2 hours')` should not stream
  the row.
- Observed Doltgres behavior: the insert commits, and the replication
  connection receives a `CopyData` change for the row. The row-filter evaluator
  appears to compare interval source spelling byte-for-byte, so subscribers can
  receive rows PostgreSQL would filter out.

### Logical replication publishes rows for `NOT IN` filters with NULL members

- Reproducer: `TestLogicalReplicationRowFilterNotInNullSuppressesRowsRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterNotInNullSuppressesRowsRepro -count=1`.
- Expected PostgreSQL behavior: `customer_id NOT IN (1, NULL)` evaluates to
  SQL `UNKNOWN` for `customer_id = 2`, so a row-filtered publication should
  not stream that inserted row.
- Observed Doltgres behavior: the insert commits, and the replication
  connection receives a `CopyData` change for the row. A subscriber can store a
  row that PostgreSQL would have filtered out, diverging from the publisher's
  publication contract.

### Logical replication publishes rows for `NOT` around `IN` filters with NULL members

- Reproducer:
  `TestLogicalReplicationRowFilterNotAroundInNullSuppressesRowsRepro` in
  `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterNotAroundInNullSuppressesRowsRepro
  -count=1`.
- Expected PostgreSQL behavior: `customer_id IN (1, NULL)` evaluates to SQL
  `UNKNOWN` for `customer_id = 2`, and `NOT UNKNOWN` remains `UNKNOWN`, so a
  row-filtered publication should not stream that inserted row.
- Observed Doltgres behavior: the insert commits, and the replication
  connection receives a `CopyData` change for the row. The row-filter evaluator
  collapses the `IN` expression to false before applying `NOT`, so subscribers
  can receive rows PostgreSQL would filter out.

### Logical replication publishes NULL input rows for `NOT IN` filters

- Reproducer: `TestLogicalReplicationRowFilterNotInInputNullSuppressesRowsRepro`
  in `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterNotInInputNullSuppressesRowsRepro
  -count=1`.
- Expected PostgreSQL behavior: `customer_id NOT IN (1, 2)` evaluates to SQL
  `UNKNOWN` when `customer_id` is `NULL`, so a row-filtered publication should
  not stream that row.
- Observed Doltgres behavior: the insert commits, and the replication
  connection receives a `CopyData` change for the row. A nullable publisher
  column can therefore leak rows through a `NOT IN` publication filter.

### Logical replication publishes rows for `NOT` over NULL comparisons

- Reproducer: `TestLogicalReplicationRowFilterNotNullComparisonSuppressesRowsRepro`
  in `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationRowFilterNotNullComparisonSuppressesRowsRepro
  -count=1`.
- Expected PostgreSQL behavior: `NOT (customer_id = NULL)` evaluates to SQL
  `UNKNOWN`, so a row-filtered publication should not stream a row with
  `customer_id = 2`.
- Observed Doltgres behavior: the insert commits, and the replication
  connection receives a `CopyData` change for the row. Doltgres treats
  `NOT UNKNOWN` like `TRUE`, so subscribers can receive rows PostgreSQL would
  have filtered out.

### Logical replication drops rows when schema publication overlaps a filtered table

- Reproducer: `TestLogicalReplicationSchemaPublicationOverridesTableRowFilterRepro`
  in `testing/go/logical_replication_row_filter_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationSchemaPublicationOverridesTableRowFilterRepro
  -count=1`.
- Expected PostgreSQL behavior: when a publication includes a table both
  explicitly with a row filter and through `TABLES IN SCHEMA`, the effective
  row filter shown by `pg_publication_tables` is empty, so rows outside the
  explicit filter still stream through the schema membership.
- Observed Doltgres behavior: inserting a row that does not match the explicit
  table filter produces no replication message and the test times out waiting
  for the row. A subscriber can miss rows that PostgreSQL would publish through
  the schema publication.

### Logical replication ignores the pgoutput `binary` option

- Reproducer: `TestLogicalReplicationHonorsBinaryOptionRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationHonorsBinaryOptionRepro -count=1`.
- Expected PostgreSQL behavior: when a client starts pgoutput with
  `"binary" 'true'`, tuple column data is sent in binary format and tagged with
  the binary tuple-data marker.
- Observed Doltgres behavior: the stream starts with `"binary" 'true'`, but an
  INSERT into `public.binary_option_items` sends column 0 with tuple-data marker
  `0x74` (`text`) instead of `0x62` (`binary`). A subscriber that requested
  binary pgoutput can decode text-formatted values as binary data.

### Logical replication sends logical messages when `messages` is false

- Reproducer:
  `TestLogicalReplicationMessagesFalseSuppressesLogicalMessagesRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationMessagesFalseSuppressesLogicalMessagesRepro
  -count=1`.
- Expected PostgreSQL behavior: pgoutput streams logical decoding messages only
  when the client enables them with the `messages` option; a stream started with
  `"messages" 'false'` should not receive `pg_logical_emit_message` payloads.
- Observed Doltgres behavior: after starting pgoutput with `"messages" 'false'`
  and `publication_names`, `SELECT pg_logical_emit_message(false,
  'messages/false', 'should not stream')` still emits a `CopyData` logical
  message containing that prefix and payload. A subscriber that opted out of
  logical messages can receive and apply unexpected message traffic.

### Quoted schema names collide with folded unquoted schema names

- Reproducer: `TestQuotedSchemaNamesAreCaseSensitiveRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestQuotedSchemaNamesAreCaseSensitiveRepro -count=1`.
- Expected PostgreSQL behavior: quoted identifiers preserve case, so schema
  `"CaseSchema"` and unquoted schema `caseschema` are distinct and can coexist.
- Observed Doltgres behavior: after `CREATE SCHEMA "CaseSchema"`,
  `CREATE SCHEMA caseschema` fails with `can't create schema caseschema; schema
  exists`, and `pg_catalog.pg_namespace` only reports `"CaseSchema"`. Distinct
  tenant or application schemas that differ by quoted case cannot be represented
  correctly.

### Quoted database names collide with folded unquoted database names

- Reproducer: `TestQuotedDatabaseNamesAreCaseSensitiveRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestQuotedDatabaseNamesAreCaseSensitiveRepro -count=1 -v`.
- Expected PostgreSQL behavior: quoted identifiers preserve case, so databases
  `"CaseDatabase"` and unquoted `casedatabase` are distinct database names and
  can coexist.
- Observed Doltgres behavior: after `CREATE DATABASE "CaseDatabase"`,
  `CREATE DATABASE casedatabase` fails with `can't create database
  casedatabase; database exists`, and `pg_catalog.pg_database` only reports
  `"CaseDatabase"`. Valid PostgreSQL database dumps or tenancy layouts that use
  quoted mixed-case database names cannot also represent the folded name.

### Quoted table names disappear from `pg_class` when folded names coexist

- Reproducer: `TestQuotedTableNamesAreCaseSensitiveRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestQuotedTableNamesAreCaseSensitiveRepro -count=1 -v`.
- Expected PostgreSQL behavior: quoted identifiers preserve case, so tables
  `"CaseTable"` and unquoted `casetable` are distinct relations in the same
  schema, each with its own `pg_catalog.pg_class` row.
- Observed Doltgres behavior: after creating `"CaseTable"` and `casetable`,
  queries against both names return their separate rows, but `pg_class` reports
  only `casetable` and omits `"CaseTable"`. Catalog-driven tools can miss an
  existing relation or generate incorrect migrations/backups when quoted and
  folded relation names coexist.

### Quoted view names collide with folded unquoted view names

- Reproducer: `TestQuotedViewNamesAreCaseSensitiveRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestQuotedViewNamesAreCaseSensitiveRepro -count=1 -v`.
- Expected PostgreSQL behavior: quoted identifiers preserve case, so views
  `"CaseView"` and unquoted `caseview` are distinct views in the same schema,
  and each resolves to its own stored query definition.
- Observed Doltgres behavior: after creating `"CaseView"`, `CREATE VIEW
  caseview AS ...` fails with `the view postgres.caseview already exists`.
  A later `SELECT ... FROM caseview` resolves to the `"CaseView"` definition,
  and `pg_class` only reports `"CaseView"`. Valid PostgreSQL schemas cannot
  represent distinct quoted and folded view names, and unquoted references can
  read from the wrong stored view definition.

### Quoted materialized view names disappear from `pg_class` when folded names coexist

- Reproducer: `TestQuotedMaterializedViewNamesAreCaseSensitiveRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestQuotedMaterializedViewNamesAreCaseSensitiveRepro -count=1 -v`.
- Expected PostgreSQL behavior: quoted identifiers preserve case, so
  materialized views `"CaseMatView"` and unquoted `casematview` are distinct
  relation entries in `pg_catalog.pg_class`.
- Observed Doltgres behavior: after creating both materialized views, queries
  against `"CaseMatView"` and `casematview` return their separate persisted
  rows, but `pg_class` reports only `casematview` and omits `"CaseMatView"`.
  Catalog-driven backup, diff, and dependency tools can miss an existing
  materialized view when quoted and folded materialized-view names coexist.

### Quoted column names collide with folded unquoted column names

- Reproducer: `TestQuotedColumnNamesAreCaseSensitiveRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestQuotedColumnNamesAreCaseSensitiveRepro -count=1 -v`.
- Expected PostgreSQL behavior: quoted identifiers preserve case, so columns
  `"CaseColumn"` and unquoted `casecolumn` are distinct columns in the same
  table and can store independent values.
- Observed Doltgres behavior: `CREATE TABLE quoted_column_items (...,
  "CaseColumn" TEXT, casecolumn TEXT)` fails during setup with
  `duplicate column name: casecolumn`. Valid PostgreSQL schemas that use quoted
  mixed-case column names cannot be represented when a folded column name
  coexists.

### Quoted index names collide with folded unquoted index names

- Reproducer: `TestQuotedIndexNamesAreCaseSensitiveRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestQuotedIndexNamesAreCaseSensitiveRepro -count=1 -v`.
- Expected PostgreSQL behavior: quoted identifiers preserve case, so indexes
  `"CaseIndex"` and unquoted `caseindex` are distinct index relations in the
  same schema and can coexist.
- Observed Doltgres behavior: after creating `"CaseIndex"`,
  `CREATE INDEX caseindex ON quoted_index_items (second_label)` fails with
  `Duplicate key name 'caseindex'`. Valid PostgreSQL schemas that use
  mixed-case quoted index names cannot add a folded index name that should be
  distinct.

### Quoted constraint names collide with folded unquoted constraint names

- Reproducer: `TestQuotedConstraintNamesAreCaseSensitiveRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestQuotedConstraintNamesAreCaseSensitiveRepro -count=1 -v`.
- Expected PostgreSQL behavior: quoted identifiers preserve case, so
  constraints `"CaseConstraint"` and unquoted `caseconstraint` are distinct
  constraints and can coexist on the same table.
- Observed Doltgres behavior: after creating `"CaseConstraint"`,
  `ALTER TABLE quoted_constraint_items ADD CONSTRAINT caseconstraint CHECK
  (amount < 100)` fails with `name caseconstraint in use`. Valid PostgreSQL
  schemas that use mixed-case quoted constraint names cannot add a folded
  constraint name that should be distinct.

### Quoted function names are treated as ambiguous with folded unquoted names

- Reproducer: `TestQuotedFunctionNamesAreCaseSensitiveRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestQuotedFunctionNamesAreCaseSensitiveRepro -count=1 -v`.
- Expected PostgreSQL behavior: quoted identifiers preserve case, so
  `"CaseFunction"(integer)` and unquoted `casefunction(integer)` are distinct
  routines that can coexist and resolve to their own function bodies.
- Observed Doltgres behavior: after defining `"CaseFunction"(integer)`,
  creating or calling `casefunction(integer)` reports that
  `public.CaseFunction(int4)` is ambiguous and matches both
  `public.casefunction(int4)` and `public.CaseFunction(int4)`. Valid
  PostgreSQL routine definitions that differ by quoted case cannot be called
  reliably.

### Quoted procedure names are treated as ambiguous with folded unquoted names

- Reproducer: `TestQuotedProcedureNamesAreCaseSensitiveRepro` in
  `testing/go/catalog_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestQuotedProcedureNamesAreCaseSensitiveRepro -count=1 -v`.
- Expected PostgreSQL behavior: quoted identifiers preserve case, so
  `"CaseProcedure"(text)` and unquoted `caseprocedure(text)` are distinct
  procedures that can coexist and execute their own procedure bodies.
- Observed Doltgres behavior: after defining `"CaseProcedure"(text)`,
  creating or calling `caseprocedure(text)` reports that
  `public.CaseProcedure(text)` is ambiguous and matches both
  `public.caseprocedure(text)` and `public.CaseProcedure(text)`. Valid
  PostgreSQL procedure definitions that differ by quoted case cannot be called
  reliably.

### `DROP_REPLICATION_SLOT WAIT` does not wait for active slots

- Reproducer: `TestLogicalReplicationDropSlotWaitWaitsForInactiveSlotRepro` in
  `testing/go/logical_replication_source_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestLogicalReplicationDropSlotWaitWaitsForInactiveSlotRepro -count=1
  -v`.
- Expected PostgreSQL behavior: `DROP_REPLICATION_SLOT ... WAIT` waits until an
  active replication slot becomes inactive, then drops the slot.
- Observed Doltgres behavior: the command returns immediately with
  `ERROR: replication slot "dg_drop_slot_wait_slot" is active (SQLSTATE XX000)`
  even though the active replication connection is closed shortly after the drop
  starts. Slot cleanup code that relies on `WAIT` can fail spuriously instead of
  blocking until the slot is safe to remove.

### Internal `"char"` casts high-bit bytes as unsigned integers

- Reproducer: `TestInternalCharCastToIntegerUsesSignedByteRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestInternalCharCastToIntegerUsesSignedByteRepro -count=1`.
- Expected PostgreSQL behavior: casting the internal `"char"` type to integer
  uses signed one-byte semantics, so the first UTF-8 byte of `'こんにちは'`,
  `\343`, casts to `-29`.
- Observed Doltgres behavior: the same cast returns `227`, treating the byte as
  unsigned and diverging from PostgreSQL's internal `"char"` integer semantics.

### `varchar(n)` and `character(n)` reject over-length values whose excess characters are spaces

- Reproducers: `TestVarcharTypmodTruncatesTrailingSpacesRepro` and
  `TestCharacterTypmodTruncatesTrailingSpacesRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'Test(Varchar|Character)TypmodTruncatesTrailingSpacesRepro' -count=1`.
- Expected PostgreSQL behavior: implicit assignment to `varchar(3)` or
  `character(3)` accepts `'abc   '` because the over-length characters are all
  spaces, and stores the truncated value `'abc'`.
- Observed Doltgres behavior: both inserts fail with `value too long for type
  varying(3): out of range`, rejecting valid PostgreSQL values.

### `character(n)` treats trailing padding spaces as significant for equality and uniqueness

- Reproducer: `TestCharacterTypmodIgnoresTrailingSpacesForUniquenessRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCharacterTypmodIgnoresTrailingSpacesForUniquenessRepro -count=1`.
- Expected PostgreSQL behavior: `character(3)` equality ignores trailing
  padding spaces, so a unique column that already contains `'a'` rejects
  inserting `'a  '`, and the stored value compares equal to
  `'a  '::character(3)`.
- Observed Doltgres behavior: the second insert succeeds, leaving two unique
  rows that differ only by trailing spaces. The original row compares false
  against `'a  '::character(3)`, so uniqueness and equality do not follow
  PostgreSQL `character(n)` semantics.

### `character(n)` columns store shorter values without padding

- Reproducer: `TestCharacterTypmodStoresPaddedValuesRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCharacterTypmodStoresPaddedValuesRepro -count=1`.
- Expected PostgreSQL behavior: assigning `'ab'` to a `CHARACTER(3)` column pads
  the stored value to the declared fixed width, so `octet_length(label)` is `3`
  and the row compares equal to `'ab '::CHARACTER(3)`.
- Observed Doltgres behavior: the stored value has `octet_length(label) = 2` and
  compares false against the padded value, so fixed-width character columns can
  persist unpadded data.

### `character(n)` literal casts do not pad shorter values

- Reproducer: `TestCharacterTypmodLiteralCastPadsToDeclaredLengthRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCharacterTypmodLiteralCastPadsToDeclaredLengthRepro -count=1`.
- Expected PostgreSQL behavior: casting a shorter literal value to
  `CHARACTER(3)` pads the result to the declared width, so
  `octet_length(CAST('ab' AS CHARACTER(3)))` is `3` and the result compares
  equal to `'ab '::CHARACTER(3)`.
- Observed Doltgres behavior: the literal cast has `octet_length(...) = 2` and
  compares false against the padded value, so expression-level `character(n)`
  casts can expose unpadded values.

### `character(n)` casts from columns overpad shorter values

- Reproducer: `TestCharacterTypmodColumnCastPadsToDeclaredLengthRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCharacterTypmodColumnCastPadsToDeclaredLengthRepro -count=1`.
- Expected PostgreSQL behavior: casting a shorter text column value to
  `CHARACTER(3)` pads the result to the declared width, so
  `octet_length(CAST(label AS CHARACTER(3)))` is `3`.
- Observed Doltgres behavior: the same cast returns `octet_length(...) = 11`,
  so column-driven `character(n)` casts use the wrong width.

### `character(n)` array elements store shorter values with the wrong width

- Reproducer: `TestCharacterArrayTypmodStoresPaddedElementsRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCharacterArrayTypmodStoresPaddedElementsRepro -count=1`.
- Expected PostgreSQL behavior: storing `ARRAY['ab']::CHARACTER(3)[]` pads the
  array element to the declared fixed width, so `octet_length(labels[1])` is `3`
  and the element compares equal to `'ab '::CHARACTER(3)`.
- Observed Doltgres behavior: the stored element has `octet_length(labels[1]) =
  11` and compares false against the padded value, so fixed-width character
  array elements can persist with the wrong width.

### Array literals with explicit lower bounds are rejected

- Reproducer: `TestArrayLiteralPreservesLowerBoundsRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestArrayLiteralPreservesLowerBoundsRepro -count=1`.
- Expected PostgreSQL behavior: array literals can specify explicit lower
  bounds such as `'[0:2]={10,20,30}'::int[]`; the stored array preserves that
  lower bound, so subscripts `0`, `1`, and `2` read `10`, `20`, and `30`, and
  `array_upper(..., 1)` returns `2`.
- Observed Doltgres behavior: inserting the valid literal fails with
  `malformed array literal: "[0:2]={10,20,30}"`, so arrays with non-default
  lower bounds cannot be stored or round-tripped.

### `array_lower` is missing for array dimension metadata

- Reproducer: `TestArrayLowerReportsDefaultLowerBoundRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestArrayLowerReportsDefaultLowerBoundRepro -count=1`.
- Expected PostgreSQL behavior: `array_lower(ARRAY[10,20,30], 1)` reports the
  default lower bound `1`, while requesting a nonexistent dimension returns
  `NULL`.
- Observed Doltgres behavior: both calls fail during planning with
  `function: 'array_lower' not found`, so clients cannot inspect array lower
  bounds.

### `array_dims` is missing for array dimension metadata

- Reproducer: `TestArrayDimsReportsDimensionsRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestArrayDimsReportsDimensionsRepro -count=1`.
- Expected PostgreSQL behavior: `array_dims(ARRAY[10,20,30])` returns
  `[1:3]`, and `array_dims(ARRAY[[1,2],[3,4]])` returns `[1:2][1:2]`.
- Observed Doltgres behavior: both calls fail during planning with
  `function: 'array_dims' not found`, so clients cannot inspect array dimension
  bounds.

### Nested array constructors cannot form multidimensional arrays

- Reproducer: `TestNestedArrayConstructorReportsDimensionsRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestNestedArrayConstructorReportsDimensionsRepro -count=1`.
- Expected PostgreSQL behavior: `ARRAY[[1,2],[3,4]]` constructs a
  two-dimensional integer array, and `array_length` / `array_upper` report
  bounds of `2` for both dimensions.
- Observed Doltgres behavior: the expression fails with `cannot find cast
  function from integer[] to integer`, so ordinary multidimensional array
  constructors cannot be used.

### `array_ndims` is missing for array dimension metadata

- Reproducer: `TestArrayNdimsReportsDimensionCountRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestArrayNdimsReportsDimensionCountRepro -count=1`.
- Expected PostgreSQL behavior: `array_ndims(ARRAY[10,20,30])` returns `1`,
  while empty and NULL arrays return `NULL` for dimension count.
- Observed Doltgres behavior: the query fails during planning with
  `function: 'array_ndims' not found`, so clients cannot inspect array
  dimensionality.

### `cardinality` is missing for array element counts

- Reproducer: `TestCardinalityCountsArrayElementsRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCardinalityCountsArrayElementsRepro -count=1`.
- Expected PostgreSQL behavior: `cardinality(NULL::int[])` returns `NULL`,
  `cardinality('{}'::int[])` returns `0`, and
  `cardinality(ARRAY[10,20,30])` returns `3`.
- Observed Doltgres behavior: the query fails during planning with
  `function: 'cardinality' not found`, so clients cannot ask for the total
  element count of an array.

### `array_replace` is missing for array mutations

- Reproducer: `TestArrayReplaceReplacesMatchingElementsRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestArrayReplaceReplacesMatchingElementsRepro -count=1`.
- Expected PostgreSQL behavior: `array_replace(ARRAY[1,2,1], 1, 9)` returns
  `{9,2,9}`, and `array_replace(ARRAY[1,NULL,3,NULL], NULL, 0)` returns
  `{1,0,3,0}` using NULL-aware matching.
- Observed Doltgres behavior: the query fails during planning with
  `function: 'array_replace' not found`, so standard array replacement
  expressions cannot be used in queries, defaults, or writes.

### `array_remove` reports a literal parse error for multidimensional arrays

- Reproducer: `TestArrayRemoveRejectsMultidimensionalArraysRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestArrayRemoveRejectsMultidimensionalArraysRepro -count=1`.
- Expected PostgreSQL behavior: `array_remove('{{1,2,2},{1,4,3}}'::int[], 2)`
  parses the accepted multidimensional array literal and then rejects the
  operation with `removing elements from multidimensional arrays is not
  supported`.
- Observed Doltgres behavior: the query fails earlier with
  `invalid input syntax for type int4: "{1"`, so a valid multidimensional
  array literal is misparsed before `array_remove` can enforce PostgreSQL's
  one-dimensional mutation boundary.

### Array subscript assignment is rejected instead of updating stored arrays

- Reproducer: `TestArraySubscriptAssignmentPersistsElementRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestArraySubscriptAssignmentPersistsElementRepro -count=1`.
- Expected PostgreSQL behavior: `UPDATE array_subscript_assignment_items SET
  values_int[2] = 22 WHERE id = 1` rewrites the second stored array element and
  persists `{1,22,3}`.
- Observed Doltgres behavior: the update fails during parsing with `at or near
  "[": syntax error`, leaving the stored array unchanged as `{1,2,3}`.

### `string_to_array` is missing for array construction

- Reproducer: `TestStringToArraySplitsTextRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestStringToArraySplitsTextRepro -count=1`.
- Expected PostgreSQL behavior: `string_to_array('1|2|3', '|')` returns
  `{1,2,3}`, a NULL delimiter splits text into characters, and a NULL-token
  argument maps matching fields to NULL elements.
- Observed Doltgres behavior: the query fails during planning with
  `function: 'string_to_array' not found`, so standard text-to-array
  conversion expressions cannot be used in queries, generated columns, or
  writes.

### `array_to_string` rejects NULL arrays when a NULL replacement is supplied

- Reproducer: `TestArrayToStringNullArrayWithNullReplacementRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestArrayToStringNullArrayWithNullReplacementRepro -count=1`.
- Expected PostgreSQL behavior: `array_to_string(NULL::int[], ',', '*')`
  returns `NULL`, matching the two-argument NULL-array behavior.
- Observed Doltgres behavior: the query fails with
  `could not determine polymorphic type because input has type unknown`, so a
  valid NULL-array conversion raises an error instead of returning NULL.

### `array_fill` is missing for array construction

- Reproducer: `TestArrayFillConstructsArraysRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestArrayFillConstructsArraysRepro -count=1`.
- Expected PostgreSQL behavior: `array_fill(7, ARRAY[3])` constructs the array
  `{7,7,7}`.
- Observed Doltgres behavior: the query fails during planning with
  `function: 'array_fill' not found`, so standard array-construction
  expressions cannot be used in queries, defaults, or writes.

### `trim_array` is missing for array truncation

- Reproducer: `TestTrimArrayRemovesTrailingElementsRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestTrimArrayRemovesTrailingElementsRepro -count=1`.
- Expected PostgreSQL behavior: `trim_array(ARRAY[1,2,3,4], 2)` removes the
  final two elements and returns `{1,2}`.
- Observed Doltgres behavior: the query fails during planning with
  `function: 'trim_array' not found`, so standard array truncation expressions
  cannot be used in queries, defaults, or writes.

### `array_append` can persist values outside varchar array element typmods

- Reproducer: `TestVarcharArrayAppendValidatesElementTypmodRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestVarcharArrayAppendValidatesElementTypmodRepro -count=1`.
- Expected PostgreSQL behavior: assigning
  `array_append(labels, 'abcd')` into a `varchar(3)[]` column rejects the
  statement with `value too long for type character varying(3)`, leaving the
  original stored array `{abc}` unchanged.
- Observed Doltgres behavior: the update succeeds and the stored array becomes
  `{abc,abc}`. The appended `abcd` is silently truncated through the array
  element typmod instead of rejecting the write, so an invalid update is
  accepted and persisted.

### `array_append` can persist underpadded or truncated character array elements

- Reproducer: `TestCharacterArrayAppendAppliesElementTypmodRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCharacterArrayAppendAppliesElementTypmodRepro -count=1`.
- Expected PostgreSQL behavior: assigning `array_append(labels, 'ab')` into a
  `character(3)[]` column pads the appended element so
  `octet_length(labels[2]) = 3` and it compares equal to
  `'ab '::character(3)`. A later `array_append(labels, 'abcd')` rejects the
  update with `value too long`, leaving the stored array unchanged.
- Observed Doltgres behavior: the short appended element has
  `octet_length(labels[2]) = 2` and does not compare equal to the padded
  `character(3)` value. The overlong append also succeeds and extends the
  stored array to `{abc,"ab ",abc}`, so array mutation can persist character
  elements with invalid fixed-width semantics.

### `array_prepend` can persist values outside varchar array element typmods

- Reproducer: `TestVarcharArrayPrependValidatesElementTypmodRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestVarcharArrayPrependValidatesElementTypmodRepro -count=1`.
- Expected PostgreSQL behavior: assigning
  `array_prepend('abcd', labels)` into a `varchar(3)[]` column rejects the
  statement with `value too long for type character varying(3)`, leaving the
  original stored array `{abc}` unchanged.
- Observed Doltgres behavior: the update succeeds and the stored array becomes
  `{abc,abc}`. The prepended `abcd` is silently truncated through the array
  element typmod instead of rejecting the write, so an invalid update is
  accepted and persisted.

### `array_cat` fails before varchar array assignment typmod validation

- Reproducer: `TestVarcharArrayCatReportsAssignmentTypmodErrorRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestVarcharArrayCatReportsAssignmentTypmodErrorRepro -count=1`.
- Expected PostgreSQL behavior: `array_cat(labels, ARRAY['abcd'])` resolves
  against the `varchar(3)[]` target expression and the assignment rejects the
  oversized concatenated element with `value too long for type character
  varying(3)`, leaving the original stored array `{abc}` unchanged.
- Observed Doltgres behavior: the update fails earlier with
  `function array_cat(varchar(3)[](3), text[]) does not exist`, so the
  compatible array concatenation call is rejected during function resolution
  rather than reaching PostgreSQL's assignment validation boundary.

### `array_append` can persist values outside numeric array element typmods

- Reproducer: `TestNumericArrayAppendValidatesElementTypmodRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestNumericArrayAppendValidatesElementTypmodRepro -count=1`.
- Expected PostgreSQL behavior: assigning
  `array_append(amounts, 999.995)` into a `numeric(5,2)[]` column rejects the
  statement with `numeric field overflow`, leaving the original stored array
  `{1.23}` unchanged.
- Observed Doltgres behavior: the update succeeds and the stored array becomes
  `{1.23,1000.00}`. The appended value rounds outside the declared precision
  but is still persisted in the `numeric(5,2)[]` column.

### `array_cat` can persist values outside numeric array element typmods

- Reproducer: `TestNumericArrayCatValidatesElementTypmodRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestNumericArrayCatValidatesElementTypmodRepro -count=1`.
- Expected PostgreSQL behavior: assigning
  `array_cat(amounts, ARRAY[999.995])` into a `numeric(5,2)[]` column rejects
  the statement with `numeric field overflow`, leaving the original stored
  array `{1.23}` unchanged.
- Observed Doltgres behavior: the update succeeds and the stored array becomes
  `{1.23,1000.00}`. The concatenated value rounds outside the declared
  precision but is still persisted in the `numeric(5,2)[]` column.

### Arrays of typmod-constrained elements cannot be compared for equality

- Reproducers: `TestCharacterArrayTypmodSupportsEqualityRepro`,
  `TestVarcharArrayTypmodSupportsEqualityRepro`,
  `TestNumericArrayTypmodSupportsEqualityRepro`, and
  `TestVarcharArrayTypmodWherePredicateUsesElementEqualityRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'Test((Varchar|Character|Numeric)ArrayTypmodSupportsEquality|VarcharArrayTypmodWherePredicateUsesElementEquality)Repro'
  -count=1`.
- Expected PostgreSQL behavior: arrays whose element type has a typmod, such as
  `character(3)[]`, `varchar(3)[]`, and `numeric(5,2)[]`, support equality
  comparisons and `WHERE` predicates using the underlying element type
  semantics.
- Observed Doltgres behavior: direct comparisons and stored-column equality
  predicates fail during planning with a missing internal equality function such
  as
  `internal_binary_operator_func_=(varchar(3)[](3), varchar(3)[](3))` or
  `internal_binary_operator_func_=(numeric(5,2)[](5,2),
  numeric(5,2)[](5,2))`, so typmod-constrained arrays cannot be compared
  normally.

### Built-in range types cannot be used for stored values

- Reproducer: `TestBuiltinRangeTypesRoundTripRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestBuiltinRangeTypesRoundTripRepro -count=1`.
- Expected PostgreSQL behavior: built-in range types such as `int4range`,
  `numrange`, and `daterange` can be used as table columns and can round-trip
  range literals.
- Observed Doltgres behavior: `CREATE TABLE ... int4range` fails with
  `type "int4range" does not exist`, so built-in range values cannot be stored
  or queried despite their catalog OIDs appearing in generated defaults.

### User-defined range types are rejected

- Reproducer: `TestCreateTypeAsRangeRepro` in
  `testing/go/range_type_definition_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateTypeAsRangeRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE TYPE price_range AS RANGE (subtype =
  numeric)` creates a usable range type, and a literal such as
  `'[1.5,2.5)'::price_range` can be cast and read back.
- Observed Doltgres behavior: setup fails with `ERROR: CREATE RANGE TYPE is not
  yet supported (SQLSTATE 0A000)`, so user-defined range types cannot be
  represented.

### Built-in multirange types cannot be used for stored values

- Reproducer: `TestBuiltinMultirangeTypesRoundTripRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestBuiltinMultirangeTypesRoundTripRepro -count=1`.
- Expected PostgreSQL behavior: built-in multirange types such as
  `int4multirange` and `nummultirange` can be used as table columns and can
  round-trip multirange literals.
- Observed Doltgres behavior: `CREATE TABLE ... int4multirange` fails with
  `type "int4multirange" does not exist`, so built-in multirange values cannot
  be stored or queried despite their catalog OIDs appearing in generated
  defaults.

### `DISTINCT` over typmod-constrained array values fails instead of using element equality

- Reproducers: `TestTypmodArrayDistinctUsesElementEqualityRepro`,
  `TestVarcharArrayDistinctUsesElementEqualityRepro`, and
  `TestNumericArrayDistinctUsesElementEqualityRepro` in
  `testing/go/type_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'Test(Typmod|Varchar|Numeric)ArrayDistinctUsesElementEqualityRepro'
  -count=1`.
- Expected PostgreSQL behavior: `DISTINCT` over typmod-constrained array values
  uses array element equality, so duplicate `varchar(3)[]` and
  `numeric(5,2)[]` values collapse, and `character(3)[]` values that differ
  only by element padding also collapse.
- Observed Doltgres behavior: each query fails with an incompatible conversion
  error such as `incompatible conversion to SQL type: '[abc]'->text` or
  `incompatible conversion to SQL type: '[1.23]'->text`, so set/aggregate
  distinct processing cannot handle typmod-constrained arrays.

### DOLT_MERGE panics on conflicting rows with stored generated columns

- Reproducer: `TestMergeGeneratedColumnConflictReportsConflictRepro` in
  `testing/go/branch_merge_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestMergeGeneratedColumnConflictReportsConflictRepro -count=1`.
- Expected Doltgres behavior: merging two branches that update the same base
  column of a row with a stored generated column should return a normal merge
  conflict and record one entry in `dolt_conflicts`, leaving the database
  process alive and the conflict resolvable.
- Observed Doltgres behavior: the merge crashes the test server with
  `panic: malformed tuple` from `val.Tuple.GetField` while
  `remapTupleWithColumnDefaults` processes the generated-column row. A
  generated-column conflict can therefore abort the SQL server instead of being
  represented as normal conflict state.

### DOLT_STASH cannot save tracked table edits with integer columns

- Reproducer: `TestDoltStashPushPopRestoresTrackedRowRepro` in
  `testing/go/dolt_versioning_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDoltStashPushPopRestoresTrackedRowRepro -count=1`.
- Expected Doltgres behavior: after a committed row is edited,
  `DOLT_STASH('push', 'tracked-edit')` should save the working-set change and
  restore the committed row; `DOLT_STASH('pop', 'tracked-edit')` should then
  reapply the saved edit.
- Observed Doltgres behavior: the stash push fails with
  `ERROR: dolt_procedures: unsupported type int`, so ordinary tracked table
  edits involving integer columns cannot be stashed and restored through SQL.

### DOLT_STASH cannot save untracked tables with integer columns

- Reproducer: `TestDoltStashPushPopRestoresUntrackedTableRepro` in
  `testing/go/dolt_versioning_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDoltStashPushPopRestoresUntrackedTableRepro -count=1`.
- Expected Doltgres behavior: `DOLT_STASH('push', 'untracked-table', '--all')`
  should save a new untracked table and its rows, clear the working set, and
  `DOLT_STASH('pop', 'untracked-table')` should restore the table and data.
- Observed Doltgres behavior: the stash push fails with
  `ERROR: dolt_procedures: unsupported type int`, so new untracked tables with
  integer columns cannot be stashed and restored through SQL.

### CREATE OR REPLACE FUNCTION ignores transaction rollback

- Reproducers: `TestRollbackRestoresReplacedFunctionDefinitionRepro` and
  `TestRollbackToSavepointRestoresReplacedFunctionDefinitionRepro` in
  `testing/go/transaction_ddl_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestRollback(RestoresReplacedFunctionDefinitionRepro|ToSavepointRestoresReplacedFunctionDefinitionRepro)' -count=1`.
- Expected Doltgres behavior: if `CREATE OR REPLACE FUNCTION` changes an
  existing function body inside a transaction or after a savepoint, `ROLLBACK`
  or `ROLLBACK TO SAVEPOINT` should restore the previous function body.
- Observed Doltgres behavior: the replacement body remains active after both a
  full rollback and a savepoint rollback. A function that returned `7` before
  the transaction continues returning `8` after the rollback, so aborted
  function-definition changes leak into durable catalog state.

### CREATE OR REPLACE VIEW ignores transaction rollback

- Reproducers: `TestRollbackRestoresReplacedViewDefinitionRepro` and
  `TestRollbackToSavepointRestoresReplacedViewDefinitionRepro` in
  `testing/go/transaction_ddl_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestRollback(RestoresReplacedViewDefinitionRepro|ToSavepointRestoresReplacedViewDefinitionRepro)' -count=1`.
- Expected Doltgres behavior: if `CREATE OR REPLACE VIEW` changes an existing
  view definition inside a transaction or after a savepoint, `ROLLBACK` or
  `ROLLBACK TO SAVEPOINT` should restore the previous view definition.
- Observed Doltgres behavior: the replacement view definition remains active
  after both a full rollback and a savepoint rollback. A view that returned `7`
  before the transaction continues returning `8` after the rollback, so aborted
  view-definition changes leak into durable catalog state.

### CREATE OR REPLACE PROCEDURE ignores savepoint rollback

- Reproducer: `TestRollbackToSavepointRestoresReplacedProcedureDefinitionRepro`
  in `testing/go/transaction_ddl_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRollbackToSavepointRestoresReplacedProcedureDefinitionRepro -count=1`.
- Expected Doltgres behavior: if `CREATE OR REPLACE PROCEDURE` changes an
  existing procedure body after a savepoint, `ROLLBACK TO SAVEPOINT` should
  restore the previous procedure body before the transaction commits.
- Observed Doltgres behavior: after rolling back to the savepoint and
  committing, calling the procedure still runs the replacement body. The
  procedure initially inserted `7`, the replacement inserted `8`, and after the
  savepoint rollback the committed procedure still inserts `8`.

### CREATE OR REPLACE TRIGGER ignores savepoint rollback

- Reproducer: `TestRollbackToSavepointRestoresReplacedTriggerDefinitionRepro`
  in `testing/go/transaction_ddl_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestRollbackToSavepointRestoresReplacedTriggerDefinitionRepro -count=1`.
- Expected Doltgres behavior: if `CREATE OR REPLACE TRIGGER` changes an
  existing trigger binding after a savepoint, `ROLLBACK TO SAVEPOINT` should
  restore the previous trigger function before the transaction commits.
- Observed Doltgres behavior: after rolling back to the savepoint and
  committing, the trigger still uses the replacement function. The trigger
  initially wrote label `seven`, the replacement wrote `eight`, and after the
  savepoint rollback a new insert still stores `eight`.

### DOLT_RESET --hard leaves uncommitted functions in the working set

- Reproducer: `TestDoltResetHardRemovesUncommittedFunctionRepro` in
  `testing/go/dolt_versioning_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDoltResetHardRemovesUncommittedFunctionRepro -count=1`.
- Expected Doltgres behavior: after creating an uncommitted SQL function,
  `DOLT_RESET('--hard')` should discard that function, leave `dolt_status`
  clean, and make later calls to the function fail as undefined.
- Observed Doltgres behavior: `DOLT_RESET('--hard')` returns success, but
  `dolt_status` still reports one change and the supposedly discarded function
  remains callable, so reset does not actually restore root-object state for
  uncommitted functions.

### DOLT_RESET --hard to an older revision leaves newer functions active

- Reproducer: `TestDoltResetHardToRevisionRestoresFunctionDefinitionRepro` in
  `testing/go/dolt_versioning_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDoltResetHardToRevisionRestoresFunctionDefinitionRepro -count=1`.
- Expected Doltgres behavior: after committing a function body change,
  `DOLT_RESET('--hard', 'HEAD~1')` should restore the prior committed function
  body, leave `dolt_status` clean, and make later calls return the older value.
- Observed Doltgres behavior: `DOLT_RESET('--hard', 'HEAD~1')` returns
  success, but `dolt_status` reports one pending change and the function still
  returns the newer body value. The branch head moves back while the working
  root-object metadata keeps the reset-away function definition active.

### DOLT_RESET --hard to an older revision leaves newer triggers active

- Reproducer: `TestDoltResetHardToRevisionRestoresTriggerDefinitionRepro` in
  `testing/go/dolt_versioning_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDoltResetHardToRevisionRestoresTriggerDefinitionRepro -count=1`.
- Expected Doltgres behavior: after committing a trigger creation,
  `DOLT_RESET('--hard', 'HEAD~1')` should restore the prior revision without
  the trigger, leave `dolt_status` clean, and allow later inserts to run
  without the reset-away trigger firing.
- Observed Doltgres behavior: `DOLT_RESET('--hard', 'HEAD~1')` returns
  success, but `dolt_status` reports one pending change,
  `information_schema.triggers` still shows the trigger, and a later insert is
  rewritten by the reset-away trigger. Resetting history therefore leaves stale
  trigger metadata live in the working root.

### DOLT_RESET cannot unstage root-object changes

- Reproducers: `TestDoltResetUnstagesFunctionDefinitionRepro`,
  `TestDoltResetUnstagesSequenceDefinitionRepro`, and
  `TestDoltResetUnstagesTriggerDefinitionRepro` in
  `testing/go/dolt_versioning_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run 'TestDoltResetUnstages(FunctionDefinitionRepro|SequenceDefinitionRepro|TriggerDefinitionRepro)' -count=1`.
- Expected Doltgres behavior: after staging a function, sequence, or trigger
  root-object change with `DOLT_ADD('-A')`, `DOLT_RESET(<root-object-name>)`
  should unstage that root object while leaving the working object present.
- Observed Doltgres behavior: `DOLT_RESET` rejects canonical root-object paths
  such as `public.reset_staged_function_value()`,
  `public.reset_staged_sequence_value`, and
  `public.reset_staged_trigger_items.reset_staged_trigger` with
  `branch not found`, and the staged changes remain staged. Users can stage
  these root objects but cannot unstage them through the matching path reset
  API.

### DOLT_CLEAN leaves uncommitted trigger metadata dirty

- Reproducer: `TestDoltCleanRemovesUncommittedTriggerRepro` in
  `testing/go/dolt_versioning_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDoltCleanRemovesUncommittedTriggerRepro -count=1`.
- Expected Doltgres behavior: after creating an uncommitted trigger,
  `DOLT_CLEAN()` should discard the trigger metadata, leave `dolt_status`
  clean, and allow later inserts to run without the discarded trigger firing.
- Observed Doltgres behavior: `DOLT_CLEAN()` returns success and later inserts
  no longer fire the trigger, but `dolt_status` still reports one pending
  change. The operation therefore leaves stale root-object status after
  cleaning trigger metadata.

### DOLT_CHERRY_PICK leaves cherry-picked functions broken

- Reproducer: `TestDoltCherryPickAppliesFunctionDefinitionRepro` in
  `testing/go/dolt_versioning_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDoltCherryPickAppliesFunctionDefinitionRepro -count=1`.
- Expected Doltgres behavior: after one branch replaces a committed function
  body and another branch cherry-picks that commit, the cherry-pick should
  leave the function callable with the cherry-picked body.
- Observed Doltgres behavior: `DOLT_CHERRY_PICK('main')` returns success, but
  the next call to the cherry-picked function fails with a caught panic:
  `runtime error: index out of range [0] with length 0` from
  `CallSqlFunction` / `sql_function.go:105`. The cherry-picked function is
  therefore persisted in a broken callable state instead of returning the new
  body value.

### dolt_commit_diff tables reject single-commit filters expressed with HASHOF()

- Reproducer: `TestDoltCommitDiffWorkingSetFilterRepro` in
  `testing/go/dolt_versioning_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDoltCommitDiffWorkingSetFilterRepro -count=1`.
- Expected Doltgres behavior: `dolt_commit_diff_<table>` should expose the same
  working-set addition as `dolt_diff('HEAD', 'WORKING', table)` when the query
  filters to one `to_commit` and one `from_commit`, including when the commit
  hash is supplied through `HASHOF('main')`.
- Observed Doltgres behavior: `dolt_diff` reports the added row, but the
  equivalent `dolt_commit_diff_<table>` query fails with
  `dolt_commit_diff_* tables must be filtered to a single 'to_commit'` despite
  the `to_commit = HASHOF('main')` predicate. Versioned diff queries that use
  commit-hash expressions cannot inspect the working-set row diff.

### dolt_query_diff rejects AS OF queries in diff inputs

- Reproducer: `TestDoltQueryDiffSupportsAsOfRevisionRepro` in
  `testing/go/dolt_versioning_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDoltQueryDiffSupportsAsOfRevisionRepro -count=1`.
- Expected Doltgres behavior: `dolt_query_diff` should compare a historical
  `AS OF main` query with the current working query and report the row added
  since `main`.
- Observed Doltgres behavior: the query fails while preparing the historical
  input with `TargetType not handled: DB_TABLE_IDENT`, so versioned query
  diffs using `AS OF` cannot inspect changes.

### DOLT_PREVIEW_MERGE_CONFLICTS cannot preview function conflicts

- Reproducer: `TestPreviewMergeConflictsReportsFunctionConflictRepro` in
  `testing/go/branch_merge_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPreviewMergeConflictsReportsFunctionConflictRepro -count=1`.
- Expected Doltgres behavior: when two branches replace the same committed
  function body differently, `DOLT_PREVIEW_MERGE_CONFLICTS` should report the
  function-definition conflict so callers can inspect the merge outcome before
  applying it.
- Observed Doltgres behavior: previewing
  `preview_conflict_value()` fails with
  `table not found: public.preview_conflict_value()`, so the preview API treats
  the function name as a table and cannot surface function conflicts.

### DOLT_PREVIEW_MERGE_CONFLICTS cannot preview sequence conflicts

- Reproducer: `TestPreviewMergeConflictsReportsSequenceConflictRepro` in
  `testing/go/branch_merge_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPreviewMergeConflictsReportsSequenceConflictRepro -count=1`.
- Expected Doltgres behavior: when a `SERIAL` sequence advances independently
  on two branches, `DOLT_PREVIEW_MERGE_CONFLICTS` should report the generated
  sequence object's conflict before the merge is applied.
- Observed Doltgres behavior: previewing
  `preview_sequence_conflict_items_id_seq` fails with
  `table not found: public.preview_sequence_conflict_items_id_seq`, so the
  preview API treats sequence names as tables and cannot surface sequence
  conflicts.

### DOLT_PREVIEW_MERGE_CONFLICTS_SUMMARY panics on function conflicts

- Reproducer: `TestPreviewMergeConflictsSummaryReportsFunctionConflictRepro` in
  `testing/go/branch_merge_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestPreviewMergeConflictsSummaryReportsFunctionConflictRepro -count=1`.
- Expected Doltgres behavior: when two branches replace the same committed
  function body differently, `DOLT_PREVIEW_MERGE_CONFLICTS_SUMMARY` should
  return one summary row for the pending root-object conflict.
- Observed Doltgres behavior: the summary query panics with a nil pointer
  dereference in `getDataConflictsForTable` / `rowsFromTable` while processing
  the function conflict, so preview-summary callers can crash the SQL request
  instead of receiving conflict metadata.

### DOLT_DIFF cannot inspect function-definition changes

- Reproducer: `TestDoltDiffReportsFunctionDefinitionChangesRepro` in
  `testing/go/dolt_versioning_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDoltDiffReportsFunctionDefinitionChangesRepro -count=1`.
- Expected Doltgres behavior: if a committed function body changes between two
  revisions, `DOLT_DIFF` should expose the root-object diff just as
  `DOLT_DIFF_STAT` already reports `public.diff_function_value()`.
- Observed Doltgres behavior: `DOLT_DIFF_STAT` reports the changed function,
  but `DOLT_DIFF('main', 'original', 'diff_function_value()')` fails with
  `table not found: diff_function_value()`, so callers cannot inspect the
  function-definition delta.

### DOLT_DIFF cannot inspect sequence-object changes

- Reproducer: `TestDoltDiffReportsSequenceChangesRepro` in
  `testing/go/dolt_versioning_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDoltDiffReportsSequenceChangesRepro -count=1`.
- Expected Doltgres behavior: if a `SERIAL` sequence advances between two
  revisions, `DOLT_DIFF` should expose the generated sequence object's diff
  just as `DOLT_DIFF_STAT` reports `public.diff_sequence_items_id_seq`.
- Observed Doltgres behavior: `DOLT_DIFF_STAT` reports the changed sequence,
  but `DOLT_DIFF('main', 'original', 'diff_sequence_items_id_seq')` fails with
  `table not found: diff_sequence_items_id_seq`, so callers cannot inspect the
  sequence-object delta.

### DOLT_DIFF cannot inspect trigger-definition changes

- Reproducer: `TestDoltDiffReportsTriggerChangesRepro` in
  `testing/go/dolt_versioning_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestDoltDiffReportsTriggerChangesRepro -count=1`.
- Expected Doltgres behavior: if a trigger definition changes between two
  revisions, `DOLT_DIFF` should expose the trigger object's diff just as
  `DOLT_DIFF_STAT` reports
  `public.diff_trigger_items.diff_trigger_changed`.
- Observed Doltgres behavior: `DOLT_DIFF_STAT` reports the changed trigger, but
  `DOLT_DIFF('main', 'original',
  'diff_trigger_items.diff_trigger_changed')` fails with `table not found:
  diff_trigger_items.diff_trigger_changed`, so callers cannot inspect the
  trigger-definition delta.

### Normal roles can create LEAKPROOF functions

- Reproducer: `TestCreateFunctionLeakproofRequiresSuperuserRepro` in
  `testing/go/routine_privilege_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestCreateFunctionLeakproofRequiresSuperuserRepro -count=1`.
- Expected PostgreSQL behavior: only a superuser can create a `LEAKPROOF`
  function, because the optimizer may execute leakproof predicates ahead of
  security-barrier checks.
- Observed Doltgres behavior: a normal role with `CREATE` on the schema can
  run `CREATE FUNCTION ... LEAKPROOF` successfully instead of receiving
  `permission denied`, so an unprivileged function can be marked with a
  superuser-only trust property.

### Function-level SET search_path is ignored during execution

- Reproducer: `TestFunctionSetSearchPathOptionAppliesDuringExecutionRepro` in
  `testing/go/function_lookup_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestFunctionSetSearchPathOptionAppliesDuringExecutionRepro -count=1`.
- Expected PostgreSQL behavior: a function declared with
  `SET search_path = dg_fn_set_safe, public` runs its body under that local
  search path regardless of the caller's current `search_path`, so an
  unqualified read of `lookup_items` sees the safe schema and returns `1`.
- Observed Doltgres behavior: the function body runs under the caller's
  `search_path = dg_fn_set_attacker, public` and returns `2`, so function-local
  GUC options are ignored and unqualified function body lookups can be
  redirected by the caller.

### Procedure-level SET search_path is ignored during execution

- Reproducer: `TestProcedureSetSearchPathOptionAppliesDuringExecutionRepro` in
  `testing/go/procedure_argument_modes_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestProcedureSetSearchPathOptionAppliesDuringExecutionRepro -count=1`.
- Expected PostgreSQL behavior: a procedure declared with
  `SET search_path = dg_proc_set_safe, public` runs its body under that local
  search path regardless of the caller's current `search_path`, so an
  unqualified `INSERT INTO audit_items` writes to the safe schema.
- Observed Doltgres behavior: the procedure body runs under the caller's
  `search_path = dg_proc_set_attacker, public`; the safe table remains empty
  while the attacker-path table receives the row. Procedure-local GUC options
  are ignored, so unqualified procedure writes can be redirected by the caller.

### Invalid function planner options are accepted

- Reproducer: `TestFunctionDefinitionsRejectInvalidPlannerOptionsRepro` in
  `testing/go/routine_option_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestFunctionDefinitionsRejectInvalidPlannerOptionsRepro -count=1`.
- Expected PostgreSQL behavior: `CREATE FUNCTION` validates planner options
  before creating the function. `ROWS` is rejected for a scalar-returning
  function with `ROWS is not applicable when function does not return a set`,
  `ROWS 0` is rejected with `ROWS must be positive`, and non-positive `COST`
  values are rejected with `COST must be positive`.
- Observed Doltgres behavior: each invalid function definition succeeds, so
  routines can be persisted with planner metadata that PostgreSQL rejects.

### SET LOCAL search_path rejects schema lists

- Reproducer: `TestSetLocalSearchPathAcceptsSchemaListRepro` in
  `testing/go/session_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSetLocalSearchPathAcceptsSchemaListRepro -count=1`.
- Expected PostgreSQL behavior: `SET LOCAL search_path = schema_name, public`
  is valid inside a transaction, applies only until transaction end, and lets
  subsequent statements in that transaction resolve unqualified objects through
  the local schema list.
- Observed Doltgres behavior: the `SET LOCAL` statement fails with
  `SET LOCAL search_path requires exactly one value`, so valid transactional
  search-path changes with multiple schemas cannot be used.

### SET LOCAL search_path TO DEFAULT stores a literal DEFAULT path

- Reproducer: `TestSetLocalSearchPathDefaultUsesDefaultValueRepro` in
  `testing/go/session_correctness_repro_test.go`.
- Command: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include
  CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go
  -run TestSetLocalSearchPathDefaultUsesDefaultValueRepro -count=1`.
- Expected PostgreSQL behavior: if the session search path is
  `set_local_default_shadow, public`, then inside a transaction
  `SET LOCAL search_path TO DEFAULT` temporarily restores the default
  `"$user", public` path. An unqualified table name resolves to the `public`
  table inside the transaction, and after `COMMIT` the prior session path is
  restored.
- Observed Doltgres behavior: `SET LOCAL search_path TO DEFAULT` succeeds, but
  the next unqualified read fails with `table not found: local_default_items`,
  consistent with storing `DEFAULT` as a literal search-path element instead
  of applying the default GUC value.
