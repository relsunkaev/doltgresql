// Copyright 2026 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package postgres16

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSetTransactionReadOnlyPreventsWritesRepro reproduces a data-consistency
// safety bug: Doltgres accepts SET TRANSACTION READ ONLY, but still permits
// writes in that transaction.
func TestSetTransactionReadOnlyPreventsWritesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SET TRANSACTION READ ONLY prevents writes",
			SetUpScript: []string{
				`CREATE TABLE read_only_target (id INT PRIMARY KEY);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SET TRANSACTION READ ONLY;`,
				},
				{
					Query: `INSERT INTO read_only_target VALUES (1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-testsettransactionreadonlypreventswritesrepro-0001-insert-into-read_only_target-values-1",

						// TestStartTransactionReadOnlyPreventsWritesRepro guards that START
						// TRANSACTION READ ONLY prevents ordinary table writes.
						Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK;`,
				},
			},
		},
	})
}

func TestStartTransactionReadOnlyPreventsWritesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "START TRANSACTION READ ONLY prevents writes",
			SetUpScript: []string{
				`CREATE TABLE start_read_only_target (id INT PRIMARY KEY);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `START TRANSACTION READ ONLY;`,
				},
				{
					Query: `INSERT INTO start_read_only_target VALUES (1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-teststarttransactionreadonlypreventswritesrepro-0001-insert-into-start_read_only_target-values-1",

						// TestSetTransactionReadWriteAfterQueryRejectedRepro reproduces a transaction
						// mode bug: PostgreSQL rejects switching a read-only transaction back to
						// read-write after any query has already run in the transaction.
						Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK;`,
				},
			},
		},
	})
}

func TestSetTransactionReadWriteAfterQueryRejectedRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SET TRANSACTION READ WRITE rejects after query",
			Assertions: []ScriptTestAssertion{
				{
					Query: `START TRANSACTION READ ONLY;`,
				},
				{
					Query: `SELECT 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-testsettransactionreadwriteafterqueryrejectedrepro-0001-select-1"},
				},
				{
					Query: `SET TRANSACTION READ WRITE;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-testsettransactionreadwriteafterqueryrejectedrepro-0002-set-transaction-read-write",

						// TestSetTransactionDeferrableAfterQueryRejectedRepro reproduces a transaction
						// mode bug: PostgreSQL rejects changing DEFERRABLE mode after any query has
						// already run in the transaction.
						Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK;`,
				},
			},
		},
	})
}

func TestSetTransactionDeferrableAfterQueryRejectedRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SET TRANSACTION DEFERRABLE rejects after query",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN ISOLATION LEVEL SERIALIZABLE;`,
				},
				{
					Query: `SELECT 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-testsettransactiondeferrableafterqueryrejectedrepro-0001-select-1"},
				},
				{
					Query: `SET TRANSACTION DEFERRABLE;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-testsettransactiondeferrableafterqueryrejectedrepro-0002-set-transaction-deferrable", Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK;`,
				},
			},
		},
		{
			Name: "SET TRANSACTION NOT DEFERRABLE rejects after query",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN ISOLATION LEVEL SERIALIZABLE;`,
				},
				{
					Query: `SELECT 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-testsettransactiondeferrableafterqueryrejectedrepro-0003-select-1"},
				},
				{
					Query: `SET TRANSACTION NOT DEFERRABLE;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-testsettransactiondeferrableafterqueryrejectedrepro-0004-set-transaction-not-deferrable",

						// TestStartTransactionReadOnlyRejectsUpdateRepro guards that START
						// TRANSACTION READ ONLY prevents persistent UPDATE writes.
						Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK;`,
				},
			},
		},
	})
}

func TestStartTransactionReadOnlyRejectsUpdateRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "START TRANSACTION READ ONLY rejects UPDATE",
			SetUpScript: []string{
				`CREATE TABLE read_only_update_target (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO read_only_update_target VALUES (1, 'before');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `START TRANSACTION READ ONLY;`,
				},
				{
					Query: `UPDATE read_only_update_target SET label = 'after' WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-teststarttransactionreadonlyrejectsupdaterepro-0001-update-read_only_update_target-set-label-=", Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT * FROM read_only_update_target;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-teststarttransactionreadonlyrejectsupdaterepro-0002-select-*-from-read_only_update_target"},
				},
			},
		},
	})
}

// TestStartTransactionReadOnlyRejectsDeleteRepro guards that START
// TRANSACTION READ ONLY prevents persistent DELETE writes.
func TestStartTransactionReadOnlyRejectsDeleteRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "START TRANSACTION READ ONLY rejects DELETE",
			SetUpScript: []string{
				`CREATE TABLE read_only_delete_target (id INT PRIMARY KEY);`,
				`INSERT INTO read_only_delete_target VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `START TRANSACTION READ ONLY;`,
				},
				{
					Query: `DELETE FROM read_only_delete_target WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-teststarttransactionreadonlyrejectsdeleterepro-0001-delete-from-read_only_delete_target-where-id", Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT * FROM read_only_delete_target;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-teststarttransactionreadonlyrejectsdeleterepro-0002-select-*-from-read_only_delete_target"},
				},
			},
		},
	})
}

// TestStartTransactionReadOnlyRejectsTruncateRepro reproduces a read-only
// transaction safety bug: persistent TRUNCATE is allowed and is not restored by
// ROLLBACK.
func TestStartTransactionReadOnlyRejectsTruncateRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "START TRANSACTION READ ONLY rejects TRUNCATE",
			SetUpScript: []string{
				`CREATE TABLE read_only_truncate_target (id INT PRIMARY KEY);`,
				`INSERT INTO read_only_truncate_target VALUES (1), (2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `START TRANSACTION READ ONLY;`,
				},
				{
					Query: `TRUNCATE read_only_truncate_target;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-teststarttransactionreadonlyrejectstruncaterepro-0001-truncate-read_only_truncate_target", Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT id FROM read_only_truncate_target ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-teststarttransactionreadonlyrejectstruncaterepro-0002-select-id-from-read_only_truncate_target-order"},
				},
			},
		},
	})
}

// TestStartTransactionReadOnlyRejectsCreateTableRepro reproduces a read-only
// transaction safety bug: persistent CREATE TABLE is allowed and committed.
func TestStartTransactionReadOnlyRejectsCreateTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "START TRANSACTION READ ONLY rejects CREATE TABLE",
			Assertions: []ScriptTestAssertion{
				{
					Query: `START TRANSACTION READ ONLY;`,
				},
				{
					Query: `CREATE TABLE read_only_create_table (id INT PRIMARY KEY);`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-teststarttransactionreadonlyrejectscreatetablerepro-0001-create-table-read_only_create_table-id-int", Compare: "sqlstate"},
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT * FROM read_only_create_table;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-teststarttransactionreadonlyrejectscreatetablerepro-0002-select-*-from-read_only_create_table",

						// TestStartTransactionReadOnlyRejectsAlterTableRepro reproduces a read-only
						// transaction safety bug: persistent ALTER TABLE is allowed and committed.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestStartTransactionReadOnlyRejectsAlterTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "START TRANSACTION READ ONLY rejects ALTER TABLE",
			SetUpScript: []string{
				`CREATE TABLE read_only_alter_table (id INT PRIMARY KEY);`,
				`INSERT INTO read_only_alter_table VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `START TRANSACTION READ ONLY;`,
				},
				{
					Query: `ALTER TABLE read_only_alter_table ADD COLUMN label TEXT;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-teststarttransactionreadonlyrejectsaltertablerepro-0001-alter-table-read_only_alter_table-add-column", Compare: "sqlstate"},
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT label FROM read_only_alter_table;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-teststarttransactionreadonlyrejectsaltertablerepro-0002-select-label-from-read_only_alter_table",

						// TestStartTransactionReadOnlyRejectsDropTableRepro reproduces a read-only
						// transaction safety bug: persistent DROP TABLE is allowed and committed.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestStartTransactionReadOnlyRejectsDropTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "START TRANSACTION READ ONLY rejects DROP TABLE",
			SetUpScript: []string{
				`CREATE TABLE read_only_drop_table (id INT PRIMARY KEY);`,
				`INSERT INTO read_only_drop_table VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `START TRANSACTION READ ONLY;`,
				},
				{
					Query: `DROP TABLE read_only_drop_table;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-teststarttransactionreadonlyrejectsdroptablerepro-0001-drop-table-read_only_drop_table", Compare: "sqlstate"},
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT * FROM read_only_drop_table;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-teststarttransactionreadonlyrejectsdroptablerepro-0002-select-*-from-read_only_drop_table"},
				},
			},
		},
	})
}

// TestStartTransactionReadOnlyRejectsCreateIndexRepro reproduces a read-only
// transaction safety bug: persistent CREATE INDEX is allowed and committed.
func TestStartTransactionReadOnlyRejectsCreateIndexRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "START TRANSACTION READ ONLY rejects CREATE INDEX",
			SetUpScript: []string{
				`CREATE TABLE read_only_create_index_items (id INT PRIMARY KEY, label TEXT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `START TRANSACTION READ ONLY;`,
				},
				{
					Query: `CREATE INDEX read_only_create_index_items_label_idx ON read_only_create_index_items (label);`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-teststarttransactionreadonlyrejectscreateindexrepro-0001-create-index-read_only_create_index_items_label_idx-on-read_only_create_index_items", Compare: "sqlstate"},
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT indexname
						FROM pg_indexes
						WHERE tablename = 'read_only_create_index_items'
							AND indexname = 'read_only_create_index_items_label_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-teststarttransactionreadonlyrejectscreateindexrepro-0002-select-indexname-from-pg_indexes-where"},
				},
			},
		},
	})
}

// TestStartTransactionReadOnlyRejectsDropIndexRepro reproduces a read-only
// transaction safety bug: persistent DROP INDEX is allowed and committed.
func TestStartTransactionReadOnlyRejectsDropIndexRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "START TRANSACTION READ ONLY rejects DROP INDEX",
			SetUpScript: []string{
				`CREATE TABLE read_only_drop_index_items (id INT PRIMARY KEY, label TEXT);`,
				`CREATE INDEX read_only_drop_index_items_label_idx ON read_only_drop_index_items (label);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `START TRANSACTION READ ONLY;`,
				},
				{
					Query: `DROP INDEX read_only_drop_index_items_label_idx;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-teststarttransactionreadonlyrejectsdropindexrepro-0001-drop-index-read_only_drop_index_items_label_idx", Compare: "sqlstate"},
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT indexname
						FROM pg_indexes
						WHERE tablename = 'read_only_drop_index_items'
							AND indexname = 'read_only_drop_index_items_label_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-teststarttransactionreadonlyrejectsdropindexrepro-0002-select-indexname-from-pg_indexes-where"},
				},
			},
		},
	})
}

// TestStartTransactionReadOnlyRejectsCreateSequenceRepro reproduces a
// read-only transaction safety bug: persistent CREATE SEQUENCE is allowed and
// committed.
func TestStartTransactionReadOnlyRejectsCreateSequenceRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "START TRANSACTION READ ONLY rejects CREATE SEQUENCE",
			Assertions: []ScriptTestAssertion{
				{
					Query: `START TRANSACTION READ ONLY;`,
				},
				{
					Query: `CREATE SEQUENCE read_only_create_sequence_seq;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-teststarttransactionreadonlyrejectscreatesequencerepro-0001-create-sequence-read_only_create_sequence_seq", Compare: "sqlstate"},
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT nextval('read_only_create_sequence_seq');`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-teststarttransactionreadonlyrejectscreatesequencerepro-0002-select-nextval-read_only_create_sequence_seq",

						// TestStartTransactionReadOnlyRejectsDropSequenceRepro reproduces a read-only
						// transaction safety bug: persistent DROP SEQUENCE is allowed and committed.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestStartTransactionReadOnlyRejectsDropSequenceRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "START TRANSACTION READ ONLY rejects DROP SEQUENCE",
			SetUpScript: []string{
				`CREATE SEQUENCE read_only_drop_sequence_seq;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `START TRANSACTION READ ONLY;`,
				},
				{
					Query: `DROP SEQUENCE read_only_drop_sequence_seq;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-teststarttransactionreadonlyrejectsdropsequencerepro-0001-drop-sequence-read_only_drop_sequence_seq", Compare: "sqlstate"},
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT nextval('read_only_drop_sequence_seq');`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-teststarttransactionreadonlyrejectsdropsequencerepro-0002-select-nextval-read_only_drop_sequence_seq"},
				},
			},
		},
	})
}

// TestDefaultTransactionReadOnlyPreventsWritesRepro reproduces the same
// read-only transaction safety bug through default_transaction_read_only.
func TestDefaultTransactionReadOnlyPreventsWritesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "default_transaction_read_only prevents writes",
			SetUpScript: []string{
				`CREATE TABLE default_read_only_target (id INT PRIMARY KEY);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SET default_transaction_read_only TO on;`,
				},
				{
					Query: `BEGIN;`,
				},
				{
					Query: `INSERT INTO default_read_only_target VALUES (1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-testdefaulttransactionreadonlypreventswritesrepro-0001-insert-into-default_read_only_target-values-1",

						// TestBeginReadWriteOverridesDefaultReadOnlyGuard guards PostgreSQL's
						// transaction mode precedence: an explicit READ WRITE mode on BEGIN overrides
						// default_transaction_read_only for that transaction.
						Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `RESET default_transaction_read_only;`,
				},
			},
		},
	})
}

func TestBeginReadWriteOverridesDefaultReadOnlyGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "BEGIN READ WRITE overrides default_transaction_read_only",
			SetUpScript: []string{
				`CREATE TABLE begin_read_write_default_target (id INT PRIMARY KEY);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SET default_transaction_read_only TO on;`,
				},
				{
					Query: `BEGIN READ WRITE;`,
				},
				{
					Query: `INSERT INTO begin_read_write_default_target VALUES (1);`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT id FROM begin_read_write_default_target;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-testbeginreadwriteoverridesdefaultreadonlyguard-0001-select-id-from-begin_read_write_default_target"},
				},
				{
					Query: `RESET default_transaction_read_only;`,
				},
			},
		},
	})
}

// TestSessionCharacteristicsReadOnlyPreventsWritesRepro reproduces the same
// read-only transaction safety bug through SET SESSION CHARACTERISTICS.
func TestSessionCharacteristicsReadOnlyPreventsWritesRepro(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE TABLE session_read_only_target (id INT PRIMARY KEY);`)
	require.NoError(t, err)

	_, err = connection.Exec(ctx, `SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;`)
	require.NoError(t, err)

	_, err = connection.Exec(ctx, `BEGIN;`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `INSERT INTO session_read_only_target VALUES (1);`)
	require.Error(t, err)
	lowerErr := strings.ToLower(err.Error())
	require.True(t,
		strings.Contains(lowerErr, "read-only transaction") ||
			strings.Contains(lowerErr, "read only transaction"),
		"expected read-only transaction error, got %v", err)

	_, err = connection.Exec(ctx, `ROLLBACK;`)
	require.NoError(t, err)
}

func TestReadOnlyTransactionAllowsPrepareAndSelectExecuteRepro(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE TABLE read_only_prepare_items (id INT PRIMARY KEY);`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `INSERT INTO read_only_prepare_items VALUES (1);`)
	require.NoError(t, err)

	_, err = connection.Exec(ctx, `START TRANSACTION READ ONLY;`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `PREPARE read_only_prepare_select AS
		SELECT count(*) FROM read_only_prepare_items;`)
	require.NoError(t, err)

	var count int64
	err = connection.Current.QueryRow(ctx, `EXECUTE read_only_prepare_select;`).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	_, err = connection.Exec(ctx, `PREPARE read_only_prepare_insert(INT) AS
		INSERT INTO read_only_prepare_items VALUES ($1);`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `EXECUTE read_only_prepare_insert(2);`)
	require.Error(t, err)
	lowerErr := strings.ToLower(err.Error())
	require.True(t,
		strings.Contains(lowerErr, "read-only transaction") ||
			strings.Contains(lowerErr, "read only transaction"),
		"expected read-only transaction error, got %v", err)

	_, err = connection.Exec(ctx, `ROLLBACK;`)
	require.NoError(t, err)
}

// TestReadOnlyTransactionRejectsPersistentSequenceNextvalRepro reproduces a
// read-only transaction safety bug for persistent sequence writes.
func TestReadOnlyTransactionRejectsPersistentSequenceNextvalRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "read-only transaction rejects persistent sequence nextval",
			SetUpScript: []string{
				`CREATE SEQUENCE read_only_persistent_seq;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `START TRANSACTION READ ONLY;`,
				},
				{
					Query: `SELECT nextval('read_only_persistent_seq');`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-testreadonlytransactionrejectspersistentsequencenextvalrepro-0001-select-nextval-read_only_persistent_seq",

						// TestReadOnlyTransactionRejectsPersistentSequenceSetvalRepro reproduces a
						// read-only transaction safety bug for persistent sequence state changes.
						Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK;`,
				},
			},
		},
	})
}

func TestReadOnlyTransactionRejectsPersistentSequenceSetvalRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "read-only transaction rejects persistent sequence setval",
			SetUpScript: []string{
				`CREATE SEQUENCE read_only_setval_seq;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `START TRANSACTION READ ONLY;`,
				},
				{
					Query: `SELECT setval('read_only_setval_seq', 50);`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-testreadonlytransactionrejectspersistentsequencesetvalrepro-0001-select-setval-read_only_setval_seq-50",

						// TestReadOnlyTransactionAllowsTemporaryTableWritesGuard guards PostgreSQL's
						// read-only transaction boundary: writes to temporary tables are allowed because
						// they do not mutate persistent state.
						Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK;`,
				},
			},
		},
	})
}

func TestReadOnlyTransactionAllowsTemporaryTableWritesGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "read-only transaction allows temporary table writes",
			SetUpScript: []string{
				`CREATE TEMPORARY TABLE read_only_temp_target (
					id INT PRIMARY KEY,
					label TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `START TRANSACTION READ ONLY;`,
				},
				{
					Query: `INSERT INTO read_only_temp_target VALUES (1, 'temporary write');`,
				},
				{
					Query: `SELECT id, label FROM read_only_temp_target;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-mode-repro-test-testreadonlytransactionallowstemporarytablewritesguard-0001-select-id-label-from-read_only_temp_target"},
				},
				{
					Query: `COMMIT;`,
				},
			},
		},
	})
}

// TestReadOnlyCopyFromFailureAllowsRollbackRepro reproduces a transaction
// recovery bug: after COPY FROM is rejected in a read-only transaction,
// ROLLBACK must still be allowed.
func TestReadOnlyCopyFromFailureAllowsRollbackRepro(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE TABLE read_only_copy_target (
		id INT PRIMARY KEY,
		label TEXT
	);`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `START TRANSACTION READ ONLY;`)
	require.NoError(t, err)

	tag, err := connection.Default.PgConn().CopyFrom(
		ctx,
		strings.NewReader("1\tcopy\n"),
		`COPY read_only_copy_target (id, label) FROM STDIN;`,
	)
	if err == nil {
		var count int64
		require.NoError(t, connection.Default.QueryRow(ctx,
			`SELECT count(*) FROM read_only_copy_target;`,
		).Scan(&count))
		require.Equalf(t, int64(0), count,
			"COPY FROM should fail in a read-only transaction; tag=%s", tag.String())
	}
	require.Error(t, err)
	lowerErr := strings.ToLower(err.Error())
	require.True(t,
		strings.Contains(lowerErr, "read-only transaction") ||
			strings.Contains(lowerErr, "read only transaction") ||
			strings.Contains(lowerErr, "read only"),
		"expected read-only transaction error, got %v", err)

	_, err = connection.Exec(ctx, `ROLLBACK;`)
	require.NoError(t, err)
}
