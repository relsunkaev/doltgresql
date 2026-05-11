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

package _go

import (
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
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
					Query:       `INSERT INTO read_only_target VALUES (1);`,
					ExpectedErr: `read-only transaction`,
				},
				{
					Query: `ROLLBACK;`,
				},
			},
		},
	})
}

// TestStartTransactionReadOnlyPreventsWritesRepro guards that START
// TRANSACTION READ ONLY prevents ordinary table writes.
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
					Query:       `INSERT INTO start_read_only_target VALUES (1);`,
					ExpectedErr: `READ ONLY transaction`,
				},
				{
					Query: `ROLLBACK;`,
				},
			},
		},
	})
}

// TestSetTransactionReadWriteAfterQueryRejectedRepro reproduces a transaction
// mode bug: PostgreSQL rejects switching a read-only transaction back to
// read-write after any query has already run in the transaction.
func TestSetTransactionReadWriteAfterQueryRejectedRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SET TRANSACTION READ WRITE rejects after query",
			Assertions: []ScriptTestAssertion{
				{
					Query: `START TRANSACTION READ ONLY;`,
				},
				{
					Query:    `SELECT 1;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:       `SET TRANSACTION READ WRITE;`,
					ExpectedErr: `transaction read-write mode must be set before any query`,
				},
				{
					Query: `ROLLBACK;`,
				},
			},
		},
	})
}

// TestSetTransactionDeferrableAfterQueryRejectedRepro reproduces a transaction
// mode bug: PostgreSQL rejects changing DEFERRABLE mode after any query has
// already run in the transaction.
func TestSetTransactionDeferrableAfterQueryRejectedRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SET TRANSACTION DEFERRABLE rejects after query",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN ISOLATION LEVEL SERIALIZABLE;`,
				},
				{
					Query:    `SELECT 1;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:       `SET TRANSACTION DEFERRABLE;`,
					ExpectedErr: `SET TRANSACTION [NOT] DEFERRABLE must be called before any query`,
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
					Query:    `SELECT 1;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:       `SET TRANSACTION NOT DEFERRABLE;`,
					ExpectedErr: `SET TRANSACTION [NOT] DEFERRABLE must be called before any query`,
				},
				{
					Query: `ROLLBACK;`,
				},
			},
		},
	})
}

// TestStartTransactionReadOnlyRejectsUpdateRepro guards that START
// TRANSACTION READ ONLY prevents persistent UPDATE writes.
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
					Query:       `UPDATE read_only_update_target SET label = 'after' WHERE id = 1;`,
					ExpectedErr: `READ ONLY transaction`,
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query:    `SELECT * FROM read_only_update_target;`,
					Expected: []sql.Row{{1, "before"}},
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
					Query:       `DELETE FROM read_only_delete_target WHERE id = 1;`,
					ExpectedErr: `READ ONLY transaction`,
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query:    `SELECT * FROM read_only_delete_target;`,
					Expected: []sql.Row{{1}},
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
					Query:       `TRUNCATE read_only_truncate_target;`,
					ExpectedErr: `READ ONLY transaction`,
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query:    `SELECT id FROM read_only_truncate_target ORDER BY id;`,
					Expected: []sql.Row{{1}, {2}},
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
					Query:       `CREATE TABLE read_only_create_table (id INT PRIMARY KEY);`,
					ExpectedErr: `READ ONLY transaction`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query:       `SELECT * FROM read_only_create_table;`,
					ExpectedErr: `table not found`,
				},
			},
		},
	})
}

// TestStartTransactionReadOnlyRejectsAlterTableRepro reproduces a read-only
// transaction safety bug: persistent ALTER TABLE is allowed and committed.
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
					Query:       `ALTER TABLE read_only_alter_table ADD COLUMN label TEXT;`,
					ExpectedErr: `READ ONLY transaction`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query:       `SELECT label FROM read_only_alter_table;`,
					ExpectedErr: `column`,
				},
			},
		},
	})
}

// TestStartTransactionReadOnlyRejectsDropTableRepro reproduces a read-only
// transaction safety bug: persistent DROP TABLE is allowed and committed.
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
					Query:       `DROP TABLE read_only_drop_table;`,
					ExpectedErr: `READ ONLY transaction`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query:    `SELECT * FROM read_only_drop_table;`,
					Expected: []sql.Row{{1}},
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
					Query:       `CREATE INDEX read_only_create_index_items_label_idx ON read_only_create_index_items (label);`,
					ExpectedErr: `READ ONLY transaction`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT indexname
						FROM pg_indexes
						WHERE tablename = 'read_only_create_index_items'
							AND indexname = 'read_only_create_index_items_label_idx';`,
					Expected: []sql.Row{},
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
					Query:       `DROP INDEX read_only_drop_index_items_label_idx;`,
					ExpectedErr: `READ ONLY transaction`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT indexname
						FROM pg_indexes
						WHERE tablename = 'read_only_drop_index_items'
							AND indexname = 'read_only_drop_index_items_label_idx';`,
					Expected: []sql.Row{{"read_only_drop_index_items_label_idx"}},
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
					Query:       `CREATE SEQUENCE read_only_create_sequence_seq;`,
					ExpectedErr: `READ ONLY transaction`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query:       `SELECT nextval('read_only_create_sequence_seq');`,
					ExpectedErr: `does not exist`,
				},
			},
		},
	})
}

// TestStartTransactionReadOnlyRejectsDropSequenceRepro reproduces a read-only
// transaction safety bug: persistent DROP SEQUENCE is allowed and committed.
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
					Query:       `DROP SEQUENCE read_only_drop_sequence_seq;`,
					ExpectedErr: `READ ONLY transaction`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query:    `SELECT nextval('read_only_drop_sequence_seq');`,
					Expected: []sql.Row{{1}},
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
					Query:       `INSERT INTO default_read_only_target VALUES (1);`,
					ExpectedErr: `read-only transaction`,
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
					Query:       `SELECT nextval('read_only_persistent_seq');`,
					ExpectedErr: `read-only transaction`,
				},
				{
					Query: `ROLLBACK;`,
				},
			},
		},
	})
}

// TestReadOnlyTransactionRejectsPersistentSequenceSetvalRepro reproduces a
// read-only transaction safety bug for persistent sequence state changes.
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
					Query:       `SELECT setval('read_only_setval_seq', 50);`,
					ExpectedErr: `read-only transaction`,
				},
				{
					Query: `ROLLBACK;`,
				},
			},
		},
	})
}

// TestReadOnlyTransactionAllowsTemporaryTableWritesGuard guards PostgreSQL's
// read-only transaction boundary: writes to temporary tables are allowed because
// they do not mutate persistent state.
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
					Query:    `SELECT id, label FROM read_only_temp_target;`,
					Expected: []sql.Row{{1, "temporary write"}},
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
