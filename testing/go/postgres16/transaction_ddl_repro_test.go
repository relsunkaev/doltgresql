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

	"testing"

	"github.com/dolthub/go-mysql-server/sql"
)

// TestRollbackDropsTempTableCreatedInTransactionRepro reproduces a
// transaction correctness bug: a temporary table created inside a transaction
// should be rolled back when that transaction rolls back.
func TestRollbackDropsTempTableCreatedInTransactionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK drops temp table created in transaction",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `CREATE TEMPORARY TABLE rollback_temp_table (a INT);`,
				},
				{
					Query: `INSERT INTO rollback_temp_table VALUES (1);`,
				},
				{
					Query: `SELECT * FROM rollback_temp_table;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackdropstemptablecreatedintransactionrepro-0001-select-*-from-rollback_temp_table"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT * FROM rollback_temp_table;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackdropstemptablecreatedintransactionrepro-0002-select-*-from-rollback_temp_table",

						// TestRollbackDropsTableCreatedInTransactionRepro guards that ordinary table
						// creation is rolled back with the surrounding transaction.
						Compare: "sqlstate"},
				},
				{
					Query: `CREATE TEMPORARY TABLE rollback_temp_table (b INT);`,
				},
			},
		},
	})
}

func TestRollbackDropsTableCreatedInTransactionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK drops table created in transaction",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `CREATE TABLE rollback_regular_table (a INT);`,
				},
				{
					Query: `INSERT INTO rollback_regular_table VALUES (1);`,
				},
				{
					Query: `SELECT * FROM rollback_regular_table;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackdropstablecreatedintransactionrepro-0001-select-*-from-rollback_regular_table"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT * FROM rollback_regular_table;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackdropstablecreatedintransactionrepro-0002-select-*-from-rollback_regular_table",

						// TestRollbackRevertsAlterTableAddColumnRepro guards that ALTER TABLE ADD
						// COLUMN is rolled back with the surrounding transaction.
						Compare: "sqlstate"},
				},
				{
					Query: `CREATE TABLE rollback_regular_table (b INT);`,
				},
			},
		},
	})
}

func TestRollbackRevertsAlterTableAddColumnRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK reverts ALTER TABLE ADD COLUMN",
			SetUpScript: []string{
				`CREATE TABLE rollback_alter_table (id INT PRIMARY KEY);`,
				`INSERT INTO rollback_alter_table VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `ALTER TABLE rollback_alter_table ADD COLUMN v INT;`,
				},
				{
					Query: `UPDATE rollback_alter_table SET v = 10 WHERE id = 1;`,
				},
				{
					Query: `SELECT id, v FROM rollback_alter_table;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrevertsaltertableaddcolumnrepro-0001-select-id-v-from-rollback_alter_table"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT v FROM rollback_alter_table;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrevertsaltertableaddcolumnrepro-0002-select-v-from-rollback_alter_table",

						// TestRollbackRestoresDroppedColumnRepro guards that ALTER TABLE DROP COLUMN
						// is rolled back with the surrounding transaction, including stored values.
						Compare: "sqlstate"},
				},
				{
					Query: `ALTER TABLE rollback_alter_table ADD COLUMN v TEXT;`,
				},
			},
		},
	})
}

func TestRollbackRestoresDroppedColumnRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK restores dropped column",
			SetUpScript: []string{
				`CREATE TABLE rollback_drop_column_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO rollback_drop_column_items VALUES (1, 'before');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `ALTER TABLE rollback_drop_column_items DROP COLUMN label;`,
				},
				{
					Query: `SELECT label FROM rollback_drop_column_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppedcolumnrepro-0001-select-label-from-rollback_drop_column_items", Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT id, label FROM rollback_drop_column_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppedcolumnrepro-0002-select-id-label-from-rollback_drop_column_items"},
				},
			},
		},
	})
}

// TestRollbackRestoresRenamedTableRepro guards that ALTER TABLE RENAME TO is
// rolled back with the surrounding transaction.
func TestRollbackRestoresRenamedTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK restores renamed table",
			SetUpScript: []string{
				`CREATE TABLE rollback_rename_table_old (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO rollback_rename_table_old VALUES (1, 'before');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `ALTER TABLE rollback_rename_table_old RENAME TO rollback_rename_table_new;`,
				},
				{
					Query: `SELECT id, label FROM rollback_rename_table_new;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresrenamedtablerepro-0001-select-id-label-from-rollback_rename_table_new"},
				},
				{
					Query: `SELECT id, label FROM rollback_rename_table_old;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresrenamedtablerepro-0002-select-id-label-from-rollback_rename_table_old", Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT id, label FROM rollback_rename_table_old;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresrenamedtablerepro-0003-select-id-label-from-rollback_rename_table_old"},
				},
				{
					Query: `SELECT id, label FROM rollback_rename_table_new;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresrenamedtablerepro-0004-select-id-label-from-rollback_rename_table_new",

						// TestRollbackRestoresRenamedColumnRepro guards that ALTER TABLE RENAME COLUMN
						// is rolled back with the surrounding transaction.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestRollbackRestoresRenamedColumnRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK restores renamed column",
			SetUpScript: []string{
				`CREATE TABLE rollback_rename_column_items (
					id INT PRIMARY KEY,
					old_label TEXT
				);`,
				`INSERT INTO rollback_rename_column_items VALUES (1, 'before');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `ALTER TABLE rollback_rename_column_items RENAME COLUMN old_label TO new_label;`,
				},
				{
					Query: `SELECT id, new_label FROM rollback_rename_column_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresrenamedcolumnrepro-0001-select-id-new_label-from-rollback_rename_column_items"},
				},
				{
					Query: `SELECT old_label FROM rollback_rename_column_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresrenamedcolumnrepro-0002-select-old_label-from-rollback_rename_column_items", Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT id, old_label FROM rollback_rename_column_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresrenamedcolumnrepro-0003-select-id-old_label-from-rollback_rename_column_items"},
				},
				{
					Query: `SELECT new_label FROM rollback_rename_column_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresrenamedcolumnrepro-0004-select-new_label-from-rollback_rename_column_items",

						// TestRollbackRevertsAddedCheckConstraintRepro guards that ALTER TABLE ADD
						// CONSTRAINT is rolled back with the surrounding transaction.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestRollbackRevertsAddedCheckConstraintRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK reverts added CHECK constraint",
			SetUpScript: []string{
				`CREATE TABLE rollback_add_check_items (
					id INT PRIMARY KEY,
					amount INT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `ALTER TABLE rollback_add_check_items
						ADD CONSTRAINT rollback_add_check_positive CHECK (amount > 0);`,
				},
				{
					Query: `INSERT INTO rollback_add_check_items VALUES (1, -1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrevertsaddedcheckconstraintrepro-0001-insert-into-rollback_add_check_items-values-1", Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `INSERT INTO rollback_add_check_items VALUES (1, -1);`,
				},
				{
					Query: `SELECT id, amount FROM rollback_add_check_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrevertsaddedcheckconstraintrepro-0002-select-id-amount-from-rollback_add_check_items"},
				},
			},
		},
	})
}

// TestRollbackRestoresDroppedCheckConstraintRepro guards that ALTER TABLE DROP
// CONSTRAINT is rolled back with the surrounding transaction.
func TestRollbackRestoresDroppedCheckConstraintRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK restores dropped CHECK constraint",
			SetUpScript: []string{
				`CREATE TABLE rollback_drop_check_items (
					id INT PRIMARY KEY,
					amount INT CONSTRAINT rollback_drop_check_positive CHECK (amount > 0)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `ALTER TABLE rollback_drop_check_items
						DROP CONSTRAINT rollback_drop_check_positive;`,
				},
				{
					Query: `INSERT INTO rollback_drop_check_items VALUES (1, -1);`,
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `INSERT INTO rollback_drop_check_items VALUES (2, -2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppedcheckconstraintrepro-0001-insert-into-rollback_drop_check_items-values-2", Compare: "sqlstate"},
				},
				{
					Query: `SELECT COUNT(*) FROM rollback_drop_check_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppedcheckconstraintrepro-0002-select-count-*-from-rollback_drop_check_items"},
				},
			},
		},
	})
}

// TestRollbackRevertsAddedUniqueConstraintGuard keeps coverage for rolling
// back ALTER TABLE ADD CONSTRAINT UNIQUE with the surrounding transaction.
func TestRollbackRevertsAddedUniqueConstraintGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK reverts added UNIQUE constraint",
			SetUpScript: []string{
				`CREATE TABLE rollback_add_unique_items (
					id INT PRIMARY KEY,
					code INT
				);`,
				`INSERT INTO rollback_add_unique_items VALUES (1, 10);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `ALTER TABLE rollback_add_unique_items
						ADD CONSTRAINT rollback_add_unique_code_key UNIQUE (code);`,
				},
				{
					Query: `INSERT INTO rollback_add_unique_items VALUES (2, 10);`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrevertsaddeduniqueconstraintguard-0001-insert-into-rollback_add_unique_items-values-2", Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `INSERT INTO rollback_add_unique_items VALUES (2, 10);`,
				},
				{
					Query: `SELECT id, code
						FROM rollback_add_unique_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrevertsaddeduniqueconstraintguard-0002-select-id-code-from-rollback_add_unique_items"},
				},
			},
		},
	})
}

// TestRollbackRestoresDroppedUniqueConstraintGuard keeps coverage for rolling
// back ALTER TABLE DROP CONSTRAINT on a UNIQUE constraint.
func TestRollbackRestoresDroppedUniqueConstraintGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK restores dropped UNIQUE constraint",
			SetUpScript: []string{
				`CREATE TABLE rollback_drop_unique_items (
					id INT PRIMARY KEY,
					code INT UNIQUE
				);`,
				`INSERT INTO rollback_drop_unique_items VALUES (1, 10);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `ALTER TABLE rollback_drop_unique_items
						DROP CONSTRAINT rollback_drop_unique_items_code_key;`,
				},
				{
					Query: `INSERT INTO rollback_drop_unique_items VALUES (2, 10);`,
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `INSERT INTO rollback_drop_unique_items VALUES (3, 10);`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppeduniqueconstraintguard-0001-insert-into-rollback_drop_unique_items-values-3", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, code
						FROM rollback_drop_unique_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppeduniqueconstraintguard-0002-select-id-code-from-rollback_drop_unique_items"},
				},
			},
		},
	})
}

// TestRollbackRevertsAddedForeignKeyConstraintGuard keeps coverage for rolling
// back ALTER TABLE ADD CONSTRAINT FOREIGN KEY with the surrounding transaction.
func TestRollbackRevertsAddedForeignKeyConstraintGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK reverts added FOREIGN KEY constraint",
			SetUpScript: []string{
				`CREATE TABLE rollback_add_fk_parents (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE rollback_add_fk_children (
					id INT PRIMARY KEY,
					parent_id INT
				);`,
				`INSERT INTO rollback_add_fk_parents VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `ALTER TABLE rollback_add_fk_children
						ADD CONSTRAINT rollback_add_fk_children_parent_fkey
						FOREIGN KEY (parent_id) REFERENCES rollback_add_fk_parents(id);`,
				},
				{
					Query: `INSERT INTO rollback_add_fk_children VALUES (1, 999);`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrevertsaddedforeignkeyconstraintguard-0001-insert-into-rollback_add_fk_children-values-1", Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `INSERT INTO rollback_add_fk_children VALUES (1, 999);`,
				},
				{
					Query: `SELECT id, parent_id FROM rollback_add_fk_children;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrevertsaddedforeignkeyconstraintguard-0002-select-id-parent_id-from-rollback_add_fk_children"},
				},
			},
		},
	})
}

// TestRollbackRestoresDroppedForeignKeyConstraintGuard keeps coverage for
// rolling back ALTER TABLE DROP CONSTRAINT on a foreign key.
func TestRollbackRestoresDroppedForeignKeyConstraintGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK restores dropped FOREIGN KEY constraint",
			SetUpScript: []string{
				`CREATE TABLE rollback_drop_fk_parents (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE rollback_drop_fk_children (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES rollback_drop_fk_parents(id)
				);`,
				`INSERT INTO rollback_drop_fk_parents VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `ALTER TABLE rollback_drop_fk_children
						DROP CONSTRAINT rollback_drop_fk_children_parent_id_fkey;`,
				},
				{
					Query: `INSERT INTO rollback_drop_fk_children VALUES (1, 999);`,
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `INSERT INTO rollback_drop_fk_children VALUES (2, 999);`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppedforeignkeyconstraintguard-0001-insert-into-rollback_drop_fk_children-values-2",

						// TestRollbackDropsIndexCreatedInTransactionRepro guards that CREATE INDEX is
						// rolled back with the surrounding transaction.
						Compare: "sqlstate"},
				},
				{
					Query: `SELECT COUNT(*) FROM rollback_drop_fk_children;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppedforeignkeyconstraintguard-0002-select-count-*-from-rollback_drop_fk_children"},
				},
			},
		},
	})
}

func TestRollbackDropsIndexCreatedInTransactionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK drops index created in transaction",
			SetUpScript: []string{
				`CREATE TABLE rollback_create_index_items (id INT PRIMARY KEY, v INT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `CREATE INDEX rollback_create_index_items_v_idx
						ON rollback_create_index_items (v);`,
				},
				{
					Query: `SELECT indexname
						FROM pg_indexes
						WHERE tablename = 'rollback_create_index_items'
							AND indexname = 'rollback_create_index_items_v_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackdropsindexcreatedintransactionrepro-0001-select-indexname-from-pg_indexes-where"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT indexname
						FROM pg_indexes
						WHERE tablename = 'rollback_create_index_items'
							AND indexname = 'rollback_create_index_items_v_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackdropsindexcreatedintransactionrepro-0002-select-indexname-from-pg_indexes-where"},
				},
				{
					Query: `CREATE INDEX rollback_create_index_items_v_idx
						ON rollback_create_index_items (v);`,
				},
			},
		},
	})
}

// TestRollbackDropsSequenceCreatedInTransactionRepro guards that CREATE
// SEQUENCE is rolled back with the surrounding transaction.
func TestRollbackDropsSequenceCreatedInTransactionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK drops sequence created in transaction",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `CREATE SEQUENCE rollback_create_sequence_seq;`,
				},
				{
					Query: `SELECT nextval('rollback_create_sequence_seq');`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackdropssequencecreatedintransactionrepro-0001-select-nextval-rollback_create_sequence_seq"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT nextval('rollback_create_sequence_seq');`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackdropssequencecreatedintransactionrepro-0002-select-nextval-rollback_create_sequence_seq",

						// TestRollbackRestoresDroppedIndexRepro guards that DROP INDEX is rolled back
						// with the surrounding transaction.
						Compare: "sqlstate"},
				},
				{
					Query: `CREATE SEQUENCE rollback_create_sequence_seq;`,
				},
			},
		},
	})
}

func TestRollbackRestoresDroppedIndexRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK restores dropped index",
			SetUpScript: []string{
				`CREATE TABLE rollback_drop_index_items (id INT PRIMARY KEY, v INT);`,
				`CREATE INDEX rollback_drop_index_items_v_idx ON rollback_drop_index_items (v);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `DROP INDEX rollback_drop_index_items_v_idx;`,
				},
				{
					Query: `SELECT indexname
						FROM pg_indexes
						WHERE tablename = 'rollback_drop_index_items'
							AND indexname = 'rollback_drop_index_items_v_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppedindexrepro-0001-select-indexname-from-pg_indexes-where"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT indexname
						FROM pg_indexes
						WHERE tablename = 'rollback_drop_index_items'
							AND indexname = 'rollback_drop_index_items_v_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppedindexrepro-0002-select-indexname-from-pg_indexes-where"},
				},
			},
		},
	})
}

// TestRollbackRestoresTruncatedRowsRepro guards that TRUNCATE is rolled back
// with the surrounding transaction.
func TestRollbackRestoresTruncatedRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK restores truncated rows",
			SetUpScript: []string{
				`CREATE TABLE rollback_truncate_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO rollback_truncate_items VALUES
					(1, 'one'),
					(2, 'two');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `TRUNCATE rollback_truncate_items;`,
				},
				{
					Query: `SELECT COUNT(*) FROM rollback_truncate_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestorestruncatedrowsrepro-0001-select-count-*-from-rollback_truncate_items"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT id, label
						FROM rollback_truncate_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestorestruncatedrowsrepro-0002-select-id-label-from-rollback_truncate_items"},
				},
			},
		},
	})
}

// TestRollbackRestoresDroppedSequenceRepro guards that DROP SEQUENCE is rolled
// back with the surrounding transaction.
func TestRollbackRestoresDroppedSequenceRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK restores dropped sequence",
			SetUpScript: []string{
				`CREATE SEQUENCE rollback_drop_sequence_seq START WITH 5;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `DROP SEQUENCE rollback_drop_sequence_seq;`,
				},
				{
					Query: `SELECT nextval('rollback_drop_sequence_seq');`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppedsequencerepro-0001-select-nextval-rollback_drop_sequence_seq", Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT nextval('rollback_drop_sequence_seq');`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppedsequencerepro-0002-select-nextval-rollback_drop_sequence_seq"},
				},
			},
		},
	})
}

// TestRollbackRestoresAlterSequenceOwnedByDependencyRepro guards that ALTER
// SEQUENCE OWNED BY is rolled back with the surrounding transaction.
func TestRollbackRestoresAlterSequenceOwnedByDependencyRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK restores ALTER SEQUENCE OWNED BY dependency",
			SetUpScript: []string{
				`CREATE TABLE rollback_sequence_owned_items (
					id INT PRIMARY KEY
				);`,
				`CREATE SEQUENCE rollback_sequence_owned_seq;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `ALTER SEQUENCE rollback_sequence_owned_seq
						OWNED BY rollback_sequence_owned_items.id;`,
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `DROP TABLE rollback_sequence_owned_items;`,
				},
				{
					Query: `SELECT nextval('rollback_sequence_owned_seq');`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresaltersequenceownedbydependencyrepro-0001-select-nextval-rollback_sequence_owned_seq"},
				},
			},
		},
	})
}

// TestRollbackRestoresAlterSequenceOwnedByNoneDependencyGuard keeps coverage
// for rolling back ALTER SEQUENCE OWNED BY NONE.
func TestRollbackRestoresAlterSequenceOwnedByNoneDependencyGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK restores ALTER SEQUENCE OWNED BY NONE dependency",
			SetUpScript: []string{
				`CREATE TABLE rollback_sequence_owned_none_items (
					id INT PRIMARY KEY
				);`,
				`CREATE SEQUENCE rollback_sequence_owned_none_seq
					OWNED BY rollback_sequence_owned_none_items.id;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `ALTER SEQUENCE rollback_sequence_owned_none_seq
						OWNED BY NONE;`,
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `DROP TABLE rollback_sequence_owned_none_items;`,
				},
				{
					Query: `SELECT nextval('rollback_sequence_owned_none_seq');`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresaltersequenceownedbynonedependencyguard-0001-select-nextval-rollback_sequence_owned_none_seq",

						// TestRollbackDropsFunctionCreatedInTransactionGuard keeps coverage for
						// rolling back CREATE FUNCTION with the surrounding transaction.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestRollbackDropsFunctionCreatedInTransactionGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK drops function created in transaction",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `CREATE FUNCTION rollback_create_function_value()
						RETURNS INT LANGUAGE SQL AS $$ SELECT 7 $$;`,
				},
				{
					Query: `SELECT rollback_create_function_value();`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackdropsfunctioncreatedintransactionguard-0001-select-rollback_create_function_value"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT rollback_create_function_value();`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackdropsfunctioncreatedintransactionguard-0002-select-rollback_create_function_value", Compare: "sqlstate"},
				},
				{
					Query: `CREATE FUNCTION rollback_create_function_value()
						RETURNS INT LANGUAGE SQL AS $$ SELECT 8 $$;`,
				},
			},
		},
	})
}

// TestRollbackRestoresDroppedFunctionGuard keeps coverage for rolling back
// DROP FUNCTION with the surrounding transaction.
func TestRollbackRestoresDroppedFunctionGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK restores dropped function",
			SetUpScript: []string{
				`CREATE FUNCTION rollback_drop_function_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 7 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `DROP FUNCTION rollback_drop_function_value();`,
				},
				{
					Query: `SELECT rollback_drop_function_value();`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppedfunctionguard-0001-select-rollback_drop_function_value", Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT rollback_drop_function_value();`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppedfunctionguard-0002-select-rollback_drop_function_value"},
				},
			},
		},
	})
}

// TestRollbackDropsProcedureCreatedInTransactionGuard keeps coverage for
// rolling back CREATE PROCEDURE with the surrounding transaction.
func TestRollbackDropsProcedureCreatedInTransactionGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK drops procedure created in transaction",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `CREATE PROCEDURE rollback_create_procedure_value()
						LANGUAGE SQL
						AS $$ SELECT 7 $$;`,
				},
				{
					Query: `CALL rollback_create_procedure_value();`,
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `CALL rollback_create_procedure_value();`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackdropsprocedurecreatedintransactionguard-0001-call-rollback_create_procedure_value", Compare: "sqlstate"},
				},
				{
					Query: `CREATE PROCEDURE rollback_create_procedure_value()
						LANGUAGE SQL
						AS $$ SELECT 8 $$;`,
				},
			},
		},
	})
}

// TestRollbackRestoresDroppedProcedureRepro reproduces a transaction persistence
// bug: rolling back DROP PROCEDURE should restore a callable procedure.
func TestRollbackRestoresDroppedProcedureRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK restores dropped procedure",
			SetUpScript: []string{
				`CREATE TABLE rollback_drop_procedure_audit (
					value INT
				);`,
				`CREATE PROCEDURE rollback_drop_procedure_value()
					LANGUAGE SQL
					AS $$ INSERT INTO rollback_drop_procedure_audit VALUES (7) $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `DROP PROCEDURE rollback_drop_procedure_value();`,
				},
				{
					Query: `CALL rollback_drop_procedure_value();`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppedprocedurerepro-0001-call-rollback_drop_procedure_value", Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `CALL rollback_drop_procedure_value();`,
				},
				{
					Query: `SELECT value FROM rollback_drop_procedure_audit;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppedprocedurerepro-0002-select-value-from-rollback_drop_procedure_audit"},
				},
			},
		},
	})
}

// TestRollbackRestoresReplacedFunctionDefinitionRepro reproduces a transaction
// persistence bug: rolling back CREATE OR REPLACE FUNCTION should restore the
// prior function body.
func TestRollbackRestoresReplacedFunctionDefinitionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK restores replaced function definition",
			SetUpScript: []string{
				`CREATE FUNCTION rollback_replace_function_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 7 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT rollback_replace_function_value();`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresreplacedfunctiondefinitionrepro-0001-select-rollback_replace_function_value"},
				},
				{
					Query: `BEGIN;`,
				},
				{
					Query: `CREATE OR REPLACE FUNCTION rollback_replace_function_value()
						RETURNS INT LANGUAGE SQL AS $$ SELECT 8 $$;`,
				},
				{
					Query: `SELECT rollback_replace_function_value();`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresreplacedfunctiondefinitionrepro-0002-select-rollback_replace_function_value"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT rollback_replace_function_value();`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresreplacedfunctiondefinitionrepro-0003-select-rollback_replace_function_value"},
				},
			},
		},
	})
}

// TestRollbackRestoresReplacedProcedureDefinitionRepro reproduces a transaction
// persistence bug: CREATE OR REPLACE PROCEDURE should keep the replacement
// procedure callable and allow rollback to restore the prior body.
func TestRollbackRestoresReplacedProcedureDefinitionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK restores replaced procedure definition",
			SetUpScript: []string{
				`CREATE TABLE rollback_replace_procedure_audit (
					value INT
				);`,
				`CREATE PROCEDURE rollback_replace_procedure_value()
					LANGUAGE SQL
					AS $$ INSERT INTO rollback_replace_procedure_audit VALUES (7) $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `CREATE OR REPLACE PROCEDURE rollback_replace_procedure_value()
						LANGUAGE SQL
						AS $$ INSERT INTO rollback_replace_procedure_audit VALUES (8) $$;`,
				},
				{
					Query: `CALL rollback_replace_procedure_value();`,
				},
				{
					Query: `SELECT value FROM rollback_replace_procedure_audit;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresreplacedproceduredefinitionrepro-0001-select-value-from-rollback_replace_procedure_audit"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `CALL rollback_replace_procedure_value();`,
				},
				{
					Query: `SELECT value FROM rollback_replace_procedure_audit;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresreplacedproceduredefinitionrepro-0002-select-value-from-rollback_replace_procedure_audit"},
				},
			},
		},
	})
}

// TestRollbackRestoresReplacedViewDefinitionRepro reproduces a transaction
// persistence bug: rolling back CREATE OR REPLACE VIEW should restore the
// prior view definition.
func TestRollbackRestoresReplacedViewDefinitionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK restores replaced view definition",
			SetUpScript: []string{
				`CREATE VIEW rollback_replace_view_reader AS
					SELECT 7 AS id;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id FROM rollback_replace_view_reader;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresreplacedviewdefinitionrepro-0001-select-id-from-rollback_replace_view_reader"},
				},
				{
					Query: `BEGIN;`,
				},
				{
					Query: `CREATE OR REPLACE VIEW rollback_replace_view_reader AS
						SELECT 8 AS id;`,
				},
				{
					Query: `SELECT id FROM rollback_replace_view_reader;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresreplacedviewdefinitionrepro-0002-select-id-from-rollback_replace_view_reader"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT id FROM rollback_replace_view_reader;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresreplacedviewdefinitionrepro-0003-select-id-from-rollback_replace_view_reader"},
				},
			},
		},
	})
}

// TestRollbackDropsViewCreatedInTransactionGuard keeps coverage for rolling
// back CREATE VIEW with the surrounding transaction.
func TestRollbackDropsViewCreatedInTransactionGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK drops view created in transaction",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `CREATE VIEW rollback_create_view_reader AS
						SELECT 7 AS id;`,
				},
				{
					Query: `SELECT id FROM rollback_create_view_reader;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackdropsviewcreatedintransactionguard-0001-select-id-from-rollback_create_view_reader"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT id FROM rollback_create_view_reader;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackdropsviewcreatedintransactionguard-0002-select-id-from-rollback_create_view_reader",

						// TestRollbackRestoresDroppedViewGuard keeps coverage for rolling back DROP
						// VIEW with the surrounding transaction.
						Compare: "sqlstate"},
				},
				{
					Query: `CREATE VIEW rollback_create_view_reader AS
						SELECT 8 AS id;`,
				},
			},
		},
	})
}

func TestRollbackRestoresDroppedViewGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK restores dropped view",
			SetUpScript: []string{
				`CREATE VIEW rollback_drop_view_reader AS
					SELECT 7 AS id;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `DROP VIEW rollback_drop_view_reader;`,
				},
				{
					Query: `SELECT id FROM rollback_drop_view_reader;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppedviewguard-0001-select-id-from-rollback_drop_view_reader", Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT id FROM rollback_drop_view_reader;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppedviewguard-0002-select-id-from-rollback_drop_view_reader"},
				},
			},
		},
	})
}

// TestRollbackDropsMaterializedViewCreatedInTransactionGuard keeps coverage for
// rolling back CREATE MATERIALIZED VIEW with the surrounding transaction.
func TestRollbackDropsMaterializedViewCreatedInTransactionGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK drops materialized view created in transaction",
			SetUpScript: []string{
				`CREATE TABLE rollback_create_matview_source (
					id INT PRIMARY KEY
				);`,
				`INSERT INTO rollback_create_matview_source VALUES (7);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `CREATE MATERIALIZED VIEW rollback_create_matview_reader AS
						SELECT id FROM rollback_create_matview_source;`,
				},
				{
					Query: `SELECT id FROM rollback_create_matview_reader;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackdropsmaterializedviewcreatedintransactionguard-0001-select-id-from-rollback_create_matview_reader"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT id FROM rollback_create_matview_reader;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackdropsmaterializedviewcreatedintransactionguard-0002-select-id-from-rollback_create_matview_reader",

						// TestRollbackRestoresDroppedMaterializedViewGuard keeps coverage for rolling
						// back DROP MATERIALIZED VIEW with the surrounding transaction.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestRollbackRestoresDroppedMaterializedViewGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK restores dropped materialized view",
			SetUpScript: []string{
				`CREATE TABLE rollback_drop_matview_source (
					id INT PRIMARY KEY
				);`,
				`INSERT INTO rollback_drop_matview_source VALUES (7);`,
				`CREATE MATERIALIZED VIEW rollback_drop_matview_reader AS
					SELECT id FROM rollback_drop_matview_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `DROP MATERIALIZED VIEW rollback_drop_matview_reader;`,
				},
				{
					Query: `SELECT id FROM rollback_drop_matview_reader;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppedmaterializedviewguard-0001-select-id-from-rollback_drop_matview_reader", Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT id FROM rollback_drop_matview_reader;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppedmaterializedviewguard-0002-select-id-from-rollback_drop_matview_reader"},
				},
			},
		},
	})
}

// TestRollbackRestoresRefreshedMaterializedViewGuard keeps coverage for rolling
// back REFRESH MATERIALIZED VIEW with the surrounding transaction.
func TestRollbackRestoresRefreshedMaterializedViewGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK restores refreshed materialized view",
			SetUpScript: []string{
				`CREATE TABLE rollback_refresh_matview_source (
					id INT PRIMARY KEY
				);`,
				`INSERT INTO rollback_refresh_matview_source VALUES (1);`,
				`CREATE MATERIALIZED VIEW rollback_refresh_matview_reader AS
					SELECT id FROM rollback_refresh_matview_source;`,
				`INSERT INTO rollback_refresh_matview_source VALUES (2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id FROM rollback_refresh_matview_reader;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresrefreshedmaterializedviewguard-0001-select-id-from-rollback_refresh_matview_reader"},
				},
				{
					Query: `BEGIN;`,
				},
				{
					Query: `REFRESH MATERIALIZED VIEW rollback_refresh_matview_reader;`,
				},
				{
					Query: `SELECT id FROM rollback_refresh_matview_reader
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresrefreshedmaterializedviewguard-0002-select-id-from-rollback_refresh_matview_reader-order"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT id FROM rollback_refresh_matview_reader;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresrefreshedmaterializedviewguard-0003-select-id-from-rollback_refresh_matview_reader"},
				},
			},
		},
	})
}

// TestRollbackRestoresRenamedMaterializedViewGuard keeps coverage for rolling
// back ALTER MATERIALIZED VIEW RENAME TO with the surrounding transaction.
func TestRollbackRestoresRenamedMaterializedViewGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK restores renamed materialized view",
			SetUpScript: []string{
				`CREATE TABLE rollback_rename_matview_source (
					id INT PRIMARY KEY
				);`,
				`INSERT INTO rollback_rename_matview_source VALUES (7);`,
				`CREATE MATERIALIZED VIEW rollback_rename_matview_old AS
					SELECT id FROM rollback_rename_matview_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `ALTER MATERIALIZED VIEW rollback_rename_matview_old
						RENAME TO rollback_rename_matview_new;`,
				},
				{
					Query: `SELECT id FROM rollback_rename_matview_new;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresrenamedmaterializedviewguard-0001-select-id-from-rollback_rename_matview_new"},
				},
				{
					Query: `SELECT id FROM rollback_rename_matview_old;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresrenamedmaterializedviewguard-0002-select-id-from-rollback_rename_matview_old", Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT id FROM rollback_rename_matview_old;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresrenamedmaterializedviewguard-0003-select-id-from-rollback_rename_matview_old"},
				},
				{
					Query: `SELECT id FROM rollback_rename_matview_new;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresrenamedmaterializedviewguard-0004-select-id-from-rollback_rename_matview_new",

						// TestRollbackRestoresRenamedMaterializedViewColumnGuard keeps coverage for
						// rolling back ALTER MATERIALIZED VIEW RENAME COLUMN with the surrounding
						// transaction.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestRollbackRestoresRenamedMaterializedViewColumnGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK restores renamed materialized view column",
			SetUpScript: []string{
				`CREATE TABLE rollback_rename_matview_column_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO rollback_rename_matview_column_source VALUES (1, 'before');`,
				`CREATE MATERIALIZED VIEW rollback_rename_matview_column_reader AS
					SELECT id, label FROM rollback_rename_matview_column_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `ALTER MATERIALIZED VIEW rollback_rename_matview_column_reader
						RENAME COLUMN label TO renamed_label;`,
				},
				{
					Query: `SELECT id, renamed_label FROM rollback_rename_matview_column_reader;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresrenamedmaterializedviewcolumnguard-0001-select-id-renamed_label-from-rollback_rename_matview_column_reader"},
				},
				{
					Query: `SELECT label FROM rollback_rename_matview_column_reader;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresrenamedmaterializedviewcolumnguard-0002-select-label-from-rollback_rename_matview_column_reader", Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT id, label FROM rollback_rename_matview_column_reader;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresrenamedmaterializedviewcolumnguard-0003-select-id-label-from-rollback_rename_matview_column_reader"},
				},
				{
					Query: `SELECT renamed_label FROM rollback_rename_matview_column_reader;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresrenamedmaterializedviewcolumnguard-0004-select-renamed_label-from-rollback_rename_matview_column_reader",

						// TestRollbackDropsTriggerCreatedInTransactionGuard keeps coverage for
						// rolling back CREATE TRIGGER with the surrounding transaction.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestRollbackDropsTriggerCreatedInTransactionGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK drops trigger created in transaction",
			SetUpScript: []string{
				`CREATE TABLE rollback_create_trigger_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE FUNCTION rollback_create_trigger_func()
					RETURNS TRIGGER AS $$
					BEGIN
						NEW.label := 'triggered';
						RETURN NEW;
					END;
					$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `CREATE TRIGGER rollback_create_trigger
						BEFORE INSERT ON rollback_create_trigger_items
						FOR EACH ROW EXECUTE FUNCTION rollback_create_trigger_func();`,
				},
				{
					Query: `INSERT INTO rollback_create_trigger_items VALUES (1, 'plain');`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackdropstriggercreatedintransactionguard-0001-insert-into-rollback_create_trigger_items-values-1"},
				},
				{
					Query: `SELECT label FROM rollback_create_trigger_items WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackdropstriggercreatedintransactionguard-0002-select-label-from-rollback_create_trigger_items-where"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `INSERT INTO rollback_create_trigger_items VALUES (2, 'plain');`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackdropstriggercreatedintransactionguard-0003-insert-into-rollback_create_trigger_items-values-2"},
				},
				{
					Query: `SELECT label FROM rollback_create_trigger_items WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackdropstriggercreatedintransactionguard-0004-select-label-from-rollback_create_trigger_items-where"},
				},
			},
		},
	})
}

// TestRollbackRestoresDroppedTriggerGuard keeps coverage for rolling back DROP
// TRIGGER with the surrounding transaction.
func TestRollbackRestoresDroppedTriggerGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK restores dropped trigger",
			SetUpScript: []string{
				`CREATE TABLE rollback_drop_trigger_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE FUNCTION rollback_drop_trigger_func()
					RETURNS TRIGGER AS $$
					BEGIN
						NEW.label := 'triggered';
						RETURN NEW;
					END;
					$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER rollback_drop_trigger
					BEFORE INSERT ON rollback_drop_trigger_items
					FOR EACH ROW EXECUTE FUNCTION rollback_drop_trigger_func();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `DROP TRIGGER rollback_drop_trigger ON rollback_drop_trigger_items;`,
				},
				{
					Query: `INSERT INTO rollback_drop_trigger_items VALUES (1, 'plain');`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppedtriggerguard-0001-insert-into-rollback_drop_trigger_items-values-1"},
				},
				{
					Query: `SELECT label FROM rollback_drop_trigger_items WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppedtriggerguard-0002-select-label-from-rollback_drop_trigger_items-where"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `INSERT INTO rollback_drop_trigger_items VALUES (2, 'plain');`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppedtriggerguard-0003-insert-into-rollback_drop_trigger_items-values-2"},
				},
				{
					Query: `SELECT label FROM rollback_drop_trigger_items WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppedtriggerguard-0004-select-label-from-rollback_drop_trigger_items-where"},
				},
			},
		},
	})
}

// TestRollbackRestoresReplacedTriggerDefinitionGuard keeps coverage for
// rolling back CREATE OR REPLACE TRIGGER with the surrounding transaction.
func TestRollbackRestoresReplacedTriggerDefinitionGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK restores replaced trigger definition",
			SetUpScript: []string{
				`CREATE TABLE rollback_replace_trigger_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE FUNCTION rollback_replace_trigger_seven()
					RETURNS TRIGGER AS $$
					BEGIN
						NEW.label := 'seven';
						RETURN NEW;
					END;
					$$ LANGUAGE plpgsql;`,
				`CREATE FUNCTION rollback_replace_trigger_eight()
					RETURNS TRIGGER AS $$
					BEGIN
						NEW.label := 'eight';
						RETURN NEW;
					END;
					$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER rollback_replace_trigger
					BEFORE INSERT ON rollback_replace_trigger_items
					FOR EACH ROW EXECUTE FUNCTION rollback_replace_trigger_seven();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `CREATE OR REPLACE TRIGGER rollback_replace_trigger
						BEFORE INSERT ON rollback_replace_trigger_items
						FOR EACH ROW EXECUTE FUNCTION rollback_replace_trigger_eight();`,
				},
				{
					Query: `INSERT INTO rollback_replace_trigger_items VALUES (1, 'plain');`,
				},
				{
					Query: `SELECT label FROM rollback_replace_trigger_items WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresreplacedtriggerdefinitionguard-0001-select-label-from-rollback_replace_trigger_items-where"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `INSERT INTO rollback_replace_trigger_items VALUES (1, 'plain');`,
				},
				{
					Query: `SELECT label FROM rollback_replace_trigger_items WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresreplacedtriggerdefinitionguard-0002-select-label-from-rollback_replace_trigger_items-where"},
				},
			},
		},
	})
}

// TestRollbackRestoresDroppedTableRepro guards that DROP TABLE is rolled back
// with the surrounding transaction.
func TestRollbackRestoresDroppedTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK restores dropped table",
			SetUpScript: []string{
				`CREATE TABLE rollback_drop_table (id INT PRIMARY KEY, v TEXT);`,
				`INSERT INTO rollback_drop_table VALUES (1, 'before');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `DROP TABLE rollback_drop_table;`,
				},
				{
					Query: `SELECT * FROM rollback_drop_table;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppedtablerepro-0001-select-*-from-rollback_drop_table", Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT * FROM rollback_drop_table;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppedtablerepro-0002-select-*-from-rollback_drop_table"},
				},
			},
		},
	})
}

// TestRollbackToSavepointDropsTableCreatedAfterSavepointRepro guards that
// ordinary table creation is rolled back to a savepoint.
func TestRollbackToSavepointDropsTableCreatedAfterSavepointRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT drops table created after savepoint",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `CREATE TABLE savepoint_create_table (a INT);`,
				},
				{
					Query: `INSERT INTO savepoint_create_table VALUES (1);`,
				},
				{
					Query: `SELECT * FROM savepoint_create_table;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointdropstablecreatedaftersavepointrepro-0001-select-*-from-savepoint_create_table"},
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT * FROM savepoint_create_table;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointdropstablecreatedaftersavepointrepro-0002-select-*-from-savepoint_create_table",

						// TestRollbackToSavepointRevertsAlterTableAddColumnRepro guards that ALTER
						// TABLE ADD COLUMN is rolled back to a savepoint.
						Compare: "sqlstate"},
				},
				{
					Query: `CREATE TABLE savepoint_create_table (b INT);`,
				},
			},
		},
	})
}

func TestRollbackToSavepointRevertsAlterTableAddColumnRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT reverts ALTER TABLE ADD COLUMN",
			SetUpScript: []string{
				`CREATE TABLE savepoint_alter_table (id INT PRIMARY KEY);`,
				`INSERT INTO savepoint_alter_table VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `ALTER TABLE savepoint_alter_table ADD COLUMN v INT;`,
				},
				{
					Query: `UPDATE savepoint_alter_table SET v = 10 WHERE id = 1;`,
				},
				{
					Query: `SELECT id, v FROM savepoint_alter_table;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrevertsaltertableaddcolumnrepro-0001-select-id-v-from-savepoint_alter_table"},
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT v FROM savepoint_alter_table;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrevertsaltertableaddcolumnrepro-0002-select-v-from-savepoint_alter_table",

						// TestRollbackToSavepointRestoresDroppedColumnRepro guards that ALTER TABLE
						// DROP COLUMN is rolled back to a savepoint, including stored values.
						Compare: "sqlstate"},
				},
				{
					Query: `ALTER TABLE savepoint_alter_table ADD COLUMN v TEXT;`,
				},
			},
		},
	})
}

func TestRollbackToSavepointRestoresDroppedColumnRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT restores dropped column",
			SetUpScript: []string{
				`CREATE TABLE savepoint_drop_column_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO savepoint_drop_column_items VALUES (1, 'before');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `ALTER TABLE savepoint_drop_column_items DROP COLUMN label;`,
				},
				{
					Query: `SELECT label FROM savepoint_drop_column_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresdroppedcolumnrepro-0001-select-label-from-savepoint_drop_column_items", Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT id, label FROM savepoint_drop_column_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresdroppedcolumnrepro-0002-select-id-label-from-savepoint_drop_column_items"},
				},
			},
		},
	})
}

// TestRollbackToSavepointRestoresRenamedTableRepro guards that ALTER TABLE
// RENAME TO is rolled back to a savepoint.
func TestRollbackToSavepointRestoresRenamedTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT restores renamed table",
			SetUpScript: []string{
				`CREATE TABLE savepoint_rename_table_old (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO savepoint_rename_table_old VALUES (1, 'before');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `ALTER TABLE savepoint_rename_table_old RENAME TO savepoint_rename_table_new;`,
				},
				{
					Query: `SELECT id, label FROM savepoint_rename_table_new;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresrenamedtablerepro-0001-select-id-label-from-savepoint_rename_table_new"},
				},
				{
					Query: `SELECT id, label FROM savepoint_rename_table_old;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresrenamedtablerepro-0002-select-id-label-from-savepoint_rename_table_old", Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT id, label FROM savepoint_rename_table_old;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresrenamedtablerepro-0003-select-id-label-from-savepoint_rename_table_old"},
				},
				{
					Query: `SELECT id, label FROM savepoint_rename_table_new;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresrenamedtablerepro-0004-select-id-label-from-savepoint_rename_table_new",

						// TestRollbackToSavepointRestoresRenamedColumnRepro guards that ALTER TABLE
						// RENAME COLUMN is rolled back to a savepoint.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestRollbackToSavepointRestoresRenamedColumnRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT restores renamed column",
			SetUpScript: []string{
				`CREATE TABLE savepoint_rename_column_items (
					id INT PRIMARY KEY,
					old_label TEXT
				);`,
				`INSERT INTO savepoint_rename_column_items VALUES (1, 'before');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `ALTER TABLE savepoint_rename_column_items RENAME COLUMN old_label TO new_label;`,
				},
				{
					Query: `SELECT id, new_label FROM savepoint_rename_column_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresrenamedcolumnrepro-0001-select-id-new_label-from-savepoint_rename_column_items"},
				},
				{
					Query: `SELECT old_label FROM savepoint_rename_column_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresrenamedcolumnrepro-0002-select-old_label-from-savepoint_rename_column_items", Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT id, old_label FROM savepoint_rename_column_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresrenamedcolumnrepro-0003-select-id-old_label-from-savepoint_rename_column_items"},
				},
				{
					Query: `SELECT new_label FROM savepoint_rename_column_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresrenamedcolumnrepro-0004-select-new_label-from-savepoint_rename_column_items",

						// TestRollbackToSavepointRevertsAddedCheckConstraintRepro guards that ALTER
						// TABLE ADD CONSTRAINT is rolled back to a savepoint.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestRollbackToSavepointRevertsAddedCheckConstraintRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT reverts added CHECK constraint",
			SetUpScript: []string{
				`CREATE TABLE savepoint_add_check_items (
					id INT PRIMARY KEY,
					amount INT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `ALTER TABLE savepoint_add_check_items
						ADD CONSTRAINT savepoint_add_check_positive CHECK (amount > 0);`,
				},
				{
					Query: `INSERT INTO savepoint_add_check_items VALUES (1, -1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrevertsaddedcheckconstraintrepro-0001-insert-into-savepoint_add_check_items-values-1", Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `INSERT INTO savepoint_add_check_items VALUES (1, -1);`,
				},
				{
					Query: `SELECT id, amount FROM savepoint_add_check_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrevertsaddedcheckconstraintrepro-0002-select-id-amount-from-savepoint_add_check_items"},
				},
			},
		},
	})
}

// TestRollbackToSavepointRestoresDroppedCheckConstraintRepro guards that ALTER
// TABLE DROP CONSTRAINT is rolled back to a savepoint.
func TestRollbackToSavepointRestoresDroppedCheckConstraintRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT restores dropped CHECK constraint",
			SetUpScript: []string{
				`CREATE TABLE savepoint_drop_check_items (
					id INT PRIMARY KEY,
					amount INT CONSTRAINT savepoint_drop_check_positive CHECK (amount > 0)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `ALTER TABLE savepoint_drop_check_items
						DROP CONSTRAINT savepoint_drop_check_positive;`,
				},
				{
					Query: `INSERT INTO savepoint_drop_check_items VALUES (1, -1);`,
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `INSERT INTO savepoint_drop_check_items VALUES (2, -2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresdroppedcheckconstraintrepro-0001-insert-into-savepoint_drop_check_items-values-2",

						// TestRollbackToSavepointRevertsAddedUniqueConstraintGuard keeps coverage for
						// rolling back ALTER TABLE ADD CONSTRAINT UNIQUE to a savepoint.
						Compare: "sqlstate"},
				},
				{
					Query: `SELECT COUNT(*) FROM savepoint_drop_check_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresdroppedcheckconstraintrepro-0002-select-count-*-from-savepoint_drop_check_items"},
				},
			},
		},
	})
}

func TestRollbackToSavepointRevertsAddedUniqueConstraintGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT reverts added UNIQUE constraint",
			SetUpScript: []string{
				`CREATE TABLE savepoint_add_unique_items (
					id INT PRIMARY KEY,
					code INT
				);`,
				`INSERT INTO savepoint_add_unique_items VALUES (1, 10);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `ALTER TABLE savepoint_add_unique_items
						ADD CONSTRAINT savepoint_add_unique_code_key UNIQUE (code);`,
				},
				{
					Query: `INSERT INTO savepoint_add_unique_items VALUES (2, 10);`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrevertsaddeduniqueconstraintguard-0001-insert-into-savepoint_add_unique_items-values-2", Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `INSERT INTO savepoint_add_unique_items VALUES (2, 10);`,
				},
				{
					Query: `SELECT id, code
						FROM savepoint_add_unique_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrevertsaddeduniqueconstraintguard-0002-select-id-code-from-savepoint_add_unique_items"},
				},
			},
		},
	})
}

// TestRollbackToSavepointRestoresDroppedUniqueConstraintGuard keeps coverage
// for rolling back ALTER TABLE DROP CONSTRAINT on a UNIQUE constraint to a
// savepoint.
func TestRollbackToSavepointRestoresDroppedUniqueConstraintGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT restores dropped UNIQUE constraint",
			SetUpScript: []string{
				`CREATE TABLE savepoint_drop_unique_items (
					id INT PRIMARY KEY,
					code INT UNIQUE
				);`,
				`INSERT INTO savepoint_drop_unique_items VALUES (1, 10);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `ALTER TABLE savepoint_drop_unique_items
						DROP CONSTRAINT savepoint_drop_unique_items_code_key;`,
				},
				{
					Query: `INSERT INTO savepoint_drop_unique_items VALUES (2, 10);`,
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `INSERT INTO savepoint_drop_unique_items VALUES (3, 10);`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresdroppeduniqueconstraintguard-0001-insert-into-savepoint_drop_unique_items-values-3", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, code
						FROM savepoint_drop_unique_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresdroppeduniqueconstraintguard-0002-select-id-code-from-savepoint_drop_unique_items"},
				},
			},
		},
	})
}

// TestRollbackToSavepointRevertsAddedForeignKeyConstraintGuard keeps coverage
// for rolling back ALTER TABLE ADD CONSTRAINT FOREIGN KEY to a savepoint.
func TestRollbackToSavepointRevertsAddedForeignKeyConstraintGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT reverts added FOREIGN KEY constraint",
			SetUpScript: []string{
				`CREATE TABLE savepoint_add_fk_parents (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE savepoint_add_fk_children (
					id INT PRIMARY KEY,
					parent_id INT
				);`,
				`INSERT INTO savepoint_add_fk_parents VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `ALTER TABLE savepoint_add_fk_children
						ADD CONSTRAINT savepoint_add_fk_children_parent_fkey
						FOREIGN KEY (parent_id) REFERENCES savepoint_add_fk_parents(id);`,
				},
				{
					Query: `INSERT INTO savepoint_add_fk_children VALUES (1, 999);`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrevertsaddedforeignkeyconstraintguard-0001-insert-into-savepoint_add_fk_children-values-1", Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `INSERT INTO savepoint_add_fk_children VALUES (1, 999);`,
				},
				{
					Query: `SELECT id, parent_id FROM savepoint_add_fk_children;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrevertsaddedforeignkeyconstraintguard-0002-select-id-parent_id-from-savepoint_add_fk_children"},
				},
			},
		},
	})
}

// TestRollbackToSavepointRestoresDroppedForeignKeyConstraintGuard keeps
// coverage for rolling back ALTER TABLE DROP CONSTRAINT on a foreign key to a
// savepoint.
func TestRollbackToSavepointRestoresDroppedForeignKeyConstraintGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT restores dropped FOREIGN KEY constraint",
			SetUpScript: []string{
				`CREATE TABLE savepoint_drop_fk_parents (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE savepoint_drop_fk_children (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES savepoint_drop_fk_parents(id)
				);`,
				`INSERT INTO savepoint_drop_fk_parents VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `ALTER TABLE savepoint_drop_fk_children
						DROP CONSTRAINT savepoint_drop_fk_children_parent_id_fkey;`,
				},
				{
					Query: `INSERT INTO savepoint_drop_fk_children VALUES (1, 999);`,
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `INSERT INTO savepoint_drop_fk_children VALUES (2, 999);`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresdroppedforeignkeyconstraintguard-0001-insert-into-savepoint_drop_fk_children-values-2",

						// TestRollbackToSavepointRestoresDroppedTableRepro guards that DROP TABLE is
						// rolled back to a savepoint.
						Compare: "sqlstate"},
				},
				{
					Query: `SELECT COUNT(*) FROM savepoint_drop_fk_children;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresdroppedforeignkeyconstraintguard-0002-select-count-*-from-savepoint_drop_fk_children"},
				},
			},
		},
	})
}

func TestRollbackToSavepointRestoresDroppedTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT restores dropped table",
			SetUpScript: []string{
				`CREATE TABLE savepoint_drop_table (id INT PRIMARY KEY, v TEXT);`,
				`INSERT INTO savepoint_drop_table VALUES (1, 'before');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `DROP TABLE savepoint_drop_table;`,
				},
				{
					Query: `SELECT * FROM savepoint_drop_table;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresdroppedtablerepro-0001-select-*-from-savepoint_drop_table", Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT * FROM savepoint_drop_table;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresdroppedtablerepro-0002-select-*-from-savepoint_drop_table"},
				},
			},
		},
	})
}

// TestRollbackToSavepointDropsIndexCreatedAfterSavepointRepro guards that
// CREATE INDEX is rolled back to a savepoint.
func TestRollbackToSavepointDropsIndexCreatedAfterSavepointRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT drops index created after savepoint",
			SetUpScript: []string{
				`CREATE TABLE savepoint_create_index_items (id INT PRIMARY KEY, v INT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `CREATE INDEX savepoint_create_index_items_v_idx
						ON savepoint_create_index_items (v);`,
				},
				{
					Query: `SELECT indexname
						FROM pg_indexes
						WHERE tablename = 'savepoint_create_index_items'
							AND indexname = 'savepoint_create_index_items_v_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointdropsindexcreatedaftersavepointrepro-0001-select-indexname-from-pg_indexes-where"},
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT indexname
						FROM pg_indexes
						WHERE tablename = 'savepoint_create_index_items'
							AND indexname = 'savepoint_create_index_items_v_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointdropsindexcreatedaftersavepointrepro-0002-select-indexname-from-pg_indexes-where"},
				},
				{
					Query: `CREATE INDEX savepoint_create_index_items_v_idx
						ON savepoint_create_index_items (v);`,
				},
			},
		},
	})
}

// TestRollbackToSavepointRestoresDroppedIndexRepro guards that DROP INDEX is
// rolled back to a savepoint.
func TestRollbackToSavepointRestoresDroppedIndexRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT restores dropped index",
			SetUpScript: []string{
				`CREATE TABLE savepoint_drop_index_items (id INT PRIMARY KEY, v INT);`,
				`CREATE INDEX savepoint_drop_index_items_v_idx ON savepoint_drop_index_items (v);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `DROP INDEX savepoint_drop_index_items_v_idx;`,
				},
				{
					Query: `SELECT indexname
						FROM pg_indexes
						WHERE tablename = 'savepoint_drop_index_items'
							AND indexname = 'savepoint_drop_index_items_v_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresdroppedindexrepro-0001-select-indexname-from-pg_indexes-where"},
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT indexname
						FROM pg_indexes
						WHERE tablename = 'savepoint_drop_index_items'
							AND indexname = 'savepoint_drop_index_items_v_idx';`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresdroppedindexrepro-0002-select-indexname-from-pg_indexes-where"},
				},
			},
		},
	})
}

// TestRollbackToSavepointDropsSequenceCreatedAfterSavepointRepro guards that
// CREATE SEQUENCE is rolled back to a savepoint.
func TestRollbackToSavepointDropsSequenceCreatedAfterSavepointRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT drops sequence created after savepoint",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `CREATE SEQUENCE savepoint_create_sequence_seq;`,
				},
				{
					Query: `SELECT nextval('savepoint_create_sequence_seq');`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointdropssequencecreatedaftersavepointrepro-0001-select-nextval-savepoint_create_sequence_seq"},
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT nextval('savepoint_create_sequence_seq');`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointdropssequencecreatedaftersavepointrepro-0002-select-nextval-savepoint_create_sequence_seq",

						// TestRollbackToSavepointRestoresDroppedSequenceRepro guards that DROP
						// SEQUENCE is rolled back to a savepoint.
						Compare: "sqlstate"},
				},
				{
					Query: `CREATE SEQUENCE savepoint_create_sequence_seq;`,
				},
			},
		},
	})
}

func TestRollbackToSavepointRestoresDroppedSequenceRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT restores dropped sequence",
			SetUpScript: []string{
				`CREATE SEQUENCE savepoint_drop_sequence_seq START WITH 5;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `DROP SEQUENCE savepoint_drop_sequence_seq;`,
				},
				{
					Query: `SELECT nextval('savepoint_drop_sequence_seq');`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresdroppedsequencerepro-0001-select-nextval-savepoint_drop_sequence_seq", Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT nextval('savepoint_drop_sequence_seq');`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresdroppedsequencerepro-0002-select-nextval-savepoint_drop_sequence_seq"},
				},
			},
		},
	})
}

// TestRollbackToSavepointRestoresAlterSequenceOwnedByDependencyRepro guards
// that ALTER SEQUENCE OWNED BY is rolled back to a savepoint.
func TestRollbackToSavepointRestoresAlterSequenceOwnedByDependencyRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT restores ALTER SEQUENCE OWNED BY dependency",
			SetUpScript: []string{
				`CREATE TABLE savepoint_sequence_owned_items (
					id INT PRIMARY KEY
				);`,
				`CREATE SEQUENCE savepoint_sequence_owned_seq;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `ALTER SEQUENCE savepoint_sequence_owned_seq
						OWNED BY savepoint_sequence_owned_items.id;`,
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `DROP TABLE savepoint_sequence_owned_items;`,
				},
				{
					Query: `SELECT nextval('savepoint_sequence_owned_seq');`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresaltersequenceownedbydependencyrepro-0001-select-nextval-savepoint_sequence_owned_seq"},
				},
			},
		},
	})
}

// TestRollbackToSavepointRestoresAlterSequenceOwnedByNoneDependencyGuard keeps
// coverage for rolling back ALTER SEQUENCE OWNED BY NONE to a savepoint.
func TestRollbackToSavepointRestoresAlterSequenceOwnedByNoneDependencyGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT restores ALTER SEQUENCE OWNED BY NONE dependency",
			SetUpScript: []string{
				`CREATE TABLE savepoint_sequence_owned_none_items (
					id INT PRIMARY KEY
				);`,
				`CREATE SEQUENCE savepoint_sequence_owned_none_seq
					OWNED BY savepoint_sequence_owned_none_items.id;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `ALTER SEQUENCE savepoint_sequence_owned_none_seq
						OWNED BY NONE;`,
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `DROP TABLE savepoint_sequence_owned_none_items;`,
				},
				{
					Query: `SELECT nextval('savepoint_sequence_owned_none_seq');`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresaltersequenceownedbynonedependencyguard-0001-select-nextval-savepoint_sequence_owned_none_seq",

						// TestRollbackToSavepointRestoresReplacedFunctionDefinitionRepro reproduces a
						// transaction persistence bug: rolling back CREATE OR REPLACE FUNCTION to a
						// savepoint should restore the prior function body.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestRollbackToSavepointRestoresReplacedFunctionDefinitionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT restores replaced function definition",
			SetUpScript: []string{
				`CREATE FUNCTION savepoint_replace_function_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 7 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `CREATE OR REPLACE FUNCTION savepoint_replace_function_value()
						RETURNS INT LANGUAGE SQL AS $$ SELECT 8 $$;`,
				},
				{
					Query: `SELECT savepoint_replace_function_value();`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresreplacedfunctiondefinitionrepro-0001-select-savepoint_replace_function_value"},
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT savepoint_replace_function_value();`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresreplacedfunctiondefinitionrepro-0002-select-savepoint_replace_function_value"},
				},
			},
		},
	})
}

// TestRollbackToSavepointRestoresReplacedProcedureDefinitionRepro reproduces a
// transaction persistence bug: CREATE OR REPLACE PROCEDURE after a savepoint
// should keep the replacement procedure callable and allow savepoint rollback
// to restore the prior body.
func TestRollbackToSavepointRestoresReplacedProcedureDefinitionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT restores replaced procedure definition",
			SetUpScript: []string{
				`CREATE TABLE savepoint_replace_procedure_audit (
					value INT
				);`,
				`CREATE PROCEDURE savepoint_replace_procedure_value()
					LANGUAGE SQL
					AS $$ INSERT INTO savepoint_replace_procedure_audit VALUES (7) $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `CREATE OR REPLACE PROCEDURE savepoint_replace_procedure_value()
						LANGUAGE SQL
						AS $$ INSERT INTO savepoint_replace_procedure_audit VALUES (8) $$;`,
				},
				{
					Query: `CALL savepoint_replace_procedure_value();`,
				},
				{
					Query: `SELECT value FROM savepoint_replace_procedure_audit;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresreplacedproceduredefinitionrepro-0001-select-value-from-savepoint_replace_procedure_audit"},
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `CALL savepoint_replace_procedure_value();`,
				},
				{
					Query: `SELECT value FROM savepoint_replace_procedure_audit;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresreplacedproceduredefinitionrepro-0002-select-value-from-savepoint_replace_procedure_audit"},
				},
			},
		},
	})
}

// TestRollbackToSavepointRestoresReplacedViewDefinitionRepro reproduces a
// transaction persistence bug: rolling back CREATE OR REPLACE VIEW to a
// savepoint should restore the prior view definition.
func TestRollbackToSavepointRestoresReplacedViewDefinitionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT restores replaced view definition",
			SetUpScript: []string{
				`CREATE VIEW savepoint_replace_view_reader AS
					SELECT 7 AS id;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `CREATE OR REPLACE VIEW savepoint_replace_view_reader AS
						SELECT 8 AS id;`,
				},
				{
					Query: `SELECT id FROM savepoint_replace_view_reader;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresreplacedviewdefinitionrepro-0001-select-id-from-savepoint_replace_view_reader"},
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT id FROM savepoint_replace_view_reader;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresreplacedviewdefinitionrepro-0002-select-id-from-savepoint_replace_view_reader"},
				},
			},
		},
	})
}

// TestRollbackToSavepointRestoresReplacedTriggerDefinitionRepro reproduces a
// transaction persistence bug: rolling back CREATE OR REPLACE TRIGGER to a
// savepoint should restore the prior trigger function binding.
func TestRollbackToSavepointRestoresReplacedTriggerDefinitionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT restores replaced trigger definition",
			SetUpScript: []string{
				`CREATE TABLE savepoint_replace_trigger_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE FUNCTION savepoint_replace_trigger_seven()
					RETURNS TRIGGER AS $$
					BEGIN
						NEW.label := 'seven';
						RETURN NEW;
					END;
					$$ LANGUAGE plpgsql;`,
				`CREATE FUNCTION savepoint_replace_trigger_eight()
					RETURNS TRIGGER AS $$
					BEGIN
						NEW.label := 'eight';
						RETURN NEW;
					END;
					$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER savepoint_replace_trigger
					BEFORE INSERT ON savepoint_replace_trigger_items
					FOR EACH ROW EXECUTE FUNCTION savepoint_replace_trigger_seven();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `CREATE OR REPLACE TRIGGER savepoint_replace_trigger
						BEFORE INSERT ON savepoint_replace_trigger_items
						FOR EACH ROW EXECUTE FUNCTION savepoint_replace_trigger_eight();`,
				},
				{
					Query: `INSERT INTO savepoint_replace_trigger_items VALUES (1, 'plain');`,
				},
				{
					Query: `SELECT label FROM savepoint_replace_trigger_items WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresreplacedtriggerdefinitionrepro-0001-select-label-from-savepoint_replace_trigger_items-where"},
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `INSERT INTO savepoint_replace_trigger_items VALUES (1, 'plain');`,
				},
				{
					Query: `SELECT label FROM savepoint_replace_trigger_items WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresreplacedtriggerdefinitionrepro-0002-select-label-from-savepoint_replace_trigger_items-where"},
				},
			},
		},
	})
}

// TestRollbackToSavepointRestoresRefreshedMaterializedViewGuard keeps coverage
// for rolling back REFRESH MATERIALIZED VIEW to a savepoint.
func TestRollbackToSavepointRestoresRefreshedMaterializedViewGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT restores refreshed materialized view",
			SetUpScript: []string{
				`CREATE TABLE savepoint_refresh_matview_source (
					id INT PRIMARY KEY
				);`,
				`INSERT INTO savepoint_refresh_matview_source VALUES (1);`,
				`CREATE MATERIALIZED VIEW savepoint_refresh_matview_reader AS
					SELECT id FROM savepoint_refresh_matview_source;`,
				`INSERT INTO savepoint_refresh_matview_source VALUES (2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `REFRESH MATERIALIZED VIEW savepoint_refresh_matview_reader;`,
				},
				{
					Query: `SELECT id FROM savepoint_refresh_matview_reader
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresrefreshedmaterializedviewguard-0001-select-id-from-savepoint_refresh_matview_reader-order"},
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT id FROM savepoint_refresh_matview_reader;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresrefreshedmaterializedviewguard-0002-select-id-from-savepoint_refresh_matview_reader"},
				},
			},
		},
	})
}

// TestRollbackToSavepointRestoresRenamedMaterializedViewGuard keeps coverage
// for rolling back ALTER MATERIALIZED VIEW RENAME TO to a savepoint.
func TestRollbackToSavepointRestoresRenamedMaterializedViewGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT restores renamed materialized view",
			SetUpScript: []string{
				`CREATE TABLE savepoint_rename_matview_source (
					id INT PRIMARY KEY
				);`,
				`INSERT INTO savepoint_rename_matview_source VALUES (7);`,
				`CREATE MATERIALIZED VIEW savepoint_rename_matview_old AS
					SELECT id FROM savepoint_rename_matview_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `ALTER MATERIALIZED VIEW savepoint_rename_matview_old
						RENAME TO savepoint_rename_matview_new;`,
				},
				{
					Query: `SELECT id FROM savepoint_rename_matview_new;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresrenamedmaterializedviewguard-0001-select-id-from-savepoint_rename_matview_new"},
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT id FROM savepoint_rename_matview_old;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresrenamedmaterializedviewguard-0002-select-id-from-savepoint_rename_matview_old"},
				},
				{
					Query: `SELECT id FROM savepoint_rename_matview_new;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresrenamedmaterializedviewguard-0003-select-id-from-savepoint_rename_matview_new",

						// TestRollbackToSavepointRestoresRenamedMaterializedViewColumnGuard keeps
						// coverage for rolling back ALTER MATERIALIZED VIEW RENAME COLUMN to a
						// savepoint.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestRollbackToSavepointRestoresRenamedMaterializedViewColumnGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT restores renamed materialized view column",
			SetUpScript: []string{
				`CREATE TABLE savepoint_rename_matview_column_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO savepoint_rename_matview_column_source VALUES (1, 'before');`,
				`CREATE MATERIALIZED VIEW savepoint_rename_matview_column_reader AS
					SELECT id, label FROM savepoint_rename_matview_column_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `ALTER MATERIALIZED VIEW savepoint_rename_matview_column_reader
						RENAME COLUMN label TO renamed_label;`,
				},
				{
					Query: `SELECT id, renamed_label FROM savepoint_rename_matview_column_reader;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresrenamedmaterializedviewcolumnguard-0001-select-id-renamed_label-from-savepoint_rename_matview_column_reader"},
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT id, label FROM savepoint_rename_matview_column_reader;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresrenamedmaterializedviewcolumnguard-0002-select-id-label-from-savepoint_rename_matview_column_reader"},
				},
				{
					Query: `SELECT renamed_label FROM savepoint_rename_matview_column_reader;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresrenamedmaterializedviewcolumnguard-0003-select-renamed_label-from-savepoint_rename_matview_column_reader",

						// TestRollbackToSavepointRestoresTruncatedRowsRepro guards that TRUNCATE is
						// rolled back to a savepoint.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestRollbackToSavepointRestoresTruncatedRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT restores truncated rows",
			SetUpScript: []string{
				`CREATE TABLE savepoint_truncate_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO savepoint_truncate_items VALUES
					(1, 'one'),
					(2, 'two');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `TRUNCATE savepoint_truncate_items;`,
				},
				{
					Query: `SELECT COUNT(*) FROM savepoint_truncate_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestorestruncatedrowsrepro-0001-select-count-*-from-savepoint_truncate_items"},
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT id, label
						FROM savepoint_truncate_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestorestruncatedrowsrepro-0002-select-id-label-from-savepoint_truncate_items"},
				},
			},
		},
	})
}

// TestRollbackDropsSchemaCreatedInTransactionGuard keeps coverage for rolling
// back CREATE SCHEMA with the surrounding transaction.
func TestRollbackDropsSchemaCreatedInTransactionGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK drops schema created in transaction",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `CREATE SCHEMA rollback_create_schema;`,
				},
				{
					Query: `SELECT COUNT(*)
						FROM information_schema.schemata
						WHERE schema_name = 'rollback_create_schema';`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackdropsschemacreatedintransactionguard-0001-select-count-*-from-information_schema.schemata"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT COUNT(*)
						FROM information_schema.schemata
						WHERE schema_name = 'rollback_create_schema';`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackdropsschemacreatedintransactionguard-0002-select-count-*-from-information_schema.schemata"},
				},
				{
					Query: `CREATE SCHEMA rollback_create_schema;`,
				},
			},
		},
	})
}

// TestRollbackRestoresDroppedSchemaGuard keeps coverage for rolling back DROP
// SCHEMA with the surrounding transaction.
func TestRollbackRestoresDroppedSchemaGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK restores dropped schema",
			SetUpScript: []string{
				`CREATE SCHEMA rollback_drop_schema;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `DROP SCHEMA rollback_drop_schema;`,
				},
				{
					Query: `SELECT COUNT(*)
						FROM information_schema.schemata
						WHERE schema_name = 'rollback_drop_schema';`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppedschemaguard-0001-select-count-*-from-information_schema.schemata"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT COUNT(*)
						FROM information_schema.schemata
						WHERE schema_name = 'rollback_drop_schema';`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbackrestoresdroppedschemaguard-0002-select-count-*-from-information_schema.schemata"},
				},
			},
		},
	})
}

// TestRollbackToSavepointDropsSchemaCreatedAfterSavepointGuard keeps coverage
// for rolling back CREATE SCHEMA to a savepoint.
func TestRollbackToSavepointDropsSchemaCreatedAfterSavepointGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT drops schema created after savepoint",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `CREATE SCHEMA savepoint_create_schema;`,
				},
				{
					Query: `SELECT COUNT(*)
						FROM information_schema.schemata
						WHERE schema_name = 'savepoint_create_schema';`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointdropsschemacreatedaftersavepointguard-0001-select-count-*-from-information_schema.schemata"},
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT COUNT(*)
						FROM information_schema.schemata
						WHERE schema_name = 'savepoint_create_schema';`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointdropsschemacreatedaftersavepointguard-0002-select-count-*-from-information_schema.schemata"},
				},
				{
					Query: `CREATE SCHEMA savepoint_create_schema;`,
				},
			},
		},
	})
}

// TestRollbackToSavepointRestoresDroppedSchemaGuard keeps coverage for rolling
// back DROP SCHEMA to a savepoint.
func TestRollbackToSavepointRestoresDroppedSchemaGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT restores dropped schema",
			SetUpScript: []string{
				`CREATE SCHEMA savepoint_drop_schema;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT ddl_sp;`,
				},
				{
					Query: `DROP SCHEMA savepoint_drop_schema;`,
				},
				{
					Query: `SELECT COUNT(*)
						FROM information_schema.schemata
						WHERE schema_name = 'savepoint_drop_schema';`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresdroppedschemaguard-0001-select-count-*-from-information_schema.schemata"},
				},
				{
					Query: `ROLLBACK TO SAVEPOINT ddl_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT COUNT(*)
						FROM information_schema.schemata
						WHERE schema_name = 'savepoint_drop_schema';`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testrollbacktosavepointrestoresdroppedschemaguard-0002-select-count-*-from-information_schema.schemata"},
				},
			},
		},
	})
}

// TestRollbackRevertsCreateExtensionRepro guards transactional DDL for
// extensions: rolling back CREATE EXTENSION should remove both extension
// metadata and extension-provided type objects.
func TestRollbackRevertsCreateExtensionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK reverts CREATE EXTENSION",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `CREATE EXTENSION hstore WITH SCHEMA public;`,
				},
				{
					Query: `SELECT extname
						FROM pg_catalog.pg_extension
						WHERE extname = 'hstore';`,
					Expected: []sql.Row{{"hstore"}},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_extension
						WHERE extname = 'hstore';`,
					Expected: []sql.Row{{int64(0)}},
				},
				{
					Query:    `SELECT to_regtype('public.hstore') IS NULL;`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
	})
}

// TestCreateDatabaseInsideTransactionRejectedRepro reproduces a transaction
// boundary correctness bug: PostgreSQL rejects CREATE DATABASE inside an
// explicit transaction block.
func TestCreateDatabaseInsideTransactionRejectedRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE DATABASE inside transaction is rejected",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `CREATE DATABASE tx_create_database_repro;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testcreatedatabaseinsidetransactionrejectedrepro-0001-create-database-tx_create_database_repro",

						// TestDropDatabaseInsideTransactionRejectedRepro reproduces a transaction
						// boundary correctness bug: PostgreSQL rejects DROP DATABASE inside an explicit
						// transaction block.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestDropDatabaseInsideTransactionRejectedRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP DATABASE inside transaction is rejected",
			SetUpScript: []string{
				`CREATE DATABASE tx_drop_database_repro;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `DROP DATABASE tx_drop_database_repro;`, PostgresOracle: ScriptTestPostgresOracle{ID: "transaction-ddl-repro-test-testdropdatabaseinsidetransactionrejectedrepro-0001-drop-database-tx_drop_database_repro", Compare: "sqlstate"},
				},
			},
		},
	})
}
