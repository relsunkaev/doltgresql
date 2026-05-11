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
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestDropSequenceReferencedByDefaultRequiresCascadeRepro reproduces a
// dependency correctness bug: Doltgres lets DROP SEQUENCE remove a sequence
// that is still referenced by a column default.
func TestDropSequenceReferencedByDefaultRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP SEQUENCE rejects referenced column defaults without cascade",
			SetUpScript: []string{
				`CREATE SEQUENCE default_dependency_seq;`,
				`CREATE TABLE default_dependency_items (
					id INT PRIMARY KEY DEFAULT nextval('default_dependency_seq'),
					label TEXT
				);`,
				`INSERT INTO default_dependency_items (label) VALUES ('before drop');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP SEQUENCE default_dependency_seq;`,
					ExpectedErr: `depends on it`,
				},
			},
		},
	})
}

// TestDropSequenceCascadeRemovesColumnDefaultRepro reproduces a dependency
// cleanup bug: CASCADE should remove defaults that depend on the dropped
// sequence instead of leaving stale nextval() expressions behind.
func TestDropSequenceCascadeRemovesColumnDefaultRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP SEQUENCE CASCADE removes dependent column defaults",
			SetUpScript: []string{
				`CREATE SEQUENCE cascade_default_seq;`,
				`CREATE TABLE cascade_default_items (
					id INT DEFAULT nextval('cascade_default_seq'),
					label TEXT
				);`,
				`INSERT INTO cascade_default_items (label) VALUES ('before cascade');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP SEQUENCE cascade_default_seq CASCADE;`,
				},
				{
					Query: `INSERT INTO cascade_default_items (label)
						VALUES ('after cascade');`,
				},
				{
					Query: `SELECT id, label
						FROM cascade_default_items
						ORDER BY label;`,
					Expected: []sql.Row{
						{1, "before cascade"},
						{nil, "after cascade"},
					},
				},
			},
		},
	})
}

// TestDropOwnedSequenceWithoutDefaultRepro reproduces a dependency correctness
// bug: OWNED BY makes the sequence depend on the column, but does not by itself
// make the column depend on the sequence.
func TestDropOwnedSequenceWithoutDefaultRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP SEQUENCE allows owned sequence without default dependency",
			SetUpScript: []string{
				`CREATE TABLE owned_sequence_without_default_items (
					id INT PRIMARY KEY
				);`,
				`CREATE SEQUENCE owned_sequence_without_default_seq
					OWNED BY owned_sequence_without_default_items.id;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP SEQUENCE owned_sequence_without_default_seq;`,
				},
				{
					Query: `SELECT c.relname
						FROM pg_catalog.pg_class c
						WHERE c.relkind = 'S'
							AND c.relname = 'owned_sequence_without_default_seq';`,
					Expected: []sql.Row{},
				},
				{
					Query: `SELECT id
						FROM owned_sequence_without_default_items;`,
					Expected: []sql.Row{},
				},
			},
		},
	})
}

// TestDropOwnedColumnDropsOwnedSequenceRepro reproduces a dependency cleanup
// bug: dropping a column should also drop sequences owned by that column.
func TestDropOwnedColumnDropsOwnedSequenceRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP COLUMN removes owned sequence",
			SetUpScript: []string{
				`CREATE TABLE drop_owned_column_sequence_items (
					id INT,
					label TEXT
				);`,
				`CREATE SEQUENCE drop_owned_column_sequence_seq
					OWNED BY drop_owned_column_sequence_items.id;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE drop_owned_column_sequence_items
						DROP COLUMN id;`,
				},
				{
					Query: `SELECT c.relname
						FROM pg_catalog.pg_class c
						WHERE c.relkind = 'S'
							AND c.relname = 'drop_owned_column_sequence_seq';`,
					Expected: []sql.Row{},
				},
			},
		},
	})
}

// TestDropOwnedTableDropsOwnedSequenceGuard guards that dropping a table also
// drops sequences owned by that table.
func TestDropOwnedTableDropsOwnedSequenceGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TABLE removes owned sequence",
			SetUpScript: []string{
				`CREATE TABLE drop_owned_table_sequence_items (
					id INT,
					label TEXT
				);`,
				`CREATE SEQUENCE drop_owned_table_sequence_seq
					OWNED BY drop_owned_table_sequence_items.id;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TABLE drop_owned_table_sequence_items;`,
				},
				{
					Query: `SELECT c.relname
						FROM pg_catalog.pg_class c
						WHERE c.relkind = 'S'
							AND c.relname = 'drop_owned_table_sequence_seq';`,
					Expected: []sql.Row{},
				},
			},
		},
	})
}

// TestRenameOwnedColumnUpdatesOwnedSequenceRepro reproduces a dependency
// metadata bug: renaming a column should keep sequences owned by that column
// associated with the renamed column.
func TestRenameOwnedColumnUpdatesOwnedSequenceRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RENAME COLUMN updates owned sequence dependency",
			SetUpScript: []string{
				`CREATE TABLE rename_owned_column_sequence_items (
					id INT,
					label TEXT
				);`,
				`CREATE SEQUENCE rename_owned_column_sequence_seq
					OWNED BY rename_owned_column_sequence_items.id;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE rename_owned_column_sequence_items
						RENAME COLUMN id TO renamed_id;`,
				},
				{
					Query: `SELECT pg_get_serial_sequence(
							'rename_owned_column_sequence_items',
							'renamed_id'
						);`,
					Expected: []sql.Row{{"public.rename_owned_column_sequence_seq"}},
				},
			},
		},
	})
}

// TestRenameOwnedTableUpdatesOwnedSequenceGuard guards that renaming a table
// keeps sequences owned by that table associated with the renamed table.
func TestRenameOwnedTableUpdatesOwnedSequenceGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RENAME TABLE updates owned sequence dependency",
			SetUpScript: []string{
				`CREATE TABLE rename_owned_table_sequence_old (
					id INT,
					label TEXT
				);`,
				`CREATE SEQUENCE rename_owned_table_sequence_seq
					OWNED BY rename_owned_table_sequence_old.id;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE rename_owned_table_sequence_old
						RENAME TO rename_owned_table_sequence_new;`,
				},
				{
					Query: `SELECT pg_get_serial_sequence(
							'rename_owned_table_sequence_new',
							'id'
						);`,
					Expected: []sql.Row{{"public.rename_owned_table_sequence_seq"}},
				},
			},
		},
	})
}

// TestPgGetSerialSequenceHandlesQuotedTableNamesWithDots guards that
// pg_get_serial_sequence parses double-quoted table names that contain dots
// as a single identifier, matching PostgreSQL's quoted-identifier semantics.
func TestPgGetSerialSequenceHandlesQuotedTableNamesWithDots(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_get_serial_sequence handles quoted table names containing dots",
			SetUpScript: []string{
				`CREATE TABLE "pgget.serial.table" (
					id INT PRIMARY KEY
				);`,
				`CREATE SEQUENCE pgget_serial_table_seq
					OWNED BY "pgget.serial.table".id;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT pg_get_serial_sequence('"pgget.serial.table"', 'id');`,
					Expected: []sql.Row{{"public.pgget_serial_table_seq"}},
				},
				{
					Query:    `SELECT pg_get_serial_sequence('public."pgget.serial.table"', 'id');`,
					Expected: []sql.Row{{"public.pgget_serial_table_seq"}},
				},
			},
		},
	})
}

// TestPgGetSerialSequenceQuotesSequenceNames guards that
// pg_get_serial_sequence quotes sequence names that need quoting (mixed case,
// special characters, reserved words) so the returned text round-trips
// through identifiers like nextval('...').
func TestPgGetSerialSequenceQuotesSequenceNames(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_get_serial_sequence quotes sequence names that require quoting",
			SetUpScript: []string{
				`CREATE TABLE pgget_quote_items (
					id INT PRIMARY KEY
				);`,
				`CREATE SEQUENCE "PgGetQuoteSeq"
					OWNED BY pgget_quote_items.id;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_get_serial_sequence(
							'pgget_quote_items',
							'id'
						);`,
					Expected: []sql.Row{{`public."PgGetQuoteSeq"`}},
				},
				{
					Query: `SELECT nextval(pg_get_serial_sequence(
							'pgget_quote_items',
							'id'
						));`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestAlterSequenceRenameToRepro reproduces a sequence DDL correctness bug:
// PostgreSQL supports ALTER SEQUENCE ... RENAME TO for persistent sequences.
func TestAlterSequenceRenameToRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SEQUENCE RENAME TO renames persistent sequence",
			SetUpScript: []string{
				`CREATE SEQUENCE rename_sequence_old_seq;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SEQUENCE rename_sequence_old_seq
						RENAME TO rename_sequence_new_seq;`,
				},
				{
					Query:    `SELECT nextval('rename_sequence_new_seq');`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:       `SELECT nextval('rename_sequence_old_seq');`,
					ExpectedErr: `does not exist`,
				},
			},
		},
	})
}

// TestCreateQualifiedSequenceOwnedByUnqualifiedTableRepro reproduces a
// correctness bug: an unqualified OWNED BY table should resolve through the
// search path, so public.seq can be owned by table.id when table is in public.
func TestCreateQualifiedSequenceOwnedByUnqualifiedTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE SEQUENCE resolves unqualified OWNED BY table names",
			SetUpScript: []string{
				`CREATE TABLE create_qualified_sequence_owner_items (
					id INT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE SEQUENCE public.create_qualified_sequence_owner_seq
						OWNED BY create_qualified_sequence_owner_items.id;`,
				},
				{
					Query: `SELECT pg_get_serial_sequence(
							'create_qualified_sequence_owner_items',
							'id'
						);`,
					Expected: []sql.Row{{"public.create_qualified_sequence_owner_seq"}},
				},
			},
		},
	})
}

// TestAlterQualifiedSequenceOwnedByUnqualifiedTableRepro reproduces a
// correctness bug: ALTER SEQUENCE should resolve an unqualified OWNED BY table
// before checking that the sequence and table are in the same schema.
func TestAlterQualifiedSequenceOwnedByUnqualifiedTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SEQUENCE resolves unqualified OWNED BY table names",
			SetUpScript: []string{
				`CREATE TABLE alter_qualified_sequence_owner_items (
					id INT
				);`,
				`CREATE SEQUENCE public.alter_qualified_sequence_owner_seq;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SEQUENCE public.alter_qualified_sequence_owner_seq
						OWNED BY alter_qualified_sequence_owner_items.id;`,
				},
				{
					Query: `SELECT pg_get_serial_sequence(
							'alter_qualified_sequence_owner_items',
							'id'
						);`,
					Expected: []sql.Row{{"public.alter_qualified_sequence_owner_seq"}},
				},
			},
		},
	})
}

// TestNextvalIsNotRolledBackRepro guards PostgreSQL sequence semantics:
// sequence value allocation is not rolled back with the surrounding
// transaction.
func TestNextvalIsNotRolledBackRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "nextval advances are not rolled back",
			SetUpScript: []string{
				`CREATE SEQUENCE rollback_nextval_seq;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query:    `SELECT nextval('rollback_nextval_seq');`,
					Expected: []sql.Row{{1}},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query:    `SELECT nextval('rollback_nextval_seq');`,
					Expected: []sql.Row{{2}},
				},
			},
		},
	})
}

// TestSetvalIsNotRolledBackRepro guards PostgreSQL sequence semantics:
// sequence state changes from setval are not rolled back with the surrounding
// transaction.
func TestSetvalIsNotRolledBackRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "setval changes are not rolled back",
			SetUpScript: []string{
				`CREATE SEQUENCE rollback_setval_seq;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query:    `SELECT setval('rollback_setval_seq', 50);`,
					Expected: []sql.Row{{50}},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query:    `SELECT nextval('rollback_setval_seq');`,
					Expected: []sql.Row{{51}},
				},
			},
		},
	})
}

// TestNextvalIsNotRolledBackToSavepointRepro guards PostgreSQL sequence
// semantics: sequence value allocation is not rolled back by savepoint
// rollback.
func TestNextvalIsNotRolledBackToSavepointRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "nextval advances are not rolled back to savepoint",
			SetUpScript: []string{
				`CREATE SEQUENCE savepoint_nextval_seq;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query:    `SELECT nextval('savepoint_nextval_seq');`,
					Expected: []sql.Row{{1}},
				},
				{
					Query: `SAVEPOINT sp;`,
				},
				{
					Query:    `SELECT nextval('savepoint_nextval_seq');`,
					Expected: []sql.Row{{2}},
				},
				{
					Query: `ROLLBACK TO SAVEPOINT sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query:    `SELECT nextval('savepoint_nextval_seq');`,
					Expected: []sql.Row{{3}},
				},
			},
		},
	})
}

// TestSetvalIsNotRolledBackToSavepointRepro guards PostgreSQL sequence
// semantics: sequence state changes from setval are not rolled back by
// savepoint rollback.
func TestSetvalIsNotRolledBackToSavepointRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "setval changes are not rolled back to savepoint",
			SetUpScript: []string{
				`CREATE SEQUENCE savepoint_setval_seq;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT sp;`,
				},
				{
					Query:    `SELECT setval('savepoint_setval_seq', 50);`,
					Expected: []sql.Row{{50}},
				},
				{
					Query: `ROLLBACK TO SAVEPOINT sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query:    `SELECT nextval('savepoint_setval_seq');`,
					Expected: []sql.Row{{51}},
				},
			},
		},
	})
}

// TestCurrvalReturnsSessionSequenceValueRepro reproduces a PostgreSQL
// compatibility correctness bug: currval should return the latest value
// obtained by nextval for the sequence in the current session.
func TestCurrvalReturnsSessionSequenceValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "currval returns the current session's latest nextval",
			SetUpScript: []string{
				`CREATE SEQUENCE currval_session_seq;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT nextval('currval_session_seq');`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT currval('currval_session_seq');`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestLastvalReturnsLatestSessionSequenceValueRepro reproduces a PostgreSQL
// compatibility correctness bug: lastval should return the latest value
// obtained from any sequence in the current session.
func TestLastvalReturnsLatestSessionSequenceValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "lastval returns the latest sequence value in the current session",
			SetUpScript: []string{
				`CREATE SEQUENCE lastval_first_seq;`,
				`CREATE SEQUENCE lastval_second_seq START 10;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT nextval('lastval_first_seq');`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT nextval('lastval_second_seq');`,
					Expected: []sql.Row{{10}},
				},
				{
					Query:    `SELECT lastval();`,
					Expected: []sql.Row{{10}},
				},
			},
		},
	})
}

// TestNextvalDoesNotResolveOutsideSearchPathRepro guards that an unqualified
// nextval() does not resolve or advance sequences outside the active
// search_path.
func TestNextvalDoesNotResolveOutsideSearchPathRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "nextval respects search_path for unqualified sequence names",
			SetUpScript: []string{
				`CREATE SCHEMA hidden_sequence_schema;`,
				`CREATE SCHEMA visible_sequence_schema;`,
				`CREATE SEQUENCE hidden_sequence_schema.search_path_hidden_seq START 50;`,
				`SET search_path TO visible_sequence_schema, public;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `SELECT nextval('search_path_hidden_seq');`,
					ExpectedErr: `does not exist`,
				},
				{
					Query:    `SELECT nextval('hidden_sequence_schema.search_path_hidden_seq');`,
					Expected: []sql.Row{{50}},
				},
			},
		},
	})
}

// TestNextvalHandlesQuotedSequenceNamesWithDotsRepro reproduces a sequence
// name parsing bug: dots inside quoted identifiers are part of the identifier,
// not schema separators.
func TestNextvalHandlesQuotedSequenceNamesWithDotsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "nextval handles quoted sequence names containing dots",
			SetUpScript: []string{
				`CREATE SEQUENCE "quoted.sequence.name";`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT nextval('"quoted.sequence.name"');`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT nextval('public."quoted.sequence.name"');`,
					Expected: []sql.Row{{2}},
				},
			},
		},
	})
}

// TestSetvalHandlesQuotedSequenceNamesWithDotsRepro reproduces a sequence state
// correctness bug: dots inside quoted identifiers are part of the identifier,
// not schema separators, and setval() must mutate that named sequence.
func TestSetvalHandlesQuotedSequenceNamesWithDotsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "setval handles quoted sequence names containing dots",
			SetUpScript: []string{
				`CREATE SEQUENCE "setval.quoted.sequence";`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT setval('"setval.quoted.sequence"', 42);`,
					Expected: []sql.Row{{42}},
				},
				{
					Query:    `SELECT setval('public."setval.quoted.sequence"', 50);`,
					Expected: []sql.Row{{50}},
				},
			},
		},
	})
}

// TestNextvalUsesSecondSearchPathSchemaRepro reproduces a sequence namespace
// correctness bug: unqualified sequence names should resolve through each
// search_path schema in order, not only the current schema.
func TestNextvalUsesSecondSearchPathSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "nextval searches later search_path schemas",
			SetUpScript: []string{
				`CREATE SCHEMA first_sequence_path_schema;`,
				`CREATE SCHEMA second_sequence_path_schema;`,
				`CREATE SEQUENCE second_sequence_path_schema.search_path_later_seq START 70;`,
				`SET search_path TO first_sequence_path_schema, second_sequence_path_schema, public;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT nextval('search_path_later_seq');`,
					Expected: []sql.Row{{70}},
				},
				{
					Query:    `SELECT nextval('second_sequence_path_schema.search_path_later_seq');`,
					Expected: []sql.Row{{71}},
				},
			},
		},
	})
}

// TestSetvalUsesSecondSearchPathSchemaRepro reproduces a sequence state
// correctness bug: unqualified sequence names in setval() should resolve
// through each search_path schema in order before mutating sequence state.
func TestSetvalUsesSecondSearchPathSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "setval searches later search_path schemas",
			SetUpScript: []string{
				`CREATE SCHEMA first_setval_path_schema;`,
				`CREATE SCHEMA second_setval_path_schema;`,
				`CREATE SEQUENCE second_setval_path_schema.search_path_setval_seq START 70;`,
				`SET search_path TO first_setval_path_schema, second_setval_path_schema, public;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT setval('search_path_setval_seq', 90);`,
					Expected: []sql.Row{{90}},
				},
				{
					Query:    `SELECT nextval('second_setval_path_schema.search_path_setval_seq');`,
					Expected: []sql.Row{{91}},
				},
			},
		},
	})
}

// TestNextvalIsVisibleAcrossTransactionsRepro guards PostgreSQL sequence
// semantics: sequence value allocation is immediately visible across sessions,
// even before the allocating transaction commits.
func TestNextvalIsVisibleAcrossTransactionsRepro(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	_, err = defaultConn.Exec(ctx, `CREATE SEQUENCE concurrent_nextval_seq;`)
	require.NoError(t, err)

	dial := func(t *testing.T) *pgx.Conn {
		t.Helper()
		conn, err := pgx.Connect(ctx, fmt.Sprintf(
			"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable",
			port,
		))
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = conn.Close(context.Background())
		})
		return conn
	}

	first := dial(t)
	second := dial(t)

	tx, err := first.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = tx.Rollback(context.Background())
	})

	var firstValue int64
	require.NoError(t, tx.QueryRow(ctx, `SELECT nextval('concurrent_nextval_seq');`).Scan(&firstValue))
	require.Equal(t, int64(1), firstValue)

	var secondValue int64
	require.NoError(t, second.QueryRow(ctx, `SELECT nextval('concurrent_nextval_seq');`).Scan(&secondValue))
	require.Equal(t, int64(2), secondValue)
}

// TestSetvalIsVisibleAcrossTransactionsRepro guards PostgreSQL sequence
// semantics: sequence reseeding is immediately visible across sessions, even
// before the reseeding transaction commits.
func TestSetvalIsVisibleAcrossTransactionsRepro(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	_, err = defaultConn.Exec(ctx, `CREATE SEQUENCE concurrent_setval_seq;`)
	require.NoError(t, err)

	dial := func(t *testing.T) *pgx.Conn {
		t.Helper()
		conn, err := pgx.Connect(ctx, fmt.Sprintf(
			"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable",
			port,
		))
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = conn.Close(context.Background())
		})
		return conn
	}

	first := dial(t)
	second := dial(t)

	tx, err := first.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = tx.Rollback(context.Background())
	})

	var setValue int64
	require.NoError(t, tx.QueryRow(ctx, `SELECT setval('concurrent_setval_seq', 50);`).Scan(&setValue))
	require.Equal(t, int64(50), setValue)

	var nextValue int64
	require.NoError(t, second.QueryRow(ctx, `SELECT nextval('concurrent_setval_seq');`).Scan(&nextValue))
	require.Equal(t, int64(51), nextValue)
}

// TestDefaultNextvalIsVisibleAcrossTransactionsRepro reproduces a data
// consistency bug: default nextval allocation is isolated per transaction,
// allowing concurrent inserts to persist duplicate generated ids.
func TestDefaultNextvalIsVisibleAcrossTransactionsRepro(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	for _, stmt := range []string{
		`CREATE SEQUENCE concurrent_default_seq;`,
		`CREATE TABLE concurrent_default_items (
			id BIGINT DEFAULT nextval('concurrent_default_seq'),
			label TEXT
		);`,
	} {
		_, err = defaultConn.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	dial := func(t *testing.T) *pgx.Conn {
		t.Helper()
		conn, err := pgx.Connect(ctx, fmt.Sprintf(
			"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable",
			port,
		))
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = conn.Close(context.Background())
		})
		return conn
	}

	first := dial(t)
	second := dial(t)

	firstTx, err := first.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = firstTx.Rollback(context.Background())
	})
	secondTx, err := second.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = secondTx.Rollback(context.Background())
	})

	var firstID int64
	require.NoError(t, firstTx.QueryRow(ctx,
		`INSERT INTO concurrent_default_items (label) VALUES ('first') RETURNING id;`,
	).Scan(&firstID))
	require.Equal(t, int64(1), firstID)

	var secondID int64
	require.NoError(t, secondTx.QueryRow(ctx,
		`INSERT INTO concurrent_default_items (label) VALUES ('second') RETURNING id;`,
	).Scan(&secondID))

	require.NoError(t, firstTx.Commit(ctx))
	require.NoError(t, secondTx.Commit(ctx))

	rows, err := defaultConn.Query(ctx, `SELECT id FROM concurrent_default_items ORDER BY label;`)
	require.NoError(t, err)
	actual, _, err := ReadRows(rows, true)
	require.NoError(t, err)
	require.Equal(t, []sql.Row{{int64(1)}, {int64(2)}}, actual)
}

// TestAddColumnNextvalDefaultBackfillsEachExistingRowRepro guards that
// PostgreSQL evaluates volatile defaults such as nextval() for each existing
// row when ADD COLUMN rewrites a non-empty table.
func TestAddColumnNextvalDefaultBackfillsEachExistingRowRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ADD COLUMN nextval default backfills each existing row",
			SetUpScript: []string{
				`CREATE SEQUENCE add_column_nextval_seq;`,
				`CREATE TABLE add_column_nextval_items (
					id BIGINT PRIMARY KEY
				);`,
				`INSERT INTO add_column_nextval_items VALUES (10), (20), (30);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE add_column_nextval_items
						ADD COLUMN generated_id BIGINT DEFAULT nextval('add_column_nextval_seq');`,
				},
				{
					Query: `SELECT id, generated_id
						FROM add_column_nextval_items
						ORDER BY id;`,
					Expected: []sql.Row{
						{int64(10), int64(1)},
						{int64(20), int64(2)},
						{int64(30), int64(3)},
					},
				},
				{
					Query:    `SELECT nextval('add_column_nextval_seq');`,
					Expected: []sql.Row{{int64(4)}},
				},
			},
		},
	})
}

// TestUpdateSetNextvalDefaultEvaluatesPerRowGuard guards that UPDATE SET
// DEFAULT evaluates volatile defaults such as nextval() once per updated row.
func TestUpdateSetNextvalDefaultEvaluatesPerRowGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE SET DEFAULT evaluates nextval per row",
			SetUpScript: []string{
				`CREATE SEQUENCE update_default_nextval_seq;`,
				`CREATE TABLE update_default_nextval_items (
					id BIGINT PRIMARY KEY,
					generated_id BIGINT DEFAULT nextval('update_default_nextval_seq')
				);`,
				`INSERT INTO update_default_nextval_items (id, generated_id) VALUES
					(1, 100),
					(2, 200),
					(3, 300);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE update_default_nextval_items
						SET generated_id = DEFAULT;`,
				},
				{
					Query: `SELECT id, generated_id
						FROM update_default_nextval_items
						ORDER BY id;`,
					Expected: []sql.Row{
						{int64(1), int64(1)},
						{int64(2), int64(2)},
						{int64(3), int64(3)},
					},
				},
				{
					Query:    `SELECT nextval('update_default_nextval_seq');`,
					Expected: []sql.Row{{int64(4)}},
				},
			},
		},
	})
}

// TestAddIdentityColumnBackfillsEachExistingRowRepro reproduces an identity
// integrity bug: adding an identity column to a populated table assigns
// distinct generated values to existing rows and continues the sequence for
// later rows.
func TestAddIdentityColumnBackfillsEachExistingRowRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ADD COLUMN identity backfills each existing row",
			SetUpScript: []string{
				`CREATE TABLE add_identity_column_items (
					id INT PRIMARY KEY
				);`,
				`INSERT INTO add_identity_column_items VALUES (10), (20), (30);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE add_identity_column_items
						ADD COLUMN generated_id INT GENERATED BY DEFAULT AS IDENTITY;`,
				},
				{
					Query: `INSERT INTO add_identity_column_items (id) VALUES (40);`,
				},
				{
					Query: `SELECT id, generated_id
						FROM add_identity_column_items
						ORDER BY id;`,
					Expected: []sql.Row{
						{10, 1},
						{20, 2},
						{30, 3},
						{40, 4},
					},
				},
			},
		},
	})
}

// TestIdentityValuesAreVisibleAcrossTransactionsRepro reproduces a data
// consistency bug: identity value allocation is isolated per transaction,
// allowing concurrent inserts to persist duplicate generated identity values.
func TestIdentityValuesAreVisibleAcrossTransactionsRepro(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	_, err = defaultConn.Exec(ctx, `CREATE TABLE concurrent_identity_items (
		id BIGINT GENERATED BY DEFAULT AS IDENTITY,
		label TEXT
	);`)
	require.NoError(t, err)

	dial := func(t *testing.T) *pgx.Conn {
		t.Helper()
		conn, err := pgx.Connect(ctx, fmt.Sprintf(
			"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable",
			port,
		))
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = conn.Close(context.Background())
		})
		return conn
	}

	first := dial(t)
	second := dial(t)

	firstTx, err := first.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = firstTx.Rollback(context.Background())
	})
	secondTx, err := second.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = secondTx.Rollback(context.Background())
	})

	var firstID int64
	require.NoError(t, firstTx.QueryRow(ctx,
		`INSERT INTO concurrent_identity_items (label) VALUES ('first') RETURNING id;`,
	).Scan(&firstID))
	require.Equal(t, int64(1), firstID)

	var secondID int64
	require.NoError(t, secondTx.QueryRow(ctx,
		`INSERT INTO concurrent_identity_items (label) VALUES ('second') RETURNING id;`,
	).Scan(&secondID))

	require.NoError(t, firstTx.Commit(ctx))
	require.NoError(t, secondTx.Commit(ctx))

	rows, err := defaultConn.Query(ctx, `SELECT id FROM concurrent_identity_items ORDER BY label;`)
	require.NoError(t, err)
	actual, _, err := ReadRows(rows, true)
	require.NoError(t, err)
	require.Equal(t, []sql.Row{{int64(1)}, {int64(2)}}, actual)
}

// TestIdentitySequenceOptionsAffectGeneratedValuesRepro reproduces an identity
// correctness bug: PostgreSQL applies sequence options such as START WITH and
// INCREMENT BY to generated identity values.
func TestIdentitySequenceOptionsAffectGeneratedValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "identity sequence options affect generated values",
			SetUpScript: []string{
				`CREATE TABLE identity_sequence_option_items (
					id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 100 INCREMENT BY 2) PRIMARY KEY,
					label TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `INSERT INTO identity_sequence_option_items (label) VALUES ('first') RETURNING id;`,
					Expected: []sql.Row{{100}},
				},
				{
					Query:    `INSERT INTO identity_sequence_option_items (label) VALUES ('second') RETURNING id;`,
					Expected: []sql.Row{{102}},
				},
			},
		},
	})
}

// TestIdentityColumnRejectsExplicitNullabilityRepro reproduces an identity DDL
// correctness bug: PostgreSQL rejects explicit NULL declarations on identity
// columns because identity columns are always NOT NULL.
func TestIdentityColumnRejectsExplicitNullabilityRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "identity columns reject explicit NULL",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE identity_explicit_null_items (
						id INT GENERATED ALWAYS AS IDENTITY NULL
					);`,
					ExpectedErr: `conflicting NULL/NOT NULL declarations`,
				},
			},
		},
	})
}

// TestIdentityColumnIsImplicitlyNotNullRepro reproduces an identity integrity
// bug: identity columns implicitly reject NULL values even when the table
// definition does not spell out NOT NULL.
func TestIdentityColumnIsImplicitlyNotNullRepro(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	_, err := conn.Current.Exec(ctx, `CREATE TABLE identity_implicit_not_null_items (
		id INT GENERATED BY DEFAULT AS IDENTITY,
		label TEXT
	);`)
	require.NoError(t, err)

	_, err = conn.Current.Exec(ctx, `INSERT INTO identity_implicit_not_null_items (id, label) VALUES (NULL, 'bad');`)
	if err == nil {
		rows, queryErr := conn.Current.Query(ctx, `SELECT id, label FROM identity_implicit_not_null_items;`)
		require.NoError(t, queryErr)
		actual, _, readErr := ReadRows(rows, true)
		require.NoError(t, readErr)
		require.Failf(t, `expected identity column to reject NULL`, `stored rows: %v`, actual)
	}
	require.ErrorContains(t, err, `null value in column "id"`)

	_, err = conn.Current.Exec(ctx, `INSERT INTO identity_implicit_not_null_items (label) VALUES ('generated');`)
	require.NoError(t, err)
	rows, err := conn.Current.Query(ctx, `SELECT id, label FROM identity_implicit_not_null_items;`)
	require.NoError(t, err)
	actual, _, err := ReadRows(rows, true)
	require.NoError(t, err)
	require.Equal(t, []sql.Row{{int64(1), "generated"}}, actual)
}

// TestGeneratedAlwaysIdentityOverridingSystemValueRepro reproduces an identity
// DML correctness bug: PostgreSQL allows explicit values for GENERATED ALWAYS
// identity columns when INSERT uses OVERRIDING SYSTEM VALUE.
func TestGeneratedAlwaysIdentityOverridingSystemValueRepro(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	_, err := conn.Current.Exec(ctx, `CREATE TABLE identity_override_system_items (
		id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		label TEXT
	);`)
	require.NoError(t, err)

	var id int32
	require.NoError(t, conn.Current.QueryRow(ctx, `INSERT INTO identity_override_system_items (id, label)
		OVERRIDING SYSTEM VALUE
		VALUES (100, 'explicit')
		RETURNING id;`).Scan(&id))
	require.Equal(t, int32(100), id)

	var label string
	require.NoError(t, conn.Current.QueryRow(ctx,
		`SELECT label FROM identity_override_system_items WHERE id = 100;`,
	).Scan(&label))
	require.Equal(t, "explicit", label)
}

// TestGeneratedByDefaultIdentityOverridingUserValueRepro reproduces an identity
// DML correctness bug: PostgreSQL ignores caller-supplied identity values when
// INSERT uses OVERRIDING USER VALUE.
func TestGeneratedByDefaultIdentityOverridingUserValueRepro(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	_, err := conn.Current.Exec(ctx, `CREATE TABLE identity_override_user_items (
		id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
		label TEXT
	);`)
	require.NoError(t, err)

	var id int32
	require.NoError(t, conn.Current.QueryRow(ctx, `INSERT INTO identity_override_user_items (id, label)
		OVERRIDING USER VALUE
		VALUES (100, 'ignored')
		RETURNING id;`).Scan(&id))
	require.Equal(t, int32(1), id)

	var label string
	require.NoError(t, conn.Current.QueryRow(ctx,
		`SELECT label FROM identity_override_user_items WHERE id = 1;`,
	).Scan(&label))
	require.Equal(t, "ignored", label)
}

// TestGeneratedAlwaysIdentityOverridingUserValueRepro reproduces an identity
// DML correctness bug: PostgreSQL ignores caller-supplied values for identity
// columns when INSERT uses OVERRIDING USER VALUE, including GENERATED ALWAYS
// identity columns.
func TestGeneratedAlwaysIdentityOverridingUserValueRepro(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	_, err := conn.Current.Exec(ctx, `CREATE TABLE identity_always_override_user_items (
		id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		label TEXT
	);`)
	require.NoError(t, err)

	var id int32
	require.NoError(t, conn.Current.QueryRow(ctx, `INSERT INTO identity_always_override_user_items (id, label)
		OVERRIDING USER VALUE
		VALUES (100, 'ignored')
		RETURNING id;`).Scan(&id))
	require.Equal(t, int32(1), id)

	var label string
	require.NoError(t, conn.Current.QueryRow(ctx,
		`SELECT label FROM identity_always_override_user_items WHERE id = 1;`,
	).Scan(&label))
	require.Equal(t, "ignored", label)
}

// TestGeneratedByDefaultIdentityOverridingSystemValueRepro reproduces an
// identity DML correctness bug: PostgreSQL accepts OVERRIDING SYSTEM VALUE for
// GENERATED BY DEFAULT identity columns and stores the explicit value.
func TestGeneratedByDefaultIdentityOverridingSystemValueRepro(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	_, err := conn.Current.Exec(ctx, `CREATE TABLE identity_default_override_system_items (
		id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
		label TEXT
	);`)
	require.NoError(t, err)

	var id int32
	require.NoError(t, conn.Current.QueryRow(ctx, `INSERT INTO identity_default_override_system_items (id, label)
		OVERRIDING SYSTEM VALUE
		VALUES (100, 'explicit')
		RETURNING id;`).Scan(&id))
	require.Equal(t, int32(100), id)

	var label string
	require.NoError(t, conn.Current.QueryRow(ctx,
		`SELECT label FROM identity_default_override_system_items WHERE id = 100;`,
	).Scan(&label))
	require.Equal(t, "explicit", label)
}

// TestAlterColumnDropIdentityRepro reproduces an identity DDL correctness bug:
// PostgreSQL supports removing the identity property while preserving the column
// and its NOT NULL constraint.
func TestAlterColumnDropIdentityRepro(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	_, err := conn.Current.Exec(ctx, `CREATE TABLE alter_drop_identity_items (
		id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
		label TEXT
	);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `INSERT INTO alter_drop_identity_items (label) VALUES ('before drop');`)
	require.NoError(t, err)

	_, err = conn.Current.Exec(ctx, `ALTER TABLE alter_drop_identity_items ALTER COLUMN id DROP IDENTITY;`)
	require.NoError(t, err)

	_, err = conn.Current.Exec(ctx, `INSERT INTO alter_drop_identity_items (label) VALUES ('missing id');`)
	require.ErrorContains(t, err, `null value in column "id"`)
	_, err = conn.Current.Exec(ctx, `INSERT INTO alter_drop_identity_items (id, label) VALUES (10, 'explicit');`)
	require.NoError(t, err)

	rows, err := conn.Current.Query(ctx, `SELECT id, label
		FROM alter_drop_identity_items
		ORDER BY id;`)
	require.NoError(t, err)
	actual, _, err := ReadRows(rows, true)
	require.NoError(t, err)
	require.Equal(t, []sql.Row{{int64(1), "before drop"}, {int64(10), "explicit"}}, actual)
}

// TestAlterColumnSetGeneratedIdentityModeRepro reproduces an identity DDL
// correctness bug: PostgreSQL can switch an identity column between ALWAYS and
// BY DEFAULT generation modes.
func TestAlterColumnSetGeneratedIdentityModeRepro(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	_, err := conn.Current.Exec(ctx, `CREATE TABLE alter_identity_mode_items (
		id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		label TEXT
	);`)
	require.NoError(t, err)

	_, err = conn.Current.Exec(ctx, `ALTER TABLE alter_identity_mode_items ALTER COLUMN id SET GENERATED BY DEFAULT;`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `INSERT INTO alter_identity_mode_items (id, label) VALUES (100, 'explicit');`)
	require.NoError(t, err)

	rows, err := conn.Current.Query(ctx, `SELECT id, label FROM alter_identity_mode_items;`)
	require.NoError(t, err)
	actual, _, err := ReadRows(rows, true)
	require.NoError(t, err)
	require.Equal(t, []sql.Row{{int64(100), "explicit"}}, actual)
}

// TestAlterColumnIdentitySequenceOptionsRepro reproduces an identity DDL
// correctness bug: PostgreSQL can change an existing identity sequence's
// options and restart point through ALTER TABLE ... ALTER COLUMN.
func TestAlterColumnIdentitySequenceOptionsRepro(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	_, err := conn.Current.Exec(ctx, `CREATE TABLE alter_identity_sequence_option_items (
		id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
		label TEXT
	);`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `INSERT INTO alter_identity_sequence_option_items (label) VALUES ('before alter');`)
	require.NoError(t, err)

	_, err = conn.Current.Exec(ctx, `ALTER TABLE alter_identity_sequence_option_items
		ALTER COLUMN id SET INCREMENT BY 2 SET START WITH 100 RESTART;`)
	require.NoError(t, err)
	_, err = conn.Current.Exec(ctx, `INSERT INTO alter_identity_sequence_option_items (label)
		VALUES ('after alter'), ('after alter again');`)
	require.NoError(t, err)

	rows, err := conn.Current.Query(ctx, `SELECT id, label
		FROM alter_identity_sequence_option_items
		ORDER BY id;`)
	require.NoError(t, err)
	actual, _, err := ReadRows(rows, true)
	require.NoError(t, err)
	require.Equal(t, []sql.Row{
		{int64(1), "before alter"},
		{int64(100), "after alter"},
		{int64(102), "after alter again"},
	}, actual)
}

// TestNextvalPersistsAcrossRestartRepro guards durable sequence semantics:
// committed sequence advances must survive a clean server restart.
func TestNextvalPersistsAcrossRestartRepro(t *testing.T) {
	dbDir, err := os.MkdirTemp(os.TempDir(), t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dbDir)

	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, conn, controller := CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)

	_, err = conn.Exec(ctx, `CREATE SEQUENCE restart_nextval_seq;`)
	require.NoError(t, err)
	var beforeRestart int64
	require.NoError(t, conn.Current.QueryRow(ctx, `SELECT nextval('restart_nextval_seq');`).Scan(&beforeRestart))
	require.Equal(t, int64(1), beforeRestart)
	_, err = conn.Exec(ctx, `SELECT DOLT_COMMIT('-Am', 'sequence state before restart');`)
	require.NoError(t, err)

	conn.Close(ctx)
	controller.Stop()
	require.NoError(t, controller.WaitForStop())

	port, err = sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, conn, controller = CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	defer func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	var afterRestart int64
	require.NoError(t, conn.Current.QueryRow(ctx, `SELECT nextval('restart_nextval_seq');`).Scan(&afterRestart))
	require.Equal(t, int64(2), afterRestart)
}

// TestNextvalDefaultIsNotRolledBackAfterFailedInsertRepro guards PostgreSQL
// sequence semantics: sequence values consumed by a failed INSERT are not
// reused by later statements.
func TestNextvalDefaultIsNotRolledBackAfterFailedInsertRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "default nextval advances are not rolled back after failed insert",
			SetUpScript: []string{
				`CREATE SEQUENCE failed_insert_default_seq;`,
				`CREATE TABLE failed_insert_default_items (
					id INT PRIMARY KEY DEFAULT nextval('failed_insert_default_seq'),
					label TEXT NOT NULL CHECK (label <> 'bad')
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `INSERT INTO failed_insert_default_items (label) VALUES ('bad');`,
					ExpectedErr: `Check constraint`,
				},
				{
					Query:    `SELECT nextval('failed_insert_default_seq');`,
					Expected: []sql.Row{{2}},
				},
			},
		},
	})
}

// TestTruncateRestartIdentityResetsOwnedSequenceRepro reproduces a sequence
// correctness bug: TRUNCATE ... RESTART IDENTITY should reset sequences owned
// by the truncated table.
func TestTruncateRestartIdentityResetsOwnedSequenceRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "TRUNCATE RESTART IDENTITY resets owned sequence",
			SetUpScript: []string{
				`CREATE TABLE truncate_restart_identity_items (
					id SERIAL PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO truncate_restart_identity_items (label) VALUES ('first'), ('second');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `TRUNCATE truncate_restart_identity_items RESTART IDENTITY;`,
				},
				{
					Query: `INSERT INTO truncate_restart_identity_items (label) VALUES ('after truncate');`,
				},
				{
					Query: `SELECT id, label
						FROM truncate_restart_identity_items;`,
					Expected: []sql.Row{{1, "after truncate"}},
				},
			},
		},
	})
}

// TestAlterSequenceOwnedByRejectsTriggerNameGuard guards that OWNED BY must
// reference an existing table column, not a trigger name.
func TestAlterSequenceOwnedByRejectsTriggerNameGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SEQUENCE OWNED BY rejects trigger names",
			SetUpScript: []string{
				`CREATE TABLE sequence_owned_trigger_items (id INT PRIMARY KEY);`,
				`CREATE SEQUENCE sequence_owned_trigger_seq;`,
				`CREATE FUNCTION sequence_owned_trigger_func()
				RETURNS TRIGGER AS $$
				BEGIN
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER sequence_owned_trigger
					BEFORE INSERT ON sequence_owned_trigger_items
					FOR EACH ROW EXECUTE FUNCTION sequence_owned_trigger_func();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SEQUENCE sequence_owned_trigger_seq
						OWNED BY sequence_owned_trigger.trig;`,
					ExpectedErr: `does not exist`,
				},
			},
		},
	})
}

// TestNextvalInWherePredicateEvaluatesPerRowRepro reproduces a sequence
// correctness bug: volatile nextval() calls in a WHERE predicate are evaluated
// once and reused instead of being evaluated for each candidate row.
func TestNextvalInWherePredicateEvaluatesPerRowRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "nextval in WHERE predicate advances once per row",
			SetUpScript: []string{
				`CREATE SEQUENCE volatile_filter_seq;`,
				`CREATE TABLE volatile_filter_items (id INT PRIMARY KEY, marker INT NOT NULL);`,
				`INSERT INTO volatile_filter_items VALUES (1, 1), (2, 2), (3, 3);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id
						FROM volatile_filter_items
						WHERE nextval('volatile_filter_seq') = marker
						ORDER BY id;`,
					Expected: []sql.Row{{1}, {2}, {3}},
				},
				{
					Query:    `SELECT nextval('volatile_filter_seq');`,
					Expected: []sql.Row{{4}},
				},
			},
		},
	})
}
