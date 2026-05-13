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
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

// TestCreateTableAsExecutePreparedStatementRepro reproduces a prepared
// statement persistence bug: CREATE TABLE AS EXECUTE should materialize the
// prepared statement result into a new table.
func TestCreateTableAsExecutePreparedStatementRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS EXECUTE materializes prepared result",
			Assertions: []ScriptTestAssertion{
				{
					Query: `PREPARE prepared_ctas_plan(INT, TEXT) AS
						SELECT $1::INT AS id, $2::TEXT AS label;`,
				},
				{
					Query: `CREATE TABLE prepared_ctas_items AS
						EXECUTE prepared_ctas_plan(7, 'seven');`,
				},
				{
					Query: `SELECT id::TEXT, label
						FROM prepared_ctas_items;`,
					Expected: []sql.Row{{"7", "seven"}},
				},
			},
		},
	})
}

// TestPreparedSelectStarRejectsChangedResultShapeRepro reproduces a prepared
// statement result-shape correctness bug: PostgreSQL rejects executing a
// prepared SELECT * plan after DDL changes its result row type.
func TestPreparedSelectStarRejectsChangedResultShapeRepro(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	for _, stmt := range []string{
		`CREATE TABLE prepared_shape_items (id INT PRIMARY KEY);`,
		`INSERT INTO prepared_shape_items VALUES (1);`,
		`PREPARE prepared_shape_plan AS
			SELECT * FROM prepared_shape_items;`,
	} {
		_, err := conn.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	var id int
	require.NoError(t, conn.Current.QueryRow(ctx, `EXECUTE prepared_shape_plan;`).Scan(&id))
	require.Equal(t, 1, id)

	_, err := conn.Exec(ctx, `ALTER TABLE prepared_shape_items
		ADD COLUMN label TEXT DEFAULT 'new shape';`)
	require.NoError(t, err)

	rows, err := conn.Current.Query(ctx, `EXECUTE prepared_shape_plan;`)
	if err == nil {
		fieldDescriptions := rows.FieldDescriptions()
		fieldNames := make([]string, 0, len(fieldDescriptions))
		for _, field := range fieldDescriptions {
			fieldNames = append(fieldNames, string(field.Name))
		}
		actualRows, _, readErr := ReadRows(rows, true)
		rows.Close()
		if readErr != nil {
			err = readErr
		}
		require.Error(t, err, "prepared SELECT * should reject changed result shape; fields=%v rows=%v", fieldNames, actualRows)
	}
	require.Contains(t, err.Error(), "cached plan must not change result type")
}

// TestPreparedStatementAcceptsUserDefinedParameterTypeRepro reproduces a
// prepared-statement compatibility gap: PostgreSQL accepts user-defined types
// such as enum types in the PREPARE parameter type list.
func TestPreparedStatementAcceptsUserDefinedParameterTypeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PREPARE accepts user-defined enum parameter type",
			SetUpScript: []string{
				`CREATE TYPE prepared_enum_mood AS ENUM ('ok', 'sad');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `PREPARE prepared_enum_plan(prepared_enum_mood) AS
						SELECT $1::TEXT;`,
				},
				{
					Query:    `EXECUTE prepared_enum_plan('ok');`,
					Expected: []sql.Row{{"ok"}},
				},
				{
					Query: `SELECT parameter_types::TEXT
						FROM pg_catalog.pg_prepared_statements
						WHERE name = 'prepared_enum_plan';`,
					Expected: []sql.Row{{"{prepared_enum_mood}"}},
				},
			},
		},
	})
}

// TestPreparedStatementsResultTypesColumnRepro reproduces a PostgreSQL 16
// catalog-shape gap: pg_prepared_statements should expose result_types.
func TestPreparedStatementsResultTypesColumnRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_prepared_statements exposes result_types",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT atttypid::regtype::text
						FROM pg_catalog.pg_attribute
						WHERE attrelid = 'pg_catalog.pg_prepared_statements'::regclass
							AND attname = 'result_types'
							AND NOT attisdropped;`,
					Expected: []sql.Row{{"regtype[]"}},
				},
			},
		},
	})
}
