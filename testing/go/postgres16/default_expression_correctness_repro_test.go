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
)

// TestCreateTableDefaultRejectsColumnReferencesRepro reproduces a correctness
// bug: PostgreSQL rejects column references inside column default expressions.
func TestCreateTableDefaultRejectsColumnReferencesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE default rejects column references",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE default_column_reference_items (
						id INT PRIMARY KEY,
						source_value TEXT,
						copied_value TEXT DEFAULT (source_value)
					);`, PostgresOracle: ScriptTestPostgresOracle{ID: "default-expression-correctness-repro-test-testcreatetabledefaultrejectscolumnreferencesrepro-0001-create-table-default_column_reference_items-id-int", Compare: "sqlstate"},
				},
			},
		},
	})
}

// TestAlterColumnDefaultRejectsColumnReferencesRepro reproduces a correctness
// bug: ALTER COLUMN SET DEFAULT must reject expressions that reference another
// column from the same row.
func TestAlterColumnDefaultRejectsColumnReferencesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER COLUMN SET DEFAULT rejects column references",
			SetUpScript: []string{
				`CREATE TABLE alter_default_column_reference_items (
					id INT PRIMARY KEY,
					source_value TEXT,
					copied_value TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_default_column_reference_items
						ALTER COLUMN copied_value SET DEFAULT (source_value);`, PostgresOracle: ScriptTestPostgresOracle{ID: "default-expression-correctness-repro-test-testaltercolumndefaultrejectscolumnreferencesrepro-0001-alter-table-alter_default_column_reference_items-alter-column", Compare: "sqlstate"},
				},
			},
		},
	})
}

// TestDefaultExpressionsRejectNonScalarExpressionsRepro reproduces default
// expression correctness bugs: PostgreSQL rejects aggregates, window
// functions, and set-returning functions in column defaults.
func TestDefaultExpressionsRejectNonScalarExpressionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DEFAULT rejects aggregate expressions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE default_aggregate_items (
						id INT DEFAULT (avg(1))
					);`, PostgresOracle: ScriptTestPostgresOracle{ID: "default-expression-correctness-repro-test-testdefaultexpressionsrejectnonscalarexpressionsrepro-0001-create-table-default_aggregate_items-id-int", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "DEFAULT rejects window expressions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE default_window_items (
						id INT DEFAULT (row_number() OVER ())
					);`, PostgresOracle: ScriptTestPostgresOracle{ID: "default-expression-correctness-repro-test-testdefaultexpressionsrejectnonscalarexpressionsrepro-0002-create-table-default_window_items-id-int", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "DEFAULT rejects set-returning expressions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE default_srf_items (
						id INT DEFAULT (generate_series(1, 2))
					);`, PostgresOracle: ScriptTestPostgresOracle{ID: "default-expression-correctness-repro-test-testdefaultexpressionsrejectnonscalarexpressionsrepro-0003-create-table-default_srf_items-id-int", Compare: "sqlstate"},
				},
			},
		},
	})
}

// TestAddColumnVolatileDefaultBackfillsEachExistingRowGuard guards that
// volatile defaults added to existing tables are evaluated per row, not reused
// as one persisted value for every existing row.
func TestAddColumnVolatileDefaultBackfillsEachExistingRowGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ADD COLUMN volatile default backfills each row",
			SetUpScript: []string{
				`CREATE TABLE add_volatile_default_items (
					id INT PRIMARY KEY
				);`,
				`INSERT INTO add_volatile_default_items VALUES (1), (2);`,
				`ALTER TABLE add_volatile_default_items
					ADD COLUMN uid UUID DEFAULT gen_random_uuid() NOT NULL;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(DISTINCT uid::text)
						FROM add_volatile_default_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "default-expression-correctness-repro-test-testaddcolumnvolatiledefaultbackfillseachexistingrowguard-0001-select-count-distinct-uid::text-from"},
				},
			},
		},
	})
}
