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

package postgres18

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"
)

// TestPostgres18NotNullConstraintCatalogRepro reproduces a PostgreSQL 18
// catalog parity gap: NOT NULL constraints are stored in pg_constraint with
// contype = 'n'.
func TestPostgres18NotNullConstraintCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_constraint exposes NOT NULL constraints",
			SetUpScript: []string{
				`CREATE TABLE pg18_not_null_constraint_items (
					id INT PRIMARY KEY,
					label TEXT NOT NULL
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT contype, conkey::text, connoinherit
						FROM pg_catalog.pg_constraint
						WHERE conrelid = 'pg18_not_null_constraint_items'::regclass
							AND contype = 'n';`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/pg18-constraint-catalog-repro-test-testpostgres18notnullconstraintcatalogrepro-0001-select-contype-conkey::text-connoinherit-from"},
				},
			},
		},
	})
}

// TestPostgres18TableNotNullConstraintSyntaxRepro reproduces a PostgreSQL 18
// constraint parity gap: NOT NULL constraints can be declared as named table
// constraints over a column.
func TestPostgres18TableNotNullConstraintSyntaxRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "table constraint NOT NULL column syntax",
			SetUpScript: []string{
				`CREATE TABLE pg18_table_not_null_constraint_items (
					id INT PRIMARY KEY,
					label TEXT,
					CONSTRAINT label_required NOT NULL label
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT conname, contype, conkey::text
						FROM pg_catalog.pg_constraint
						WHERE conrelid = 'pg18_table_not_null_constraint_items'::regclass
							AND conname = 'label_required';`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/pg18-constraint-catalog-repro-test-testpostgres18tablenotnullconstraintsyntaxrepro-0001-select-conname-contype-conkey::text-from"},
				},
				{
					Query: `INSERT INTO pg18_table_not_null_constraint_items VALUES (1, NULL);`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/pg18-constraint-catalog-repro-test-testpostgres18tablenotnullconstraintsyntaxrepro-0002-insert-into-pg18_table_not_null_constraint_items-values-1", Compare: "sqlstate"},

					// TestPostgres18AlterNotNullConstraintInheritanceRepro reproduces a PostgreSQL
					// 18 constraint parity gap: named NOT NULL constraints can be marked
					// NO INHERIT and later changed with ALTER CONSTRAINT ... INHERIT.

				},
			},
		},
	})
}

func TestPostgres18AlterNotNullConstraintInheritanceRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER CONSTRAINT changes NOT NULL inheritance",
			SetUpScript: []string{
				`CREATE TABLE pg18_not_null_inherit_items (
					id INT PRIMARY KEY,
					label TEXT CONSTRAINT label_required NOT NULL NO INHERIT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT conname, connoinherit
						FROM pg_catalog.pg_constraint
						WHERE conrelid = 'pg18_not_null_inherit_items'::regclass
							AND conname = 'label_required';`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/pg18-constraint-catalog-repro-test-testpostgres18alternotnullconstraintinheritancerepro-0001-select-conname-connoinherit-from-pg_catalog.pg_constraint"},
				},
				{
					Query: `ALTER TABLE pg18_not_null_inherit_items
						ALTER CONSTRAINT label_required INHERIT;`,
				},
				{
					Query: `SELECT conname, connoinherit
						FROM pg_catalog.pg_constraint
						WHERE conrelid = 'pg18_not_null_inherit_items'::regclass
							AND conname = 'label_required';`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/pg18-constraint-catalog-repro-test-testpostgres18alternotnullconstraintinheritancerepro-0002-select-conname-connoinherit-from-pg_catalog.pg_constraint"},
				},
				{
					Query: `INSERT INTO pg18_not_null_inherit_items VALUES (1, NULL);`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/pg18-constraint-catalog-repro-test-testpostgres18alternotnullconstraintinheritancerepro-0003-insert-into-pg18_not_null_inherit_items-values-1", Compare: "sqlstate"},
				},
			},
		},
	})
}
