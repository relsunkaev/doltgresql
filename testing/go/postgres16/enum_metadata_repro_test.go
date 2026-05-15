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

// TestCreateEnumPopulatesPgEnumRepro reproduces a catalog persistence bug:
// Doltgres accepts CREATE TYPE ... AS ENUM but pg_enum does not expose the
// enum labels.
func TestCreateEnumPopulatesPgEnumRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TYPE AS ENUM populates pg_enum labels",
			SetUpScript: []string{
				`CREATE TYPE enum_catalog_target AS ENUM ('sad', 'ok', 'happy');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT enumlabel
						FROM pg_catalog.pg_enum
						WHERE enumtypid = (SELECT oid FROM pg_type WHERE typname = 'enum_catalog_target')
						ORDER BY enumsortorder;`, PostgresOracle: ScriptTestPostgresOracle{ID: "enum-metadata-repro-test-testcreateenumpopulatespgenumrepro-0001-select-enumlabel-from-pg_catalog.pg_enum-where"},
				},
			},
		},
	})
}

// TestEnumLabelsAreCaseSensitive guards enum value identity: PostgreSQL enum
// labels are case-sensitive values, so 'Open' and 'open' are distinct labels
// for the same enum type.
func TestEnumLabelsAreCaseSensitive(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "enum labels remain case-sensitive",
			SetUpScript: []string{
				`CREATE TYPE enum_label_case_status AS ENUM ('Open', 'open');`,
				`CREATE TABLE enum_label_case_items (
					id INT PRIMARY KEY,
					status enum_label_case_status
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO enum_label_case_items VALUES (1, 'Open'), (2, 'open');`, PostgresOracle: ScriptTestPostgresOracle{ID: "enum-metadata-repro-test-testenumlabelsarecasesensitive-0001-insert-into-enum_label_case_items-values-1"},
				},
				{
					Query: `SELECT id, status::text
						FROM enum_label_case_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "enum-metadata-repro-test-testenumlabelsarecasesensitive-0002-select-id-status::text-from-enum_label_case_items"},
				},
			},
		},
	})
}

// TestEnumOrderingUsesDeclarationOrderRepro reproduces an enum correctness bug:
// PostgreSQL compares enum values by declared sort order, not by label text.
func TestEnumOrderingUsesDeclarationOrderRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "enum comparisons and ORDER BY use declaration order",
			SetUpScript: []string{
				`CREATE TYPE enum_declared_order AS ENUM ('beta', 'alpha', 'gamma');`,
				`CREATE TABLE enum_declared_order_items (
					id INT PRIMARY KEY,
					status enum_declared_order
				);`,
				`INSERT INTO enum_declared_order_items VALUES
					(1, 'gamma'),
					(2, 'beta'),
					(3, 'alpha');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 'alpha'::enum_declared_order > 'beta'::enum_declared_order,
							'beta'::enum_declared_order < 'gamma'::enum_declared_order;`, PostgresOracle: ScriptTestPostgresOracle{ID: "enum-metadata-repro-test-testenumorderingusesdeclarationorderrepro-0001-select-alpha-::enum_declared_order->-beta"},
				},
				{
					Query: `SELECT status::text
						FROM enum_declared_order_items
						ORDER BY enum_declared_order_items.status;`, PostgresOracle: ScriptTestPostgresOracle{ID: "enum-metadata-repro-test-testenumorderingusesdeclarationorderrepro-0002-select-status::text-from-enum_declared_order_items-order"},
				},
				{
					Query: `SELECT status::text
						FROM enum_declared_order_items
						ORDER BY status;`, PostgresOracle: ScriptTestPostgresOracle{ID: "enum-metadata-repro-test-testenumorderingusesdeclarationorderrepro-0003-select-status::text-from-enum_declared_order_items-order"},
				},
			},
		},
	})
}

// TestEnumMinMaxUseDeclarationOrder guards enum aggregate behavior:
// PostgreSQL min/max over enum values use declaration order, not label text
// order.
func TestEnumMinMaxUseDeclarationOrder(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "enum min and max use declaration order",
			SetUpScript: []string{
				`CREATE TYPE enum_minmax_order AS ENUM ('beta', 'alpha', 'gamma');`,
				`CREATE TABLE enum_minmax_items (
					id INT PRIMARY KEY,
					status enum_minmax_order
				);`,
				`INSERT INTO enum_minmax_items VALUES
					(1, 'gamma'),
					(2, 'beta'),
					(3, 'alpha');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT min(status)::text, max(status)::text
						FROM enum_minmax_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "enum-metadata-repro-test-testenumminmaxusedeclarationorder-0001-select-min-status-::text-max"},
				},
			},
		},
	})
}

// TestEnumArrayColumnRoundTripsValuesRepro reproduces an enum-array
// persistence bug: PostgreSQL stores arrays whose element type is an enum and
// supports ordinary array subscripting and membership predicates.
func TestEnumArrayColumnRoundTripsValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "enum array column round trips values",
			SetUpScript: []string{
				`CREATE TYPE enum_array_mood AS ENUM ('sad', 'ok', 'happy');`,
				`CREATE TABLE enum_array_items (
					id INT PRIMARY KEY,
					moods enum_array_mood[]
				);`,
				`INSERT INTO enum_array_items VALUES
					(1, ARRAY['sad', 'happy']::enum_array_mood[]);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, moods, moods[2]::text, 'sad'::enum_array_mood = ANY (moods)
						FROM enum_array_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "enum-metadata-repro-test-testenumarraycolumnroundtripsvaluesrepro-0001-select-id-moods-moods[2]::text-sad"},
				},
			},
		},
	})
}

// TestDropEnumTypeUsedByTableRequiresCascadeRepro guards enum type dependency
// enforcement for table columns.
func TestDropEnumTypeUsedByTableRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TYPE rejects table column dependencies",
			SetUpScript: []string{
				`CREATE TYPE enum_dependency_mood AS ENUM ('sad', 'ok', 'happy');`,
				`CREATE TABLE enum_dependency_items (
					id INT PRIMARY KEY,
					mood enum_dependency_mood NOT NULL
				);`,
				`INSERT INTO enum_dependency_items VALUES (1, 'ok');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TYPE enum_dependency_mood;`, PostgresOracle: ScriptTestPostgresOracle{ID: "enum-metadata-repro-test-testdropenumtypeusedbytablerequirescascaderepro-0001-drop-type-enum_dependency_mood",

						// TestDropEnumTypeUsedByColumnDefaultRequiresCascadeRepro reproduces a
						// dependency bug: PostgreSQL rejects dropping an enum type referenced by a
						// column default unless CASCADE is requested.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestDropEnumTypeUsedByColumnDefaultRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TYPE rejects column default expression dependencies",
			SetUpScript: []string{
				`CREATE TYPE enum_default_dependency_status AS ENUM ('new', 'done');`,
				`CREATE TABLE enum_default_dependency_items (
					id INT PRIMARY KEY,
					status TEXT DEFAULT ('new'::enum_default_dependency_status)::text
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TYPE enum_default_dependency_status;`, PostgresOracle: ScriptTestPostgresOracle{ID: "enum-metadata-repro-test-testdropenumtypeusedbycolumndefaultrequirescascaderepro-0001-drop-type-enum_default_dependency_status", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO enum_default_dependency_items (id)
						VALUES (1);`,
				},
				{
					Query: `SELECT id::text, status
						FROM enum_default_dependency_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "enum-metadata-repro-test-testdropenumtypeusedbycolumndefaultrequirescascaderepro-0002-select-id::text-status-from-enum_default_dependency_items"},
				},
			},
		},
	})
}

// TestDropEnumTypeUsedByCheckConstraintRequiresCascadeRepro reproduces a
// dependency bug: PostgreSQL rejects dropping an enum type referenced by a
// CHECK constraint unless CASCADE is requested.
func TestDropEnumTypeUsedByCheckConstraintRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TYPE rejects check constraint expression dependencies",
			SetUpScript: []string{
				`CREATE TYPE enum_check_dependency_status AS ENUM ('new', 'done');`,
				`CREATE TABLE enum_check_dependency_items (
					id INT PRIMARY KEY,
					status TEXT,
					CONSTRAINT enum_check_dependency_not_done
						CHECK (status::enum_check_dependency_status <> 'done'::enum_check_dependency_status)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TYPE enum_check_dependency_status;`, PostgresOracle: ScriptTestPostgresOracle{ID: "enum-metadata-repro-test-testdropenumtypeusedbycheckconstraintrequirescascaderepro-0001-drop-type-enum_check_dependency_status", Compare: "sqlstate"},
				},
				{
					Query: `INSERT INTO enum_check_dependency_items
						VALUES (1, 'new');`,
				},
				{
					Query: `INSERT INTO enum_check_dependency_items VALUES (2, 'done');`, PostgresOracle: ScriptTestPostgresOracle{ID: "enum-metadata-repro-test-testdropenumtypeusedbycheckconstraintrequirescascaderepro-0002-insert-into-enum_check_dependency_items-values-2",

						// TestDropEnumTypeUsedByGeneratedColumnRequiresCascadeRepro reproduces a
						// dependency bug: PostgreSQL rejects dropping an enum type referenced by a
						// stored generated column unless CASCADE is requested.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestDropEnumTypeUsedByGeneratedColumnRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TYPE rejects generated column expression dependencies",
			SetUpScript: []string{
				`CREATE TYPE enum_generated_dependency_status AS ENUM ('new', 'done');`,
				`CREATE TABLE enum_generated_dependency_items (
					id INT PRIMARY KEY,
					status TEXT,
					normalized TEXT GENERATED ALWAYS AS (
						(status::enum_generated_dependency_status)::text
					) STORED
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP TYPE enum_generated_dependency_status;`,
					ExpectedErr: `other objects depend on it`,
				},
				{
					Query: `INSERT INTO enum_generated_dependency_items (id, status)
						VALUES (1, 'new');`,
				},
				{
					Query: `SELECT id::text, status, normalized
						FROM enum_generated_dependency_items;`,
					Expected: []sql.Row{{"1", "new", "new"}},
				},
			},
		},
	})
}

// TestDropEnumTypeUsedByExpressionIndexRequiresCascadeRepro reproduces a
// dependency bug: PostgreSQL rejects dropping an enum type referenced by an
// expression index unless CASCADE is requested.
func TestDropEnumTypeUsedByExpressionIndexRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TYPE rejects expression index dependencies",
			SetUpScript: []string{
				`CREATE TYPE enum_index_dependency_status AS ENUM ('new', 'done');`,
				`CREATE TABLE enum_index_dependency_items (
					id INT PRIMARY KEY,
					status TEXT
				);`,
				`CREATE INDEX enum_index_dependency_status_idx
					ON enum_index_dependency_items (((status::enum_index_dependency_status)::text));`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP TYPE enum_index_dependency_status;`,
					ExpectedErr: `other objects depend on it`,
				},
				{
					Query: `INSERT INTO enum_index_dependency_items
						VALUES (1, 'new');`,
				},
				{
					Query: `SELECT id::text, status
						FROM enum_index_dependency_items;`,
					Expected: []sql.Row{{"1", "new"}},
				},
			},
		},
	})
}

// TestDropEnumTypeUsedByViewRequiresCascadeRepro reproduces a dependency bug:
// PostgreSQL rejects dropping an enum type referenced by a view unless CASCADE
// is requested.
func TestDropEnumTypeUsedByViewRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TYPE rejects view expression dependencies",
			SetUpScript: []string{
				`CREATE TYPE enum_view_dependency_status AS ENUM ('new', 'done');`,
				`CREATE VIEW enum_view_dependency_reader AS
					SELECT 'new'::enum_view_dependency_status AS status;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TYPE enum_view_dependency_status;`, PostgresOracle: ScriptTestPostgresOracle{ID: "enum-metadata-repro-test-testdropenumtypeusedbyviewrequirescascaderepro-0001-drop-type-enum_view_dependency_status",

						// TestDropEnumTypeUsedByFunctionRequiresCascadeRepro reproduces a dependency
						// bug: PostgreSQL rejects dropping an enum type referenced by a function
						// signature unless CASCADE is requested.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestDropEnumTypeUsedByFunctionRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TYPE rejects function signature dependencies",
			SetUpScript: []string{
				`CREATE TYPE enum_function_dependency_status AS ENUM ('new', 'done');`,
				`CREATE FUNCTION enum_function_dependency_label(
					input_value enum_function_dependency_status
				) RETURNS TEXT
					LANGUAGE SQL IMMUTABLE AS $$ SELECT input_value::text $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TYPE enum_function_dependency_status;`, PostgresOracle: ScriptTestPostgresOracle{ID: "enum-metadata-repro-test-testdropenumtypeusedbyfunctionrequirescascaderepro-0001-drop-type-enum_function_dependency_status",

						// TestAlterEnumAddValuePersistsUsableLabelRepro reproduces an enum
						// persistence bug: PostgreSQL persists values added with ALTER TYPE ADD VALUE
						// and accepts them in future enum-typed rows.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestAlterEnumAddValuePersistsUsableLabelRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TYPE ADD VALUE persists usable enum label",
			SetUpScript: []string{
				`CREATE TYPE enum_alter_status AS ENUM ('new', 'done');`,
				`CREATE TABLE enum_alter_items (
					id INT PRIMARY KEY,
					status enum_alter_status
				);`,
				`ALTER TYPE enum_alter_status ADD VALUE 'archived' AFTER 'new';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO enum_alter_items VALUES (1, 'archived');`,
				},
				{
					Query: `SELECT enumlabel
						FROM pg_catalog.pg_enum
						WHERE enumtypid = 'enum_alter_status'::regtype
						ORDER BY enumsortorder;`, PostgresOracle: ScriptTestPostgresOracle{ID: "enum-metadata-repro-test-testalterenumaddvaluepersistsusablelabelrepro-0001-select-enumlabel-from-pg_catalog.pg_enum-where"},
				},
				{
					Query: `SELECT status::text
						FROM enum_alter_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "enum-metadata-repro-test-testalterenumaddvaluepersistsusablelabelrepro-0002-select-status::text-from-enum_alter_items"},
				},
			},
		},
	})
}

// TestAlterEnumRenameValueUpdatesStoredRowsRepro reproduces an enum persistence
// bug: PostgreSQL renames enum labels in the catalog, so existing stored enum
// values display through the new label.
func TestAlterEnumRenameValueUpdatesStoredRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TYPE RENAME VALUE updates stored enum labels",
			SetUpScript: []string{
				`CREATE TYPE enum_rename_status AS ENUM ('new', 'done');`,
				`CREATE TABLE enum_rename_items (
					id INT PRIMARY KEY,
					status enum_rename_status
				);`,
				`INSERT INTO enum_rename_items VALUES (1, 'new');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TYPE enum_rename_status
						RENAME VALUE 'new' TO 'open';`,
				},
				{
					Query: `SELECT status::text
						FROM enum_rename_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "enum-metadata-repro-test-testalterenumrenamevalueupdatesstoredrowsrepro-0001-select-status::text-from-enum_rename_items"},
				},
				{
					Query: `INSERT INTO enum_rename_items VALUES (2, 'new');`, PostgresOracle: ScriptTestPostgresOracle{ID: "enum-metadata-repro-test-testalterenumrenamevalueupdatesstoredrowsrepro-0002-insert-into-enum_rename_items-values-2", Compare: "sqlstate"},
				},
			},
		},
	})
}
