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

// TestDropFunctionUsedByColumnDefaultRequiresCascadeRepro reproduces a
// dependency bug: PostgreSQL rejects dropping a function used by a column
// default unless CASCADE is requested.
func TestDropFunctionUsedByColumnDefaultRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP FUNCTION rejects column default dependencies",
			SetUpScript: []string{
				`CREATE FUNCTION default_dependency_value() RETURNS INT
					LANGUAGE SQL IMMUTABLE AS $$ SELECT 42 $$;`,
				`CREATE TABLE function_default_dependency_items (
					id INT PRIMARY KEY,
					v INT DEFAULT default_dependency_value()
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP FUNCTION default_dependency_value();`, PostgresOracle: ScriptTestPostgresOracle{ID: "function-dependency-repro-test-testdropfunctionusedbycolumndefaultrequirescascaderepro-0001-drop-function-default_dependency_value",

						// TestDropFunctionUsedByViewRequiresCascadeRepro reproduces a dependency bug:
						// PostgreSQL rejects dropping a function referenced by a view unless CASCADE is
						// requested.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestDropFunctionUsedByViewRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP FUNCTION rejects view dependencies",
			SetUpScript: []string{
				`CREATE FUNCTION view_dependency_value(input_value INT) RETURNS INT
					LANGUAGE SQL IMMUTABLE AS $$ SELECT input_value * 2 $$;`,
				`CREATE TABLE function_view_dependency_source (id INT PRIMARY KEY, v INT);`,
				`CREATE VIEW function_view_dependency_reader AS
					SELECT id, view_dependency_value(v) AS doubled
					FROM function_view_dependency_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP FUNCTION view_dependency_value(INT);`, PostgresOracle: ScriptTestPostgresOracle{ID: "function-dependency-repro-test-testdropfunctionusedbyviewrequirescascaderepro-0001-drop-function-view_dependency_value-int",

						// TestDropFunctionUsedByMaterializedViewRequiresCascadeRepro reproduces a
						// dependency bug: PostgreSQL rejects dropping a function referenced by a
						// materialized view unless CASCADE is requested.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestDropFunctionUsedByMaterializedViewRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP FUNCTION rejects materialized view dependencies",
			SetUpScript: []string{
				`CREATE FUNCTION matview_dependency_value(input_value INT) RETURNS INT
					LANGUAGE SQL IMMUTABLE AS $$ SELECT input_value * 2 $$;`,
				`CREATE TABLE function_matview_dependency_source (id INT PRIMARY KEY, v INT);`,
				`INSERT INTO function_matview_dependency_source VALUES (1, 5);`,
				`CREATE MATERIALIZED VIEW function_matview_dependency_reader AS
					SELECT id, matview_dependency_value(v) AS doubled
					FROM function_matview_dependency_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP FUNCTION matview_dependency_value(INT);`, PostgresOracle: ScriptTestPostgresOracle{ID: "function-dependency-repro-test-testdropfunctionusedbymaterializedviewrequirescascaderepro-0001-drop-function-matview_dependency_value-int",

						// TestDropFunctionUsedByTriggerRequiresCascadeRepro reproduces a dependency
						// bug: PostgreSQL rejects dropping a trigger function while a trigger still
						// depends on it unless CASCADE is requested.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestDropFunctionUsedByTriggerRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP FUNCTION rejects trigger dependencies",
			SetUpScript: []string{
				`CREATE TABLE function_trigger_dependency_target (id INT PRIMARY KEY, v TEXT);`,
				`CREATE FUNCTION trigger_dependency_value() RETURNS TRIGGER AS $$
				BEGIN
					NEW.v := NEW.v || '_triggered';
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER function_trigger_dependency_before_insert
					BEFORE INSERT ON function_trigger_dependency_target
					FOR EACH ROW EXECUTE FUNCTION trigger_dependency_value();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP FUNCTION trigger_dependency_value();`, PostgresOracle: ScriptTestPostgresOracle{ID: "function-dependency-repro-test-testdropfunctionusedbytriggerrequirescascaderepro-0001-drop-function-trigger_dependency_value",

						// TestDropFunctionUsedByGeneratedColumnRequiresCascadeRepro reproduces a
						// dependency bug: PostgreSQL rejects dropping a function referenced by a stored
						// generated column unless CASCADE is requested.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestDropFunctionUsedByGeneratedColumnRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP FUNCTION rejects generated column dependencies",
			SetUpScript: []string{
				`CREATE FUNCTION generated_dependency_value(input_value INT) RETURNS INT
					LANGUAGE SQL IMMUTABLE AS $$ SELECT input_value * 2 $$;`,
				`CREATE TABLE function_generated_dependency_items (
					id INT PRIMARY KEY,
					v INT,
					doubled INT GENERATED ALWAYS AS (generated_dependency_value(v)) STORED
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP FUNCTION generated_dependency_value(INT);`, PostgresOracle: ScriptTestPostgresOracle{ID: "function-dependency-repro-test-testdropfunctionusedbygeneratedcolumnrequirescascaderepro-0001-drop-function-generated_dependency_value-int",

						// TestDropFunctionUsedByExpressionIndexRequiresCascadeRepro reproduces a
						// dependency bug: PostgreSQL rejects dropping a function referenced by an
						// expression index unless CASCADE is requested.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestDropFunctionUsedByExpressionIndexRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP FUNCTION rejects expression index dependencies",
			SetUpScript: []string{
				`CREATE FUNCTION expression_index_dependency_value(input_value INT) RETURNS INT
					LANGUAGE SQL IMMUTABLE AS $$ SELECT input_value * 2 $$;`,
				`CREATE TABLE function_expression_index_dependency_items (
					id INT PRIMARY KEY,
					v INT
				);`,
				`CREATE INDEX function_expression_index_dependency_idx
					ON function_expression_index_dependency_items (
						expression_index_dependency_value(v)
					);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP FUNCTION expression_index_dependency_value(INT);`, PostgresOracle: ScriptTestPostgresOracle{ID: "function-dependency-repro-test-testdropfunctionusedbyexpressionindexrequirescascaderepro-0001-drop-function-expression_index_dependency_value-int", Compare: "sqlstate"},
				},
			},
		},
	})
}
