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
)

// TestInformationSchemaTablesHidesUngrantableTablesRepro reproduces an
// information_schema compatibility bug: ordinary users should be able to query
// information_schema.tables, with rows filtered by object privileges.
func TestInformationSchemaTablesHidesUngrantableTablesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "information_schema.tables hides ungranted tables",
			SetUpScript: []string{
				`CREATE USER info_schema_viewer PASSWORD 'pw';`,
				`CREATE TABLE info_schema_private (id INT PRIMARY KEY);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT table_name
						FROM information_schema.tables
						WHERE table_schema = 'public'
							AND table_name = 'info_schema_private';`,
					Expected: []sql.Row{},
					Username: `info_schema_viewer`,
					Password: `pw`,
				},
			},
		},
	})
}

// TestInformationSchemaColumnsHidesUngrantableColumnsRepro reproduces an
// information_schema compatibility bug: ordinary users should be able to query
// information_schema.columns, with rows filtered by object privileges.
func TestInformationSchemaColumnsHidesUngrantableColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "information_schema.columns hides ungranted columns",
			SetUpScript: []string{
				`CREATE USER info_schema_column_viewer PASSWORD 'pw';`,
				`CREATE TABLE info_schema_columns_private (
					id INT PRIMARY KEY,
					secret TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT column_name
						FROM information_schema.columns
						WHERE table_schema = 'public'
							AND table_name = 'info_schema_columns_private';`,
					Expected: []sql.Row{},
					Username: `info_schema_column_viewer`,
					Password: `pw`,
				},
			},
		},
	})
}

// TestInformationSchemaSchemataHidesUngrantableSchemasRepro reproduces an
// information_schema compatibility bug: ordinary users should be able to query
// information_schema.schemata, with rows filtered by schema privileges.
func TestInformationSchemaSchemataHidesUngrantableSchemasRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "information_schema.schemata hides ungranted schemas",
			SetUpScript: []string{
				`CREATE USER info_schema_schema_viewer PASSWORD 'pw';`,
				`CREATE SCHEMA info_schema_private_schema;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT schema_name
						FROM information_schema.schemata
						WHERE schema_name = 'info_schema_private_schema';`,

					Username: `info_schema_schema_viewer`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestInformationSchemaViewsHidesUngrantableViewsRepro reproduces an
						// information_schema compatibility bug: ordinary users should be able to query
						// information_schema.views, with rows filtered by object privileges.
						ID: "information-schema-visibility-repro-test-testinformationschemaschematahidesungrantableschemasrepro-0001-select-schema_name-from-information_schema.schemata-where"},
				},
			},
		},
	})
}

func TestInformationSchemaViewsHidesUngrantableViewsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "information_schema.views hides ungranted views",
			SetUpScript: []string{
				`CREATE USER info_schema_view_viewer PASSWORD 'pw';`,
				`CREATE TABLE info_schema_view_private_base (id INT PRIMARY KEY);`,
				`CREATE VIEW info_schema_private_view AS
					SELECT id FROM info_schema_view_private_base;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT table_name
						FROM information_schema.views
						WHERE table_schema = 'public'
							AND table_name = 'info_schema_private_view';`,
					Expected: []sql.Row{},
					Username: `info_schema_view_viewer`,
					Password: `pw`,
				},
			},
		},
	})
}

// TestInformationSchemaTriggersHidesUngrantableTriggersRepro reproduces an
// information_schema compatibility bug: ordinary users should be able to query
// information_schema.triggers, with rows filtered by trigger/table privileges.
func TestInformationSchemaTriggersHidesUngrantableTriggersRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "information_schema.triggers hides ungranted triggers",
			SetUpScript: []string{
				`CREATE USER info_schema_trigger_viewer PASSWORD 'pw';`,
				`CREATE TABLE info_schema_trigger_private (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE FUNCTION info_schema_trigger_private_func() RETURNS trigger AS $$
					BEGIN
						NEW.label := upper(NEW.label);
						RETURN NEW;
					END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER info_schema_trigger_private_before_insert
					BEFORE INSERT ON info_schema_trigger_private
					FOR EACH ROW EXECUTE FUNCTION info_schema_trigger_private_func();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT trigger_name
						FROM information_schema.triggers
						WHERE event_object_schema = 'public'
							AND event_object_table = 'info_schema_trigger_private';`,

					Username: `info_schema_trigger_viewer`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestInformationSchemaTablePrivilegesHidesUngrantableTablesRepro reproduces
						// an information_schema compatibility bug: ordinary users should be able to
						// query information_schema.table_privileges, with rows filtered by privileges
						// they hold or granted.
						ID: "information-schema-visibility-repro-test-testinformationschematriggershidesungrantabletriggersrepro-0001-select-trigger_name-from-information_schema.triggers-where"},
				},
			},
		},
	})
}

func TestInformationSchemaTablePrivilegesHidesUngrantableTablesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "information_schema.table_privileges hides ungranted tables",
			SetUpScript: []string{
				`CREATE USER info_schema_privilege_viewer PASSWORD 'pw';`,
				`CREATE TABLE info_schema_privilege_private (id INT PRIMARY KEY);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT table_name
						FROM information_schema.table_privileges
						WHERE table_schema = 'public'
							AND table_name = 'info_schema_privilege_private';`,
					Expected: []sql.Row{},
					Username: `info_schema_privilege_viewer`,
					Password: `pw`,
				},
			},
		},
	})
}

// TestInformationSchemaTablePrivilegesReflectsTableGrantRepro reproduces an
// ACL introspection bug: information_schema.table_privileges should expose
// table-level grants visible to the grantee.
func TestInformationSchemaTablePrivilegesReflectsTableGrantRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "information_schema.table_privileges reflects granted tables",
			SetUpScript: []string{
				`CREATE USER info_schema_table_grantee PASSWORD 'pw';`,
				`CREATE TABLE info_schema_table_grant_private (id INT PRIMARY KEY);`,
				`GRANT USAGE ON SCHEMA public TO info_schema_table_grantee;`,
				`GRANT SELECT ON info_schema_table_grant_private
					TO info_schema_table_grantee;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT table_name, privilege_type
						FROM information_schema.table_privileges
						WHERE table_schema = 'public'
							AND table_name = 'info_schema_table_grant_private'
							AND grantee = current_user
						ORDER BY table_name, privilege_type;`,
					Expected: []sql.Row{{"info_schema_table_grant_private", "SELECT"}},
					Username: `info_schema_table_grantee`,
					Password: `pw`,
				},
			},
		},
	})
}

// TestInformationSchemaColumnPrivilegesReflectsColumnGrantRepro reproduces an
// ACL introspection bug: information_schema.column_privileges should expose
// column-level grants visible to the grantee.
func TestInformationSchemaColumnPrivilegesReflectsColumnGrantRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "information_schema.column_privileges reflects granted columns",
			SetUpScript: []string{
				`CREATE USER info_schema_column_grantee PASSWORD 'pw';`,
				`CREATE TABLE info_schema_column_grant_private (
					id INT PRIMARY KEY,
					secret TEXT
				);`,
				`GRANT USAGE ON SCHEMA public TO info_schema_column_grantee;`,
				`GRANT SELECT (id) ON info_schema_column_grant_private
					TO info_schema_column_grantee;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT column_name, privilege_type
						FROM information_schema.column_privileges
						WHERE table_schema = 'public'
							AND table_name = 'info_schema_column_grant_private'
							AND grantee = current_user
						ORDER BY column_name, privilege_type;`,
					Expected: []sql.Row{{"id", "SELECT"}},
					Username: `info_schema_column_grantee`,
					Password: `pw`,
				},
			},
		},
	})
}

// TestInformationSchemaRoutinePrivilegesReflectsFunctionGrantRepro reproduces
// an ACL introspection bug: information_schema.routine_privileges should expose
// function EXECUTE grants visible to the grantee.
func TestInformationSchemaRoutinePrivilegesReflectsFunctionGrantRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "information_schema.routine_privileges reflects granted functions",
			SetUpScript: []string{
				`CREATE USER info_schema_routine_grantee PASSWORD 'pw';`,
				`CREATE FUNCTION info_schema_routine_grant_target(input INT)
					RETURNS INT
					LANGUAGE SQL
					AS $$ SELECT input + 1 $$;`,
				`GRANT USAGE ON SCHEMA public TO info_schema_routine_grantee;`,
				`GRANT EXECUTE ON FUNCTION info_schema_routine_grant_target(INT)
					TO info_schema_routine_grantee;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT routine_name, privilege_type
						FROM information_schema.routine_privileges
						WHERE routine_schema = 'public'
							AND routine_name = 'info_schema_routine_grant_target'
							AND grantee = current_user
						ORDER BY routine_name, privilege_type;`,

					Username: `info_schema_routine_grantee`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{ID: "information-schema-visibility-repro-test-testinformationschemaroutineprivilegesreflectsfunctiongrantrepro-0001-select-routine_name-privilege_type-from-information_schema.routine_privileges"},
				},
			},
		},
	})
}
