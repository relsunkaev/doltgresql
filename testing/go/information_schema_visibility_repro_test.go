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
					Expected: []sql.Row{},
					Username: `info_schema_schema_viewer`,
					Password: `pw`,
				},
			},
		},
	})
}

// TestInformationSchemaViewsHidesUngrantableViewsRepro reproduces an
// information_schema compatibility bug: ordinary users should be able to query
// information_schema.views, with rows filtered by object privileges.
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
					Expected: []sql.Row{},
					Username: `info_schema_trigger_viewer`,
					Password: `pw`,
				},
			},
		},
	})
}

// TestInformationSchemaTablePrivilegesHidesUngrantableTablesRepro reproduces
// an information_schema compatibility bug: ordinary users should be able to
// query information_schema.table_privileges, with rows filtered by privileges
// they hold or granted.
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
