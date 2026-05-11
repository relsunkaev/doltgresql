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

// TestAlterDatabaseRenameRepro reproduces a database DDL correctness bug:
// PostgreSQL can rename a database that is not the current connection target.
func TestAlterDatabaseRenameRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER DATABASE RENAME TO renames the database",
			SetUpScript: []string{
				`CREATE DATABASE rename_database_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER DATABASE rename_database_source
						RENAME TO rename_database_target;`,
				},
				{
					Query: `USE rename_database_target;`,
				},
			},
		},
	})
}

// TestAlterDatabaseCatalogOptionsRepro reproduces database DDL catalog
// persistence bugs: PostgreSQL accepts ALTER DATABASE catalog options and
// stores them in pg_database.
func TestAlterDatabaseCatalogOptionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER DATABASE ALLOW_CONNECTIONS persists datallowconn",
			SetUpScript: []string{
				`CREATE DATABASE alter_no_connections_db;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER DATABASE alter_no_connections_db
						WITH ALLOW_CONNECTIONS false;`,
				},
				{
					Query: `SELECT datallowconn
						FROM pg_database
						WHERE datname = 'alter_no_connections_db';`,
					Expected: []sql.Row{{"f"}},
				},
			},
		},
		{
			Name: "ALTER DATABASE CONNECTION LIMIT persists datconnlimit",
			SetUpScript: []string{
				`CREATE DATABASE alter_connection_limit_db;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER DATABASE alter_connection_limit_db
						WITH CONNECTION LIMIT 0;`,
				},
				{
					Query: `SELECT datconnlimit
						FROM pg_database
						WHERE datname = 'alter_connection_limit_db';`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
		{
			Name: "ALTER DATABASE IS_TEMPLATE persists datistemplate",
			SetUpScript: []string{
				`CREATE DATABASE alter_template_option_db;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER DATABASE alter_template_option_db
						WITH IS_TEMPLATE true;`,
				},
				{
					Query: `SELECT datistemplate
						FROM pg_database
						WHERE datname = 'alter_template_option_db';`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
	})
}

// TestAlterDatabaseSetPopulatesPgDbRoleSettingRepro reproduces a catalog
// persistence bug: PostgreSQL persists ALTER DATABASE ... SET configuration in
// pg_db_role_setting.
func TestAlterDatabaseSetPopulatesPgDbRoleSettingRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER DATABASE SET populates pg_db_role_setting",
			SetUpScript: []string{
				`CREATE DATABASE database_setting_catalog;`,
				`ALTER DATABASE database_setting_catalog SET work_mem = '64kB';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT setrole, setdatabase::regdatabase::text,
							array_to_string(setconfig, ',')
						FROM pg_catalog.pg_db_role_setting
						WHERE setdatabase = 'database_setting_catalog'::regdatabase;`,
					Expected: []sql.Row{{uint32(0), "database_setting_catalog", "work_mem=64kB"}},
				},
			},
		},
	})
}
