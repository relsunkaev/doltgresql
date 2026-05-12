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

// TestCreateDatabaseRejectsInvalidEncodingRepro reproduces a database DDL
// correctness bug: PostgreSQL rejects unknown CREATE DATABASE encodings.
func TestCreateDatabaseRejectsInvalidEncodingRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE DATABASE rejects invalid encoding",
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE DATABASE invalid_encoding_db ENCODING notexist;`,
					ExpectedErr: `not a valid encoding name`,
				},
			},
		},
	})
}

// TestCreateDatabaseDefaultTablespace guards that CREATE DATABASE accepts
// TABLESPACE pg_default, since that is the only tablespace Doltgres exposes
// and PostgreSQL allows spelling out the default.
func TestCreateDatabaseDefaultTablespace(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE DATABASE with default tablespace",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE DATABASE default_tablespace_db TABLESPACE pg_default;`,
				},
				{
					Query: `USE default_tablespace_db;`,
				},
			},
		},
	})
}

// TestCreateDatabaseUnknownTablespaceErrors guards that CREATE DATABASE
// targeting a tablespace that does not exist returns PostgreSQL's catalog
// error rather than silently creating the database in the default tablespace.
func TestCreateDatabaseUnknownTablespaceErrors(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE DATABASE rejects unknown tablespace",
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE DATABASE bad_tablespace_db TABLESPACE custom_space;`,
					ExpectedErr: `tablespace "custom_space" does not exist`,
				},
			},
		},
	})
}

// TestCreateDatabaseCatalogOptionsRepro reproduces database DDL correctness
// bugs: PostgreSQL accepts CREATE DATABASE catalog options and stores them in
// pg_database.
func TestCreateDatabaseCatalogOptionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE DATABASE ALLOW_CONNECTIONS persists datallowconn",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE DATABASE no_connections_db ALLOW_CONNECTIONS false;`,
				},
				{
					Query: `SELECT datallowconn
						FROM pg_database
						WHERE datname = 'no_connections_db';`,
					Expected: []sql.Row{{false}},
				},
			},
		},
		{
			Name: "CREATE DATABASE CONNECTION LIMIT persists datconnlimit",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE DATABASE connection_limit_db CONNECTION LIMIT 0;`,
				},
				{
					Query: `SELECT datconnlimit
						FROM pg_database
						WHERE datname = 'connection_limit_db';`,
					Expected: []sql.Row{{int32(0)}},
				},
			},
		},
		{
			Name: "CREATE DATABASE IS_TEMPLATE persists datistemplate",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE DATABASE template_option_db IS_TEMPLATE true;`,
				},
				{
					Query: `SELECT datistemplate
						FROM pg_database
						WHERE datname = 'template_option_db';`,
					Expected: []sql.Row{{true}},
				},
			},
		},
	})
}

// TestCreateDatabaseTemplateCopiesDataRepro reproduces a database DDL
// consistency bug: PostgreSQL copies schema and data from the named template
// database into the newly-created database.
func TestCreateDatabaseTemplateCopiesDataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE DATABASE TEMPLATE copies source database data",
			SetUpScript: []string{
				`CREATE DATABASE template_source_db;`,
				`USE template_source_db;`,
				`CREATE TABLE template_copy_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO template_copy_items VALUES (1, 'copied');`,
				`USE postgres;`,
				`CREATE DATABASE template_copied_db TEMPLATE template_source_db;`,
				`USE template_copied_db;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label
						FROM template_copy_items;`,
					Expected: []sql.Row{{1, "copied"}},
				},
			},
		},
	})
}

// TestCreateDatabaseStrategyWalLogRepro reproduces a database DDL compatibility
// gap: PostgreSQL accepts CREATE DATABASE STRATEGY WAL_LOG.
func TestCreateDatabaseStrategyWalLogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE DATABASE STRATEGY WAL_LOG creates usable database",
			SetUpScript: []string{
				`CREATE DATABASE strategy_wal_log_db STRATEGY WAL_LOG;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `USE strategy_wal_log_db;`,
				},
				{
					Query: `CREATE TABLE strategy_created_items (
						id INT PRIMARY KEY
					);`,
				},
				{
					Query: `SELECT datname
						FROM pg_database
						WHERE datname = 'strategy_wal_log_db';`,
					Expected: []sql.Row{{"strategy_wal_log_db"}},
				},
			},
		},
	})
}

// TestCreateDatabaseOidOptionPersistsCatalogRepro reproduces a database DDL
// catalog gap: PostgreSQL accepts CREATE DATABASE OID and stores the requested
// OID in pg_database.
func TestCreateDatabaseOidOptionPersistsCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE DATABASE OID persists pg_database oid",
			SetUpScript: []string{
				`CREATE DATABASE oid_option_db OID 987654;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT oid::INT
						FROM pg_database
						WHERE datname = 'oid_option_db';`,
					Expected: []sql.Row{{987654}},
				},
			},
		},
	})
}

// TestCreateDatabaseLocaleProviderRepro reproduces a database DDL compatibility
// gap: PostgreSQL dump headers can specify TEMPLATE template0 with libc locale
// provider options.
func TestCreateDatabaseLocaleProviderRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE DATABASE LOCALE_PROVIDER creates usable database",
			SetUpScript: []string{
				`CREATE DATABASE locale_provider_db
					WITH TEMPLATE = template0
					LOCALE_PROVIDER = libc
					LOCALE = 'C';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `USE locale_provider_db;`,
				},
				{
					Query: `SELECT datname
						FROM pg_database
						WHERE datname = 'locale_provider_db';`,
					Expected: []sql.Row{{"locale_provider_db"}},
				},
			},
		},
	})
}

// TestCreateDatabaseLocaleCatalogRepro reproduces a database DDL catalog gap:
// PostgreSQL stores LC_COLLATE and LC_CTYPE in pg_database.
func TestCreateDatabaseLocaleCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE DATABASE LC_COLLATE LC_CTYPE persist catalog values",
			SetUpScript: []string{
				`CREATE DATABASE locale_catalog_db
					WITH TEMPLATE = template0
					LC_COLLATE = 'C'
					LC_CTYPE = 'C';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT datcollate, datctype
						FROM pg_database
						WHERE datname = 'locale_catalog_db';`,
					Expected: []sql.Row{{"C", "C"}},
				},
			},
		},
	})
}

// TestCreateDatabaseCollationVersionRepro reproduces a database DDL catalog
// gap: PostgreSQL accepts COLLATION_VERSION and exposes it through pg_database.
func TestCreateDatabaseCollationVersionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE DATABASE COLLATION_VERSION persists datcollversion",
			SetUpScript: []string{
				`CREATE DATABASE collation_version_db
					WITH TEMPLATE = template0
					COLLATION_VERSION = '1';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT datcollversion
						FROM pg_database
						WHERE datname = 'collation_version_db';`,
					Expected: []sql.Row{{"1"}},
				},
			},
		},
	})
}

// TestCreateDatabaseIcuLocaleRepro reproduces a database DDL catalog gap:
// PostgreSQL accepts ICU_LOCALE for ICU-backed databases and stores the locale
// in pg_database.daticulocale.
func TestCreateDatabaseIcuLocaleRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE DATABASE ICU_LOCALE persists daticulocale",
			SetUpScript: []string{
				`CREATE DATABASE icu_locale_db
					WITH TEMPLATE = template0
					ICU_LOCALE = 'und';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT daticulocale
						FROM pg_database
						WHERE datname = 'icu_locale_db';`,
					Expected: []sql.Row{{"und"}},
				},
			},
		},
	})
}

// TestCreateDatabaseIcuRulesRepro reproduces a database DDL catalog gap:
// PostgreSQL accepts ICU_RULES and stores them in pg_database.daticurules.
func TestCreateDatabaseIcuRulesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE DATABASE ICU_RULES persists daticurules",
			SetUpScript: []string{
				`CREATE DATABASE icu_rules_db
					ICU_RULES = '&V << w <<< W';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT daticurules
						FROM pg_database
						WHERE datname = 'icu_rules_db';`,
					Expected: []sql.Row{{"&V << w <<< W"}},
				},
			},
		},
	})
}
