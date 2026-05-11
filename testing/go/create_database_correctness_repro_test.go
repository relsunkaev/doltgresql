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
