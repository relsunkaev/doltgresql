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

// TestAlterDefaultPrivilegesGrantAppliesToFutureTablesRepro reproduces a
// security/ACL persistence bug: Doltgres accepts ALTER DEFAULT PRIVILEGES, but
// the default grant is not applied to tables created later in the schema.
func TestAlterDefaultPrivilegesGrantAppliesToFutureTablesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "default SELECT grant applies to future tables",
			SetUpScript: []string{
				`CREATE USER default_reader PASSWORD 'reader';`,
				`GRANT USAGE ON SCHEMA public TO default_reader;`,
				`ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO default_reader;`,
				`CREATE TABLE default_priv_items (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO default_priv_items VALUES (1, 'alpha'), (2, 'beta');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT id, label FROM default_priv_items ORDER BY id;`,
					Expected: []sql.Row{{1, "alpha"}, {2, "beta"}},
					Username: `default_reader`,
					Password: `reader`,
				},
			},
		},
	})
}

// TestAlterDefaultPrivilegesDoesNotGrantExistingTablesRepro guards the
// PostgreSQL boundary that ALTER DEFAULT PRIVILEGES affects objects created
// after the default ACL change, not objects that already existed.
func TestAlterDefaultPrivilegesDoesNotGrantExistingTablesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "default SELECT grant does not apply to existing tables",
			SetUpScript: []string{
				`CREATE USER default_existing_reader PASSWORD 'reader';`,
				`GRANT USAGE ON SCHEMA public TO default_existing_reader;`,
				`CREATE TABLE default_priv_existing (id INT PRIMARY KEY);`,
				`INSERT INTO default_priv_existing VALUES (1);`,
				`ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO default_existing_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `SELECT id FROM default_priv_existing;`,
					ExpectedErr: `permission denied`,
					Username:    `default_existing_reader`,
					Password:    `reader`,
				},
			},
		},
	})
}

// TestAlterDefaultPrivilegesGrantAppliesToFutureSequencesRepro reproduces a
// security/ACL persistence bug: Doltgres accepts ALTER DEFAULT PRIVILEGES for
// sequences, but the default grant is not applied to sequences created later.
func TestAlterDefaultPrivilegesGrantAppliesToFutureSequencesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "default USAGE grant applies to future sequences",
			SetUpScript: []string{
				`CREATE USER default_sequence_user PASSWORD 'sequence';`,
				`GRANT USAGE ON SCHEMA public TO default_sequence_user;`,
				`ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE ON SEQUENCES TO default_sequence_user;`,
				`CREATE SEQUENCE default_priv_sequence START WITH 5;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT nextval('default_priv_sequence');`,
					Expected: []sql.Row{{5}},
					Username: `default_sequence_user`,
					Password: `sequence`,
				},
			},
		},
	})
}

// TestAlterDefaultPrivilegesGrantAppliesToFutureFunctionsRepro reproduces a
// security/ACL persistence bug: Doltgres accepts ALTER DEFAULT PRIVILEGES for
// functions, but the default grant is not applied to functions created later.
func TestAlterDefaultPrivilegesGrantAppliesToFutureFunctionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "default EXECUTE grant applies to future functions",
			SetUpScript: []string{
				`CREATE USER default_function_user PASSWORD 'function';`,
				`GRANT USAGE ON SCHEMA public TO default_function_user;`,
				`ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT EXECUTE ON FUNCTIONS TO default_function_user;`,
				`CREATE FUNCTION default_priv_function() RETURNS int AS $$ BEGIN RETURN 7; END; $$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT default_priv_function();`,

					Username: `default_function_user`,
					Password: `function`, PostgresOracle: ScriptTestPostgresOracle{

						// TestAlterDefaultPrivilegesPopulatesPgDefaultAclRepro reproduces a catalog
						// persistence bug: Doltgres accepts ALTER DEFAULT PRIVILEGES but pg_default_acl
						// does not expose the default ACL row.
						ID: "default-privileges-repro-test-testalterdefaultprivilegesgrantappliestofuturefunctionsrepro-0001-select-default_priv_function"},
				},
			},
		},
	})
}

func TestAlterDefaultPrivilegesPopulatesPgDefaultAclRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER DEFAULT PRIVILEGES populates pg_default_acl",
			SetUpScript: []string{
				`CREATE USER default_acl_catalog_user PASSWORD 'acl';`,
				`ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO default_acl_catalog_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT defaclobjtype
						FROM pg_catalog.pg_default_acl
						WHERE defaclnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public');`,
					Expected: []sql.Row{{"r"}},
				},
			},
		},
	})
}
