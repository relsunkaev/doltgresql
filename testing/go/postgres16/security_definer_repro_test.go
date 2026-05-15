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

// TestSecurityDefinerFunctionUsesOwnerPrivilegesRepro reproduces a security
// semantics bug: Doltgres accepts CREATE FUNCTION ... SECURITY DEFINER, but
// SQL functions still run with the caller's table privileges.
func TestSecurityDefinerFunctionUsesOwnerPrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SECURITY DEFINER function reads through owner privileges",
			SetUpScript: []string{
				`CREATE USER definer_reader PASSWORD 'reader';`,
				`CREATE TABLE definer_private (id INT PRIMARY KEY, secret TEXT);`,
				`INSERT INTO definer_private VALUES (1, 'alpha');`,
				`CREATE FUNCTION definer_secret() RETURNS TEXT
					LANGUAGE SQL
					SECURITY DEFINER
					AS $$ SELECT secret FROM definer_private WHERE id = 1 $$;`,
				`GRANT USAGE ON SCHEMA public TO definer_reader;`,
				`GRANT EXECUTE ON FUNCTION definer_secret() TO definer_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT definer_secret();`,

					Username: `definer_reader`,
					Password: `reader`, PostgresOracle: ScriptTestPostgresOracle{

						// TestSecurityDefinerProcedureUsesOwnerPrivilegesRepro reproduces the same
						// security semantics bug for CALL: accepted SECURITY DEFINER procedures still
						// execute SQL statements with the caller's table privileges.
						ID: "security-definer-repro-test-testsecuritydefinerfunctionusesownerprivilegesrepro-0001-select-definer_secret"},
				},
			},
		},
	})
}

func TestSecurityDefinerProcedureUsesOwnerPrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SECURITY DEFINER procedure writes through owner privileges",
			SetUpScript: []string{
				`CREATE USER definer_proc_caller PASSWORD 'caller';`,
				`CREATE TABLE definer_proc_private (id INT PRIMARY KEY, secret TEXT);`,
				`CREATE PROCEDURE definer_proc_insert()
					LANGUAGE SQL
					SECURITY DEFINER
					AS $$ INSERT INTO definer_proc_private VALUES (1, 'alpha') $$;`,
				`GRANT USAGE ON SCHEMA public TO definer_proc_caller;`,
				`GRANT EXECUTE ON PROCEDURE definer_proc_insert() TO definer_proc_caller;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CALL definer_proc_insert();`,

					Username: `definer_proc_caller`,
					Password: `caller`, PostgresOracle: ScriptTestPostgresOracle{ID: "security-definer-repro-test-testsecuritydefinerprocedureusesownerprivilegesrepro-0001-call-definer_proc_insert"},
				},
				{
					Query: `SELECT id, secret FROM definer_proc_private ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "security-definer-repro-test-testsecuritydefinerprocedureusesownerprivilegesrepro-0002-select-id-secret-from-definer_proc_private"},
				},
			},
		},
	})
}
