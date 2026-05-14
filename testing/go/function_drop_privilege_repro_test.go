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
)

// TestDropFunctionClearsExecutePrivilegeRepro reproduces an ACL persistence bug:
// dropping a function does not clear its EXECUTE privileges, so a later function
// with the same signature inherits access granted to the dropped function.
func TestDropFunctionClearsExecutePrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP FUNCTION clears EXECUTE privilege before recreate",
			SetUpScript: []string{
				`CREATE USER drop_recreate_function_user PASSWORD 'function';`,
				`CREATE FUNCTION drop_recreate_acl_func()
					RETURNS TEXT
					LANGUAGE SQL
					AS $$ SELECT 'old visible' $$;`,
				`REVOKE ALL ON FUNCTION drop_recreate_acl_func() FROM PUBLIC;`,
				`GRANT USAGE ON SCHEMA public TO drop_recreate_function_user;`,
				`GRANT EXECUTE ON FUNCTION drop_recreate_acl_func()
					TO drop_recreate_function_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT drop_recreate_acl_func();`,

					Username: `drop_recreate_function_user`,
					Password: `function`, PostgresOracle: ScriptTestPostgresOracle{ID: "function-drop-privilege-repro-test-testdropfunctionclearsexecuteprivilegerepro-0001-select-drop_recreate_acl_func"},
				},
				{
					Query: `DROP FUNCTION drop_recreate_acl_func();`,
				},
				{
					Query: `CREATE FUNCTION drop_recreate_acl_func()
						RETURNS TEXT
						LANGUAGE SQL
						AS $$ SELECT 'new sensitive' $$;`,
				},
				{
					Query: `REVOKE ALL ON FUNCTION drop_recreate_acl_func() FROM PUBLIC;`,
				},
				{
					Query: `SELECT drop_recreate_acl_func();`,

					Username: `drop_recreate_function_user`,
					Password: `function`, PostgresOracle: ScriptTestPostgresOracle{ID: "function-drop-privilege-repro-test-testdropfunctionclearsexecuteprivilegerepro-0002-select-drop_recreate_acl_func", Compare: "sqlstate"},
				},
			},
		},
	})
}
