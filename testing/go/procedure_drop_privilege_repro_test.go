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

import "testing"

// TestDropProcedureClearsExecutePrivilegeRepro reproduces an ACL persistence
// bug: dropping a procedure does not clear its EXECUTE privileges, so a later
// procedure with the same signature inherits access granted to the dropped
// procedure.
func TestDropProcedureClearsExecutePrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP PROCEDURE clears EXECUTE privilege before recreate",
			SetUpScript: []string{
				`CREATE USER drop_recreate_procedure_user PASSWORD 'procedure';`,
				`CREATE PROCEDURE drop_recreate_acl_proc()
					LANGUAGE SQL
					AS $$ SELECT 1 $$;`,
				`REVOKE ALL ON PROCEDURE drop_recreate_acl_proc() FROM PUBLIC;`,
				`GRANT USAGE ON SCHEMA public TO drop_recreate_procedure_user;`,
				`GRANT EXECUTE ON PROCEDURE drop_recreate_acl_proc()
					TO drop_recreate_procedure_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `CALL drop_recreate_acl_proc();`,
					Username: `drop_recreate_procedure_user`,
					Password: `procedure`,
				},
				{
					Query: `DROP PROCEDURE drop_recreate_acl_proc();`,
				},
				{
					Query: `CREATE PROCEDURE drop_recreate_acl_proc()
						LANGUAGE SQL
						AS $$ SELECT 2 $$;`,
				},
				{
					Query: `REVOKE ALL ON PROCEDURE drop_recreate_acl_proc() FROM PUBLIC;`,
				},
				{
					Query: `CALL drop_recreate_acl_proc();`,

					Username: `drop_recreate_procedure_user`,
					Password: `procedure`, PostgresOracle: ScriptTestPostgresOracle{ID: "procedure-drop-privilege-repro-test-testdropprocedureclearsexecuteprivilegerepro-0001-call-drop_recreate_acl_proc", Compare: "sqlstate"},
				},
			},
		},
	})
}
