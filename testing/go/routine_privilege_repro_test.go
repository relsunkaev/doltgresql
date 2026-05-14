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

// TestFunctionExecuteGrantDoesNotApplyToOtherOverloadsRepro reproduces a
// security bug: granting EXECUTE on one function overload grants access to
// another overload with the same name.
func TestFunctionExecuteGrantDoesNotApplyToOtherOverloadsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "function EXECUTE grant is overload-specific",
			SetUpScript: []string{
				`CREATE USER function_overload_user PASSWORD 'pw';`,
				`CREATE FUNCTION overload_secret(input INT) RETURNS INT
					LANGUAGE SQL
					AS $$ SELECT input + 1 $$;`,
				`CREATE FUNCTION overload_secret(input TEXT) RETURNS TEXT
					LANGUAGE SQL
					AS $$ SELECT input || '-secret' $$;`,
				`GRANT USAGE ON SCHEMA public TO function_overload_user;`,
				`GRANT EXECUTE ON FUNCTION overload_secret(INT) TO function_overload_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT overload_secret('hidden'::TEXT);`,

					Username: `function_overload_user`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

					// TestProcedureExecuteGrantDoesNotApplyToOtherOverloadsRepro reproduces a
					// security bug: granting EXECUTE on one procedure overload grants access to
					// another overload with the same name.
					ID: "routine-privilege-repro-test-testfunctionexecutegrantdoesnotapplytootheroverloadsrepro-0001-select-overload_secret-hidden-::text"},
				},
			},
		},
	})
}

func TestProcedureExecuteGrantDoesNotApplyToOtherOverloadsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "procedure EXECUTE grant is overload-specific",
			SetUpScript: []string{
				`CREATE USER procedure_overload_user PASSWORD 'pw';`,
				`CREATE PROCEDURE overload_secret_proc(input INT)
					LANGUAGE SQL
					AS $$ SELECT input + 1 $$;`,
				`CREATE PROCEDURE overload_secret_proc(input TEXT)
					LANGUAGE SQL
					AS $$ SELECT input || '-secret' $$;`,
				`GRANT USAGE ON SCHEMA public TO procedure_overload_user;`,
				`GRANT EXECUTE ON PROCEDURE overload_secret_proc(INT) TO procedure_overload_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CALL overload_secret_proc('hidden'::TEXT);`,

					Username: `procedure_overload_user`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

					// TestCreateFunctionLeakproofRequiresSuperuserRepro reproduces a security bug:
					// PostgreSQL only allows superusers to create LEAKPROOF functions because the
					// optimizer may execute leakproof predicates ahead of security barriers.
					ID: "routine-privilege-repro-test-testprocedureexecutegrantdoesnotapplytootheroverloadsrepro-0001-call-overload_secret_proc-hidden-::text"},
				},
			},
		},
	})
}

func TestCreateFunctionLeakproofRequiresSuperuserRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "LEAKPROOF function creation requires superuser",
			SetUpScript: []string{
				`CREATE USER leakproof_function_user PASSWORD 'pw';`,
				`GRANT USAGE, CREATE ON SCHEMA public TO leakproof_function_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE FUNCTION leakproof_user_func(input INT)
						RETURNS BOOL
						LANGUAGE SQL
						LEAKPROOF
						AS $$ SELECT input > 0 $$;`,

					Username: `leakproof_function_user`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{ID: "routine-privilege-repro-test-testcreatefunctionleakproofrequiressuperuserrepro-0001-create-function-leakproof_user_func-input-int", Compare: "sqlstate"},
				},
			},
		},
	})
}
