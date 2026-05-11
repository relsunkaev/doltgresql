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

// TestAlterProcedureRenameRepro reproduces a routine DDL correctness bug:
// PostgreSQL supports ALTER PROCEDURE ... RENAME TO and resolves the procedure
// under its new name afterward.
func TestAlterProcedureRenameRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PROCEDURE RENAME TO updates procedure lookup",
			SetUpScript: []string{
				`CREATE TABLE rename_procedure_audit (
					value INT
				);`,
				`CREATE PROCEDURE rename_procedure_old()
					LANGUAGE SQL
					AS $$ INSERT INTO rename_procedure_audit VALUES (7) $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PROCEDURE rename_procedure_old()
						RENAME TO rename_procedure_new;`,
				},
				{
					Query: `CALL rename_procedure_new();`,
				},
				{
					Query:    `SELECT value FROM rename_procedure_audit;`,
					Expected: []sql.Row{{7}},
				},
				{
					Query:       `CALL rename_procedure_old();`,
					ExpectedErr: `does not exist`,
				},
			},
		},
	})
}

// PostgreSQL CALL returns result rows for OUT and INOUT procedure arguments.
// Doltgres executes the procedure but returns an empty result set.
func TestProcedureOutArgumentsReturnRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CALL returns INOUT procedure result",
			SetUpScript: []string{
				`CREATE PROCEDURE proc_inout_value(INOUT value INT)
					LANGUAGE SQL
					AS $$ SELECT value + 7 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `CALL proc_inout_value(5);`,
					Expected: []sql.Row{{12}},
				},
			},
		},
		{
			Name: "CALL returns OUT procedure result",
			SetUpScript: []string{
				`CREATE PROCEDURE proc_out_value(IN input INT, OUT doubled INT)
					LANGUAGE SQL
					AS $$ SELECT input * 2 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `CALL proc_out_value(7, NULL);`,
					Expected: []sql.Row{{14}},
				},
			},
		},
	})
}

// TestProcedureSetSearchPathOptionAppliesDuringExecutionRepro reproduces a
// procedure execution correctness bug: a procedure-level SET search_path option
// should apply while the procedure body runs, regardless of the caller's
// search_path.
func TestProcedureSetSearchPathOptionAppliesDuringExecutionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "procedure SET search_path controls unqualified writes",
			SetUpScript: []string{
				`CREATE SCHEMA dg_proc_set_safe;`,
				`CREATE SCHEMA dg_proc_set_attacker;`,
				`CREATE TABLE dg_proc_set_safe.audit_items (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE dg_proc_set_attacker.audit_items (
					id INT PRIMARY KEY
				);`,
				`SET search_path = dg_proc_set_attacker, public;`,
				`CREATE PROCEDURE procedure_set_path_insert()
				LANGUAGE SQL
				SET search_path = dg_proc_set_safe, public
				AS $$ INSERT INTO audit_items VALUES (1) $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SET search_path = dg_proc_set_attacker, public;`,
				},
				{
					Query: `CALL procedure_set_path_insert();`,
				},
				{
					Query: `SELECT
						(SELECT count(*) FROM dg_proc_set_safe.audit_items),
						(SELECT count(*) FROM dg_proc_set_attacker.audit_items);`,
					Expected: []sql.Row{{int64(1), int64(0)}},
				},
			},
		},
	})
}
