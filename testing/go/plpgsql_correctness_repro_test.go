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

// TestPlpgsqlCaseWithoutElseRaisesCaseNotFoundRepro reproduces a PL/pgSQL
// correctness bug: CASE statements without ELSE must raise case_not_found when
// no WHEN branch matches.
func TestPlpgsqlCaseWithoutElseRaisesCaseNotFoundRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL CASE without ELSE raises case_not_found",
			SetUpScript: []string{
				`CREATE FUNCTION plpgsql_case_without_else(input_value INT4)
				RETURNS TEXT AS $$
				DECLARE
					msg TEXT;
				BEGIN
					CASE input_value
						WHEN 1 THEN
							msg := 'one';
					END CASE;
					RETURN msg;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT plpgsql_case_without_else(1);`,
					Expected: []sql.Row{{"one"}},
				},
				{
					Query:       `SELECT plpgsql_case_without_else(2);`,
					ExpectedErr: `case not found`,
				},
			},
		},
	})
}

// TestPlpgsqlRaiseRejectsDuplicateMessageOptionRepro reproduces a PL/pgSQL
// correctness bug: a RAISE statement cannot specify the MESSAGE option both via
// the format string and the USING clause.
func TestPlpgsqlRaiseRejectsDuplicateMessageOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL RAISE rejects duplicate MESSAGE option",
			SetUpScript: []string{
				`CREATE FUNCTION plpgsql_raise_duplicate_message()
				RETURNS VOID AS $$
				BEGIN
					RAISE DEBUG 'DebugTest1' USING MESSAGE = 'DebugMessage';
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `SELECT plpgsql_raise_duplicate_message();`,
					ExpectedErr: `RAISE option already specified: MESSAGE`,
				},
			},
		},
	})
}

// TestPlpgsqlRaiseRejectsDuplicateDetailOptionRepro reproduces a PL/pgSQL
// correctness bug: a RAISE statement cannot specify the same USING option more
// than once.
func TestPlpgsqlRaiseRejectsDuplicateDetailOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL RAISE rejects duplicate DETAIL option",
			SetUpScript: []string{
				`CREATE FUNCTION plpgsql_raise_duplicate_detail()
				RETURNS VOID AS $$
				BEGIN
					RAISE EXCEPTION USING MESSAGE = 'raise message', DETAIL = 'first detail', DETAIL = 'second detail';
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `SELECT plpgsql_raise_duplicate_detail();`,
					ExpectedErr: `RAISE option already specified: DETAIL`,
				},
			},
		},
	})
}

// TestPlpgsqlAliasVariablesResolveRepro reproduces a PL/pgSQL correctness bug:
// ALIAS variables should be assignable names for local variables and function
// arguments.
func TestPlpgsqlAliasVariablesResolveRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL ALIAS variables resolve",
			SetUpScript: []string{
				`CREATE FUNCTION plpgsql_alias_echo(input_value TEXT)
				RETURNS TEXT AS $$
				DECLARE
					base_value TEXT;
					base_alias ALIAS FOR base_value;
					nested_alias ALIAS FOR base_alias;
					input_alias ALIAS FOR input_value;
				BEGIN
					nested_alias := input_alias;
					RETURN base_value;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT plpgsql_alias_echo('aliased value');`,
					Expected: []sql.Row{{"aliased value"}},
				},
			},
		},
	})
}

// TestPlpgsqlReturnsTableCompositeVariableRepro reproduces a PL/pgSQL
// correctness bug: PostgreSQL lets a function declare a variable using a
// table row type, SELECT a row into it, and return that composite value.
func TestPlpgsqlReturnsTableCompositeVariableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PL/pgSQL returns table-typed composite variable",
			SetUpScript: []string{
				`CREATE TABLE plpgsql_composite_return_items (
					id INT PRIMARY KEY,
					name TEXT NOT NULL,
					qty INT NOT NULL,
					price REAL NOT NULL
				);`,
				`INSERT INTO plpgsql_composite_return_items VALUES
					(1, 'apple', 3, 2.5),
					(2, 'banana', 5, 1.2);`,
				`CREATE FUNCTION plpgsql_composite_single_return()
				RETURNS plpgsql_composite_return_items AS $$
				DECLARE
					result plpgsql_composite_return_items;
				BEGIN
					SELECT * INTO result
					FROM plpgsql_composite_return_items
					WHERE id = 1;
					RETURN result;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT plpgsql_composite_single_return()::text;`,
					Expected: []sql.Row{{"(1,apple,3,2.5)"}},
				},
			},
		},
	})
}
