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

// PostgreSQL allows CREATE OPERATOR to bind a SQL function as an infix
// operator. Doltgres currently rejects the valid definition during parsing.
func TestCreateOperatorInstallsCallableOperatorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE OPERATOR installs callable operator",
			SetUpScript: []string{
				`CREATE FUNCTION same_parity_operator(left_value INT, right_value INT)
					RETURNS BOOL
					LANGUAGE SQL
					IMMUTABLE
					AS $$ SELECT (left_value % 2) = (right_value % 2) $$;`,
				`CREATE OPERATOR === (
					LEFTARG = INT,
					RIGHTARG = INT,
					PROCEDURE = same_parity_operator
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT 2 === 4, 2 === 3;`,
					Expected: []sql.Row{{"t", "f"}},
				},
			},
		},
	})
}
