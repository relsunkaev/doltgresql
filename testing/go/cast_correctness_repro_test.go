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

// TestCreateCastFunctionIsUsedByExplicitCastRepro reproduces a type correctness
// gap: PostgreSQL lets user-defined casts route explicit casts through a SQL
// function.
func TestCreateCastFunctionIsUsedByExplicitCastRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE CAST function is used by explicit casts",
			SetUpScript: []string{
				`CREATE TYPE cast_color AS ENUM ('red', 'green');`,
				`CREATE FUNCTION cast_color_to_int(input_color cast_color)
					RETURNS INT
					LANGUAGE SQL
					IMMUTABLE
					AS $$ SELECT CASE WHEN input_color = 'red'::cast_color THEN 1 ELSE 2 END $$;`,
				`CREATE CAST (cast_color AS INT)
					WITH FUNCTION cast_color_to_int(cast_color);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ('green'::cast_color)::INT;`, PostgresOracle: ScriptTestPostgresOracle{ID: "cast-correctness-repro-test-testcreatecastfunctionisusedbyexplicitcastrepro-0001-select-green-::cast_color-::int"},
				},
			},
		},
	})
}

// TestDropCastIfExistsMissingTypeRepro reproduces a compatibility gap:
// PostgreSQL accepts DROP CAST IF EXISTS even when the cast's type name is
// missing, treating it as a no-op.
func TestDropCastIfExistsMissingTypeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP CAST IF EXISTS missing type succeeds",
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP CAST IF EXISTS (INTEGER AS missing_cast_type_repro);`,
				},
			},
		},
	})
}
