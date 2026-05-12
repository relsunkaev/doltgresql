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

// TestBitStringOperatorsAndFunctionsRepro reproduces a PostgreSQL bit-string
// compatibility gap: fixed and varying bit strings support storage, bitwise
// operators, concatenation, get_bit, and bit_count.
func TestBitStringOperatorsAndFunctionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "bit strings use operators and functions",
			SetUpScript: []string{
				`CREATE TABLE bit_string_items (
					id INT PRIMARY KEY,
					flags BIT(4),
					flex BIT VARYING(8)
				);`,
				`INSERT INTO bit_string_items VALUES (1, B'1010', B'101011');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT flags::text,
							flex::text,
							(flags & B'1100')::text,
							(flex || B'00')::text,
							get_bit(flags, 1),
							bit_count(flex)
						FROM bit_string_items;`,
					Expected: []sql.Row{{"1010", "101011", "1000", "10101100", 0, int64(4)}},
				},
			},
		},
	})
}
