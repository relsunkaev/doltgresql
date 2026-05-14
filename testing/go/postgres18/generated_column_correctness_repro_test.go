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

package postgres18

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"
)

// TestPostgres18VirtualGeneratedColumnRepro reproduces a PostgreSQL 18
// persistence gap: virtual generated columns are computed on read rather than
// stored in the row.
func TestPostgres18VirtualGeneratedColumnRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "virtual generated columns compute on read",
			SetUpScript: []string{
				`CREATE TABLE generated_virtual_items (
					id INT PRIMARY KEY,
					width INT NOT NULL,
					height INT NOT NULL,
					area INT GENERATED ALWAYS AS (width * height) VIRTUAL
				);`,
				`INSERT INTO generated_virtual_items (id, width, height) VALUES (1, 3, 4);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT area FROM generated_virtual_items WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/generated-column-correctness-repro-test-testpostgres18virtualgeneratedcolumnrepro-0001-select-area-from-generated_virtual_items-where"},
				},
				{
					Query: `UPDATE generated_virtual_items
						SET width = 5
						WHERE id = 1;`,
				},
				{
					Query: `SELECT area FROM generated_virtual_items WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/generated-column-correctness-repro-test-testpostgres18virtualgeneratedcolumnrepro-0002-select-area-from-generated_virtual_items-where"},
				},
			},
		},
	})
}
