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

// TestInsertRejectsDuplicateTargetColumnsRepro reproduces an INSERT correctness
// bug: PostgreSQL rejects INSERT target column lists that mention the same
// column more than once and leaves the table unchanged.
func TestInsertRejectsDuplicateTargetColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "INSERT rejects duplicate target columns",
			SetUpScript: []string{
				`CREATE TABLE insert_duplicate_column_items (
					id INT PRIMARY KEY,
					a INT,
					b INT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO insert_duplicate_column_items (id, a, a)
						VALUES (1, 10, 20);`,
					ExpectedErr: `column "a" specified more than once`,
				},
				{
					Query:    `SELECT count(*) FROM insert_duplicate_column_items;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}
