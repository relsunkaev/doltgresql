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

// TestPostgres18CheckConstraintNotEnforcedRepro reproduces a PostgreSQL 18
// compatibility gap: NOT ENFORCED check constraints are metadata only and do
// not reject violating rows.
func TestPostgres18CheckConstraintNotEnforcedRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CHECK NOT ENFORCED allows violating rows",
			SetUpScript: []string{
				`CREATE TABLE check_not_enforced_items (
					id INT PRIMARY KEY,
					qty INT,
					CONSTRAINT qty_positive CHECK (qty > 0) NOT ENFORCED
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO check_not_enforced_items VALUES (1, -5);`,
				},
				{
					Query:    `SELECT qty FROM check_not_enforced_items WHERE id = 1;`,
					Expected: []sql.Row{{-5}},
				},
			},
		},
	})
}
