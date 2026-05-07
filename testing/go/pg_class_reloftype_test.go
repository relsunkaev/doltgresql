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
	"github.com/dolthub/go-mysql-server/sql"
	"testing"
)

// TestPgClassReloftypeForOrdinaryTables verifies that pg_class.reloftype
// reports 0 (the PG-correct value) for ordinary, untyped tables. Real
// PostgreSQL only sets reloftype to a nonzero composite-type OID when
// a table is created with `CREATE TABLE name OF composite_type` — a
// PG-specific feature Doltgres does not implement. The previous audit
// flagged a constant id.Null as suspect; this test pins the current
// behavior so a future implementation that adds typed-table support
// breaks loudly here instead of regressing GUI introspection.
func TestPgClassReloftypeForOrdinaryTables(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ordinary tables report reloftype=0 (PG-correct)",
			SetUpScript: []string{
				`CREATE TABLE plain_t (id INT PRIMARY KEY, v INT);`,
				`CREATE TABLE with_unique (id INT PRIMARY KEY, code TEXT UNIQUE);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT relname, reloftype
FROM pg_catalog.pg_class
WHERE relname IN ('plain_t', 'with_unique')
ORDER BY relname;`,
					Expected: []sql.Row{
						{"plain_t", uint32(0)},
						{"with_unique", uint32(0)},
					},
				},
			},
		},
	})
}
