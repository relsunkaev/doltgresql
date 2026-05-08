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

// TestSessionReplicationRoleProbe pins how `SET
// session_replication_role` is handled today. pg_dump and many ORM
// data-import paths flip this to 'replica' to suppress trigger and FK
// firing during bulk load. Per the Schema/DDL TODO in
// docs/app-compatibility-checklist.md.
func TestSessionReplicationRoleProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "SET session_replication_role keyword acceptance",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SET session_replication_role = 'replica';`,
				},
				{
					Query:    `SHOW session_replication_role;`,
					Expected: []sql.Row{{"replica"}},
				},
				{
					Query: `SET session_replication_role = 'origin';`,
				},
				{
					Query:    `SHOW session_replication_role;`,
					Expected: []sql.Row{{"origin"}},
				},
			},
		},
		{
			// pg_dump bulk-load path uses session_replication_role =
			// 'replica' to suppress FK enforcement during data
			// import. Today the GUC value is settable and readable
			// but FK enforcement runs unconditionally — pin the
			// gap so it stays visible. PG-correct semantics would
			// allow the violating row when the role is 'replica'.
			Name: "session_replication_role = replica does NOT yet suppress FK enforcement",
			SetUpScript: []string{
				`CREATE TABLE p (id INT PRIMARY KEY);`,
				`CREATE TABLE c (id INT PRIMARY KEY, pid INT REFERENCES p(id));`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SET session_replication_role = 'replica';`,
				},
				{
					// PG would accept this in 'replica' mode.
					// Doltgres still rejects.
					Query:       `INSERT INTO c VALUES (1, 999);`,
					ExpectedErr: "Foreign key violation",
				},
			},
		},
	})
}
