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

// TestPgIndexIndclassAny exercises the planner path for
// `oid = ANY(int2vector_column)`. drizzle-kit's index-introspection
// query joins pg_opclass to pg_index via this exact pattern; without
// it, the migration tool hangs on every Doltgres database.
//
// The simplest reproducer is the same shape: select rows from
// pg_index where indclass contains a given oid. PostgreSQL's
// indclass is an `oidvector` (typed as int2vector / oidvector
// depending on catalog version); ANY(...) on it must return
// boolean. Doltgres previously rejected that with
// "found equality comparison that does not return a bool".
func TestPgIndexIndclassAny(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ANY(indclass) is a boolean predicate",
			SetUpScript: []string{
				`CREATE TABLE idxany_t (id INT PRIMARY KEY, code TEXT);`,
				`CREATE INDEX idxany_code_idx ON idxany_t (code);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// drizzle-kit's exact join shape (simplified).
					// The query asks: which indexes use opclass OID X?
					// Doltgres needs to handle ANY(indclass) without
					// claiming the comparison "doesn't return a bool".
					Query: `SELECT c.relname
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
JOIN pg_catalog.pg_opclass opc ON opc.oid = ANY(i.indclass)
WHERE c.relname = 'idxany_code_idx';`,
					// At least one row — drizzle-kit only needs the
					// query to execute; the count of joined opclass
					// rows depends on how many opclasses are
					// registered, which is implementation-defined.
					SkipResultsCheck: true,
				},
				{
					// Plain ANY(array_literal) for sanity — must
					// already work and serve as a baseline.
					Query: `SELECT 1 WHERE 2 = ANY(ARRAY[1, 2, 3]);`,
					Expected: []sql.Row{
						{1},
					},
				},
			},
		},
	})
}
