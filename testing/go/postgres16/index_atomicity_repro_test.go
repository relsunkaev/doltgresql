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

package postgres16

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"

	"github.com/dolthub/go-mysql-server/sql"
)

// TestCreateUniqueIndexOnDuplicateRowsRollsBackIndexRepro guards CREATE INDEX
// atomicity: a failed unique index build must not leave a durable index catalog
// entry behind.
func TestCreateUniqueIndexOnDuplicateRowsRollsBackIndexRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE UNIQUE INDEX on duplicates rolls back index",
			SetUpScript: []string{
				`CREATE TABLE index_duplicate_items (
					id INT PRIMARY KEY,
					code INT
				);`,
				`INSERT INTO index_duplicate_items VALUES (1, 10), (2, 10);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE UNIQUE INDEX index_duplicate_items_code_idx
						ON index_duplicate_items (code);`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-atomicity-repro-test-testcreateuniqueindexonduplicaterowsrollsbackindexrepro-0001-create-unique-index-index_duplicate_items_code_idx-on", Compare: "sqlstate"},
				},
				{
					Query: `SELECT to_regclass('index_duplicate_items_code_idx') IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "index-atomicity-repro-test-testcreateuniqueindexonduplicaterowsrollsbackindexrepro-0002-select-to_regclass-index_duplicate_items_code_idx-is-null"},
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_indexes
						WHERE schemaname = 'public'
							AND indexname = 'index_duplicate_items_code_idx';`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}
