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
)

// TestPgAttributeIndexAttributeNames asserts that pg_attribute rows
// for index objects use the real underlying column names, not
// synthetic placeholders. Drizzle Kit's index introspection joins
// pg_index back to pg_attribute on the index's columns; if the
// names don't match the table's columns, the migration diff thinks
// the index changed and emits a spurious DROP INDEX / CREATE INDEX
// pair.
func TestPgAttributeIndexAttributeNames(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_attribute reports real column names for index entries",
			SetUpScript: []string{
				`CREATE TABLE t_idxnames (id INT PRIMARY KEY, code TEXT, hits INT);`,
				`CREATE INDEX t_idxnames_code_idx ON t_idxnames (code);`,
				`CREATE INDEX t_idxnames_multi_idx ON t_idxnames (hits, code);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// Single-column index: attname should be 'code'.
					Query: `SELECT a.attname
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
WHERE c.relname = 't_idxnames_code_idx'
ORDER BY a.attnum;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-attribute-index-names-test-testpgattributeindexattributenames-0001-select-a.attname-from-pg_catalog.pg_attribute-a"},
				},
				{
					// Multi-column index: attnames should be 'hits', 'code'
					// (in declaration order), not 'column_1', 'column_2'.
					Query: `SELECT a.attname
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
WHERE c.relname = 't_idxnames_multi_idx'
ORDER BY a.attnum;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-attribute-index-names-test-testpgattributeindexattributenames-0002-select-a.attname-from-pg_catalog.pg_attribute-a"},
				},
				{
					// PK index attname is 'id'.
					Query: `SELECT a.attname
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
WHERE c.relname = 't_idxnames_pkey'
ORDER BY a.attnum;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-attribute-index-names-test-testpgattributeindexattributenames-0003-select-a.attname-from-pg_catalog.pg_attribute-a"},
				},
			},
		},
	})
}
