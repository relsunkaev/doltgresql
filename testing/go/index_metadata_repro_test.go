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

// TestPgGetIndexdefQuotesIdentifiersRepro reproduces a catalog correctness
// bug: pg_get_indexdef() should quote index, table, and column identifiers that
// require quoting.
func TestPgGetIndexdefQuotesIdentifiersRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_get_indexdef quotes identifiers",
			SetUpScript: []string{
				`CREATE TABLE "IndexQuoteItems" (
					"CaseColumn" INT,
					label TEXT
				);`,
				`CREATE INDEX "IndexQuoteIdx"
					ON "IndexQuoteItems" ("CaseColumn");`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
							pg_catalog.pg_get_indexdef(c.oid),
							pg_catalog.pg_get_indexdef(c.oid, 1, false)
						FROM pg_catalog.pg_class c
						WHERE c.relname = 'IndexQuoteIdx';`,
					Expected: []sql.Row{{
						`CREATE INDEX "IndexQuoteIdx" ON public."IndexQuoteItems" USING btree ("CaseColumn")`,
						`"CaseColumn"`,
					}},
				},
			},
		},
	})
}

// TestRenameTableUpdatesIndexDefinitionsRepro guards catalog metadata after a
// table rename: pg_indexes and pg_get_indexdef should reference the renamed
// table.
func TestRenameTableUpdatesIndexDefinitionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RENAME TABLE updates index definitions",
			SetUpScript: []string{
				`CREATE TABLE index_rename_table_old (id INT PRIMARY KEY, label TEXT);`,
				`CREATE INDEX index_rename_table_label_idx ON index_rename_table_old (label);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE index_rename_table_old RENAME TO index_rename_table_new;`,
				},
				{
					Query: `SELECT tablename, indexdef
						FROM pg_catalog.pg_indexes
						WHERE indexname = 'index_rename_table_label_idx';`,
					Expected: []sql.Row{{
						"index_rename_table_new",
						"CREATE INDEX index_rename_table_label_idx ON public.index_rename_table_new USING btree (label)",
					}},
				},
			},
		},
	})
}

// TestRenameColumnUpdatesIndexDefinitionsRepro guards catalog metadata after a
// column rename: pg_get_indexdef should reference the renamed column.
func TestRenameColumnUpdatesIndexDefinitionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RENAME COLUMN updates index definitions",
			SetUpScript: []string{
				`CREATE TABLE index_rename_column_items (id INT PRIMARY KEY, old_label TEXT);`,
				`CREATE INDEX index_rename_column_label_idx ON index_rename_column_items (old_label);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE index_rename_column_items RENAME COLUMN old_label TO new_label;`,
				},
				{
					Query: `SELECT
							pg_catalog.pg_get_indexdef(c.oid),
							pg_catalog.pg_get_indexdef(c.oid, 1, false)
						FROM pg_catalog.pg_class c
						WHERE c.relname = 'index_rename_column_label_idx';`,
					Expected: []sql.Row{{
						"CREATE INDEX index_rename_column_label_idx ON public.index_rename_column_items USING btree (new_label)",
						"new_label",
					}},
				},
			},
		},
	})
}
