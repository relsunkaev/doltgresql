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

// TestMaterializedViewProbe pins where CREATE MATERIALIZED VIEW
// stands in doltgresql today. Per the Schema/DDL TODO in
// docs/app-compatibility-checklist.md.
func TestMaterializedViewProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE MATERIALIZED VIEW snapshot probe",
			SetUpScript: []string{
				`CREATE TABLE source (id INT PRIMARY KEY, v INT);`,
				`INSERT INTO source VALUES (1, 100), (2, 200);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE MATERIALIZED VIEW source_mv AS SELECT id, v FROM source;`,
				},
				{
					Query: `SELECT id, v FROM source_mv ORDER BY id;`,
					Expected: []sql.Row{
						{1, 100},
						{2, 200},
					},
				},
				{
					Query: `SELECT relkind FROM pg_class WHERE relname = 'source_mv';`,
					Expected: []sql.Row{
						{"m"},
					},
				},
				{
					Query: `SELECT schemaname, matviewname, hasindexes::text, ispopulated::text, definition
						FROM pg_matviews
						WHERE schemaname = 'public' AND matviewname = 'source_mv';`,
					Expected: []sql.Row{
						{"public", "source_mv", "false", "true", "SELECT id, v FROM source"},
					},
				},
				{
					Query:    `SELECT count(*)::text FROM pg_tables WHERE schemaname = 'public' AND tablename = 'source_mv';`,
					Expected: []sql.Row{{"0"}},
				},
				{
					Query: `SELECT table_type
						FROM information_schema.tables
						WHERE table_schema = 'public' AND table_name = 'source_mv';`,
					Expected: []sql.Row{},
				},
				{
					Query: `CREATE UNIQUE INDEX source_mv_id_idx ON source_mv (id);`,
				},
				{
					Query: `CREATE INDEX source_mv_v_idx ON source_mv (v);`,
				},
				{
					Query: `SELECT indexname, indexdef
						FROM pg_indexes
						WHERE tablename = 'source_mv'
						ORDER BY indexname;`,
					Expected: []sql.Row{
						{"source_mv_id_idx", "CREATE UNIQUE INDEX source_mv_id_idx ON public.source_mv USING btree (id)"},
						{"source_mv_v_idx", "CREATE INDEX source_mv_v_idx ON public.source_mv USING btree (v)"},
					},
				},
				{
					Query: `SELECT relhasindex::text FROM pg_class WHERE relname = 'source_mv';`,
					Expected: []sql.Row{
						{"true"},
					},
				},
				{
					Query: `SELECT hasindexes::text
						FROM pg_matviews
						WHERE schemaname = 'public' AND matviewname = 'source_mv';`,
					Expected: []sql.Row{
						{"true"},
					},
				},
			},
		},
		{
			Name: "DROP MATERIALIZED VIEW snapshot probe",
			SetUpScript: []string{
				`CREATE TABLE source (id INT PRIMARY KEY, v INT);`,
				`INSERT INTO source VALUES (1, 100), (2, 200);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE MATERIALIZED VIEW source_mv AS SELECT id, v FROM source;`,
				},
				{
					Query: `SELECT id, v FROM source_mv ORDER BY id;`,
					Expected: []sql.Row{
						{1, 100},
						{2, 200},
					},
				},
				{
					Query: `INSERT INTO source VALUES (3, 300);`,
				},
				{
					Query: `SELECT id, v FROM source_mv ORDER BY id;`,
					Expected: []sql.Row{
						{1, 100},
						{2, 200},
					},
				},
				{
					Query: `DROP MATERIALIZED VIEW source_mv;`,
				},
				{
					Query:       `SELECT * FROM source_mv;`,
					ExpectedErr: "table not found",
				},
			},
		},
	})
}
