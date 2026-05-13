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
					Query: `SELECT (schemaname = current_schema())::text, matviewname, hasindexes::text, ispopulated::text,
							trim(trailing ';' from regexp_replace(trim(definition), '\s+', ' ', 'g')) AS definition
						FROM pg_matviews
						WHERE schemaname = current_schema() AND matviewname = 'source_mv';`,
					Expected: []sql.Row{
						{"true", "source_mv", "false", "true", "SELECT id, v FROM source"},
					},
				},
				{
					Query:    `SELECT count(*)::text FROM pg_tables WHERE schemaname = current_schema() AND tablename = 'source_mv';`,
					Expected: []sql.Row{{"0"}},
				},
				{
					Query: `SELECT table_type
						FROM information_schema.tables
						WHERE table_schema = current_schema() AND table_name = 'source_mv';`,
					Expected: []sql.Row{},
				},
				{
					Query: `CREATE UNIQUE INDEX source_mv_id_idx ON source_mv (id);`,
				},
				{
					Query: `CREATE INDEX source_mv_v_idx ON source_mv (v);`,
				},
				{
					Query: `SELECT indexname, replace(indexdef, current_schema() || '.', '') AS indexdef
						FROM pg_indexes
						WHERE schemaname = current_schema() AND tablename = 'source_mv'
						ORDER BY indexname;`,
					Expected: []sql.Row{
						{"source_mv_id_idx", "CREATE UNIQUE INDEX source_mv_id_idx ON source_mv USING btree (id)"},
						{"source_mv_v_idx", "CREATE INDEX source_mv_v_idx ON source_mv USING btree (v)"},
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
						WHERE schemaname = current_schema() AND matviewname = 'source_mv';`,
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
		{
			Name: "ALTER MATERIALIZED VIEW RENAME TO preserves metadata",
			SetUpScript: []string{
				`CREATE TABLE source (id INT PRIMARY KEY, v INT);`,
				`INSERT INTO source VALUES (1, 100), (2, 200);`,
				`CREATE MATERIALIZED VIEW source_mv AS SELECT id, v FROM source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER MATERIALIZED VIEW source_mv RENAME TO renamed_mv;`,
				},
				{
					Query: `SELECT id, v FROM renamed_mv ORDER BY id;`,
					Expected: []sql.Row{
						{1, 100},
						{2, 200},
					},
				},
				{
					Query:       `SELECT count(*)::text FROM source_mv;`,
					ExpectedErr: `table not found: source_mv`,
				},
				{
					Query: `SELECT relkind FROM pg_class WHERE relname = 'renamed_mv';`,
					Expected: []sql.Row{
						{"m"},
					},
				},
				{
					Query: `SELECT (schemaname = current_schema())::text, matviewname, ispopulated::text,
								trim(trailing ';' from regexp_replace(trim(definition), '\s+', ' ', 'g')) AS definition
							FROM pg_matviews
							WHERE schemaname = current_schema() AND matviewname = 'renamed_mv';`,
					Expected: []sql.Row{
						{"true", "renamed_mv", "true", "SELECT id, v FROM source"},
					},
				},
				{
					Query:    `SELECT count(*)::text FROM pg_matviews WHERE matviewname = 'source_mv';`,
					Expected: []sql.Row{{"0"}},
				},
			},
		},
		{
			Name: "ALTER MATERIALIZED VIEW RENAME COLUMN preserves metadata and refresh",
			SetUpScript: []string{
				`CREATE TABLE source (id INT PRIMARY KEY, v INT);`,
				`INSERT INTO source VALUES (1, 100), (2, 200);`,
				`CREATE TABLE ordinary_table (id INT PRIMARY KEY, v INT);`,
				`CREATE MATERIALIZED VIEW source_mv AS SELECT id, v FROM source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER MATERIALIZED VIEW source_mv RENAME COLUMN v TO amount;`,
				},
				{
					Query: `SELECT id, amount FROM source_mv ORDER BY id;`,
					Expected: []sql.Row{
						{1, 100},
						{2, 200},
					},
				},
				{
					Query:       `SELECT v FROM source_mv;`,
					ExpectedErr: `column "v" could not be found`,
				},
				{
					Query: `SELECT relkind FROM pg_class WHERE relname = 'source_mv';`,
					Expected: []sql.Row{
						{"m"},
					},
				},
				{
					Query: `SELECT matviewname, ispopulated::text, definition
						FROM pg_matviews
						WHERE schemaname = 'public' AND matviewname = 'source_mv';`,
					Expected: []sql.Row{
						{"source_mv", "true", "SELECT id, v FROM source"},
					},
				},
				{
					Query: `UPDATE source SET v = v + 1;`,
				},
				{
					Query: `REFRESH MATERIALIZED VIEW source_mv;`,
				},
				{
					Query: `SELECT id, amount FROM source_mv ORDER BY id;`,
					Expected: []sql.Row{
						{1, 101},
						{2, 201},
					},
				},
				{
					Query:       `ALTER MATERIALIZED VIEW ordinary_table RENAME COLUMN v TO amount;`,
					ExpectedErr: `relation "ordinary_table" is not a materialized view`,
				},
				{
					Query: `ALTER MATERIALIZED VIEW IF EXISTS missing_mv RENAME COLUMN v TO amount;`,
				},
			},
		},
		{
			Name: "CREATE MATERIALIZED VIEW column aliases",
			SetUpScript: []string{
				`CREATE TABLE source (id INT PRIMARY KEY, v INT);`,
				`INSERT INTO source VALUES (1, 100), (2, 200);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE MATERIALIZED VIEW renamed_mv (account_id, amount) AS SELECT id, v FROM source;`,
				},
				{
					Query: `SELECT account_id, amount FROM renamed_mv ORDER BY account_id;`,
					Expected: []sql.Row{
						{1, 100},
						{2, 200},
					},
				},
				{
					Query: `SELECT attname
						FROM pg_attribute
						WHERE attrelid = 'renamed_mv'::regclass AND attnum > 0 AND NOT attisdropped
						ORDER BY attnum;`,
					Expected: []sql.Row{
						{"account_id"},
						{"amount"},
					},
				},
				{
					Query: `CREATE MATERIALIZED VIEW partial_alias_mv (account_id) AS SELECT id, v FROM source;`,
				},
				{
					Query: `SELECT account_id, v FROM partial_alias_mv ORDER BY account_id;`,
					Expected: []sql.Row{
						{1, 100},
						{2, 200},
					},
				},
				{
					Query: `CREATE MATERIALIZED VIEW quoted_alias_mv ("AccountID", amount) AS SELECT id, v FROM source;`,
				},
				{
					Query: `SELECT "AccountID", amount FROM quoted_alias_mv ORDER BY "AccountID";`,
					Expected: []sql.Row{
						{1, 100},
						{2, 200},
					},
				},
				{
					Query: `CREATE MATERIALIZED VIEW swapped_alias_mv (v, id) AS SELECT id, v FROM source;`,
				},
				{
					Query: `SELECT v, id FROM swapped_alias_mv ORDER BY v;`,
					Expected: []sql.Row{
						{1, 100},
						{2, 200},
					},
				},
				{
					Query:       `CREATE MATERIALIZED VIEW too_many_mv (account_id, amount, extra) AS SELECT id, v FROM source;`,
					ExpectedErr: "too many column names were specified",
				},
				{
					Query:       `CREATE MATERIALIZED VIEW duplicate_alias_mv (account_id, account_id) AS SELECT id, v FROM source;`,
					ExpectedErr: `column "account_id" specified more than once`,
				},
				{
					Query:       `CREATE MATERIALIZED VIEW duplicate_remaining_mv (v) AS SELECT id, v FROM source;`,
					ExpectedErr: `column "v" specified more than once`,
				},
			},
		},
		{
			Name: "CREATE MATERIALIZED VIEW WITH NO DATA stays unpopulated until refresh",
			SetUpScript: []string{
				`CREATE TABLE source (id INT PRIMARY KEY, v INT);`,
				`INSERT INTO source VALUES (1, 100), (2, 200);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE MATERIALIZED VIEW source_mv (account_id, amount) AS SELECT id, v FROM source WITH NO DATA;`,
				},
				{
					Query: `SELECT relkind FROM pg_class WHERE relname = 'source_mv';`,
					Expected: []sql.Row{
						{"m"},
					},
				},
				{
					Query: `SELECT hasindexes::text, ispopulated::text, definition
						FROM pg_matviews
						WHERE schemaname = 'public' AND matviewname = 'source_mv';`,
					Expected: []sql.Row{
						{"false", "false", "SELECT id, v FROM source"},
					},
				},
				{
					Query:       `SELECT account_id, amount FROM source_mv ORDER BY account_id;`,
					ExpectedErr: `materialized view "source_mv" has not been populated`,
				},
				{
					Query: `SELECT attname
						FROM pg_attribute
						WHERE attrelid = 'source_mv'::regclass AND attnum > 0 AND NOT attisdropped
						ORDER BY attnum;`,
					Expected: []sql.Row{
						{"account_id"},
						{"amount"},
					},
				},
				{
					Query: `CREATE UNIQUE INDEX source_mv_id_idx ON source_mv (account_id);`,
				},
				{
					Query: `SELECT hasindexes::text, ispopulated::text
						FROM pg_matviews
						WHERE schemaname = 'public' AND matviewname = 'source_mv';`,
					Expected: []sql.Row{
						{"true", "false"},
					},
				},
				{
					Query: `REFRESH MATERIALIZED VIEW source_mv WITH DATA;`,
				},
				{
					Query: `SELECT account_id, amount FROM source_mv ORDER BY account_id;`,
					Expected: []sql.Row{
						{1, 100},
						{2, 200},
					},
				},
				{
					Query: `SELECT ispopulated::text
						FROM pg_matviews
						WHERE schemaname = 'public' AND matviewname = 'source_mv';`,
					Expected: []sql.Row{
						{"true"},
					},
				},
				{
					Query: `REFRESH MATERIALIZED VIEW source_mv WITH NO DATA;`,
				},
				{
					Query: `SELECT hasindexes::text, ispopulated::text
						FROM pg_matviews
						WHERE schemaname = 'public' AND matviewname = 'source_mv';`,
					Expected: []sql.Row{
						{"true", "false"},
					},
				},
				{
					Query:       `SELECT account_id, amount FROM source_mv ORDER BY account_id;`,
					ExpectedErr: `materialized view "source_mv" has not been populated`,
				},
				{
					Query: `REFRESH MATERIALIZED VIEW source_mv;`,
				},
				{
					Query: `SELECT account_id, amount FROM source_mv ORDER BY account_id;`,
					Expected: []sql.Row{
						{1, 100},
						{2, 200},
					},
				},
				{
					Query: `CREATE MATERIALIZED VIEW paren_mv AS (SELECT id, v FROM source) WITH NO DATA;`,
				},
				{
					Query: `SELECT ispopulated::text
						FROM pg_matviews
						WHERE schemaname = 'public' AND matviewname = 'paren_mv';`,
					Expected: []sql.Row{
						{"false"},
					},
				},
				{
					Query:       `SELECT id, v FROM paren_mv ORDER BY id;`,
					ExpectedErr: `materialized view "paren_mv" has not been populated`,
				},
			},
		},
		{
			Name: "REFRESH MATERIALIZED VIEW refreshes snapshot data",
			SetUpScript: []string{
				`CREATE TABLE source (id INT PRIMARY KEY, v INT);`,
				`INSERT INTO source VALUES (1, 100), (2, 200);`,
				`CREATE MATERIALIZED VIEW source_mv (account_id, amount) AS SELECT id, v FROM source;`,
				`CREATE UNIQUE INDEX source_mv_id_idx ON source_mv (account_id);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO source VALUES (3, 300);`,
				},
				{
					Query: `UPDATE source SET v = 250 WHERE id = 2;`,
				},
				{
					Query: `DELETE FROM source WHERE id = 1;`,
				},
				{
					Query: `SELECT account_id, amount FROM source_mv ORDER BY account_id;`,
					Expected: []sql.Row{
						{1, 100},
						{2, 200},
					},
				},
				{
					Query: `REFRESH MATERIALIZED VIEW source_mv;`,
				},
				{
					Query: `SELECT account_id, amount FROM source_mv ORDER BY account_id;`,
					Expected: []sql.Row{
						{2, 250},
						{3, 300},
					},
				},
				{
					Query: `SELECT hasindexes::text, ispopulated::text
						FROM pg_matviews
						WHERE schemaname = 'public' AND matviewname = 'source_mv';`,
					Expected: []sql.Row{
						{"true", "true"},
					},
				},
				{
					Query: `UPDATE source SET v = 350 WHERE id = 3;`,
				},
				{
					Query: `REFRESH MATERIALIZED VIEW public.source_mv WITH DATA;`,
				},
				{
					Query: `SELECT account_id, amount FROM source_mv ORDER BY account_id;`,
					Expected: []sql.Row{
						{2, 250},
						{3, 350},
					},
				},
				{
					Query: `UPDATE source SET v = 450 WHERE id = 3;`,
				},
				{
					Query: `REFRESH MATERIALIZED VIEW CONCURRENTLY source_mv;`,
				},
				{
					Query: `SELECT account_id, amount FROM source_mv ORDER BY account_id;`,
					Expected: []sql.Row{
						{2, 250},
						{3, 450},
					},
				},
				{
					Query:       `REFRESH MATERIALIZED VIEW CONCURRENTLY source_mv WITH NO DATA;`,
					ExpectedErr: "REFRESH options CONCURRENTLY and WITH NO DATA cannot be used together",
				},
				{
					Query: `REFRESH MATERIALIZED VIEW source_mv WITH NO DATA;`,
				},
				{
					Query: `SELECT ispopulated::text
						FROM pg_matviews
						WHERE schemaname = 'public' AND matviewname = 'source_mv';`,
					Expected: []sql.Row{
						{"false"},
					},
				},
				{
					Query:       `SELECT account_id, amount FROM source_mv ORDER BY account_id;`,
					ExpectedErr: `materialized view "source_mv" has not been populated`,
				},
				{
					Query:       `REFRESH MATERIALIZED VIEW CONCURRENTLY source_mv;`,
					ExpectedErr: "CONCURRENTLY cannot be used when the materialized view is not populated",
				},
				{
					Query: `REFRESH MATERIALIZED VIEW source_mv;`,
				},
				{
					Query: `SELECT account_id, amount FROM source_mv ORDER BY account_id;`,
					Expected: []sql.Row{
						{2, 250},
						{3, 450},
					},
				},
				{
					Query: `CREATE TABLE plain_table (id INT);`,
				},
				{
					Query:       `REFRESH MATERIALIZED VIEW plain_table;`,
					ExpectedErr: `relation "plain_table" is not a materialized view`,
				},
				{
					Query: `CREATE TABLE dup_source (id INT PRIMARY KEY, grp INT);`,
				},
				{
					Query: `INSERT INTO dup_source VALUES (1, 1), (2, 2);`,
				},
				{
					Query: `CREATE MATERIALIZED VIEW dup_mv AS SELECT grp FROM dup_source;`,
				},
				{
					Query: `CREATE UNIQUE INDEX dup_mv_grp_idx ON dup_mv (grp);`,
				},
				{
					Query: `UPDATE dup_source SET grp = 1 WHERE id = 2;`,
				},
				{
					Query:       `REFRESH MATERIALIZED VIEW dup_mv;`,
					ExpectedErr: "duplicate",
				},
				{
					Query: `SELECT grp FROM dup_mv ORDER BY grp;`,
					Expected: []sql.Row{
						{1},
						{2},
					},
				},
				{
					Query: `CREATE MATERIALIZED VIEW no_unique_mv AS SELECT id, v FROM source;`,
				},
				{
					Query:       `REFRESH MATERIALIZED VIEW CONCURRENTLY no_unique_mv;`,
					ExpectedErr: `cannot refresh materialized view "public.no_unique_mv" concurrently`,
				},
				{
					Query: `CREATE MATERIALIZED VIEW partial_unique_mv AS SELECT id, v FROM source;`,
				},
				{
					Query: `CREATE UNIQUE INDEX partial_unique_mv_idx ON partial_unique_mv (id) WHERE v IS NOT NULL;`,
				},
				{
					Query:       `REFRESH MATERIALIZED VIEW CONCURRENTLY partial_unique_mv;`,
					ExpectedErr: `cannot refresh materialized view "public.partial_unique_mv" concurrently`,
				},
			},
		},
	})
}
