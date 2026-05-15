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
					Query: `SELECT id, v FROM source_mv ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0001-select-id-v-from-source_mv"},
				},
				{
					Query: `SELECT relkind FROM pg_class WHERE relname = 'source_mv';`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0002-select-relkind-from-pg_class-where"},
				},
				{
					Query: `SELECT (schemaname = current_schema())::text, matviewname, hasindexes::text, ispopulated::text,
							trim(trailing ';' from regexp_replace(trim(definition), '\s+', ' ', 'g')) AS definition
						FROM pg_matviews
						WHERE schemaname = current_schema() AND matviewname = 'source_mv';`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0003-select-schemaname-=-current_schema-::text"},
				},
				{
					Query: `SELECT count(*)::text FROM pg_tables WHERE schemaname = current_schema() AND tablename = 'source_mv';`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0004-select-count-*-::text-from"},
				},
				{
					Query: `SELECT table_type
						FROM information_schema.tables
						WHERE table_schema = current_schema() AND table_name = 'source_mv';`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0005-select-table_type-from-information_schema.tables-where"},
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
						ORDER BY indexname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0006-select-indexname-replace-indexdef-current_schema"},
				},
				{
					Query: `SELECT relhasindex::text FROM pg_class WHERE relname = 'source_mv';`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0007-select-relhasindex::text-from-pg_class-where"},
				},
				{
					Query: `SELECT hasindexes::text
						FROM pg_matviews
						WHERE schemaname = current_schema() AND matviewname = 'source_mv';`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0008-select-hasindexes::text-from-pg_matviews-where"},
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
					Query: `SELECT id, v FROM source_mv ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0009-select-id-v-from-source_mv"},
				},
				{
					Query: `INSERT INTO source VALUES (3, 300);`,
				},
				{
					Query: `SELECT id, v FROM source_mv ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0010-select-id-v-from-source_mv"},
				},
				{
					Query: `DROP MATERIALIZED VIEW source_mv;`,
				},
				{
					Query: `SELECT * FROM source_mv;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0011-select-*-from-source_mv", Compare: "sqlstate"},
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
					Query: `SELECT id, v FROM renamed_mv ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0012-select-id-v-from-renamed_mv"},
				},
				{
					Query: `SELECT count(*)::text FROM source_mv;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0013-select-count-*-::text-from", Compare: "sqlstate"},
				},
				{
					Query: `SELECT relkind FROM pg_class WHERE relname = 'renamed_mv';`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0014-select-relkind-from-pg_class-where"},
				},
				{
					Query: `SELECT (schemaname = current_schema())::text, matviewname, ispopulated::text,
								trim(trailing ';' from regexp_replace(trim(definition), '\s+', ' ', 'g')) AS definition
							FROM pg_matviews
							WHERE schemaname = current_schema() AND matviewname = 'renamed_mv';`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0015-select-schemaname-=-current_schema-::text"},
				},
				{
					Query: `SELECT count(*)::text FROM pg_matviews WHERE matviewname = 'source_mv';`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0016-select-count-*-::text-from"},
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
					Query: `SELECT id, amount FROM source_mv ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0017-select-id-amount-from-source_mv"},
				},
				{
					Query: `SELECT v FROM source_mv;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0018-select-v-from-source_mv", Compare: "sqlstate"},
				},
				{
					Query: `SELECT relkind FROM pg_class WHERE relname = 'source_mv';`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0019-select-relkind-from-pg_class-where"},
				},
				{
					Query: `SELECT matviewname, ispopulated::text,
								trim(trailing ';' from regexp_replace(trim(definition), '\s+', ' ', 'g')) AS definition
							FROM pg_matviews
							WHERE schemaname = current_schema() AND matviewname = 'source_mv';`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0020-select-matviewname-ispopulated::text-trim-trailing"},
				},
				{
					Query: `UPDATE source SET v = v + 1;`,
				},
				{
					Query: `REFRESH MATERIALIZED VIEW source_mv;`,
				},
				{
					Query: `SELECT id, amount FROM source_mv ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0021-select-id-amount-from-source_mv"},
				},
				{
					Query: `ALTER MATERIALIZED VIEW ordinary_table RENAME COLUMN v TO amount;`,
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
					Query: `SELECT account_id, amount FROM renamed_mv ORDER BY account_id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0022-select-account_id-amount-from-renamed_mv"},
				},
				{
					Query: `SELECT attname
						FROM pg_attribute
						WHERE attrelid = 'renamed_mv'::regclass AND attnum > 0 AND NOT attisdropped
						ORDER BY attnum;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0023-select-attname-from-pg_attribute-where"},
				},
				{
					Query: `CREATE MATERIALIZED VIEW partial_alias_mv (account_id) AS SELECT id, v FROM source;`,
				},
				{
					Query: `SELECT account_id, v FROM partial_alias_mv ORDER BY account_id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0024-select-account_id-v-from-partial_alias_mv"},
				},
				{
					Query: `CREATE MATERIALIZED VIEW quoted_alias_mv ("AccountID", amount) AS SELECT id, v FROM source;`,
				},
				{
					Query: `SELECT "AccountID", amount FROM quoted_alias_mv ORDER BY "AccountID";`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0025-select-accountid-amount-from-quoted_alias_mv"},
				},
				{
					Query: `CREATE MATERIALIZED VIEW swapped_alias_mv (v, id) AS SELECT id, v FROM source;`,
				},
				{
					Query: `SELECT v, id FROM swapped_alias_mv ORDER BY v;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0026-select-v-id-from-swapped_alias_mv"},
				},
				{
					Query: `CREATE MATERIALIZED VIEW too_many_mv (account_id, amount, extra) AS SELECT id, v FROM source;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0027-create-materialized-view-too_many_mv-account_id", Compare: "sqlstate"},
				},
				{
					Query: `CREATE MATERIALIZED VIEW duplicate_alias_mv (account_id, account_id) AS SELECT id, v FROM source;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0028-create-materialized-view-duplicate_alias_mv-account_id", Compare: "sqlstate"},
				},
				{
					Query: `CREATE MATERIALIZED VIEW duplicate_remaining_mv (v) AS SELECT id, v FROM source;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0029-create-materialized-view-duplicate_remaining_mv-v", Compare: "sqlstate"},
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
					Query: `SELECT relkind FROM pg_class WHERE relname = 'source_mv';`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0030-select-relkind-from-pg_class-where"},
				},
				{
					Query: `SELECT hasindexes::text, ispopulated::text,
							trim(trailing ';' from regexp_replace(trim(definition), '\s+', ' ', 'g')) AS definition
						FROM pg_matviews
						WHERE schemaname = current_schema() AND matviewname = 'source_mv';`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0031-select-hasindexes::text-ispopulated::text-trim-trailing"},
				},
				{
					Query: `SELECT account_id, amount FROM source_mv ORDER BY account_id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0032-select-account_id-amount-from-source_mv", Compare: "sqlstate"},
				},
				{
					Query: `SELECT attname
						FROM pg_attribute
						WHERE attrelid = 'source_mv'::regclass AND attnum > 0 AND NOT attisdropped
						ORDER BY attnum;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0033-select-attname-from-pg_attribute-where"},
				},
				{
					Query: `CREATE UNIQUE INDEX source_mv_id_idx ON source_mv (account_id);`,
				},
				{
					Query: `SELECT hasindexes::text, ispopulated::text
						FROM pg_matviews
						WHERE schemaname = current_schema() AND matviewname = 'source_mv';`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0034-select-hasindexes::text-ispopulated::text-from-pg_matviews"},
				},
				{
					Query: `REFRESH MATERIALIZED VIEW source_mv WITH DATA;`,
				},
				{
					Query: `SELECT account_id, amount FROM source_mv ORDER BY account_id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0035-select-account_id-amount-from-source_mv"},
				},
				{
					Query: `SELECT ispopulated::text
						FROM pg_matviews
						WHERE schemaname = current_schema() AND matviewname = 'source_mv';`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0036-select-ispopulated::text-from-pg_matviews-where"},
				},
				{
					Query: `REFRESH MATERIALIZED VIEW source_mv WITH NO DATA;`,
				},
				{
					Query: `SELECT hasindexes::text, ispopulated::text
						FROM pg_matviews
						WHERE schemaname = current_schema() AND matviewname = 'source_mv';`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0037-select-hasindexes::text-ispopulated::text-from-pg_matviews"},
				},
				{
					Query: `SELECT account_id, amount FROM source_mv ORDER BY account_id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0038-select-account_id-amount-from-source_mv", Compare: "sqlstate"},
				},
				{
					Query: `REFRESH MATERIALIZED VIEW source_mv;`,
				},
				{
					Query: `SELECT account_id, amount FROM source_mv ORDER BY account_id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0039-select-account_id-amount-from-source_mv"},
				},
				{
					Query: `CREATE MATERIALIZED VIEW paren_mv AS (SELECT id, v FROM source) WITH NO DATA;`,
				},
				{
					Query: `SELECT ispopulated::text
						FROM pg_matviews
						WHERE schemaname = current_schema() AND matviewname = 'paren_mv';`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0040-select-ispopulated::text-from-pg_matviews-where"},
				},
				{
					Query: `SELECT id, v FROM paren_mv ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0041-select-id-v-from-paren_mv", Compare: "sqlstate"},
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
					Query: `SELECT account_id, amount FROM source_mv ORDER BY account_id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0042-select-account_id-amount-from-source_mv"},
				},
				{
					Query: `REFRESH MATERIALIZED VIEW source_mv;`,
				},
				{
					Query: `SELECT account_id, amount FROM source_mv ORDER BY account_id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0043-select-account_id-amount-from-source_mv"},
				},
				{
					Query: `SELECT hasindexes::text, ispopulated::text
						FROM pg_matviews
						WHERE schemaname = current_schema() AND matviewname = 'source_mv';`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0044-select-hasindexes::text-ispopulated::text-from-pg_matviews"},
				},
				{
					Query: `UPDATE source SET v = 350 WHERE id = 3;`,
				},
				{
					Query: `REFRESH MATERIALIZED VIEW source_mv WITH DATA;`,
				},
				{
					Query: `SELECT account_id, amount FROM source_mv ORDER BY account_id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0045-select-account_id-amount-from-source_mv"},
				},
				{
					Query: `UPDATE source SET v = 450 WHERE id = 3;`,
				},
				{
					Query: `REFRESH MATERIALIZED VIEW CONCURRENTLY source_mv;`,
				},
				{
					Query: `SELECT account_id, amount FROM source_mv ORDER BY account_id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0046-select-account_id-amount-from-source_mv"},
				},
				{
					Query: `REFRESH MATERIALIZED VIEW CONCURRENTLY source_mv WITH NO DATA;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0047-refresh-materialized-view-concurrently-source_mv", Compare: "sqlstate"},
				},
				{
					Query: `REFRESH MATERIALIZED VIEW source_mv WITH NO DATA;`,
				},
				{
					Query: `SELECT ispopulated::text
						FROM pg_matviews
						WHERE schemaname = current_schema() AND matviewname = 'source_mv';`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0048-select-ispopulated::text-from-pg_matviews-where"},
				},
				{
					Query: `SELECT account_id, amount FROM source_mv ORDER BY account_id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0049-select-account_id-amount-from-source_mv", Compare: "sqlstate"},
				},
				{
					Query: `REFRESH MATERIALIZED VIEW CONCURRENTLY source_mv;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0050-refresh-materialized-view-concurrently-source_mv", Compare: "sqlstate"},
				},
				{
					Query: `REFRESH MATERIALIZED VIEW source_mv;`,
				},
				{
					Query: `SELECT account_id, amount FROM source_mv ORDER BY account_id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0051-select-account_id-amount-from-source_mv"},
				},
				{
					Query: `CREATE TABLE plain_table (id INT);`,
				},
				{
					Query: `REFRESH MATERIALIZED VIEW plain_table;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0052-refresh-materialized-view-plain_table", Compare: "sqlstate"},
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
					Query: `REFRESH MATERIALIZED VIEW dup_mv;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0053-refresh-materialized-view-dup_mv", Compare: "sqlstate"},
				},
				{
					Query: `SELECT grp FROM dup_mv ORDER BY grp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0054-select-grp-from-dup_mv-order"},
				},
				{
					Query: `CREATE MATERIALIZED VIEW no_unique_mv AS SELECT id, v FROM source;`,
				},
				{
					Query: `REFRESH MATERIALIZED VIEW CONCURRENTLY no_unique_mv;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0055-refresh-materialized-view-concurrently-no_unique_mv", Compare: "sqlstate"},
				},
				{
					Query: `CREATE MATERIALIZED VIEW partial_unique_mv AS SELECT id, v FROM source;`,
				},
				{
					Query: `CREATE UNIQUE INDEX partial_unique_mv_idx ON partial_unique_mv (id) WHERE v IS NOT NULL;`,
				},
				{
					Query: `REFRESH MATERIALIZED VIEW CONCURRENTLY partial_unique_mv;`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-probe-test-testmaterializedviewprobe-0056-refresh-materialized-view-concurrently-partial_unique_mv", Compare: "sqlstate"},
				},
			},
		},
	})
}
