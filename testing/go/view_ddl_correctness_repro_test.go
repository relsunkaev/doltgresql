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

// TestCreateTemporaryViewRoundTripRepro reproduces a view DDL correctness bug:
// PostgreSQL accepts CREATE TEMPORARY VIEW and records it in a temp schema.
func TestCreateTemporaryViewRoundTripRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TEMPORARY VIEW round trips from temp schema",
			SetUpScript: []string{
				`CREATE TEMPORARY VIEW temp_view_roundtrip AS SELECT 7 AS id;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT id FROM temp_view_roundtrip;`,
					Expected: []sql.Row{{7}},
				},
				{
					Query: `SELECT table_schema LIKE 'pg_temp_%'
						FROM information_schema.views
						WHERE table_name = 'temp_view_roundtrip';`,
					Expected: []sql.Row{{true}},
				},
			},
		},
	})
}

// TestCreateRecursiveViewRoundTripRepro reproduces a view DDL correctness bug:
// PostgreSQL accepts CREATE RECURSIVE VIEW and evaluates the recursive query.
func TestCreateRecursiveViewRoundTripRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE RECURSIVE VIEW evaluates recursive definition",
			SetUpScript: []string{
				`CREATE RECURSIVE VIEW recursive_nums (n) AS
					VALUES (1)
				UNION ALL
					SELECT n + 1 FROM recursive_nums WHERE n < 3;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT n FROM recursive_nums ORDER BY n;`,
					Expected: []sql.Row{{1}, {2}, {3}},
				},
			},
		},
	})
}

// TestCreateViewFromOrderedUnionSubqueriesGuard guards that PostgreSQL-style
// view definitions can union parenthesized ordered subqueries.
func TestCreateViewFromOrderedUnionSubqueriesGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE VIEW from ordered UNION subqueries",
			SetUpScript: []string{
				`CREATE TABLE ordered_union_view_source (pk INT);`,
				`INSERT INTO ordered_union_view_source VALUES (1), (2), (3), (4);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE VIEW ordered_union_view AS
						(SELECT pk FROM ordered_union_view_source ORDER BY pk DESC LIMIT 1)
						UNION ALL
						(SELECT pk FROM ordered_union_view_source ORDER BY pk LIMIT 1);`,
				},
				{
					Query:    `SELECT pk FROM ordered_union_view ORDER BY pk;`,
					Expected: []sql.Row{{1}, {4}},
				},
			},
		},
	})
}

// TestAlterViewRenameToRepro reproduces a view DDL correctness bug:
// PostgreSQL accepts ALTER VIEW ... RENAME TO and keeps the view queryable under
// the new name.
func TestAlterViewRenameToRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER VIEW RENAME TO moves view name",
			SetUpScript: []string{
				`CREATE TABLE alter_view_rename_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO alter_view_rename_source VALUES (1, 'renamed');`,
				`CREATE VIEW alter_view_rename_reader AS
					SELECT id, label FROM alter_view_rename_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER VIEW alter_view_rename_reader
						RENAME TO alter_view_renamed_reader;`,
				},
				{
					Query: `SELECT id, label
						FROM alter_view_renamed_reader;`,
					Expected: []sql.Row{{1, "renamed"}},
				},
				{
					Query:       `SELECT id, label FROM alter_view_rename_reader;`,
					ExpectedErr: `not found`,
				},
			},
		},
	})
}

// TestCreateOrReplaceViewRejectsColumnRenameRepro reproduces a view DDL
// correctness bug: PostgreSQL rejects CREATE OR REPLACE VIEW when an existing
// output column would be renamed.
func TestCreateOrReplaceViewRejectsColumnRenameRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE OR REPLACE VIEW rejects column rename",
			SetUpScript: []string{
				`CREATE VIEW replace_view_column_name AS
					SELECT 1 AS id, 'old'::text AS label;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE OR REPLACE VIEW replace_view_column_name AS
						SELECT 1 AS id, 'new'::text AS renamed_label;`,
					ExpectedErr: `cannot change name of view column`,
				},
				{
					Query:    `SELECT id, label FROM replace_view_column_name;`,
					Expected: []sql.Row{{1, "old"}},
				},
			},
		},
	})
}

// TestCreateOrReplaceViewRejectsColumnTypeChangeRepro reproduces a view DDL
// correctness bug: PostgreSQL rejects CREATE OR REPLACE VIEW when an existing
// output column would change type.
func TestCreateOrReplaceViewRejectsColumnTypeChangeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE OR REPLACE VIEW rejects column type change",
			SetUpScript: []string{
				`CREATE VIEW replace_view_column_type AS
					SELECT 1 AS id, 'old'::text AS label;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE OR REPLACE VIEW replace_view_column_type AS
						SELECT 1 AS id, 7 AS label;`,
					ExpectedErr: `cannot change data type of view column`,
				},
				{
					Query:    `SELECT id, label FROM replace_view_column_type;`,
					Expected: []sql.Row{{1, "old"}},
				},
			},
		},
	})
}

// TestCreateOrReplaceViewRejectsColumnDropRepro reproduces a view DDL
// correctness bug: PostgreSQL rejects CREATE OR REPLACE VIEW when the
// replacement would remove an existing output column.
func TestCreateOrReplaceViewRejectsColumnDropRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE OR REPLACE VIEW rejects column drop",
			SetUpScript: []string{
				`CREATE VIEW replace_view_column_drop AS
					SELECT 1 AS id, 'old'::text AS label;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE OR REPLACE VIEW replace_view_column_drop AS
						SELECT 1 AS id;`,
					ExpectedErr: `cannot drop columns from view`,
				},
				{
					Query:    `SELECT id, label FROM replace_view_column_drop;`,
					Expected: []sql.Row{{1, "old"}},
				},
			},
		},
	})
}

// TestCreateOrReplaceViewRejectsColumnReorderRepro reproduces a view DDL
// correctness bug: PostgreSQL rejects CREATE OR REPLACE VIEW when the
// replacement would reorder existing output columns.
func TestCreateOrReplaceViewRejectsColumnReorderRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE OR REPLACE VIEW rejects column reorder",
			SetUpScript: []string{
				`CREATE VIEW replace_view_column_reorder AS
					SELECT 1 AS id, 'old'::text AS label;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE OR REPLACE VIEW replace_view_column_reorder AS
						SELECT 'old'::text AS label, 1 AS id;`,
					ExpectedErr: `cannot change name of view column`,
				},
				{
					Query:    `SELECT id, label FROM replace_view_column_reorder;`,
					Expected: []sql.Row{{1, "old"}},
				},
			},
		},
	})
}

// TestCreateOrReplaceViewAllowsAppendingColumnsGuard covers the compatible
// CREATE OR REPLACE VIEW case: PostgreSQL allows appending new output columns
// after the existing view columns.
func TestCreateOrReplaceViewAllowsAppendingColumnsGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE OR REPLACE VIEW allows appending columns",
			SetUpScript: []string{
				`CREATE VIEW replace_view_append_column AS
					SELECT 1 AS id, 'old'::text AS label;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE OR REPLACE VIEW replace_view_append_column AS
						SELECT 1 AS id, 'old'::text AS label, true AS active;`,
				},
				{
					Query: `SELECT id, label, active
						FROM replace_view_append_column;`,
					Expected: []sql.Row{{1, "old", "t"}},
				},
			},
		},
	})
}

// TestCreateMaterializedViewWithDataGuard covers materialized-view DDL
// semantics: PostgreSQL accepts an explicit WITH DATA clause and populates the
// materialized view immediately.
func TestCreateMaterializedViewWithDataGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE MATERIALIZED VIEW WITH DATA populates rows",
			SetUpScript: []string{
				`CREATE TABLE matview_with_data_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO matview_with_data_source VALUES (1, 'one'), (2, 'two');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE MATERIALIZED VIEW matview_with_data_reader AS
						SELECT id, label FROM matview_with_data_source
						WITH DATA;`,
				},
				{
					Query: `SELECT id, label
						FROM matview_with_data_reader
						ORDER BY id;`,
					Expected: []sql.Row{{1, "one"}, {2, "two"}},
				},
			},
		},
	})
}

// TestCreateMaterializedViewTimetzTypmodMaterializesRoundedValueRepro
// reproduces a materialized-view persistence bug: PostgreSQL materializes
// typmod-constrained timetz query output using the rounded value and preserves
// the output column typmod.
func TestCreateMaterializedViewTimetzTypmodMaterializesRoundedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE MATERIALIZED VIEW timetz typmod materializes rounded value",
			SetUpScript: []string{
				`CREATE MATERIALIZED VIEW matview_timetz_typmod_reader AS
					SELECT CAST('21:43:56.789+00'::timetz AS TIMETZ(0)) AS tz;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT tz::text
						FROM matview_timetz_typmod_reader;`,
					Expected: []sql.Row{{"21:43:57+00"}},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'matview_timetz_typmod_reader'::regclass
							AND a.attname = 'tz';`,
					Expected: []sql.Row{{"time(0) with time zone"}},
				},
			},
		},
	})
}

// TestCreateMaterializedViewTimeTypmodMaterializesRoundedValueGuard guards that
// materialized views persist typmod-constrained time query output using the
// rounded value and preserve the output column typmod.
func TestCreateMaterializedViewTimeTypmodMaterializesRoundedValueGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE MATERIALIZED VIEW time typmod materializes rounded value",
			SetUpScript: []string{
				`CREATE MATERIALIZED VIEW matview_time_typmod_reader AS
					SELECT CAST('21:43:56.789'::time AS TIME(0)) AS t;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT t::text
						FROM matview_time_typmod_reader;`,
					Expected: []sql.Row{{"21:43:57"}},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'matview_time_typmod_reader'::regclass
							AND a.attname = 't';`,
					Expected: []sql.Row{{"time(0) without time zone"}},
				},
			},
		},
	})
}

// TestCreateMaterializedViewTimestampTypmodMaterializesRoundedValueRepro
// reproduces a materialized-view persistence bug: PostgreSQL materializes
// typmod-constrained timestamp query output using the rounded value and
// preserves the output column typmod.
func TestCreateMaterializedViewTimestampTypmodMaterializesRoundedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE MATERIALIZED VIEW timestamp typmod materializes rounded value",
			SetUpScript: []string{
				`CREATE MATERIALIZED VIEW matview_timestamp_typmod_reader AS
					SELECT CAST(TIMESTAMP '2021-09-15 21:43:56.789' AS TIMESTAMP(0)) AS ts;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ts::text
						FROM matview_timestamp_typmod_reader;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57"}},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'matview_timestamp_typmod_reader'::regclass
							AND a.attname = 'ts';`,
					Expected: []sql.Row{{"timestamp(0) without time zone"}},
				},
			},
		},
	})
}

// TestCreateMaterializedViewTimestamptzTypmodMaterializesRoundedValueRepro
// reproduces a materialized-view persistence bug: PostgreSQL materializes
// typmod-constrained timestamptz query output using the rounded value and
// preserves the output column typmod.
func TestCreateMaterializedViewTimestamptzTypmodMaterializesRoundedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE MATERIALIZED VIEW timestamptz typmod materializes rounded value",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
				`CREATE MATERIALIZED VIEW matview_timestamptz_typmod_reader AS
					SELECT CAST(TIMESTAMPTZ '2021-09-15 21:43:56.789+00' AS TIMESTAMPTZ(0)) AS ts;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ts::text
						FROM matview_timestamptz_typmod_reader;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57+00"}},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'matview_timestamptz_typmod_reader'::regclass
							AND a.attname = 'ts';`,
					Expected: []sql.Row{{"timestamp(0) with time zone"}},
				},
			},
		},
	})
}

// TestCreateMaterializedViewIntervalTypmodMaterializesRestrictedValueRepro
// reproduces a materialized-view persistence bug: PostgreSQL materializes
// typmod-constrained interval query output using the restricted value and
// preserves the output column typmod.
func TestCreateMaterializedViewIntervalTypmodMaterializesRestrictedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE MATERIALIZED VIEW interval typmod materializes restricted value",
			SetUpScript: []string{
				`CREATE MATERIALIZED VIEW matview_interval_typmod_reader AS
					SELECT CAST(INTERVAL '3 days 04:05:06.789' AS INTERVAL DAY TO SECOND(0)) AS ds;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ds::text
						FROM matview_interval_typmod_reader;`,
					Expected: []sql.Row{{"3 days 04:05:07"}},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'matview_interval_typmod_reader'::regclass
							AND a.attname = 'ds';`,
					Expected: []sql.Row{{"interval day to second(0)"}},
				},
			},
		},
	})
}

// TestCreateMaterializedViewVarcharTypmodMaterializesTruncatedValueGuard guards
// that materialized views persist typmod-constrained varchar query output using
// the truncated value and preserve the output column typmod.
func TestCreateMaterializedViewVarcharTypmodMaterializesTruncatedValueGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE MATERIALIZED VIEW varchar typmod materializes truncated value",
			SetUpScript: []string{
				`CREATE MATERIALIZED VIEW matview_varchar_typmod_reader AS
					SELECT CAST('abcd' AS VARCHAR(3)) AS label;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT label
						FROM matview_varchar_typmod_reader;`,
					Expected: []sql.Row{{"abc"}},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'matview_varchar_typmod_reader'::regclass
							AND a.attname = 'label';`,
					Expected: []sql.Row{{"character varying(3)"}},
				},
			},
		},
	})
}

// TestCreateMaterializedViewCharacterTypmodMaterializesPaddedValueRepro
// reproduces a materialized-view persistence bug: PostgreSQL persists
// typmod-constrained character query output using the padded value and preserves
// the output column typmod.
func TestCreateMaterializedViewCharacterTypmodMaterializesPaddedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE MATERIALIZED VIEW character typmod materializes padded value",
			SetUpScript: []string{
				`CREATE MATERIALIZED VIEW matview_character_typmod_reader AS
					SELECT CAST('ab' AS CHARACTER(3)) AS label;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT octet_length(label)
						FROM matview_character_typmod_reader;`,
					Expected: []sql.Row{{3}},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'matview_character_typmod_reader'::regclass
							AND a.attname = 'label';`,
					Expected: []sql.Row{{"character(3)"}},
				},
			},
		},
	})
}

// TestRefreshMaterializedViewTimetzTypmodMaterializesRoundedValueRepro
// reproduces a materialized-view refresh persistence bug: PostgreSQL refreshes
// typmod-constrained timetz query output using the rounded value and preserves
// the output column typmod.
func TestRefreshMaterializedViewTimetzTypmodMaterializesRoundedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REFRESH MATERIALIZED VIEW timetz typmod materializes rounded value",
			SetUpScript: []string{
				`CREATE TABLE matview_refresh_timetz_typmod_source (
					id INT PRIMARY KEY,
					tz TIMETZ
				);`,
				`CREATE MATERIALIZED VIEW matview_refresh_timetz_typmod_reader AS
					SELECT CAST(tz AS TIMETZ(0)) AS tz
					FROM matview_refresh_timetz_typmod_source
					WITH NO DATA;`,
				`INSERT INTO matview_refresh_timetz_typmod_source
					VALUES (1, '21:43:56.789+00');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'matview_refresh_timetz_typmod_reader'::regclass
							AND a.attname = 'tz';`,
					Expected: []sql.Row{{"time(0) with time zone"}},
				},
				{
					Query: `REFRESH MATERIALIZED VIEW matview_refresh_timetz_typmod_reader;`,
				},
				{
					Query: `SELECT tz::text
						FROM matview_refresh_timetz_typmod_reader;`,
					Expected: []sql.Row{{"21:43:57+00"}},
				},
			},
		},
	})
}

// TestRefreshMaterializedViewTimeTypmodMaterializesRoundedValueGuard guards that
// refresh persists typmod-constrained time query output using the rounded value
// and preserves the output column typmod.
func TestRefreshMaterializedViewTimeTypmodMaterializesRoundedValueGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REFRESH MATERIALIZED VIEW time typmod materializes rounded value",
			SetUpScript: []string{
				`CREATE TABLE matview_refresh_time_typmod_source (
					id INT PRIMARY KEY,
					t TIME
				);`,
				`CREATE MATERIALIZED VIEW matview_refresh_time_typmod_reader AS
					SELECT CAST(t AS TIME(0)) AS t
					FROM matview_refresh_time_typmod_source
					WITH NO DATA;`,
				`INSERT INTO matview_refresh_time_typmod_source
					VALUES (1, '21:43:56.789'::time);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'matview_refresh_time_typmod_reader'::regclass
							AND a.attname = 't';`,
					Expected: []sql.Row{{"time(0) without time zone"}},
				},
				{
					Query: `REFRESH MATERIALIZED VIEW matview_refresh_time_typmod_reader;`,
				},
				{
					Query: `SELECT t::text
						FROM matview_refresh_time_typmod_reader;`,
					Expected: []sql.Row{{"21:43:57"}},
				},
			},
		},
	})
}

// TestRefreshMaterializedViewTimestampTypmodMaterializesRoundedValueRepro
// reproduces a materialized-view refresh persistence bug: PostgreSQL refreshes
// typmod-constrained timestamp query output using the rounded value and
// preserves the output column typmod.
func TestRefreshMaterializedViewTimestampTypmodMaterializesRoundedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REFRESH MATERIALIZED VIEW timestamp typmod materializes rounded value",
			SetUpScript: []string{
				`CREATE TABLE matview_refresh_timestamp_typmod_source (
					id INT PRIMARY KEY,
					ts TIMESTAMP
				);`,
				`CREATE MATERIALIZED VIEW matview_refresh_timestamp_typmod_reader AS
					SELECT CAST(ts AS TIMESTAMP(0)) AS ts
					FROM matview_refresh_timestamp_typmod_source
					WITH NO DATA;`,
				`INSERT INTO matview_refresh_timestamp_typmod_source
					VALUES (1, TIMESTAMP '2021-09-15 21:43:56.789');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'matview_refresh_timestamp_typmod_reader'::regclass
							AND a.attname = 'ts';`,
					Expected: []sql.Row{{"timestamp(0) without time zone"}},
				},
				{
					Query: `REFRESH MATERIALIZED VIEW matview_refresh_timestamp_typmod_reader;`,
				},
				{
					Query: `SELECT ts::text
						FROM matview_refresh_timestamp_typmod_reader;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57"}},
				},
			},
		},
	})
}

// TestRefreshMaterializedViewTimestamptzTypmodMaterializesRoundedValueRepro
// reproduces a materialized-view refresh persistence bug: PostgreSQL refreshes
// typmod-constrained timestamptz query output using the rounded value and
// preserves the output column typmod.
func TestRefreshMaterializedViewTimestamptzTypmodMaterializesRoundedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REFRESH MATERIALIZED VIEW timestamptz typmod materializes rounded value",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
				`CREATE TABLE matview_refresh_timestamptz_typmod_source (
					id INT PRIMARY KEY,
					ts TIMESTAMPTZ
				);`,
				`CREATE MATERIALIZED VIEW matview_refresh_timestamptz_typmod_reader AS
					SELECT CAST(ts AS TIMESTAMPTZ(0)) AS ts
					FROM matview_refresh_timestamptz_typmod_source
					WITH NO DATA;`,
				`INSERT INTO matview_refresh_timestamptz_typmod_source
					VALUES (1, TIMESTAMPTZ '2021-09-15 21:43:56.789+00');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'matview_refresh_timestamptz_typmod_reader'::regclass
							AND a.attname = 'ts';`,
					Expected: []sql.Row{{"timestamp(0) with time zone"}},
				},
				{
					Query: `REFRESH MATERIALIZED VIEW matview_refresh_timestamptz_typmod_reader;`,
				},
				{
					Query: `SELECT ts::text
						FROM matview_refresh_timestamptz_typmod_reader;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57+00"}},
				},
			},
		},
	})
}

// TestRefreshMaterializedViewIntervalTypmodMaterializesRestrictedValueRepro
// reproduces a materialized-view refresh persistence bug: PostgreSQL refreshes
// typmod-constrained interval query output using the restricted value and
// preserves the output column typmod.
func TestRefreshMaterializedViewIntervalTypmodMaterializesRestrictedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REFRESH MATERIALIZED VIEW interval typmod materializes restricted value",
			SetUpScript: []string{
				`CREATE TABLE matview_refresh_interval_typmod_source (
					id INT PRIMARY KEY,
					ds INTERVAL
				);`,
				`CREATE MATERIALIZED VIEW matview_refresh_interval_typmod_reader AS
					SELECT CAST(ds AS INTERVAL DAY TO SECOND(0)) AS ds
					FROM matview_refresh_interval_typmod_source
					WITH NO DATA;`,
				`INSERT INTO matview_refresh_interval_typmod_source
					VALUES (1, INTERVAL '3 days 04:05:06.789');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'matview_refresh_interval_typmod_reader'::regclass
							AND a.attname = 'ds';`,
					Expected: []sql.Row{{"interval day to second(0)"}},
				},
				{
					Query: `REFRESH MATERIALIZED VIEW matview_refresh_interval_typmod_reader;`,
				},
				{
					Query: `SELECT ds::text
						FROM matview_refresh_interval_typmod_reader;`,
					Expected: []sql.Row{{"3 days 04:05:07"}},
				},
			},
		},
	})
}

// TestRefreshMaterializedViewCharacterTypmodMaterializesPaddedValueRepro
// reproduces a materialized-view refresh persistence bug: PostgreSQL refreshes
// typmod-constrained character query output using the padded value and preserves
// the output column typmod.
func TestRefreshMaterializedViewCharacterTypmodMaterializesPaddedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REFRESH MATERIALIZED VIEW character typmod materializes padded value",
			SetUpScript: []string{
				`CREATE TABLE matview_refresh_character_typmod_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE MATERIALIZED VIEW matview_refresh_character_typmod_reader AS
					SELECT CAST(label AS CHARACTER(3)) AS label
					FROM matview_refresh_character_typmod_source
					WITH NO DATA;`,
				`INSERT INTO matview_refresh_character_typmod_source
					VALUES (1, 'ab');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'matview_refresh_character_typmod_reader'::regclass
							AND a.attname = 'label';`,
					Expected: []sql.Row{{"character(3)"}},
				},
				{
					Query: `REFRESH MATERIALIZED VIEW matview_refresh_character_typmod_reader;`,
				},
				{
					Query: `SELECT octet_length(label)
						FROM matview_refresh_character_typmod_reader;`,
					Expected: []sql.Row{{3}},
				},
			},
		},
	})
}

// TestRefreshMaterializedViewConcurrentlyTimetzTypmodMaterializesRoundedValueRepro
// reproduces a materialized-view concurrent refresh persistence bug:
// PostgreSQL refreshes typmod-constrained timetz query output using the rounded
// value and preserves the output column typmod.
func TestRefreshMaterializedViewConcurrentlyTimetzTypmodMaterializesRoundedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REFRESH MATERIALIZED VIEW CONCURRENTLY timetz typmod materializes rounded value",
			SetUpScript: []string{
				`CREATE TABLE matview_concurrently_timetz_typmod_source (
					id INT PRIMARY KEY,
					tz TIMETZ
				);`,
				`INSERT INTO matview_concurrently_timetz_typmod_source
					VALUES (1, '10:00:00+00');`,
				`CREATE MATERIALIZED VIEW matview_concurrently_timetz_typmod_reader AS
					SELECT id, CAST(tz AS TIMETZ(0)) AS tz
					FROM matview_concurrently_timetz_typmod_source;`,
				`CREATE UNIQUE INDEX matview_concurrently_timetz_typmod_reader_id_idx
					ON matview_concurrently_timetz_typmod_reader (id);`,
				`UPDATE matview_concurrently_timetz_typmod_source
					SET tz = '21:43:56.789+00'
					WHERE id = 1;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `REFRESH MATERIALIZED VIEW CONCURRENTLY
						matview_concurrently_timetz_typmod_reader;`,
				},
				{
					Query: `SELECT tz::text
						FROM matview_concurrently_timetz_typmod_reader;`,
					Expected: []sql.Row{{"21:43:57+00"}},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'matview_concurrently_timetz_typmod_reader'::regclass
							AND a.attname = 'tz';`,
					Expected: []sql.Row{{"time(0) with time zone"}},
				},
			},
		},
	})
}

// TestRefreshMaterializedViewConcurrentlyTimeTypmodMaterializesRoundedValueGuard
// guards that concurrent refresh persists typmod-constrained time query output
// using the rounded value and preserves the output column typmod.
func TestRefreshMaterializedViewConcurrentlyTimeTypmodMaterializesRoundedValueGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REFRESH MATERIALIZED VIEW CONCURRENTLY time typmod materializes rounded value",
			SetUpScript: []string{
				`CREATE TABLE matview_concurrently_time_typmod_source (
					id INT PRIMARY KEY,
					t TIME
				);`,
				`INSERT INTO matview_concurrently_time_typmod_source
					VALUES (1, '10:00:00'::time);`,
				`CREATE MATERIALIZED VIEW matview_concurrently_time_typmod_reader AS
					SELECT id, CAST(t AS TIME(0)) AS t
					FROM matview_concurrently_time_typmod_source;`,
				`CREATE UNIQUE INDEX matview_concurrently_time_typmod_reader_id_idx
					ON matview_concurrently_time_typmod_reader (id);`,
				`UPDATE matview_concurrently_time_typmod_source
					SET t = '21:43:56.789'::time
					WHERE id = 1;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `REFRESH MATERIALIZED VIEW CONCURRENTLY
						matview_concurrently_time_typmod_reader;`,
				},
				{
					Query: `SELECT t::text
						FROM matview_concurrently_time_typmod_reader;`,
					Expected: []sql.Row{{"21:43:57"}},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'matview_concurrently_time_typmod_reader'::regclass
							AND a.attname = 't';`,
					Expected: []sql.Row{{"time(0) without time zone"}},
				},
			},
		},
	})
}

// TestRefreshMaterializedViewConcurrentlyTimestampTypmodMaterializesRoundedValueRepro
// reproduces a materialized-view concurrent refresh persistence bug: PostgreSQL
// refreshes typmod-constrained timestamp query output using the rounded value
// and preserves the output column typmod.
func TestRefreshMaterializedViewConcurrentlyTimestampTypmodMaterializesRoundedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REFRESH MATERIALIZED VIEW CONCURRENTLY timestamp typmod materializes rounded value",
			SetUpScript: []string{
				`CREATE TABLE matview_concurrently_timestamp_typmod_source (
					id INT PRIMARY KEY,
					ts TIMESTAMP
				);`,
				`INSERT INTO matview_concurrently_timestamp_typmod_source
					VALUES (1, TIMESTAMP '2021-09-15 10:00:00');`,
				`CREATE MATERIALIZED VIEW matview_concurrently_timestamp_typmod_reader AS
					SELECT id, CAST(ts AS TIMESTAMP(0)) AS ts
					FROM matview_concurrently_timestamp_typmod_source;`,
				`CREATE UNIQUE INDEX matview_concurrently_timestamp_typmod_reader_id_idx
					ON matview_concurrently_timestamp_typmod_reader (id);`,
				`UPDATE matview_concurrently_timestamp_typmod_source
					SET ts = TIMESTAMP '2021-09-15 21:43:56.789'
					WHERE id = 1;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `REFRESH MATERIALIZED VIEW CONCURRENTLY
						matview_concurrently_timestamp_typmod_reader;`,
				},
				{
					Query: `SELECT ts::text
						FROM matview_concurrently_timestamp_typmod_reader;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57"}},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'matview_concurrently_timestamp_typmod_reader'::regclass
							AND a.attname = 'ts';`,
					Expected: []sql.Row{{"timestamp(0) without time zone"}},
				},
			},
		},
	})
}

// TestRefreshMaterializedViewConcurrentlyTimestamptzTypmodMaterializesRoundedValueRepro
// reproduces a materialized-view concurrent refresh persistence bug: PostgreSQL
// refreshes typmod-constrained timestamptz query output using the rounded value
// and preserves the output column typmod.
func TestRefreshMaterializedViewConcurrentlyTimestamptzTypmodMaterializesRoundedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REFRESH MATERIALIZED VIEW CONCURRENTLY timestamptz typmod materializes rounded value",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
				`CREATE TABLE matview_concurrently_timestamptz_typmod_source (
					id INT PRIMARY KEY,
					ts TIMESTAMPTZ
				);`,
				`INSERT INTO matview_concurrently_timestamptz_typmod_source
					VALUES (1, TIMESTAMPTZ '2021-09-15 10:00:00+00');`,
				`CREATE MATERIALIZED VIEW matview_concurrently_timestamptz_typmod_reader AS
					SELECT id, CAST(ts AS TIMESTAMPTZ(0)) AS ts
					FROM matview_concurrently_timestamptz_typmod_source;`,
				`CREATE UNIQUE INDEX matview_concurrently_timestamptz_typmod_reader_id_idx
					ON matview_concurrently_timestamptz_typmod_reader (id);`,
				`UPDATE matview_concurrently_timestamptz_typmod_source
					SET ts = TIMESTAMPTZ '2021-09-15 21:43:56.789+00'
					WHERE id = 1;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `REFRESH MATERIALIZED VIEW CONCURRENTLY
						matview_concurrently_timestamptz_typmod_reader;`,
				},
				{
					Query: `SELECT ts::text
						FROM matview_concurrently_timestamptz_typmod_reader;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57+00"}},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'matview_concurrently_timestamptz_typmod_reader'::regclass
							AND a.attname = 'ts';`,
					Expected: []sql.Row{{"timestamp(0) with time zone"}},
				},
			},
		},
	})
}

// TestRefreshMaterializedViewConcurrentlyIntervalTypmodMaterializesRestrictedValueRepro
// reproduces a materialized-view concurrent refresh persistence bug: PostgreSQL
// refreshes typmod-constrained interval query output using the restricted value
// and preserves the output column typmod.
func TestRefreshMaterializedViewConcurrentlyIntervalTypmodMaterializesRestrictedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REFRESH MATERIALIZED VIEW CONCURRENTLY interval typmod materializes restricted value",
			SetUpScript: []string{
				`CREATE TABLE matview_concurrently_interval_typmod_source (
					id INT PRIMARY KEY,
					ds INTERVAL
				);`,
				`INSERT INTO matview_concurrently_interval_typmod_source
					VALUES (1, INTERVAL '1 day');`,
				`CREATE MATERIALIZED VIEW matview_concurrently_interval_typmod_reader AS
					SELECT id, CAST(ds AS INTERVAL DAY TO SECOND(0)) AS ds
					FROM matview_concurrently_interval_typmod_source;`,
				`CREATE UNIQUE INDEX matview_concurrently_interval_typmod_reader_id_idx
					ON matview_concurrently_interval_typmod_reader (id);`,
				`UPDATE matview_concurrently_interval_typmod_source
					SET ds = INTERVAL '3 days 04:05:06.789'
					WHERE id = 1;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `REFRESH MATERIALIZED VIEW CONCURRENTLY
						matview_concurrently_interval_typmod_reader;`,
				},
				{
					Query: `SELECT ds::text
						FROM matview_concurrently_interval_typmod_reader;`,
					Expected: []sql.Row{{"3 days 04:05:07"}},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'matview_concurrently_interval_typmod_reader'::regclass
							AND a.attname = 'ds';`,
					Expected: []sql.Row{{"interval day to second(0)"}},
				},
			},
		},
	})
}

// TestRefreshMaterializedViewConcurrentlyCharacterTypmodMaterializesPaddedValueRepro
// reproduces a materialized-view concurrent refresh persistence bug: PostgreSQL
// refreshes typmod-constrained character query output using the padded value and
// preserves the output column typmod.
func TestRefreshMaterializedViewConcurrentlyCharacterTypmodMaterializesPaddedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REFRESH MATERIALIZED VIEW CONCURRENTLY character typmod materializes padded value",
			SetUpScript: []string{
				`CREATE TABLE matview_concurrently_character_typmod_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO matview_concurrently_character_typmod_source
					VALUES (1, 'xy');`,
				`CREATE MATERIALIZED VIEW matview_concurrently_character_typmod_reader AS
					SELECT id, CAST(label AS CHARACTER(3)) AS label
					FROM matview_concurrently_character_typmod_source;`,
				`CREATE UNIQUE INDEX matview_concurrently_character_typmod_reader_id_idx
					ON matview_concurrently_character_typmod_reader (id);`,
				`UPDATE matview_concurrently_character_typmod_source
					SET label = 'ab'
					WHERE id = 1;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `REFRESH MATERIALIZED VIEW CONCURRENTLY
						matview_concurrently_character_typmod_reader;`,
				},
				{
					Query: `SELECT octet_length(label)
						FROM matview_concurrently_character_typmod_reader;`,
					Expected: []sql.Row{{3}},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'matview_concurrently_character_typmod_reader'::regclass
							AND a.attname = 'label';`,
					Expected: []sql.Row{{"character(3)"}},
				},
			},
		},
	})
}

// TestCreateMaterializedViewDefaultTablespaceRepro reproduces a materialized
// view DDL correctness bug: PostgreSQL accepts TABLESPACE pg_default.
func TestCreateMaterializedViewDefaultTablespaceRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE MATERIALIZED VIEW with default tablespace",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE MATERIALIZED VIEW matview_default_tablespace_reader
						TABLESPACE pg_default AS
						SELECT 1 AS id;`,
				},
				{
					Query:    `SELECT id FROM matview_default_tablespace_reader;`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestCreateMaterializedViewUsingHeapRepro reproduces a materialized view DDL
// correctness bug: PostgreSQL accepts the default heap access method when it is
// spelled explicitly.
func TestCreateMaterializedViewUsingHeapRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE MATERIALIZED VIEW using heap access method",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE MATERIALIZED VIEW matview_using_heap_reader
						USING heap AS
						SELECT 1 AS id;`,
				},
				{
					Query:    `SELECT id FROM matview_using_heap_reader;`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestCreateMaterializedViewStorageParamsRepro reproduces a catalog
// persistence gap: PostgreSQL stores materialized-view reloptions from CREATE
// MATERIALIZED VIEW ... WITH (...).
func TestCreateMaterializedViewStorageParamsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE MATERIALIZED VIEW storage parameters persist",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE MATERIALIZED VIEW matview_storage_params_reader
						WITH (fillfactor=70, autovacuum_enabled=false) AS
						SELECT 1 AS id;`,
				},
				{
					Query: `SELECT CAST(reloptions AS TEXT)
						FROM pg_catalog.pg_class
						WHERE oid = 'matview_storage_params_reader'::regclass;`,
					Expected: []sql.Row{{"{fillfactor=70,autovacuum_enabled=false}"}},
				},
			},
		},
	})
}

// TestAlterMaterializedViewSetDefaultTablespaceRepro reproduces a materialized
// view DDL correctness bug: PostgreSQL accepts SET TABLESPACE pg_default.
func TestAlterMaterializedViewSetDefaultTablespaceRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER MATERIALIZED VIEW SET TABLESPACE pg_default",
			SetUpScript: []string{
				`CREATE MATERIALIZED VIEW alter_matview_default_tablespace_reader AS
					SELECT 1 AS id;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER MATERIALIZED VIEW alter_matview_default_tablespace_reader
						SET TABLESPACE pg_default;`,
				},
				{
					Query:    `SELECT id FROM alter_matview_default_tablespace_reader;`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestAlterMaterializedViewSetHeapAccessMethodRepro reproduces a materialized
// view DDL correctness bug: PostgreSQL accepts SET ACCESS METHOD heap.
func TestAlterMaterializedViewSetHeapAccessMethodRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER MATERIALIZED VIEW SET ACCESS METHOD heap",
			SetUpScript: []string{
				`CREATE MATERIALIZED VIEW alter_matview_access_method_reader AS
					SELECT 1 AS id;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER MATERIALIZED VIEW alter_matview_access_method_reader
						SET ACCESS METHOD heap;`,
				},
				{
					Query:    `SELECT id FROM alter_matview_access_method_reader;`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestAlterMaterializedViewReloptionsPersistRepro reproduces a materialized
// view catalog persistence gap: PostgreSQL persists reloptions changed with
// ALTER MATERIALIZED VIEW ... SET (...).
func TestAlterMaterializedViewReloptionsPersistRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER MATERIALIZED VIEW storage parameters persist",
			SetUpScript: []string{
				`CREATE MATERIALIZED VIEW alter_matview_reloptions_reader AS
					SELECT 1 AS id;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER MATERIALIZED VIEW alter_matview_reloptions_reader
						SET (fillfactor=80, autovacuum_enabled=false);`,
				},
				{
					Query: `SELECT CAST(reloptions AS TEXT)
						FROM pg_catalog.pg_class
						WHERE oid = 'alter_matview_reloptions_reader'::regclass;`,
					Expected: []sql.Row{{"{fillfactor=80,autovacuum_enabled=false}"}},
				},
			},
		},
	})
}

// TestCreateMaterializedViewWithNoDataGuard covers materialized-view DDL
// semantics: PostgreSQL accepts WITH NO DATA, creates an unpopulated
// materialized view, and later REFRESH populates it.
func TestCreateMaterializedViewWithNoDataGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE MATERIALIZED VIEW WITH NO DATA defers population",
			SetUpScript: []string{
				`CREATE TABLE matview_no_data_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO matview_no_data_source VALUES (1, 'one');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE MATERIALIZED VIEW matview_no_data_reader AS
						SELECT id, label FROM matview_no_data_source
						WITH NO DATA;`,
				},
				{
					Query: `INSERT INTO matview_no_data_source VALUES (2, 'two');`,
				},
				{
					Query: `REFRESH MATERIALIZED VIEW matview_no_data_reader;`,
				},
				{
					Query: `SELECT id, label
						FROM matview_no_data_reader
						ORDER BY id;`,
					Expected: []sql.Row{{1, "one"}, {2, "two"}},
				},
			},
		},
	})
}

// TestCreateMaterializedViewWithNoDataDoesNotEvaluateQueryRepro guards
// PostgreSQL's WITH NO DATA semantics: the materialized-view definition is
// stored without executing the query until REFRESH.
func TestCreateMaterializedViewWithNoDataDoesNotEvaluateQueryRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE MATERIALIZED VIEW WITH NO DATA does not evaluate query",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE MATERIALIZED VIEW matview_no_data_error AS
						SELECT 1 / 0 AS value
						WITH NO DATA;`,
				},
				{
					Query:       `SELECT value FROM matview_no_data_error;`,
					ExpectedErr: `has not been populated`,
				},
				{
					Query:       `REFRESH MATERIALIZED VIEW matview_no_data_error;`,
					ExpectedErr: `division by zero`,
				},
			},
		},
	})
}

// TestRefreshMaterializedViewWithNoDataGuard covers materialized-view refresh
// semantics: PostgreSQL REFRESH ... WITH NO DATA discards the stored snapshot
// and marks the materialized view unscannable until a later refresh with data.
func TestRefreshMaterializedViewWithNoDataGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REFRESH MATERIALIZED VIEW WITH NO DATA clears populated snapshot",
			SetUpScript: []string{
				`CREATE TABLE matview_refresh_no_data_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO matview_refresh_no_data_source VALUES (1, 'one');`,
				`CREATE MATERIALIZED VIEW matview_refresh_no_data_reader AS
					SELECT id, label FROM matview_refresh_no_data_source;`,
				`INSERT INTO matview_refresh_no_data_source VALUES (2, 'two');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `REFRESH MATERIALIZED VIEW matview_refresh_no_data_reader WITH NO DATA;`,
				},
				{
					Query:       `SELECT id, label FROM matview_refresh_no_data_reader;`,
					ExpectedErr: `has not been populated`,
				},
				{
					Query: `REFRESH MATERIALIZED VIEW matview_refresh_no_data_reader WITH DATA;`,
				},
				{
					Query: `SELECT id, label
						FROM matview_refresh_no_data_reader
						ORDER BY id;`,
					Expected: []sql.Row{{1, "one"}, {2, "two"}},
				},
			},
		},
	})
}
