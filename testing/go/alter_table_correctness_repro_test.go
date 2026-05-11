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

// TestAlterTableIfExistsMissingTableNoopsGuard guards that ALTER TABLE IF
// EXISTS skips missing-table errors for the whole command.
func TestAlterTableIfExistsMissingTableNoopsGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE IF EXISTS no-ops on missing table",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `ALTER TABLE IF EXISTS alter_if_exists_missing_table ADD PRIMARY KEY (id);`,
					Expected: []sql.Row{},
				},
			},
		},
	})
}

// TestAlterTableIfExistsMissingTableDropColumnNoopsGuard guards that a missing
// table with ALTER TABLE IF EXISTS no-ops before resolving subcommands such as
// DROP COLUMN.
func TestAlterTableIfExistsMissingTableDropColumnNoopsGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE IF EXISTS DROP COLUMN no-ops on missing table",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `ALTER TABLE IF EXISTS alter_if_exists_missing_drop_column DROP COLUMN id;`,
					Expected: []sql.Row{},
				},
			},
		},
	})
}

// TestAlterTableDropColumnIfExistsMissingColumnNoops guards that DROP COLUMN
// IF EXISTS skips missing-column errors and leaves existing rows intact.
func TestAlterTableDropColumnIfExistsMissingColumnNoops(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP COLUMN IF EXISTS no-ops on missing column",
			SetUpScript: []string{
				`CREATE TABLE drop_missing_column_if_exists_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO drop_missing_column_if_exists_items VALUES (1, 'kept');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `ALTER TABLE drop_missing_column_if_exists_items DROP COLUMN IF EXISTS missing_label;`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT id, label FROM drop_missing_column_if_exists_items;`,
					Expected: []sql.Row{{1, "kept"}},
				},
			},
		},
	})
}

// TestAlterTableDropColumnIfExistsExistingColumnDrops guards that DROP COLUMN
// IF EXISTS still drops the column when it actually exists, leaving the rest
// of the schema and surviving rows intact.
func TestAlterTableDropColumnIfExistsExistingColumnDrops(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP COLUMN IF EXISTS drops the column when it exists",
			SetUpScript: []string{
				`CREATE TABLE drop_existing_column_if_exists_items (
					id INT PRIMARY KEY,
					label TEXT,
					note TEXT
				);`,
				`INSERT INTO drop_existing_column_if_exists_items VALUES (1, 'kept', 'gone');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `ALTER TABLE drop_existing_column_if_exists_items DROP COLUMN IF EXISTS note;`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT id, label FROM drop_existing_column_if_exists_items;`,
					Expected: []sql.Row{{1, "kept"}},
				},
				{
					Query:       `SELECT note FROM drop_existing_column_if_exists_items;`,
					ExpectedErr: `column "note" could not be found`,
				},
			},
		},
	})
}

// TestAlterTableDropColumnIfExistsEmitsNotice guards that the missing-column
// no-op path emits the PostgreSQL-style "column ... does not exist, skipping"
// NOTICE so that client tools can surface the message exactly as Postgres would.
func TestAlterTableDropColumnIfExistsEmitsNotice(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP COLUMN IF EXISTS emits a NOTICE on missing column",
			SetUpScript: []string{
				`CREATE TABLE drop_if_exists_notice_items (
					id INT PRIMARY KEY
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `ALTER TABLE drop_if_exists_notice_items DROP COLUMN IF EXISTS missing_label;`,
					Expected: []sql.Row{},
					ExpectedNotices: []ExpectedNotice{
						{
							Severity: "NOTICE",
							Message:  `column "missing_label" of relation "drop_if_exists_notice_items" does not exist, skipping`,
						},
					},
				},
			},
		},
	})
}

// TestAlterTableDropColumnIfExistsMultiClause guards that mixing DROP COLUMN
// IF EXISTS (missing) with other clauses in the same ALTER TABLE statement
// still resolves correctly: existing clauses execute and the missing-column
// clause silently no-ops.
func TestAlterTableDropColumnIfExistsMultiClause(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE applies surviving clauses when DROP COLUMN IF EXISTS targets a missing column",
			SetUpScript: []string{
				`CREATE TABLE drop_if_exists_multi_clause_items (
					id INT PRIMARY KEY,
					keep_me TEXT,
					drop_me TEXT
				);`,
				`INSERT INTO drop_if_exists_multi_clause_items VALUES (1, 'kept', 'gone');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE drop_if_exists_multi_clause_items
								DROP COLUMN IF EXISTS missing_col,
								DROP COLUMN drop_me,
								ADD COLUMN added_col INT;`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT id, keep_me, added_col FROM drop_if_exists_multi_clause_items;`,
					Expected: []sql.Row{{1, "kept", nil}},
				},
				{
					Query:       `SELECT drop_me FROM drop_if_exists_multi_clause_items;`,
					ExpectedErr: `column "drop_me" could not be found`,
				},
			},
		},
	})
}

// TestAlterTableDropColumnWithoutIfExistsStillErrors guards that the regular
// (non-IF EXISTS) form continues to error when the column is missing.
func TestAlterTableDropColumnWithoutIfExistsStillErrors(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP COLUMN without IF EXISTS still errors on missing column",
			SetUpScript: []string{
				`CREATE TABLE drop_missing_strict_items (id INT PRIMARY KEY);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER TABLE drop_missing_strict_items DROP COLUMN missing_col;`,
					ExpectedErr: "missing_col",
				},
			},
		},
	})
}

// TestAlterTableDropColumnIfExistsCaseInsensitiveMatch guards that the
// existence check uses PostgreSQL's case-folding rules: an unquoted column
// reference resolves case-insensitively, so DROP COLUMN IF EXISTS still drops
// an existing column regardless of case.
func TestAlterTableDropColumnIfExistsCaseInsensitiveMatch(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP COLUMN IF EXISTS folds unquoted identifiers to lowercase",
			SetUpScript: []string{
				`CREATE TABLE drop_if_exists_case_items (
					id INT PRIMARY KEY,
					mixedcasecol TEXT
				);`,
				`INSERT INTO drop_if_exists_case_items VALUES (1, 'kept');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `ALTER TABLE drop_if_exists_case_items DROP COLUMN IF EXISTS MixedCaseCol;`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT id FROM drop_if_exists_case_items;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:       `SELECT mixedcasecol FROM drop_if_exists_case_items;`,
					ExpectedErr: `mixedcasecol`,
				},
			},
		},
	})
}

// TestAlterColumnTypeUsingConvertsExistingRowsRepro reproduces a persistence
// correctness gap: PostgreSQL uses the USING expression to convert stored
// values while changing a column's type.
func TestAlterColumnTypeUsingConvertsExistingRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER COLUMN TYPE USING converts existing rows",
			SetUpScript: []string{
				`CREATE TABLE alter_type_using_items (
					id INT PRIMARY KEY,
					amount_text TEXT
				);`,
				`INSERT INTO alter_type_using_items VALUES
					(1, '10'),
					(2, '25');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_type_using_items
						ALTER COLUMN amount_text TYPE INT
						USING amount_text::INT;`,
				},
				{
					Query: `SELECT id, amount_text, pg_typeof(amount_text)::text
						FROM alter_type_using_items
						ORDER BY id;`,
					Expected: []sql.Row{
						{1, 10, "integer"},
						{2, 25, "integer"},
					},
				},
			},
		},
	})
}

// TestAlterTableSameTypeRejectsRowTypeDependentsRepro reproduces a dependency
// correctness bug: PostgreSQL rejects ALTER COLUMN TYPE on a table whose row
// type is used by another table, even when the requested type is the same.
func TestAlterTableSameTypeRejectsRowTypeDependentsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER COLUMN TYPE rejects row type dependents",
			SetUpScript: []string{
				`CREATE TABLE row_type_parent (
					a INT,
					b TEXT
				);`,
				`CREATE TABLE row_type_child (
					id INT PRIMARY KEY,
					parent_row row_type_parent
				);`,
				`INSERT INTO row_type_child VALUES (1, ROW(1, 'one'));`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE row_type_parent
						ALTER COLUMN a SET DATA TYPE INT;`,
					ExpectedErr: `cannot alter table "row_type_parent" because column "row_type_child.parent_row" uses its row type`,
				},
			},
		},
	})
}

// TestAlterColumnTypeAppliesTypmodsToExistingRowsRepro reproduces an ALTER
// TABLE persistence bug: PostgreSQL rewrites existing rows through the new
// column typmod and rejects rows that overflow the new typmod.
func TestAlterColumnTypeAppliesTypmodsToExistingRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER COLUMN TYPE applies typmods to existing rows",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
				`CREATE TABLE alter_type_typmod_items (
					id INT PRIMARY KEY,
					amount NUMERIC,
					created_at TIMESTAMP
				);`,
				`INSERT INTO alter_type_typmod_items VALUES
					(1, 123.456, '2021-09-15 21:43:56.789');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_type_typmod_items
						ALTER COLUMN amount TYPE NUMERIC(5,2),
						ALTER COLUMN created_at TYPE TIMESTAMP(0);`,
				},
				{
					Query: `SELECT amount::text, created_at::text
						FROM alter_type_typmod_items
						ORDER BY id;`,
					Expected: []sql.Row{{"123.46", "2021-09-15 21:43:57"}},
				},
			},
		},
		{
			Name: "ALTER COLUMN TYPE rejects typmod overflow",
			SetUpScript: []string{
				`CREATE TABLE alter_type_typmod_overflow_items (
					id INT PRIMARY KEY,
					amount NUMERIC
				);`,
				`INSERT INTO alter_type_typmod_overflow_items VALUES (1, 999.995);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_type_typmod_overflow_items
						ALTER COLUMN amount TYPE NUMERIC(5,2);`,
					ExpectedErr: `numeric field overflow`,
				},
				{
					Query: `SELECT amount::text
						FROM alter_type_typmod_overflow_items
						ORDER BY id;`,
					Expected: []sql.Row{{"999.995"}},
				},
			},
		},
	})
}

// TestAlterColumnTypeVarcharRejectsTypmodOverflowRepro reproduces an ALTER
// TABLE persistence bug: PostgreSQL rejects an ALTER COLUMN TYPE rewrite when
// an existing value is too long for the target varchar(n) typmod.
func TestAlterColumnTypeVarcharRejectsTypmodOverflowRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER COLUMN TYPE varchar rejects existing overflow",
			SetUpScript: []string{
				`CREATE TABLE alter_type_varchar_overflow_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO alter_type_varchar_overflow_items VALUES (1, 'abcd');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_type_varchar_overflow_items
						ALTER COLUMN label TYPE VARCHAR(3);`,
					ExpectedErr: `too long`,
				},
				{
					Query: `SELECT id, label, pg_typeof(label)::text
						FROM alter_type_varchar_overflow_items;`,
					Expected: []sql.Row{{1, "abcd", "text"}},
				},
			},
		},
	})
}

// TestAlterColumnTypeAppliesTimetzTypmodToExistingRowsRepro reproduces an
// ALTER TABLE persistence bug: PostgreSQL rewrites existing timetz values
// through the new column typmod.
func TestAlterColumnTypeAppliesTimetzTypmodToExistingRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER COLUMN TYPE applies timetz typmod to existing rows",
			SetUpScript: []string{
				`CREATE TABLE alter_type_timetz_typmod_items (
					id INT PRIMARY KEY,
					tz TIMETZ
				);`,
				`INSERT INTO alter_type_timetz_typmod_items VALUES
					(1, '21:43:56.789+00'::timetz);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_type_timetz_typmod_items
						ALTER COLUMN tz TYPE TIMETZ(0);`,
				},
				{
					Query: `SELECT tz::text
						FROM alter_type_timetz_typmod_items
						ORDER BY id;`,
					Expected: []sql.Row{{"21:43:57+00"}},
				},
			},
		},
	})
}

// TestAlterColumnTypeAppliesTimeTypmodToExistingRowsRepro reproduces an ALTER
// TABLE persistence bug: PostgreSQL rewrites existing time values through the
// new column typmod.
func TestAlterColumnTypeAppliesTimeTypmodToExistingRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER COLUMN TYPE applies time typmod to existing rows",
			SetUpScript: []string{
				`CREATE TABLE alter_type_time_typmod_items (
					id INT PRIMARY KEY,
					tm TIME
				);`,
				`INSERT INTO alter_type_time_typmod_items VALUES
					(1, '21:43:56.789'::time);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_type_time_typmod_items
						ALTER COLUMN tm TYPE TIME(0);`,
				},
				{
					Query: `SELECT tm::text
						FROM alter_type_time_typmod_items
						ORDER BY id;`,
					Expected: []sql.Row{{"21:43:57"}},
				},
			},
		},
	})
}

// TestAlterColumnTypeAppliesTimestamptzTypmodToExistingRowsRepro reproduces an
// ALTER TABLE persistence bug: PostgreSQL rewrites existing timestamptz values
// through the new column typmod.
func TestAlterColumnTypeAppliesTimestamptzTypmodToExistingRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER COLUMN TYPE applies timestamptz typmod to existing rows",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
				`CREATE TABLE alter_type_timestamptz_typmod_items (
					id INT PRIMARY KEY,
					tz TIMESTAMPTZ
				);`,
				`INSERT INTO alter_type_timestamptz_typmod_items VALUES
					(1, '2021-09-15 21:43:56.789+00'::timestamptz);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_type_timestamptz_typmod_items
						ALTER COLUMN tz TYPE TIMESTAMPTZ(0);`,
				},
				{
					Query: `SELECT tz::text
						FROM alter_type_timestamptz_typmod_items
						ORDER BY id;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57+00"}},
				},
			},
		},
	})
}

// TestAlterColumnTypeAppliesIntervalTypmodToExistingRowsRepro reproduces an
// ALTER TABLE persistence bug: PostgreSQL rewrites existing interval values
// through the new column typmod.
func TestAlterColumnTypeAppliesIntervalTypmodToExistingRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER COLUMN TYPE applies interval typmod to existing rows",
			SetUpScript: []string{
				`CREATE TABLE alter_type_interval_typmod_items (
					id INT PRIMARY KEY,
					ds INTERVAL
				);`,
				`INSERT INTO alter_type_interval_typmod_items VALUES
					(1, '3 days 04:05:06.789'::interval);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_type_interval_typmod_items
						ALTER COLUMN ds TYPE INTERVAL DAY TO SECOND(0);`,
				},
				{
					Query: `SELECT ds::text
						FROM alter_type_interval_typmod_items
						ORDER BY id;`,
					Expected: []sql.Row{{"3 days 04:05:07"}},
				},
			},
		},
	})
}

// TestAlterTableAddTimetzTypmodColumnDefaultBackfillsRoundedValueRepro
// reproduces an ALTER TABLE persistence bug: PostgreSQL backfills existing rows
// through the new timetz column typmod when ADD COLUMN includes a default.
func TestAlterTableAddTimetzTypmodColumnDefaultBackfillsRoundedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ADD timetz typmod column default backfills rounded value",
			SetUpScript: []string{
				`CREATE TABLE alter_add_timetz_typmod_default_items (
					id INT PRIMARY KEY
				);`,
				`INSERT INTO alter_add_timetz_typmod_default_items VALUES (1), (2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_add_timetz_typmod_default_items
						ADD COLUMN tz TIMETZ(0) DEFAULT '21:43:56.789+00'::timetz;`,
				},
				{
					Query: `SELECT tz::text, count(*)
						FROM alter_add_timetz_typmod_default_items
						GROUP BY tz::text;`,
					Expected: []sql.Row{{"21:43:57+00", 2}},
				},
			},
		},
	})
}

// TestAlterTableAddTimeTypmodColumnDefaultBackfillsRoundedValueGuard guards that
// PostgreSQL-compatible ADD COLUMN backfills existing rows through the new time
// column typmod when the column includes a default.
func TestAlterTableAddTimeTypmodColumnDefaultBackfillsRoundedValueGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ADD time typmod column default backfills rounded value",
			SetUpScript: []string{
				`CREATE TABLE alter_add_time_typmod_default_items (
					id INT PRIMARY KEY
				);`,
				`INSERT INTO alter_add_time_typmod_default_items VALUES (1), (2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_add_time_typmod_default_items
						ADD COLUMN tm TIME(0) DEFAULT '21:43:56.789'::time;`,
				},
				{
					Query: `SELECT tm::text, count(*)
						FROM alter_add_time_typmod_default_items
						GROUP BY tm::text;`,
					Expected: []sql.Row{{"21:43:57", 2}},
				},
			},
		},
	})
}

// TestAlterTableAddTimestampTypmodColumnDefaultBackfillsRoundedValueRepro
// reproduces an ALTER TABLE persistence bug: PostgreSQL backfills existing rows
// through the new timestamp column typmod when ADD COLUMN includes a default.
func TestAlterTableAddTimestampTypmodColumnDefaultBackfillsRoundedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ADD timestamp typmod column default backfills rounded value",
			SetUpScript: []string{
				`CREATE TABLE alter_add_timestamp_typmod_default_items (
					id INT PRIMARY KEY
				);`,
				`INSERT INTO alter_add_timestamp_typmod_default_items VALUES (1), (2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_add_timestamp_typmod_default_items
						ADD COLUMN ts TIMESTAMP(0) DEFAULT '2021-09-15 21:43:56.789'::timestamp;`,
				},
				{
					Query: `SELECT ts::text, count(*)
						FROM alter_add_timestamp_typmod_default_items
						GROUP BY ts::text;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57", 2}},
				},
			},
		},
	})
}

// TestAlterTableAddTimestamptzTypmodColumnDefaultBackfillsRoundedValueRepro
// reproduces an ALTER TABLE persistence bug: PostgreSQL backfills existing rows
// through the new timestamptz column typmod when ADD COLUMN includes a default.
func TestAlterTableAddTimestamptzTypmodColumnDefaultBackfillsRoundedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ADD timestamptz typmod column default backfills rounded value",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
				`CREATE TABLE alter_add_timestamptz_typmod_default_items (
					id INT PRIMARY KEY
				);`,
				`INSERT INTO alter_add_timestamptz_typmod_default_items VALUES (1), (2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_add_timestamptz_typmod_default_items
						ADD COLUMN tz TIMESTAMPTZ(0) DEFAULT '2021-09-15 21:43:56.789+00'::timestamptz;`,
				},
				{
					Query: `SELECT tz::text, count(*)
						FROM alter_add_timestamptz_typmod_default_items
						GROUP BY tz::text;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57+00", 2}},
				},
			},
		},
	})
}

// TestAlterTableAddIntervalTypmodColumnDefaultBackfillsRestrictedValueRepro
// reproduces an ALTER TABLE persistence bug: PostgreSQL backfills existing rows
// through the new interval column typmod when ADD COLUMN includes a default.
func TestAlterTableAddIntervalTypmodColumnDefaultBackfillsRestrictedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ADD interval typmod column default backfills restricted value",
			SetUpScript: []string{
				`CREATE TABLE alter_add_interval_typmod_default_items (
					id INT PRIMARY KEY
				);`,
				`INSERT INTO alter_add_interval_typmod_default_items VALUES (1), (2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_add_interval_typmod_default_items
						ADD COLUMN ds INTERVAL DAY TO SECOND(0) DEFAULT '3 days 04:05:06.789'::interval;`,
				},
				{
					Query: `SELECT ds::text, count(*)
						FROM alter_add_interval_typmod_default_items
						GROUP BY ds::text;`,
					Expected: []sql.Row{{"3 days 04:05:07", 2}},
				},
			},
		},
	})
}

// TestAlterTableReloptionsPersistRepro reproduces a catalog persistence gap:
// PostgreSQL persists table reloptions changed with ALTER TABLE ... SET (...).
func TestAlterTableReloptionsPersistRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE reloptions persist in pg_class",
			SetUpScript: []string{
				`CREATE TABLE alter_table_reloptions_items (id INT);`,
				`ALTER TABLE alter_table_reloptions_items
					SET (fillfactor=40, autovacuum_enabled=false);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT CAST(reloptions AS TEXT)
						FROM pg_catalog.pg_class
						WHERE oid = 'alter_table_reloptions_items'::regclass;`,
					Expected: []sql.Row{{"{fillfactor=40,autovacuum_enabled=false}"}},
				},
			},
		},
	})
}

// TestAlterTableSetDefaultTablespace guards that ALTER TABLE SET TABLESPACE
// pg_default succeeds as a no-op, matching PostgreSQL's behavior for the only
// tablespace Doltgres exposes.
func TestAlterTableSetDefaultTablespace(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE SET TABLESPACE pg_default",
			SetUpScript: []string{
				`CREATE TABLE alter_table_default_tablespace_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_table_default_tablespace_items
						SET TABLESPACE pg_default;`,
				},
				{
					Query: `INSERT INTO alter_table_default_tablespace_items
						VALUES (1, 'ok');`,
				},
				{
					Query: `SELECT id, label
						FROM alter_table_default_tablespace_items;`,
					Expected: []sql.Row{{1, "ok"}},
				},
			},
		},
	})
}

// TestAlterTableSetTablespaceUnknownErrors guards that an ALTER TABLE SET
// TABLESPACE targeting a tablespace that does not exist returns the same
// "tablespace ... does not exist" error PostgreSQL produces, so migration
// tools see a real failure rather than a silent no-op.
func TestAlterTableSetTablespaceUnknownErrors(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SET TABLESPACE rejects unknown tablespace",
			SetUpScript: []string{
				`CREATE TABLE alter_table_unknown_tablespace_items (
					id INT PRIMARY KEY
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_table_unknown_tablespace_items
						SET TABLESPACE custom_space;`,
					ExpectedErr: `tablespace "custom_space" does not exist`,
				},
			},
		},
	})
}

// TestAlterTableSetHeapAccessMethod guards that ALTER TABLE SET ACCESS METHOD
// heap succeeds as a no-op, matching PostgreSQL's default table access method.
func TestAlterTableSetHeapAccessMethod(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE SET ACCESS METHOD heap",
			SetUpScript: []string{
				`CREATE TABLE alter_table_access_method_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_table_access_method_items
						SET ACCESS METHOD heap;`,
				},
				{
					Query: `INSERT INTO alter_table_access_method_items
						VALUES (1, 'ok');`,
				},
				{
					Query: `SELECT id, label
						FROM alter_table_access_method_items;`,
					Expected: []sql.Row{{1, "ok"}},
				},
			},
		},
	})
}

// TestAlterTableSetAccessMethodUnknownErrors guards that an ALTER TABLE SET
// ACCESS METHOD targeting a non-heap access method returns PostgreSQL's
// "access method ... does not exist" error rather than a silent no-op.
func TestAlterTableSetAccessMethodUnknownErrors(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SET ACCESS METHOD rejects non-heap target",
			SetUpScript: []string{
				`CREATE TABLE alter_table_unknown_access_method_items (
					id INT PRIMARY KEY
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE alter_table_unknown_access_method_items
						SET ACCESS METHOD btree;`,
					ExpectedErr: `access method "btree" does not exist`,
				},
			},
		},
	})
}
