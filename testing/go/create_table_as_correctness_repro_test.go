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

// TestCreateTableAsWithNoDataDoesNotEvaluateQueryRepro reproduces a CTAS
// correctness bug: CREATE TABLE AS ... WITH NO DATA should create the target
// table from the query shape without evaluating result rows.
func TestCreateTableAsWithNoDataDoesNotEvaluateQueryRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS WITH NO DATA does not evaluate query",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE ctas_no_data_error AS
						SELECT 1 / 0 AS value
						WITH NO DATA;`,
				},
				{
					Query:    `SELECT value FROM ctas_no_data_error;`,
					Expected: []sql.Row{},
				},
			},
		},
	})
}

// TestCreateTableAsWithDataClauseRepro guards PostgreSQL's explicit WITH DATA
// clause, which is equivalent to the default CTAS behavior.
func TestCreateTableAsWithDataClauseRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS WITH DATA evaluates query",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE ctas_with_data AS
						SELECT 42 AS value
						WITH DATA;`,
				},
				{
					Query:    `SELECT value FROM ctas_with_data;`,
					Expected: []sql.Row{{42}},
				},
			},
		},
	})
}

// TestCreateTableAsIfNotExistsDoesNotEvaluateQueryRepro reproduces a CTAS
// correctness bug: once the relation already exists, IF NOT EXISTS should skip
// the replacement query.
func TestCreateTableAsIfNotExistsDoesNotEvaluateQueryRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS IF NOT EXISTS skips replacement query",
			SetUpScript: []string{
				`CREATE TABLE ctas_if_not_exists_error AS SELECT 1 AS value;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE IF NOT EXISTS ctas_if_not_exists_error AS
						SELECT 1 / 0 AS value;`,
				},
				{
					Query:    `SELECT value FROM ctas_if_not_exists_error;`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestCreateTableAsDoesNotCopyDefaultsRepro reproduces a CTAS schema bug: CTAS
// creates regular result columns and must not copy source-table defaults.
func TestCreateTableAsDoesNotCopyDefaultsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS does not copy defaults",
			SetUpScript: []string{
				`CREATE TABLE ctas_default_source (
					id INT,
					label TEXT DEFAULT 'source default'
				);`,
				`INSERT INTO ctas_default_source VALUES (1, 'source row');`,
				`CREATE TABLE ctas_default_copy AS
					SELECT id, label FROM ctas_default_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO ctas_default_copy (id) VALUES (2);`,
				},
				{
					Query: `SELECT id, label FROM ctas_default_copy ORDER BY id;`,
					Expected: []sql.Row{
						{1, "source row"},
						{2, nil},
					},
				},
			},
		},
	})
}

// TestCreateTableAsDoesNotCopyCheckConstraintsRepro guards that CTAS does not
// copy source-table CHECK constraints.
func TestCreateTableAsDoesNotCopyCheckConstraintsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS does not copy check constraints",
			SetUpScript: []string{
				`CREATE TABLE ctas_check_source (
					id INT,
					amount INT CHECK (amount > 0)
				);`,
				`INSERT INTO ctas_check_source VALUES (1, 10);`,
				`CREATE TABLE ctas_check_copy AS
					SELECT id, amount FROM ctas_check_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO ctas_check_copy VALUES (2, -10);`,
				},
				{
					Query: `SELECT id, amount FROM ctas_check_copy ORDER BY id;`,
					Expected: []sql.Row{
						{1, 10},
						{2, -10},
					},
				},
			},
		},
	})
}

// TestCreateTableAsDoesNotCopyUniqueIndexesRepro guards that CTAS does not copy
// source-table indexes or unique constraints.
func TestCreateTableAsDoesNotCopyUniqueIndexesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS does not copy unique indexes",
			SetUpScript: []string{
				`CREATE TABLE ctas_unique_source (
					id INT,
					code INT UNIQUE
				);`,
				`INSERT INTO ctas_unique_source VALUES (1, 7);`,
				`CREATE TABLE ctas_unique_copy AS
					SELECT id, code FROM ctas_unique_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO ctas_unique_copy VALUES (2, 7);`,
				},
				{
					Query: `SELECT id, code FROM ctas_unique_copy ORDER BY id;`,
					Expected: []sql.Row{
						{1, 7},
						{2, 7},
					},
				},
			},
		},
	})
}

// TestCreateTableAsPreservesDomainColumnTypesGuard guards that CTAS preserves
// domain-typed query output columns so future writes to the new table still
// enforce the domain's constraints.
func TestCreateTableAsPreservesDomainColumnTypesGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS preserves domain column types",
			SetUpScript: []string{
				`CREATE DOMAIN ctas_positive_domain AS integer
					CONSTRAINT ctas_positive_domain_check CHECK (VALUE > 0);`,
				`CREATE TABLE ctas_domain_source (
					id INT,
					amount ctas_positive_domain
				);`,
				`INSERT INTO ctas_domain_source VALUES (1, 10);`,
				`CREATE TABLE ctas_domain_copy AS
					SELECT id, amount FROM ctas_domain_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `INSERT INTO ctas_domain_copy VALUES (2, -1);`,
					ExpectedErr: `ctas_positive_domain_check`,
				},
				{
					Query: `SELECT id, amount
						FROM ctas_domain_copy
						ORDER BY id;`,
					Expected: []sql.Row{{1, 10}},
				},
			},
		},
	})
}

// TestCreateTableAsTimetzTypmodMaterializesRoundedValueRepro reproduces a CTAS
// persistence bug: PostgreSQL materializes typmod-constrained timetz query
// output using the rounded value and preserves the output column typmod.
func TestCreateTableAsTimetzTypmodMaterializesRoundedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS timetz typmod materializes rounded value",
			SetUpScript: []string{
				`CREATE TABLE ctas_timetz_typmod_items AS
					SELECT CAST('21:43:56.789+00'::timetz AS TIMETZ(0)) AS tz;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT tz::text
						FROM ctas_timetz_typmod_items;`,
					Expected: []sql.Row{{"21:43:57+00"}},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'ctas_timetz_typmod_items'::regclass
							AND a.attname = 'tz';`,
					Expected: []sql.Row{{"time(0) with time zone"}},
				},
			},
		},
	})
}

// TestCreateTableAsTimeTypmodMaterializesRoundedValueGuard guards that CTAS
// materializes typmod-constrained time query output using the rounded value and
// preserves the output column typmod.
func TestCreateTableAsTimeTypmodMaterializesRoundedValueGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS time typmod materializes rounded value",
			SetUpScript: []string{
				`CREATE TABLE ctas_time_typmod_items AS
					SELECT CAST('21:43:56.789'::time AS TIME(0)) AS t;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT t::text
						FROM ctas_time_typmod_items;`,
					Expected: []sql.Row{{"21:43:57"}},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'ctas_time_typmod_items'::regclass
							AND a.attname = 't';`,
					Expected: []sql.Row{{"time(0) without time zone"}},
				},
			},
		},
	})
}

// TestCreateTableAsNumericTypmodMaterializesRoundedValueGuard guards that CTAS
// materializes typmod-constrained numeric query output using the rounded value
// and preserves the output column typmod.
func TestCreateTableAsNumericTypmodMaterializesRoundedValueGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS numeric typmod materializes rounded value",
			SetUpScript: []string{
				`CREATE TABLE ctas_numeric_typmod_items AS
					SELECT CAST(123.456 AS NUMERIC(5,2)) AS amount;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT amount::text
						FROM ctas_numeric_typmod_items;`,
					Expected: []sql.Row{{"123.46"}},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'ctas_numeric_typmod_items'::regclass
							AND a.attname = 'amount';`,
					Expected: []sql.Row{{"numeric(5,2)"}},
				},
			},
		},
	})
}

// TestCreateTableAsTimestampTypmodMaterializesRoundedValueRepro reproduces a
// CTAS persistence bug: PostgreSQL materializes typmod-constrained timestamp
// query output using the rounded value and preserves the output column typmod.
func TestCreateTableAsTimestampTypmodMaterializesRoundedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS timestamp typmod materializes rounded value",
			SetUpScript: []string{
				`CREATE TABLE ctas_timestamp_typmod_items AS
					SELECT CAST(TIMESTAMP '2021-09-15 21:43:56.789' AS TIMESTAMP(0)) AS ts;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ts::text
						FROM ctas_timestamp_typmod_items;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57"}},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'ctas_timestamp_typmod_items'::regclass
							AND a.attname = 'ts';`,
					Expected: []sql.Row{{"timestamp(0) without time zone"}},
				},
			},
		},
	})
}

// TestCreateTableAsTimestamptzTypmodMaterializesRoundedValueRepro reproduces a
// CTAS persistence bug: PostgreSQL materializes typmod-constrained timestamptz
// query output using the rounded value and preserves the output column typmod.
func TestCreateTableAsTimestamptzTypmodMaterializesRoundedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS timestamptz typmod materializes rounded value",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
				`CREATE TABLE ctas_timestamptz_typmod_items AS
					SELECT CAST(TIMESTAMPTZ '2021-09-15 21:43:56.789+00' AS TIMESTAMPTZ(0)) AS ts;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ts::text
						FROM ctas_timestamptz_typmod_items;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57+00"}},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'ctas_timestamptz_typmod_items'::regclass
							AND a.attname = 'ts';`,
					Expected: []sql.Row{{"timestamp(0) with time zone"}},
				},
			},
		},
	})
}

// TestCreateTableAsIntervalTypmodMaterializesRestrictedValueRepro reproduces a
// CTAS persistence bug: PostgreSQL materializes typmod-constrained interval
// query output using the restricted value and preserves the output column typmod.
func TestCreateTableAsIntervalTypmodMaterializesRestrictedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS interval typmod materializes restricted value",
			SetUpScript: []string{
				`CREATE TABLE ctas_interval_typmod_items AS
					SELECT CAST(INTERVAL '3 days 04:05:06.789' AS INTERVAL DAY TO SECOND(0)) AS ds;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ds::text
						FROM ctas_interval_typmod_items;`,
					Expected: []sql.Row{{"3 days 04:05:07"}},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'ctas_interval_typmod_items'::regclass
							AND a.attname = 'ds';`,
					Expected: []sql.Row{{"interval day to second(0)"}},
				},
			},
		},
	})
}

// TestCreateTableAsVarcharTypmodMaterializesTruncatedValueGuard guards that
// CTAS materializes typmod-constrained varchar output using the truncated value
// and preserves the output column typmod.
func TestCreateTableAsVarcharTypmodMaterializesTruncatedValueGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS varchar typmod materializes truncated value",
			SetUpScript: []string{
				`CREATE TABLE ctas_varchar_typmod_items AS
					SELECT CAST('abcd' AS VARCHAR(3)) AS label;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT label
						FROM ctas_varchar_typmod_items;`,
					Expected: []sql.Row{{"abc"}},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'ctas_varchar_typmod_items'::regclass
							AND a.attname = 'label';`,
					Expected: []sql.Row{{"character varying(3)"}},
				},
			},
		},
	})
}

// TestCreateTableAsCharacterTypmodMaterializesPaddedValueRepro reproduces a
// CTAS persistence bug: PostgreSQL materializes typmod-constrained character
// query output using the padded value and preserves the output column typmod.
func TestCreateTableAsCharacterTypmodMaterializesPaddedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS character typmod materializes padded value",
			SetUpScript: []string{
				`CREATE TABLE ctas_character_typmod_items AS
					SELECT CAST('ab' AS CHARACTER(3)) AS label;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT octet_length(label)
						FROM ctas_character_typmod_items;`,
					Expected: []sql.Row{{3}},
				},
				{
					Query: `SELECT format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						WHERE a.attrelid = 'ctas_character_typmod_items'::regclass
							AND a.attname = 'label';`,
					Expected: []sql.Row{{"character(3)"}},
				},
			},
		},
	})
}

// TestCreateTableAsExplicitColumnNamesRepro reproduces a CTAS column-list bug:
// an explicit column list should rename the query output columns.
func TestCreateTableAsExplicitColumnNamesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE AS explicit column names rename output",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE ctas_explicit_names (new_id, new_label) AS
						SELECT 1 AS old_id, 'one'::text AS old_label;`,
				},
				{
					Query: `SELECT column_name
						FROM information_schema.columns
						WHERE table_schema = 'public'
							AND table_name = 'ctas_explicit_names'
						ORDER BY ordinal_position;`,
					Expected: []sql.Row{{"new_id"}, {"new_label"}},
				},
				{
					Query:    `SELECT new_id, new_label FROM ctas_explicit_names;`,
					Expected: []sql.Row{{1, "one"}},
				},
			},
		},
	})
}
