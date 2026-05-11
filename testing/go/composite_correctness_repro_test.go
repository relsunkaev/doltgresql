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

// TestCompositeAttributeTypmodsRoundStoredValuesRepro reproduces a composite
// storage correctness bug: PostgreSQL applies attribute typmods when composite
// values are assigned to columns.
func TestCompositeAttributeTypmodsRoundStoredValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "composite attribute typmods round stored values",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
				`CREATE TYPE composite_typmod_pair AS (
					amount NUMERIC(5,2),
					ts TIMESTAMP(0)
				);`,
				`CREATE TABLE composite_typmod_items (
					id INT PRIMARY KEY,
					item composite_typmod_pair
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO composite_typmod_items VALUES
						(1, ROW(123.456, '2021-09-15 21:43:56.789')::composite_typmod_pair);`,
				},
				{
					Query: `SELECT (item).amount::text, (item).ts::text
						FROM composite_typmod_items
						ORDER BY id;`,
					Expected: []sql.Row{{"123.46", "2021-09-15 21:43:57"}},
				},
				{
					Query: `INSERT INTO composite_typmod_items VALUES
						(2, ROW(999.995, '2021-09-15 21:43:56.789')::composite_typmod_pair);`,
					ExpectedErr: `numeric field overflow`,
				},
			},
		},
	})
}

// TestCompositeTimetzAttributeTypmodsRoundStoredValuesRepro reproduces a
// composite storage correctness bug: PostgreSQL applies timetz attribute
// typmods when composite values are assigned to columns.
func TestCompositeTimetzAttributeTypmodsRoundStoredValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "composite timetz attribute typmods round stored values",
			SetUpScript: []string{
				`CREATE TYPE composite_timetz_typmod_pair AS (
					tz TIMETZ(0)
				);`,
				`CREATE TABLE composite_timetz_typmod_items (
					id INT PRIMARY KEY,
					item composite_timetz_typmod_pair
				);`,
				`INSERT INTO composite_timetz_typmod_items VALUES
					(1, ROW('21:43:56.789+00'::timetz)::composite_timetz_typmod_pair);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT (item).tz::text
						FROM composite_timetz_typmod_items
						ORDER BY id;`,
					Expected: []sql.Row{{"21:43:57+00"}},
				},
			},
		},
	})
}

// TestCompositeArrayColumnRoundTripsValuesRepro reproduces a composite-array
// persistence bug: PostgreSQL stores arrays of composite values and allows field
// access on subscripting results.
func TestCompositeArrayColumnRoundTripsValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "composite array column round trips values",
			SetUpScript: []string{
				`CREATE TYPE composite_array_line AS (
					sku TEXT,
					qty INT
				);`,
				`CREATE TABLE composite_array_orders (
					id INT PRIMARY KEY,
					lines composite_array_line[]
				);`,
				`INSERT INTO composite_array_orders VALUES (
					1,
					ARRAY[
						ROW('abc', 2)::composite_array_line,
						ROW('def', 3)::composite_array_line
					]
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, lines
						FROM composite_array_orders;`,
					Expected: []sql.Row{{1, "{\"(abc,2)\",\"(def,3)\"}"}},
				},
				{
					Query: `SELECT (lines[2]).sku, (lines[2]).qty
						FROM composite_array_orders;`,
					Expected: []sql.Row{{"def", 3}},
				},
			},
		},
	})
}

// TestAlterCompositeTypeAddAttributeRepro reproduces a composite-type
// evolution gap: PostgreSQL can add attributes to an existing composite type.
func TestAlterCompositeTypeAddAttributeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TYPE ADD ATTRIBUTE updates composite shape",
			SetUpScript: []string{
				`CREATE TYPE mutable_composite_add AS (a INT);`,
				`ALTER TYPE mutable_composite_add ADD ATTRIBUTE b TEXT;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT ROW(1, 'x')::mutable_composite_add::text;`,
					Expected: []sql.Row{{"(1,x)"}},
				},
			},
		},
	})
}

// TestAlterCompositeTypeDropAttributeRepro reproduces a composite-type
// evolution gap: PostgreSQL can drop attributes from an existing composite
// type.
func TestAlterCompositeTypeDropAttributeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TYPE DROP ATTRIBUTE updates composite shape",
			SetUpScript: []string{
				`CREATE TYPE mutable_composite_drop AS (a INT, b TEXT);`,
				`ALTER TYPE mutable_composite_drop DROP ATTRIBUTE b;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT ROW(1)::mutable_composite_drop::text;`,
					Expected: []sql.Row{{"(1)"}},
				},
			},
		},
	})
}
