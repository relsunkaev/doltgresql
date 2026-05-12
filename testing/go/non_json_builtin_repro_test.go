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

// TestIntegerBaseFormattingBuiltinsRepro reproduces a PostgreSQL compatibility
// gap: PostgreSQL exposes to_bin and to_oct for base-formatting integers.
func TestIntegerBaseFormattingBuiltinsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "to_bin and to_oct format integers",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT to_bin(10);`,
					Expected: []sql.Row{{"1010"}},
				},
				{
					Query:    `SELECT to_oct(10);`,
					Expected: []sql.Row{{"12"}},
				},
			},
		},
	})
}

// TestUuidExtractionBuiltinsRepro reproduces a PostgreSQL compatibility gap:
// PostgreSQL exposes built-ins for extracting version and timestamp metadata
// from UUID values.
func TestUuidExtractionBuiltinsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "uuid_extract_version and uuid_extract_timestamp inspect UUIDs",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT uuid_extract_version('41db1265-8bc1-4ab3-992f-885799a4af1d'::uuid)::text;`,
					Expected: []sql.Row{{"4"}},
				},
				{
					Query:    `SELECT uuid_extract_timestamp('41db1265-8bc1-4ab3-992f-885799a4af1d'::uuid) IS NULL;`,
					Expected: []sql.Row{{true}},
				},
			},
		},
	})
}

// TestTypeMetadataBuiltinsRepro reproduces PostgreSQL compatibility gaps:
// PostgreSQL exposes pg_basetype for domain base-type lookup and to_regtypemod
// for parsing type modifiers from textual type specifications.
func TestTypeMetadataBuiltinsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_basetype and to_regtypemod expose type metadata",
			SetUpScript: []string{
				`CREATE DOMAIN non_json_builtin_domain AS text;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT pg_basetype('non_json_builtin_domain'::regtype)::text;`,
					Expected: []sql.Row{{"text"}},
				},
				{
					Query:    `SELECT format_type(to_regtype('varchar(32)'), to_regtypemod('varchar(32)'));`,
					Expected: []sql.Row{{"character varying(32)"}},
				},
			},
		},
	})
}

// TestUnicodeInformationBuiltinsRepro reproduces PostgreSQL compatibility gaps:
// PostgreSQL exposes Unicode metadata helpers for checking assigned codepoints
// and reporting the built-in Unicode data version.
func TestUnicodeInformationBuiltinsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "unicode_assigned and unicode_version report Unicode metadata",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT unicode_assigned('abc');`,
					Expected: []sql.Row{{true}},
				},
				{
					Query:    `SELECT unicode_version() IS NOT NULL;`,
					Expected: []sql.Row{{true}},
				},
			},
		},
	})
}

// TestPostgres16InputValidationBuiltinsRepro reproduces PostgreSQL 16
// compatibility gaps: pg_input_is_valid and pg_input_error_info should expose
// soft input-validation checks.
func TestPostgres16InputValidationBuiltinsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_input_is_valid and pg_input_error_info report soft input errors",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT pg_input_is_valid('42', 'integer'), pg_input_is_valid('42000000000', 'integer');`,
					Expected: []sql.Row{{true, false}},
				},
				{
					Query:    `SELECT (pg_input_error_info('42000000000', 'integer')).sql_error_code;`,
					Expected: []sql.Row{{"22003"}},
				},
			},
		},
	})
}

// TestPostgres16ArrayRandomBuiltinsRepro reproduces PostgreSQL 16
// compatibility gaps: array_sample and array_shuffle should randomly sample
// or shuffle the first array dimension.
func TestPostgres16ArrayRandomBuiltinsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "array_sample and array_shuffle preserve requested cardinality",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT array_length(array_sample(ARRAY[1,2,3,4], 2), 1);`,
					Expected: []sql.Row{{2}},
				},
				{
					Query:    `SELECT array_length(array_shuffle(ARRAY[1,2,3,4]), 1);`,
					Expected: []sql.Row{{4}},
				},
			},
		},
	})
}

// TestPostgres16DateAndRandomBuiltinsRepro reproduces PostgreSQL 16
// compatibility gaps: date_add, date_subtract, and random_normal should be
// available as built-in functions.
func TestPostgres16DateAndRandomBuiltinsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "date_add, date_subtract, and random_normal evaluate",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT date_add(
						'2024-01-01 00:00:00+00'::timestamptz,
						'1 day'::interval,
						'UTC') = '2024-01-02 00:00:00+00'::timestamptz;`,
					Expected: []sql.Row{{true}},
				},
				{
					Query: `SELECT date_subtract(
						'2024-01-02 00:00:00+00'::timestamptz,
						'1 day'::interval,
						'UTC') = '2024-01-01 00:00:00+00'::timestamptz;`,
					Expected: []sql.Row{{true}},
				},
				{
					Query:    `SELECT random_normal(0.0, 1.0) IS NOT NULL;`,
					Expected: []sql.Row{{true}},
				},
			},
		},
	})
}

// TestPostgres16AnyValueAndSystemUserRepro reproduces PostgreSQL 16
// compatibility gaps: ANY_VALUE should be available as an aggregate and
// SYSTEM_USER should expose the authenticated identity.
func TestPostgres16AnyValueAndSystemUserRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ANY_VALUE and SYSTEM_USER evaluate",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT any_value(v) FROM (VALUES (NULL::text), ('picked')) AS t(v);`,
					Expected: []sql.Row{{"picked"}},
				},
				{
					Query:    `SELECT system_user IS NOT NULL;`,
					Expected: []sql.Row{{true}},
				},
			},
		},
	})
}

// TestPostgres18UuidAndMathBuiltinsRepro reproduces PostgreSQL 18
// compatibility gaps: uuidv7, gamma, and lgamma should be available as
// built-in functions.
func TestPostgres18UuidAndMathBuiltinsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "uuidv7, gamma, and lgamma evaluate",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT uuidv7() IS NOT NULL;`,
					Expected: []sql.Row{{true}},
				},
				{
					Query:    `SELECT gamma(6::double precision)::text;`,
					Expected: []sql.Row{{"120"}},
				},
				{
					Query:    `SELECT lgamma(6::double precision) > 4.7 AND lgamma(6::double precision) < 4.8;`,
					Expected: []sql.Row{{true}},
				},
			},
		},
	})
}

// TestPostgres18ArrayAndStringBuiltinsRepro reproduces PostgreSQL 18
// compatibility gaps: array_sort, array_reverse, and casefold should be
// available as built-in functions.
func TestPostgres18ArrayAndStringBuiltinsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "array_sort, array_reverse, and casefold evaluate",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT array_to_string(array_sort(ARRAY[3,1,2]), ',');`,
					Expected: []sql.Row{{"1,2,3"}},
				},
				{
					Query:    `SELECT array_to_string(array_reverse(ARRAY[1,2,3]), ',');`,
					Expected: []sql.Row{{"3,2,1"}},
				},
				{
					Query:    `SELECT casefold('HELLO');`,
					Expected: []sql.Row{{"hello"}},
				},
			},
		},
	})
}

// TestPostgres18BinaryStringBuiltinsRepro reproduces PostgreSQL 18
// compatibility gaps: bytea CRC helpers, bytea reverse, and integer-bytea
// casts should be available.
func TestPostgres18BinaryStringBuiltinsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "bytea CRC helpers, reverse, and integer casts evaluate",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT crc32('abc'::bytea)::text, crc32c('abc'::bytea)::text;`,
					Expected: []sql.Row{{"891568578", "910901175"}},
				},
				{
					Query:    `SELECT encode(reverse('\xabcd'::bytea), 'hex');`,
					Expected: []sql.Row{{"cdab"}},
				},
				{
					Query:    `SELECT encode(1234::integer::bytea, 'hex');`,
					Expected: []sql.Row{{"000004d2"}},
				},
				{
					Query:    `SELECT '\x8000'::bytea::smallint::text;`,
					Expected: []sql.Row{{"-32768"}},
				},
			},
		},
	})
}
