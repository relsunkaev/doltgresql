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

package postgres18

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"
)

// TestPostgres18UuidAndMathBuiltinsRepro reproduces PostgreSQL 18
// compatibility gaps: uuidv7, gamma, and lgamma should be available as
// built-in functions.
func TestPostgres18UuidAndMathBuiltinsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "uuidv7, gamma, and lgamma evaluate",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT uuidv7() IS NOT NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/non-json-builtin-repro-test-testpostgres18uuidandmathbuiltinsrepro-0001-select-uuidv7-is-not-null"},
				},
				{
					Query: `SELECT gamma(6::double precision)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/non-json-builtin-repro-test-testpostgres18uuidandmathbuiltinsrepro-0002-select-gamma-6::double-precision-::text"},
				},
				{
					Query: `SELECT lgamma(6::double precision) > 4.7 AND lgamma(6::double precision) < 4.8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/non-json-builtin-repro-test-testpostgres18uuidandmathbuiltinsrepro-0003-select-lgamma-6::double-precision->"},
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
					Query: `SELECT array_to_string(array_sort(ARRAY[3,1,2]), ',');`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/non-json-builtin-repro-test-testpostgres18arrayandstringbuiltinsrepro-0001-select-array_to_string-array_sort-array[3-1"},
				},
				{
					Query: `SELECT array_to_string(array_reverse(ARRAY[1,2,3]), ',');`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/non-json-builtin-repro-test-testpostgres18arrayandstringbuiltinsrepro-0002-select-array_to_string-array_reverse-array[1-2"},
				},
				{
					Query: `SELECT casefold('HELLO');`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/non-json-builtin-repro-test-testpostgres18arrayandstringbuiltinsrepro-0003-select-casefold-hello"},
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
					Query: `SELECT crc32('abc'::bytea)::text, crc32c('abc'::bytea)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/non-json-builtin-repro-test-testpostgres18binarystringbuiltinsrepro-0001-select-crc32-abc-::bytea-::text"},
				},
				{
					Query: `SELECT encode(reverse('\xabcd'::bytea), 'hex');`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/non-json-builtin-repro-test-testpostgres18binarystringbuiltinsrepro-0002-select-encode-reverse-\\xabcd-::bytea"},
				},
				{
					Query: `SELECT encode(1234::integer::bytea, 'hex');`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/non-json-builtin-repro-test-testpostgres18binarystringbuiltinsrepro-0003-select-encode-1234::integer::bytea-hex"},
				},
				{
					Query: `SELECT '\x8000'::bytea::smallint::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/non-json-builtin-repro-test-testpostgres18binarystringbuiltinsrepro-0004-select-\\x8000-::bytea::smallint::text"},
				},
			},
		},
	})
}
