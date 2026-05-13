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
)

// PostgreSQL supports case-insensitive LIKE pattern matching through ILIKE.
// Doltgres currently rejects the expression as unsupported.
func TestILikePatternMatchRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ILIKE evaluates case-insensitive pattern",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 'Alpha' ILIKE 'a%',
						'Alpha' ILIKE 'ALP_A',
						'Alpha' NOT ILIKE 'b%';`, PostgresOracle: ScriptTestPostgresOracle{ID: "expression-operator-repro-test-testilikepatternmatchrepro-0001-select-alpha-ilike-a%-alpha"},
				},
			},
		},
	})
}

// PostgreSQL supports SQL SIMILAR TO pattern matching. Doltgres currently
// rejects the expression as unsupported.
func TestSimilarToPatternMatchRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SIMILAR TO evaluates SQL regular expression pattern",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 'abc' SIMILAR TO 'a%(b|c)',
						'adc' SIMILAR TO 'a%(b|c)',
						'abx' SIMILAR TO 'a%(b|c)';`, PostgresOracle: ScriptTestPostgresOracle{ID: "expression-operator-repro-test-testsimilartopatternmatchrepro-0001-select-abc-similar-to-a%"},
				},
			},
		},
	})
}

// PostgreSQL supports case-insensitive regular-expression match operators.
// Doltgres currently rejects both ~* and !~* as unsupported.
func TestCaseInsensitiveRegexMatchRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "case-insensitive regex operators evaluate matches",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 'Alpha' ~* '^a',
						'Alpha' ~* 'LPH',
						'Alpha' !~* '^b';`, PostgresOracle: ScriptTestPostgresOracle{ID: "expression-operator-repro-test-testcaseinsensitiveregexmatchrepro-0001-select-alpha-~*-^a-alpha"},
				},
			},
		},
	})
}

// PostgreSQL supports ^ as numeric exponentiation. Doltgres currently rejects
// the expression as unsupported.
func TestPowerOperatorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "power operator evaluates numeric exponentiation",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT (2 ^ 3)::text, (4 ^ 0.5)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "expression-operator-repro-test-testpoweroperatorrepro-0001-select-2-^-3-::text"},
				},
			},
		},
	})
}

// PostgreSQL supports unary numeric root and absolute-value operators.
// Doltgres currently rejects |/, ||/, and @ as unsupported.
func TestUnaryNumericOperatorsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "unary numeric operators evaluate roots and absolute value",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT (|/ 25.0)::text, (||/ 27.0)::text, (@ -5.0)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "expression-operator-repro-test-testunarynumericoperatorsrepro-0001-select-|/-25.0-::text-||/"},
				},
			},
		},
	})
}

// TestUnaryPlusOperatorGuard covers PostgreSQL unary plus as a no-op numeric
// operator.
func TestUnaryPlusOperatorGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "unary plus preserves numeric value",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT (+1)::text, (+(-2))::text, (+'3'::int)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "expression-operator-repro-test-testunaryplusoperatorguard-0001-select-+1-::text-+-2"},
				},
			},
		},
	})
}

// PostgreSQL 16 accepts underscores in numeric literals and non-decimal integer
// literal prefixes. Doltgres currently rejects this parser-level syntax.
func TestPostgres16IntegerLiteralSyntaxRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PostgreSQL integer literal prefixes and underscores",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 1_000::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "expression-operator-repro-test-testpostgres16integerliteralsyntaxrepro-0001-select-1_000::text"},
				},
				{
					Query: `SELECT 0x10::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "expression-operator-repro-test-testpostgres16integerliteralsyntaxrepro-0002-select-0x10::text"},
				},
				{
					Query: `SELECT 0o10::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "expression-operator-repro-test-testpostgres16integerliteralsyntaxrepro-0003-select-0o10::text"},
				},
				{
					Query: `SELECT 0b1010::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "expression-operator-repro-test-testpostgres16integerliteralsyntaxrepro-0004-select-0b1010::text"},
				},
			},
		},
	})
}

// PostgreSQL supports the SQL OVERLAPS operator for temporal periods. Doltgres
// currently rejects the syntax before evaluating the period intersections.
func TestTemporalOverlapsOperatorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "OVERLAPS evaluates period intersection",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT (DATE '2024-01-01', DATE '2024-01-10')
							OVERLAPS (DATE '2024-01-05', DATE '2024-01-20'),
						(DATE '2024-01-01', DATE '2024-01-02')
							OVERLAPS (DATE '2024-01-02', DATE '2024-01-03'),
						(DATE '2024-01-01', INTERVAL '2 days')
							OVERLAPS (DATE '2024-01-02', INTERVAL '1 day');`, PostgresOracle: ScriptTestPostgresOracle{ID: "expression-operator-repro-test-testtemporaloverlapsoperatorrepro-0001-select-date-2024-01-01-date-2024-01-10"},
				},
			},
		},
	})
}
