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

// TestRegexSetReturningFunctions pins regexp_matches and
// regexp_split_to_table workload shapes used by PG views and ETL
// queries to project text into rows. Per the View/query TODO in
// docs/app-compatibility-checklist.md.
func TestRegexSetReturningFunctions(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			// Without the 'g' flag regexp_matches returns at most one
			// row per call (the first match), as a text[] of capture
			// groups (or the whole match if there are no groups).
			Name:        "regexp_matches without 'g' flag",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT (regexp_matches('foo bar baz', '(\w+)'))[1];`, PostgresOracle: ScriptTestPostgresOracle{ID: "regex-srf-test-testregexsetreturningfunctions-0001-select-regexp_matches-foo-bar-baz"},
				},
				{
					Query: `SELECT (regexp_matches('Order #123 placed', '#(\d+)'))[1];`, PostgresOracle: ScriptTestPostgresOracle{ID: "regex-srf-test-testregexsetreturningfunctions-0002-select-regexp_matches-order-#123-placed"},
				},
			},
		},
		{
			// With the 'g' flag regexp_matches returns one row per
			// match. Pin the row count via count(*) so we don't rely
			// on text[] return-shape comparisons in test rows.
			Name:        "regexp_matches with 'g' flag returns one row per match",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*)::text FROM regexp_matches('foo bar baz', '(\w+)', 'g') t;`, PostgresOracle: ScriptTestPostgresOracle{ID: "regex-srf-test-testregexsetreturningfunctions-0003-select-count-*-::text-from"},
				},
				{
					Query: `SELECT count(*)::text FROM regexp_matches('a1 b2 c3 d4', '(\w)(\d)', 'g') t;`, PostgresOracle: ScriptTestPostgresOracle{ID: "regex-srf-test-testregexsetreturningfunctions-0004-select-count-*-::text-from"},
				},
			},
		},
		{
			// Case-insensitive flag.
			Name:        "regexp_matches respects 'i' flag",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*)::text FROM regexp_matches('Foo FOO foo', 'foo', 'gi') t;`, PostgresOracle: ScriptTestPostgresOracle{ID: "regex-srf-test-testregexsetreturningfunctions-0005-select-count-*-::text-from"},
				},
			},
		},
		{
			Name:        "regexp_split_to_table",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT regexp_split_to_table('a,b,c', ',');`, PostgresOracle: ScriptTestPostgresOracle{ID: "regex-srf-test-testregexsetreturningfunctions-0006-select-regexp_split_to_table-a-b-c"},
				},
				{
					Query: `SELECT regexp_split_to_table('one two   three', '\s+');`, PostgresOracle: ScriptTestPostgresOracle{ID: "regex-srf-test-testregexsetreturningfunctions-0007-select-regexp_split_to_table-one-two-three"},
				},
				{
					Query: `SELECT count(*)::text FROM regexp_split_to_table('1, 2, 3, 4, 5', ',\s*') t;`, PostgresOracle: ScriptTestPostgresOracle{ID: "regex-srf-test-testregexsetreturningfunctions-0008-select-count-*-::text-from"},
				},
			},
		},
	})
}
