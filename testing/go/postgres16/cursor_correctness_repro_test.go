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

// TestCursorWithHoldSurvivesCommitRepro reproduces a cursor state persistence
// bug: a WITH HOLD cursor should survive COMMIT and continue fetching from its
// materialized result.
func TestCursorWithHoldSurvivesCommitRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "WITH HOLD cursor survives commit",
			SetUpScript: []string{
				`CREATE TABLE hold_cursor_items (id INT);`,
				`INSERT INTO hold_cursor_items VALUES (1), (2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:            `BEGIN;`,
					SkipResultsCheck: true,
				},
				{
					Query: `DECLARE hold_cur CURSOR WITH HOLD FOR
						SELECT id FROM hold_cursor_items ORDER BY id;`,
					SkipResultsCheck: true,
				},
				{
					Query: `FETCH NEXT FROM hold_cur;`, PostgresOracle: ScriptTestPostgresOracle{ID: "cursor-correctness-repro-test-testcursorwithholdsurvivescommitrepro-0001-fetch-next-from-hold_cur"},
				},
				{
					Query:            `COMMIT;`,
					SkipResultsCheck: true,
				},
				{
					Query: `FETCH NEXT FROM hold_cur;`, PostgresOracle: ScriptTestPostgresOracle{ID: "cursor-correctness-repro-test-testcursorwithholdsurvivescommitrepro-0002-fetch-next-from-hold_cur"},
				},
				{
					Query:            `CLOSE hold_cur;`,
					SkipResultsCheck: true,
				},
			},
		},
	})
}
