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

// TestToTimestampRejectsOutputOnlyOFPatternRepro reproduces a timestamp
// formatting correctness bug: PostgreSQL rejects the output-only OF timezone
// pattern in to_timestamp(), but Doltgres accepts it as an input offset.
func TestToTimestampRejectsOutputOnlyOFPatternRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "to_timestamp rejects output-only OF pattern",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT to_timestamp('2011-12-18 11:38 +05', 'YYYY-MM-DD HH12:MI OF');`, PostgresOracle: ScriptTestPostgresOracle{ID: "to-timestamp-correctness-repro-test-testtotimestamprejectsoutputonlyofpatternrepro-0001-select-to_timestamp-2011-12-18-11:38-+05", Compare: "sqlstate"},
				},
			},
		},
	})
}

// TestToTimestampIsoWeekdayParsesMondayRepro reproduces a timestamp formatting
// correctness bug: PostgreSQL parses ISO weekday ID=1 as Monday, but Doltgres
// maps it to the wrong day when resolving ISO week dates.
func TestToTimestampIsoWeekdayParsesMondayRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "to_timestamp ISO weekday parses Monday",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT to_timestamp('2005521', 'IYYYIWID')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "to-timestamp-correctness-repro-test-testtotimestampisoweekdayparsesmondayrepro-0001-select-to_timestamp-2005521-iyyyiwid-::text"},
				},
			},
		},
	})
}
