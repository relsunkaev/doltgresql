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

// TestToDateRejectsOutputOnlyOFPatternRepro reproduces a date formatting
// correctness bug: PostgreSQL rejects the output-only OF timezone pattern in
// to_date(), but Doltgres accepts it and returns a date.
func TestToDateRejectsOutputOnlyOFPatternRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "to_date rejects output-only OF pattern",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT to_date('2011-12-18 +05', 'YYYY-MM-DD OF');`, PostgresOracle: ScriptTestPostgresOracle{ID: "to-date-correctness-repro-test-testtodaterejectsoutputonlyofpatternrepro-0001-select-to_date-2011-12-18-+05-yyyy-mm-dd", Compare: "sqlstate"},
				},
			},
		},
	})
}

// TestToDateIsoWeekdayParsesMondayRepro reproduces a date formatting
// correctness bug: PostgreSQL parses ISO weekday ID=1 as Monday, but Doltgres
// maps it to the wrong day when resolving ISO week dates.
func TestToDateIsoWeekdayParsesMondayRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "to_date ISO weekday parses Monday",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT to_date('2005521', 'IYYYIWID')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "to-date-correctness-repro-test-testtodateisoweekdayparsesmondayrepro-0001-select-to_date-2005521-iyyyiwid-::text"},
				},
			},
		},
	})
}
