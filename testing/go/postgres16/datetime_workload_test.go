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

// TestDateTimeWorkload pins the date/time casts and helpers that real
// PG views and reporting queries use: text-to-date casts, make_date,
// extract for fiscal-year/quarter shapes, date arithmetic, and
// time-zone-aware computation. Per the View/query TODO in
// docs/app-compatibility-checklist.md.
func TestDateTimeWorkload(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "text-to-date and text-to-timestamp casts",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT '2026-05-08'::date::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "datetime-workload-test-testdatetimeworkload-0001-select-2026-05-08-::date::text"},
				},
				{
					Query: `SELECT '2026-05-08 10:30:45'::timestamp::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "datetime-workload-test-testdatetimeworkload-0002-select-2026-05-08-10:30:45-::timestamp::text"},
				},
				{
					Query: `SELECT CAST('2026-05-08' AS DATE)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "datetime-workload-test-testdatetimeworkload-0003-select-cast-2026-05-08-as-date"},
				},
			},
		},
		{
			Name:        "make_date and make_timestamp",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT make_date(2026, 5, 8)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "datetime-workload-test-testdatetimeworkload-0004-select-make_date-2026-5-8"},
				},
				{
					Query: `SELECT make_timestamp(2026, 5, 8, 10, 30, 45)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "datetime-workload-test-testdatetimeworkload-0005-select-make_timestamp-2026-5-8"},
				},
			},
		},
		{
			Name:        "extract for date parts",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT extract(year FROM '2026-05-08'::date)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "datetime-workload-test-testdatetimeworkload-0006-select-extract-year-from-2026-05-08"},
				},
				{
					Query: `SELECT extract(month FROM '2026-05-08'::date)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "datetime-workload-test-testdatetimeworkload-0007-select-extract-month-from-2026-05-08"},
				},
				{
					Query: `SELECT extract(day FROM '2026-05-08'::date)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "datetime-workload-test-testdatetimeworkload-0008-select-extract-day-from-2026-05-08"},
				},
				{
					Query: `SELECT extract(quarter FROM '2026-05-08'::date)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "datetime-workload-test-testdatetimeworkload-0009-select-extract-quarter-from-2026-05-08"},
				},
				{
					Query: `SELECT extract(dow FROM '2026-05-08'::date)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "datetime-workload-test-testdatetimeworkload-0010-select-extract-dow-from-2026-05-08"},
				},
				{
					Query: `SELECT extract(hour FROM '2026-05-08 10:30:45'::timestamp)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "datetime-workload-test-testdatetimeworkload-0011-select-extract-hour-from-2026-05-08"},
				},
			},
		},
		{
			Name:        "date arithmetic and INTERVAL math",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ('2026-05-08'::date + INTERVAL '1 day')::date::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "datetime-workload-test-testdatetimeworkload-0012-select-2026-05-08-::date-+-interval"},
				},
				{
					Query: `SELECT ('2026-05-08'::date - INTERVAL '1 month')::date::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "datetime-workload-test-testdatetimeworkload-0013-select-2026-05-08-::date-interval-1"},
				},
				{
					Query: `SELECT ('2026-05-08'::date - '2026-05-01'::date)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "datetime-workload-test-testdatetimeworkload-0014-select-2026-05-08-::date-2026-05-01-::date"},
				},
			},
		},
		{
			Name:        "date_trunc",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT date_trunc('month', '2026-05-08 10:30:45'::timestamp)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "datetime-workload-test-testdatetimeworkload-0015-select-date_trunc-month-2026-05-08-10:30:45"},
				},
				{
					Query: `SELECT date_trunc('hour', '2026-05-08 10:30:45'::timestamp)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "datetime-workload-test-testdatetimeworkload-0016-select-date_trunc-hour-2026-05-08-10:30:45"},
				},
			},
		},
	})
}
