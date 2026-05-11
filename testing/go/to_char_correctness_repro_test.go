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

// TestToCharIntervalPreservesFractionalSecondsRepro reproduces an interval
// formatting correctness bug: PostgreSQL formats interval seconds from
// nanoseconds and preserves the fractional remainder, but Doltgres treats the
// nanosecond count as microseconds.
func TestToCharIntervalPreservesFractionalSecondsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "to_char interval preserves fractional seconds",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT to_char(interval '1.234 seconds', 'HH24:MI:SS.US');`,
					Expected: []sql.Row{{"00:00:01.234000"}},
				},
				{
					Query:    `SELECT to_char(interval '1 hour 2 minutes 3.456 seconds', 'HH24:MI:SS.MS.US SSSS');`,
					Expected: []sql.Row{{"01:02:03.456.456000 3723"}},
				},
			},
		},
	})
}

// TestToCharFractionalSecondPrecisionTokensRepro reproduces a timestamp
// formatting correctness bug: PostgreSQL FF1..FF6 tokens emit progressively
// more fractional-second digits, but Doltgres derives several of them from the
// millisecond component.
func TestToCharFractionalSecondPrecisionTokensRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "to_char fractional second precision tokens",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT to_char(timestamp '2021-09-15 21:43:56.123456', 'FF1 FF2 FF3 FF4 FF5 FF6');`,
					Expected: []sql.Row{{"1 12 123 1234 12345 123456"}},
				},
			},
		},
	})
}

// TestToCharFirstMonthAndWeekdayNamesRepro reproduces a timestamp formatting
// correctness bug: PostgreSQL formats January and Sunday names normally, but
// Doltgres drops the first entry of the month and weekday name arrays.
func TestToCharFirstMonthAndWeekdayNamesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "to_char first month and weekday names",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT to_char(timestamp '2021-01-03 12:00:00',
						'MONTH Month month MON Mon mon MM DAY Day day DY Dy dy D');`,
					Expected: []sql.Row{{"JANUARY   January   january   JAN Jan jan 01 SUNDAY    Sunday    sunday    SUN Sun sun 1"}},
				},
			},
		},
	})
}

// TestToCharOrdinalSuffixTeenDatesRepro reproduces a formatting correctness
// bug: PostgreSQL uses TH/th for 11, 12, and 13, but Doltgres chooses suffixes
// solely from the last digit.
func TestToCharOrdinalSuffixTeenDatesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "to_char ordinal suffix teen dates",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT to_char(timestamp '2021-01-11', 'DDTH DDth');`,
					Expected: []sql.Row{{"11TH 11th"}},
				},
				{
					Query:    `SELECT to_char(timestamp '2021-01-12', 'DDTH DDth');`,
					Expected: []sql.Row{{"12TH 12th"}},
				},
				{
					Query:    `SELECT to_char(timestamp '2021-01-13', 'DDTH DDth');`,
					Expected: []sql.Row{{"13TH 13th"}},
				},
			},
		},
	})
}

// TestToCharTimezoneFieldsMatchPostgresSessionZoneRepro reproduces timestamp
// formatting correctness bugs: PostgreSQL's timestamp-without-time-zone
// formatter leaves timezone fields empty/zero, and its timestamptz formatter
// emits the active zone abbreviation. Doltgres instead applies the named
// location to plain timestamps and prints the location name for TZ.
func TestToCharTimezoneFieldsMatchPostgresSessionZoneRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "to_char timezone fields match PostgreSQL session zone semantics",
			SetUpScript: []string{
				`SET TIME ZONE 'America/New_York';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT to_char(timestamp '2021-03-14 12:00:00', 'YYYY-MM-DD HH24:MI TZ OF TZH:TZM');`,
					Expected: []sql.Row{{"2021-03-14 12:00  +00 +00:00"}},
				},
				{
					Query:    `SELECT to_char(timestamptz '2021-03-14 12:00:00+00', 'YYYY-MM-DD HH24:MI TZ OF TZH:TZM');`,
					Expected: []sql.Row{{"2021-03-14 08:00 EDT -04 -04:00"}},
				},
			},
		},
	})
}

// TestToCharNumericFormatsPostgresPatternsRepro reproduces a numeric formatting
// correctness bug: PostgreSQL supports to_char(numeric, text), but Doltgres
// registers the function and returns a not-supported error.
func TestToCharNumericFormatsPostgresPatternsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "to_char numeric formats PostgreSQL patterns",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT to_char(1234.5::numeric, 'FM9,999.00');`,
					Expected: []sql.Row{{"1,234.50"}},
				},
				{
					Query:    `SELECT to_char((-42.5)::numeric, 'S999.9');`,
					Expected: []sql.Row{{" -42.5"}},
				},
			},
		},
	})
}
