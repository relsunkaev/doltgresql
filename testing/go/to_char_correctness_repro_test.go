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
					Query: `SELECT to_char(interval '1.234 seconds', 'HH24:MI:SS.US');`, PostgresOracle: ScriptTestPostgresOracle{ID: "to-char-correctness-repro-test-testtocharintervalpreservesfractionalsecondsrepro-0001-select-to_char-interval-1.234-seconds"},
				},
				{
					Query: `SELECT to_char(interval '1 hour 2 minutes 3.456 seconds', 'HH24:MI:SS.MS.US SSSS');`, PostgresOracle: ScriptTestPostgresOracle{ID: "to-char-correctness-repro-test-testtocharintervalpreservesfractionalsecondsrepro-0002-select-to_char-interval-1-hour"},
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
					Query: `SELECT to_char(timestamp '2021-09-15 21:43:56.123456', 'FF1 FF2 FF3 FF4 FF5 FF6');`, PostgresOracle: ScriptTestPostgresOracle{ID: "to-char-correctness-repro-test-testtocharfractionalsecondprecisiontokensrepro-0001-select-to_char-timestamp-2021-09-15-21:43:56.123456"},
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
						'MONTH Month month MON Mon mon MM DAY Day day DY Dy dy D');`, PostgresOracle: ScriptTestPostgresOracle{ID: "to-char-correctness-repro-test-testtocharfirstmonthandweekdaynamesrepro-0001-select-to_char-timestamp-2021-01-03-12:00:00"},
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
					Query: `SELECT to_char(timestamp '2021-01-11', 'DDTH DDth');`, PostgresOracle: ScriptTestPostgresOracle{ID: "to-char-correctness-repro-test-testtocharordinalsuffixteendatesrepro-0001-select-to_char-timestamp-2021-01-11-ddth"},
				},
				{
					Query: `SELECT to_char(timestamp '2021-01-12', 'DDTH DDth');`, PostgresOracle: ScriptTestPostgresOracle{ID: "to-char-correctness-repro-test-testtocharordinalsuffixteendatesrepro-0002-select-to_char-timestamp-2021-01-12-ddth"},
				},
				{
					Query: `SELECT to_char(timestamp '2021-01-13', 'DDTH DDth');`, PostgresOracle: ScriptTestPostgresOracle{ID: "to-char-correctness-repro-test-testtocharordinalsuffixteendatesrepro-0003-select-to_char-timestamp-2021-01-13-ddth"},
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
					Query: `SELECT to_char(timestamp '2021-03-14 12:00:00', 'YYYY-MM-DD HH24:MI TZ OF TZH:TZM');`, PostgresOracle: ScriptTestPostgresOracle{ID: "to-char-correctness-repro-test-testtochartimezonefieldsmatchpostgressessionzonerepro-0001-select-to_char-timestamp-2021-03-14-12:00:00"},
				},
				{
					Query: `SELECT to_char(timestamptz '2021-03-14 12:00:00+00', 'YYYY-MM-DD HH24:MI TZ OF TZH:TZM');`, PostgresOracle: ScriptTestPostgresOracle{ID: "to-char-correctness-repro-test-testtochartimezonefieldsmatchpostgressessionzonerepro-0002-select-to_char-timestamptz-2021-03-14-12:00:00+00"},
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
					Query: `SELECT to_char(1234.5::numeric, 'FM9,999.00');`, PostgresOracle: ScriptTestPostgresOracle{ID: "to-char-correctness-repro-test-testtocharnumericformatspostgrespatternsrepro-0001-select-to_char-1234.5::numeric-fm9-999.00"},
				},
				{
					Query: `SELECT to_char((-42.5)::numeric, 'S999.9');`, PostgresOracle: ScriptTestPostgresOracle{ID: "to-char-correctness-repro-test-testtocharnumericformatspostgrespatternsrepro-0002-select-to_char-42.5-::numeric-s999.9"},
				},
			},
		},
	})
}
