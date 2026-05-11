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
	"github.com/stretchr/testify/require"
)

// TestTimestampMinusIntervalSubtractsDayComponentsRepro reproduces a timestamp
// arithmetic correctness bug: PostgreSQL subtracts interval day components from
// timestamps, but Doltgres ignores them.
func TestTimestampMinusIntervalSubtractsDayComponentsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamp minus interval subtracts day components",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT (timestamp '2025-07-21 04:05:06' - interval '2 days')::text;`,
					Expected: []sql.Row{{"2025-07-19 04:05:06"}},
				},
			},
		},
	})
}

// TestTimestampMinusIntervalSubtractsMonthComponentsRepro reproduces a
// timestamp arithmetic correctness bug: PostgreSQL subtracts interval month
// components with calendar semantics, but Doltgres ignores them.
func TestTimestampMinusIntervalSubtractsMonthComponentsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamp minus interval subtracts month components",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT (timestamp '2025-03-31 12:00:00' - interval '1 month')::text;`,
					Expected: []sql.Row{{"2025-02-28 12:00:00"}},
				},
			},
		},
	})
}

// TestTimestampPlusIntervalUsesCalendarMonthsRepro reproduces a timestamp
// arithmetic correctness bug: PostgreSQL applies month intervals using calendar
// month semantics rather than fixed thirty-day durations.
func TestTimestampPlusIntervalUsesCalendarMonthsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamp plus interval uses calendar month semantics",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT (timestamp '2025-01-31 12:00:00' + interval '1 month')::text;`,
					Expected: []sql.Row{{"2025-02-28 12:00:00"}},
				},
			},
		},
	})
}

// TestTimestamptzPlusIntervalUsesCalendarMonthsRepro reproduces a timestamptz
// arithmetic correctness bug: PostgreSQL applies month intervals using calendar
// month semantics rather than fixed thirty-day durations.
func TestTimestamptzPlusIntervalUsesCalendarMonthsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamptz plus interval uses calendar month semantics",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT extract(epoch FROM
						(timestamp with time zone '2025-01-31 12:00:00+00' + interval '1 month'))::bigint::text;`,
					Expected: []sql.Row{{"1740744000"}},
				},
			},
		},
	})
}

// TestDatePlusIntervalUsesCalendarMonthsRepro reproduces a date arithmetic
// correctness bug: PostgreSQL applies month intervals to dates using calendar
// month semantics rather than fixed thirty-day durations.
func TestDatePlusIntervalUsesCalendarMonthsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "date plus interval uses calendar month semantics",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT (date '2025-01-31' + interval '1 month')::text;`,
					Expected: []sql.Row{{"2025-02-28 00:00:00"}},
				},
			},
		},
	})
}

// TestIntervalPlusDateUsesCalendarMonthsRepro reproduces a date arithmetic
// correctness bug: the commuted interval-plus-date operator should use
// calendar month semantics rather than fixed thirty-day durations.
func TestIntervalPlusDateUsesCalendarMonthsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "interval plus date uses calendar month semantics",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT (interval '1 month' + date '2025-01-31')::text;`,
					Expected: []sql.Row{{"2025-02-28 00:00:00"}},
				},
			},
		},
	})
}

// TestIntervalPlusTimestampUsesCalendarMonthsRepro reproduces a timestamp
// arithmetic correctness bug: the commuted interval-plus-timestamp operator
// should use calendar month semantics rather than fixed thirty-day durations.
func TestIntervalPlusTimestampUsesCalendarMonthsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "interval plus timestamp uses calendar month semantics",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT (interval '1 month' + timestamp '2025-01-31 12:00:00')::text;`,
					Expected: []sql.Row{{"2025-02-28 12:00:00"}},
				},
			},
		},
	})
}

// TestIntervalPlusTimestamptzUsesCalendarMonthsRepro reproduces a timestamptz
// arithmetic correctness bug: the commuted interval-plus-timestamptz operator
// should use calendar month semantics rather than fixed thirty-day durations.
func TestIntervalPlusTimestamptzUsesCalendarMonthsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "interval plus timestamptz uses calendar month semantics",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT extract(epoch FROM
						(interval '1 month' + timestamp with time zone '2025-01-31 12:00:00+00'))::bigint::text;`,
					Expected: []sql.Row{{"1740744000"}},
				},
			},
		},
	})
}

// TestDateMinusIntervalUsesCalendarMonthsRepro reproduces a date arithmetic
// correctness bug: PostgreSQL subtracts month intervals from dates using
// calendar month semantics rather than fixed thirty-day durations.
func TestDateMinusIntervalUsesCalendarMonthsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "date minus interval uses calendar month semantics",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT (date '2025-03-31' - interval '1 month')::text;`,
					Expected: []sql.Row{{"2025-02-28 00:00:00"}},
				},
			},
		},
	})
}

// TestTimestamptzMinusIntervalSubtractsDayComponentsRepro reproduces a
// timestamptz arithmetic correctness bug: PostgreSQL subtracts interval day
// components from timestamps with time zone, but Doltgres ignores them.
func TestTimestamptzMinusIntervalSubtractsDayComponentsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamptz minus interval subtracts day components",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT extract(epoch FROM
						(timestamp with time zone '2025-07-21 04:05:06+00' - interval '2 days'))::bigint::text;`,
					Expected: []sql.Row{{"1752897906"}},
				},
			},
		},
	})
}

// TestTimestamptzMinusIntervalSubtractsMonthComponentsRepro reproduces a
// timestamptz arithmetic correctness bug: PostgreSQL subtracts interval month
// components from timestamps with time zone, but Doltgres ignores them.
func TestTimestamptzMinusIntervalSubtractsMonthComponentsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamptz minus interval subtracts month components",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT extract(epoch FROM
						(timestamp with time zone '2025-03-31 12:00:00+00' - interval '1 month'))::bigint::text;`,
					Expected: []sql.Row{{"1740744000"}},
				},
			},
		},
	})
}

// TestExtractTimestampJulianIncludesFractionalDayRepro reproduces a timestamp
// extraction correctness bug: PostgreSQL includes the fractional day in Julian
// values for timestamps, but Doltgres returns only the date's Julian day.
func TestExtractTimestampJulianIncludesFractionalDayRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "extract timestamp julian includes fractional day",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT extract(julian FROM timestamp '2001-02-18 20:38:40')::text;`,
					Expected: []sql.Row{{"2451959.86018518518518518519"}},
				},
				{
					Query:    `SELECT extract(julian FROM timestamptz '2001-02-18 20:38:40+00')::text;`,
					Expected: []sql.Row{{"2451959.86018518518518518519"}},
				},
			},
		},
	})
}

// TestDatePartTimestampJulianIncludesFractionalDayRepro reproduces a timestamp
// date_part correctness bug: PostgreSQL includes the fractional day in Julian
// values for timestamps, but Doltgres returns only the date's Julian day.
func TestDatePartTimestampJulianIncludesFractionalDayRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "date_part timestamp julian includes fractional day",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT date_part('julian', timestamp '2001-02-18 20:38:40')::text;`,
					Expected: []sql.Row{{"2451959.860185185"}},
				},
				{
					Query:    `SELECT date_part('julian', timestamptz '2001-02-18 20:38:40+00')::text;`,
					Expected: []sql.Row{{"2451959.860185185"}},
				},
			},
		},
	})
}

// TestDateTruncTimestamptzNamedZoneUsesTruncatedOffsetRepro reproduces a
// timezone correctness bug: PostgreSQL truncates a timestamptz in the named
// zone and uses the offset that applies at the truncated wall time, but
// Doltgres keeps the offset from the input instant.
func TestDateTruncTimestamptzNamedZoneUsesTruncatedOffsetRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "date_trunc timestamptz named zone uses truncated wall time offset",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT extract(epoch FROM
						date_trunc('day', timestamptz '2021-03-14 12:00:00+00', 'America/New_York'))::bigint::text;`,
					Expected: []sql.Row{{"1615698000"}},
				},
				{
					Query: `SELECT extract(epoch FROM
						date_trunc('day', timestamptz '2021-11-07 12:00:00+00', 'America/New_York'))::bigint::text;`,
					Expected: []sql.Row{{"1636257600"}},
				},
			},
		},
	})
}

// TestDateBinUsesPostgresTimestampRangeRepro reproduces a timestamp
// correctness bug: PostgreSQL date_bin works across the supported timestamp
// range, but Doltgres bins using Unix nanoseconds and overflows outside the
// Go nanosecond epoch window.
func TestDateBinUsesPostgresTimestampRangeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "date_bin supports PostgreSQL timestamp range",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT date_bin(
						'1 day'::interval,
						timestamp '1500-01-02 12:00:00',
						timestamp '1500-01-01 00:00:00')::text;`,
					Expected: []sql.Row{{"1500-01-02 00:00:00"}},
				},
				{
					Query: `SELECT date_bin(
						'5 min'::interval,
						timestamp '2300-01-01 00:06:00',
						timestamp '2300-01-01 00:00:00')::text;`,
					Expected: []sql.Row{{"2300-01-01 00:05:00"}},
				},
				{
					Query: `SELECT date_bin(
						'1 day'::interval,
						timestamptz '1500-01-02 12:00:00+00',
						timestamptz '1500-01-01 00:00:00+00')::text;`,
					Expected: []sql.Row{{"1500-01-02 00:00:00+00"}},
				},
				{
					Query: `SELECT date_bin(
						'5 min'::interval,
						timestamptz '2300-01-01 00:06:00+00',
						timestamptz '2300-01-01 00:00:00+00')::text;`,
					Expected: []sql.Row{{"2300-01-01 00:05:00+00"}},
				},
			},
		},
	})
}

// TestAgeUsesCalendarMonthBorrowingRepro reproduces an interval correctness
// bug: PostgreSQL age() borrows days using calendar month lengths, but
// Doltgres uses a fixed 30-day borrow.
func TestAgeUsesCalendarMonthBorrowingRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "age uses calendar month borrowing",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT age(timestamp '2001-02-28', timestamp '2001-01-31')::text;`,
					Expected: []sql.Row{{"28 days"}},
				},
				{
					Query:    `SELECT age(timestamp '2004-03-01', timestamp '2004-01-31')::text;`,
					Expected: []sql.Row{{"1 mon 1 day"}},
				},
			},
		},
	})
}

// TestTimezoneTextTimestamptzUsesTargetOffsetRepro reproduces a timestamptz
// conversion correctness bug: PostgreSQL converts a timestamptz to local time
// in the requested zone, but Doltgres applies the target offset with the wrong
// sign.
func TestTimezoneTextTimestamptzUsesTargetOffsetRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timezone text timestamptz uses target offset",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT timezone(
						'America/New_York',
						timestamptz '2021-03-14 12:00:00+00')::text;`,
					Expected: []sql.Row{{"2021-03-14 08:00:00"}},
				},
				{
					Query: `SELECT (timestamptz '2021-03-14 12:00:00+00'
						AT TIME ZONE 'America/New_York')::text;`,
					Expected: []sql.Row{{"2021-03-14 08:00:00"}},
				},
				{
					Query: `SELECT timezone(
						'-04:45',
						timestamptz '2001-02-16 20:38:40.12-05')::text;`,
					Expected: []sql.Row{{"2001-02-17 06:23:40.12"}},
				},
			},
		},
	})
}

// TestTimezoneTextTimestampUsesWallTimeOffsetRepro reproduces a timestamptz
// conversion correctness bug: PostgreSQL interprets timestamp AT TIME ZONE as
// local wall time in the named zone, but Doltgres chooses the zone offset for
// the same timestamp treated as a UTC instant.
func TestTimezoneTextTimestampUsesWallTimeOffsetRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timezone text timestamp uses wall time offset",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT extract(epoch FROM
						(timestamp '2021-03-14 03:30:00' AT TIME ZONE 'America/New_York'))::bigint::text;`,
					Expected: []sql.Row{{"1615707000"}},
				},
				{
					Query: `SELECT extract(epoch FROM
						timezone('America/New_York', timestamp '2021-03-14 03:30:00'))::bigint::text;`,
					Expected: []sql.Row{{"1615707000"}},
				},
				{
					Query: `SELECT extract(epoch FROM
						(timestamp '2021-11-07 03:30:00' AT TIME ZONE 'America/New_York'))::bigint::text;`,
					Expected: []sql.Row{{"1636273800"}},
				},
			},
		},
	})
}

// TestMakeDateTimestampRejectInvalidCalendarDateRepro reproduces a
// correctness bug: PostgreSQL rejects impossible calendar dates, but Doltgres
// lets Go normalize them to a different date.
func TestMakeDateTimestampRejectInvalidCalendarDateRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "date constructors reject invalid calendar dates",
			Assertions: []ScriptTestAssertion{
				{
					Query:       `SELECT make_date(2021, 2, 29);`,
					ExpectedErr: `date field value out of range`,
				},
				{
					Query:       `SELECT make_timestamp(2021, 2, 29, 0, 0, 0);`,
					ExpectedErr: `date field value out of range`,
				},
				{
					Query:       `SELECT make_timestamptz(2021, 2, 29, 0, 0, 0, 'UTC');`,
					ExpectedErr: `date field value out of range`,
				},
			},
		},
	})
}

// TestToTimestampFloatSupportsPostgresRangeAndInfinityRepro reproduces a
// timestamptz correctness bug: PostgreSQL to_timestamp(float8) supports
// timestamps beyond Go's UnixNano range and maps float infinities to timestamp
// infinities, but Doltgres rejects or NULLs those values.
func TestToTimestampFloatSupportsPostgresRangeAndInfinityRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "to_timestamp float supports PostgreSQL range and infinity",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT to_timestamp(10413792000)::text;`,
					Expected: []sql.Row{{"2300-01-01 00:00:00+00"}},
				},
				{
					Query:    `SELECT to_timestamp('Infinity'::float8)::text;`,
					Expected: []sql.Row{{"infinity"}},
				},
				{
					Query:    `SELECT to_timestamp('-Infinity'::float8)::text;`,
					Expected: []sql.Row{{"-infinity"}},
				},
			},
		},
	})
}

// TestTemporalTypmodsRoundFractionalSecondsGuard protects timestamp/time typmod
// precision behavior: PostgreSQL applies typmod precision by rounding
// fractional seconds, and Doltgres currently matches that behavior.
func TestTemporalTypmodsRoundFractionalSecondsGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "temporal typmods round fractional seconds",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT timestamp(0) '2021-09-15 21:43:56.789'::text;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57"}},
				},
				{
					Query:    `SELECT timestamp(2) '2021-09-15 21:43:56.789'::text;`,
					Expected: []sql.Row{{"2021-09-15 21:43:56.79"}},
				},
				{
					Query:    `SELECT time(0) '21:43:56.789'::text;`,
					Expected: []sql.Row{{"21:43:57"}},
				},
				{
					Query:    `SELECT timetz(0) '21:43:56.789+00'::text;`,
					Expected: []sql.Row{{"21:43:57+00"}},
				},
				{
					Query:    `SELECT timetz(2) '21:43:56.789+00'::text;`,
					Expected: []sql.Row{{"21:43:56.79+00"}},
				},
				{
					Query:    `SELECT timestamptz(0) '2021-09-15 21:43:56.789+00'::text;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57+00"}},
				},
			},
		},
	})
}

// TestTimestampColumnTypmodsRoundStoredFractionalSecondsRepro reproduces a
// timestamp storage correctness bug: PostgreSQL applies typmod precision to
// stored timestamp and timestamptz values, not just literal casts.
func TestTimestampColumnTypmodsRoundStoredFractionalSecondsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamp column typmods round stored fractional seconds",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
				`CREATE TABLE timestamp_column_typmod_items (
					id INT PRIMARY KEY,
					ts TIMESTAMP(0),
					ts2 TIMESTAMP(2),
					tz TIMESTAMPTZ(0)
				);`,
				`INSERT INTO timestamp_column_typmod_items VALUES
					(1,
						'2021-09-15 21:43:56.789',
						'2021-09-15 21:43:56.789',
						'2021-09-15 21:43:56.789+00');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ts::text, ts2::text, tz::text
						FROM timestamp_column_typmod_items
						ORDER BY id;`,
					Expected: []sql.Row{{
						"2021-09-15 21:43:57",
						"2021-09-15 21:43:56.79",
						"2021-09-15 21:43:57+00",
					}},
				},
			},
		},
	})
}

// TestTimestampTypmodDefaultRoundsStoredValueRepro reproduces a timestamp
// storage correctness bug: PostgreSQL applies timestamp typmods when a column
// default is used for an inserted row.
func TestTimestampTypmodDefaultRoundsStoredValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamp typmod default rounds stored value",
			SetUpScript: []string{
				`CREATE TABLE timestamp_typmod_default_items (
					id INT PRIMARY KEY,
					ts TIMESTAMP(0) DEFAULT '2021-09-15 21:43:56.789'
				);`,
				`INSERT INTO timestamp_typmod_default_items (id) VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ts::text
						FROM timestamp_typmod_default_items
						ORDER BY id;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57"}},
				},
			},
		},
	})
}

// TestTimestampTypmodCopyFromRoundsStoredValueRepro reproduces a timestamp
// storage correctness bug: PostgreSQL applies timestamp typmods to values
// loaded through COPY FROM STDIN.
func TestTimestampTypmodCopyFromRoundsStoredValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamp typmod COPY FROM rounds stored value",
			SetUpScript: []string{
				`CREATE TABLE timestamp_typmod_copy_items (
					id INT PRIMARY KEY,
					ts TIMESTAMP(0)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:             `COPY timestamp_typmod_copy_items (id, ts) FROM STDIN;`,
					CopyFromStdInFile: "timestamp-typmod-copy.tsv",
				},
				{
					Query: `SELECT ts::text
						FROM timestamp_typmod_copy_items
						ORDER BY id;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57"}},
				},
			},
		},
	})
}

// TestTimestampTypmodUpdateRoundsStoredValueRepro reproduces a timestamp
// storage correctness bug: PostgreSQL applies timestamp typmods when UPDATE
// assigns a new value to a typmod-constrained timestamp column.
func TestTimestampTypmodUpdateRoundsStoredValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamp typmod UPDATE rounds stored value",
			SetUpScript: []string{
				`CREATE TABLE timestamp_typmod_update_items (
					id INT PRIMARY KEY,
					ts TIMESTAMP(0)
				);`,
				`INSERT INTO timestamp_typmod_update_items VALUES
					(1, '2021-09-15 21:43:55');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE timestamp_typmod_update_items
						SET ts = '2021-09-15 21:43:56.789'
						WHERE id = 1;`,
				},
				{
					Query: `SELECT ts::text
						FROM timestamp_typmod_update_items
						ORDER BY id;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57"}},
				},
			},
		},
	})
}

// TestTimestampTypmodOnConflictUpdateRoundsStoredValueRepro reproduces a
// timestamp storage correctness bug: PostgreSQL applies timestamp typmods on
// the ON CONFLICT DO UPDATE assignment path.
func TestTimestampTypmodOnConflictUpdateRoundsStoredValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamp typmod ON CONFLICT UPDATE rounds stored value",
			SetUpScript: []string{
				`CREATE TABLE timestamp_typmod_upsert_items (
					id INT PRIMARY KEY,
					ts TIMESTAMP(0)
				);`,
				`INSERT INTO timestamp_typmod_upsert_items VALUES
					(1, '2021-09-15 21:43:55');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO timestamp_typmod_upsert_items VALUES
						(1, '2021-09-15 21:43:56.789')
						ON CONFLICT (id) DO UPDATE SET ts = EXCLUDED.ts;`,
				},
				{
					Query: `SELECT ts::text
						FROM timestamp_typmod_upsert_items
						ORDER BY id;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57"}},
				},
			},
		},
	})
}

// TestTimestampTypmodInsertSelectRoundsStoredValuesRepro reproduces a timestamp
// storage correctness bug: PostgreSQL applies timestamp typmods when
// INSERT ... SELECT writes into a typmod-constrained timestamp column.
func TestTimestampTypmodInsertSelectRoundsStoredValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamp typmod INSERT SELECT rounds stored values",
			SetUpScript: []string{
				`CREATE TABLE timestamp_typmod_insert_select_source (
					id INT PRIMARY KEY,
					ts TIMESTAMP
				);`,
				`CREATE TABLE timestamp_typmod_insert_select_items (
					id INT PRIMARY KEY,
					ts TIMESTAMP(0)
				);`,
				`INSERT INTO timestamp_typmod_insert_select_source VALUES
					(1, '2021-09-15 21:43:56.789');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO timestamp_typmod_insert_select_items
						SELECT id, ts FROM timestamp_typmod_insert_select_source;`,
				},
				{
					Query: `SELECT ts::text
						FROM timestamp_typmod_insert_select_items
						ORDER BY id;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57"}},
				},
			},
		},
	})
}

// TestTimestampTypmodUpdateFromRoundsStoredValuesRepro reproduces a timestamp
// storage correctness bug: PostgreSQL applies timestamp typmods when
// UPDATE ... FROM assigns a joined source value to a typmod-constrained
// timestamp column.
func TestTimestampTypmodUpdateFromRoundsStoredValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamp typmod UPDATE FROM rounds stored values",
			SetUpScript: []string{
				`CREATE TABLE timestamp_typmod_update_from_source (
					id INT PRIMARY KEY,
					new_ts TIMESTAMP
				);`,
				`CREATE TABLE timestamp_typmod_update_from_items (
					id INT PRIMARY KEY,
					ts TIMESTAMP(0)
				);`,
				`INSERT INTO timestamp_typmod_update_from_items VALUES
					(1, '2021-09-15 21:43:55');`,
				`INSERT INTO timestamp_typmod_update_from_source VALUES
					(1, '2021-09-15 21:43:56.789');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE timestamp_typmod_update_from_items AS t
						SET ts = s.new_ts
						FROM timestamp_typmod_update_from_source AS s
						WHERE t.id = s.id;`,
				},
				{
					Query: `SELECT ts::text
						FROM timestamp_typmod_update_from_items
						ORDER BY id;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57"}},
				},
			},
		},
	})
}

// TestTimestamptzTypmodDefaultRoundsStoredValueRepro reproduces a timestamptz
// storage correctness bug: PostgreSQL applies timestamptz typmods when a column
// default is used for an inserted row.
func TestTimestamptzTypmodDefaultRoundsStoredValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamptz typmod default rounds stored value",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
				`CREATE TABLE timestamptz_typmod_default_items (
					id INT PRIMARY KEY,
					tz TIMESTAMPTZ(0) DEFAULT '2021-09-15 21:43:56.789+00'
				);`,
				`INSERT INTO timestamptz_typmod_default_items (id) VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT tz::text
						FROM timestamptz_typmod_default_items
						ORDER BY id;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57+00"}},
				},
			},
		},
	})
}

// TestTimestamptzTypmodCopyFromRoundsStoredValueRepro reproduces a timestamptz
// storage correctness bug: PostgreSQL applies timestamptz typmods to values
// loaded through COPY FROM STDIN.
func TestTimestamptzTypmodCopyFromRoundsStoredValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamptz typmod COPY FROM rounds stored value",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
				`CREATE TABLE timestamptz_typmod_copy_items (
					id INT PRIMARY KEY,
					tz TIMESTAMPTZ(0)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:             `COPY timestamptz_typmod_copy_items (id, tz) FROM STDIN;`,
					CopyFromStdInFile: "timestamptz-typmod-copy.tsv",
				},
				{
					Query: `SELECT tz::text
						FROM timestamptz_typmod_copy_items
						ORDER BY id;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57+00"}},
				},
			},
		},
	})
}

// TestTimestamptzTypmodUpdateRoundsStoredValueRepro reproduces a timestamptz
// storage correctness bug: PostgreSQL applies timestamptz typmods when UPDATE
// assigns a new value to a typmod-constrained timestamptz column.
func TestTimestamptzTypmodUpdateRoundsStoredValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamptz typmod UPDATE rounds stored value",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
				`CREATE TABLE timestamptz_typmod_update_items (
					id INT PRIMARY KEY,
					tz TIMESTAMPTZ(0)
				);`,
				`INSERT INTO timestamptz_typmod_update_items VALUES
					(1, '2021-09-15 21:43:55+00');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE timestamptz_typmod_update_items
						SET tz = '2021-09-15 21:43:56.789+00'
						WHERE id = 1;`,
				},
				{
					Query: `SELECT tz::text
						FROM timestamptz_typmod_update_items
						ORDER BY id;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57+00"}},
				},
			},
		},
	})
}

// TestTimestamptzTypmodOnConflictUpdateRoundsStoredValueRepro reproduces a
// timestamptz storage correctness bug: PostgreSQL applies timestamptz typmods
// on the ON CONFLICT DO UPDATE assignment path.
func TestTimestamptzTypmodOnConflictUpdateRoundsStoredValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamptz typmod ON CONFLICT UPDATE rounds stored value",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
				`CREATE TABLE timestamptz_typmod_upsert_items (
					id INT PRIMARY KEY,
					tz TIMESTAMPTZ(0)
				);`,
				`INSERT INTO timestamptz_typmod_upsert_items VALUES
					(1, '2021-09-15 21:43:55+00');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO timestamptz_typmod_upsert_items VALUES
						(1, '2021-09-15 21:43:56.789+00')
						ON CONFLICT (id) DO UPDATE SET tz = EXCLUDED.tz;`,
				},
				{
					Query: `SELECT tz::text
						FROM timestamptz_typmod_upsert_items
						ORDER BY id;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57+00"}},
				},
			},
		},
	})
}

// TestTimestamptzTypmodInsertSelectRoundsStoredValuesRepro reproduces a
// timestamptz storage correctness bug: PostgreSQL applies timestamptz typmods
// when INSERT ... SELECT writes into a typmod-constrained timestamptz column.
func TestTimestamptzTypmodInsertSelectRoundsStoredValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamptz typmod INSERT SELECT rounds stored values",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
				`CREATE TABLE timestamptz_typmod_insert_select_source (
					id INT PRIMARY KEY,
					tz TIMESTAMPTZ
				);`,
				`CREATE TABLE timestamptz_typmod_insert_select_items (
					id INT PRIMARY KEY,
					tz TIMESTAMPTZ(0)
				);`,
				`INSERT INTO timestamptz_typmod_insert_select_source VALUES
					(1, '2021-09-15 21:43:56.789+00');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO timestamptz_typmod_insert_select_items
						SELECT id, tz FROM timestamptz_typmod_insert_select_source;`,
				},
				{
					Query: `SELECT tz::text
						FROM timestamptz_typmod_insert_select_items
						ORDER BY id;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57+00"}},
				},
			},
		},
	})
}

// TestTimestamptzTypmodUpdateFromRoundsStoredValuesRepro reproduces a
// timestamptz storage correctness bug: PostgreSQL applies timestamptz typmods
// when UPDATE ... FROM assigns a joined source value to a typmod-constrained
// timestamptz column.
func TestTimestamptzTypmodUpdateFromRoundsStoredValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamptz typmod UPDATE FROM rounds stored values",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
				`CREATE TABLE timestamptz_typmod_update_from_source (
					id INT PRIMARY KEY,
					new_tz TIMESTAMPTZ
				);`,
				`CREATE TABLE timestamptz_typmod_update_from_items (
					id INT PRIMARY KEY,
					tz TIMESTAMPTZ(0)
				);`,
				`INSERT INTO timestamptz_typmod_update_from_items VALUES
					(1, '2021-09-15 21:43:55+00');`,
				`INSERT INTO timestamptz_typmod_update_from_source VALUES
					(1, '2021-09-15 21:43:56.789+00');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE timestamptz_typmod_update_from_items AS t
						SET tz = s.new_tz
						FROM timestamptz_typmod_update_from_source AS s
						WHERE t.id = s.id;`,
				},
				{
					Query: `SELECT tz::text
						FROM timestamptz_typmod_update_from_items
						ORDER BY id;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57+00"}},
				},
			},
		},
	})
}

// TestTimestampTypmodTableCheckUsesRoundedValueRepro reproduces a data
// consistency bug: PostgreSQL evaluates table CHECK constraints after applying
// the timestamp column typmod.
func TestTimestampTypmodTableCheckUsesRoundedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamp typmod table CHECK uses rounded value",
			SetUpScript: []string{
				`CREATE TABLE timestamp_typmod_check_items (
					id INT PRIMARY KEY,
					ts TIMESTAMP(0) CHECK (ts = '2021-09-15 21:43:56.789'::timestamp)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO timestamp_typmod_check_items VALUES
						(1, '2021-09-15 21:43:56.789');`,
					ExpectedErr: `timestamp_typmod_check_items_ts_check`,
				},
				{
					Query:    `SELECT count(*) FROM timestamp_typmod_check_items;`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestTimestamptzTypmodTableCheckUsesRoundedValueRepro reproduces a data
// consistency bug: PostgreSQL evaluates table CHECK constraints after applying
// the timestamptz column typmod.
func TestTimestamptzTypmodTableCheckUsesRoundedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamptz typmod table CHECK uses rounded value",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
				`CREATE TABLE timestamptz_typmod_check_items (
					id INT PRIMARY KEY,
					tz TIMESTAMPTZ(0) CHECK (tz = '2021-09-15 21:43:56.789+00'::timestamptz)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO timestamptz_typmod_check_items VALUES
						(1, '2021-09-15 21:43:56.789+00');`,
					ExpectedErr: `timestamptz_typmod_check_items_tz_check`,
				},
				{
					Query:    `SELECT count(*) FROM timestamptz_typmod_check_items;`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestTimestampTypmodGeneratedColumnRoundsStoredValueRepro reproduces a stored
// generated column correctness bug: PostgreSQL applies the generated column's
// declared timestamp typmod before storage.
func TestTimestampTypmodGeneratedColumnRoundsStoredValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamp typmod generated column rounds stored value",
			SetUpScript: []string{
				`CREATE TABLE timestamp_typmod_generated_items (
					id INT PRIMARY KEY,
					raw TIMESTAMP,
					ts TIMESTAMP(0) GENERATED ALWAYS AS (raw) STORED
				);`,
				`INSERT INTO timestamp_typmod_generated_items (id, raw) VALUES
					(1, '2021-09-15 21:43:56.789'::timestamp);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ts::text
						FROM timestamp_typmod_generated_items
						ORDER BY id;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57"}},
				},
			},
		},
	})
}

// TestTimestamptzTypmodGeneratedColumnRoundsStoredValueRepro reproduces a
// stored generated column correctness bug: PostgreSQL applies the generated
// column's declared timestamptz typmod before storage.
func TestTimestamptzTypmodGeneratedColumnRoundsStoredValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamptz typmod generated column rounds stored value",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
				`CREATE TABLE timestamptz_typmod_generated_items (
					id INT PRIMARY KEY,
					raw TIMESTAMPTZ,
					tz TIMESTAMPTZ(0) GENERATED ALWAYS AS (raw) STORED
				);`,
				`INSERT INTO timestamptz_typmod_generated_items (id, raw) VALUES
					(1, '2021-09-15 21:43:56.789+00'::timestamptz);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT tz::text
						FROM timestamptz_typmod_generated_items
						ORDER BY id;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57+00"}},
				},
			},
		},
	})
}

// TestTimestampTypmodUniqueUsesRoundedValuesRepro reproduces a data
// consistency bug: PostgreSQL enforces unique constraints after timestamp
// typmod coercion, so values that round to the same stored timestamp conflict.
func TestTimestampTypmodUniqueUsesRoundedValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamp typmod unique constraint uses rounded values",
			SetUpScript: []string{
				`CREATE TABLE timestamp_typmod_unique_items (
					id INT PRIMARY KEY,
					ts TIMESTAMP(0) UNIQUE
				);`,
				`INSERT INTO timestamp_typmod_unique_items VALUES
					(1, '2021-09-15 21:43:56.600');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO timestamp_typmod_unique_items VALUES
						(2, '2021-09-15 21:43:56.700');`,
					ExpectedErr: `duplicate`,
				},
				{
					Query: `SELECT id, ts::text
						FROM timestamp_typmod_unique_items
						ORDER BY id;`,
					Expected: []sql.Row{{1, "2021-09-15 21:43:57"}},
				},
			},
		},
	})
}

// TestTimestamptzTypmodUniqueUsesRoundedValuesRepro reproduces a data
// consistency bug: PostgreSQL enforces unique constraints after timestamptz
// typmod coercion, so values that round to the same stored instant conflict.
func TestTimestamptzTypmodUniqueUsesRoundedValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamptz typmod unique constraint uses rounded values",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
				`CREATE TABLE timestamptz_typmod_unique_items (
					id INT PRIMARY KEY,
					tz TIMESTAMPTZ(0) UNIQUE
				);`,
				`INSERT INTO timestamptz_typmod_unique_items VALUES
					(1, '2021-09-15 21:43:56.600+00'::timestamptz);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO timestamptz_typmod_unique_items VALUES
						(2, '2021-09-15 21:43:56.700+00'::timestamptz);`,
					ExpectedErr: `duplicate`,
				},
				{
					Query: `SELECT id, tz::text
						FROM timestamptz_typmod_unique_items
						ORDER BY id;`,
					Expected: []sql.Row{{1, "2021-09-15 21:43:57+00"}},
				},
			},
		},
	})
}

// TestTimestampTypmodForeignKeyUsesRoundedValuesRepro reproduces a referential
// consistency bug: PostgreSQL applies timestamp typmods before comparing
// foreign-key values.
func TestTimestampTypmodForeignKeyUsesRoundedValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamp typmod foreign key uses rounded values",
			SetUpScript: []string{
				`CREATE TABLE timestamp_typmod_fk_parent (
					ts TIMESTAMP(0) PRIMARY KEY
				);`,
				`CREATE TABLE timestamp_typmod_fk_child (
					id INT PRIMARY KEY,
					ts TIMESTAMP(0) REFERENCES timestamp_typmod_fk_parent(ts)
				);`,
				`INSERT INTO timestamp_typmod_fk_parent VALUES
					('2021-09-15 21:43:56.600'::timestamp);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO timestamp_typmod_fk_child VALUES
						(1, '2021-09-15 21:43:56.700'::timestamp);`,
				},
				{
					Query:    `SELECT ts::text FROM timestamp_typmod_fk_child;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57"}},
				},
			},
		},
	})
}

// TestTimestamptzTypmodForeignKeyUsesRoundedValuesRepro reproduces a
// referential consistency bug: PostgreSQL applies timestamptz typmods before
// comparing foreign-key values.
func TestTimestamptzTypmodForeignKeyUsesRoundedValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamptz typmod foreign key uses rounded values",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
				`CREATE TABLE timestamptz_typmod_fk_parent (
					tz TIMESTAMPTZ(0) PRIMARY KEY
				);`,
				`CREATE TABLE timestamptz_typmod_fk_child (
					id INT PRIMARY KEY,
					tz TIMESTAMPTZ(0) REFERENCES timestamptz_typmod_fk_parent(tz)
				);`,
				`INSERT INTO timestamptz_typmod_fk_parent VALUES
					('2021-09-15 21:43:56.600+00'::timestamptz);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO timestamptz_typmod_fk_child VALUES
						(1, '2021-09-15 21:43:56.700+00'::timestamptz);`,
				},
				{
					Query:    `SELECT tz::text FROM timestamptz_typmod_fk_child;`,
					Expected: []sql.Row{{"2021-09-15 21:43:57+00"}},
				},
			},
		},
	})
}

// TestTimestampArrayTypmodsRoundStoredElementsRepro reproduces a timestamp
// array storage correctness bug: PostgreSQL applies the declared element
// typmod to every stored array element.
func TestTimestampArrayTypmodsRoundStoredElementsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamp array typmods round stored elements",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
				`CREATE TABLE timestamp_array_typmod_items (
					id INT PRIMARY KEY,
					ts_values TIMESTAMP(0)[],
					tz_values TIMESTAMPTZ(2)[]
				);`,
				`INSERT INTO timestamp_array_typmod_items VALUES
					(1,
						ARRAY[
							'2021-09-15 21:43:56.789'::timestamp,
							'2021-09-15 21:43:57.123'::timestamp
						],
						ARRAY[
							'2021-09-15 21:43:56.789+00'::timestamptz,
							'2021-09-15 21:43:57.123+00'::timestamptz
						]);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ts_values::text, tz_values::text
						FROM timestamp_array_typmod_items
						ORDER BY id;`,
					Expected: []sql.Row{{
						`{"2021-09-15 21:43:57","2021-09-15 21:43:57"}`,
						`{"2021-09-15 21:43:56.79+00","2021-09-15 21:43:57.12+00"}`,
					}},
				},
			},
		},
	})
}

// TestTimeArrayTypmodsRoundStoredElementsRepro reproduces a time-array storage
// correctness bug: PostgreSQL applies the declared element typmod to every
// stored time and timetz array element.
func TestTimeArrayTypmodsRoundStoredElementsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "time array typmods round stored elements",
			SetUpScript: []string{
				`CREATE TABLE time_array_typmod_items (
					id INT PRIMARY KEY,
					t_values TIME(0)[],
					tz_values TIMETZ(0)[]
				);`,
				`INSERT INTO time_array_typmod_items VALUES
					(1,
						ARRAY[
							'21:43:56.789'::time,
							'21:43:57.123'::time
						],
						ARRAY[
							'21:43:56.789+00'::timetz,
							'21:43:57.123+00'::timetz
						]);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT t_values::text, tz_values::text
						FROM time_array_typmod_items
						ORDER BY id;`,
					Expected: []sql.Row{{
						`{21:43:57,21:43:57}`,
						`{21:43:57+00,21:43:57+00}`,
					}},
				},
			},
		},
	})
}

// TestTimeColumnTypmodsRoundStoredFractionalSecondsRepro reproduces a timetz
// storage correctness bug: PostgreSQL applies typmod precision to stored time
// and timetz values, not just literal casts.
func TestTimeColumnTypmodsRoundStoredFractionalSecondsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "time column typmods round stored fractional seconds",
			SetUpScript: []string{
				`CREATE TABLE time_column_typmod_items (
					id INT PRIMARY KEY,
					t TIME(0),
					tz TIMETZ(0)
				);`,
				`INSERT INTO time_column_typmod_items VALUES
					(1, '21:43:56.789'::time, '21:43:56.789+00'::timetz);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT t::text, tz::text
						FROM time_column_typmod_items
						ORDER BY id;`,
					Expected: []sql.Row{{"21:43:57", "21:43:57+00"}},
				},
			},
		},
	})
}

// TestTimeTypmodDefaultRoundsStoredValueRepro reproduces a timetz storage
// correctness bug: PostgreSQL applies time and timetz typmods when a column
// default is used for an inserted row.
func TestTimeTypmodDefaultRoundsStoredValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "time typmod default rounds stored value",
			SetUpScript: []string{
				`CREATE TABLE time_typmod_default_items (
					id INT PRIMARY KEY,
					t TIME(0) DEFAULT '21:43:56.789'::time,
					tz TIMETZ(0) DEFAULT '21:43:56.789+00'::timetz
				);`,
				`INSERT INTO time_typmod_default_items (id) VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT t::text, tz::text
						FROM time_typmod_default_items
						ORDER BY id;`,
					Expected: []sql.Row{{"21:43:57", "21:43:57+00"}},
				},
			},
		},
	})
}

// TestTimeTypmodCopyFromRoundsStoredValueGuard guards that time and timetz
// typmods are applied to values loaded through COPY FROM STDIN.
func TestTimeTypmodCopyFromRoundsStoredValueGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "time typmod COPY FROM rounds stored value",
			SetUpScript: []string{
				`CREATE TABLE time_typmod_copy_items (
					id INT PRIMARY KEY,
					t TIME(0),
					tz TIMETZ(0)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:             `COPY time_typmod_copy_items (id, t, tz) FROM STDIN;`,
					CopyFromStdInFile: "time-typmod-copy.tsv",
				},
				{
					Query: `SELECT t::text, tz::text
						FROM time_typmod_copy_items
						ORDER BY id;`,
					Expected: []sql.Row{{"21:43:57", "21:43:57+00"}},
				},
			},
		},
	})
}

// TestTimeTypmodUpdateRoundsStoredValueRepro reproduces a timetz storage
// correctness bug: PostgreSQL applies time and timetz typmods when UPDATE
// assigns a new value to a typmod-constrained time column.
func TestTimeTypmodUpdateRoundsStoredValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "time typmod UPDATE rounds stored value",
			SetUpScript: []string{
				`CREATE TABLE time_typmod_update_items (
					id INT PRIMARY KEY,
					t TIME(0),
					tz TIMETZ(0)
				);`,
				`INSERT INTO time_typmod_update_items VALUES
					(1, '00:00:00'::time, '00:00:00+00'::timetz);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE time_typmod_update_items
						SET t = '21:43:56.789'::time,
							tz = '21:43:56.789+00'::timetz
						WHERE id = 1;`,
				},
				{
					Query: `SELECT t::text, tz::text
						FROM time_typmod_update_items
						ORDER BY id;`,
					Expected: []sql.Row{{"21:43:57", "21:43:57+00"}},
				},
			},
		},
	})
}

// TestTimeTypmodOnConflictUpdateRoundsStoredValueRepro reproduces a timetz
// storage correctness bug: PostgreSQL applies time and timetz typmods on the
// ON CONFLICT DO UPDATE assignment path.
func TestTimeTypmodOnConflictUpdateRoundsStoredValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "time typmod ON CONFLICT UPDATE rounds stored value",
			SetUpScript: []string{
				`CREATE TABLE time_typmod_upsert_items (
					id INT PRIMARY KEY,
					t TIME(0),
					tz TIMETZ(0)
				);`,
				`INSERT INTO time_typmod_upsert_items VALUES
					(1, '00:00:00'::time, '00:00:00+00'::timetz);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO time_typmod_upsert_items VALUES
						(1, '21:43:56.789'::time, '21:43:56.789+00'::timetz)
						ON CONFLICT (id) DO UPDATE SET t = EXCLUDED.t, tz = EXCLUDED.tz;`,
				},
				{
					Query: `SELECT t::text, tz::text
						FROM time_typmod_upsert_items
						ORDER BY id;`,
					Expected: []sql.Row{{"21:43:57", "21:43:57+00"}},
				},
			},
		},
	})
}

// TestTimeTypmodInsertSelectRoundsStoredValuesRepro reproduces a timetz storage
// correctness bug: PostgreSQL applies time and timetz typmods when
// INSERT ... SELECT writes into typmod-constrained time columns.
func TestTimeTypmodInsertSelectRoundsStoredValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "time typmod INSERT SELECT rounds stored values",
			SetUpScript: []string{
				`CREATE TABLE time_typmod_insert_select_source (
					id INT PRIMARY KEY,
					t TIME,
					tz TIMETZ
				);`,
				`CREATE TABLE time_typmod_insert_select_items (
					id INT PRIMARY KEY,
					t TIME(0),
					tz TIMETZ(0)
				);`,
				`INSERT INTO time_typmod_insert_select_source VALUES
					(1, '21:43:56.789'::time, '21:43:56.789+00'::timetz);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO time_typmod_insert_select_items
						SELECT id, t, tz FROM time_typmod_insert_select_source;`,
				},
				{
					Query: `SELECT t::text, tz::text
						FROM time_typmod_insert_select_items
						ORDER BY id;`,
					Expected: []sql.Row{{"21:43:57", "21:43:57+00"}},
				},
			},
		},
	})
}

// TestTimeTypmodUpdateFromRoundsStoredValuesRepro reproduces a timetz storage
// correctness bug: PostgreSQL applies time and timetz typmods when
// UPDATE ... FROM assigns joined source values into typmod-constrained time
// columns.
func TestTimeTypmodUpdateFromRoundsStoredValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "time typmod UPDATE FROM rounds stored values",
			SetUpScript: []string{
				`CREATE TABLE time_typmod_update_from_source (
					id INT PRIMARY KEY,
					new_t TIME,
					new_tz TIMETZ
				);`,
				`CREATE TABLE time_typmod_update_from_items (
					id INT PRIMARY KEY,
					t TIME(0),
					tz TIMETZ(0)
				);`,
				`INSERT INTO time_typmod_update_from_items VALUES
					(1, '00:00:00'::time, '00:00:00+00'::timetz);`,
				`INSERT INTO time_typmod_update_from_source VALUES
					(1, '21:43:56.789'::time, '21:43:56.789+00'::timetz);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE time_typmod_update_from_items AS target
						SET t = source.new_t, tz = source.new_tz
						FROM time_typmod_update_from_source AS source
						WHERE target.id = source.id;`,
				},
				{
					Query: `SELECT t::text, tz::text
						FROM time_typmod_update_from_items
						ORDER BY id;`,
					Expected: []sql.Row{{"21:43:57", "21:43:57+00"}},
				},
			},
		},
	})
}

// TestTimeTypmodTableCheckUsesRoundedValueRepro reproduces a data consistency
// bug: PostgreSQL evaluates table CHECK constraints after applying the timetz
// column typmod.
func TestTimeTypmodTableCheckUsesRoundedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "time typmod table CHECK uses rounded value",
			SetUpScript: []string{
				`CREATE TABLE time_typmod_check_items (
					id INT PRIMARY KEY,
					tz TIMETZ(0) CHECK (tz = '21:43:56.789+00'::timetz)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO time_typmod_check_items VALUES
						(1, '21:43:56.789+00'::timetz);`,
					ExpectedErr: `time_typmod_check_items_tz_check`,
				},
				{
					Query:    `SELECT count(*) FROM time_typmod_check_items;`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestTimeTypmodGeneratedColumnUsesRoundedValueRepro reproduces a stored
// generated column correctness bug: PostgreSQL applies the generated column's
// declared timetz typmod before storage.
func TestTimeTypmodGeneratedColumnUsesRoundedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "time typmod generated column uses rounded value",
			SetUpScript: []string{
				`CREATE TABLE time_typmod_generated_items (
					id INT PRIMARY KEY,
					raw TIMETZ,
					tz TIMETZ(0) GENERATED ALWAYS AS (raw) STORED
				);`,
				`INSERT INTO time_typmod_generated_items (id, raw) VALUES
					(1, '21:43:56.789+00'::timetz);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT tz::text
						FROM time_typmod_generated_items
						ORDER BY id;`,
					Expected: []sql.Row{{"21:43:57+00"}},
				},
			},
		},
	})
}

// TestTimeTypmodUniqueUsesRoundedValuesRepro reproduces a uniqueness
// consistency bug: PostgreSQL enforces unique constraints after applying the
// timetz column typmod.
func TestTimeTypmodUniqueUsesRoundedValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "time typmod UNIQUE uses rounded values",
			SetUpScript: []string{
				`CREATE TABLE time_typmod_unique_items (
					id INT PRIMARY KEY,
					tz TIMETZ(0) UNIQUE
				);`,
				`INSERT INTO time_typmod_unique_items VALUES
					(1, '21:43:56.600+00'::timetz);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO time_typmod_unique_items VALUES
						(2, '21:43:56.700+00'::timetz);`,
					ExpectedErr: `time_typmod_unique_items_tz_key`,
				},
				{
					Query:    `SELECT count(*) FROM time_typmod_unique_items;`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestTimeTypmodForeignKeyUsesRoundedValuesRepro reproduces a referential
// consistency bug: PostgreSQL applies timetz typmods before comparing
// foreign-key values.
func TestTimeTypmodForeignKeyUsesRoundedValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "time typmod foreign key uses rounded values",
			SetUpScript: []string{
				`CREATE TABLE time_typmod_fk_parent (
					tz TIMETZ(0) PRIMARY KEY
				);`,
				`CREATE TABLE time_typmod_fk_child (
					tz TIMETZ(0) REFERENCES time_typmod_fk_parent(tz)
				);`,
				`INSERT INTO time_typmod_fk_parent VALUES
					('21:43:56.600+00'::timetz);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO time_typmod_fk_child VALUES
						('21:43:56.700+00'::timetz);`,
				},
				{
					Query:    `SELECT count(*) FROM time_typmod_fk_child;`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestTimeTypmodCastsUseRoundedValueRepro reproduces a timetz cast correctness
// bug: PostgreSQL applies precision typmods for explicit casts to time and
// timetz typmod types.
func TestTimeTypmodCastsUseRoundedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "time typmod casts use rounded value",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
						CAST('21:43:56.789'::time AS TIME(0))::text,
						CAST('21:43:56.789+00'::timetz AS TIMETZ(0))::text;`,
					Expected: []sql.Row{{"21:43:57", "21:43:57+00"}},
				},
			},
		},
	})
}

// TestIntervalTypmodsRoundFractionalSecondsGuard protects interval typmod
// precision behavior: PostgreSQL applies interval typmod precision by rounding
// fractional seconds, and Doltgres currently matches that behavior.
func TestIntervalTypmodsRoundFractionalSecondsGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "interval typmods round fractional seconds",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT interval(0) '1.789 seconds'::text;`,
					Expected: []sql.Row{{"00:00:02"}},
				},
				{
					Query:    `SELECT interval(2) '1.789 seconds'::text;`,
					Expected: []sql.Row{{"00:00:01.79"}},
				},
			},
		},
	})
}

// TestIntervalFieldTypmodsRestrictStoredFieldsRepro reproduces an interval
// storage correctness bug: PostgreSQL applies interval field restrictions and
// fractional precision when values are assigned to typed columns.
func TestIntervalFieldTypmodsRestrictStoredFieldsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "interval field typmods restrict stored fields",
			SetUpScript: []string{
				`CREATE TABLE interval_field_typmod_items (
					id INT PRIMARY KEY,
					ym INTERVAL YEAR TO MONTH,
					ds INTERVAL DAY TO SECOND(0)
				);`,
				`INSERT INTO interval_field_typmod_items VALUES
					(1, '1 year 2 months 3 days 04:05:06.789',
						'3 days 04:05:06.789');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ym::text, ds::text
						FROM interval_field_typmod_items
						ORDER BY id;`,
					Expected: []sql.Row{{"1 year 2 mons", "3 days 04:05:07"}},
				},
			},
		},
	})
}

// TestIntervalTypmodDefaultRestrictsStoredValueRepro reproduces an interval
// storage correctness bug: PostgreSQL applies interval typmods when a column
// default is used for an inserted row.
func TestIntervalTypmodDefaultRestrictsStoredValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "interval typmod default restricts stored value",
			SetUpScript: []string{
				`CREATE TABLE interval_typmod_default_items (
					id INT PRIMARY KEY,
					ds INTERVAL DAY TO SECOND(0) DEFAULT '3 days 04:05:06.789'::interval
				);`,
				`INSERT INTO interval_typmod_default_items (id) VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ds::text
						FROM interval_typmod_default_items
						ORDER BY id;`,
					Expected: []sql.Row{{"3 days 04:05:07"}},
				},
			},
		},
	})
}

// TestTemporalTypmodExplicitDefaultCoercesStoredValuesRepro reproduces a
// temporal storage correctness bug: PostgreSQL applies temporal typmods when an
// INSERT explicitly uses DEFAULT for typmod-constrained temporal columns.
func TestTemporalTypmodExplicitDefaultCoercesStoredValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "temporal typmod explicit DEFAULT coerces stored values",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
				`CREATE TABLE temporal_typmod_explicit_default_items (
					id INT PRIMARY KEY,
					ts TIMESTAMP(0) DEFAULT '2021-09-15 21:43:56.789'::timestamp,
					tz TIMESTAMPTZ(0) DEFAULT '2021-09-15 21:43:56.789+00'::timestamptz,
					tm TIME(0) DEFAULT '21:43:56.789'::time,
					tzt TIMETZ(0) DEFAULT '21:43:56.789+00'::timetz,
					ds INTERVAL DAY TO SECOND(0) DEFAULT '3 days 04:05:06.789'::interval
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO temporal_typmod_explicit_default_items
						VALUES (1, DEFAULT, DEFAULT, DEFAULT, DEFAULT, DEFAULT);`,
				},
				{
					Query: `SELECT ts::text, tz::text, tm::text, tzt::text, ds::text
						FROM temporal_typmod_explicit_default_items
						ORDER BY id;`,
					Expected: []sql.Row{{
						"2021-09-15 21:43:57",
						"2021-09-15 21:43:57+00",
						"21:43:57",
						"21:43:57+00",
						"3 days 04:05:07",
					}},
				},
			},
		},
	})
}

// TestTemporalTypmodAlterSetDefaultCoercesStoredValuesRepro reproduces a
// temporal storage correctness bug: PostgreSQL applies temporal typmods to
// future writes that use defaults installed with ALTER COLUMN SET DEFAULT.
func TestTemporalTypmodAlterSetDefaultCoercesStoredValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "temporal typmod ALTER SET DEFAULT coerces stored values",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
				`CREATE TABLE temporal_typmod_alter_set_default_items (
					id INT PRIMARY KEY,
					ts TIMESTAMP(0),
					tz TIMESTAMPTZ(0),
					tm TIME(0),
					tzt TIMETZ(0),
					ds INTERVAL DAY TO SECOND(0)
				);`,
				`ALTER TABLE temporal_typmod_alter_set_default_items
					ALTER COLUMN ts SET DEFAULT '2021-09-15 21:43:56.789'::timestamp,
					ALTER COLUMN tz SET DEFAULT '2021-09-15 21:43:56.789+00'::timestamptz,
					ALTER COLUMN tm SET DEFAULT '21:43:56.789'::time,
					ALTER COLUMN tzt SET DEFAULT '21:43:56.789+00'::timetz,
					ALTER COLUMN ds SET DEFAULT '3 days 04:05:06.789'::interval;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO temporal_typmod_alter_set_default_items (id) VALUES (1);`,
				},
				{
					Query: `SELECT ts::text, tz::text, tm::text, tzt::text, ds::text
						FROM temporal_typmod_alter_set_default_items
						ORDER BY id;`,
					Expected: []sql.Row{{
						"2021-09-15 21:43:57",
						"2021-09-15 21:43:57+00",
						"21:43:57",
						"21:43:57+00",
						"3 days 04:05:07",
					}},
				},
			},
		},
	})
}

// TestTemporalTypmodUpdateSetDefaultCoercesStoredValuesRepro reproduces a
// temporal storage correctness bug: PostgreSQL applies temporal typmods when
// UPDATE SET DEFAULT writes the column default into a typmod-constrained column.
func TestTemporalTypmodUpdateSetDefaultCoercesStoredValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "temporal typmod UPDATE SET DEFAULT coerces stored values",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
				`CREATE TABLE temporal_typmod_update_default_items (
					id INT PRIMARY KEY,
					ts TIMESTAMP(0) DEFAULT '2021-09-15 21:43:56.789'::timestamp,
					tz TIMESTAMPTZ(0) DEFAULT '2021-09-15 21:43:56.789+00'::timestamptz,
					tm TIME(0) DEFAULT '21:43:56.789'::time,
					tzt TIMETZ(0) DEFAULT '21:43:56.789+00'::timetz,
					ds INTERVAL DAY TO SECOND(0) DEFAULT '3 days 04:05:06.789'::interval
				);`,
				`INSERT INTO temporal_typmod_update_default_items VALUES (
					1,
					'2000-01-01 00:00:00'::timestamp,
					'2000-01-01 00:00:00+00'::timestamptz,
					'00:00:00'::time,
					'00:00:00+00'::timetz,
					'1 day'::interval
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE temporal_typmod_update_default_items
						SET ts = DEFAULT,
							tz = DEFAULT,
							tm = DEFAULT,
							tzt = DEFAULT,
							ds = DEFAULT
						WHERE id = 1;`,
				},
				{
					Query: `SELECT ts::text, tz::text, tm::text, tzt::text, ds::text
						FROM temporal_typmod_update_default_items
						ORDER BY id;`,
					Expected: []sql.Row{{
						"2021-09-15 21:43:57",
						"2021-09-15 21:43:57+00",
						"21:43:57",
						"21:43:57+00",
						"3 days 04:05:07",
					}},
				},
			},
		},
	})
}

// TestTemporalTypmodOnConflictSetDefaultCoercesStoredValuesRepro reproduces a
// temporal storage correctness bug: PostgreSQL applies temporal typmods when an
// ON CONFLICT DO UPDATE assignment writes column defaults.
func TestTemporalTypmodOnConflictSetDefaultCoercesStoredValuesRepro(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	for _, stmt := range []string{
		`SET TIME ZONE 'UTC';`,
		`CREATE TABLE temporal_typmod_upsert_default_items (
			id INT PRIMARY KEY,
			ts TIMESTAMP(0) DEFAULT '2021-09-15 21:43:56.789'::timestamp,
			tz TIMESTAMPTZ(0) DEFAULT '2021-09-15 21:43:56.789+00'::timestamptz,
			tm TIME(0) DEFAULT '21:43:56.789'::time,
			tzt TIMETZ(0) DEFAULT '21:43:56.789+00'::timetz,
			ds INTERVAL DAY TO SECOND(0) DEFAULT '3 days 04:05:06.789'::interval
		);`,
		`INSERT INTO temporal_typmod_upsert_default_items VALUES (
			1,
			'2000-01-01 00:00:00'::timestamp,
			'2000-01-01 00:00:00+00'::timestamptz,
			'00:00:00'::time,
			'00:00:00+00'::timetz,
			'1 day'::interval
		);`,
	} {
		_, err := conn.Current.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	_, err := conn.Current.Exec(ctx, `INSERT INTO temporal_typmod_upsert_default_items (id) VALUES (1)
		ON CONFLICT (id) DO UPDATE
		SET ts = DEFAULT,
			tz = DEFAULT,
			tm = DEFAULT,
			tzt = DEFAULT,
			ds = DEFAULT;`)
	require.NoError(t, err)

	rows, err := conn.Current.Query(ctx, `SELECT ts::text, tz::text, tm::text, tzt::text, ds::text
		FROM temporal_typmod_upsert_default_items
		ORDER BY id;`)
	require.NoError(t, err)
	actual, _, err := ReadRows(rows, true)
	require.NoError(t, err)
	require.Equal(t, []sql.Row{{
		"2021-09-15 21:43:57",
		"2021-09-15 21:43:57+00",
		"21:43:57",
		"21:43:57+00",
		"3 days 04:05:07",
	}}, actual)
}

// TestIntervalTypmodCopyFromRestrictsStoredValueRepro reproduces an interval
// storage correctness bug: PostgreSQL applies interval typmods to values loaded
// through COPY FROM STDIN.
func TestIntervalTypmodCopyFromRestrictsStoredValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "interval typmod COPY FROM restricts stored value",
			SetUpScript: []string{
				`CREATE TABLE interval_typmod_copy_items (
					id INT PRIMARY KEY,
					ds INTERVAL DAY TO SECOND(0)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:             `COPY interval_typmod_copy_items (id, ds) FROM STDIN;`,
					CopyFromStdInFile: "interval-typmod-copy.tsv",
				},
				{
					Query: `SELECT ds::text
						FROM interval_typmod_copy_items
						ORDER BY id;`,
					Expected: []sql.Row{{"3 days 04:05:07"}},
				},
			},
		},
	})
}

// TestIntervalTypmodUpdateRestrictsStoredValueRepro reproduces an interval
// storage correctness bug: PostgreSQL applies interval typmods when UPDATE
// assigns a new value to a typmod-constrained interval column.
func TestIntervalTypmodUpdateRestrictsStoredValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "interval typmod UPDATE restricts stored value",
			SetUpScript: []string{
				`CREATE TABLE interval_typmod_update_items (
					id INT PRIMARY KEY,
					ds INTERVAL DAY TO SECOND(0)
				);`,
				`INSERT INTO interval_typmod_update_items VALUES
					(1, interval '1 day');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE interval_typmod_update_items
						SET ds = interval '3 days 04:05:06.789'
						WHERE id = 1;`,
				},
				{
					Query: `SELECT ds::text
						FROM interval_typmod_update_items
						ORDER BY id;`,
					Expected: []sql.Row{{"3 days 04:05:07"}},
				},
			},
		},
	})
}

// TestIntervalTypmodOnConflictUpdateRestrictsStoredValueRepro reproduces an
// interval storage correctness bug: PostgreSQL applies interval typmods on the
// ON CONFLICT DO UPDATE assignment path.
func TestIntervalTypmodOnConflictUpdateRestrictsStoredValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "interval typmod ON CONFLICT UPDATE restricts stored value",
			SetUpScript: []string{
				`CREATE TABLE interval_typmod_upsert_items (
					id INT PRIMARY KEY,
					ds INTERVAL DAY TO SECOND(0)
				);`,
				`INSERT INTO interval_typmod_upsert_items VALUES
					(1, interval '1 day');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO interval_typmod_upsert_items VALUES
						(1, interval '3 days 04:05:06.789')
						ON CONFLICT (id) DO UPDATE SET ds = EXCLUDED.ds;`,
				},
				{
					Query: `SELECT ds::text
						FROM interval_typmod_upsert_items
						ORDER BY id;`,
					Expected: []sql.Row{{"3 days 04:05:07"}},
				},
			},
		},
	})
}

// TestIntervalTypmodInsertSelectRestrictsStoredValueRepro reproduces an
// interval storage correctness bug: PostgreSQL applies interval typmods when
// INSERT ... SELECT writes into a typmod-constrained interval column.
func TestIntervalTypmodInsertSelectRestrictsStoredValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "interval typmod INSERT SELECT restricts stored value",
			SetUpScript: []string{
				`CREATE TABLE interval_typmod_insert_select_source (
					id INT PRIMARY KEY,
					ds INTERVAL
				);`,
				`CREATE TABLE interval_typmod_insert_select_items (
					id INT PRIMARY KEY,
					ds INTERVAL DAY TO SECOND(0)
				);`,
				`INSERT INTO interval_typmod_insert_select_source VALUES
					(1, interval '3 days 04:05:06.789');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO interval_typmod_insert_select_items
						SELECT id, ds FROM interval_typmod_insert_select_source;`,
				},
				{
					Query: `SELECT ds::text
						FROM interval_typmod_insert_select_items
						ORDER BY id;`,
					Expected: []sql.Row{{"3 days 04:05:07"}},
				},
			},
		},
	})
}

// TestIntervalTypmodUpdateFromRestrictsStoredValueRepro reproduces an interval
// storage correctness bug: PostgreSQL applies interval typmods when
// UPDATE ... FROM assigns joined values into a typmod-constrained interval
// column.
func TestIntervalTypmodUpdateFromRestrictsStoredValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "interval typmod UPDATE FROM restricts stored value",
			SetUpScript: []string{
				`CREATE TABLE interval_typmod_update_from_source (
					id INT PRIMARY KEY,
					new_ds INTERVAL
				);`,
				`CREATE TABLE interval_typmod_update_from_items (
					id INT PRIMARY KEY,
					ds INTERVAL DAY TO SECOND(0)
				);`,
				`INSERT INTO interval_typmod_update_from_source VALUES
					(1, '3 days 04:05:06.789'::interval);`,
				`INSERT INTO interval_typmod_update_from_items VALUES
					(1, interval '1 day');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE interval_typmod_update_from_items target
						SET ds = source.new_ds
						FROM interval_typmod_update_from_source source
						WHERE target.id = source.id;`,
				},
				{
					Query: `SELECT ds::text
						FROM interval_typmod_update_from_items
						ORDER BY id;`,
					Expected: []sql.Row{{"3 days 04:05:07"}},
				},
			},
		},
	})
}

// TestIntervalTypmodTableCheckUsesRestrictedValueRepro reproduces a data
// consistency bug: PostgreSQL evaluates table CHECK constraints after applying
// the interval column typmod.
func TestIntervalTypmodTableCheckUsesRestrictedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "interval typmod table CHECK uses restricted value",
			SetUpScript: []string{
				`CREATE TABLE interval_typmod_check_items (
					id INT PRIMARY KEY,
					ds INTERVAL DAY TO SECOND(0) CHECK (ds = '3 days 04:05:06.789'::interval)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO interval_typmod_check_items VALUES
						(1, '3 days 04:05:06.789'::interval);`,
					ExpectedErr: `interval_typmod_check_items_ds_check`,
				},
				{
					Query:    `SELECT count(*) FROM interval_typmod_check_items;`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestIntervalTypmodGeneratedColumnUsesRestrictedValueRepro reproduces a stored
// generated column correctness bug: PostgreSQL applies the generated column's
// declared interval typmod before storage.
func TestIntervalTypmodGeneratedColumnUsesRestrictedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "interval typmod generated column uses restricted value",
			SetUpScript: []string{
				`CREATE TABLE interval_typmod_generated_items (
					id INT PRIMARY KEY,
					raw INTERVAL,
					ds INTERVAL DAY TO SECOND(0) GENERATED ALWAYS AS (raw) STORED
				);`,
				`INSERT INTO interval_typmod_generated_items (id, raw) VALUES
					(1, '3 days 04:05:06.789'::interval);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ds::text
						FROM interval_typmod_generated_items
						ORDER BY id;`,
					Expected: []sql.Row{{"3 days 04:05:07"}},
				},
			},
		},
	})
}

// TestIntervalTypmodUniqueUsesRestrictedValuesRepro reproduces a uniqueness
// consistency bug: PostgreSQL enforces unique constraints after applying the
// interval column typmod.
func TestIntervalTypmodUniqueUsesRestrictedValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "interval typmod UNIQUE uses restricted values",
			SetUpScript: []string{
				`CREATE TABLE interval_typmod_unique_items (
					id INT PRIMARY KEY,
					ds INTERVAL DAY TO SECOND(0) UNIQUE
				);`,
				`INSERT INTO interval_typmod_unique_items VALUES
					(1, '3 days 04:05:06.600'::interval);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO interval_typmod_unique_items VALUES
						(2, '3 days 04:05:06.700'::interval);`,
					ExpectedErr: `interval_typmod_unique_items_ds_key`,
				},
				{
					Query:    `SELECT count(*) FROM interval_typmod_unique_items;`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestIntervalTypmodForeignKeyUsesRestrictedValuesRepro reproduces a
// referential consistency bug: PostgreSQL applies interval typmods before
// comparing foreign-key values.
func TestIntervalTypmodForeignKeyUsesRestrictedValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "interval typmod foreign key uses restricted values",
			SetUpScript: []string{
				`CREATE TABLE interval_typmod_fk_parent (
					ds INTERVAL DAY TO SECOND(0) PRIMARY KEY
				);`,
				`CREATE TABLE interval_typmod_fk_child (
					ds INTERVAL DAY TO SECOND(0) REFERENCES interval_typmod_fk_parent(ds)
				);`,
				`INSERT INTO interval_typmod_fk_parent VALUES
					('3 days 04:05:06.600'::interval);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO interval_typmod_fk_child VALUES
						('3 days 04:05:06.700'::interval);`,
				},
				{
					Query:    `SELECT count(*) FROM interval_typmod_fk_child;`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestIntervalTypmodCastsUseRestrictedValueRepro reproduces an interval cast
// correctness bug: PostgreSQL applies field and precision typmods for explicit
// casts to interval typmod types.
func TestIntervalTypmodCastsUseRestrictedValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "interval typmod casts use restricted value",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
						CAST('1 year 2 months 3 days 04:05:06.789'::interval AS INTERVAL YEAR TO MONTH)::text,
						CAST('3 days 04:05:06.789'::interval AS INTERVAL DAY TO SECOND(0))::text;`,
					Expected: []sql.Row{{"1 year 2 mons", "3 days 04:05:07"}},
				},
			},
		},
	})
}

// TestIntervalArrayTypmodsRestrictStoredElementsRepro reproduces an interval
// array storage correctness bug: PostgreSQL applies the declared element
// typmod to every stored interval array element.
func TestIntervalArrayTypmodsRestrictStoredElementsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "interval array typmods restrict stored elements",
			SetUpScript: []string{
				`CREATE TABLE interval_array_typmod_items (
					id INT PRIMARY KEY,
					ym_values INTERVAL YEAR TO MONTH[],
					ds_values INTERVAL DAY TO SECOND(0)[]
				);`,
				`INSERT INTO interval_array_typmod_items VALUES
					(1,
						ARRAY[
							'1 year 2 months 3 days 04:05:06.789'::interval,
							'2 years 3 months 4 days'::interval
						],
						ARRAY[
							'3 days 04:05:06.789'::interval,
							'4 days 05:06:07.123'::interval
						]);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ym_values::text, ds_values::text
						FROM interval_array_typmod_items
						ORDER BY id;`,
					Expected: []sql.Row{{
						`{"1 year 2 mons","2 years 3 mons"}`,
						`{"3 days 04:05:07","4 days 05:06:07"}`,
					}},
				},
			},
		},
	})
}

// TestExtractTimestamptzTimezoneUsesSessionTimeZoneRepro reproduces a
// timestamptz extraction correctness bug: PostgreSQL reports timezone fields
// from the active session time zone, but Doltgres returns a fixed offset.
func TestExtractTimestamptzTimezoneUsesSessionTimeZoneRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "extract timestamptz timezone fields use session time zone",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT extract(timezone FROM timestamptz '2025-01-01 00:00:00+00')::text;`,
					Expected: []sql.Row{{"0"}},
				},
				{
					Query:    `SELECT extract(timezone_hour FROM timestamptz '2025-01-01 00:00:00+00')::text;`,
					Expected: []sql.Row{{"0"}},
				},
				{
					Query:    `SELECT extract(timezone_minute FROM timestamptz '2025-01-01 00:00:00+00')::text;`,
					Expected: []sql.Row{{"0"}},
				},
			},
		},
	})
}

// TestDatePartTimestamptzTimezoneUsesSessionTimeZoneRepro reproduces a
// timestamptz date_part correctness bug: PostgreSQL reports timezone fields
// from the active session time zone, but Doltgres returns a fixed offset.
func TestDatePartTimestamptzTimezoneUsesSessionTimeZoneRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "date_part timestamptz timezone fields use session time zone",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT date_part('timezone', timestamptz '2025-01-01 00:00:00+00')::bigint::text;`,
					Expected: []sql.Row{{"0"}},
				},
				{
					Query:    `SELECT date_part('timezone_hour', timestamptz '2025-01-01 00:00:00+00')::bigint::text;`,
					Expected: []sql.Row{{"0"}},
				},
				{
					Query:    `SELECT date_part('timezone_minute', timestamptz '2025-01-01 00:00:00+00')::bigint::text;`,
					Expected: []sql.Row{{"0"}},
				},
			},
		},
	})
}

// TestExtractIntervalQuarterUsesPostgresMonthBucketRepro reproduces an interval
// extraction correctness bug: PostgreSQL treats an interval month field of three
// months as quarter 2, but Doltgres reports quarter 1.
func TestExtractIntervalQuarterUsesPostgresMonthBucketRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "extract interval quarter uses PostgreSQL month bucket",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT extract(quarter FROM interval '3 months')::text;`,
					Expected: []sql.Row{{"2"}},
				},
			},
		},
	})
}

// TestDatePartIntervalQuarterUsesPostgresMonthBucketRepro reproduces an interval
// date_part correctness bug: PostgreSQL treats an interval month field of three
// months as quarter 2, but Doltgres reports quarter 1.
func TestDatePartIntervalQuarterUsesPostgresMonthBucketRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "date_part interval quarter uses PostgreSQL month bucket",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT date_part('quarter', interval '3 months')::bigint::text;`,
					Expected: []sql.Row{{"2"}},
				},
			},
		},
	})
}

// TestExtractNegativeIntervalFieldsUsePostgresNormalizationRepro reproduces an
// interval extraction correctness bug: PostgreSQL extracts normalized negative
// interval fields, but Doltgres floors total nanoseconds and reports larger
// negative hour/minute values.
func TestExtractNegativeIntervalFieldsUsePostgresNormalizationRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "extract negative interval fields use PostgreSQL normalization",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT extract(hour FROM interval '-65 minutes 10 seconds')::text;`,
					Expected: []sql.Row{{"-1"}},
				},
				{
					Query:    `SELECT extract(minute FROM interval '-65 minutes 10 seconds')::text;`,
					Expected: []sql.Row{{"-4"}},
				},
				{
					Query:    `SELECT extract(second FROM interval '-65 minutes 10 seconds')::text;`,
					Expected: []sql.Row{{"-50.000000"}},
				},
				{
					Query:    `SELECT extract(year FROM interval '-13 months')::text;`,
					Expected: []sql.Row{{"-1"}},
				},
				{
					Query:    `SELECT extract(decade FROM interval '-13 months')::text;`,
					Expected: []sql.Row{{"0"}},
				},
			},
		},
	})
}

// TestDatePartNegativeIntervalFieldsUsePostgresNormalizationRepro reproduces an
// interval date_part correctness bug: PostgreSQL extracts normalized negative
// interval fields, but Doltgres floors total nanoseconds and reports larger
// negative hour/minute values.
func TestDatePartNegativeIntervalFieldsUsePostgresNormalizationRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "date_part negative interval fields use PostgreSQL normalization",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT date_part('hour', interval '-65 minutes 10 seconds')::bigint::text;`,
					Expected: []sql.Row{{"-1"}},
				},
				{
					Query:    `SELECT date_part('minute', interval '-65 minutes 10 seconds')::bigint::text;`,
					Expected: []sql.Row{{"-4"}},
				},
				{
					Query:    `SELECT date_part('second', interval '-65 minutes 10 seconds')::bigint::text;`,
					Expected: []sql.Row{{"-50"}},
				},
				{
					Query:    `SELECT date_part('year', interval '-13 months')::bigint::text;`,
					Expected: []sql.Row{{"-1"}},
				},
				{
					Query:    `SELECT date_part('decade', interval '-13 months')::bigint::text;`,
					Expected: []sql.Row{{"0"}},
				},
			},
		},
	})
}

// TestTimetzMinusIntervalSubtractsIntervalRepro reproduces a timetz arithmetic
// correctness bug: PostgreSQL supports time with time zone minus interval, but
// Doltgres registers the operator with its operands reversed.
func TestTimetzMinusIntervalSubtractsIntervalRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timetz minus interval subtracts the interval",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT (time with time zone '04:05:06+00' - interval '2 minutes')::text;`,
					Expected: []sql.Row{{"04:03:06+00"}},
				},
			},
		},
	})
}

// TestIntervalMinusTimetzIsRejectedRepro reproduces a timetz arithmetic
// correctness bug: PostgreSQL rejects interval minus time with time zone, but
// Doltgres accepts the invalid operator because the timetz-minus-interval
// function is registered with reversed operands.
func TestIntervalMinusTimetzIsRejectedRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "interval minus timetz is rejected",
			SetUpScript: []string{
				`SET TIME ZONE 'UTC';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `SELECT (interval '2 minutes' - time with time zone '04:05:06+00')::text;`,
					ExpectedErr: `operator does not exist: interval - time with time zone`,
				},
			},
		},
	})
}
