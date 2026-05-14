// Copyright 2024 Dolthub, Inc.
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

package functions

import (
	"math"
	"math/big"
	"strings"
	"time"

	cerrors "github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/shopspring/decimal"

	"github.com/dolthub/doltgresql/postgres/parser/duration"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/postgres/parser/timeofday"
	"github.com/dolthub/doltgresql/postgres/parser/timetz"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initExtract registers the functions to the catalog.
func initExtract() {
	framework.RegisterFunction(extract_text_date)
	framework.RegisterFunction(extract_text_time)
	framework.RegisterFunction(extract_text_timetz)
	framework.RegisterFunction(extract_text_timestamp)
	framework.RegisterFunction(extract_text_timestamptz)
	framework.RegisterFunction(extract_text_interval)
}

func newUnitNotSupportedError(field string, typ string) error {
	return pgerror.Newf(pgcode.FeatureNotSupported, `unit "%s" not supported for type %s`, field, typ)
}

// extract_text_date represents the PostgreSQL date/time function, taking {text, date}
var extract_text_date = framework.Function2{
	Name:               "extract",
	Return:             pgtypes.Numeric,
	Parameters:         [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Date},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		field := val1.(string)
		dateVal := val2.(time.Time)
		switch strings.ToLower(field) {
		case "hour", "hours", "microsecond", "microseconds", "millisecond", "milliseconds",
			"minute", "minutes", "second", "seconds", "timezone", "timezone_hour", "timezone_minute":
			return nil, newUnitNotSupportedError(field, "date")
		case "epoch":
			return decimal.NewFromFloat(float64(dateVal.UnixMicro()) / 1000000), nil
		default:
			return getFieldFromTimeVal(field, dateVal)
		}
	},
}

// extract_text_time represents the PostgreSQL date/time function, taking {text, time without time zone}
var extract_text_time = framework.Function2{
	Name:               "extract",
	Return:             pgtypes.Numeric,
	Parameters:         [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Time},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		field := val1.(string)
		timeVal := val2.(timeofday.TimeOfDay).ToTime()
		switch strings.ToLower(field) {
		case "century", "centuries", "day", "days", "decade", "decades", "dow", "doy",
			"isodow", "isoyear", "julian", "millennium", "millenniums", "month", "months",
			"quarter", "timezone", "timezone_hour", "timezone_minute", "week", "year", "years":
			return nil, newUnitNotSupportedError(field, "time without time zone")
		default:
			return getFieldFromTimeVal(field, timeVal)
		}
	},
}

// extract_text_timetz represents the PostgreSQL date/time function, taking {text, time with time zone}
var extract_text_timetz = framework.Function2{
	Name:               "extract",
	Return:             pgtypes.Numeric,
	Parameters:         [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.TimeTZ},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		field := val1.(string)
		timetzVal := val2.(timetz.TimeTZ).ToTime()
		_, currentOffset := timetzVal.Zone()
		switch strings.ToLower(field) {
		case "century", "centuries", "day", "days", "decade", "decades", "dow", "doy",
			"isodow", "isoyear", "julian", "millennium", "millenniums", "month", "months",
			"quarter", "week", "year", "years":
			return nil, newUnitNotSupportedError(field, "time with time zone")
		case "timezone":
			return decimal.NewFromInt(-int64(-currentOffset)), nil
		case "timezone_hour":
			return decimal.NewFromInt(-int64(-currentOffset / 3600)), nil
		case "timezone_minute":
			return decimal.NewFromInt(-int64((-currentOffset % 3600) / 60)), nil
		default:
			return getFieldFromTimeVal(field, timetzVal)
		}
	},
}

// extract_text_timestamp represents the PostgreSQL date/time function, taking {text, timestamp without time zone}
var extract_text_timestamp = framework.Function2{
	Name:               "extract",
	Return:             pgtypes.Numeric,
	Parameters:         [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Timestamp},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		field := val1.(string)
		tsVal := val2.(time.Time)
		switch strings.ToLower(field) {
		case "timezone", "timezone_hour", "timezone_minute":
			return nil, newUnitNotSupportedError(field, "timestamp without time zone")
		default:
			return getFieldFromTimeVal(field, tsVal)
		}
	},
}

// extract_text_timestamptz represents the PostgreSQL date/time function, taking {text, timestamp with time zone}
var extract_text_timestamptz = framework.Function2{
	Name:               "extract",
	Return:             pgtypes.Numeric,
	Parameters:         [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.TimestampTZ},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		field := val1.(string)
		loc, err := GetServerLocation(ctx)
		if err != nil {
			return nil, err
		}
		tstzVal := val2.(time.Time).In(loc)
		_, currentOffset := tstzVal.Zone()
		switch strings.ToLower(field) {
		case "timezone":
			return decimal.NewFromInt(int64(currentOffset)), nil
		case "timezone_hour":
			return decimal.NewFromInt(int64(currentOffset / duration.SecsPerHour)), nil
		case "timezone_minute":
			return decimal.NewFromInt(int64((currentOffset % duration.SecsPerHour) / duration.SecsPerMinute)), nil
		default:
			return getFieldFromTimeVal(field, tstzVal)
		}
	},
}

const (
	NanosPerMicro = 1000
	NanosPerMilli = NanosPerMicro * duration.MicrosPerMilli
	NanosPerSec   = NanosPerMicro * duration.MicrosPerMilli * duration.MillisPerSec
)

func intervalFieldFromMonths(months int64, monthsPerField int64) int64 {
	return months / monthsPerField
}

func intervalHourField(nanos int64) int64 {
	return nanos / (NanosPerSec * duration.SecsPerHour)
}

func intervalMinuteField(nanos int64) int64 {
	return (nanos % (NanosPerSec * duration.SecsPerHour)) / (NanosPerSec * duration.SecsPerMinute)
}

func julianFromTime(tVal time.Time) decimal.Decimal {
	julian := decimal.NewFromInt(int64(date2J(tVal.Year(), int(tVal.Month()), tVal.Day())))
	nanosOfDay := int64(tVal.Hour())*NanosPerSec*duration.SecsPerHour +
		int64(tVal.Minute())*NanosPerSec*duration.SecsPerMinute +
		int64(tVal.Second())*NanosPerSec +
		int64(tVal.Nanosecond())
	if nanosOfDay == 0 {
		return julian
	}
	fraction := decimal.NewFromBigRat(
		new(big.Rat).SetFrac(
			big.NewInt(nanosOfDay),
			big.NewInt(NanosPerSec*duration.SecsPerDay),
		),
		20,
	)
	return julian.Add(fraction)
}

// extract_text_interval represents the PostgreSQL date/time function, taking {text, interval}
var extract_text_interval = framework.Function2{
	Name:               "extract",
	Return:             pgtypes.Numeric,
	Parameters:         [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Interval},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		field := val1.(string)
		dur := val2.(duration.Duration)
		switch strings.ToLower(field) {
		case "century", "centuries":
			return decimal.NewFromInt(intervalFieldFromMonths(dur.Months, duration.MonthsPerYear*100)), nil
		case "day", "days":
			return decimal.NewFromInt(dur.Days), nil
		case "decade", "decades":
			return decimal.NewFromInt(intervalFieldFromMonths(dur.Months, duration.MonthsPerYear*10)), nil
		case "epoch":
			epoch := float64(duration.SecsPerDay*duration.DaysPerMonth*dur.Months) + float64(duration.SecsPerDay*dur.Days) +
				(float64(dur.Nanos()) / (NanosPerSec))
			return decimal.NewFromString(decimal.NewFromFloat(epoch).StringFixed(6))
		case "hour", "hours":
			return decimal.NewFromInt(intervalHourField(dur.Nanos())), nil
		case "microsecond", "microseconds":
			secondsInNanos := dur.Nanos() % (NanosPerSec * duration.SecsPerMinute)
			microseconds := float64(secondsInNanos) / NanosPerMicro
			return decimal.NewFromFloat(microseconds), nil
		case "millennium", "millenniums":
			return decimal.NewFromInt(intervalFieldFromMonths(dur.Months, duration.MonthsPerYear*1000)), nil
		case "millisecond", "milliseconds":
			secondsInNanos := dur.Nanos() % (NanosPerSec * duration.SecsPerMinute)
			milliseconds := float64(secondsInNanos) / NanosPerMilli
			return decimal.NewFromString(decimal.NewFromFloat(milliseconds).StringFixed(3))
		case "minute", "minutes":
			return decimal.NewFromInt(intervalMinuteField(dur.Nanos())), nil
		case "month", "months":
			return decimal.NewFromInt(dur.Months % 12), nil
		case "quarter":
			return decimal.NewFromInt((dur.Months%12)/3 + 1), nil
		case "second", "seconds":
			secondsInNanos := dur.Nanos() % (NanosPerSec * duration.SecsPerMinute)
			seconds := float64(secondsInNanos) / NanosPerSec
			return decimal.NewFromString(decimal.NewFromFloat(seconds).StringFixed(6))
		case "year", "years":
			return decimal.NewFromInt(intervalFieldFromMonths(dur.Months, duration.MonthsPerYear)), nil
		case "dow", "doy", "isodow", "isoyear", "julian", "timezone", "timezone_hour", "timezone_minute", "week":
			return nil, newUnitNotSupportedError(field, "interval")
		default:
			return nil, cerrors.Errorf("unknown field given: %s", field)
		}
	},
}

// getFieldFromTimeVal returns the value for given field extracted from non-interval values.
func getFieldFromTimeVal(field string, tVal time.Time) (decimal.Decimal, error) {
	switch strings.ToLower(field) {
	case "century", "centuries":
		if year := tVal.Year(); year <= 0 {
			return decimal.NewFromFloat(math.Floor(float64(year-1) / 100)), nil
		} else {
			return decimal.NewFromFloat(math.Ceil(float64(year) / 100)), nil
		}
	case "day", "days":
		return decimal.NewFromInt(int64(tVal.Day())), nil
	case "decade", "decades":
		return decimal.NewFromFloat(math.Floor(float64(tVal.Year()) / 10)), nil
	case "dow":
		return decimal.NewFromInt(int64(tVal.Weekday())), nil
	case "doy":
		return decimal.NewFromInt(int64(tVal.YearDay())), nil
	case "epoch":
		return decimal.NewFromString(decimal.NewFromFloat(float64(tVal.UnixMicro()) / 1000000).StringFixed(6))
	case "hour", "hours":
		return decimal.NewFromInt(int64(tVal.Hour())), nil
	case "isodow":
		wd := int64(tVal.Weekday())
		if wd == 0 {
			wd = 7
		}
		return decimal.NewFromInt(wd), nil
	case "isoyear":
		year, _ := tVal.ISOWeek()
		return decimal.NewFromInt(int64(year)), nil
	case "julian":
		return julianFromTime(tVal), nil
	case "microsecond", "microseconds", "usec", "usecs":
		w := float64(tVal.Second() * 1000000)
		f := float64(tVal.Nanosecond()) / float64(1000)
		return decimal.NewFromFloat(w + f), nil
	case "millennium", "millenniums":
		return decimal.NewFromFloat(math.Ceil(float64(tVal.Year()) / 1000)), nil
	case "millisecond", "milliseconds", "msec", "msecs":
		w := float64(tVal.Second() * 1000)
		f := float64(tVal.Nanosecond()) / float64(1000000)
		return decimal.NewFromString(decimal.NewFromFloat(w + f).StringFixed(3))
	case "minute", "minutes":
		return decimal.NewFromInt(int64(tVal.Minute())), nil
	case "month", "months":
		return decimal.NewFromInt(int64(tVal.Month())), nil
	case "quarter":
		q := (int(tVal.Month())-1)/3 + 1
		return decimal.NewFromInt(int64(q)), nil
	case "second", "seconds":
		w := float64(tVal.Second())
		f := float64(tVal.Nanosecond()) / float64(1000000000)
		return decimal.NewFromString(decimal.NewFromFloat(w + f).StringFixed(6))
	case "week":
		_, week := tVal.ISOWeek()
		return decimal.NewFromInt(int64(week)), nil
	case "year", "years":
		return decimal.NewFromInt(int64(tVal.Year())), nil
	default:
		return decimal.Decimal{}, cerrors.Errorf("unknown field given: %s", field)
	}
}
