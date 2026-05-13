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
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/shopspring/decimal"

	"github.com/dolthub/doltgresql/postgres/parser/duration"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initToChar registers the functions to the catalog.
func initToChar() {
	framework.RegisterFunction(to_char_timestamp_text)
	framework.RegisterFunction(to_char_timestamptz_text)
	framework.RegisterFunction(to_char_interval_text)
	framework.RegisterFunction(to_char_numeric_text)
}

// to_char_timestamp_text represents the PostgreSQL function of the same name, taking the same parameters.
// Postgres date formatting: https://www.postgresql.org/docs/15/functions-formatting.html
var to_char_timestamp_text = framework.Function2{
	Name:       "to_char",
	Return:     pgtypes.Text,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Timestamp, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		timestamp := val1.(time.Time)
		format := val2.(string)

		ttc := timestampTtc(time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), timestamp.Hour(), timestamp.Minute(), timestamp.Second(), timestamp.Nanosecond(), time.UTC))
		ttc.gmtoff = 0
		ttc.tzn = ""
		return tsToChar(ttc, format, false)
	},
}

// to_char_timestamptz_text represents the PostgreSQL function of the same name, taking the same parameters.
var to_char_timestamptz_text = framework.Function2{
	Name:       "to_char",
	Return:     pgtypes.Text,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.TimestampTZ, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		timestamp := val1.(time.Time)
		format := val2.(string)

		loc, err := GetServerLocation(ctx)
		if err != nil {
			return nil, err
		}

		ttc := timestampTtc(timestamp.In(loc))
		return tsToChar(ttc, format, false)
	},
}

// timestampTtc takes time.Time value and converts it to tmToChar value
// which is used for to_char function.
func timestampTtc(ts time.Time) *tmToChar {
	ttc := &tmToChar{}
	ttc.year = ts.Year()
	ttc.mon = int(ts.Month())
	ttc.mday = ts.Day()
	ttc.hour = ts.Hour()
	ttc.min = ts.Minute()
	ttc.sec = ts.Second()
	ttc.fsec = int64(ts.Nanosecond() / 1000)
	tzn, gmtoff := ts.Zone()
	ttc.gmtoff = gmtoff
	if strings.HasPrefix(tzn, "fixed") {
		tzn = ""
	}
	ttc.tzn = tzn

	// calculate wday and yday
	thisDate := date2J(ttc.year, ttc.mon, ttc.mday)
	ttc.wday = j2Day(thisDate)
	ttc.yday = thisDate - date2J(ttc.year, 1, 1) + 1
	return ttc
}

// to_char_interval_text represents the PostgreSQL function of the same name, taking the same parameters.
var to_char_interval_text = framework.Function2{
	Name:       "to_char",
	Return:     pgtypes.Text,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Interval, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		interval := val1.(duration.Duration)
		format := val2.(string)

		ttc := &tmToChar{}
		ttc.year = int(interval.Months) / monthsPerYear
		ttc.mon = int(interval.Months) % monthsPerYear
		ttc.mday = int(interval.Days)
		t := interval.Nanos() / int64(time.Microsecond)

		tFrac := t / (usecsPerSecs * duration.SecsPerHour)
		t -= tFrac * (usecsPerSecs * duration.SecsPerHour)
		ttc.hour = int(tFrac)
		tFrac = t / (usecsPerSecs * duration.SecsPerMinute)
		t -= tFrac * (usecsPerSecs * duration.SecsPerMinute)
		ttc.min = int(tFrac)
		tFrac = t / usecsPerSecs
		t -= tFrac * usecsPerSecs
		ttc.sec = int(tFrac)
		ttc.fsec = t

		return tsToChar(ttc, format, true)
	},
}

// to_char_numeric_text represents the PostgreSQL function of the same name, taking the same parameters.
var to_char_numeric_text = framework.Function2{
	Name:       "to_char",
	Return:     pgtypes.Text,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Numeric, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		return numericToChar(val1.(decimal.Decimal), val2.(string))
	},
}

func numericToChar(value decimal.Decimal, format string) (string, error) {
	fillMode := false
	pattern := format
	if strings.HasPrefix(strings.ToUpper(pattern), "FM") {
		fillMode = true
		pattern = pattern[2:]
	}
	signPattern := false
	if strings.Contains(pattern, "S") {
		if strings.Count(pattern, "S") != 1 || pattern[0] != 'S' {
			return "", errors.Errorf("to_char(numeric,text) format %q is not supported yet", format)
		}
		signPattern = true
		pattern = pattern[1:]
	}
	decimalIdx := strings.IndexByte(pattern, '.')
	integerPattern := pattern
	fractionPattern := ""
	if decimalIdx >= 0 {
		integerPattern = pattern[:decimalIdx]
		fractionPattern = pattern[decimalIdx+1:]
	}
	if !numericToCharPatternSupported(integerPattern, true) || !numericToCharPatternSupported(fractionPattern, false) {
		return "", errors.Errorf("to_char(numeric,text) format %q is not supported yet", format)
	}

	scale := int32(len(fractionPattern))
	negative := value.Sign() < 0
	rounded := value.Abs().Round(scale)
	parts := strings.SplitN(rounded.StringFixed(scale), ".", 2)
	integerText := parts[0]
	fractionText := ""
	if len(parts) == 2 {
		fractionText = parts[1]
	}

	integerOut := numericToCharInteger(integerText, integerPattern, fillMode)
	if signPattern {
		sign := "+"
		if negative {
			sign = "-"
		}
		insertAt := strings.IndexFunc(integerOut, func(r rune) bool { return r >= '0' && r <= '9' })
		if insertAt < 0 {
			insertAt = len(integerOut)
		}
		integerOut = integerOut[:insertAt] + sign + integerOut[insertAt:]
	} else if negative {
		integerOut = "-" + integerOut
	}

	if scale > 0 {
		integerOut += "." + numericToCharFraction(fractionText, fractionPattern)
	}
	if fillMode {
		integerOut = strings.TrimSpace(integerOut)
	}
	return integerOut, nil
}

func numericToCharPatternSupported(pattern string, allowGrouping bool) bool {
	for _, ch := range pattern {
		switch ch {
		case '9', '0':
		case ',':
			if !allowGrouping {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func numericToCharInteger(integerText string, pattern string, fillMode bool) string {
	var reversed strings.Builder
	digitIdx := len(integerText) - 1
	for i := len(pattern) - 1; i >= 0; i-- {
		switch pattern[i] {
		case '9':
			if digitIdx >= 0 {
				reversed.WriteByte(integerText[digitIdx])
				digitIdx--
			} else if !fillMode {
				reversed.WriteByte(' ')
			}
		case '0':
			if digitIdx >= 0 {
				reversed.WriteByte(integerText[digitIdx])
				digitIdx--
			} else {
				reversed.WriteByte('0')
			}
		case ',':
			if !fillMode || digitIdx >= 0 {
				reversed.WriteByte(',')
			}
		}
	}
	for digitIdx >= 0 {
		reversed.WriteByte(integerText[digitIdx])
		digitIdx--
	}
	return reverseASCII(reversed.String())
}

func numericToCharFraction(fractionText string, pattern string) string {
	var out strings.Builder
	digitIdx := 0
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '9', '0':
			if digitIdx < len(fractionText) {
				out.WriteByte(fractionText[digitIdx])
				digitIdx++
			} else {
				out.WriteByte('0')
			}
		}
	}
	return out.String()
}

func reverseASCII(s string) string {
	out := []byte(s)
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return string(out)
}
