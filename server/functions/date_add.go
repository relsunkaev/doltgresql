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

package functions

import (
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/postgres/parser/duration"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func initDateAdd() {
	framework.RegisterFunction(date_add_timestamptz_interval_text)
	framework.RegisterFunction(date_subtract_timestamptz_interval_text)
}

var date_add_timestamptz_interval_text = framework.Function3{
	Name:       "date_add",
	Return:     pgtypes.TimestampTZ,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.TimestampTZ, pgtypes.Interval, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, ts any, interval any, timezone any) (any, error) {
		return dateAddInTimezone(ts.(time.Time), interval.(duration.Duration), timezone.(string))
	},
}

var date_subtract_timestamptz_interval_text = framework.Function3{
	Name:       "date_subtract",
	Return:     pgtypes.TimestampTZ,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.TimestampTZ, pgtypes.Interval, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, ts any, interval any, timezone any) (any, error) {
		return dateAddInTimezone(ts.(time.Time), interval.(duration.Duration).Mul(-1), timezone.(string))
	},
}

func dateAddInTimezone(ts time.Time, interval duration.Duration, timezone string) (time.Time, error) {
	loc, _, _, err := convertTzToOffsetSecs(ts, timezone)
	if err != nil {
		return time.Time{}, err
	}
	return duration.Add(ts.In(loc), interval).In(ts.Location()), nil
}
