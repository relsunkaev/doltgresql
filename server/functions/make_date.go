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

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initMakeDate registers the functions to the catalog.
func initMakeDate() {
	framework.RegisterFunction(make_date)
}

// make_date represents the PostgreSQL function of the same name, taking the same parameters.
var make_date = framework.Function3{
	Name:       "make_date",
	Return:     pgtypes.Date,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Int32, pgtypes.Int32, pgtypes.Int32},
	ParameterNames: [3]string{
		"year",
		"month",
		"day",
	},
	Strict: true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, val1, val2, val3 any) (any, error) {
		year, month, day := val1.(int32), val2.(int32), val3.(int32)
		if year == 0 {
			return time.Time{}, errDateFieldOutOfRange
		} else if year < 0 {
			year++
		}
		if month < 1 || month > 12 {
			return time.Time{}, errDateFieldOutOfRange
		}
		if day < 1 || day > 31 {
			return time.Time{}, errDateFieldOutOfRange
		}
		d := time.Date(int(year), time.Month(month), int(day), 0, 0, 0, 0, time.UTC)
		if !sameDateParts(d, year, month, day) {
			return time.Time{}, errDateFieldOutOfRange
		}
		return d, nil
	},
}
