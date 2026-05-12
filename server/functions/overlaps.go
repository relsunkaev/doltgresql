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

// initOverlaps registers helper functions used by SQL-standard OVERLAPS syntax.
func initOverlaps() {
	framework.RegisterFunction(doltgres_overlaps_date_date)
	framework.RegisterFunction(doltgres_overlaps_date_interval)
}

var doltgres_overlaps_date_date = framework.Function4{
	Name:       "__doltgres_overlaps",
	Return:     pgtypes.Bool,
	Parameters: [4]*pgtypes.DoltgresType{pgtypes.Date, pgtypes.Date, pgtypes.Date, pgtypes.Date},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [5]*pgtypes.DoltgresType, start1, end1, start2, end2 any) (any, error) {
		return periodsOverlap(start1.(time.Time), end1.(time.Time), start2.(time.Time), end2.(time.Time)), nil
	},
}

var doltgres_overlaps_date_interval = framework.Function4{
	Name:       "__doltgres_overlaps",
	Return:     pgtypes.Bool,
	Parameters: [4]*pgtypes.DoltgresType{pgtypes.Date, pgtypes.Interval, pgtypes.Date, pgtypes.Interval},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [5]*pgtypes.DoltgresType, start1, interval1, start2, interval2 any) (any, error) {
		s1 := start1.(time.Time)
		s2 := start2.(time.Time)
		e1 := duration.Add(s1, interval1.(duration.Duration))
		e2 := duration.Add(s2, interval2.(duration.Duration))
		return periodsOverlap(s1, e1, s2, e2), nil
	},
}

func periodsOverlap(start1, end1, start2, end2 time.Time) bool {
	if end1.Before(start1) {
		start1, end1 = end1, start1
	}
	if end2.Before(start2) {
		start2, end2 = end2, start2
	}
	return start1.Before(end2) && start2.Before(end1)
}
