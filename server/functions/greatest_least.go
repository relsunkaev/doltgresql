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
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initGreatestLeast registers the functions to the catalog.
func initGreatestLeast() {
	framework.RegisterFunction(greatest_any)
	framework.RegisterFunction(least_any)
}

// greatest_any represents the PostgreSQL function of the same name, taking the same parameters.
var greatest_any = framework.Function1N{
	Name:       "greatest",
	Return:     pgtypes.AnyElement,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.AnyElement},
	Strict:     false,
	Callable: func(ctx *sql.Context, t []*pgtypes.DoltgresType, val1 any, vals []any) (any, error) {
		return greatestLeast(ctx, t, true, val1, vals)
	},
}

// least_any represents the PostgreSQL function of the same name, taking the same parameters.
var least_any = framework.Function1N{
	Name:       "least",
	Return:     pgtypes.AnyElement,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.AnyElement},
	Strict:     false,
	Callable: func(ctx *sql.Context, t []*pgtypes.DoltgresType, val1 any, vals []any) (any, error) {
		return greatestLeast(ctx, t, false, val1, vals)
	},
}

func greatestLeast(ctx *sql.Context, t []*pgtypes.DoltgresType, greatest bool, val1 any, vals []any) (any, error) {
	best := val1
	bestType := t[0]
	if best == nil {
		bestType = nil
	}
	for i, val := range vals {
		if val == nil {
			continue
		}
		valType := t[i+1]
		if best == nil {
			best = val
			bestType = valType
			continue
		}
		cmp, err := bestType.Compare(ctx, best, val)
		if err != nil {
			return nil, err
		}
		if (greatest && cmp < 0) || (!greatest && cmp > 0) {
			best = val
			bestType = valType
		}
	}
	return best, nil
}
