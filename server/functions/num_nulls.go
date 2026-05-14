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

func initNumNulls() {
	framework.RegisterFunction(num_nulls_any)
	framework.RegisterFunction(num_nonnulls_any)
}

var num_nulls_any = framework.Function1N{
	Name:       "num_nulls",
	Return:     pgtypes.Int32,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Any},
	Strict:     false,
	Callable: func(ctx *sql.Context, _ []*pgtypes.DoltgresType, val1 any, vals []any) (any, error) {
		return countNulls(ctx, append([]any{val1}, vals...), true)
	},
}

var num_nonnulls_any = framework.Function1N{
	Name:       "num_nonnulls",
	Return:     pgtypes.Int32,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Any},
	Strict:     false,
	Callable: func(ctx *sql.Context, _ []*pgtypes.DoltgresType, val1 any, vals []any) (any, error) {
		return countNulls(ctx, append([]any{val1}, vals...), false)
	},
}

func countNulls(ctx *sql.Context, vals []any, wantNulls bool) (int32, error) {
	var count int32
	for _, val := range vals {
		unwrapped, err := sql.UnwrapAny(ctx, val)
		if err != nil {
			return 0, err
		}
		if (unwrapped == nil) == wantNulls {
			count++
		}
	}
	return count, nil
}
