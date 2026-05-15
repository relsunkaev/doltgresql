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

package binary

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	pgfunctions "github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

var citextType = pgtypes.NewUnresolvedDoltgresType("public", "citext")

func initCitext() {
	framework.RegisterFunction(citext_cmp)
	framework.RegisterFunction(citext_hash)
	framework.RegisterFunction(citext_hash_extended)
}

func citextCompare(ctx *sql.Context, params [3]*pgtypes.DoltgresType, val1 any, val2 any) (int, error) {
	return params[0].Compare(ctx, val1, val2)
}

var citext_cmp = framework.Function2{
	Name:       "citext_cmp",
	Return:     pgtypes.Int32,
	Parameters: [2]*pgtypes.DoltgresType{citextType, citextType},
	Strict:     true,
	Callable: func(ctx *sql.Context, params [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		res, err := citextCompare(ctx, params, val1, val2)
		return int32(res), err
	},
}

var citext_hash = framework.Function1{
	Name:       "citext_hash",
	Return:     pgtypes.Int32,
	Parameters: [1]*pgtypes.DoltgresType{citextType},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return int32(pgfunctions.PgHashBytes([]byte(strings.ToLower(val.(string))))), nil
	},
}

var citext_hash_extended = framework.Function2{
	Name:       "citext_hash_extended",
	Return:     pgtypes.Int64,
	Parameters: [2]*pgtypes.DoltgresType{citextType, pgtypes.Int64},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		return int64(pgfunctions.PgHashBytesExtended([]byte(strings.ToLower(val1.(string))), uint64(val2.(int64)))), nil
	},
}

var citext_eq = framework.Function2{
	Name:       "citext_eq",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{citextType, citextType},
	Strict:     true,
	Callable: func(ctx *sql.Context, params [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		res, err := citextCompare(ctx, params, val1, val2)
		return res == 0, err
	},
}

var citext_ne = framework.Function2{
	Name:       "citext_ne",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{citextType, citextType},
	Strict:     true,
	Callable: func(ctx *sql.Context, params [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		res, err := citextCompare(ctx, params, val1, val2)
		return res != 0, err
	},
}

var citext_lt = framework.Function2{
	Name:       "citext_lt",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{citextType, citextType},
	Strict:     true,
	Callable: func(ctx *sql.Context, params [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		res, err := citextCompare(ctx, params, val1, val2)
		return res < 0, err
	},
}

var citext_le = framework.Function2{
	Name:       "citext_le",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{citextType, citextType},
	Strict:     true,
	Callable: func(ctx *sql.Context, params [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		res, err := citextCompare(ctx, params, val1, val2)
		return res <= 0, err
	},
}

var citext_gt = framework.Function2{
	Name:       "citext_gt",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{citextType, citextType},
	Strict:     true,
	Callable: func(ctx *sql.Context, params [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		res, err := citextCompare(ctx, params, val1, val2)
		return res > 0, err
	},
}

var citext_ge = framework.Function2{
	Name:       "citext_ge",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{citextType, citextType},
	Strict:     true,
	Callable: func(ctx *sql.Context, params [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		res, err := citextCompare(ctx, params, val1, val2)
		return res >= 0, err
	},
}
