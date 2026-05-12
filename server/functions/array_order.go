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

func initArrayOrder() {
	framework.RegisterFunction(array_sort_anyarray)
	framework.RegisterFunction(array_reverse_anyarray)
}

var array_sort_anyarray = framework.Function1{
	Name:       "array_sort",
	Return:     pgtypes.AnyArray,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.AnyArray},
	Strict:     true,
	Callable: func(ctx *sql.Context, t [2]*pgtypes.DoltgresType, val any) (any, error) {
		out := append([]any(nil), val.([]any)...)
		baseType := t[0].ArrayBaseType()
		for i := 1; i < len(out); i++ {
			for j := i; j > 0; j-- {
				less, err := arrayElementLess(ctx, baseType, out[j], out[j-1])
				if err != nil {
					return nil, err
				}
				if !less {
					break
				}
				out[j], out[j-1] = out[j-1], out[j]
			}
		}
		return out, nil
	},
}

var array_reverse_anyarray = framework.Function1{
	Name:       "array_reverse",
	Return:     pgtypes.AnyArray,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.AnyArray},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		out := append([]any(nil), val.([]any)...)
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}
		return out, nil
	},
}

func arrayElementLess(ctx *sql.Context, typ *pgtypes.DoltgresType, left any, right any) (bool, error) {
	if left == nil || right == nil {
		return right != nil, nil
	}
	cmp, err := typ.Compare(ctx, left, right)
	if err != nil {
		return false, err
	}
	return cmp < 0, nil
}
