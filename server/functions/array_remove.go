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
	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initArrayRemove registers the functions to the catalog.
func initArrayRemove() {
	framework.RegisterFunction(array_remove_anyarray_anyelement)
}

// array_remove_anyarray_anyelement represents the PostgreSQL function of the
// same name, taking the same parameters.
var array_remove_anyarray_anyelement = framework.Function2{
	Name:       "array_remove",
	Return:     pgtypes.AnyArray,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.AnyArray, pgtypes.AnyElement},
	Strict:     false,
	Callable: func(ctx *sql.Context, t [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		if val1 == nil {
			return nil, nil
		}

		array := val1.([]any)
		baseType := t[0].ArrayBaseType()
		result := make([]any, 0, len(array))
		for _, element := range array {
			if _, ok := element.([]any); ok {
				return nil, errors.New("removing elements from multidimensional arrays is not supported")
			}
			if element == nil || val2 == nil {
				if element == nil && val2 == nil {
					continue
				}
				result = append(result, element)
				continue
			}
			cmp, err := baseType.Compare(ctx, element, val2)
			if err != nil {
				return nil, err
			}
			if cmp == 0 {
				continue
			}
			result = append(result, element)
		}
		return result, nil
	},
}
