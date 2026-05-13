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

// initArrayReplace registers the functions to the catalog.
func initArrayReplace() {
	framework.RegisterFunction(array_replace_anyarray_anyelement_anyelement)
}

// array_replace_anyarray_anyelement_anyelement represents the PostgreSQL function of the same name.
var array_replace_anyarray_anyelement_anyelement = framework.Function3{
	Name:       "array_replace",
	Return:     pgtypes.AnyArray,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.AnyArray, pgtypes.AnyElement, pgtypes.AnyElement},
	Strict:     false,
	Callable: func(ctx *sql.Context, t [4]*pgtypes.DoltgresType, val1 any, val2 any, val3 any) (any, error) {
		if val1 == nil {
			return nil, nil
		}

		array := val1.([]any)
		baseType := t[0].ArrayBaseType()
		result := make([]any, 0, len(array))
		for _, element := range array {
			if element == nil || val2 == nil {
				if element == nil && val2 == nil {
					result = append(result, val3)
				} else {
					result = append(result, element)
				}
				continue
			}
			cmp, err := baseType.Compare(ctx, element, val2)
			if err != nil {
				return nil, err
			}
			if cmp == 0 {
				result = append(result, val3)
			} else {
				result = append(result, element)
			}
		}
		return result, nil
	},
}
