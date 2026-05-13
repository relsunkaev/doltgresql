// Copyright 2025 Dolthub, Inc.
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

// initArrayLength registers the functions to the catalog.
func initArrayLength() {
	framework.RegisterFunction(array_length_anyarray_int32)
}

// array_length_anyarray_int32 represents the PostgreSQL function of the same name, taking the same parameters.
var array_length_anyarray_int32 = framework.Function2{
	Name:       "array_length",
	Return:     pgtypes.Int32,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.AnyArray, pgtypes.Int32},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		dimension := val2.(int32)
		if length, ok := arrayDimensionLength(val1, dimension); ok {
			return length, nil
		}
		return nil, nil
	},
}

func arrayDimensionLength(val any, dimension int32) (int32, bool) {
	array, ok := pgtypes.ArrayElements(val)
	if !ok {
		return 0, false
	}
	if dimension <= 0 || len(array) == 0 {
		return 0, false
	}
	if dimension == 1 {
		return int32(len(array)), true
	}
	var length int32
	for _, value := range array {
		nested, ok := value.([]any)
		if !ok {
			return 0, false
		}
		nestedLength, ok := arrayDimensionLength(nested, dimension-1)
		if !ok {
			return 0, false
		}
		if length == 0 {
			length = nestedLength
		} else if length != nestedLength {
			return 0, false
		}
	}
	return length, true
}
