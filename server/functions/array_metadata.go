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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initArrayMetadata registers the functions to the catalog.
func initArrayMetadata() {
	framework.RegisterFunction(array_dims_anyarray)
	framework.RegisterFunction(array_lower_anyarray_int32)
	framework.RegisterFunction(array_ndims_anyarray)
	framework.RegisterFunction(cardinality_anyarray)
}

// array_dims_anyarray represents the PostgreSQL function of the same name, taking the same parameters.
var array_dims_anyarray = framework.Function1{
	Name:       "array_dims",
	Return:     pgtypes.Text,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.AnyArray},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		dimensions := arrayDimensions(val.([]any))
		if len(dimensions) == 0 {
			return nil, nil
		}
		sb := strings.Builder{}
		for _, length := range dimensions {
			sb.WriteString(fmt.Sprintf("[1:%d]", length))
		}
		return sb.String(), nil
	},
}

// array_lower_anyarray_int32 represents the PostgreSQL function of the same name, taking the same parameters.
var array_lower_anyarray_int32 = framework.Function2{
	Name:       "array_lower",
	Return:     pgtypes.Int32,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.AnyArray, pgtypes.Int32},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		array := val1.([]any)
		dimension := val2.(int32)
		if _, ok := arrayDimensionLength(array, dimension); ok {
			return int32(1), nil
		}
		return nil, nil
	},
}

// array_ndims_anyarray represents the PostgreSQL function of the same name, taking the same parameters.
var array_ndims_anyarray = framework.Function1{
	Name:       "array_ndims",
	Return:     pgtypes.Int32,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.AnyArray},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		dimensions := arrayDimensions(val.([]any))
		if len(dimensions) == 0 {
			return nil, nil
		}
		return int32(len(dimensions)), nil
	},
}

// cardinality_anyarray represents the PostgreSQL function of the same name, taking the same parameters.
var cardinality_anyarray = framework.Function1{
	Name:       "cardinality",
	Return:     pgtypes.Int32,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.AnyArray},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return arrayCardinality(val.([]any)), nil
	},
}

func arrayDimensions(array []any) []int32 {
	if len(array) == 0 {
		return nil
	}
	dimensions := []int32{int32(len(array))}
	nested, ok := array[0].([]any)
	if !ok {
		return dimensions
	}
	nestedDimensions := arrayDimensions(nested)
	if len(nestedDimensions) == 0 {
		return dimensions
	}
	return append(dimensions, nestedDimensions...)
}

func arrayCardinality(array []any) int32 {
	var count int32
	for _, value := range array {
		if nested, ok := value.([]any); ok {
			count += arrayCardinality(nested)
		} else {
			count++
		}
	}
	return count
}
