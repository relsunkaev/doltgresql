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
	"math"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initArrayPosition registers the functions to the catalog.
func initArrayPosition() {
	framework.RegisterFunction(array_position_anyarray_anyelement)
	framework.RegisterFunction(array_position_anyarray_anyelement_int32)
	framework.RegisterFunction(array_position_int2array_oid)
	framework.RegisterFunction(array_position_int2vector_int2)
	framework.RegisterFunction(array_position_unknown_unknown)
	framework.RegisterFunction(array_position_unknown_unknown_int32)
	framework.RegisterFunction(array_positions_anyarray_anyelement)
}

// array_position_anyarray_anyelement represents the PostgreSQL function of the same name, taking the same parameters.
var array_position_anyarray_anyelement = framework.Function2{
	Name:       "array_position",
	Return:     pgtypes.Int32,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.AnyArray, pgtypes.AnyElement},
	Strict:     false,
	Callable: func(ctx *sql.Context, t [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		if val1 == nil {
			return nil, nil
		}

		array, _ := pgtypes.ArrayElements(val1)
		searchElement := val2
		arrayType := t[0]
		baseType := arrayType.ArrayBaseType()
		lowerBound := pgtypes.ArrayLowerBound(val1, 1)

		for i, element := range array {
			cmp, err := baseType.Compare(ctx, element, searchElement)
			if err != nil {
				return nil, err
			}
			if cmp == 0 {
				return lowerBound + int32(i), nil
			}
		}

		// Element not found
		return nil, nil
	},
}

// array_position_anyarray_anyelement_int32 represents the PostgreSQL function of the same name, taking the same parameters.
var array_position_anyarray_anyelement_int32 = framework.Function3{
	Name:       "array_position",
	Return:     pgtypes.Int32,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.AnyArray, pgtypes.AnyElement, pgtypes.Int32},
	Strict:     false,
	Callable: func(ctx *sql.Context, t [4]*pgtypes.DoltgresType, val1 any, val2 any, val3 any) (any, error) {
		if val1 == nil {
			return nil, nil
		}

		array, _ := pgtypes.ArrayElements(val1)
		searchElement := val2
		start := val3.(int32)
		arrayType := t[0]
		baseType := arrayType.ArrayBaseType()
		lowerBound := pgtypes.ArrayLowerBound(val1, 1)

		startIdx := int(start - lowerBound)
		if startIdx < 0 {
			startIdx = 0
		}
		if startIdx >= len(array) {
			return nil, nil
		}

		// Search for the element starting from the specified position
		for i := startIdx; i < len(array); i++ {
			cmp, err := baseType.Compare(ctx, array[i], searchElement)
			if err != nil {
				return nil, err
			}
			if cmp == 0 {
				return lowerBound + int32(i), nil
			}
		}

		// Element not found
		return nil, nil
	},
}

var array_position_unknown_unknown = framework.Function2{
	Name:       "array_position",
	Return:     pgtypes.Int32,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Unknown, pgtypes.Unknown},
	Strict:     false,
	Callable: func(ctx *sql.Context, t [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		return nil, nil
	},
}

var array_position_unknown_unknown_int32 = framework.Function3{
	Name:       "array_position",
	Return:     pgtypes.Int32,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Unknown, pgtypes.Unknown, pgtypes.Int32},
	Strict:     false,
	Callable: func(ctx *sql.Context, t [4]*pgtypes.DoltgresType, val1 any, val2 any, val3 any) (any, error) {
		return nil, nil
	},
}

// array_position_int2vector_int2 represents the PostgreSQL function of the same name for the int2vector catalog type.
var array_position_int2vector_int2 = framework.Function2{
	Name:       "array_position",
	Return:     pgtypes.Int32,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Int16vector, pgtypes.Int16},
	Strict:     false,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		if val1 == nil || val2 == nil {
			return nil, nil
		}

		searchElement := val2.(int16)
		array, _ := pgtypes.ArrayElements(val1)
		lowerBound := pgtypes.ArrayLowerBound(val1, 1)
		for i, element := range array {
			cmp, err := pgtypes.Int16.Compare(ctx, element, searchElement)
			if err != nil {
				return nil, err
			}
			if cmp == 0 {
				return lowerBound + int32(i), nil
			}
		}

		return nil, nil
	},
}

var array_position_int2array_oid = framework.Function2{
	Name:       "array_position",
	Return:     pgtypes.Int32,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Int16Array, pgtypes.Oid},
	Strict:     false,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		if val1 == nil || val2 == nil {
			return nil, nil
		}
		searchElement, ok := int16SearchElement(val2)
		if !ok {
			return nil, nil
		}
		array, _ := pgtypes.ArrayElements(val1)
		lowerBound := pgtypes.ArrayLowerBound(val1, 1)
		for i, element := range array {
			cmp, err := pgtypes.Int16.Compare(ctx, element, searchElement)
			if err != nil {
				return nil, err
			}
			if cmp == 0 {
				return lowerBound + int32(i), nil
			}
		}
		return nil, nil
	},
}

func int16SearchElement(val any) (int16, bool) {
	switch v := val.(type) {
	case int16:
		return v, true
	case int32:
		if v < math.MinInt16 || v > math.MaxInt16 {
			return 0, false
		}
		return int16(v), true
	case int64:
		if v < math.MinInt16 || v > math.MaxInt16 {
			return 0, false
		}
		return int16(v), true
	case id.Id:
		oid := id.Cache().ToOID(v)
		if oid > math.MaxInt16 {
			return 0, false
		}
		return int16(oid), true
	default:
		return 0, false
	}
}

// array_positions_anyarray_anyelement represents the PostgreSQL function of the same name, taking the same parameters.
var array_positions_anyarray_anyelement = framework.Function2{
	Name:       "array_positions",
	Return:     pgtypes.Int32Array,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.AnyArray, pgtypes.AnyElement},
	Strict:     false,
	Callable: func(ctx *sql.Context, t [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		if val1 == nil {
			return nil, nil
		}

		array, _ := pgtypes.ArrayElements(val1)
		searchElement := val2
		arrayType := t[0]
		baseType := arrayType.ArrayBaseType()
		lowerBound := pgtypes.ArrayLowerBound(val1, 1)
		var positions []any

		for i, element := range array {
			cmp, err := baseType.Compare(ctx, element, searchElement)
			if err != nil {
				return nil, err
			}
			if cmp == 0 {
				positions = append(positions, lowerBound+int32(i))
			}
		}

		// Return array of positions, or empty array if no matches
		return positions, nil
	},
}
