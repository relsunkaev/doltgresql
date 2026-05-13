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

// initArrayFill registers the functions to the catalog.
func initArrayFill() {
	framework.RegisterFunction(array_fill_anyelement_int32array)
}

// array_fill_anyelement_int32array represents the PostgreSQL function of the same name, taking the same parameters.
var array_fill_anyelement_int32array = framework.Function2{
	Name:       "array_fill",
	Return:     pgtypes.AnyArray,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.AnyElement, pgtypes.Int32Array},
	Strict:     false,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, value any, dims any) (any, error) {
		if dims == nil {
			return nil, nil
		}
		dimensions, err := arrayFillDimensions(dims.([]any))
		if err != nil {
			return nil, err
		}
		return buildFilledArray(value, dimensions), nil
	},
}

func arrayFillDimensions(values []any) ([]int32, error) {
	dimensions := make([]int32, len(values))
	for i, value := range values {
		if value == nil {
			return nil, errors.New("dimension values cannot be null")
		}
		dimension, ok := value.(int32)
		if !ok {
			return nil, errors.Errorf("unexpected array_fill dimension type %T", value)
		}
		if dimension < 0 {
			return nil, errors.New("dimension values cannot be negative")
		}
		dimensions[i] = dimension
	}
	return dimensions, nil
}

func buildFilledArray(value any, dimensions []int32) []any {
	if len(dimensions) == 0 || dimensions[0] == 0 {
		return []any{}
	}
	result := make([]any, dimensions[0])
	if len(dimensions) == 1 {
		for i := range result {
			result[i] = value
		}
		return result
	}
	for i := range result {
		result[i] = buildFilledArray(value, dimensions[1:])
	}
	return result
}
