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
	"math"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func initVector() {
	framework.RegisterBinaryFunction(framework.Operator_BinaryLessThan, vector_lt)
	framework.RegisterBinaryFunction(framework.Operator_BinaryLessOrEqual, vector_le)
	framework.RegisterBinaryFunction(framework.Operator_BinaryVectorL2Distance, l2_distance)
	framework.RegisterFunction(inner_product)
	framework.RegisterBinaryFunction(framework.Operator_BinaryVectorNegativeInnerProduct, vector_negative_inner_product)
	framework.RegisterBinaryFunction(framework.Operator_BinaryVectorCosineDistance, cosine_distance)
	framework.RegisterBinaryFunction(framework.Operator_BinaryVectorL1Distance, l1_distance)
	framework.RegisterFunction(vector_l2_squared_distance)
	framework.RegisterBinaryFunction(framework.Operator_BinaryGreaterOrEqual, vector_ge)
	framework.RegisterBinaryFunction(framework.Operator_BinaryGreaterThan, vector_gt)
}

var vector_lt = framework.Function2{
	Name:       "vector_lt",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Vector, pgtypes.Vector},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		return pgtypes.CompareVectors(val1.([]float32), val2.([]float32)) < 0, nil
	},
}

var vector_le = framework.Function2{
	Name:       "vector_le",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Vector, pgtypes.Vector},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		return pgtypes.CompareVectors(val1.([]float32), val2.([]float32)) <= 0, nil
	},
}

var l2_distance = framework.Function2{
	Name:       "l2_distance",
	Return:     pgtypes.Float64,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Vector, pgtypes.Vector},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		left, right, err := vectorDistanceInputs(val1, val2)
		if err != nil {
			return nil, err
		}
		return math.Sqrt(vectorL2SquaredDistance(left, right)), nil
	},
}

var vector_l2_squared_distance = framework.Function2{
	Name:       "vector_l2_squared_distance",
	Return:     pgtypes.Float64,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Vector, pgtypes.Vector},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		left, right, err := vectorDistanceInputs(val1, val2)
		if err != nil {
			return nil, err
		}
		return vectorL2SquaredDistance(left, right), nil
	},
}

var inner_product = framework.Function2{
	Name:       "inner_product",
	Return:     pgtypes.Float64,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Vector, pgtypes.Vector},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		left, right, err := vectorDistanceInputs(val1, val2)
		if err != nil {
			return nil, err
		}
		return vectorInnerProduct(left, right), nil
	},
}

var vector_negative_inner_product = framework.Function2{
	Name:       "vector_negative_inner_product",
	Return:     pgtypes.Float64,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Vector, pgtypes.Vector},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		left, right, err := vectorDistanceInputs(val1, val2)
		if err != nil {
			return nil, err
		}
		return -vectorInnerProduct(left, right), nil
	},
}

var cosine_distance = framework.Function2{
	Name:       "cosine_distance",
	Return:     pgtypes.Float64,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Vector, pgtypes.Vector},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		left, right, err := vectorDistanceInputs(val1, val2)
		if err != nil {
			return nil, err
		}
		return vectorCosineDistance(left, right), nil
	},
}

var l1_distance = framework.Function2{
	Name:       "l1_distance",
	Return:     pgtypes.Float64,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Vector, pgtypes.Vector},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		left, right, err := vectorDistanceInputs(val1, val2)
		if err != nil {
			return nil, err
		}
		var distance float64
		for i := range left {
			distance += math.Abs(float64(left[i] - right[i]))
		}
		return distance, nil
	},
}

var vector_ge = framework.Function2{
	Name:       "vector_ge",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Vector, pgtypes.Vector},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		return pgtypes.CompareVectors(val1.([]float32), val2.([]float32)) >= 0, nil
	},
}

var vector_gt = framework.Function2{
	Name:       "vector_gt",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Vector, pgtypes.Vector},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		return pgtypes.CompareVectors(val1.([]float32), val2.([]float32)) > 0, nil
	},
}

func vectorDistanceInputs(val1 any, val2 any) ([]float32, []float32, error) {
	left := val1.([]float32)
	right := val2.([]float32)
	if len(left) != len(right) {
		return nil, nil, errors.Errorf("different vector dimensions %d and %d", len(left), len(right))
	}
	return left, right, nil
}

func vectorL2SquaredDistance(left []float32, right []float32) float64 {
	var distance float64
	for i := range left {
		diff := float64(left[i] - right[i])
		distance += diff * diff
	}
	return distance
}

func vectorInnerProduct(left []float32, right []float32) float64 {
	var product float64
	for i := range left {
		product += float64(left[i] * right[i])
	}
	return product
}

func vectorCosineDistance(left []float32, right []float32) float64 {
	var similarity float64
	var normLeft float64
	var normRight float64
	for i := range left {
		similarity += float64(left[i] * right[i])
		normLeft += float64(left[i] * left[i])
		normRight += float64(right[i] * right[i])
	}

	similarity /= math.Sqrt(normLeft * normRight)
	if similarity > 1 {
		similarity = 1
	} else if similarity < -1 {
		similarity = -1
	}
	return 1 - similarity
}
