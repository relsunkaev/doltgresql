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
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func initVector() {
	framework.RegisterBinaryFunction(framework.Operator_BinaryPlus, vector_add)
	framework.RegisterBinaryFunction(framework.Operator_BinaryMinus, vector_sub)
	framework.RegisterBinaryFunction(framework.Operator_BinaryMultiply, vector_mul)
	framework.RegisterBinaryFunction(framework.Operator_BinaryConcatenate, vector_concat)
	framework.RegisterBinaryFunction(framework.Operator_BinaryLessThan, vector_lt)
	framework.RegisterBinaryFunction(framework.Operator_BinaryLessOrEqual, vector_le)
	framework.RegisterBinaryFunction(framework.Operator_BinaryVectorL2Distance, l2_distance)
	framework.RegisterFunction(inner_product)
	framework.RegisterBinaryFunction(framework.Operator_BinaryVectorNegativeInnerProduct, vector_negative_inner_product)
	framework.RegisterBinaryFunction(framework.Operator_BinaryVectorCosineDistance, cosine_distance)
	framework.RegisterBinaryFunction(framework.Operator_BinaryVectorL1Distance, l1_distance)
	framework.RegisterFunction(vector_dims)
	framework.RegisterFunction(vector_norm)
	framework.RegisterFunction(l2_normalize)
	framework.RegisterFunction(subvector)
	framework.RegisterFunction(binary_quantize)
	framework.RegisterFunction(vector_l2_squared_distance)
	framework.RegisterBinaryFunction(framework.Operator_BinaryGreaterOrEqual, vector_ge)
	framework.RegisterBinaryFunction(framework.Operator_BinaryGreaterThan, vector_gt)
}

var vector_add = framework.Function2{
	Name:       "vector_add",
	Return:     pgtypes.Vector,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Vector, pgtypes.Vector},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		return vectorElementwise(val1, val2, func(left float32, right float32) float32 {
			return left + right
		})
	},
}

var vector_sub = framework.Function2{
	Name:       "vector_sub",
	Return:     pgtypes.Vector,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Vector, pgtypes.Vector},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		return vectorElementwise(val1, val2, func(left float32, right float32) float32 {
			return left - right
		})
	},
}

var vector_mul = framework.Function2{
	Name:       "vector_mul",
	Return:     pgtypes.Vector,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Vector, pgtypes.Vector},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		return vectorElementwise(val1, val2, func(left float32, right float32) float32 {
			return left * right
		})
	},
}

var vector_concat = framework.Function2{
	Name:       "vector_concat",
	Return:     pgtypes.Vector,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Vector, pgtypes.Vector},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		left := val1.([]float32)
		right := val2.([]float32)
		if err := checkVectorDimension(len(left) + len(right)); err != nil {
			return nil, err
		}
		result := make([]float32, 0, len(left)+len(right))
		result = append(result, left...)
		result = append(result, right...)
		return result, nil
	},
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

var vector_dims = framework.Function1{
	Name:       "vector_dims",
	Return:     pgtypes.Int32,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Vector},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return int32(len(val.([]float32))), nil
	},
}

var vector_norm = framework.Function1{
	Name:       "vector_norm",
	Return:     pgtypes.Float64,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Vector},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return math.Sqrt(vectorNormSquared(val.([]float32))), nil
	},
}

var l2_normalize = framework.Function1{
	Name:       "l2_normalize",
	Return:     pgtypes.Vector,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Vector},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		values := val.([]float32)
		result := make([]float32, len(values))
		norm := math.Sqrt(vectorNormSquared(values))
		if norm > 0 {
			for i, value := range values {
				result[i] = float32(float64(value) / norm)
			}
		}
		return validateVectorResult(result)
	},
}

var subvector = framework.Function3{
	Name:       "subvector",
	Return:     pgtypes.Vector,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Vector, pgtypes.Int32, pgtypes.Int32},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, val1 any, val2 any, val3 any) (any, error) {
		values := val1.([]float32)
		start := val2.(int32)
		count := val3.(int32)
		if count < 1 {
			return nil, errors.Errorf("vector must have at least 1 dimension")
		}

		vectorLength := int32(len(values))
		var end int32
		if start > vectorLength-count {
			end = vectorLength + 1
		} else {
			end = start + count
		}

		if start < 1 {
			start = 1
		} else if start > vectorLength {
			return nil, errors.Errorf("vector must have at least 1 dimension")
		}

		dimensions := int(end - start)
		if err := checkVectorDimension(dimensions); err != nil {
			return nil, err
		}
		result := make([]float32, dimensions)
		copy(result, values[start-1:start-1+int32(dimensions)])
		return result, nil
	},
}

var binary_quantize = framework.Function1{
	Name:       "binary_quantize",
	Return:     pgtypes.Bit,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Vector},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		values := val.([]float32)
		var result strings.Builder
		result.Grow(len(values))
		for _, value := range values {
			if value > 0 {
				result.WriteByte('1')
			} else {
				result.WriteByte('0')
			}
		}
		return result.String(), nil
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

func vectorElementwise(val1 any, val2 any, operation func(float32, float32) float32) ([]float32, error) {
	left, right, err := vectorDistanceInputs(val1, val2)
	if err != nil {
		return nil, err
	}
	result := make([]float32, len(left))
	for i := range left {
		result[i] = operation(left[i], right[i])
	}
	return validateVectorResult(result)
}

func validateVectorResult(values []float32) ([]float32, error) {
	if err := checkVectorDimension(len(values)); err != nil {
		return nil, err
	}
	for _, value := range values {
		if math.IsInf(float64(value), 0) {
			return nil, errors.Errorf("value out of range: overflow")
		}
	}
	return values, nil
}

func checkVectorDimension(dimensions int) error {
	if dimensions < 1 {
		return errors.Errorf("vector must have at least 1 dimension")
	}
	if dimensions > pgtypes.MaxVectorDimensions {
		return errors.Errorf("vector cannot have more than %d dimensions", pgtypes.MaxVectorDimensions)
	}
	return nil
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

func vectorNormSquared(values []float32) float64 {
	var norm float64
	for _, value := range values {
		norm += float64(value) * float64(value)
	}
	return norm
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
