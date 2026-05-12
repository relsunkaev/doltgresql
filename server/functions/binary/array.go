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
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initArray registers the array operators.
func initArray() {
	framework.RegisterBinaryFunction(framework.Operator_BinaryEqual, array_eq)
	framework.RegisterBinaryFunction(framework.Operator_BinaryJSONContainsRight, arraycontains)
	framework.RegisterBinaryFunction(framework.Operator_BinaryJSONContainsLeft, arraycontained)
	framework.RegisterBinaryFunction(framework.Operator_BinaryOverlaps, arrayoverlap)
}

// array_eq represents PostgreSQL's array = operator.
var array_eq = framework.Function2{
	Name:       "array_eq",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.AnyArray, pgtypes.AnyArray},
	Strict:     true,
	Callable: func(ctx *sql.Context, t [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		left := val1.([]any)
		right := val2.([]any)
		if len(left) != len(right) {
			return false, nil
		}
		return arrayEqual(ctx, t[0].ArrayBaseType(), left, right)
	},
}

// arraycontains represents PostgreSQL's array @> operator.
var arraycontains = framework.Function2{
	Name:       "arraycontains",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.AnyArray, pgtypes.AnyArray},
	Strict:     true,
	Callable: func(ctx *sql.Context, t [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		return arrayContains(ctx, t[0].ArrayBaseType(), val1.([]any), val2.([]any))
	},
}

// arraycontained represents PostgreSQL's array <@ operator.
var arraycontained = framework.Function2{
	Name:       "arraycontained",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.AnyArray, pgtypes.AnyArray},
	Strict:     true,
	Callable: func(ctx *sql.Context, t [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		return arrayContains(ctx, t[0].ArrayBaseType(), val2.([]any), val1.([]any))
	},
}

// arrayoverlap represents PostgreSQL's array && operator.
var arrayoverlap = framework.Function2{
	Name:       "arrayoverlap",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.AnyArray, pgtypes.AnyArray},
	Strict:     true,
	Callable: func(ctx *sql.Context, t [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		left := val1.([]any)
		right := val2.([]any)
		baseType := t[0].ArrayBaseType()
		for _, leftValue := range left {
			if leftValue == nil {
				continue
			}
			for _, rightValue := range right {
				if rightValue == nil {
					continue
				}
				cmp, err := baseType.Compare(ctx, leftValue, rightValue)
				if err != nil {
					return nil, err
				}
				if cmp == 0 {
					return true, nil
				}
			}
		}
		return false, nil
	},
}

func arrayContains(ctx *sql.Context, baseType *pgtypes.DoltgresType, container []any, contained []any) (bool, error) {
	for _, containedValue := range contained {
		if containedValue == nil {
			return false, nil
		}
		found := false
		for _, containerValue := range container {
			if containerValue == nil {
				continue
			}
			cmp, err := baseType.Compare(ctx, containerValue, containedValue)
			if err != nil {
				return false, err
			}
			if cmp == 0 {
				found = true
				break
			}
		}
		if !found {
			return false, nil
		}
	}
	return true, nil
}

func arrayEqual(ctx *sql.Context, baseType *pgtypes.DoltgresType, left []any, right []any) (bool, error) {
	for i, leftValue := range left {
		rightValue := right[i]
		if leftValue == nil || rightValue == nil {
			if leftValue != rightValue {
				return false, nil
			}
			continue
		}
		if leftNested, ok := leftValue.([]any); ok {
			rightNested, ok := rightValue.([]any)
			if !ok || len(leftNested) != len(rightNested) {
				return false, nil
			}
			equal, err := arrayEqual(ctx, baseType, leftNested, rightNested)
			if err != nil || !equal {
				return equal, err
			}
			continue
		}
		cmp, err := baseType.Compare(ctx, leftValue, rightValue)
		if err != nil {
			return false, err
		}
		if cmp != 0 {
			return false, nil
		}
	}
	return true, nil
}
