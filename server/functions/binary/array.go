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
	framework.RegisterBinaryFunction(framework.Operator_BinaryJSONContainsRight, arraycontains)
	framework.RegisterBinaryFunction(framework.Operator_BinaryJSONContainsLeft, arraycontained)
	framework.RegisterBinaryFunction(framework.Operator_BinaryOverlaps, arrayoverlap)
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
