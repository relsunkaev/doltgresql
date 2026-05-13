// Copyright 2024 Dolthub, Inc.
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

package cast

import (
	"math"

	"github.com/cockroachdb/errors"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initNumeric handles all casts that are built-in. This comprises only the "From" types.
func initNumeric() {
	numericAssignment()
	numericImplicit()
}

// numericAssignment registers all assignment casts. This comprises only the "From" types.
func numericAssignment() {
	framework.MustAddAssignmentTypeCast(framework.TypeCast{
		FromType: pgtypes.Numeric,
		ToType:   pgtypes.Int16,
		Function: func(ctx *sql.Context, val any, targetType *pgtypes.DoltgresType) (any, error) {
			d, ok, err := pgtypes.NumericValueAsDecimal(val)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, errors.Wrap(pgtypes.ErrCastOutOfRange, "smallint out of range")
			}
			if d.LessThan(pgtypes.NumericValueMinInt16) || d.GreaterThan(pgtypes.NumericValueMaxInt16) {
				return nil, errors.Wrap(pgtypes.ErrCastOutOfRange, "smallint out of range")
			}
			return int16(d.Round(0).IntPart()), nil
		},
	})
	framework.MustAddAssignmentTypeCast(framework.TypeCast{
		FromType: pgtypes.Numeric,
		ToType:   pgtypes.Int32,
		Function: func(ctx *sql.Context, val any, targetType *pgtypes.DoltgresType) (any, error) {
			d, ok, err := pgtypes.NumericValueAsDecimal(val)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, errors.Wrap(pgtypes.ErrCastOutOfRange, "integer out of range")
			}
			if d.LessThan(pgtypes.NumericValueMinInt32) || d.GreaterThan(pgtypes.NumericValueMaxInt32) {
				return nil, errors.Wrap(pgtypes.ErrCastOutOfRange, "integer out of range")
			}
			return int32(d.Round(0).IntPart()), nil
		},
	})
	framework.MustAddAssignmentTypeCast(framework.TypeCast{
		FromType: pgtypes.Numeric,
		ToType:   pgtypes.Int64,
		Function: func(ctx *sql.Context, val any, targetType *pgtypes.DoltgresType) (any, error) {
			d, ok, err := pgtypes.NumericValueAsDecimal(val)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, errors.Wrap(pgtypes.ErrCastOutOfRange, "bigint out of range")
			}
			if d.LessThan(pgtypes.NumericValueMinInt64) || d.GreaterThan(pgtypes.NumericValueMaxInt64) {
				return nil, errors.Wrap(pgtypes.ErrCastOutOfRange, "bigint out of range")
			}
			return int64(d.Round(0).IntPart()), nil
		},
	})
}

// numericImplicit registers all implicit casts. This comprises only the "From" types.
func numericImplicit() {
	framework.MustAddImplicitTypeCast(framework.TypeCast{
		FromType: pgtypes.Numeric,
		ToType:   pgtypes.Float32,
		Function: func(ctx *sql.Context, val any, targetType *pgtypes.DoltgresType) (any, error) {
			f, err := numericValueToFloat64(val)
			if err != nil {
				return nil, err
			}
			return float32(f), nil
		},
	})
	framework.MustAddImplicitTypeCast(framework.TypeCast{
		FromType: pgtypes.Numeric,
		ToType:   pgtypes.Float64,
		Function: func(ctx *sql.Context, val any, targetType *pgtypes.DoltgresType) (any, error) {
			f, err := numericValueToFloat64(val)
			if err != nil {
				return nil, err
			}
			return f, nil
		},
	})
	framework.MustAddImplicitTypeCast(framework.TypeCast{
		FromType: pgtypes.Numeric,
		ToType:   pgtypes.Numeric,
		Function: func(ctx *sql.Context, val any, targetType *pgtypes.DoltgresType) (any, error) {
			return pgtypes.GetAnyNumericValueWithTypmod(val, targetType.GetAttTypMod())
		},
	})
}

func numericValueToFloat64(val any) (float64, error) {
	dec, ok, err := pgtypes.NumericValueAsDecimal(val)
	if err != nil {
		return 0, err
	}
	if ok {
		f, _ := dec.Float64()
		return f, nil
	}
	numeric, err := pgtypes.NumericValueToPgtype(val)
	if err != nil {
		return 0, err
	}
	if numeric.NaN {
		return math.NaN(), nil
	}
	if numeric.InfinityModifier > 0 {
		return math.Inf(1), nil
	}
	return math.Inf(-1), nil
}
