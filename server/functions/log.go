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

package functions

import (
	"math"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/shopspring/decimal"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

const (
	numericLogOutputPrecision = int32(38)
	numericLogWorkPrecision   = numericLogOutputPrecision + 10
)

// initLog registers the functions to the catalog.
func initLog() {
	framework.RegisterFunction(log_float64)
	framework.RegisterFunction(log_numeric)
	framework.RegisterFunction(log_numeric_numeric)
}

// log_float64 represents the PostgreSQL function of the same name, taking the same parameters.
var log_float64 = framework.Function1{
	Name:       "log",
	Return:     pgtypes.Float64,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Float64},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val1Interface any) (any, error) {
		val1 := val1Interface.(float64)
		if val1 == 0 {
			return nil, errors.Errorf("cannot take logarithm of zero")
		} else if val1 < 0 {
			return nil, errors.Errorf("cannot take logarithm of a negative number")
		}
		return math.Log10(val1), nil
	},
}

// log_numeric represents the PostgreSQL function of the same name, taking the same parameters.
var log_numeric = framework.Function1{
	Name:       "log",
	Return:     pgtypes.Numeric,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Numeric},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val1Interface any) (any, error) {
		if val1Interface == nil {
			return nil, nil
		}
		val1 := val1Interface.(decimal.Decimal)
		if val1.Equal(decimal.Zero) {
			return nil, errors.Errorf("cannot take logarithm of zero")
		} else if val1.LessThan(decimal.Zero) {
			return nil, errors.Errorf("cannot take logarithm of a negative number")
		}
		ln, err := numericNaturalLog(val1, numericLogWorkPrecision)
		if err != nil {
			return nil, err
		}
		ln10, err := numericNaturalLog(decimal.NewFromInt(10), numericLogWorkPrecision)
		if err != nil {
			return nil, err
		}
		return roundedNumericLogResult(ln.DivRound(ln10, numericLogWorkPrecision), numericLogOutputPrecision), nil
	},
}

// log_numeric_numeric represents the PostgreSQL function of the same name, taking the same parameters.
var log_numeric_numeric = framework.Function2{
	Name:       "log",
	Return:     pgtypes.Numeric,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Numeric, pgtypes.Numeric},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1Interface any, val2Interface any) (any, error) {
		if val1Interface == nil || val2Interface == nil {
			return nil, nil
		}
		val1 := val1Interface.(decimal.Decimal)
		val2 := val2Interface.(decimal.Decimal)
		if val1.Equal(decimal.Zero) || val2.Equal(decimal.Zero) {
			return nil, errors.Errorf("cannot take logarithm of zero")
		} else if val1.LessThan(decimal.Zero) || val2.LessThan(decimal.Zero) {
			return nil, errors.Errorf("cannot take logarithm of a negative number")
		}
		logBase, err := numericNaturalLog(val1, numericLogWorkPrecision)
		if err != nil {
			return nil, err
		}
		if logBase.Equal(decimal.Zero) {
			return nil, errors.Errorf("division by zero")
		}
		logNum, err := numericNaturalLog(val2, numericLogWorkPrecision)
		if err != nil {
			return nil, err
		}
		return roundedNumericLogResult(logNum.DivRound(logBase, numericLogWorkPrecision), numericLogScale(val1, val2)), nil
	},
}

func numericNaturalLog(val decimal.Decimal, precision int32) (decimal.Decimal, error) {
	if val.Equal(decimal.Zero) {
		return decimal.Decimal{}, errors.Errorf("cannot take logarithm of zero")
	} else if val.LessThan(decimal.Zero) {
		return decimal.Decimal{}, errors.Errorf("cannot take logarithm of a negative number")
	}
	return val.Ln(precision)
}

func numericLogScale(vals ...decimal.Decimal) int32 {
	var scale int32
	for _, val := range vals {
		if exp := -val.Exponent(); exp > scale {
			scale = exp
		}
	}
	return scale
}

func roundedNumericLogResult(val decimal.Decimal, precision int32) decimal.Decimal {
	rounded := val.Round(precision)
	if rounded.Equal(decimal.Zero) {
		return decimal.Zero
	}
	return rounded
}
