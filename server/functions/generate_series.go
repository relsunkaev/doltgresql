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
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtablefunctions"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/shopspring/decimal"

	"github.com/dolthub/doltgresql/postgres/parser/duration"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initGenerateSeries registers the functions to the catalog.
func initGenerateSeries() {
	framework.RegisterFunction(generate_series_int32_int32)
	framework.RegisterFunction(generate_series_int32_int32_int32)
	framework.RegisterFunction(generate_series_int64_int64)
	framework.RegisterFunction(generate_series_int64_int64_int64)
	framework.RegisterFunction(generate_series_numeric_numeric)
	framework.RegisterFunction(generate_series_numeric_numeric_numeric)
	framework.RegisterFunction(generate_series_timestamp_timestamp_interval)
	dtablefunctions.DoltTableFunctions = append(dtablefunctions.DoltTableFunctions, &generateSeriesWithOrdinalityTableFunction{})
}

// errStepSizeZero is an error for a step size of zero in the generate_series functions.
var errStepSizeZero = errors.New("step size cannot equal zero")

var _ sql.TableFunction = (*generateSeriesWithOrdinalityTableFunction)(nil)
var _ sql.ExecSourceRel = (*generateSeriesWithOrdinalityTableFunction)(nil)

type generateSeriesWithOrdinalityTableFunction struct {
	db        sql.Database
	exprs     []sql.Expression
	valueType sql.Type
}

func (g *generateSeriesWithOrdinalityTableFunction) NewInstance(ctx *sql.Context, db sql.Database, args []sql.Expression) (sql.Node, error) {
	if len(args) != 2 && len(args) != 3 {
		return nil, sql.ErrInvalidArgumentNumber.New(g.Name(), 2, len(args))
	}
	return &generateSeriesWithOrdinalityTableFunction{
		db:        db,
		exprs:     args,
		valueType: generateSeriesValueType(ctx, args),
	}, nil
}

func (g *generateSeriesWithOrdinalityTableFunction) Name() string {
	return "doltgres_generate_series_with_ordinality"
}

func (g *generateSeriesWithOrdinalityTableFunction) String() string {
	args := make([]string, len(g.exprs))
	for i, expr := range g.exprs {
		args[i] = expr.String()
	}
	return fmt.Sprintf("%s(%s)", g.Name(), strings.Join(args, ", "))
}

func (g *generateSeriesWithOrdinalityTableFunction) Resolved() bool {
	for _, expr := range g.exprs {
		if !expr.Resolved() {
			return false
		}
	}
	return true
}

func (g *generateSeriesWithOrdinalityTableFunction) Expressions() []sql.Expression {
	return g.exprs
}

func (g *generateSeriesWithOrdinalityTableFunction) WithExpressions(ctx *sql.Context, exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 2 && len(exprs) != 3 {
		return nil, sql.ErrInvalidChildrenNumber.New(g, len(exprs), 2)
	}
	ng := *g
	ng.exprs = exprs
	ng.valueType = generateSeriesValueType(ctx, exprs)
	return &ng, nil
}

func (g *generateSeriesWithOrdinalityTableFunction) Database() sql.Database {
	return g.db
}

func (g *generateSeriesWithOrdinalityTableFunction) WithDatabase(db sql.Database) (sql.Node, error) {
	ng := *g
	ng.db = db
	return &ng, nil
}

func (g *generateSeriesWithOrdinalityTableFunction) IsReadOnly() bool {
	return true
}

func (g *generateSeriesWithOrdinalityTableFunction) Schema(ctx *sql.Context) sql.Schema {
	var dbName string
	if g.db != nil {
		dbName = g.db.Name()
	}
	valueType := g.valueType
	if valueType == nil {
		valueType = pgtypes.Int64
	}
	return sql.Schema{
		&sql.Column{
			DatabaseSource: dbName,
			Source:         g.Name(),
			Name:           "value",
			Type:           valueType,
			Nullable:       false,
		},
		&sql.Column{
			DatabaseSource: dbName,
			Source:         g.Name(),
			Name:           "ordinality",
			Type:           pgtypes.Int64,
			Nullable:       false,
		},
	}
}

func (g *generateSeriesWithOrdinalityTableFunction) Children() []sql.Node {
	return nil
}

func (g *generateSeriesWithOrdinalityTableFunction) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(g, len(children), 0)
	}
	return g, nil
}

func (g *generateSeriesWithOrdinalityTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	values := make([]any, len(g.exprs))
	for i, expr := range g.exprs {
		value, err := expr.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return sql.RowsToRowIter(), nil
		}
		values[i] = value
	}
	iter, err := generateSeriesRowIter(values)
	if err != nil {
		return nil, err
	}
	return &generateSeriesWithOrdinalityRowIter{iter: iter}, nil
}

func (g *generateSeriesWithOrdinalityTableFunction) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (g *generateSeriesWithOrdinalityTableFunction) Collation() sql.CollationID {
	return sql.Collation_Default
}

func generateSeriesValueType(ctx *sql.Context, exprs []sql.Expression) sql.Type {
	if len(exprs) == 0 {
		return pgtypes.Int64
	}
	if typ, ok := exprs[0].Type(ctx).(*pgtypes.DoltgresType); ok {
		return typ
	}
	return pgtypes.Int64
}

type generateSeriesWithOrdinalityRowIter struct {
	iter sql.RowIter
	idx  int64
}

func (g *generateSeriesWithOrdinalityRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	row, err := g.iter.Next(ctx)
	if err != nil {
		return nil, err
	}
	g.idx++
	return append(append(sql.Row{}, row...), g.idx), nil
}

func (g *generateSeriesWithOrdinalityRowIter) Close(ctx *sql.Context) error {
	return g.iter.Close(ctx)
}

func generateSeriesRowIter(values []any) (sql.RowIter, error) {
	switch start := values[0].(type) {
	case int32:
		step := int32(1)
		if len(values) == 3 {
			step = values[2].(int32)
		}
		return int32GenerateSeries(start, values[1].(int32), step)
	case int64:
		step := int64(1)
		if len(values) == 3 {
			step = values[2].(int64)
		}
		return int64GenerateSeries(start, values[1].(int64), step)
	case decimal.Decimal:
		step := numericOne
		if len(values) == 3 {
			step = values[2].(decimal.Decimal)
		}
		return numericGenerateSeries(start, values[1].(decimal.Decimal), step)
	case time.Time:
		if len(values) != 3 {
			return nil, errors.Errorf("timestamp generate_series requires step argument")
		}
		step := values[2].(duration.Duration)
		stepInt, ok := step.AsInt64()
		if !ok {
			return nil, errors.Errorf("step argument of generate_series function is overflown")
		}
		if stepInt == 0 {
			return nil, errStepSizeZero
		}
		finish := values[1].(time.Time)
		return pgtypes.NewSetReturningFunctionRowIter(func(ctx *sql.Context) (sql.Row, error) {
			defer func() {
				start = start.Add(time.Duration(stepInt) * time.Second)
			}()
			if (stepInt > 0 && start.After(finish)) || (stepInt < 0 && start.Before(finish)) {
				return nil, io.EOF
			}
			return sql.Row{start}, nil
		}), nil
	default:
		return nil, errors.Errorf("unsupported generate_series argument type %T", values[0])
	}
}

// generate_series_int32_int32 represents the PostgreSQL function of the same name, taking the same parameters.
var generate_series_int32_int32 = framework.Function2{
	Name:       "generate_series",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.Int32),
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Int32, pgtypes.Int32},
	Strict:     true,
	SRF:        true,
	Callable: func(ctx *sql.Context, t [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		start := val1.(int32)
		finish := val2.(int32)
		step := int32(1) // by default
		return int32GenerateSeries(start, finish, step)
	},
}

// generate_series_int32_int32_int32 represents the PostgreSQL function of the same name, taking the same parameters.
var generate_series_int32_int32_int32 = framework.Function3{
	Name:       "generate_series",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.Int32),
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Int32, pgtypes.Int32, pgtypes.Int32},
	Strict:     true,
	SRF:        true,
	Callable: func(ctx *sql.Context, t [4]*pgtypes.DoltgresType, val1, val2, val3 any) (any, error) {
		start := val1.(int32)
		finish := val2.(int32)
		step := val3.(int32)
		return int32GenerateSeries(start, finish, step)
	},
}

// int32GenerateSeries returns RowIter for generate_series function results for given int32 values.
// This function checks for error of step being zero.
func int32GenerateSeries(start, finish, step int32) (*pgtypes.SetReturningFunctionRowIter, error) {
	if step == 0 {
		return nil, errStepSizeZero
	}
	return pgtypes.NewSetReturningFunctionRowIter(func(ctx *sql.Context) (sql.Row, error) {
		defer func() {
			start += step
		}()
		if (step > 0 && start > finish) || (step < 0 && start < finish) {
			return nil, io.EOF
		}
		return sql.Row{start}, nil
	}), nil
}

// generate_series_int64_int64 represents the PostgreSQL function of the same name, taking the same parameters.
var generate_series_int64_int64 = framework.Function2{
	Name:       "generate_series",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.Int64),
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Int64, pgtypes.Int64},
	Strict:     true,
	SRF:        true,
	Callable: func(ctx *sql.Context, t [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		start := val1.(int64)
		finish := val2.(int64)
		step := int64(1) // by default
		return int64GenerateSeries(start, finish, step)
	},
}

// generate_series_int64_int64_int64 represents the PostgreSQL function of the same name, taking the same parameters.
var generate_series_int64_int64_int64 = framework.Function3{
	Name:       "generate_series",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.Int64),
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Int64, pgtypes.Int64, pgtypes.Int64},
	Strict:     true,
	SRF:        true,
	Callable: func(ctx *sql.Context, t [4]*pgtypes.DoltgresType, val1, val2, val3 any) (any, error) {
		start := val1.(int64)
		finish := val2.(int64)
		step := val3.(int64)
		return int64GenerateSeries(start, finish, step)
	},
}

// int64GenerateSeries returns RowIter for generate_series function results for given int64 values.
// This function checks for error of step being zero.
func int64GenerateSeries(start, finish, step int64) (*pgtypes.SetReturningFunctionRowIter, error) {
	if step == 0 {
		return nil, errStepSizeZero
	}
	return pgtypes.NewSetReturningFunctionRowIter(func(ctx *sql.Context) (sql.Row, error) {
		defer func() {
			start += step
		}()
		if (step > 0 && start > finish) || (step < 0 && start < finish) {
			return nil, io.EOF
		}
		return sql.Row{start}, nil
	}), nil
}

// generate_series_numeric_numeric represents the PostgreSQL function of the same name, taking the same parameters.
var generate_series_numeric_numeric = framework.Function2{
	Name:       "generate_series",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.Numeric),
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Numeric, pgtypes.Numeric},
	Strict:     true,
	SRF:        true,
	Callable: func(ctx *sql.Context, t [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		start := val1.(decimal.Decimal)
		finish := val2.(decimal.Decimal)
		step := numericOne // by default
		return numericGenerateSeries(start, finish, step)
	},
}

// generate_series_numeric_numeric_numeric represents the PostgreSQL function of the same name, taking the same parameters.
var generate_series_numeric_numeric_numeric = framework.Function3{
	Name:       "generate_series",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.Numeric),
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Numeric, pgtypes.Numeric, pgtypes.Numeric},
	Strict:     true,
	SRF:        true,
	Callable: func(ctx *sql.Context, t [4]*pgtypes.DoltgresType, val1, val2, val3 any) (any, error) {
		start := val1.(decimal.Decimal)
		finish := val2.(decimal.Decimal)
		step := val3.(decimal.Decimal)
		return numericGenerateSeries(start, finish, step)
	},
}

// numericGenerateSeries returns RowIter for generate_series function results for given numeric values.
// This function checks for error of step being zero.
func numericGenerateSeries(start, finish, step decimal.Decimal) (*pgtypes.SetReturningFunctionRowIter, error) {
	if step.Equal(decimal.Zero) {
		return nil, errStepSizeZero
	}
	return pgtypes.NewSetReturningFunctionRowIter(func(ctx *sql.Context) (sql.Row, error) {
		defer func() {
			start = start.Add(step)
		}()
		if (step.GreaterThan(decimal.Zero) && start.GreaterThan(finish)) ||
			(step.LessThan(decimal.Zero) && start.LessThan(finish)) {
			return nil, io.EOF
		}
		return sql.Row{start}, nil
	}), nil
}

// generate_series_timestamp_timestamp_interval represents the PostgreSQL function of the same name, taking the same parameters.
var generate_series_timestamp_timestamp_interval = framework.Function3{
	Name:       "generate_series",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.Timestamp),
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Timestamp, pgtypes.Timestamp, pgtypes.Interval},
	Strict:     true,
	SRF:        true,
	Callable: func(ctx *sql.Context, t [4]*pgtypes.DoltgresType, val1, val2, val3 any) (any, error) {
		start := val1.(time.Time)
		finish := val2.(time.Time)
		step := val3.(duration.Duration)
		stepInt, ok := step.AsInt64()
		if !ok {
			// TODO: overflown
			return nil, errors.Errorf("step argument of generate_series function is overflown")
		}
		if stepInt == 0 {
			return nil, errStepSizeZero
		}

		return pgtypes.NewSetReturningFunctionRowIter(func(ctx *sql.Context) (sql.Row, error) {
			defer func() {
				start = start.Add(time.Duration(stepInt) * time.Second)
			}()
			if (stepInt > 0 && start.After(finish)) || (stepInt < 0 && start.Before(finish)) {
				return nil, io.EOF
			}
			return sql.Row{start}, nil
		}), nil
	},
}
