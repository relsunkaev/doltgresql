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

package expression

import (
	"context"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	gmsexpression "github.com/dolthub/go-mysql-server/sql/expression"
	gmsaggregation "github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/server/types"
)

const ArrayAggWindowFunctionName = "count_distinct"

type ArrayAgg struct {
	selectExprs []sql.Expression
	orderBy     sql.SortFields
	window      *sql.WindowDefinition
	id          sql.ColumnId
	distinct    bool
}

// NewArrayAgg constructs a fresh ArrayAgg expression. distinct mirrors
// the SQL `DISTINCT` modifier inside the function call.
func NewArrayAgg(distinct bool) *ArrayAgg {
	return &ArrayAgg{distinct: distinct}
}

// NewArrayAggWindow constructs the internal GMS window-function hook for
// PostgreSQL array_agg(... ) OVER (...).
func NewArrayAggWindow(ctx *sql.Context, child sql.Expression) sql.Expression {
	return &ArrayAgg{selectExprs: []sql.Expression{child}}
}

var _ sql.Aggregation = (*ArrayAgg)(nil)
var _ vitess.Injectable = (*ArrayAgg)(nil)
var _ sql.OrderedAggregation = (*ArrayAgg)(nil)
var _ sql.WindowAdaptableExpression = (*ArrayAgg)(nil)

// WithResolvedChildren returns a new ArrayAgg with the provided children as its select expressions.
// The last child is expected to be the order by expressions.
func (a *ArrayAgg) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	a.selectExprs = make([]sql.Expression, len(children)-1)
	for i := 0; i < len(children)-1; i++ {
		a.selectExprs[i] = children[i].(sql.Expression)
	}

	a.orderBy = children[len(children)-1].(sql.SortFields)
	return a, nil
}

// Resolved implements sql.Expression
func (a *ArrayAgg) Resolved() bool {
	if !gmsexpression.ExpressionsResolved(a.selectExprs...) || !gmsexpression.ExpressionsResolved(a.orderBy.ToExpressions()...) {
		return false
	}
	if a.window == nil {
		return true
	}
	return gmsexpression.ExpressionsResolved(append(a.window.OrderBy.ToExpressions(), a.window.PartitionBy...)...)
}

// String implements sql.Expression
func (a *ArrayAgg) String() string {
	sb := strings.Builder{}
	sb.WriteString("array_agg(")

	if a.selectExprs != nil {
		var exprs = make([]string, len(a.selectExprs))
		for i, expr := range a.selectExprs {
			exprs[i] = expr.String()
		}

		sb.WriteString(strings.Join(exprs, ", "))
	}

	if len(a.orderBy) > 0 {
		sb.WriteString(" order by ")
		for i, ob := range a.orderBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(ob.String())
		}
	}

	sb.WriteString(")")
	if a.window != nil {
		sb.WriteString(" ")
		sb.WriteString(a.window.String())
	}
	return sb.String()
}

// Type implements sql.Expression
func (a *ArrayAgg) Type(ctx *sql.Context) sql.Type {
	dt := a.selectExprs[0].Type(ctx).(*types.DoltgresType)
	return dt.ToArrayType()
}

// IsNullable implements sql.Expression
func (a *ArrayAgg) IsNullable(ctx *sql.Context) bool {
	return true
}

// Eval implements sql.Expression
func (a *ArrayAgg) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	panic("eval should never be called on an aggregation function")
}

// Children implements sql.Expression
func (a *ArrayAgg) Children() []sql.Expression {
	children := make([]sql.Expression, 0, len(a.selectExprs)+len(a.orderBy)+len(a.window.ToExpressions()))
	children = append(children, a.selectExprs...)
	children = append(children, a.orderBy.ToExpressions()...)
	children = append(children, a.window.ToExpressions()...)
	return children
}

func (a *ArrayAgg) OutputExpressions() []sql.Expression {
	return a.selectExprs
}

// WithChildren implements sql.Expression
func (a ArrayAgg) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	expected := len(a.selectExprs) + len(a.orderBy) + len(a.window.ToExpressions())
	if len(children) != expected {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(children), expected)
	}

	a.selectExprs = children[:len(a.selectExprs)]
	orderByEnd := len(a.selectExprs) + len(a.orderBy)
	a.orderBy = a.orderBy.FromExpressions(ctx, children[len(a.selectExprs):orderByEnd]...)
	if a.window != nil {
		window, err := a.window.FromExpressions(ctx, children[orderByEnd:])
		if err != nil {
			return nil, err
		}
		a.window = window
	}
	return &a, nil
}

// Id implements sql.IdExpression
func (a *ArrayAgg) Id() sql.ColumnId {
	return a.id
}

// WithId implements sql.IdExpression
func (a ArrayAgg) WithId(id sql.ColumnId) sql.IdExpression {
	a.id = id
	return &a
}

// NewWindowFunction implements sql.WindowAdaptableExpression
func (a *ArrayAgg) NewWindowFunction(ctx *sql.Context) (sql.WindowFunction, error) {
	return (&arrayAggWindowFunction{a: a}).WithWindow(ctx, a.Window())
}

// WithWindow implements sql.WindowAdaptableExpression
func (a ArrayAgg) WithWindow(ctx *sql.Context, window *sql.WindowDefinition) sql.WindowAdaptableExpression {
	a.window = window
	return &a
}

// Window implements sql.WindowAdaptableExpression
func (a *ArrayAgg) Window() *sql.WindowDefinition {
	return a.window
}

// NewBuffer implements sql.Aggregation
func (a *ArrayAgg) NewBuffer(ctx *sql.Context) (sql.AggregationBuffer, error) {
	buf := &arrayAggBuffer{
		elements: make([]sql.Row, 0),
		a:        a,
	}
	if a.distinct {
		buf.seen = make(map[string]struct{})
	}
	return buf, nil
}

// arrayAggBuffer is the buffer used to accumulate values for the array_agg aggregation function.
type arrayAggBuffer struct {
	elements []sql.Row
	a        *ArrayAgg
	// seen tracks already-recorded element keys when DISTINCT is set;
	// nil otherwise. Reuses jsonAggDistinctKey for shape-stable encoding.
	seen map[string]struct{}
}

// Dispose implements sql.AggregationBuffer
func (a *arrayAggBuffer) Dispose(ctx *sql.Context) {}

// Eval implements sql.AggregationBuffer
func (a *arrayAggBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	if len(a.elements) == 0 {
		return nil, nil
	}

	if a.a.orderBy != nil {
		sorter := &gmsexpression.Sorter{
			SortFields: a.a.orderBy,
			Rows:       a.elements,
			Ctx:        ctx,
		}

		sort.Stable(sorter)
		if sorter.LastError != nil {
			return nil, sorter.LastError
		}
	}

	// convert to []interface for return. The last element in each row is the one we want to return, the rest are sort fields.
	result := make([]interface{}, len(a.elements))
	for i, row := range a.elements {
		result[i] = row[(len(row) - 1)]
	}

	if dt, ok := a.a.selectExprs[0].Type(ctx).(*types.DoltgresType); ok && dt.IsArrayType() {
		return types.ArrayValue{Elements: result}, nil
	}
	return result, nil
}

// Update implements sql.AggregationBuffer
func (a *arrayAggBuffer) Update(ctx *sql.Context, row sql.Row) error {
	evalRow, err := evalExprs(ctx, a.a.selectExprs, row)
	if err != nil {
		return err
	}

	if a.seen != nil {
		key, err := jsonAggDistinctKey(ctx, evalRow)
		if err != nil {
			return err
		}
		if _, dup := a.seen[key]; dup {
			return nil
		}
		a.seen[key] = struct{}{}
	}

	// TODO: unwrap values as necessary
	// Append the current value to the end of the row. We want to preserve the row's original structure
	// for sort ordering in the final step.
	a.elements = append(a.elements, append(row, evalRow[0]))
	return nil
}

// evalExprs evaluates the provided expressions against the given row and returns the results as a new row.
func evalExprs(ctx *sql.Context, exprs []sql.Expression, row sql.Row) (sql.Row, error) {
	result := make(sql.Row, len(exprs))
	for i, expr := range exprs {
		var err error
		result[i], err = expr.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

type arrayAggWindowFunction struct {
	a      *ArrayAgg
	framer sql.WindowFramer
}

var _ sql.WindowFunction = (*arrayAggWindowFunction)(nil)

func (a *arrayAggWindowFunction) WithWindow(ctx *sql.Context, window *sql.WindowDefinition) (sql.WindowFunction, error) {
	next := *a
	if window != nil && window.Frame != nil {
		framer, err := window.Frame.NewFramer(window)
		if err != nil {
			return nil, err
		}
		next.framer = framer
	}
	return &next, nil
}

func (a *arrayAggWindowFunction) Dispose(ctx *sql.Context) {
	gmsexpression.Dispose(ctx, a.a)
}

func (a *arrayAggWindowFunction) StartPartition(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) error {
	return nil
}

func (a *arrayAggWindowFunction) DefaultFramer() sql.WindowFramer {
	if a.framer != nil {
		return a.framer
	}
	return gmsaggregation.NewUnboundedPrecedingToCurrentRowFramer()
}

func (a *arrayAggWindowFunction) Compute(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) (interface{}, error) {
	aggBuffer, err := a.a.NewBuffer(ctx)
	if err != nil {
		return nil, err
	}
	defer aggBuffer.Dispose(ctx)
	for i := interval.Start; i < interval.End; i++ {
		if err := aggBuffer.Update(ctx, buffer[i]); err != nil {
			return nil, err
		}
	}
	return aggBuffer.Eval(ctx)
}
