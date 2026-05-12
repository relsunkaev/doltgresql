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

package expression

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/shopspring/decimal"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// OrderedSetAgg implements the currently supported WITHIN GROUP aggregates.
type OrderedSetAgg struct {
	name        string
	selectExprs []sql.Expression
	orderBy     sql.SortFields
	id          sql.ColumnId
}

var _ sql.Aggregation = (*OrderedSetAgg)(nil)
var _ vitess.Injectable = (*OrderedSetAgg)(nil)
var _ sql.OrderedAggregation = (*OrderedSetAgg)(nil)

// NewOrderedSetAgg constructs a new OrderedSetAgg expression.
func NewOrderedSetAgg(name string) *OrderedSetAgg {
	return &OrderedSetAgg{name: strings.ToLower(name)}
}

// WithResolvedChildren returns a new OrderedSetAgg with direct arguments and sort fields.
func (o *OrderedSetAgg) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	o.selectExprs = make([]sql.Expression, len(children)-1)
	for i := 0; i < len(children)-1; i++ {
		o.selectExprs[i] = children[i].(sql.Expression)
	}
	o.orderBy = children[len(children)-1].(sql.SortFields)
	return o, nil
}

// Resolved implements sql.Expression.
func (o *OrderedSetAgg) Resolved() bool {
	return expression.ExpressionsResolved(o.selectExprs...) && expression.ExpressionsResolved(o.orderBy.ToExpressions()...)
}

// String implements sql.Expression.
func (o *OrderedSetAgg) String() string {
	return o.name + "() within group"
}

// Type implements sql.Expression.
func (o *OrderedSetAgg) Type(ctx *sql.Context) sql.Type {
	switch o.name {
	case "percentile_cont":
		return pgtypes.Float64
	case "rank":
		return pgtypes.Int64
	default:
		if len(o.orderBy) > 0 {
			return o.valueOrderBy().Column.Type(ctx)
		}
		return pgtypes.Unknown
	}
}

// IsNullable implements sql.Expression.
func (o *OrderedSetAgg) IsNullable(ctx *sql.Context) bool {
	return true
}

// Eval implements sql.Expression.
func (o *OrderedSetAgg) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	panic("eval should never be called on an aggregation function")
}

// Children implements sql.Expression.
func (o *OrderedSetAgg) Children() []sql.Expression {
	return append(o.selectExprs, o.orderBy.ToExpressions()...)
}

// OutputExpressions implements sql.Aggregation.
func (o *OrderedSetAgg) OutputExpressions() []sql.Expression {
	return o.selectExprs
}

// WithChildren implements sql.Expression.
func (o OrderedSetAgg) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != len(o.selectExprs)+len(o.orderBy) {
		return nil, sql.ErrInvalidChildrenNumber.New(o, len(children), len(o.selectExprs)+len(o.orderBy))
	}
	o.selectExprs = children[:len(o.selectExprs)]
	o.orderBy = o.orderBy.FromExpressions(ctx, children[len(o.selectExprs):]...)
	return &o, nil
}

// Id implements sql.IdExpression.
func (o *OrderedSetAgg) Id() sql.ColumnId {
	return o.id
}

// WithId implements sql.IdExpression.
func (o OrderedSetAgg) WithId(id sql.ColumnId) sql.IdExpression {
	o.id = id
	return &o
}

// NewWindowFunction implements sql.WindowAdaptableExpression.
func (o *OrderedSetAgg) NewWindowFunction(ctx *sql.Context) (sql.WindowFunction, error) {
	panic("window functions not yet supported for ordered-set aggregates")
}

// WithWindow implements sql.WindowAdaptableExpression.
func (o *OrderedSetAgg) WithWindow(ctx *sql.Context, window *sql.WindowDefinition) sql.WindowAdaptableExpression {
	panic("window functions not yet supported for ordered-set aggregates")
}

// Window implements sql.WindowAdaptableExpression.
func (o *OrderedSetAgg) Window() *sql.WindowDefinition {
	return nil
}

// NewBuffer implements sql.Aggregation.
func (o *OrderedSetAgg) NewBuffer(ctx *sql.Context) (sql.AggregationBuffer, error) {
	return &orderedSetAggBuffer{
		elements: make([]sql.Row, 0),
		o:        o,
	}, nil
}

type orderedSetAggBuffer struct {
	elements     []sql.Row
	directValues sql.Row
	o            *OrderedSetAgg
}

// Dispose implements sql.AggregationBuffer.
func (o *orderedSetAggBuffer) Dispose(ctx *sql.Context) {}

// Eval implements sql.AggregationBuffer.
func (o *orderedSetAggBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	if len(o.elements) == 0 {
		return nil, nil
	}
	if len(o.o.orderBy) == 0 {
		return nil, errors.Errorf("%s requires an ORDER BY expression", o.o.name)
	}
	sorter := &expression.Sorter{
		SortFields: o.o.orderBy,
		Rows:       o.elements,
		Ctx:        ctx,
	}
	sort.Stable(sorter)
	if sorter.LastError != nil {
		return nil, sorter.LastError
	}
	values, err := o.orderedValues(ctx)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, nil
	}
	switch o.o.name {
	case "percentile_cont":
		return o.percentileCont(ctx, values)
	case "percentile_disc":
		return o.percentileDisc(ctx, values)
	case "mode":
		return o.mode(ctx, values)
	case "rank":
		return o.rank(ctx, values)
	default:
		return nil, errors.Errorf("ordered-set aggregate %s is not yet supported", o.o.name)
	}
}

// Update implements sql.AggregationBuffer.
func (o *orderedSetAggBuffer) Update(ctx *sql.Context, row sql.Row) error {
	directValues, err := evalExprs(ctx, o.o.selectExprs, row)
	if err != nil {
		return err
	}
	if o.directValues == nil {
		o.directValues = directValues
	}
	storedRow := make(sql.Row, len(row))
	copy(storedRow, row)
	o.elements = append(o.elements, storedRow)
	return nil
}

func (o *orderedSetAggBuffer) orderedValues(ctx *sql.Context) ([]any, error) {
	values := make([]any, 0, len(o.elements))
	for _, row := range o.elements {
		value, err := o.o.valueOrderBy().Column.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		value, err = sql.UnwrapAny(ctx, value)
		if err != nil {
			return nil, err
		}
		if value != nil {
			values = append(values, value)
		}
	}
	return values, nil
}

func (o *orderedSetAggBuffer) percentileCont(ctx *sql.Context, values []any) (any, error) {
	fraction, err := o.fraction(ctx)
	if err != nil {
		return nil, err
	}
	if fraction < 0 || fraction > 1 {
		return nil, errors.Errorf("percentile value %v is not between 0 and 1", fraction)
	}
	if len(values) == 1 {
		return numericAsFloat(values[0])
	}
	position := 1 + fraction*float64(len(values)-1)
	lowerPosition := int(math.Floor(position))
	upperPosition := int(math.Ceil(position))
	lowerValue, err := numericAsFloat(values[lowerPosition-1])
	if err != nil {
		return nil, err
	}
	if lowerPosition == upperPosition {
		return lowerValue, nil
	}
	upperValue, err := numericAsFloat(values[upperPosition-1])
	if err != nil {
		return nil, err
	}
	weight := position - float64(lowerPosition)
	return lowerValue + (upperValue-lowerValue)*weight, nil
}

func (o *orderedSetAggBuffer) percentileDisc(ctx *sql.Context, values []any) (any, error) {
	fraction, err := o.fraction(ctx)
	if err != nil {
		return nil, err
	}
	if fraction < 0 || fraction > 1 {
		return nil, errors.Errorf("percentile value %v is not between 0 and 1", fraction)
	}
	index := int(math.Ceil(fraction*float64(len(values)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index], nil
}

func (o *orderedSetAggBuffer) mode(ctx *sql.Context, values []any) (any, error) {
	counts := make(map[string]int, len(values))
	var best any
	bestCount := 0
	for _, value := range values {
		key := fmt.Sprintf("%T:%#v", value, value)
		counts[key]++
		if counts[key] > bestCount {
			best = value
			bestCount = counts[key]
		}
	}
	return best, nil
}

func (o *orderedSetAggBuffer) rank(ctx *sql.Context, values []any) (any, error) {
	if len(o.directValues) != 1 {
		return nil, errors.Errorf("rank requires exactly one direct argument")
	}
	hypothetical := o.directValues[0]
	valueOrderBy := o.o.valueOrderBy()
	valueType, ok := valueOrderBy.Column.Type(ctx).(*pgtypes.DoltgresType)
	if !ok {
		return nil, errors.Errorf("rank requires a PostgreSQL ORDER BY type")
	}
	rank := int64(1)
	for _, value := range values {
		cmp, err := valueType.Compare(ctx, value, hypothetical)
		if err != nil {
			return nil, err
		}
		if valueOrderBy.Order == sql.Descending {
			cmp = -cmp
		}
		if cmp < 0 {
			rank++
		}
	}
	return rank, nil
}

func (o *OrderedSetAgg) valueOrderBy() sql.SortField {
	return o.orderBy[len(o.orderBy)-1]
}

func (o *orderedSetAggBuffer) fraction(ctx *sql.Context) (float64, error) {
	if len(o.directValues) != 1 {
		return 0, errors.Errorf("%s requires exactly one direct argument", o.o.name)
	}
	return numericAsFloat(o.directValues[0])
}

func numericAsFloat(value any) (float64, error) {
	value, err := sql.UnwrapAny(context.Background(), value)
	if err != nil {
		return 0, err
	}
	switch v := value.(type) {
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case decimal.Decimal:
		f, _ := v.Float64()
		return f, nil
	default:
		return 0, errors.Errorf("value %v is not numeric", value)
	}
}
