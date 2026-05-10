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
	"fmt"
	"math"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	gmsexpression "github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/transform"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

type VectorAggregation struct {
	child       sql.Expression
	id          sql.ColumnId
	function    string
	description string
	average     bool
	window      *sql.WindowDefinition
}

var _ sql.FunctionExpression = (*VectorAggregation)(nil)
var _ sql.Aggregation = (*VectorAggregation)(nil)

func NewVectorSum(child sql.Expression) *VectorAggregation {
	return &VectorAggregation{
		child:       child,
		function:    "sum",
		description: "returns the vector sum of expr in all rows",
	}
}

func NewVectorAvg(child sql.Expression) *VectorAggregation {
	return &VectorAggregation{
		child:       child,
		function:    "avg",
		description: "returns the vector average of expr in all rows",
		average:     true,
	}
}

func (a *VectorAggregation) FunctionName() string {
	return a.function
}

func (a *VectorAggregation) Description() string {
	return a.description
}

func (a *VectorAggregation) Type(ctx *sql.Context) sql.Type {
	return pgtypes.Vector
}

func (a *VectorAggregation) IsNullable(ctx *sql.Context) bool {
	return true
}

func (a *VectorAggregation) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return nil, errors.Errorf("Eval should not be called on %s aggregate", a.function)
}

func (a *VectorAggregation) Children() []sql.Expression {
	children := []sql.Expression{a.child}
	if a.window != nil {
		children = append(children, a.window.ToExpressions()...)
	}
	return children
}

func (a *VectorAggregation) Resolved() bool {
	if !a.child.Resolved() {
		return false
	}
	if a.window == nil {
		return true
	}
	return gmsexpression.ExpressionsResolved(append(a.window.OrderBy.ToExpressions(), a.window.PartitionBy...)...)
}

func (a *VectorAggregation) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	expected := 1
	if a.window != nil {
		expected += len(a.window.ToExpressions())
	}
	if len(children) != expected {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(children), expected)
	}

	next := *a
	next.child = children[0]
	if a.window != nil {
		window, err := a.window.FromExpressions(ctx, children[1:])
		if err != nil {
			return nil, err
		}
		next.window = window
	}
	return &next, nil
}

func (a *VectorAggregation) String() string {
	if a.window != nil {
		pr := sql.NewTreePrinter()
		_ = pr.WriteNode(strings.ToUpper(a.function))
		pr.WriteChildren(a.window.String(), a.child.String())
		return pr.String()
	}
	return fmt.Sprintf("%s(%s)", strings.ToUpper(a.function), a.child.String())
}

func (a *VectorAggregation) Id() sql.ColumnId {
	return a.id
}

func (a VectorAggregation) WithId(id sql.ColumnId) sql.IdExpression {
	a.id = id
	return &a
}

func (a VectorAggregation) WithWindow(ctx *sql.Context, window *sql.WindowDefinition) sql.WindowAdaptableExpression {
	a.window = window
	return &a
}

func (a *VectorAggregation) Window() *sql.WindowDefinition {
	return a.window
}

func (a *VectorAggregation) NewWindowFunction(ctx *sql.Context) (sql.WindowFunction, error) {
	return nil, errors.Errorf("%s(vector) window functions are not supported", a.function)
}

func (a *VectorAggregation) NewBuffer(ctx *sql.Context) (sql.AggregationBuffer, error) {
	child, err := transform.Clone(ctx, a.child)
	if err != nil {
		return nil, err
	}
	return &vectorAggregationBuffer{
		child:   child,
		average: a.average,
	}, nil
}

type vectorAggregationBuffer struct {
	child   sql.Expression
	sums    []float64
	count   int64
	average bool
}

func (b *vectorAggregationBuffer) Update(ctx *sql.Context, row sql.Row) error {
	value, err := b.child.Eval(ctx, row)
	if err != nil {
		return err
	}
	if value == nil {
		return nil
	}

	values, ok := value.([]float32)
	if !ok {
		return errors.Errorf("expected vector aggregate input []float32, got %T", value)
	}
	if len(values) < 1 {
		return errors.Errorf("vector must have at least 1 dimension")
	}
	if len(values) > pgtypes.MaxVectorDimensions {
		return errors.Errorf("vector cannot have more than %d dimensions", pgtypes.MaxVectorDimensions)
	}
	if len(b.sums) == 0 {
		b.sums = make([]float64, len(values))
	} else if len(b.sums) != len(values) {
		return errors.Errorf("different vector dimensions %d and %d", len(b.sums), len(values))
	}

	for i, element := range values {
		next := b.sums[i] + float64(element)
		if math.IsInf(next, 0) {
			return errors.Errorf("value out of range: overflow")
		}
		b.sums[i] = next
	}
	b.count++
	return nil
}

func (b *vectorAggregationBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	if b.count == 0 {
		return nil, nil
	}

	result := make([]float32, len(b.sums))
	for i, sum := range b.sums {
		value := sum
		if b.average {
			value = value / float64(b.count)
		}
		element := float32(value)
		if math.IsInf(value, 0) || math.IsInf(float64(element), 0) {
			return nil, errors.Errorf("value out of range: overflow")
		}
		result[i] = element
	}
	return result, nil
}

func (b *vectorAggregationBuffer) Dispose(ctx *sql.Context) {
	gmsexpression.Dispose(ctx, b.child)
}
