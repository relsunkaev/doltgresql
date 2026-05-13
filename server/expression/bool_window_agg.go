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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	gmsexpression "github.com/dolthub/go-mysql-server/sql/expression"
	gmsaggregation "github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

const (
	BoolAndWindowMarker = "__doltgres_bool_and_window"
	BoolOrWindowMarker  = "__doltgres_bool_or_window"
)

// NewDoltgresWindowAggregate dispatches PostgreSQL aggregate-window calls
// through a GMS-recognized internal window function name.
func NewDoltgresWindowAggregate(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	if len(args) == 1 {
		return NewArrayAggWindow(ctx, args[0]), nil
	}
	if len(args) != 2 {
		return nil, fmt.Errorf("%s requires one array_agg argument or a marker plus child expression", ArrayAggWindowFunctionName)
	}

	marker, err := literalString(ctx, args[0])
	if err != nil {
		return nil, err
	}
	switch marker {
	case BoolAndWindowMarker:
		return NewBoolWindowAgg(args[1], true), nil
	case BoolOrWindowMarker:
		return NewBoolWindowAgg(args[1], false), nil
	default:
		return nil, fmt.Errorf("unknown Doltgres window aggregate marker %q", marker)
	}
}

func literalString(ctx *sql.Context, expr sql.Expression) (string, error) {
	val, err := expr.Eval(ctx, nil)
	if err != nil {
		return "", err
	}
	str, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("expected Doltgres window aggregate marker string, got %T", val)
	}
	return str, nil
}

type BoolWindowAgg struct {
	child  sql.Expression
	isAnd  bool
	window *sql.WindowDefinition
	id     sql.ColumnId
}

var _ sql.WindowAdaptableExpression = (*BoolWindowAgg)(nil)

func NewBoolWindowAgg(child sql.Expression, isAnd bool) *BoolWindowAgg {
	return &BoolWindowAgg{child: child, isAnd: isAnd}
}

func (b *BoolWindowAgg) Resolved() bool {
	if !b.child.Resolved() {
		return false
	}
	for _, expr := range b.window.ToExpressions() {
		if !expr.Resolved() {
			return false
		}
	}
	return true
}

func (b *BoolWindowAgg) String() string {
	sb := strings.Builder{}
	if b.isAnd {
		sb.WriteString("bool_and(")
	} else {
		sb.WriteString("bool_or(")
	}
	sb.WriteString(b.child.String())
	sb.WriteString(")")
	if b.window != nil {
		sb.WriteString(" ")
		sb.WriteString(b.window.String())
	}
	return sb.String()
}

func (b *BoolWindowAgg) Type(ctx *sql.Context) sql.Type {
	return pgtypes.Bool
}

func (b *BoolWindowAgg) IsNullable(ctx *sql.Context) bool {
	return true
}

func (b *BoolWindowAgg) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	panic("eval should never be called on a window aggregate")
}

func (b *BoolWindowAgg) Children() []sql.Expression {
	children := make([]sql.Expression, 0, 1+len(b.window.ToExpressions()))
	children = append(children, b.child)
	children = append(children, b.window.ToExpressions()...)
	return children
}

func (b BoolWindowAgg) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	expected := 1 + len(b.window.ToExpressions())
	if len(children) != expected {
		return nil, sql.ErrInvalidChildrenNumber.New(b, len(children), expected)
	}
	window, err := b.window.FromExpressions(ctx, children[1:])
	if err != nil {
		return nil, err
	}
	b.child = children[0]
	b.window = window
	return &b, nil
}

func (b *BoolWindowAgg) Id() sql.ColumnId {
	return b.id
}

func (b BoolWindowAgg) WithId(id sql.ColumnId) sql.IdExpression {
	b.id = id
	return &b
}

func (b *BoolWindowAgg) NewWindowFunction(ctx *sql.Context) (sql.WindowFunction, error) {
	return (&boolWindowFunction{agg: b}).WithWindow(ctx, b.Window())
}

func (b BoolWindowAgg) WithWindow(ctx *sql.Context, window *sql.WindowDefinition) sql.WindowAdaptableExpression {
	b.window = window
	return &b
}

func (b *BoolWindowAgg) Window() *sql.WindowDefinition {
	return b.window
}

type boolWindowFunction struct {
	agg    *BoolWindowAgg
	framer sql.WindowFramer
}

var _ sql.WindowFunction = (*boolWindowFunction)(nil)

func (b *boolWindowFunction) WithWindow(ctx *sql.Context, window *sql.WindowDefinition) (sql.WindowFunction, error) {
	next := *b
	if window != nil && window.Frame != nil {
		framer, err := window.Frame.NewFramer(window)
		if err != nil {
			return nil, err
		}
		next.framer = framer
	}
	return &next, nil
}

func (b *boolWindowFunction) Dispose(ctx *sql.Context) {
	gmsexpression.Dispose(ctx, b.agg.child)
}

func (b *boolWindowFunction) StartPartition(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) error {
	return nil
}

func (b *boolWindowFunction) DefaultFramer() sql.WindowFramer {
	if b.framer != nil {
		return b.framer
	}
	return gmsaggregation.NewUnboundedPrecedingToCurrentRowFramer()
}

func (b *boolWindowFunction) Compute(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) (interface{}, error) {
	result := true
	if !b.agg.isAnd {
		result = false
	}
	sawOne := false
	for i := interval.Start; i < interval.End; i++ {
		val, err := b.agg.child.Eval(ctx, buffer[i])
		if err != nil {
			return nil, err
		}
		if val == nil {
			continue
		}
		converted, _, err := pgtypes.Bool.Convert(ctx, val)
		if err != nil {
			return nil, err
		}
		boolVal := converted.(bool)
		sawOne = true
		if b.agg.isAnd {
			result = result && boolVal
		} else {
			result = result || boolVal
		}
	}
	if !sawOne {
		return nil, nil
	}
	return result, nil
}
