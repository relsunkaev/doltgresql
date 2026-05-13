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
	"github.com/dolthub/go-mysql-server/sql/types"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

type valueWindowKind string

const (
	valueWindowLag      valueWindowKind = "lag"
	valueWindowLead     valueWindowKind = "lead"
	valueWindowNthValue valueWindowKind = "nth_value"
	valueWindowNTile    valueWindowKind = "ntile"
)

type ValueWindowAgg struct {
	kind   valueWindowKind
	args   []sql.Expression
	window *sql.WindowDefinition
	id     sql.ColumnId
}

var _ sql.FunctionExpression = (*ValueWindowAgg)(nil)
var _ sql.WindowAdaptableExpression = (*ValueWindowAgg)(nil)
var _ sql.CollationCoercible = (*ValueWindowAgg)(nil)

func NewValueWindowAgg(kind valueWindowKind, args ...sql.Expression) (*ValueWindowAgg, error) {
	switch kind {
	case valueWindowLag, valueWindowLead:
		if len(args) < 1 || len(args) > 3 {
			return nil, sql.ErrInvalidArgumentNumber.New(strings.ToUpper(string(kind)), "1, 2, or 3", len(args))
		}
	case valueWindowNthValue:
		if len(args) != 2 {
			return nil, sql.ErrInvalidArgumentNumber.New("NTH_VALUE", "2", len(args))
		}
	case valueWindowNTile:
		if len(args) != 1 {
			return nil, sql.ErrInvalidArgumentNumber.New("NTILE", "1", len(args))
		}
	default:
		return nil, fmt.Errorf("unknown Doltgres value window kind %q", kind)
	}
	return &ValueWindowAgg{kind: kind, args: args}, nil
}

func (v *ValueWindowAgg) FunctionName() string {
	return strings.ToUpper(string(v.kind))
}

func (v *ValueWindowAgg) Description() string {
	switch v.kind {
	case valueWindowLag:
		return "returns the value evaluated at the lag offset row"
	case valueWindowLead:
		return "returns the value evaluated at the lead offset row"
	case valueWindowNthValue:
		return "returns the value evaluated at the nth row of the current window frame"
	case valueWindowNTile:
		return "returns the current row's bucket number"
	default:
		return ""
	}
}

func (v *ValueWindowAgg) Resolved() bool {
	for _, arg := range v.args {
		if !arg.Resolved() {
			return false
		}
	}
	for _, expr := range windowExpressions(v.window) {
		if !expr.Resolved() {
			return false
		}
	}
	return true
}

func (v *ValueWindowAgg) String() string {
	sb := strings.Builder{}
	sb.WriteString(string(v.kind))
	sb.WriteString("(")
	for i, arg := range v.args {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(arg.String())
	}
	sb.WriteString(")")
	if v.window != nil {
		sb.WriteString(" ")
		sb.WriteString(v.window.String())
	}
	return sb.String()
}

func (v *ValueWindowAgg) Type(ctx *sql.Context) sql.Type {
	if v.kind == valueWindowNTile {
		return pgtypes.Int32
	}
	if len(v.args) == 0 {
		return pgtypes.Unknown
	}
	return v.args[0].Type(ctx)
}

func (v *ValueWindowAgg) IsNullable(ctx *sql.Context) bool {
	return v.kind != valueWindowNTile
}

func (v *ValueWindowAgg) CollationCoercibility(ctx *sql.Context) (sql.CollationID, byte) {
	if v.kind == valueWindowNTile || len(v.args) == 0 {
		return sql.Collation_binary, 5
	}
	return sql.GetCoercibility(ctx, v.args[0])
}

func (v *ValueWindowAgg) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return nil, sql.ErrWindowUnsupported.New(v.FunctionName())
}

func (v *ValueWindowAgg) Children() []sql.Expression {
	children := make([]sql.Expression, 0, len(v.args)+len(windowExpressions(v.window)))
	children = append(children, v.args...)
	children = append(children, windowExpressions(v.window)...)
	return children
}

func (v ValueWindowAgg) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) < len(v.args) {
		return nil, sql.ErrInvalidChildrenNumber.New(v, len(children), len(v.args))
	}
	windowChildren := children[len(v.args):]
	window := v.window
	if window != nil {
		var err error
		window, err = window.FromExpressions(ctx, windowChildren)
		if err != nil {
			return nil, err
		}
	}
	v.args = children[:len(v.args)]
	v.window = window
	return &v, nil
}

func (v *ValueWindowAgg) Id() sql.ColumnId {
	return v.id
}

func (v ValueWindowAgg) WithId(id sql.ColumnId) sql.IdExpression {
	v.id = id
	return &v
}

func (v ValueWindowAgg) WithWindow(ctx *sql.Context, window *sql.WindowDefinition) sql.WindowAdaptableExpression {
	v.window = window
	return &v
}

func (v *ValueWindowAgg) Window() *sql.WindowDefinition {
	return v.window
}

func (v *ValueWindowAgg) NewWindowFunction(ctx *sql.Context) (sql.WindowFunction, error) {
	return (&valueWindowFunction{
		kind:   v.kind,
		args:   v.args,
		window: v.window,
	}).WithWindow(ctx, v.window)
}

func windowExpressions(window *sql.WindowDefinition) []sql.Expression {
	if window == nil {
		return nil
	}
	return window.ToExpressions()
}

type valueWindowFunction struct {
	kind           valueWindowKind
	args           []sql.Expression
	window         *sql.WindowDefinition
	framer         sql.WindowFramer
	partitionStart int
	partitionEnd   int
	pos            int
}

var _ sql.WindowFunction = (*valueWindowFunction)(nil)

func (v *valueWindowFunction) WithWindow(ctx *sql.Context, window *sql.WindowDefinition) (sql.WindowFunction, error) {
	next := *v
	next.window = window
	if window != nil && window.Frame != nil {
		framer, err := window.Frame.NewFramer(window)
		if err != nil {
			return nil, err
		}
		next.framer = framer
	}
	return &next, nil
}

func (v *valueWindowFunction) Dispose(ctx *sql.Context) {
	for _, arg := range v.args {
		gmsexpression.Dispose(ctx, arg)
	}
}

func (v *valueWindowFunction) StartPartition(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) error {
	v.partitionStart = interval.Start
	v.partitionEnd = interval.End
	v.pos = interval.Start
	return nil
}

func (v *valueWindowFunction) DefaultFramer() sql.WindowFramer {
	if v.framer != nil {
		return v.framer
	}
	switch v.kind {
	case valueWindowNthValue:
		return gmsaggregation.NewUnboundedPrecedingToCurrentRowFramer()
	default:
		return gmsaggregation.NewPartitionFramer()
	}
}

func (v *valueWindowFunction) Compute(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) (interface{}, error) {
	current := v.pos
	v.pos++
	switch v.kind {
	case valueWindowLag:
		return v.computeLeadLag(ctx, current, buffer, -1)
	case valueWindowLead:
		return v.computeLeadLag(ctx, current, buffer, 1)
	case valueWindowNthValue:
		return v.computeNthValue(ctx, current, interval, buffer)
	case valueWindowNTile:
		return v.computeNTile(ctx, current, buffer)
	default:
		return nil, fmt.Errorf("unknown Doltgres value window kind %q", v.kind)
	}
}

func (v *valueWindowFunction) computeLeadLag(ctx *sql.Context, current int, buffer sql.WindowBuffer, direction int) (interface{}, error) {
	offset := int64(1)
	var err error
	if len(v.args) >= 2 {
		var ok bool
		offset, ok, err = evalWindowInt(ctx, v.args[1], buffer[current], v.FunctionName())
		if err != nil || !ok {
			return nil, err
		}
		if offset < 0 {
			return nil, sql.ErrInvalidArgument.New(v.FunctionName())
		}
	}
	target := current + direction*int(offset)
	if target >= v.partitionStart && target < v.partitionEnd {
		return v.args[0].Eval(ctx, buffer[target])
	}
	if len(v.args) >= 3 {
		return v.args[2].Eval(ctx, buffer[current])
	}
	return nil, nil
}

func (v *valueWindowFunction) computeNthValue(ctx *sql.Context, current int, interval sql.WindowInterval, buffer sql.WindowBuffer) (interface{}, error) {
	nth, ok, err := evalWindowInt(ctx, v.args[1], buffer[current], v.FunctionName())
	if err != nil || !ok {
		return nil, err
	}
	if nth <= 0 {
		return nil, sql.ErrInvalidArgument.New(v.FunctionName())
	}
	target := interval.Start + int(nth) - 1
	if target < interval.Start || target >= interval.End {
		return nil, nil
	}
	return v.args[0].Eval(ctx, buffer[target])
}

func (v *valueWindowFunction) computeNTile(ctx *sql.Context, current int, buffer sql.WindowBuffer) (interface{}, error) {
	buckets, ok, err := evalWindowInt(ctx, v.args[0], buffer[current], v.FunctionName())
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrInvalidArgument.New(v.FunctionName())
	}
	if buckets <= 0 {
		return nil, sql.ErrInvalidArgument.New(v.FunctionName())
	}
	count := int64(v.partitionEnd - v.partitionStart)
	if count <= 0 {
		return nil, nil
	}
	pos := int64(current - v.partitionStart)
	var bucket int64
	if buckets >= count {
		bucket = pos + 1
	} else {
		baseSize := count / buckets
		largeBuckets := count % buckets
		largeRows := largeBuckets * (baseSize + 1)
		if pos < largeRows {
			bucket = pos/(baseSize+1) + 1
		} else {
			bucket = largeBuckets + (pos-largeRows)/baseSize + 1
		}
	}
	return int32(bucket), nil
}

func (v *valueWindowFunction) FunctionName() string {
	return strings.ToUpper(string(v.kind))
}

func evalWindowInt(ctx *sql.Context, expr sql.Expression, row sql.Row, functionName string) (int64, bool, error) {
	val, err := expr.Eval(ctx, row)
	if err != nil || val == nil {
		return 0, false, err
	}
	converted, _, err := types.Int64.Convert(ctx, val)
	if err != nil {
		return 0, false, err
	}
	intVal, ok := converted.(int64)
	if !ok {
		return 0, false, fmt.Errorf("%s expected integer argument, got %T", functionName, converted)
	}
	return intVal, true, nil
}
