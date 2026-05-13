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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/server/types"
)

// Subscript represents a subscript expression, e.g. `a[1]`.
type Subscript struct {
	Child sql.Expression
	Index sql.Expression
}

var _ vitess.Injectable = (*Subscript)(nil)
var _ sql.Expression = (*Subscript)(nil)

// NewSubscript creates a new Subscript expression.
func NewSubscript(child, index sql.Expression) *Subscript {
	return &Subscript{
		Child: child,
		Index: index,
	}
}

// Resolved implements the sql.Expression interface.
func (s Subscript) Resolved() bool {
	if s.Child == nil || s.Index == nil {
		return false
	}
	return s.Child.Resolved() && s.Index.Resolved()
}

// String implements the sql.Expression interface.
func (s Subscript) String() string {
	if s.Child == nil || s.Index == nil {
		return "unresolved[unresolved]"
	}
	return fmt.Sprintf("%s[%s]", s.Child, s.Index)
}

// Type implements the sql.Expression interface.
func (s Subscript) Type(ctx *sql.Context) sql.Type {
	dt, ok := s.Child.Type(ctx).(*types.DoltgresType)
	if !ok {
		panic(fmt.Sprintf("unexpected type %T for subscript", s.Child.Type(ctx)))
	}
	if dt == types.Name {
		return types.InternalChar
	}
	baseType, err := dt.ResolveArrayBaseType(ctx)
	if err != nil {
		panic(err.Error())
	}
	return baseType
}

// IsNullable implements the sql.Expression interface.
func (s Subscript) IsNullable(ctx *sql.Context) bool {
	return true
}

// Eval implements the sql.Expression interface.
func (s Subscript) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	childVal, err := s.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if childVal == nil {
		return nil, nil
	}

	indexVal, err := s.Index.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if indexVal == nil {
		return nil, nil
	}

	if child, ok := types.ArrayElements(childVal); ok {
		index, ok := indexVal.(int32)
		if !ok {
			converted, _, err := types.Int32.Convert(ctx, indexVal)
			if err != nil {
				return nil, err
			}
			index = converted.(int32)
		}

		lowerBound := arrayLowerBound(ctx, s.Child)
		if types.ArrayHasNonDefaultLowerBounds(types.ArrayLowerBounds(childVal)) {
			lowerBound = types.ArrayLowerBound(childVal, 1)
		}
		if index < lowerBound || int(index-lowerBound) >= len(child) {
			return nil, nil
		}
		return child[index-lowerBound], nil
	}

	switch child := childVal.(type) {
	case string:
		dt, ok := s.Child.Type(ctx).(*types.DoltgresType)
		if !ok || dt != types.Name {
			return nil, fmt.Errorf("unsupported type %T for subscript", child)
		}

		index, ok := indexVal.(int32)
		if !ok {
			converted, _, err := types.Int32.Convert(ctx, indexVal)
			if err != nil {
				return nil, err
			}
			index = converted.(int32)
		}

		// PostgreSQL name uses raw_array_subscript_handler over its fixed byte buffer.
		// Unlike SQL arrays, this raw subscript path is zero-based.
		if index < 0 || int(index) >= len(child) {
			return nil, nil
		}
		return child[index : index+1], nil
	default:
		return nil, fmt.Errorf("unsupported type %T for subscript", child)
	}
}

// Children implements the sql.Expression interface.
func (s Subscript) Children() []sql.Expression {
	return []sql.Expression{s.Child, s.Index}
}

// WithChildren implements the sql.Expression interface.
func (s Subscript) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, fmt.Errorf("expected 2 children, got %d", len(children))
	}
	return NewSubscript(children[0], children[1]), nil
}

// WithResolvedChildren implements the vitess.Injectable interface.
func (s Subscript) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 2 {
		return nil, fmt.Errorf("expected 2 children, got %d", len(children))
	}
	child, ok := children[0].(sql.Expression)
	if !ok {
		return nil, fmt.Errorf("expected child to be an expression but has type `%T`", children[0])
	}
	index, ok := children[1].(sql.Expression)
	if !ok {
		return nil, fmt.Errorf("expected index to be an expression but has type `%T`", children[1])
	}

	return NewSubscript(child, index), nil
}

// SliceSubscript represents a one-dimensional array slice expression, e.g. `a[2:3]`.
type SliceSubscript struct {
	Child    sql.Expression
	Begin    sql.Expression
	End      sql.Expression
	HasBegin bool
	HasEnd   bool
}

var _ vitess.Injectable = (*SliceSubscript)(nil)
var _ sql.Expression = (*SliceSubscript)(nil)

// NewSliceSubscript creates a new SliceSubscript expression.
func NewSliceSubscript(child, begin, end sql.Expression, hasBegin, hasEnd bool) *SliceSubscript {
	return &SliceSubscript{
		Child:    child,
		Begin:    begin,
		End:      end,
		HasBegin: hasBegin,
		HasEnd:   hasEnd,
	}
}

// NewSliceSubscriptInjectable creates a SliceSubscript template for Vitess injection.
func NewSliceSubscriptInjectable(hasBegin, hasEnd bool) *SliceSubscript {
	return &SliceSubscript{
		HasBegin: hasBegin,
		HasEnd:   hasEnd,
	}
}

// Resolved implements the sql.Expression interface.
func (s SliceSubscript) Resolved() bool {
	if s.Child == nil || (s.HasBegin && s.Begin == nil) || (s.HasEnd && s.End == nil) {
		return false
	}
	return s.Child.Resolved() &&
		(!s.HasBegin || s.Begin.Resolved()) &&
		(!s.HasEnd || s.End.Resolved())
}

// String implements the sql.Expression interface.
func (s SliceSubscript) String() string {
	child := "unresolved"
	if s.Child != nil {
		child = s.Child.String()
	}
	begin := ""
	if s.HasBegin && s.Begin != nil {
		begin = s.Begin.String()
	}
	end := ""
	if s.HasEnd && s.End != nil {
		end = s.End.String()
	}
	return fmt.Sprintf("%s[%s:%s]", child, begin, end)
}

// Type implements the sql.Expression interface.
func (s SliceSubscript) Type(ctx *sql.Context) sql.Type {
	dt, ok := s.Child.Type(ctx).(*types.DoltgresType)
	if !ok {
		panic(fmt.Sprintf("unexpected type %T for slice subscript", s.Child.Type(ctx)))
	}
	if isZeroBasedVectorType(dt) {
		return dt.ArrayBaseType().ToArrayType()
	}
	return dt
}

// IsNullable implements the sql.Expression interface.
func (s SliceSubscript) IsNullable(ctx *sql.Context) bool {
	return true
}

// Eval implements the sql.Expression interface.
func (s SliceSubscript) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	childVal, err := s.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if childVal == nil {
		return nil, nil
	}

	child, ok := types.ArrayElements(childVal)
	if !ok {
		return nil, fmt.Errorf("unsupported type %T for slice subscript", childVal)
	}

	lowerBound := arrayLowerBound(ctx, s.Child)
	if types.ArrayHasNonDefaultLowerBounds(types.ArrayLowerBounds(childVal)) {
		lowerBound = types.ArrayLowerBound(childVal, 1)
	}
	upperBound := lowerBound + int32(len(child)) - 1
	begin := lowerBound
	end := upperBound

	if s.HasBegin {
		begin, ok, err = evalSliceBound(ctx, row, s.Begin)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}
	}
	if s.HasEnd {
		end, ok, err = evalSliceBound(ctx, row, s.End)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}
	}

	if begin < lowerBound {
		begin = lowerBound
	}
	if end > upperBound {
		end = upperBound
	}
	if end < begin {
		return []any{}, nil
	}

	start := int(begin - lowerBound)
	stop := int(end-lowerBound) + 1
	result := make([]any, stop-start)
	copy(result, child[start:stop])
	return result, nil
}

func evalSliceBound(ctx *sql.Context, row sql.Row, bound sql.Expression) (int32, bool, error) {
	val, err := bound.Eval(ctx, row)
	if err != nil {
		return 0, false, err
	}
	if val == nil {
		return 0, false, nil
	}
	index, ok := val.(int32)
	if ok {
		return index, true, nil
	}
	converted, _, err := types.Int32.Convert(ctx, val)
	if err != nil {
		return 0, false, err
	}
	return converted.(int32), true, nil
}

func arrayLowerBound(ctx *sql.Context, expr sql.Expression) int32 {
	if dt, ok := expr.Type(ctx).(*types.DoltgresType); ok && isZeroBasedVectorType(dt) {
		return 0
	}
	if cast, ok := expr.(*ExplicitCast); ok {
		source, sourceOk := cast.Child().Type(ctx).(*types.DoltgresType)
		target, targetOk := cast.Type(ctx).(*types.DoltgresType)
		if sourceOk && targetOk && isZeroBasedVectorType(source) && target.IsArrayCategory() &&
			target.ArrayBaseType().ID == source.ArrayBaseType().ID {
			return 0
		}
	}
	return 1
}

func isZeroBasedVectorType(dt *types.DoltgresType) bool {
	return dt.ID == types.Int16vector.ID || dt.ID == types.Oidvector.ID
}

// Children implements the sql.Expression interface.
func (s SliceSubscript) Children() []sql.Expression {
	children := []sql.Expression{s.Child}
	if s.HasBegin {
		children = append(children, s.Begin)
	}
	if s.HasEnd {
		children = append(children, s.End)
	}
	return children
}

// WithChildren implements the sql.Expression interface.
func (s SliceSubscript) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	expected := 1
	if s.HasBegin {
		expected++
	}
	if s.HasEnd {
		expected++
	}
	if len(children) != expected {
		return nil, fmt.Errorf("expected %d children, got %d", expected, len(children))
	}

	idx := 1
	var begin sql.Expression
	if s.HasBegin {
		begin = children[idx]
		idx++
	}
	var end sql.Expression
	if s.HasEnd {
		end = children[idx]
	}
	return NewSliceSubscript(children[0], begin, end, s.HasBegin, s.HasEnd), nil
}

// WithResolvedChildren implements the vitess.Injectable interface.
func (s SliceSubscript) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	expected := 1
	if s.HasBegin {
		expected++
	}
	if s.HasEnd {
		expected++
	}
	if len(children) != expected {
		return nil, fmt.Errorf("expected %d children, got %d", expected, len(children))
	}

	expressions := make([]sql.Expression, len(children))
	for i, child := range children {
		expr, ok := child.(sql.Expression)
		if !ok {
			return nil, fmt.Errorf("expected child to be an expression but has type `%T`", child)
		}
		expressions[i] = expr
	}

	idx := 1
	var begin sql.Expression
	if s.HasBegin {
		begin = expressions[idx]
		idx++
	}
	var end sql.Expression
	if s.HasEnd {
		end = expressions[idx]
	}
	return NewSliceSubscript(expressions[0], begin, end, s.HasBegin, s.HasEnd), nil
}
