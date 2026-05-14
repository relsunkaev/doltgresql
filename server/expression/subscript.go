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
	"strconv"

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
	if dt.ID == types.JsonB.ID {
		return types.JsonB
	}
	if dt.ID == types.Name.ID {
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

	if dt, ok := s.Child.Type(ctx).(*types.DoltgresType); ok && dt.ID == types.JsonB.ID {
		pathElement, err := jsonbSubscriptPathElement(ctx, indexVal)
		if err != nil {
			return nil, err
		}
		doc, err := types.JsonDocumentFromSQLValue(ctx, types.JsonB, childVal)
		if err != nil {
			return nil, err
		}
		value, ok, err := types.JsonValueExtractPath(doc.Value, []string{pathElement})
		if err != nil || !ok {
			return nil, err
		}
		return types.JsonDocument{Value: value}, nil
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
		if !ok || dt.ID != types.Name.ID {
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
		if index < 0 || int(index) >= types.NameLength+1 {
			return nil, nil
		}
		if int(index) >= len(child) {
			return "\x00", nil
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

// ArraySetElement returns a copy of a one-dimensional array with one element updated.
type ArraySetElement struct {
	Array sql.Expression
	Index sql.Expression
	Value sql.Expression
}

var _ vitess.Injectable = (*ArraySetElement)(nil)
var _ sql.Expression = (*ArraySetElement)(nil)

// NewArraySetElement creates a new array element assignment expression.
func NewArraySetElement(array, index, value sql.Expression) *ArraySetElement {
	return &ArraySetElement{
		Array: array,
		Index: index,
		Value: value,
	}
}

// Resolved implements the sql.Expression interface.
func (s ArraySetElement) Resolved() bool {
	return s.Array != nil && s.Index != nil && s.Value != nil &&
		s.Array.Resolved() && s.Index.Resolved() && s.Value.Resolved()
}

// String implements the sql.Expression interface.
func (s ArraySetElement) String() string {
	if s.Array == nil || s.Index == nil || s.Value == nil {
		return "array_set_element(unresolved, unresolved, unresolved)"
	}
	return fmt.Sprintf("array_set_element(%s, %s, %s)", s.Array, s.Index, s.Value)
}

// Type implements the sql.Expression interface.
func (s ArraySetElement) Type(ctx *sql.Context) sql.Type {
	if s.Array == nil {
		return nil
	}
	return s.Array.Type(ctx)
}

// IsNullable implements the sql.Expression interface.
func (s ArraySetElement) IsNullable(ctx *sql.Context) bool {
	return s.Array == nil || s.Array.IsNullable(ctx)
}

// Eval implements the sql.Expression interface.
func (s ArraySetElement) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	indexVal, err := s.Index.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if indexVal == nil {
		return s.Array.Eval(ctx, row)
	}

	arrayType, ok := s.Array.Type(ctx).(*types.DoltgresType)
	if ok && arrayType.ID == types.JsonB.ID {
		return s.evalJsonb(ctx, row, indexVal)
	}

	index, ok := indexVal.(int32)
	if !ok {
		converted, _, err := types.Int32.Convert(ctx, indexVal)
		if err != nil {
			return nil, err
		}
		index = converted.(int32)
	}

	value, err := s.Value.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	arrayType, ok = s.Array.Type(ctx).(*types.DoltgresType)
	if !ok {
		return nil, fmt.Errorf("unsupported array assignment target type %T", s.Array.Type(ctx))
	}
	baseType, err := arrayType.ResolveArrayBaseType(ctx)
	if err != nil {
		return nil, err
	}
	if value != nil {
		value, _, err = baseType.Convert(ctx, value)
		if err != nil {
			return nil, err
		}
	}

	arrayVal, err := s.Array.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	elements, ok := types.ArrayElements(arrayVal)
	if arrayVal != nil && !ok {
		return nil, fmt.Errorf("unsupported type %T for array assignment", arrayVal)
	}

	lowerBound := index
	if arrayVal != nil {
		lowerBound = arrayLowerBound(ctx, s.Array)
		if types.ArrayHasNonDefaultLowerBounds(types.ArrayLowerBounds(arrayVal)) {
			lowerBound = types.ArrayLowerBound(arrayVal, 1)
		}
	}

	upperBound := lowerBound + int32(len(elements)) - 1
	newLowerBound := lowerBound
	if len(elements) == 0 || index < newLowerBound {
		newLowerBound = index
	}
	newUpperBound := upperBound
	if len(elements) == 0 || index > newUpperBound {
		newUpperBound = index
	}

	newElements := make([]any, int(newUpperBound-newLowerBound)+1)
	copy(newElements[int(lowerBound-newLowerBound):], elements)
	newElements[int(index-newLowerBound)] = value
	return types.NewArrayValue(newElements, []int32{newLowerBound}), nil
}

func (s ArraySetElement) evalJsonb(ctx *sql.Context, row sql.Row, indexVal any) (interface{}, error) {
	pathElement, err := jsonbSubscriptPathElement(ctx, indexVal)
	if err != nil {
		return nil, err
	}
	value, err := s.Value.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	newDoc, err := types.JsonDocumentFromSQLValue(ctx, types.JsonB, value)
	if err != nil {
		return nil, err
	}
	target, err := s.Array.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	targetDoc, err := types.JsonDocumentFromSQLValue(ctx, types.JsonB, target)
	if err != nil {
		return nil, err
	}
	newValue, err := types.JsonValueSetPath(targetDoc.Value, []string{pathElement}, newDoc.Value, true)
	if err != nil {
		return nil, err
	}
	return types.JsonDocument{Value: newValue}, nil
}

func jsonbSubscriptPathElement(ctx *sql.Context, value any) (string, error) {
	switch value := value.(type) {
	case string:
		return value, nil
	case int:
		return strconv.Itoa(value), nil
	case int8:
		return strconv.FormatInt(int64(value), 10), nil
	case int16:
		return strconv.FormatInt(int64(value), 10), nil
	case int32:
		return strconv.FormatInt(int64(value), 10), nil
	case int64:
		return strconv.FormatInt(value, 10), nil
	case uint:
		return strconv.FormatUint(uint64(value), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(value), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(value), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(value), 10), nil
	case uint64:
		return strconv.FormatUint(value, 10), nil
	default:
		converted, _, err := types.Text.Convert(ctx, value)
		if err != nil {
			return "", err
		}
		return converted.(string), nil
	}
}

// Children implements the sql.Expression interface.
func (s ArraySetElement) Children() []sql.Expression {
	return []sql.Expression{s.Array, s.Index, s.Value}
}

// WithChildren implements the sql.Expression interface.
func (s ArraySetElement) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 3 {
		return nil, fmt.Errorf("expected 3 children, got %d", len(children))
	}
	return NewArraySetElement(children[0], children[1], children[2]), nil
}

// WithResolvedChildren implements the vitess.Injectable interface.
func (s ArraySetElement) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 3 {
		return nil, fmt.Errorf("expected 3 children, got %d", len(children))
	}
	expressions := make([]sql.Expression, len(children))
	for i, child := range children {
		expr, ok := child.(sql.Expression)
		if !ok {
			return nil, fmt.Errorf("expected child to be an expression but has type `%T`", child)
		}
		expressions[i] = expr
	}
	return NewArraySetElement(expressions[0], expressions[1], expressions[2]), nil
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
