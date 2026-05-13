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

package expression

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/shopspring/decimal"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// GMSCast handles the conversion from a GMS expression's type to its Doltgres type that is most similar.
type GMSCast struct {
	sqlChild sql.Expression
}

var _ sql.Expression = (*GMSCast)(nil)

// NewGMSCast returns a new *GMSCast.
func NewGMSCast(child sql.Expression) *GMSCast {
	return &GMSCast{
		sqlChild: child,
	}
}

// WindowGMSCast handles GMS-to-Doltgres value conversion for window functions.
// Window execution requires the top-level expression to implement
// sql.WindowAggregation, so a plain GMSCast wrapper would otherwise force the
// executor down the ordinary Eval path.
type WindowGMSCast struct {
	*GMSCast
}

var _ sql.Expression = (*WindowGMSCast)(nil)
var _ sql.WindowAggregation = (*WindowGMSCast)(nil)

// NewWindowGMSCast returns a GMS cast that remains executable as a window aggregation.
func NewWindowGMSCast(child sql.WindowAdaptableExpression) *WindowGMSCast {
	return &WindowGMSCast{GMSCast: NewGMSCast(child)}
}

// AggregationGMSCast handles GMS-to-Doltgres value conversion for ordinary
// aggregations. GroupBy execution requires select expressions to implement
// sql.Aggregation, so a plain GMSCast wrapper would otherwise be evaluated as a
// non-aggregate first-value expression.
type AggregationGMSCast struct {
	*GMSCast
}

var _ sql.Expression = (*AggregationGMSCast)(nil)
var _ sql.Aggregation = (*AggregationGMSCast)(nil)

// NewAggregationGMSCast returns a GMS cast that remains executable as an aggregation.
func NewAggregationGMSCast(child sql.Aggregation) *AggregationGMSCast {
	return &AggregationGMSCast{GMSCast: NewGMSCast(child)}
}

// Children implements the sql.Expression interface.
func (c *GMSCast) Children() []sql.Expression {
	return []sql.Expression{c.sqlChild}
}

// Child returns the child that is being cast.
func (c *GMSCast) Child() sql.Expression {
	return c.sqlChild
}

// DoltgresType returns the DoltgresType that the cast evaluates to. This is the same value that is returned by Type().
func (c *GMSCast) DoltgresType(ctx *sql.Context) *pgtypes.DoltgresType {
	if dt, ok := FunctionDoltgresType(ctx, c.sqlChild); ok {
		return dt
	}
	// GMSCast shouldn't receive a DoltgresType, but we shouldn't error if it happens
	if t, ok := c.sqlChild.Type(ctx).(*pgtypes.DoltgresType); ok {
		return t
	}

	return pgtypes.FromGmsType(c.sqlChild.Type(ctx))
}

// Eval implements the sql.Expression interface.
func (c *GMSCast) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	val, err := c.sqlChild.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	return castGMSExpressionValue(ctx, val, c.sqlChild)
}

func castGMSExpressionValue(ctx *sql.Context, val any, expr sql.Expression) (any, error) {
	if val == nil {
		return nil, nil
	}
	if newVal, ok, err := castFunctionValue(ctx, val, expr); ok {
		return newVal, err
	}
	// GMSCast shouldn't receive a DoltgresType, but we shouldn't error if it happens
	if _, ok := expr.Type(ctx).(*pgtypes.DoltgresType); ok {
		return val, nil
	}
	return castGMSValue(ctx, val, expr.Type(ctx))
}

func castGMSValue(ctx *sql.Context, val any, sqlTyp sql.Type) (any, error) {
	switch sqlTyp.Type() {
	// Boolean types are a special case because of how they are translated on the wire in Postgres. If we identify a
	// boolean result, we want to convert it from an int back to a boolean.
	case query.Type_INT8:
		if sqlTyp == types.Boolean {
			newVal, _, err := types.Int32.Convert(ctx, val)
			if err != nil {
				return nil, err
			}
			if _, ok := newVal.(int32); !ok {
				return nil, errors.Errorf("GMSCast expected type `int32`, got `%T`", val)
			}
			if newVal.(int32) == 0 {
				return false, nil
			} else {
				return true, nil
			}
		}
		fallthrough
		// In Postgres, Int32 is generally the smallest value returned. But we convert int8 and int16 to this type during
		// schema conversion, which means we must do so here as well to avoid runtime panics.
	case query.Type_INT16, query.Type_INT24, query.Type_INT32, query.Type_YEAR:
		newVal, _, err := types.Int32.Convert(ctx, val)
		if err != nil {
			return nil, err
		}
		if _, ok := newVal.(int32); !ok {
			return nil, errors.Errorf("GMSCast expected type `int32`, got `%T`", val)
		}
		return newVal, nil
	case query.Type_INT64, query.Type_BIT, query.Type_UINT8, query.Type_UINT16, query.Type_UINT24, query.Type_UINT32:
		newVal, _, err := types.Int64.Convert(ctx, val)
		if err != nil {
			return nil, err
		}
		if _, ok := newVal.(int64); !ok {
			return nil, errors.Errorf("GMSCast expected type `int64`, got `%T`", val)
		}
		return newVal, nil
	case query.Type_UINT64:
		// Postgres doesn't have a "public" Uint64 type, so we return a Numeric value
		newVal, _, err := types.InternalDecimalType.Convert(ctx, val)
		if err != nil {
			return nil, err
		}
		if _, ok := newVal.(decimal.Decimal); !ok {
			return nil, errors.Errorf("GMSCast expected type `decimal.Decimal`, got `%T`", val)
		}
		return newVal, nil
	case query.Type_FLOAT32:
		newVal, _, err := types.Float32.Convert(ctx, val)
		if err != nil {
			return nil, err
		}
		if _, ok := newVal.(float32); !ok {
			return nil, errors.Errorf("GMSCast expected type `float32`, got `%T`", val)
		}
		return newVal, nil
	case query.Type_FLOAT64:
		newVal, _, err := types.Float64.Convert(ctx, val)
		if err != nil {
			return nil, err
		}
		if _, ok := newVal.(float64); !ok {
			return nil, errors.Errorf("GMSCast expected type `float64`, got `%T`", val)
		}
		return newVal, nil
	case query.Type_DECIMAL:
		newVal, _, err := types.InternalDecimalType.Convert(ctx, val)
		if err != nil {
			return nil, err
		}
		if _, ok := newVal.(decimal.Decimal); !ok {
			return nil, errors.Errorf("GMSCast expected type `decimal.Decimal`, got `%T`", val)
		}
		return newVal, nil
	case query.Type_DATE, query.Type_DATETIME, query.Type_TIMESTAMP:
		if val, ok := val.(time.Time); ok {
			return val, nil
		}
		return nil, errors.Errorf("GMSCast expected type `Time`, got `%T`", val)
	case query.Type_TIME:
		if val, ok := val.(types.Timespan); ok {
			return val.String(), nil
		}
		return nil, errors.Errorf("GMSCast expected type `Timespan`, got `%T`", val)
	case query.Type_CHAR, query.Type_VARCHAR, query.Type_TEXT, query.Type_BINARY, query.Type_VARBINARY, query.Type_BLOB, query.Type_SET, query.Type_ENUM:
		newVal, _, err := types.LongText.Convert(ctx, val)
		if err != nil {
			return nil, err
		}
		switch newVal := newVal.(type) {
		case string:
			return newVal, nil
		case sql.StringWrapper:
			return newVal.Unwrap(ctx)
		default:
			return nil, errors.Errorf("GMSCast expected type `string`, got `%T`", val)
		}
	case query.Type_JSON:
		switch val := val.(type) {
		case types.JSONDocument:
			return val.JSONString()
		case tree.IndexedJsonDocument:
			return val.String(), nil
		default:
			// TODO: there are particular dolt tables (dolt_constraint_violations) that return json-marshallable structs
			//  that we need to handle here like this
			bytes, err := json.Marshal(val)
			return string(bytes), err
		}
	case query.Type_NULL_TYPE:
		return nil, nil
	case query.Type_GEOMETRY:
		return nil, errors.Errorf("GMS geometry types are not supported")
	default:
		return nil, errors.Errorf("GMS type `%s` is not supported", sqlTyp.String())
	}
}

// FunctionDoltgresType returns PostgreSQL return types for GMS functions
// whose runtime values need Doltgres conversion.
func FunctionDoltgresType(ctx *sql.Context, expr sql.Expression) (*pgtypes.DoltgresType, bool) {
	fn, ok := expr.(sql.FunctionExpression)
	if !ok {
		return nil, false
	}
	switch strings.ToUpper(fn.FunctionName()) {
	case "AVG":
		return avgDoltgresType(ctx, expr)
	case "COALESCE":
		return coalesceDoltgresType(ctx, expr)
	case "SUM":
		return sumDoltgresType(ctx, expr)
	case "ROW_NUMBER", "RANK", "DENSE_RANK":
		return pgtypes.Int64, true
	case "NTILE":
		return pgtypes.Int32, true
	case "PERCENT_RANK":
		return pgtypes.Float64, true
	default:
		return nil, false
	}
}

func coalesceDoltgresType(ctx *sql.Context, expr sql.Expression) (*pgtypes.DoltgresType, bool) {
	var result *pgtypes.DoltgresType
	for _, child := range expr.Children() {
		childType, ok := expressionDoltgresType(ctx, child)
		if !ok || childType.ID == pgtypes.Unknown.ID {
			continue
		}
		if result == nil {
			result = childType
			continue
		}
		promoted, ok := promoteCoalesceDoltgresType(result, childType)
		if !ok {
			return nil, false
		}
		result = promoted
	}
	if result == nil {
		return nil, false
	}
	return result, true
}

func expressionDoltgresType(ctx *sql.Context, expr sql.Expression) (*pgtypes.DoltgresType, bool) {
	if fnType, ok := FunctionDoltgresType(ctx, expr); ok {
		return fnType, true
	}
	if dt, ok := expr.Type(ctx).(*pgtypes.DoltgresType); ok {
		return dt, true
	}
	if expr.Type(ctx) == types.Null {
		return pgtypes.Unknown, true
	}
	return pgtypes.FromGmsType(expr.Type(ctx)), true
}

func promoteCoalesceDoltgresType(left *pgtypes.DoltgresType, right *pgtypes.DoltgresType) (*pgtypes.DoltgresType, bool) {
	if left.ID == pgtypes.Unknown.ID {
		return right, true
	}
	if right.ID == pgtypes.Unknown.ID {
		return left, true
	}
	if left.ID == right.ID {
		return left, true
	}
	if left.TypCategory == pgtypes.TypeCategory_NumericTypes && right.TypCategory == pgtypes.TypeCategory_NumericTypes {
		return promoteNumericCoalesceDoltgresType(left, right), true
	}
	if left.TypCategory == pgtypes.TypeCategory_StringTypes && right.TypCategory == pgtypes.TypeCategory_StringTypes {
		return pgtypes.Text, true
	}
	return nil, false
}

func promoteNumericCoalesceDoltgresType(left *pgtypes.DoltgresType, right *pgtypes.DoltgresType) *pgtypes.DoltgresType {
	switch {
	case left.ID == pgtypes.Float64.ID || right.ID == pgtypes.Float64.ID:
		return pgtypes.Float64
	case left.ID == pgtypes.Float32.ID || right.ID == pgtypes.Float32.ID:
		return pgtypes.Float32
	case left.ID == pgtypes.Numeric.ID || right.ID == pgtypes.Numeric.ID:
		return pgtypes.Numeric
	case left.ID == pgtypes.Int64.ID || right.ID == pgtypes.Int64.ID:
		return pgtypes.Int64
	default:
		return pgtypes.Int32
	}
}

// WindowFunctionDoltgresType returns the PostgreSQL return type for GMS window functions
// whose runtime values need Doltgres conversion.
func WindowFunctionDoltgresType(ctx *sql.Context, expr sql.Expression) (*pgtypes.DoltgresType, bool) {
	return FunctionDoltgresType(ctx, expr)
}

func avgDoltgresType(ctx *sql.Context, expr sql.Expression) (*pgtypes.DoltgresType, bool) {
	children := expr.Children()
	if len(children) < 1 {
		return nil, false
	}
	childType, ok := children[0].Type(ctx).(*pgtypes.DoltgresType)
	if !ok {
		childType = pgtypes.FromGmsType(children[0].Type(ctx))
	}
	childTypeName, isPgvectorType := pgtypes.PgvectorBaseTypeName(childType)
	switch {
	case isPgvectorType && childTypeName == "vector":
		return pgtypes.Vector, true
	case childType.Equals(pgtypes.Float32), childType.Equals(pgtypes.Float64):
		return pgtypes.Float64, true
	default:
		return pgtypes.Numeric, true
	}
}

func sumDoltgresType(ctx *sql.Context, expr sql.Expression) (*pgtypes.DoltgresType, bool) {
	children := expr.Children()
	if len(children) < 1 {
		return nil, false
	}
	childType, ok := children[0].Type(ctx).(*pgtypes.DoltgresType)
	if !ok {
		childType = pgtypes.FromGmsType(children[0].Type(ctx))
	}
	switch {
	case childType.Equals(pgtypes.Int16), childType.Equals(pgtypes.Int32):
		return pgtypes.Int64, true
	case childType.Equals(pgtypes.Int64), childType.Equals(pgtypes.Numeric):
		return pgtypes.Numeric, true
	case childType.Equals(pgtypes.Float32):
		return pgtypes.Float32, true
	case childType.Equals(pgtypes.Float64):
		return pgtypes.Float64, true
	default:
		return childType, true
	}
}

func castFunctionValue(ctx *sql.Context, val any, expr sql.Expression) (any, bool, error) {
	dt, ok := FunctionDoltgresType(ctx, expr)
	if !ok {
		return nil, false, nil
	}
	switch {
	case dt.Equals(pgtypes.Int64):
		newVal, _, err := types.Int64.Convert(ctx, val)
		if err != nil {
			return nil, true, err
		}
		if _, ok := newVal.(int64); !ok {
			return nil, true, errors.Errorf("GMSCast expected type `int64`, got `%T`", val)
		}
		return newVal, true, nil
	case dt.Equals(pgtypes.Int32):
		newVal, _, err := types.Int32.Convert(ctx, val)
		if err != nil {
			return nil, true, err
		}
		if _, ok := newVal.(int32); !ok {
			return nil, true, errors.Errorf("GMSCast expected type `int32`, got `%T`", val)
		}
		return newVal, true, nil
	case dt.Equals(pgtypes.Float32):
		newVal, _, err := types.Float32.Convert(ctx, val)
		if err != nil {
			return nil, true, err
		}
		if _, ok := newVal.(float32); !ok {
			return nil, true, errors.Errorf("GMSCast expected type `float32`, got `%T`", val)
		}
		return newVal, true, nil
	case dt.Equals(pgtypes.Float64):
		newVal, _, err := types.Float64.Convert(ctx, val)
		if err != nil {
			return nil, true, err
		}
		if _, ok := newVal.(float64); !ok {
			return nil, true, errors.Errorf("GMSCast expected type `float64`, got `%T`", val)
		}
		return newVal, true, nil
	case dt.Equals(pgtypes.Numeric):
		newVal, _, err := types.InternalDecimalType.Convert(ctx, val)
		if err != nil {
			return nil, true, err
		}
		if _, ok := newVal.(decimal.Decimal); !ok {
			return nil, true, errors.Errorf("GMSCast expected type `decimal.Decimal`, got `%T`", val)
		}
		return newVal, true, nil
	default:
		return nil, false, nil
	}
}

func (c *WindowGMSCast) Id() sql.ColumnId {
	if idExpr, ok := c.sqlChild.(sql.IdExpression); ok {
		return idExpr.Id()
	}
	return 0
}

func (c *WindowGMSCast) WithId(id sql.ColumnId) sql.IdExpression {
	idExpr, ok := c.sqlChild.(sql.IdExpression)
	if !ok {
		return c
	}
	child, ok := idExpr.WithId(id).(sql.WindowAdaptableExpression)
	if !ok {
		return c
	}
	return NewWindowGMSCast(child)
}

func (c *WindowGMSCast) Window() *sql.WindowDefinition {
	if child, ok := c.sqlChild.(sql.WindowAdaptableExpression); ok {
		return child.Window()
	}
	return nil
}

func (c *WindowGMSCast) WithWindow(ctx *sql.Context, window *sql.WindowDefinition) sql.WindowAdaptableExpression {
	child, ok := c.sqlChild.(sql.WindowAdaptableExpression)
	if !ok {
		return c
	}
	return NewWindowGMSCast(child.WithWindow(ctx, window))
}

func (c *WindowGMSCast) NewWindowFunction(ctx *sql.Context) (sql.WindowFunction, error) {
	child, ok := c.sqlChild.(sql.WindowAdaptableExpression)
	if !ok {
		return nil, errors.Errorf("GMSCast expected window-compatible child, got `%T`", c.sqlChild)
	}
	fn, err := child.NewWindowFunction(ctx)
	if err != nil {
		return nil, err
	}
	return &gmsCastWindowFunction{
		child: c.sqlChild,
		inner: fn,
	}, nil
}

func (c *WindowGMSCast) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	child, ok := children[0].(sql.WindowAdaptableExpression)
	if !ok {
		return nil, errors.Errorf("GMSCast expected window-compatible child, got `%T`", children[0])
	}
	return NewWindowGMSCast(child), nil
}

func (c *AggregationGMSCast) NewBuffer(ctx *sql.Context) (sql.AggregationBuffer, error) {
	aggregation, ok := c.sqlChild.(sql.Aggregation)
	if !ok {
		return nil, errors.Errorf("GMSCast expected aggregation-compatible child, got `%T`", c.sqlChild)
	}
	buffer, err := aggregation.NewBuffer(ctx)
	if err != nil {
		return nil, err
	}
	return &gmsCastAggregationBuffer{
		child: c.sqlChild,
		inner: buffer,
	}, nil
}

func (c *AggregationGMSCast) NewWindowFunction(ctx *sql.Context) (sql.WindowFunction, error) {
	child, ok := c.sqlChild.(sql.WindowAdaptableExpression)
	if !ok {
		return nil, errors.Errorf("GMSCast expected window-compatible child, got `%T`", c.sqlChild)
	}
	fn, err := child.NewWindowFunction(ctx)
	if err != nil {
		return nil, err
	}
	return &gmsCastWindowFunction{
		child: c.sqlChild,
		inner: fn,
	}, nil
}

func (c *AggregationGMSCast) WithWindow(ctx *sql.Context, window *sql.WindowDefinition) sql.WindowAdaptableExpression {
	child, ok := c.sqlChild.(sql.Aggregation)
	if !ok {
		return c
	}
	windowChild := child.WithWindow(ctx, window)
	aggregationChild, ok := windowChild.(sql.Aggregation)
	if !ok {
		return c
	}
	return NewAggregationGMSCast(aggregationChild)
}

func (c *AggregationGMSCast) Window() *sql.WindowDefinition {
	if child, ok := c.sqlChild.(sql.WindowAdaptableExpression); ok {
		return child.Window()
	}
	return nil
}

func (c *AggregationGMSCast) Id() sql.ColumnId {
	if idExpr, ok := c.sqlChild.(sql.IdExpression); ok {
		return idExpr.Id()
	}
	return 0
}

func (c *AggregationGMSCast) WithId(id sql.ColumnId) sql.IdExpression {
	idExpr, ok := c.sqlChild.(sql.IdExpression)
	if !ok {
		return c
	}
	child, ok := idExpr.WithId(id).(sql.Aggregation)
	if !ok {
		return c
	}
	return NewAggregationGMSCast(child)
}

func (c *AggregationGMSCast) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	child, ok := children[0].(sql.Aggregation)
	if !ok {
		return nil, errors.Errorf("GMSCast expected aggregation-compatible child, got `%T`", children[0])
	}
	return NewAggregationGMSCast(child), nil
}

type gmsCastAggregationBuffer struct {
	child sql.Expression
	inner sql.AggregationBuffer
}

func (b *gmsCastAggregationBuffer) Dispose(ctx *sql.Context) {
	b.inner.Dispose(ctx)
}

func (b *gmsCastAggregationBuffer) Update(ctx *sql.Context, row sql.Row) error {
	return b.inner.Update(ctx, row)
}

func (b *gmsCastAggregationBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	val, err := b.inner.Eval(ctx)
	if err != nil {
		return nil, err
	}
	return castGMSExpressionValue(ctx, val, b.child)
}

type gmsCastWindowFunction struct {
	child sql.Expression
	inner sql.WindowFunction
}

func (f *gmsCastWindowFunction) Dispose(ctx *sql.Context) {
	f.inner.Dispose(ctx)
}

func (f *gmsCastWindowFunction) StartPartition(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) error {
	return f.inner.StartPartition(ctx, interval, buffer)
}

func (f *gmsCastWindowFunction) DefaultFramer() sql.WindowFramer {
	return f.inner.DefaultFramer()
}

func (f *gmsCastWindowFunction) Compute(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) (interface{}, error) {
	if avgChild, ok := avgWindowChild(f.child); ok {
		hasNonNull, err := windowIntervalHasNonNull(ctx, avgChild, interval, buffer)
		if err != nil {
			return nil, err
		}
		if !hasNonNull {
			return nil, nil
		}
	}
	val, err := f.inner.Compute(ctx, interval, buffer)
	if err != nil {
		return nil, err
	}
	return castGMSExpressionValue(ctx, val, f.child)
}

func avgWindowChild(expr sql.Expression) (sql.Expression, bool) {
	fn, ok := expr.(sql.FunctionExpression)
	if !ok || !strings.EqualFold(fn.FunctionName(), "avg") {
		return nil, false
	}
	children := expr.Children()
	if len(children) == 0 {
		return nil, false
	}
	return children[0], true
}

func windowIntervalHasNonNull(ctx *sql.Context, expr sql.Expression, interval sql.WindowInterval, buffer sql.WindowBuffer) (bool, error) {
	for i := interval.Start; i < interval.End; i++ {
		val, err := expr.Eval(ctx, buffer[i])
		if err != nil {
			return false, err
		}
		if val != nil {
			return true, nil
		}
	}
	return false, nil
}

// IsNullable implements the sql.Expression interface.
func (c *GMSCast) IsNullable(ctx *sql.Context) bool {
	return true
}

// Resolved implements the sql.Expression interface.
func (c *GMSCast) Resolved() bool {
	return c.sqlChild.Resolved()
}

// String implements the sql.Expression interface.
func (c *GMSCast) String() string {
	if gf, ok := c.sqlChild.(*expression.GetField); ok {
		return gf.Name()
	}
	return c.sqlChild.String()
}

// Type implements the sql.Expression interface.
func (c *GMSCast) Type(ctx *sql.Context) sql.Type {
	return c.DoltgresType(ctx)
}

// WithChildren implements the sql.Expression interface.
func (c *GMSCast) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	return &GMSCast{
		sqlChild: children[0],
	}, nil
}
