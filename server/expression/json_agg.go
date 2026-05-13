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
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/goccy/go-json"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

type JsonAgg struct {
	name        string
	selectExprs []sql.Expression
	orderBy     sql.SortFields
	id          sql.ColumnId
	object      bool
	jsonb       bool
	distinct    bool
}

var _ sql.Aggregation = (*JsonAgg)(nil)
var _ vitess.Injectable = (*JsonAgg)(nil)
var _ sql.OrderedAggregation = (*JsonAgg)(nil)

func NewJsonAgg(name string, object bool, jsonb bool, distinct bool) *JsonAgg {
	return &JsonAgg{
		name:     name,
		object:   object,
		jsonb:    jsonb,
		distinct: distinct,
	}
}

// WithResolvedChildren returns a new JsonAgg with the provided select expressions and sort fields.
func (j *JsonAgg) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	j.selectExprs = make([]sql.Expression, len(children)-1)
	for i := 0; i < len(children)-1; i++ {
		j.selectExprs[i] = children[i].(sql.Expression)
	}
	j.orderBy = children[len(children)-1].(sql.SortFields)
	return j, nil
}

// Resolved implements sql.Expression.
func (j *JsonAgg) Resolved() bool {
	return expression.ExpressionsResolved(j.selectExprs...) && expression.ExpressionsResolved(j.orderBy.ToExpressions()...)
}

// String implements sql.Expression.
func (j *JsonAgg) String() string {
	sb := strings.Builder{}
	sb.WriteString(j.name)
	sb.WriteRune('(')
	if j.distinct {
		sb.WriteString("DISTINCT ")
	}
	if j.selectExprs != nil {
		exprs := make([]string, len(j.selectExprs))
		for i, expr := range j.selectExprs {
			exprs[i] = expr.String()
		}
		sb.WriteString(strings.Join(exprs, ", "))
	}
	if len(j.orderBy) > 0 {
		sb.WriteString(" order by ")
		for i, ob := range j.orderBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(ob.String())
		}
	}
	sb.WriteRune(')')
	return sb.String()
}

// Type implements sql.Expression.
func (j *JsonAgg) Type(ctx *sql.Context) sql.Type {
	if j.jsonb {
		return pgtypes.JsonB
	}
	return pgtypes.Json
}

// IsNullable implements sql.Expression.
func (j *JsonAgg) IsNullable(ctx *sql.Context) bool {
	return true
}

// Eval implements sql.Expression.
func (j *JsonAgg) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	panic("eval should never be called on an aggregation function")
}

// Children implements sql.Expression.
func (j *JsonAgg) Children() []sql.Expression {
	return append(j.selectExprs, j.orderBy.ToExpressions()...)
}

func (j *JsonAgg) OutputExpressions() []sql.Expression {
	return j.selectExprs
}

// WithChildren implements sql.Expression.
func (j JsonAgg) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != len(j.selectExprs)+len(j.orderBy) {
		return nil, sql.ErrInvalidChildrenNumber.New(j, len(children), len(j.selectExprs)+len(j.orderBy))
	}
	j.selectExprs = children[:len(j.selectExprs)]
	j.orderBy = j.orderBy.FromExpressions(ctx, children[len(j.selectExprs):]...)
	return &j, nil
}

// Id implements sql.IdExpression.
func (j *JsonAgg) Id() sql.ColumnId {
	return j.id
}

// WithId implements sql.IdExpression.
func (j JsonAgg) WithId(id sql.ColumnId) sql.IdExpression {
	j.id = id
	return &j
}

// NewWindowFunction implements sql.WindowAdaptableExpression.
func (j *JsonAgg) NewWindowFunction(ctx *sql.Context) (sql.WindowFunction, error) {
	panic("window functions not yet supported for JSON aggregates")
}

// WithWindow implements sql.WindowAdaptableExpression.
func (j *JsonAgg) WithWindow(ctx *sql.Context, window *sql.WindowDefinition) sql.WindowAdaptableExpression {
	panic("window functions not yet supported for JSON aggregates")
}

// Window implements sql.WindowAdaptableExpression.
func (j *JsonAgg) Window() *sql.WindowDefinition {
	return nil
}

// NewBuffer implements sql.Aggregation.
func (j *JsonAgg) NewBuffer(ctx *sql.Context) (sql.AggregationBuffer, error) {
	return &jsonAggBuffer{
		elements: make([]sql.Row, 0),
		seen:     make(map[string]struct{}),
		j:        j,
	}, nil
}

type jsonAggBuffer struct {
	elements []sql.Row
	seen     map[string]struct{}
	j        *JsonAgg
}

// Dispose implements sql.AggregationBuffer.
func (j *jsonAggBuffer) Dispose(ctx *sql.Context) {}

// Eval implements sql.AggregationBuffer.
func (j *jsonAggBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	if len(j.elements) == 0 {
		return nil, nil
	}
	if j.j.orderBy != nil {
		sorter := &expression.Sorter{
			SortFields: j.j.orderBy,
			Rows:       j.elements,
			Ctx:        ctx,
		}
		sort.Stable(sorter)
		if sorter.LastError != nil {
			return nil, sorter.LastError
		}
	}
	if j.j.object {
		return j.evalObject(ctx)
	}
	return j.evalArray(ctx)
}

func (j *jsonAggBuffer) evalArray(ctx *sql.Context) (interface{}, error) {
	valueType := jsonAggDoltgresType(ctx, j.j.selectExprs[0])
	array := make(pgtypes.JsonValueArray, len(j.elements))
	for i, row := range j.elements {
		value, err := pgtypes.JsonValueFromSQLValue(ctx, valueType, row[len(row)-1])
		if err != nil {
			return nil, err
		}
		array[i] = value
	}
	return jsonAggOutput(array, j.j.jsonb), nil
}

func (j *jsonAggBuffer) evalObject(ctx *sql.Context) (interface{}, error) {
	keyType := jsonAggDoltgresType(ctx, j.j.selectExprs[0])
	valueType := jsonAggDoltgresType(ctx, j.j.selectExprs[1])
	items := make([]pgtypes.JsonValueObjectItem, 0, len(j.elements))
	for _, row := range j.elements {
		key, err := jsonAggObjectKey(ctx, keyType, row[len(row)-2])
		if err != nil {
			return nil, err
		}
		value, err := pgtypes.JsonValueFromSQLValue(ctx, valueType, row[len(row)-1])
		if err != nil {
			return nil, err
		}
		items = append(items, pgtypes.JsonValueObjectItem{Key: key, Value: value})
	}
	return jsonAggOutput(pgtypes.JsonObjectFromItems(items, j.j.jsonb), j.j.jsonb), nil
}

// Update implements sql.AggregationBuffer.
func (j *jsonAggBuffer) Update(ctx *sql.Context, row sql.Row) error {
	evalRow, err := evalExprs(ctx, j.j.selectExprs, row)
	if err != nil {
		return err
	}
	if j.j.distinct {
		key, err := jsonAggDistinctKeyForExpressions(ctx, evalRow, j.j.selectExprs)
		if err != nil {
			return err
		}
		if _, ok := j.seen[key]; ok {
			return nil
		}
		j.seen[key] = struct{}{}
	}
	storedRow := make(sql.Row, len(row)+len(evalRow))
	copy(storedRow, row)
	copy(storedRow[len(row):], evalRow)
	j.elements = append(j.elements, storedRow)
	return nil
}

func jsonAggDistinctKeyForExpressions(ctx *sql.Context, row sql.Row, exprs []sql.Expression) (string, error) {
	sb := strings.Builder{}
	for i, val := range row {
		if i > 0 {
			sb.WriteRune(0)
		}
		typ := jsonAggDoltgresType(ctx, exprs[i])
		key, err := jsonAggDistinctKeyPart(ctx, typ, val)
		if err != nil {
			return "", err
		}
		sb.WriteString(key)
	}
	return sb.String(), nil
}

func jsonAggDistinctKey(ctx *sql.Context, row sql.Row) (string, error) {
	sb := strings.Builder{}
	for i, val := range row {
		if i > 0 {
			sb.WriteRune(0)
		}
		key, err := jsonAggDistinctKeyPart(ctx, nil, val)
		if err != nil {
			return "", err
		}
		sb.WriteString(key)
	}
	return sb.String(), nil
}

func jsonAggDistinctKeyPart(ctx *sql.Context, typ *pgtypes.DoltgresType, val any) (string, error) {
	res, err := sql.UnwrapAny(ctx, val)
	if err != nil {
		return "", err
	}
	if typ != nil {
		switch typ.ID.TypeName() {
		case "json":
			return "", errors.New("could not identify an equality operator for type json")
		case "jsonb":
			value, err := pgtypes.JsonValueFromSQLValue(ctx, typ, res)
			if err != nil {
				return "", err
			}
			sb := strings.Builder{}
			pgtypes.JsonValueFormatter(&sb, value)
			return "jsonb:" + sb.String(), nil
		}
	}
	return fmt.Sprintf("%T:%#v", res, res), nil
}

func jsonAggDoltgresType(ctx *sql.Context, expr sql.Expression) *pgtypes.DoltgresType {
	if expr == nil {
		return nil
	}
	dt, _ := expr.Type(ctx).(*pgtypes.DoltgresType)
	return dt
}

const jsonAggObjectNonScalarKeyErr = "key value must be scalar, not array, composite, or json"

func jsonAggObjectKeyIsNonScalar(typ *pgtypes.DoltgresType, val any) bool {
	if _, ok := val.([]pgtypes.RecordValue); ok {
		return true
	}
	switch val.(type) {
	case []any, sql.Row, pgtypes.JsonDocument:
		return true
	}
	if typ == nil {
		return false
	}
	switch typ.ID.TypeName() {
	case "json", "jsonb":
		return true
	}
	return typ.IsArrayCategory() || typ.IsCompositeType()
}

func jsonAggObjectKey(ctx *sql.Context, typ *pgtypes.DoltgresType, val any) (string, error) {
	res, err := sql.UnwrapAny(ctx, val)
	if err != nil {
		return "", err
	}
	if res == nil {
		return "", errors.New("field name must not be null")
	}
	if jsonAggObjectKeyIsNonScalar(typ, res) {
		return "", errors.New(jsonAggObjectNonScalarKeyErr)
	}
	if str, ok := res.(string); ok {
		return str, nil
	}
	if typ != nil {
		return typ.IoOutput(ctx, res)
	}
	return fmt.Sprint(res), nil
}

func jsonAggOutput(value pgtypes.JsonValue, jsonb bool) interface{} {
	if jsonb {
		return pgtypes.JsonDocument{Value: value}
	}
	sb := strings.Builder{}
	jsonAggFormatPlain(&sb, value)
	return sb.String()
}

func jsonAggFormatPlain(sb *strings.Builder, value pgtypes.JsonValue) {
	if raw, ok := pgtypes.JsonValueRawText(value); ok {
		sb.WriteString(raw)
		return
	}
	switch value := pgtypes.JsonValueUnwrapRaw(value).(type) {
	case pgtypes.JsonValueArray:
		sb.WriteRune('[')
		for i, item := range value {
			if i > 0 {
				sb.WriteString(", ")
			}
			jsonAggFormatPlain(sb, item)
		}
		sb.WriteRune(']')
	case pgtypes.JsonValueObject:
		if len(value.Items) == 0 {
			sb.WriteString("{}")
			return
		}
		sb.WriteString("{ ")
		for i, item := range value.Items {
			if i > 0 {
				sb.WriteString(", ")
			}
			jsonAggWriteKey(sb, item.Key)
			sb.WriteString(" : ")
			jsonAggFormatPlain(sb, item.Value)
		}
		sb.WriteString(" }")
	default:
		pgtypes.JsonValueFormatterCompact(sb, value)
	}
}

func jsonAggWriteKey(sb *strings.Builder, value string) {
	bytes, _ := json.MarshalWithOption(value, json.DisableHTMLEscape())
	sb.Write(bytes)
}
