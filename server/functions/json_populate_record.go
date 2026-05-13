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

package functions

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

var _ sql.TableFunction = (*jsonbPopulateRecordTableFunction)(nil)
var _ sql.ExecSourceRel = (*jsonbPopulateRecordTableFunction)(nil)
var _ sql.TableFunction = (*jsonToRecordTableFunction)(nil)
var _ sql.ExecSourceRel = (*jsonToRecordTableFunction)(nil)

// jsonb_populate_record represents the PostgreSQL function jsonb_populate_record(anyelement, jsonb).
var jsonb_populate_record = framework.Function2{
	Name:       "jsonb_populate_record",
	Return:     pgtypes.AnyElement,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.AnyElement, pgtypes.JsonB},
	Strict:     false,
	Callable: func(ctx *sql.Context, t [3]*pgtypes.DoltgresType, base any, fromJson any) (any, error) {
		return jsonbPopulateRecord(ctx, t[2], base, fromJson)
	},
}

type jsonbPopulateRecordTableFunction struct {
	db    sql.Database
	exprs []sql.Expression
}

type jsonRecordColumn struct {
	name string
	typ  *pgtypes.DoltgresType
}

type jsonToRecordTableFunction struct {
	db        sql.Database
	name      string
	inputType *pgtypes.DoltgresType
	recordSet bool
	exprs     []sql.Expression
	columns   []jsonRecordColumn
}

func newJsonToRecordTableFunction(name string, inputType *pgtypes.DoltgresType, recordSet bool) *jsonToRecordTableFunction {
	return &jsonToRecordTableFunction{
		name:      name,
		inputType: inputType,
		recordSet: recordSet,
	}
}

// NewInstance creates a new instance of the JSONB populate record table function.
func (j *jsonbPopulateRecordTableFunction) NewInstance(ctx *sql.Context, db sql.Database, args []sql.Expression) (sql.Node, error) {
	if len(args) != 2 {
		return nil, sql.ErrInvalidArgumentNumber.New(j.Name(), 2, len(args))
	}
	nt := *j
	nt.db = db
	nt.exprs = args
	return &nt, nil
}

// Name implements the sql.Nameable interface.
func (j *jsonbPopulateRecordTableFunction) Name() string {
	return "jsonb_populate_record"
}

// String implements fmt.Stringer.
func (j *jsonbPopulateRecordTableFunction) String() string {
	args := make([]string, len(j.exprs))
	for i, expr := range j.exprs {
		args[i] = expr.String()
	}
	return fmt.Sprintf("%s(%s)", j.Name(), strings.Join(args, ", "))
}

// Resolved implements the sql.Resolvable interface.
func (j *jsonbPopulateRecordTableFunction) Resolved() bool {
	for _, expr := range j.exprs {
		if !expr.Resolved() {
			return false
		}
	}
	return true
}

// Expressions implements sql.Expressioner.
func (j *jsonbPopulateRecordTableFunction) Expressions() []sql.Expression {
	return j.exprs
}

// WithExpressions implements sql.Expressioner.
func (j *jsonbPopulateRecordTableFunction) WithExpressions(ctx *sql.Context, exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(j, len(exprs), 2)
	}
	nt := *j
	nt.exprs = exprs
	return &nt, nil
}

// Database implements sql.Databaser.
func (j *jsonbPopulateRecordTableFunction) Database() sql.Database {
	return j.db
}

// WithDatabase implements sql.Databaser.
func (j *jsonbPopulateRecordTableFunction) WithDatabase(db sql.Database) (sql.Node, error) {
	nt := *j
	nt.db = db
	return &nt, nil
}

// IsReadOnly implements sql.Node.
func (j *jsonbPopulateRecordTableFunction) IsReadOnly() bool {
	return true
}

// Schema implements sql.Node.
func (j *jsonbPopulateRecordTableFunction) Schema(ctx *sql.Context) sql.Schema {
	compositeType, err := jsonbPopulateRecordCompositeType(ctx, j.exprs[0])
	if err != nil {
		return nil
	}
	var dbName string
	if j.db != nil {
		dbName = j.db.Name()
	}
	schema := make(sql.Schema, len(compositeType.CompositeAttrs))
	for i, attr := range compositeType.CompositeAttrs {
		attrType, err := jsonPopulateLookupType(ctx, attr.TypeID)
		if err != nil || attrType == nil {
			return nil
		}
		attrType = attr.ApplyTypMod(attrType)
		schema[i] = &sql.Column{
			DatabaseSource: dbName,
			Source:         j.Name(),
			Name:           attr.Name,
			Type:           attrType,
			Nullable:       true,
		}
	}
	return schema
}

// Children implements sql.Node.
func (j *jsonbPopulateRecordTableFunction) Children() []sql.Node {
	return nil
}

// WithChildren implements sql.Node.
func (j *jsonbPopulateRecordTableFunction) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(j, len(children), 0)
	}
	return j, nil
}

// RowIter implements sql.Node.
func (j *jsonbPopulateRecordTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	compositeType, err := jsonbPopulateRecordCompositeType(ctx, j.exprs[0])
	if err != nil {
		return nil, err
	}
	base, err := j.exprs[0].Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	fromJson, err := j.exprs[1].Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	record, err := jsonbPopulateRecord(ctx, compositeType, base, fromJson)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return sql.RowsToRowIter(make(sql.Row, len(compositeType.CompositeAttrs))), nil
	}
	output := make(sql.Row, len(record))
	for i, value := range record {
		output[i] = value.Value
	}
	return sql.RowsToRowIter(output), nil
}

// CollationCoercibility implements sql.CollationCoercible.
func (j *jsonbPopulateRecordTableFunction) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// Collation implements sql.Table.
func (j *jsonbPopulateRecordTableFunction) Collation() sql.CollationID {
	return sql.Collation_Default
}

// NewInstance creates a new instance of the JSON to record table function.
func (j *jsonToRecordTableFunction) NewInstance(ctx *sql.Context, db sql.Database, args []sql.Expression) (sql.Node, error) {
	if len(args) < 5 || (len(args)-1)%4 != 0 {
		return nil, sql.ErrInvalidArgumentNumber.New(j.Name(), "1 + 4n", len(args))
	}
	columns, err := jsonRecordColumnsFromArgs(ctx, args[1:])
	if err != nil {
		return nil, err
	}
	nt := *j
	nt.db = db
	nt.exprs = args
	nt.columns = columns
	return &nt, nil
}

// Name implements the sql.Nameable interface.
func (j *jsonToRecordTableFunction) Name() string {
	return j.name
}

// String implements fmt.Stringer.
func (j *jsonToRecordTableFunction) String() string {
	args := make([]string, len(j.exprs))
	for i, expr := range j.exprs {
		args[i] = expr.String()
	}
	return fmt.Sprintf("%s(%s)", j.Name(), strings.Join(args, ", "))
}

// Resolved implements the sql.Resolvable interface.
func (j *jsonToRecordTableFunction) Resolved() bool {
	for _, expr := range j.exprs {
		if !expr.Resolved() {
			return false
		}
	}
	return true
}

// Expressions implements sql.Expressioner.
func (j *jsonToRecordTableFunction) Expressions() []sql.Expression {
	return j.exprs
}

// WithExpressions implements sql.Expressioner.
func (j *jsonToRecordTableFunction) WithExpressions(ctx *sql.Context, exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) < 5 || (len(exprs)-1)%4 != 0 {
		return nil, errors.Errorf("%s expected 1 + 4n expressions, got %d", j.Name(), len(exprs))
	}
	columns, err := jsonRecordColumnsFromArgs(ctx, exprs[1:])
	if err != nil {
		return nil, err
	}
	nt := *j
	nt.exprs = exprs
	nt.columns = columns
	return &nt, nil
}

// Database implements sql.Databaser.
func (j *jsonToRecordTableFunction) Database() sql.Database {
	return j.db
}

// WithDatabase implements sql.Databaser.
func (j *jsonToRecordTableFunction) WithDatabase(db sql.Database) (sql.Node, error) {
	nt := *j
	nt.db = db
	return &nt, nil
}

// IsReadOnly implements sql.Node.
func (j *jsonToRecordTableFunction) IsReadOnly() bool {
	return true
}

// Schema implements sql.Node.
func (j *jsonToRecordTableFunction) Schema(ctx *sql.Context) sql.Schema {
	var dbName string
	if j.db != nil {
		dbName = j.db.Name()
	}
	schema := make(sql.Schema, len(j.columns))
	for i, col := range j.columns {
		schema[i] = &sql.Column{
			DatabaseSource: dbName,
			Source:         j.Name(),
			Name:           col.name,
			Type:           col.typ,
			Nullable:       true,
		}
	}
	return schema
}

// Children implements sql.Node.
func (j *jsonToRecordTableFunction) Children() []sql.Node {
	return nil
}

// WithChildren implements sql.Node.
func (j *jsonToRecordTableFunction) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(j, len(children), 0)
	}
	return j, nil
}

// RowIter implements sql.Node.
func (j *jsonToRecordTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	value, err := j.exprs[0].Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if value == nil {
		if j.recordSet {
			return sql.RowsToRowIter(), nil
		}
		return sql.RowsToRowIter(make(sql.Row, len(j.columns))), nil
	}
	doc, err := jsonDocumentFromFunctionValue(ctx, j.inputType, value)
	if err != nil {
		return nil, err
	}
	if j.recordSet {
		array, ok := pgtypes.JsonValueUnwrapRaw(doc.Value).(pgtypes.JsonValueArray)
		if !ok {
			return nil, errors.Errorf("cannot call %s on a non-array", j.Name())
		}
		rows := make([]sql.Row, len(array))
		for i, item := range array {
			object, ok := pgtypes.JsonValueUnwrapRaw(item).(pgtypes.JsonValueObject)
			if !ok {
				return nil, errors.Errorf("cannot call %s on an array containing a non-object", j.Name())
			}
			rows[i], err = j.rowFromObject(ctx, object)
			if err != nil {
				return nil, err
			}
		}
		return sql.RowsToRowIter(rows...), nil
	}
	object, ok := pgtypes.JsonValueUnwrapRaw(doc.Value).(pgtypes.JsonValueObject)
	if !ok {
		return nil, errors.Errorf("cannot call %s on a non-object", j.Name())
	}
	output, err := j.rowFromObject(ctx, object)
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(output), nil
}

func (j *jsonToRecordTableFunction) rowFromObject(ctx *sql.Context, object pgtypes.JsonValueObject) (sql.Row, error) {
	output := make(sql.Row, len(j.columns))
	for i, col := range j.columns {
		if objectIdx, ok := object.Index[col.name]; ok {
			value, err := jsonPopulateValue(ctx, col.typ, nil, object.Items[objectIdx].Value)
			if err != nil {
				return nil, err
			}
			output[i] = value
		}
	}
	return output, nil
}

// CollationCoercibility implements sql.CollationCoercible.
func (j *jsonToRecordTableFunction) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// Collation implements sql.Table.
func (j *jsonToRecordTableFunction) Collation() sql.CollationID {
	return sql.Collation_Default
}

func jsonRecordColumnsFromArgs(ctx *sql.Context, args []sql.Expression) ([]jsonRecordColumn, error) {
	if len(args)%4 != 0 {
		return nil, errors.Errorf("record column metadata must be name, schema, type, and typmod tuples")
	}
	columns := make([]jsonRecordColumn, len(args)/4)
	for i := range columns {
		name, err := jsonRecordStringArg(ctx, args[i*4])
		if err != nil {
			return nil, err
		}
		schemaName, err := jsonRecordStringArg(ctx, args[i*4+1])
		if err != nil {
			return nil, err
		}
		typeName, err := jsonRecordStringArg(ctx, args[i*4+2])
		if err != nil {
			return nil, err
		}
		typmodText, err := jsonRecordStringArg(ctx, args[i*4+3])
		if err != nil {
			return nil, err
		}
		typmod, err := strconv.ParseInt(typmodText, 10, 32)
		if err != nil {
			return nil, err
		}
		colType := pgtypes.NewUnresolvedDoltgresType(schemaName, typeName)
		if typmod != -1 {
			colType = colType.WithAttTypMod(int32(typmod))
		}
		colType, err = jsonPopulateResolveType(ctx, colType)
		if err != nil {
			return nil, err
		}
		columns[i] = jsonRecordColumn{
			name: name,
			typ:  colType,
		}
	}
	return columns, nil
}

func jsonRecordStringArg(ctx *sql.Context, expr sql.Expression) (string, error) {
	value, err := expr.Eval(ctx, nil)
	if err != nil {
		return "", err
	}
	str, ok := value.(string)
	if !ok {
		return "", errors.Errorf("expected record column metadata to be a string, but got %T", value)
	}
	return str, nil
}

func jsonbPopulateRecordCompositeType(ctx *sql.Context, expr sql.Expression) (*pgtypes.DoltgresType, error) {
	compositeType, ok := expr.Type(ctx).(*pgtypes.DoltgresType)
	if !ok {
		return nil, errors.Errorf("first argument of jsonb_populate_record must be a composite type")
	}
	var err error
	compositeType, err = jsonPopulateResolveType(ctx, compositeType)
	if err != nil {
		return nil, err
	}
	if !compositeType.IsCompositeType() || len(compositeType.CompositeAttrs) == 0 {
		return nil, errors.Errorf("first argument of jsonb_populate_record must be a composite type")
	}
	return compositeType, nil
}

func jsonbPopulateRecord(ctx *sql.Context, compositeType *pgtypes.DoltgresType, base any, fromJson any) ([]pgtypes.RecordValue, error) {
	var err error
	compositeType, err = jsonPopulateResolveType(ctx, compositeType)
	if err != nil {
		return nil, err
	}
	if compositeType == nil || !compositeType.IsCompositeType() || len(compositeType.CompositeAttrs) == 0 {
		return nil, errors.Errorf("first argument of jsonb_populate_record must be a composite type")
	}
	resolvedJson, err := sql.UnwrapAny(ctx, fromJson)
	if err != nil {
		return nil, err
	}
	if resolvedJson == nil {
		return nil, nil
	}
	doc, err := jsonDocumentFromFunctionValue(ctx, pgtypes.JsonB, resolvedJson)
	if err != nil {
		return nil, err
	}
	object, ok := pgtypes.JsonValueUnwrapRaw(doc.Value).(pgtypes.JsonValueObject)
	if !ok {
		return nil, errors.New("cannot call jsonb_populate_record on a non-object")
	}
	baseRecord, err := jsonbPopulateRecordBase(ctx, base)
	if err != nil {
		return nil, err
	}
	return jsonPopulateRecordFromObject(ctx, compositeType, baseRecord, object)
}

func jsonbPopulateRecordBase(ctx *sql.Context, base any) ([]pgtypes.RecordValue, error) {
	resolvedBase, err := sql.UnwrapAny(ctx, base)
	if err != nil {
		return nil, err
	}
	if resolvedBase == nil {
		return nil, nil
	}
	record, ok := resolvedBase.([]pgtypes.RecordValue)
	if !ok {
		return nil, errors.Errorf("expected []RecordValue, but got %T", resolvedBase)
	}
	return record, nil
}

func jsonPopulateRecordFromObject(
	ctx *sql.Context,
	compositeType *pgtypes.DoltgresType,
	baseRecord []pgtypes.RecordValue,
	object pgtypes.JsonValueObject,
) ([]pgtypes.RecordValue, error) {
	record := make([]pgtypes.RecordValue, len(compositeType.CompositeAttrs))
	for i, attr := range compositeType.CompositeAttrs {
		attrType, err := jsonPopulateLookupType(ctx, attr.TypeID)
		if err != nil {
			return nil, err
		}
		if attrType == nil {
			return nil, pgtypes.ErrTypeDoesNotExist.New(attr.TypeID.TypeName())
		}
		attrType = attr.ApplyTypMod(attrType)
		var baseValue any
		if i < len(baseRecord) {
			baseValue = baseRecord[i].Value
		}
		record[i] = pgtypes.RecordValue{
			Value: baseValue,
			Type:  attrType,
		}
		if objectIdx, ok := object.Index[attr.Name]; ok {
			record[i].Value, err = jsonPopulateValue(ctx, attrType, baseValue, object.Items[objectIdx].Value)
			if err != nil {
				return nil, err
			}
		}
	}
	return record, nil
}

func jsonPopulateValue(ctx *sql.Context, targetType *pgtypes.DoltgresType, baseValue any, value pgtypes.JsonValue) (any, error) {
	unwrapped := pgtypes.JsonValueUnwrapRaw(value)
	if _, ok := unwrapped.(pgtypes.JsonValueNull); ok {
		return nil, nil
	}
	switch targetType.ID.TypeName() {
	case "jsonb":
		return pgtypes.JsonDocument{Value: pgtypes.JsonValueCopy(value)}, nil
	case "json":
		return jsonValueOutput(ctx, pgtypes.JsonValueCopy(value))
	}
	if targetType.IsCompositeType() {
		if object, ok := unwrapped.(pgtypes.JsonValueObject); ok {
			baseRecord, ok := baseValue.([]pgtypes.RecordValue)
			if baseValue != nil && !ok {
				return nil, errors.Errorf("expected []RecordValue, but got %T", baseValue)
			}
			return jsonPopulateRecordFromObject(ctx, targetType, baseRecord, object)
		}
	}
	if targetType.IsArrayType() {
		if array, ok := unwrapped.(pgtypes.JsonValueArray); ok {
			baseType := targetType.ArrayBaseType()
			output := make([]any, len(array))
			for i, item := range array {
				converted, err := jsonPopulateValue(ctx, baseType, nil, item)
				if err != nil {
					return nil, err
				}
				output[i] = converted
			}
			return output, nil
		}
	}
	var input string
	if str, ok := unwrapped.(pgtypes.JsonValueString); ok {
		decoded, err := pgtypes.JsonStringUnescape(str)
		if err != nil {
			return nil, err
		}
		input = decoded
	} else {
		var sb strings.Builder
		pgtypes.JsonValueFormatter(&sb, value)
		input = sb.String()
	}
	return targetType.IoInput(ctx, input)
}

func jsonPopulateResolveType(ctx *sql.Context, typ *pgtypes.DoltgresType) (*pgtypes.DoltgresType, error) {
	if typ == nil || typ.IsResolvedType() {
		return typ, nil
	}
	resolved, err := jsonPopulateLookupType(ctx, typ.ID)
	if err != nil {
		return nil, err
	}
	if typ.GetAttTypMod() != -1 {
		return resolved.WithAttTypMod(typ.GetAttTypMod()), nil
	}
	return resolved, nil
}

func jsonPopulateLookupType(ctx *sql.Context, typeID id.Type) (*pgtypes.DoltgresType, error) {
	typeCollection, err := core.GetTypesCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	resolved, err := typeCollection.GetType(ctx, typeID)
	if err != nil {
		return nil, err
	}
	if resolved != nil {
		return resolved, nil
	}
	if typeID.SchemaName() != "" {
		return nil, pgtypes.ErrTypeDoesNotExist.New(typeID.TypeName())
	}
	schema, err := core.GetSchemaName(ctx, nil, "")
	if err != nil {
		return nil, err
	}
	resolved, err = typeCollection.GetType(ctx, id.NewType(schema, typeID.TypeName()))
	if err != nil {
		return nil, err
	}
	if resolved != nil {
		return resolved, nil
	}
	resolved, err = typeCollection.GetType(ctx, id.NewType("pg_catalog", typeID.TypeName()))
	if err != nil {
		return nil, err
	}
	if resolved == nil {
		return nil, pgtypes.ErrTypeDoesNotExist.New(typeID.TypeName())
	}
	return resolved, nil
}
