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

package functions

import (
	"fmt"
	"strings"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/goccy/go-json"

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
	"github.com/dolthub/doltgresql/utils"
)

// initJson registers the functions to the catalog.
func initJson() {
	framework.RegisterFunction(json_in)
	framework.RegisterFunction(json_out)
	framework.RegisterFunction(json_recv)
	framework.RegisterFunction(json_send)
	framework.RegisterFunction(json_build_array_empty)
	framework.RegisterFunction(json_build_array)
	framework.RegisterFunction(json_build_object_empty)
	framework.RegisterFunction(json_build_object)
	framework.RegisterFunction(json_object_text_array)
	framework.RegisterFunction(json_object_text_arrays)
	framework.RegisterFunction(to_json_anyelement)
	framework.RegisterFunction(json_array_length)
	framework.RegisterFunction(json_array_elements)
	framework.RegisterFunction(json_array_elements_text)
	framework.RegisterFunction(json_object_keys)
	framework.RegisterFunction(json_each)
	framework.RegisterFunction(json_each_text)
	framework.RegisterFunction(json_typeof)
	framework.RegisterFunction(json_strip_nulls)
	framework.RegisterFunction(json_strip_nulls_strip_in_arrays)
	framework.RegisterFunction(json_populate_record)
	framework.RegisterFunction(doltgres_json_predicate)
	framework.RegisterFunction(row_to_json_record)
	framework.RegisterFunction(row_to_json_record_pretty)
}

// json_in represents the PostgreSQL function of json type IO input.
var json_in = framework.Function1{
	Name:       "json_in",
	Return:     pgtypes.Json,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Cstring},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		input := val.(string)
		if json.Valid(unsafe.Slice(unsafe.StringData(input), len(input))) {
			return input, nil
		}
		return nil, pgtypes.ErrInvalidSyntaxForType.New("json", input[:10]+"...")
	},
}

// json_out represents the PostgreSQL function of json type IO output.
var json_out = framework.Function1{
	Name:       "json_out",
	Return:     pgtypes.Cstring,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Json},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		unwrapped, err := sql.UnwrapAny(ctx, val)
		if err != nil {
			return nil, err
		}
		if unwrapped == nil {
			return nil, nil
		}
		str, ok := unwrapped.(string)
		if !ok {
			return nil, errors.Errorf(`"json" output requires a string argument, got %T`, unwrapped)
		}
		return str, nil
	},
}

// json_recv represents the PostgreSQL function of json type IO receive.
var json_recv = framework.Function1{
	Name:       "json_recv",
	Return:     pgtypes.Json,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Internal},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		data := val.([]byte)
		if data == nil {
			return nil, nil
		}
		return string(data), nil
	},
}

// json_send represents the PostgreSQL function of json type IO send.
var json_send = framework.Function1{
	Name:       "json_send",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Json},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		if wrapper, ok := val.(sql.AnyWrapper); ok {
			var err error
			val, err = wrapper.UnwrapAny(ctx)
			if err != nil {
				return nil, err
			}
			if val == nil {
				return nil, nil
			}
		}
		writer := utils.NewWireWriter()
		writer.WriteString(val.(string))
		return writer.BufferData(), nil
	},
}

// json_build_array represents the PostgreSQL function json_build_array.
var json_build_array_empty = framework.Function0{
	Name:   "json_build_array",
	Return: pgtypes.Json,
	Callable: func(ctx *sql.Context) (any, error) {
		return "[]", nil
	},
}

// json_build_array represents the PostgreSQL function json_build_array.
var json_build_array = framework.Function1N{
	Name:       "json_build_array",
	Return:     pgtypes.Json,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Any},
	Callable: func(ctx *sql.Context, argTypes []*pgtypes.DoltgresType, val1 any, vals []any) (any, error) {
		value, err := buildJsonArrayValue(ctx, argTypes, append([]any{val1}, vals...))
		if err != nil {
			return nil, err
		}
		return jsonBuildValueOutput(ctx, value)
	},
}

// json_build_object represents the PostgreSQL function json_build_object.
var json_build_object_empty = framework.Function0{
	Name:   "json_build_object",
	Return: pgtypes.Json,
	Callable: func(ctx *sql.Context) (any, error) {
		return "{}", nil
	},
}

// json_build_object represents the PostgreSQL function json_build_object.
var json_build_object = framework.Function1N{
	Name:       "json_build_object",
	Return:     pgtypes.Json,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Any},
	Callable: func(ctx *sql.Context, argTypes []*pgtypes.DoltgresType, val1 any, vals []any) (any, error) {
		value, err := buildJsonObjectValue(ctx, "json_build_object", argTypes, append([]any{val1}, vals...), false)
		if err != nil {
			return nil, err
		}
		return jsonBuildValueOutput(ctx, value)
	},
}

// json_object_text_array represents the PostgreSQL function json_object(text[]).
var json_object_text_array = framework.Function1{
	Name:       "json_object",
	Return:     pgtypes.Json,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.TextArray},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		value, err := buildJsonObjectFromTextArray(ctx, val, false)
		if err != nil {
			return nil, err
		}
		return jsonValueOutput(ctx, value)
	},
}

// json_object_text_arrays represents the PostgreSQL function json_object(text[], text[]).
var json_object_text_arrays = framework.Function2{
	Name:       "json_object",
	Return:     pgtypes.Json,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.TextArray, pgtypes.TextArray},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		value, err := buildJsonObjectFromTextArrays(ctx, val1, val2, false)
		if err != nil {
			return nil, err
		}
		return jsonValueOutput(ctx, value)
	},
}

// to_json_anyelement represents the PostgreSQL function to_json(anyelement).
var to_json_anyelement = framework.Function1{
	Name:       "to_json",
	Return:     pgtypes.Json,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.AnyElement},
	Strict:     false,
	Callable: func(ctx *sql.Context, t [2]*pgtypes.DoltgresType, val any) (any, error) {
		value, err := jsonValueFromAnyElement(ctx, t[0], val, false)
		if err != nil {
			return nil, err
		}
		return jsonValueOutput(ctx, value)
	},
}

// json_array_length represents the PostgreSQL function json_array_length.
var json_array_length = framework.Function1{
	Name:       "json_array_length",
	Return:     pgtypes.Int32,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Json},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		doc, err := jsonDocumentFromFunctionValue(ctx, pgtypes.Json, val)
		if err != nil {
			return nil, err
		}
		array, err := jsonValueAsArrayForLength(doc.Value)
		if err != nil {
			return nil, err
		}
		return int32(len(array)), nil
	},
}

// json_array_elements represents the PostgreSQL function json_array_elements.
var json_array_elements = framework.Function1{
	Name:       "json_array_elements",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.Json),
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Json},
	Strict:     true,
	SRF:        true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		doc, err := jsonDocumentFromFunctionValue(ctx, pgtypes.Json, val)
		if err != nil {
			return nil, err
		}
		array, err := jsonValueAsArrayForElements(doc.Value)
		if err != nil {
			return nil, err
		}
		return jsonArrayElementsRowIter(array, jsonValueToOutput), nil
	},
}

// json_array_elements_text represents the PostgreSQL function json_array_elements_text.
var json_array_elements_text = framework.Function1{
	Name:       "json_array_elements_text",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.Text),
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Json},
	Strict:     true,
	SRF:        true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		doc, err := jsonDocumentFromFunctionValue(ctx, pgtypes.Json, val)
		if err != nil {
			return nil, err
		}
		array, err := jsonValueAsArrayForElements(doc.Value)
		if err != nil {
			return nil, err
		}
		return jsonArrayElementsRowIter(array, jsonValueAsText), nil
	},
}

// json_object_keys represents the PostgreSQL function json_object_keys.
var json_object_keys = framework.Function1{
	Name:       "json_object_keys",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.Text),
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Json},
	Strict:     true,
	SRF:        true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		doc, err := jsonDocumentFromFunctionValue(ctx, pgtypes.Json, val)
		if err != nil {
			return nil, err
		}
		object, err := jsonValueAsObjectForKeys("json_object_keys", doc.Value)
		if err != nil {
			return nil, err
		}
		return jsonObjectKeysRowIter(object), nil
	},
}

// json_each represents the PostgreSQL function json_each.
var json_each = framework.Function1{
	Name:       "json_each",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.Record),
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Json},
	Strict:     true,
	SRF:        true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		doc, err := jsonDocumentFromFunctionValue(ctx, pgtypes.Json, val)
		if err != nil {
			return nil, err
		}
		object, err := jsonValueAsObjectForKeys("json_each", doc.Value)
		if err != nil {
			return nil, err
		}
		return jsonEachRowIter(object, pgtypes.Json, jsonValueToOutput), nil
	},
}

// json_each_text represents the PostgreSQL function json_each_text.
var json_each_text = framework.Function1{
	Name:       "json_each_text",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.Record),
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Json},
	Strict:     true,
	SRF:        true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		doc, err := jsonDocumentFromFunctionValue(ctx, pgtypes.Json, val)
		if err != nil {
			return nil, err
		}
		object, err := jsonValueAsObjectForKeys("json_each_text", doc.Value)
		if err != nil {
			return nil, err
		}
		return jsonEachRowIter(object, pgtypes.Text, jsonValueAsText), nil
	},
}

// json_typeof represents the PostgreSQL function json_typeof.
var json_typeof = framework.Function1{
	Name:       "json_typeof",
	Return:     pgtypes.Text,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Json},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		doc, err := jsonDocumentFromFunctionValue(ctx, pgtypes.Json, val)
		if err != nil {
			return nil, err
		}
		return pgtypes.JsonValueTypeName(doc.Value), nil
	},
}

// json_strip_nulls represents the PostgreSQL function json_strip_nulls.
var json_strip_nulls = framework.Function1{
	Name:       "json_strip_nulls",
	Return:     pgtypes.Json,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Json},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		doc, err := jsonDocumentFromFunctionValue(ctx, pgtypes.Json, val)
		if err != nil {
			return nil, err
		}
		return jsonValueOutput(ctx, pgtypes.JsonValueStripNulls(doc.Value, false))
	},
}

// json_strip_nulls_strip_in_arrays represents the PostgreSQL function json_strip_nulls(json, boolean).
var json_strip_nulls_strip_in_arrays = framework.Function2{
	Name:       "json_strip_nulls",
	Return:     pgtypes.Json,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Json, pgtypes.Bool},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val any, stripInArrays any) (any, error) {
		doc, err := jsonDocumentFromFunctionValue(ctx, pgtypes.Json, val)
		if err != nil {
			return nil, err
		}
		return jsonValueOutput(ctx, pgtypes.JsonValueStripNulls(doc.Value, stripInArrays.(bool)))
	},
}

// row_to_json_record represents the PostgreSQL function row_to_json(record).
var row_to_json_record = framework.Function1{
	Name:       "row_to_json",
	Return:     pgtypes.Json,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.AnyElement},
	Strict:     true,
	Callable: func(ctx *sql.Context, t [2]*pgtypes.DoltgresType, val any) (any, error) {
		return rowToJson(ctx, t[0], val, false)
	},
}

// row_to_json_record_pretty represents the PostgreSQL function row_to_json(record, boolean).
var row_to_json_record_pretty = framework.Function2{
	Name:       "row_to_json",
	Return:     pgtypes.Json,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.AnyElement, pgtypes.Bool},
	Strict:     true,
	Callable: func(ctx *sql.Context, t [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		return rowToJson(ctx, t[0], val1, val2.(bool))
	},
}

func rowToJson(ctx *sql.Context, typ *pgtypes.DoltgresType, val any, pretty bool) (string, error) {
	res, err := sql.UnwrapAny(ctx, val)
	if err != nil {
		return "", err
	}
	record, ok := res.([]pgtypes.RecordValue)
	if !ok {
		return "", errors.Errorf("expected []RecordValue, but got %T", res)
	}
	value, err := jsonValueFromRecord(ctx, typ, record, false)
	if err != nil {
		return "", err
	}
	if pretty {
		return jsonValueOutputPretty(ctx, value)
	}
	return jsonValueOutput(ctx, value)
}

func buildJsonArrayValue(ctx *sql.Context, argTypes []*pgtypes.DoltgresType, values []any) (pgtypes.JsonValueArray, error) {
	array := make(pgtypes.JsonValueArray, len(values))
	for i, value := range values {
		jsonValue, err := pgtypes.JsonValueFromSQLValue(ctx, jsonBuildArgType(argTypes, i), value)
		if err != nil {
			return nil, err
		}
		array[i] = jsonValue
	}
	return array, nil
}

func buildJsonObjectValue(ctx *sql.Context, fnName string, argTypes []*pgtypes.DoltgresType, values []any, sortKeys bool) (pgtypes.JsonValueObject, error) {
	if len(values)%2 != 0 {
		return pgtypes.JsonValueObject{}, pgerror.Newf(pgcode.InvalidParameterValue, "%s requires an even number of arguments", fnName)
	}
	items := make([]pgtypes.JsonValueObjectItem, 0, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		key, err := jsonBuildObjectKey(ctx, jsonBuildArgType(argTypes, i), values[i])
		if err != nil {
			return pgtypes.JsonValueObject{}, err
		}
		value, err := pgtypes.JsonValueFromSQLValue(ctx, jsonBuildArgType(argTypes, i+1), values[i+1])
		if err != nil {
			return pgtypes.JsonValueObject{}, err
		}
		items = append(items, pgtypes.JsonValueObjectItem{Key: key, Value: value})
	}
	return jsonObjectFromItems(items, sortKeys), nil
}

func buildJsonObjectFromTextArray(ctx *sql.Context, val any, sortKeys bool) (pgtypes.JsonValueObject, error) {
	values, err := textArrayArg(ctx, val)
	if err != nil {
		return pgtypes.JsonValueObject{}, err
	}
	rows, ok, err := textArrayRows(ctx, values)
	if err != nil {
		return pgtypes.JsonValueObject{}, err
	}
	if ok {
		items := make([]pgtypes.JsonValueObjectItem, 0, len(rows))
		for _, row := range rows {
			if len(row) != 2 {
				return pgtypes.JsonValueObject{}, errors.New("array must have two columns")
			}
			item, err := jsonObjectItemFromTextValues(ctx, row[0], row[1])
			if err != nil {
				return pgtypes.JsonValueObject{}, err
			}
			items = append(items, item)
		}
		return jsonObjectFromItems(items, sortKeys), nil
	}
	if len(values)%2 != 0 {
		return pgtypes.JsonValueObject{}, errors.New("array must have even number of elements")
	}
	items := make([]pgtypes.JsonValueObjectItem, 0, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		item, err := jsonObjectItemFromTextValues(ctx, values[i], values[i+1])
		if err != nil {
			return pgtypes.JsonValueObject{}, err
		}
		items = append(items, item)
	}
	return jsonObjectFromItems(items, sortKeys), nil
}

func buildJsonObjectFromTextArrays(ctx *sql.Context, keys any, values any, sortKeys bool) (pgtypes.JsonValueObject, error) {
	keyValues, err := textArrayArg(ctx, keys)
	if err != nil {
		return pgtypes.JsonValueObject{}, err
	}
	valueValues, err := textArrayArg(ctx, values)
	if err != nil {
		return pgtypes.JsonValueObject{}, err
	}
	keysAreMultidimensional, err := textArrayIsMultidimensional(ctx, keyValues)
	if err != nil {
		return pgtypes.JsonValueObject{}, err
	}
	valuesAreMultidimensional, err := textArrayIsMultidimensional(ctx, valueValues)
	if err != nil {
		return pgtypes.JsonValueObject{}, err
	}
	if keysAreMultidimensional || valuesAreMultidimensional {
		return pgtypes.JsonValueObject{}, errors.New("wrong number of array subscripts")
	}
	if len(keyValues) != len(valueValues) {
		return pgtypes.JsonValueObject{}, errors.New("mismatched array dimensions")
	}
	items := make([]pgtypes.JsonValueObjectItem, 0, len(keyValues))
	for i := range keyValues {
		item, err := jsonObjectItemFromTextValues(ctx, keyValues[i], valueValues[i])
		if err != nil {
			return pgtypes.JsonValueObject{}, err
		}
		items = append(items, item)
	}
	return jsonObjectFromItems(items, sortKeys), nil
}

func textArrayArg(ctx *sql.Context, val any) ([]any, error) {
	res, err := sql.UnwrapAny(ctx, val)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	values, ok := res.([]any)
	if !ok {
		return nil, errors.Errorf("expected text array, but got %T", res)
	}
	return values, nil
}

func textArrayRows(ctx *sql.Context, values []any) ([][]any, bool, error) {
	if len(values) == 0 {
		return nil, false, nil
	}
	rows := make([][]any, 0, len(values))
	foundNested := false
	foundScalar := false
	for _, value := range values {
		row, ok, err := textArrayNestedValue(ctx, value)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			foundScalar = true
			if foundNested {
				return nil, false, errors.New("array must have two columns")
			}
			continue
		}
		if foundScalar {
			return nil, false, errors.New("array must have two columns")
		}
		foundNested = true
		rows = append(rows, row)
	}
	if !foundNested {
		return nil, false, nil
	}
	return rows, true, nil
}

func textArrayIsMultidimensional(ctx *sql.Context, values []any) (bool, error) {
	for _, value := range values {
		if _, ok, err := textArrayNestedValue(ctx, value); err != nil {
			return false, err
		} else if ok {
			return true, nil
		}
	}
	return false, nil
}

func textArrayNestedValue(ctx *sql.Context, value any) ([]any, bool, error) {
	unwrapped, err := sql.UnwrapAny(ctx, value)
	if err != nil {
		return nil, false, err
	}
	switch row := value.(type) {
	case []any:
		return row, true, nil
	case sql.Row:
		return []any(row), true, nil
	}
	switch row := unwrapped.(type) {
	case []any:
		return row, true, nil
	case sql.Row:
		return []any(row), true, nil
	default:
		return nil, false, nil
	}
}

func jsonObjectItemFromTextValues(ctx *sql.Context, keyValue any, itemValue any) (pgtypes.JsonValueObjectItem, error) {
	key, err := jsonObjectTextKey(ctx, keyValue)
	if err != nil {
		return pgtypes.JsonValueObjectItem{}, err
	}
	value, err := jsonObjectTextValue(ctx, itemValue)
	if err != nil {
		return pgtypes.JsonValueObjectItem{}, err
	}
	return pgtypes.JsonValueObjectItem{Key: key, Value: value}, nil
}

func jsonObjectTextKey(ctx *sql.Context, val any) (string, error) {
	res, err := sql.UnwrapAny(ctx, val)
	if err != nil {
		return "", err
	}
	if res == nil {
		return "", errors.New("null value not allowed for object key")
	}
	str, ok := res.(string)
	if !ok {
		return "", errors.Errorf("expected text value for object key, but got %T", res)
	}
	return str, nil
}

func jsonObjectTextValue(ctx *sql.Context, val any) (pgtypes.JsonValue, error) {
	res, err := sql.UnwrapAny(ctx, val)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return pgtypes.JsonValueNull(0), nil
	}
	return pgtypes.JsonValueFromSQLValue(ctx, pgtypes.Text, res)
}

func jsonBuildArgType(argTypes []*pgtypes.DoltgresType, idx int) *pgtypes.DoltgresType {
	if idx < len(argTypes) {
		return argTypes[idx]
	}
	return nil
}

const jsonObjectNonScalarKeyErr = "key value must be scalar, not array, composite, or json"

func jsonObjectKeyIsNonScalar(typ *pgtypes.DoltgresType, val any) bool {
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

func jsonBuildObjectKey(ctx *sql.Context, typ *pgtypes.DoltgresType, val any) (string, error) {
	res, err := sql.UnwrapAny(ctx, val)
	if err != nil {
		return "", err
	}
	if res == nil {
		return "", pgerror.New(pgcode.InvalidParameterValue, "argument key must not be null")
	}
	if jsonObjectKeyIsNonScalar(typ, res) {
		return "", pgerror.New(pgcode.InvalidParameterValue, jsonObjectNonScalarKeyErr)
	}
	if str, ok := res.(string); ok {
		return str, nil
	}
	if typ != nil {
		return typ.IoOutput(ctx, res)
	}
	return fmt.Sprint(res), nil
}

func jsonValueOutput(ctx *sql.Context, value pgtypes.JsonValue) (string, error) {
	sb := strings.Builder{}
	pgtypes.JsonValueFormatterPreserveRaw(&sb, value)
	return sb.String(), nil
}

func jsonBuildValueOutput(ctx *sql.Context, value pgtypes.JsonValue) (string, error) {
	sb := strings.Builder{}
	jsonBuildValueFormatter(&sb, value)
	return sb.String(), nil
}

func jsonBuildValueFormatter(sb *strings.Builder, value pgtypes.JsonValue) {
	if raw, ok := pgtypes.JsonValueRawText(value); ok {
		sb.WriteString(raw)
		return
	}
	value = pgtypes.JsonValueUnwrapRaw(value)
	switch value := value.(type) {
	case pgtypes.JsonValueObject:
		sb.WriteRune('{')
		for i, item := range value.Items {
			if i > 0 {
				sb.WriteString(", ")
			}
			jsonWriteKeyString(sb, item.Key)
			sb.WriteString(" : ")
			jsonBuildValueFormatter(sb, item.Value)
		}
		sb.WriteRune('}')
	case pgtypes.JsonValueArray:
		sb.WriteRune('[')
		for i, item := range value {
			if i > 0 {
				sb.WriteString(", ")
			}
			jsonBuildValueFormatter(sb, item)
		}
		sb.WriteRune(']')
	default:
		pgtypes.JsonValueFormatterCompact(sb, value)
	}
}

func jsonWriteKeyString(sb *strings.Builder, value string) {
	bytes, _ := json.MarshalWithOption(value, json.DisableHTMLEscape())
	sb.Write(bytes)
}

func jsonValueOutputPretty(ctx *sql.Context, value pgtypes.JsonValue) (string, error) {
	sb := strings.Builder{}
	pgtypes.JsonValueFormatPretty(&sb, value, 0)
	return sb.String(), nil
}
