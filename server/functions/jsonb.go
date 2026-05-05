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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/goccy/go-json"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
	"github.com/dolthub/doltgresql/utils"
)

// initJsonB registers the functions to the catalog.
func initJsonB() {
	framework.RegisterFunction(jsonb_in)
	framework.RegisterFunction(jsonb_out)
	framework.RegisterFunction(jsonb_recv)
	framework.RegisterFunction(jsonb_send)
	framework.RegisterFunction(jsonb_cmp)
	framework.RegisterFunction(jsonb_build_array_empty)
	framework.RegisterFunction(jsonb_build_array)
	framework.RegisterFunction(jsonb_build_object_empty)
	framework.RegisterFunction(jsonb_build_object)
	framework.RegisterFunction(to_jsonb_anyelement)
	framework.RegisterFunction(jsonb_array_length)
	framework.RegisterFunction(jsonb_array_elements)
	framework.RegisterFunction(jsonb_array_elements_text)
	framework.RegisterFunction(jsonb_object_keys)
	framework.RegisterFunction(jsonb_each)
	framework.RegisterFunction(jsonb_each_text)
	framework.RegisterFunction(jsonb_typeof)
	framework.RegisterFunction(jsonb_strip_nulls)
	framework.RegisterFunction(jsonb_set)
	framework.RegisterFunction(jsonb_set_create)
	framework.RegisterFunction(jsonb_delete_path)
	framework.RegisterFunction(json_remove_path)
	framework.RegisterFunction(jsonb_pretty)
}

// jsonb_in represents the PostgreSQL function of jsonb type IO input.
var jsonb_in = framework.Function1{
	Name:       "jsonb_in",
	Return:     pgtypes.JsonB,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Cstring},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		input := val.(string)
		inputBytes := unsafe.Slice(unsafe.StringData(input), len(input))
		if json.Valid(inputBytes) {
			doc, err := pgtypes.UnmarshalToJsonDocument(inputBytes)
			return doc, err
		}
		if len(input) > 10 {
			input = input[:10] + "..."
		}
		return nil, pgtypes.ErrInvalidSyntaxForType.New("jsonb", input)
	},
}

// jsonb_out represents the PostgreSQL function of jsonb type IO output.
var jsonb_out = framework.Function1{
	Name:       "jsonb_out",
	Return:     pgtypes.Cstring,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.JsonB},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		res, err := pgtypes.JsonDocumentFromSQLValue(ctx, pgtypes.JsonB, val)
		if err != nil {
			return nil, err
		}
		sb := strings.Builder{}
		sb.Grow(256)
		pgtypes.JsonValueFormatter(&sb, res.Value)
		return sb.String(), nil
	},
}

// jsonb_recv represents the PostgreSQL function of jsonb type IO receive.
var jsonb_recv = framework.Function1{
	Name:       "jsonb_recv",
	Return:     pgtypes.JsonB,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Internal},
	Strict:     true,
	Callable: func(ctx *sql.Context, t [2]*pgtypes.DoltgresType, val any) (any, error) {
		data := val.([]byte)
		if data == nil {
			return nil, nil
		}
		if len(data) <= 1 {
			return "", nil
		}
		return t[1].IoInput(ctx, string(data[1:]))
	},
}

// jsonb_send represents the PostgreSQL function of jsonb type IO send.
var jsonb_send = framework.Function1{
	Name:       "jsonb_send",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.JsonB},
	Strict:     true,
	Callable: func(ctx *sql.Context, t [2]*pgtypes.DoltgresType, val any) (any, error) {
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
		textVal, err := t[0].SQL(ctx, nil, val)
		if err != nil {
			return nil, err
		}
		writer := utils.NewWireWriter()
		writer.WriteUint8(1)
		writer.WriteBytes(textVal.ToBytes())
		return writer.BufferData(), nil
	},
}

// jsonb_cmp represents the PostgreSQL function of jsonb type compare.
var jsonb_cmp = framework.Function2{
	Name:       "jsonb_cmp",
	Return:     pgtypes.Int32,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.JsonB, pgtypes.JsonB},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		ab, err := pgtypes.JsonDocumentFromSQLValue(ctx, pgtypes.JsonB, val1)
		if err != nil {
			return nil, err
		}
		bb, err := pgtypes.JsonDocumentFromSQLValue(ctx, pgtypes.JsonB, val2)
		if err != nil {
			return nil, err
		}
		return int32(pgtypes.JsonValueCompare(ab.Value, bb.Value)), nil
	},
}

// jsonb_build_array represents the PostgreSQL function jsonb_build_array.
var jsonb_build_array_empty = framework.Function0{
	Name:   "jsonb_build_array",
	Return: pgtypes.JsonB,
	Callable: func(ctx *sql.Context) (any, error) {
		return pgtypes.JsonDocument{Value: pgtypes.JsonValueArray{}}, nil
	},
}

// jsonb_build_array represents the PostgreSQL function jsonb_build_array.
var jsonb_build_array = framework.Function1N{
	Name:       "jsonb_build_array",
	Return:     pgtypes.JsonB,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Any},
	Callable: func(ctx *sql.Context, argTypes []*pgtypes.DoltgresType, val1 any, vals []any) (any, error) {
		value, err := buildJsonArrayValue(ctx, argTypes, append([]any{val1}, vals...))
		if err != nil {
			return nil, err
		}
		return pgtypes.JsonDocument{Value: value}, nil
	},
}

// jsonb_build_object represents the PostgreSQL function jsonb_build_object.
var jsonb_build_object_empty = framework.Function0{
	Name:   "jsonb_build_object",
	Return: pgtypes.JsonB,
	Callable: func(ctx *sql.Context) (any, error) {
		return pgtypes.JsonDocument{Value: pgtypes.JsonValueObject{Index: map[string]int{}}}, nil
	},
}

// jsonb_build_object represents the PostgreSQL function jsonb_build_object.
var jsonb_build_object = framework.Function1N{
	Name:       "jsonb_build_object",
	Return:     pgtypes.JsonB,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Any},
	Callable: func(ctx *sql.Context, argTypes []*pgtypes.DoltgresType, val1 any, vals []any) (any, error) {
		value, err := buildJsonObjectValue(ctx, "jsonb_build_object", argTypes, append([]any{val1}, vals...), true)
		if err != nil {
			return nil, err
		}
		return pgtypes.JsonDocument{Value: value}, nil
	},
}

// to_jsonb_anyelement represents the PostgreSQL function to_jsonb(anyelement).
var to_jsonb_anyelement = framework.Function1{
	Name:       "to_jsonb",
	Return:     pgtypes.JsonB,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.AnyElement},
	Strict:     false,
	Callable: func(ctx *sql.Context, t [2]*pgtypes.DoltgresType, val any) (any, error) {
		value, err := jsonValueFromAnyElement(ctx, t[0], val)
		if err != nil {
			return nil, err
		}
		return pgtypes.JsonDocument{Value: value}, nil
	},
}

func jsonValueFromAnyElement(ctx *sql.Context, typ *pgtypes.DoltgresType, val any) (pgtypes.JsonValue, error) {
	res, err := sql.UnwrapAny(ctx, val)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return pgtypes.JsonValueNull(0), nil
	}
	if record, ok := res.([]pgtypes.RecordValue); ok {
		return jsonValueFromRecord(ctx, typ, record)
	}
	return pgtypes.JsonValueFromSQLValue(ctx, typ, res)
}

func jsonValueFromRecord(ctx *sql.Context, typ *pgtypes.DoltgresType, record []pgtypes.RecordValue) (pgtypes.JsonValue, error) {
	items := make([]pgtypes.JsonValueObjectItem, len(record))
	for i, field := range record {
		fieldType, _ := field.Type.(*pgtypes.DoltgresType)
		value, err := pgtypes.JsonValueFromSQLValue(ctx, fieldType, field.Value)
		if err != nil {
			return nil, err
		}
		items[i] = pgtypes.JsonValueObjectItem{
			Key:   jsonRecordFieldName(typ, i),
			Value: value,
		}
	}
	return jsonObjectFromItems(items, true), nil
}

func jsonRecordFieldName(typ *pgtypes.DoltgresType, idx int) string {
	if typ != nil && idx < len(typ.CompositeAttrs) && typ.CompositeAttrs[idx].Name != "" {
		return typ.CompositeAttrs[idx].Name
	}
	return fmt.Sprintf("f%d", idx+1)
}

// jsonb_array_length represents the PostgreSQL function jsonb_array_length.
var jsonb_array_length = framework.Function1{
	Name:       "jsonb_array_length",
	Return:     pgtypes.Int32,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.JsonB},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		doc, err := jsonDocumentFromFunctionValue(ctx, pgtypes.JsonB, val)
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

// jsonb_array_elements represents the PostgreSQL function jsonb_array_elements.
var jsonb_array_elements = framework.Function1{
	Name:       "jsonb_array_elements",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.JsonB),
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.JsonB},
	Strict:     true,
	SRF:        true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		doc, err := jsonDocumentFromFunctionValue(ctx, pgtypes.JsonB, val)
		if err != nil {
			return nil, err
		}
		array, err := jsonValueAsArrayForElements(doc.Value)
		if err != nil {
			return nil, err
		}
		return jsonArrayElementsRowIter(array, jsonbValueToOutput), nil
	},
}

// jsonb_array_elements_text represents the PostgreSQL function jsonb_array_elements_text.
var jsonb_array_elements_text = framework.Function1{
	Name:       "jsonb_array_elements_text",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.Text),
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.JsonB},
	Strict:     true,
	SRF:        true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		doc, err := jsonDocumentFromFunctionValue(ctx, pgtypes.JsonB, val)
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

// jsonb_object_keys represents the PostgreSQL function jsonb_object_keys.
var jsonb_object_keys = framework.Function1{
	Name:       "jsonb_object_keys",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.Text),
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.JsonB},
	Strict:     true,
	SRF:        true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		doc, err := jsonDocumentFromFunctionValue(ctx, pgtypes.JsonB, val)
		if err != nil {
			return nil, err
		}
		object, err := jsonValueAsObjectForKeys("jsonb_object_keys", doc.Value)
		if err != nil {
			return nil, err
		}
		return jsonObjectKeysRowIter(object), nil
	},
}

// jsonb_each represents the PostgreSQL function jsonb_each.
var jsonb_each = framework.Function1{
	Name:       "jsonb_each",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.Record),
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.JsonB},
	Strict:     true,
	SRF:        true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		doc, err := jsonDocumentFromFunctionValue(ctx, pgtypes.JsonB, val)
		if err != nil {
			return nil, err
		}
		object, err := jsonValueAsObjectForKeys("jsonb_each", doc.Value)
		if err != nil {
			return nil, err
		}
		return jsonEachRowIter(object, pgtypes.JsonB, jsonbValueToOutput), nil
	},
}

// jsonb_each_text represents the PostgreSQL function jsonb_each_text.
var jsonb_each_text = framework.Function1{
	Name:       "jsonb_each_text",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.Record),
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.JsonB},
	Strict:     true,
	SRF:        true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		doc, err := jsonDocumentFromFunctionValue(ctx, pgtypes.JsonB, val)
		if err != nil {
			return nil, err
		}
		object, err := jsonValueAsObjectForKeys("jsonb_each_text", doc.Value)
		if err != nil {
			return nil, err
		}
		return jsonEachRowIter(object, pgtypes.Text, jsonValueAsText), nil
	},
}

// jsonb_typeof represents the PostgreSQL function jsonb_typeof.
var jsonb_typeof = framework.Function1{
	Name:       "jsonb_typeof",
	Return:     pgtypes.Text,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.JsonB},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		doc, err := jsonDocumentFromFunctionValue(ctx, pgtypes.JsonB, val)
		if err != nil {
			return nil, err
		}
		return pgtypes.JsonValueTypeName(doc.Value), nil
	},
}

// jsonb_strip_nulls represents the PostgreSQL function jsonb_strip_nulls.
var jsonb_strip_nulls = framework.Function1{
	Name:       "jsonb_strip_nulls",
	Return:     pgtypes.JsonB,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.JsonB},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		doc, err := jsonDocumentFromFunctionValue(ctx, pgtypes.JsonB, val)
		if err != nil {
			return nil, err
		}
		return pgtypes.JsonDocument{Value: pgtypes.JsonValueStripNulls(doc.Value)}, nil
	},
}

// jsonb_set represents the PostgreSQL function jsonb_set with create_if_missing defaulting to true.
var jsonb_set = framework.Function3{
	Name:       "jsonb_set",
	Return:     pgtypes.JsonB,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.JsonB, pgtypes.TextArray, pgtypes.JsonB},
	Strict:     true,
	Callable: func(ctx *sql.Context, t [4]*pgtypes.DoltgresType, val1 any, val2 any, val3 any) (any, error) {
		return jsonbSetCallable(ctx, val1, val2, val3, true)
	},
}

// jsonb_set_create represents the PostgreSQL function jsonb_set with an explicit create_if_missing argument.
var jsonb_set_create = framework.Function4{
	Name:       "jsonb_set",
	Return:     pgtypes.JsonB,
	Parameters: [4]*pgtypes.DoltgresType{pgtypes.JsonB, pgtypes.TextArray, pgtypes.JsonB, pgtypes.Bool},
	Strict:     true,
	Callable: func(ctx *sql.Context, t [5]*pgtypes.DoltgresType, val1 any, val2 any, val3 any, val4 any) (any, error) {
		return jsonbSetCallable(ctx, val1, val2, val3, val4.(bool))
	},
}

func jsonbSetCallable(ctx *sql.Context, target any, path any, newValue any, createMissing bool) (any, error) {
	targetDoc, err := jsonDocumentFromFunctionValue(ctx, pgtypes.JsonB, target)
	if err != nil {
		return nil, err
	}
	newDoc, err := jsonDocumentFromFunctionValue(ctx, pgtypes.JsonB, newValue)
	if err != nil {
		return nil, err
	}
	jsonPath, err := textArrayToStringSlice(path)
	if err != nil {
		return nil, err
	}
	return pgtypes.JsonDocument{Value: jsonbSetValue(targetDoc.Value, jsonPath, newDoc.Value, createMissing)}, nil
}

// jsonb_delete_path represents the PostgreSQL function jsonb_delete_path.
var jsonb_delete_path = framework.Function2{
	Name:       "jsonb_delete_path",
	Return:     pgtypes.JsonB,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.JsonB, pgtypes.TextArray},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		return jsonbDeletePathCallable(ctx, val1, val2)
	},
}

// json_remove_path backs Cockroach's parser rewrite for PostgreSQL's #- JSONB operator.
var json_remove_path = framework.Function2{
	Name:       "json_remove_path",
	Return:     pgtypes.JsonB,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.JsonB, pgtypes.TextArray},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		return jsonbDeletePathCallable(ctx, val1, val2)
	},
}

func jsonbDeletePathCallable(ctx *sql.Context, target any, path any) (any, error) {
	targetDoc, err := jsonDocumentFromFunctionValue(ctx, pgtypes.JsonB, target)
	if err != nil {
		return nil, err
	}
	jsonPath, err := textArrayToStringSlice(path)
	if err != nil {
		return nil, err
	}
	value, err := pgtypes.JsonValueDeletePath(targetDoc.Value, jsonPath)
	if err != nil {
		return nil, err
	}
	return pgtypes.JsonDocument{Value: value}, nil
}

// jsonb_pretty represents the PostgreSQL function jsonb_pretty.
var jsonb_pretty = framework.Function1{
	Name:       "jsonb_pretty",
	Return:     pgtypes.Text,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.JsonB},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		doc, err := jsonDocumentFromFunctionValue(ctx, pgtypes.JsonB, val)
		if err != nil {
			return nil, err
		}
		return jsonbPretty(doc.Value), nil
	},
}
