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
	framework.RegisterFunction(json_array_length)
	framework.RegisterFunction(json_array_elements)
	framework.RegisterFunction(json_array_elements_text)
	framework.RegisterFunction(json_object_keys)
	framework.RegisterFunction(json_each)
	framework.RegisterFunction(json_each_text)
	framework.RegisterFunction(json_typeof)
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
		return val.(string), nil
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
		return jsonValueOutput(ctx, value)
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
		return pgtypes.JsonValueObject{}, sql.ErrInvalidArgumentNumber.New(fnName, "even number of arguments", len(values))
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

func jsonBuildArgType(argTypes []*pgtypes.DoltgresType, idx int) *pgtypes.DoltgresType {
	if idx < len(argTypes) {
		return argTypes[idx]
	}
	return nil
}

func jsonBuildObjectKey(ctx *sql.Context, typ *pgtypes.DoltgresType, val any) (string, error) {
	res, err := sql.UnwrapAny(ctx, val)
	if err != nil {
		return "", err
	}
	if res == nil {
		return "", errors.New("argument key must not be null")
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
	pgtypes.JsonValueFormatterCompact(&sb, value)
	return sb.String(), nil
}
