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
	"io"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func jsonDocumentFromFunctionValue(ctx *sql.Context, typ *pgtypes.DoltgresType, val any) (pgtypes.JsonDocument, error) {
	res, err := sql.UnwrapAny(ctx, val)
	if err != nil {
		return pgtypes.JsonDocument{}, err
	}
	if res == nil {
		return pgtypes.JsonDocument{}, nil
	}
	switch typ {
	case pgtypes.Json:
		doc, err := pgtypes.UnmarshalToJsonDocumentPreserveObjectItems([]byte(res.(string)))
		if err != nil {
			return pgtypes.JsonDocument{}, err
		}
		return doc, nil
	case pgtypes.JsonB:
		return pgtypes.JsonDocumentFromSQLValue(ctx, typ, res)
	default:
		return pgtypes.JsonDocumentFromSQLValue(ctx, typ, res)
	}
}

// jsonValueAsArrayForElements validates that a JSON value is an array for *_array_elements.
func jsonValueAsArrayForElements(value pgtypes.JsonValue) (pgtypes.JsonValueArray, error) {
	value = pgtypes.JsonValueUnwrapRaw(value)
	switch value := value.(type) {
	case pgtypes.JsonValueArray:
		return value, nil
	case pgtypes.JsonValueObject:
		return nil, pgerror.New(pgcode.InvalidParameterValue, "cannot extract elements from an object")
	default:
		return nil, pgerror.New(pgcode.InvalidParameterValue, "cannot extract elements from a scalar")
	}
}

func jsonValueAsArrayForLength(value pgtypes.JsonValue) (pgtypes.JsonValueArray, error) {
	value = pgtypes.JsonValueUnwrapRaw(value)
	array, ok := value.(pgtypes.JsonValueArray)
	if !ok {
		return nil, pgerror.New(pgcode.InvalidParameterValue, "cannot get array length of a non-array")
	}
	return array, nil
}

func jsonValueAsObjectForKeys(fnName string, value pgtypes.JsonValue) (pgtypes.JsonValueObject, error) {
	value = pgtypes.JsonValueUnwrapRaw(value)
	switch value := value.(type) {
	case pgtypes.JsonValueObject:
		return value, nil
	case pgtypes.JsonValueArray:
		return pgtypes.JsonValueObject{}, errors.Errorf("cannot call %s on an array", fnName)
	default:
		return pgtypes.JsonValueObject{}, errors.Errorf("cannot call %s on a scalar", fnName)
	}
}

// jsonArrayElementsRowIter returns a row iterator that emits one row per JSON array element.
func jsonArrayElementsRowIter(
	array pgtypes.JsonValueArray,
	rowMapper func(ctx *sql.Context, value pgtypes.JsonValue) (any, error),
) *pgtypes.SetReturningFunctionRowIter {
	var i int
	return pgtypes.NewSetReturningFunctionRowIter(func(ctx *sql.Context) (sql.Row, error) {
		if i >= len(array) {
			return nil, io.EOF
		}
		value := array[i]
		i++
		mapped, err := rowMapper(ctx, value)
		if err != nil {
			return nil, err
		}
		return sql.Row{mapped}, nil
	})
}

func jsonObjectKeysRowIter(object pgtypes.JsonValueObject) *pgtypes.SetReturningFunctionRowIter {
	var i int
	return pgtypes.NewSetReturningFunctionRowIter(func(ctx *sql.Context) (sql.Row, error) {
		if i >= len(object.Items) {
			return nil, io.EOF
		}
		key := object.Items[i].Key
		i++
		return sql.Row{key}, nil
	})
}

func jsonEachRowIter(
	object pgtypes.JsonValueObject,
	valueType *pgtypes.DoltgresType,
	rowMapper func(ctx *sql.Context, value pgtypes.JsonValue) (any, error),
) *pgtypes.SetReturningFunctionRowIter {
	var i int
	return pgtypes.NewSetReturningFunctionRowIter(func(ctx *sql.Context) (sql.Row, error) {
		if i >= len(object.Items) {
			return nil, io.EOF
		}
		item := object.Items[i]
		i++
		mapped, err := rowMapper(ctx, item.Value)
		if err != nil {
			return nil, err
		}
		return sql.Row{[]pgtypes.RecordValue{
			{Type: pgtypes.Text, Value: item.Key},
			{Type: valueType, Value: mapped},
		}}, nil
	})
}

// jsonValueAsText returns the textual representation used by *_text helpers.
func jsonValueAsText(ctx *sql.Context, value pgtypes.JsonValue) (any, error) {
	raw, hasRaw := pgtypes.JsonValueRawText(value)
	switch value := pgtypes.JsonValueUnwrapRaw(value).(type) {
	case pgtypes.JsonValueString:
		return pgtypes.JsonStringUnescape(value)
	case pgtypes.JsonValueNull:
		return nil, nil
	default:
		if hasRaw {
			return raw, nil
		}
		return pgtypes.JsonB.IoOutput(ctx, pgtypes.JsonDocument{Value: value})
	}
}

func jsonValueToOutput(ctx *sql.Context, value pgtypes.JsonValue) (any, error) {
	sb := strings.Builder{}
	pgtypes.JsonValueFormatterPreserveRaw(&sb, value)
	return sb.String(), nil
}

func jsonbValueToOutput(_ *sql.Context, value pgtypes.JsonValue) (any, error) {
	return pgtypes.JsonDocument{Value: value}, nil
}

func jsonObjectFromItems(items []pgtypes.JsonValueObjectItem, sortKeys bool) pgtypes.JsonValueObject {
	return pgtypes.JsonObjectFromItems(items, sortKeys)
}

func jsonbContainsValue(container pgtypes.JsonValue, contained pgtypes.JsonValue) bool {
	return pgtypes.JsonBContainsValue(container, contained)
}

func jsonbSetValue(target pgtypes.JsonValue, path []string, newValue pgtypes.JsonValue, createMissing bool) (pgtypes.JsonValue, error) {
	return jsonbSetValueAt(target, path, newValue, createMissing, 1)
}

func jsonbSetValueAt(target pgtypes.JsonValue, path []string, newValue pgtypes.JsonValue, createMissing bool, position int) (pgtypes.JsonValue, error) {
	if len(path) == 0 {
		return pgtypes.JsonValueCopy(target), nil
	}
	switch value := target.(type) {
	case pgtypes.JsonValueObject:
		newObject := pgtypes.JsonValueCopy(value).(pgtypes.JsonValueObject)
		key := path[0]
		if len(path) == 1 {
			if idx, ok := newObject.Index[key]; ok {
				newObject.Items[idx].Value = pgtypes.JsonValueCopy(newValue)
			} else if createMissing {
				newObject.Items = append(newObject.Items, pgtypes.JsonValueObjectItem{Key: key, Value: pgtypes.JsonValueCopy(newValue)})
				newObject = jsonObjectFromItems(newObject.Items, true)
			}
			return newObject, nil
		}
		if idx, ok := newObject.Index[key]; ok {
			nested, err := jsonbSetValueAt(newObject.Items[idx].Value, path[1:], newValue, createMissing, position+1)
			if err != nil {
				return nil, err
			}
			newObject.Items[idx].Value = nested
		}
		return newObject, nil
	case pgtypes.JsonValueArray:
		newArray := pgtypes.JsonValueCopy(value).(pgtypes.JsonValueArray)
		idx, ok := jsonArrayPathIndex(path[0], len(newArray))
		if !ok {
			return nil, pgerror.Newf(pgcode.InvalidTextRepresentation, "path element at position %d is not an integer: %q", position, path[0])
		}
		if len(path) == 1 {
			if idx >= 0 && idx < len(newArray) {
				newArray[idx] = pgtypes.JsonValueCopy(newValue)
			} else if createMissing {
				if idx < 0 {
					return append(pgtypes.JsonValueArray{pgtypes.JsonValueCopy(newValue)}, newArray...), nil
				}
				return append(newArray, pgtypes.JsonValueCopy(newValue)), nil
			}
			return newArray, nil
		}
		if idx < 0 || idx >= len(newArray) {
			return newArray, nil
		}
		nested, err := jsonbSetValueAt(newArray[idx], path[1:], newValue, createMissing, position+1)
		if err != nil {
			return nil, err
		}
		newArray[idx] = nested
		return newArray, nil
	default:
		return nil, pgerror.New(pgcode.InvalidParameterValue, "cannot set path in scalar")
	}
}

func jsonArrayPathIndex(path string, length int) (int, bool) {
	idx, err := strconv.Atoi(path)
	if err != nil {
		return 0, false
	}
	if idx < 0 {
		idx += length
	}
	return idx, true
}

func textArrayToStringSlice(ctx *sql.Context, val any) ([]string, error) {
	values := val.([]any)
	path := make([]string, len(values))
	for i, value := range values {
		unwrapped, err := sql.UnwrapAny(ctx, value)
		if err != nil {
			return nil, err
		}
		if unwrapped == nil {
			return nil, pgerror.Newf(pgcode.NullValueNotAllowed, "path element at position %d is null", i+1)
		}
		path[i] = unwrapped.(string)
	}
	return path, nil
}

func jsonbPretty(value pgtypes.JsonValue) string {
	sb := strings.Builder{}
	pgtypes.JsonValueFormatPretty(&sb, value, 0)
	return sb.String()
}
