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
		doc, err := pgtypes.UnmarshalToJsonDocument([]byte(res.(string)))
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
	switch value := value.(type) {
	case pgtypes.JsonValueArray:
		return value, nil
	case pgtypes.JsonValueObject:
		return nil, errors.New("cannot extract elements from an object")
	default:
		return nil, errors.New("cannot extract elements from a scalar")
	}
}

func jsonValueAsArrayForLength(value pgtypes.JsonValue) (pgtypes.JsonValueArray, error) {
	array, ok := value.(pgtypes.JsonValueArray)
	if !ok {
		return nil, errors.New("cannot get array length of a non-array")
	}
	return array, nil
}

func jsonValueAsObjectForKeys(fnName string, value pgtypes.JsonValue) (pgtypes.JsonValueObject, error) {
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
	switch value := value.(type) {
	case pgtypes.JsonValueString:
		return pgtypes.JsonStringUnescape(value)
	case pgtypes.JsonValueNull:
		return nil, nil
	default:
		return pgtypes.JsonB.IoOutput(ctx, pgtypes.JsonDocument{Value: value})
	}
}

func jsonValueToOutput(ctx *sql.Context, value pgtypes.JsonValue) (any, error) {
	return pgtypes.JsonB.IoOutput(ctx, pgtypes.JsonDocument{Value: value})
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

func jsonbSetValue(target pgtypes.JsonValue, path []string, newValue pgtypes.JsonValue, createMissing bool) pgtypes.JsonValue {
	if len(path) == 0 {
		return pgtypes.JsonValueCopy(target)
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
			return newObject
		}
		if idx, ok := newObject.Index[key]; ok {
			newObject.Items[idx].Value = jsonbSetValue(newObject.Items[idx].Value, path[1:], newValue, createMissing)
		}
		return newObject
	case pgtypes.JsonValueArray:
		newArray := pgtypes.JsonValueCopy(value).(pgtypes.JsonValueArray)
		idx, ok := jsonArrayPathIndex(path[0], len(newArray))
		if !ok {
			return newArray
		}
		if len(path) == 1 {
			if idx >= 0 && idx < len(newArray) {
				newArray[idx] = pgtypes.JsonValueCopy(newValue)
			} else if createMissing {
				if idx < 0 {
					return append(pgtypes.JsonValueArray{pgtypes.JsonValueCopy(newValue)}, newArray...)
				}
				return append(newArray, pgtypes.JsonValueCopy(newValue))
			}
			return newArray
		}
		if idx < 0 || idx >= len(newArray) {
			return newArray
		}
		newArray[idx] = jsonbSetValue(newArray[idx], path[1:], newValue, createMissing)
		return newArray
	default:
		return pgtypes.JsonValueCopy(target)
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

func textArrayToStringSlice(val any) ([]string, error) {
	values := val.([]any)
	path := make([]string, len(values))
	for i, value := range values {
		if value == nil {
			return nil, errors.Errorf("path element at position %d is null", i+1)
		}
		path[i] = value.(string)
	}
	return path, nil
}

func jsonbPretty(value pgtypes.JsonValue) string {
	sb := strings.Builder{}
	pgtypes.JsonValueFormatPretty(&sb, value, 0)
	return sb.String()
}
