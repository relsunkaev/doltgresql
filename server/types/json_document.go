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

package types

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/goccy/go-json"
	"github.com/shopspring/decimal"

	"github.com/dolthub/doltgresql/utils"
)

// jsonDocumentStringUnicodeRegex is used on a JsonDocument's string to find all Unicode escape sequences that have an
// additional backslash.
var jsonDocumentStringUnicodeRegex = regexp.MustCompile(`\\\\u([0-9A-Fa-f]{4})`)

// JsonValueType represents a JSON value type. These values are serialized, and therefore should never be modified.
type JsonValueType byte

const (
	JsonValueType_Object  JsonValueType = 0
	JsonValueType_Array   JsonValueType = 1
	JsonValueType_String  JsonValueType = 2
	JsonValueType_Number  JsonValueType = 3
	JsonValueType_Boolean JsonValueType = 4
	JsonValueType_Null    JsonValueType = 5
)

// JsonDocument represents an entire JSON document.
type JsonDocument struct {
	Value JsonValue
}

// JsonValue is a value that represents some kind of data in JSON.
type JsonValue interface {
	// enforceJsonInterfaceInheritance is a special function that ensures only the expected types inherit this interface.
	enforceJsonInterfaceInheritance(error)
}

// JsonValueObject represents a JSON object.
type JsonValueObject struct {
	Items []JsonValueObjectItem
	Index map[string]int
}

// JsonValueObjectItem represents a specific item inside a JsonObject.
type JsonValueObjectItem struct {
	Key   string
	Value JsonValue
}

// JsonValueArray represents a JSON array.
type JsonValueArray []JsonValue

// JsonValueString represents a string value.
type JsonValueString string

// JsonValueNumber represents a number.
type JsonValueNumber decimal.Decimal

// JsonValueBoolean represents a boolean value.
type JsonValueBoolean bool

// JsonValueNull represents a null value.
type JsonValueNull byte

var _ JsonValue = JsonValueObject{}
var _ JsonValue = JsonValueArray{}
var _ JsonValue = JsonValueString("")
var _ JsonValue = JsonValueNumber{}
var _ JsonValue = JsonValueBoolean(false)
var _ JsonValue = JsonValueNull(0)

// enforceJsonInterfaceInheritance implements the JsonValue interface.
func (JsonValueObject) enforceJsonInterfaceInheritance(error) {}

// enforceJsonInterfaceInheritance implements the JsonValue interface.
func (JsonValueArray) enforceJsonInterfaceInheritance(error) {}

// enforceJsonInterfaceInheritance implements the JsonValue interface.
func (JsonValueString) enforceJsonInterfaceInheritance(error) {}

// enforceJsonInterfaceInheritance implements the JsonValue interface.
func (JsonValueNumber) enforceJsonInterfaceInheritance(error) {}

// enforceJsonInterfaceInheritance implements the JsonValue interface.
func (JsonValueBoolean) enforceJsonInterfaceInheritance(error) {}

// enforceJsonInterfaceInheritance implements the JsonValue interface.
func (JsonValueNull) enforceJsonInterfaceInheritance(error) {}

// JsonValueCopy returns a new copy of the given JsonValue that may be freely modified.
func JsonValueCopy(value JsonValue) JsonValue {
	switch value := value.(type) {
	case JsonValueObject:
		newItems := make([]JsonValueObjectItem, len(value.Items))
		newIndex := make(map[string]int)
		for i := range value.Items {
			newItems[i].Key = value.Items[i].Key
			newItems[i].Value = JsonValueCopy(value.Items[i].Value)
			newIndex[newItems[i].Key] = i
		}
		return JsonValueObject{
			Items: newItems,
			Index: newIndex,
		}
	case JsonValueArray:
		newArray := make(JsonValueArray, len(value))
		for i := range value {
			newArray[i] = JsonValueCopy(value[i])
		}
		return newArray
	default:
		return value
	}
}

// JsonObjectFromItems constructs a JSON object from its item list.
func JsonObjectFromItems(items []JsonValueObjectItem, sortKeys bool) JsonValueObject {
	copied := make([]JsonValueObjectItem, len(items))
	for i, item := range items {
		copied[i] = JsonValueObjectItem{
			Key:   item.Key,
			Value: JsonValueCopy(item.Value),
		}
	}
	if sortKeys {
		seen := make(map[string]JsonValue, len(copied))
		for _, item := range copied {
			seen[item.Key] = item.Value
		}
		copied = copied[:0]
		for key, value := range seen {
			copied = append(copied, JsonValueObjectItem{Key: key, Value: value})
		}
		sort.Slice(copied, func(i, j int) bool {
			if len(copied[i].Key) < len(copied[j].Key) {
				return true
			} else if len(copied[i].Key) > len(copied[j].Key) {
				return false
			}
			return copied[i].Key < copied[j].Key
		})
	}
	index := make(map[string]int, len(copied))
	for i, item := range copied {
		index[item.Key] = i
	}
	return JsonValueObject{Items: copied, Index: index}
}

// JsonValueCompare compares two values.
func JsonValueCompare(v1 JsonValue, v2 JsonValue) int {
	// Some types sort before others, so we'll check those first
	v1TypeSortOrder := jsonValueTypeSortOrder(v1)
	v2TypeSortOrder := jsonValueTypeSortOrder(v2)
	if v1TypeSortOrder < v2TypeSortOrder {
		return -1
	} else if v1TypeSortOrder > v2TypeSortOrder {
		return 1
	}

	// TODO: these should use the actual comparison operator functions for their respective types
	switch v1 := v1.(type) {
	case JsonValueObject:
		v2 := v2.(JsonValueObject)
		if len(v1.Items) < len(v2.Items) {
			return -1
		} else if len(v1.Items) > len(v2.Items) {
			return 1
		}
		// Items in an object are already sorted, so we can simply iterate over the items
		for i := 0; i < len(v1.Items); i++ {
			if v1.Items[i].Key < v2.Items[i].Key {
				return -1
			} else if v1.Items[i].Key > v2.Items[i].Key {
				return 1
			} else {
				innerCmp := JsonValueCompare(v1.Items[i].Value, v2.Items[i].Value)
				if innerCmp != 0 {
					return innerCmp
				}
			}
		}
		return 0
	case JsonValueArray:
		v2 := v2.(JsonValueArray)
		if len(v1) < len(v2) {
			return -1
		} else if len(v1) > len(v2) {
			return 1
		}
		for i := 0; i < len(v1); i++ {
			innerCmp := JsonValueCompare(v1[i], v2[i])
			if innerCmp != 0 {
				return innerCmp
			}
		}
		return 0
	case JsonValueString:
		v2 := v2.(JsonValueString)
		if v1 == v2 {
			return 0
		} else if v1 < v2 {
			return -1
		} else {
			return 1
		}
	case JsonValueNumber:
		return decimal.Decimal(v1).Cmp(decimal.Decimal(v2.(JsonValueNumber)))
	case JsonValueBoolean:
		v2 := v2.(JsonValueBoolean)
		if v1 == v2 {
			return 0
		} else if !v1 {
			return -1
		} else {
			return 1
		}
	case JsonValueNull:
		return 0
	default:
		return 0
	}
}

// JsonBContainsValue returns whether the container JSONB value contains the contained JSONB value.
func JsonBContainsValue(container JsonValue, contained JsonValue) bool {
	return jsonBContainsValue(container, contained, true)
}

// JsonValueDeleteKey returns a copy of value with the given object key or array string element removed.
func JsonValueDeleteKey(value JsonValue, key string) (JsonValue, error) {
	switch value := value.(type) {
	case JsonValueObject:
		return jsonValueObjectDeleteKeys(value, map[string]struct{}{key: {}}), nil
	case JsonValueArray:
		return jsonValueArrayDeleteStrings(value, map[string]struct{}{key: {}})
	default:
		return nil, errors.Errorf("cannot delete from scalar")
	}
}

// JsonValueDeleteKeys returns a copy of value with all matching object keys or array string elements removed.
func JsonValueDeleteKeys(value JsonValue, keys []string) (JsonValue, error) {
	keySet := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		keySet[key] = struct{}{}
	}
	switch value := value.(type) {
	case JsonValueObject:
		return jsonValueObjectDeleteKeys(value, keySet), nil
	case JsonValueArray:
		return jsonValueArrayDeleteStrings(value, keySet)
	default:
		return nil, errors.Errorf("cannot delete from scalar")
	}
}

// JsonValueDeleteIndex returns a copy of value with the array element at idx removed.
func JsonValueDeleteIndex(value JsonValue, idx int) (JsonValue, error) {
	switch value := value.(type) {
	case JsonValueArray:
		return jsonValueArrayDeleteIndex(value, idx), nil
	case JsonValueObject:
		return nil, errors.Errorf("cannot delete from object using integer index")
	default:
		return nil, errors.Errorf("cannot delete from scalar")
	}
}

// JsonValueDeletePath returns a copy of value with the item at path removed.
func JsonValueDeletePath(value JsonValue, path []string) (JsonValue, error) {
	switch value.(type) {
	case JsonValueObject, JsonValueArray:
		newValue, _, err := jsonValueDeletePath(value, path, 1)
		return newValue, err
	default:
		return nil, errors.Errorf("cannot delete path in scalar")
	}
}

// JsonValueStripNulls returns a copy of value with all object fields containing JSON null removed recursively.
func JsonValueStripNulls(value JsonValue) JsonValue {
	switch value := value.(type) {
	case JsonValueObject:
		items := make([]JsonValueObjectItem, 0, len(value.Items))
		for _, item := range value.Items {
			if _, ok := item.Value.(JsonValueNull); ok {
				continue
			}
			items = append(items, JsonValueObjectItem{
				Key:   item.Key,
				Value: JsonValueStripNulls(item.Value),
			})
		}
		return JsonObjectFromItems(items, false)
	case JsonValueArray:
		items := make(JsonValueArray, len(value))
		for i, item := range value {
			items[i] = JsonValueStripNulls(item)
		}
		return items
	default:
		return JsonValueCopy(value)
	}
}

func jsonValueObjectDeleteKeys(value JsonValueObject, keys map[string]struct{}) JsonValueObject {
	items := make([]JsonValueObjectItem, 0, len(value.Items))
	for _, item := range value.Items {
		if _, ok := keys[item.Key]; ok {
			continue
		}
		items = append(items, JsonValueObjectItem{
			Key:   item.Key,
			Value: JsonValueCopy(item.Value),
		})
	}
	return JsonObjectFromItems(items, false)
}

func jsonValueArrayDeleteStrings(value JsonValueArray, keys map[string]struct{}) (JsonValueArray, error) {
	items := make(JsonValueArray, 0, len(value))
	for _, item := range value {
		if str, ok := item.(JsonValueString); ok {
			decoded, err := JsonStringUnescape(str)
			if err != nil {
				return nil, err
			}
			if _, ok = keys[decoded]; ok {
				continue
			}
		}
		items = append(items, JsonValueCopy(item))
	}
	return items, nil
}

func jsonValueArrayDeleteIndex(value JsonValueArray, idx int) JsonValueArray {
	if idx < 0 {
		idx += len(value)
	}
	if idx < 0 || idx >= len(value) {
		return JsonValueCopy(value).(JsonValueArray)
	}
	items := make(JsonValueArray, 0, len(value)-1)
	for i, item := range value {
		if i == idx {
			continue
		}
		items = append(items, JsonValueCopy(item))
	}
	return items
}

func jsonValueDeletePath(value JsonValue, path []string, position int) (JsonValue, bool, error) {
	if len(path) == 0 {
		return JsonValueCopy(value), false, nil
	}
	switch value := value.(type) {
	case JsonValueObject:
		key := path[0]
		idx, ok := value.Index[key]
		if !ok {
			return JsonValueCopy(value), false, nil
		}
		if len(path) == 1 {
			return jsonValueObjectDeleteKeys(value, map[string]struct{}{key: {}}), true, nil
		}
		newChild, changed, err := jsonValueDeletePath(value.Items[idx].Value, path[1:], position+1)
		if err != nil {
			return nil, false, err
		}
		if !changed {
			return JsonValueCopy(value), false, nil
		}
		newObject := JsonValueCopy(value).(JsonValueObject)
		newObject.Items[idx].Value = newChild
		return newObject, true, nil
	case JsonValueArray:
		idx, err := strconv.Atoi(path[0])
		if err != nil {
			return nil, false, errors.Errorf("path element at position %d is not an integer: %s", position, path[0])
		}
		if len(path) == 1 {
			return jsonValueArrayDeleteIndex(value, idx), true, nil
		}
		if idx < 0 {
			idx += len(value)
		}
		if idx < 0 || idx >= len(value) {
			return JsonValueCopy(value), false, nil
		}
		newChild, changed, err := jsonValueDeletePath(value[idx], path[1:], position+1)
		if err != nil {
			return nil, false, err
		}
		if !changed {
			return JsonValueCopy(value), false, nil
		}
		newArray := JsonValueCopy(value).(JsonValueArray)
		newArray[idx] = newChild
		return newArray, true, nil
	default:
		return JsonValueCopy(value), false, nil
	}
}

func jsonBContainsValue(container JsonValue, contained JsonValue, allowArrayScalar bool) bool {
	switch contained := contained.(type) {
	case JsonValueObject:
		object, ok := container.(JsonValueObject)
		if !ok {
			return false
		}
		for _, containedItem := range contained.Items {
			idx, ok := object.Index[containedItem.Key]
			if !ok || !jsonBContainsValue(object.Items[idx].Value, containedItem.Value, false) {
				return false
			}
		}
		return true
	case JsonValueArray:
		array, ok := container.(JsonValueArray)
		if !ok {
			return false
		}
		for _, containedItem := range contained {
			found := false
			for _, containerItem := range array {
				if jsonBContainsArrayElement(containerItem, containedItem) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	default:
		if array, ok := container.(JsonValueArray); ok && allowArrayScalar {
			for _, item := range array {
				if JsonValueCompare(item, contained) == 0 {
					return true
				}
			}
			return false
		}
		return JsonValueCompare(container, contained) == 0
	}
}

func jsonBContainsArrayElement(containerItem JsonValue, containedItem JsonValue) bool {
	switch containedItem.(type) {
	case JsonValueArray, JsonValueObject:
		return jsonBContainsValue(containerItem, containedItem, true)
	default:
		return JsonValueCompare(containerItem, containedItem) == 0
	}
}

// jsonValueTypeSortOrder returns the relative sorting order based on the JsonValueType of the JsonValue. This should
// only be used from within jsonValueCompare. Lower values sort before larger values.
func jsonValueTypeSortOrder(value JsonValue) int {
	switch value.(type) {
	case JsonValueObject:
		return 5
	case JsonValueArray:
		return 4
	case JsonValueString:
		return 1
	case JsonValueNumber:
		return 2
	case JsonValueBoolean:
		return 3
	case JsonValueNull:
		return 0
	default:
		return 6
	}
}

// JsonValueSerialize is the recursive serializer for JSON values.
func JsonValueSerialize(writer *utils.Writer, value JsonValue) {
	switch value := value.(type) {
	case JsonValueObject:
		writer.Byte(byte(JsonValueType_Object))
		writer.VariableUint(uint64(len(value.Items)))
		for _, item := range value.Items {
			writer.String(item.Key)
			JsonValueSerialize(writer, item.Value)
		}
	case JsonValueArray:
		writer.Byte(byte(JsonValueType_Array))
		writer.VariableUint(uint64(len(value)))
		for _, item := range value {
			JsonValueSerialize(writer, item)
		}
	case JsonValueString:
		writer.Byte(byte(JsonValueType_String))
		writer.String(string(value))
	case JsonValueNumber:
		writer.Byte(byte(JsonValueType_Number))
		// MarshalBinary cannot error, so we can safely ignore it
		bytes, _ := decimal.Decimal(value).MarshalBinary()
		writer.ByteSlice(bytes)
	case JsonValueBoolean:
		writer.Byte(byte(JsonValueType_Boolean))
		writer.Bool(bool(value))
	case JsonValueNull:
		writer.Byte(byte(JsonValueType_Null))
	}
}

// JsonValueDeserialize is the recursive deserializer for JSON values.
func JsonValueDeserialize(reader *utils.Reader) (_ JsonValue, err error) {
	switch JsonValueType(reader.Byte()) {
	case JsonValueType_Object:
		items := make([]JsonValueObjectItem, reader.VariableUint())
		index := make(map[string]int)
		for i := range items {
			items[i].Key = reader.String()
			items[i].Value, err = JsonValueDeserialize(reader)
			if err != nil {
				return nil, err
			}
			index[items[i].Key] = i
		}
		return JsonValueObject{
			Items: items,
			Index: index,
		}, nil
	case JsonValueType_Array:
		values := make(JsonValueArray, reader.VariableUint())
		for i := range values {
			values[i], err = JsonValueDeserialize(reader)
			if err != nil {
				return nil, err
			}
		}
		return values, nil
	case JsonValueType_String:
		return JsonValueString(reader.String()), nil
	case JsonValueType_Number:
		d := decimal.Decimal{}
		err = d.UnmarshalBinary(reader.ByteSlice())
		return JsonValueNumber(d), err
	case JsonValueType_Boolean:
		return JsonValueBoolean(reader.Bool()), nil
	case JsonValueType_Null:
		return JsonValueNull(0), nil
	default:
		return nil, errors.Errorf("unknown json value type")
	}
}

// JsonValueFormatter is the recursive formatter for JSON values.
func JsonValueFormatter(sb *strings.Builder, value JsonValue) {
	switch value := value.(type) {
	case JsonValueObject:
		sb.WriteRune('{')
		for i, item := range value.Items {
			if i > 0 {
				sb.WriteString(", ")
			}
			writeJsonKeyString(sb, item.Key)
			sb.WriteString(`: `)
			JsonValueFormatter(sb, item.Value)
		}
		sb.WriteRune('}')
	case JsonValueArray:
		sb.WriteRune('[')
		for i, item := range value {
			if i > 0 {
				sb.WriteString(", ")
			}
			JsonValueFormatter(sb, item)
		}
		sb.WriteRune(']')
	case JsonValueString:
		writeJsonStoredString(sb, string(value))
	case JsonValueNumber:
		sb.WriteString(decimal.Decimal(value).String())
	case JsonValueBoolean:
		if value {
			sb.WriteString(`true`)
		} else {
			sb.WriteString(`false`)
		}
	case JsonValueNull:
		sb.WriteString(`null`)
	}
}

// JsonValueFormatterCompact is the recursive compact formatter for JSON values.
func JsonValueFormatterCompact(sb *strings.Builder, value JsonValue) {
	switch value := value.(type) {
	case JsonValueObject:
		sb.WriteRune('{')
		for i, item := range value.Items {
			if i > 0 {
				sb.WriteRune(',')
			}
			writeJsonKeyString(sb, item.Key)
			sb.WriteRune(':')
			JsonValueFormatterCompact(sb, item.Value)
		}
		sb.WriteRune('}')
	case JsonValueArray:
		sb.WriteRune('[')
		for i, item := range value {
			if i > 0 {
				sb.WriteRune(',')
			}
			JsonValueFormatterCompact(sb, item)
		}
		sb.WriteRune(']')
	case JsonValueString:
		writeJsonStoredString(sb, string(value))
	case JsonValueNumber:
		sb.WriteString(decimal.Decimal(value).String())
	case JsonValueBoolean:
		if value {
			sb.WriteString("true")
		} else {
			sb.WriteString("false")
		}
	case JsonValueNull:
		sb.WriteString("null")
	}
}

func writeJsonKeyString(sb *strings.Builder, value string) {
	bytes, _ := json.MarshalWithOption(value, json.DisableHTMLEscape())
	sb.Write(bytes)
}

func writeJsonStoredString(sb *strings.Builder, value string) {
	sb.WriteRune('"')
	sb.WriteString(strings.ReplaceAll(value, `"`, `\"`))
	sb.WriteRune('"')
}

// JsonStringUnescape returns the decoded text represented by a stored JSON string.
func JsonStringUnescape(value JsonValueString) (string, error) {
	sb := strings.Builder{}
	writeJsonStoredString(&sb, string(value))
	var decoded string
	if err := json.Unmarshal([]byte(sb.String()), &decoded); err != nil {
		return "", err
	}
	return decoded, nil
}

func jsonStringEscape(value string) string {
	sb := strings.Builder{}
	for _, r := range value {
		switch r {
		case '\\':
			sb.WriteString(`\\`)
		case '\b':
			sb.WriteString(`\b`)
		case '\f':
			sb.WriteString(`\f`)
		case '\n':
			sb.WriteString(`\n`)
		case '\r':
			sb.WriteString(`\r`)
		case '\t':
			sb.WriteString(`\t`)
		default:
			if r < 0x20 {
				sb.WriteString(fmt.Sprintf(`\u%04x`, r))
			} else {
				sb.WriteRune(r)
			}
		}
	}
	return sb.String()
}

func jsonParsedStringEscape(value string) string {
	value = jsonStringEscape(value)
	return jsonDocumentStringUnicodeRegex.ReplaceAllString(value, `\u$1`)
}

// JsonValueTypeName returns the PostgreSQL json/jsonb type name for the given JSON value.
func JsonValueTypeName(value JsonValue) string {
	switch value.(type) {
	case JsonValueObject:
		return "object"
	case JsonValueArray:
		return "array"
	case JsonValueString:
		return "string"
	case JsonValueNumber:
		return "number"
	case JsonValueBoolean:
		return "boolean"
	case JsonValueNull:
		return "null"
	default:
		return ""
	}
}

// JsonValueFromSQLValue converts a Doltgres SQL value to the logical JSON value used by json/jsonb functions.
func JsonValueFromSQLValue(ctx *sql.Context, typ *DoltgresType, val any) (JsonValue, error) {
	res, err := sql.UnwrapAny(ctx, val)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return JsonValueNull(0), nil
	}
	if doc, ok := res.(JsonDocument); ok {
		return JsonValueCopy(doc.Value), nil
	}
	if typ != nil {
		switch typ.ID.TypeName() {
		case "json", "jsonb":
			str, ok := res.(string)
			if ok {
				doc, err := UnmarshalToJsonDocument([]byte(str))
				if err != nil {
					return nil, err
				}
				return doc.Value, nil
			}
		}
		if typ.IsArrayType() {
			values, ok := res.([]any)
			if !ok {
				values, ok = res.([]interface{})
			}
			if ok {
				baseType := typ.ArrayBaseType()
				array := make(JsonValueArray, len(values))
				for i, value := range values {
					array[i], err = JsonValueFromSQLValue(ctx, baseType, value)
					if err != nil {
						return nil, err
					}
				}
				return array, nil
			}
		}
	}
	switch v := res.(type) {
	case string:
		return JsonValueString(jsonStringEscape(v)), nil
	case bool:
		return JsonValueBoolean(v), nil
	case int:
		return JsonValueNumber(decimal.NewFromInt(int64(v))), nil
	case int8:
		return JsonValueNumber(decimal.NewFromInt(int64(v))), nil
	case int16:
		return JsonValueNumber(decimal.NewFromInt(int64(v))), nil
	case int32:
		return JsonValueNumber(decimal.NewFromInt(int64(v))), nil
	case int64:
		return JsonValueNumber(decimal.NewFromInt(v)), nil
	case uint:
		return JsonValueNumber(decimal.NewFromUint64(uint64(v))), nil
	case uint8:
		return JsonValueNumber(decimal.NewFromUint64(uint64(v))), nil
	case uint16:
		return JsonValueNumber(decimal.NewFromUint64(uint64(v))), nil
	case uint32:
		return JsonValueNumber(decimal.NewFromUint64(uint64(v))), nil
	case uint64:
		return JsonValueNumber(decimal.NewFromUint64(v)), nil
	case float32:
		return JsonValueNumber(decimal.NewFromFloat32(v)), nil
	case float64:
		return JsonValueNumber(decimal.NewFromFloat(v)), nil
	case decimal.Decimal:
		return JsonValueNumber(v), nil
	case []any:
		array := make(JsonValueArray, len(v))
		for i, value := range v {
			array[i], err = JsonValueFromSQLValue(ctx, nil, value)
			if err != nil {
				return nil, err
			}
		}
		return array, nil
	case time.Time:
		return JsonValueString(v.Format("2006-01-02T15:04:05.999999Z07:00")), nil
	}
	if typ != nil {
		output, err := typ.IoOutput(ctx, res)
		if err != nil {
			return nil, err
		}
		return JsonValueString(jsonStringEscape(output)), nil
	}
	return JsonValueString(jsonStringEscape(fmt.Sprint(res))), nil
}

// JsonDocumentFromSQLValue converts a Doltgres SQL value to a JsonDocument.
func JsonDocumentFromSQLValue(ctx *sql.Context, typ *DoltgresType, val any) (JsonDocument, error) {
	value, err := JsonValueFromSQLValue(ctx, typ, val)
	if err != nil {
		return JsonDocument{}, err
	}
	return JsonDocument{Value: value}, nil
}

// JsonValueFormatPretty formats a JSON value with PostgreSQL-style indentation used by jsonb_pretty.
func JsonValueFormatPretty(sb *strings.Builder, value JsonValue, indent int) {
	indentString := func(n int) {
		for i := 0; i < n; i++ {
			sb.WriteByte(' ')
		}
	}
	switch value := value.(type) {
	case JsonValueObject:
		if len(value.Items) == 0 {
			sb.WriteString("{}")
			return
		}
		sb.WriteString("{\n")
		for i, item := range value.Items {
			if i > 0 {
				sb.WriteString(",\n")
			}
			indentString(indent + 4)
			writeJsonKeyString(sb, item.Key)
			sb.WriteString(`: `)
			JsonValueFormatPretty(sb, item.Value, indent+4)
		}
		sb.WriteByte('\n')
		indentString(indent)
		sb.WriteByte('}')
	case JsonValueArray:
		if len(value) == 0 {
			sb.WriteString("[]")
			return
		}
		sb.WriteString("[\n")
		for i, item := range value {
			if i > 0 {
				sb.WriteString(",\n")
			}
			indentString(indent + 4)
			JsonValueFormatPretty(sb, item, indent+4)
		}
		sb.WriteByte('\n')
		indentString(indent)
		sb.WriteByte(']')
	default:
		JsonValueFormatter(sb, value)
	}
}

// UnmarshalToJsonDocument converts a JSON document byte slice into the actual JSON document.
func UnmarshalToJsonDocument(val []byte) (JsonDocument, error) {
	var decoded interface{}
	if err := json.Unmarshal(val, &decoded); err != nil {
		return JsonDocument{}, err
	}
	jsonValue, err := ConvertToJsonDocument(decoded)
	if err != nil {
		return JsonDocument{}, err
	}
	return JsonDocument{Value: jsonValue}, nil
}

// ConvertToJsonDocument recursively constructs a valid JsonDocument based on the structures returned by the decoder.
func ConvertToJsonDocument(val interface{}) (JsonValue, error) {
	var err error
	switch val := val.(type) {
	case map[string]interface{}:
		keys := utils.GetMapKeys(val)
		sort.Slice(keys, func(i, j int) bool {
			// Key length is sorted before key contents
			if len(keys[i]) < len(keys[j]) {
				return true
			} else if len(keys[i]) > len(keys[j]) {
				return false
			} else {
				return keys[i] < keys[j]
			}
		})
		items := make([]JsonValueObjectItem, len(val))
		index := make(map[string]int)
		for i, key := range keys {
			items[i].Key = key
			items[i].Value, err = ConvertToJsonDocument(val[key])
			if err != nil {
				return nil, err
			}
			index[key] = i
		}
		return JsonValueObject{
			Items: items,
			Index: index,
		}, nil
	case []interface{}:
		values := make(JsonValueArray, len(val))
		for i, item := range val {
			values[i], err = ConvertToJsonDocument(item)
			if err != nil {
				return nil, err
			}
		}
		return values, nil
	case string:
		return JsonValueString(jsonParsedStringEscape(val)), nil
	case float64:
		// TODO: handle this as a proper numeric as float64 is not precise enough
		return JsonValueNumber(decimal.NewFromFloat(val)), nil
	case bool:
		return JsonValueBoolean(val), nil
	case nil:
		return JsonValueNull(0), nil
	default:
		return nil, errors.Errorf("unexpected type while constructing JsonDocument: %T", val)
	}
}
