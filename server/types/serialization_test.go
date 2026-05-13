// Copyright 2024 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"fmt"
	"sync"
	"testing"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/stretchr/testify/require"
)

// TestSerializationConsistency checks that all types serialization and deserialization.
func TestSerializationConsistency(t *testing.T) {
	for _, typ := range GetAllBuitInTypes() {
		t.Run(typ.Name(), func(t *testing.T) {
			serializedType := typ.Serialize()
			dt, err := DeserializeType(serializedType)
			require.NoError(t, err)
			dgt := dt.(*DoltgresType)
			// require.Equal: Function equality cannot be determined and will always fail.
			typ.SerializationFunc = nil
			typ.DeserializationFunc = nil
			dgt.SerializationFunc = nil
			dgt.DeserializationFunc = nil
			require.Equal(t, typ, dgt)
		})
	}
}

func TestFunctionRegistryGrowsBeyondInitialCapacity(t *testing.T) {
	registry := functionRegistry{
		mutex:      &sync.Mutex{},
		counter:    1,
		mapping:    map[id.Function]uint32{id.NullFunction: 0},
		revMapping: map[uint32]id.Function{0: id.NullFunction},
		functions:  make([]QuickFunction, 1),
	}

	var lastRegistryID uint32
	var lastFunctionID id.Function
	for i := 0; i < initialFunctionRegistrySize+1; i++ {
		lastFunctionID = id.NewFunction("pg_catalog", fmt.Sprintf("test_registry_fn_%d", i))
		lastRegistryID = registry.InternalToRegistryID(lastFunctionID)
	}

	require.Equal(t, uint32(initialFunctionRegistrySize+1), lastRegistryID)
	require.Equal(t, lastFunctionID, registry.GetInternalID(lastRegistryID))
	require.Equal(t, "test_registry_fn_256", registry.GetString(lastRegistryID))
}

// TestJsonValueType operates as a line of defense to prevent accidental changes to JSON type values. If this test
// fails, then a JsonValueType was changed that should not have been changed.
func TestJsonValueType(t *testing.T) {
	types := []struct {
		JsonValueType
		Value byte
		Name  string
	}{
		{JsonValueType_Object, 0, "Object"},
		{JsonValueType_Array, 1, "Array"},
		{JsonValueType_String, 2, "String"},
		{JsonValueType_Number, 3, "Number"},
		{JsonValueType_Boolean, 4, "Boolean"},
		{JsonValueType_Null, 5, "Null"},
	}
	allValues := make(map[byte]string)
	for _, typ := range types {
		if byte(typ.JsonValueType) != typ.Value {
			t.Logf("JSON value type `%s` has been changed from its permanent value of `%d` to `%d`",
				typ.Name, typ.Value, byte(typ.JsonValueType))
			t.Fail()
		} else if existingName, ok := allValues[typ.Value]; ok {
			t.Logf("JSON value type `%s` has the same value as `%s`: `%d`",
				typ.Name, existingName, typ.Value)
			t.Fail()
		} else {
			allValues[typ.Value] = typ.Name
		}
	}
}

func TestJsonDocumentString(t *testing.T) {
	doc, err := UnmarshalToJsonDocument([]byte(`{"b":2,"a":[true,null,"x"]}`))
	require.NoError(t, err)
	require.Equal(t, `{"a": [true, null, "x"], "b": 2}`, doc.String())
	require.Equal(t, doc.String(), fmt.Sprint(doc))

	scalar, err := UnmarshalToJsonDocument([]byte(`2`))
	require.NoError(t, err)
	require.Equal(t, `2`, scalar.String())
	require.Equal(t, scalar.String(), fmt.Sprint(scalar))
}

func TestUnmarshalToJsonDocumentPreserveObjectItems(t *testing.T) {
	doc, err := UnmarshalToJsonDocumentPreserveObjectItems([]byte(`{"b":1,"a":2,"a":3}`))
	require.NoError(t, err)

	require.Equal(t, `{"b":1,"a":2,"a":3}`, doc.Value.(JsonValueRaw).Raw)
	object, ok := JsonValueUnwrapRaw(doc.Value).(JsonValueObject)
	require.True(t, ok)
	require.Len(t, object.Items, 3)
	require.Equal(t, []string{"b", "a", "a"}, []string{
		object.Items[0].Key,
		object.Items[1].Key,
		object.Items[2].Key,
	})
	require.Equal(t, `{"b": 1, "a": 2, "a": 3}`, doc.String())
}

func TestJsonStringUnescape(t *testing.T) {
	tests := []struct {
		name  string
		value JsonValueString
		want  string
	}{
		{name: "plain", value: JsonValueString("status"), want: "status"},
		{name: "quote", value: JsonValueString(`a"b`), want: `a"b`},
		{name: "backslash", value: JsonValueString(`a\\b`), want: `a\b`},
		{name: "newline", value: JsonValueString(`a\nb`), want: "a\nb"},
		{name: "unicode", value: JsonValueString(`\u263a`), want: "☺"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := JsonStringUnescape(test.value)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}
