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
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
)

const (
	serializedArrayBoundsMarker = ^uint32(0)
	serializedArrayNestedMarker = ^uint32(1)
)

// ArrayValue carries PostgreSQL array bound metadata for arrays whose lower
// bounds are not the default one-based bounds.
type ArrayValue struct {
	Elements    []any
	LowerBounds []int32
}

// NewArrayValue returns a wrapped array only when metadata must be preserved.
func NewArrayValue(elements []any, lowerBounds []int32) any {
	if !ArrayHasNonDefaultLowerBounds(lowerBounds) {
		return elements
	}
	return ArrayValue{
		Elements:    elements,
		LowerBounds: append([]int32(nil), lowerBounds...),
	}
}

// ArrayElements unwraps both plain and bound-carrying array values.
func ArrayElements(value any) ([]any, bool) {
	switch v := value.(type) {
	case ArrayValue:
		return v.Elements, true
	case []any:
		return v, true
	default:
		return nil, false
	}
}

// ArrayLowerBounds returns explicit array lower bounds, if present.
func ArrayLowerBounds(value any) []int32 {
	if v, ok := value.(ArrayValue); ok {
		return v.LowerBounds
	}
	return nil
}

// ArrayHasNonDefaultLowerBounds reports whether lower bounds differ from PostgreSQL's default of 1.
func ArrayHasNonDefaultLowerBounds(lowerBounds []int32) bool {
	for _, lowerBound := range lowerBounds {
		if lowerBound != 1 {
			return true
		}
	}
	return false
}

// ArrayLowerBound returns the stored lower bound for a dimension, defaulting to 1.
func ArrayLowerBound(value any, dimension int32) int32 {
	if dimension <= 0 {
		return 1
	}
	if lowerBounds := ArrayLowerBounds(value); int(dimension) <= len(lowerBounds) {
		return lowerBounds[dimension-1]
	}
	return 1
}

// CreateArrayTypeFromBaseType create array type from given type.
func CreateArrayTypeFromBaseType(baseType *DoltgresType) *DoltgresType {
	align := TypeAlignment_Int
	if baseType.Align == TypeAlignment_Double {
		align = TypeAlignment_Double
	}
	return &DoltgresType{
		ID:                  baseType.Array,
		TypLength:           int16(-1),
		PassedByVal:         false,
		TypType:             TypeType_Base,
		TypCategory:         TypeCategory_ArrayTypes,
		IsPreferred:         false,
		IsDefined:           true,
		Delimiter:           ",",
		RelID:               id.Null,
		SubscriptFunc:       toFuncID("array_subscript_handler", toInternal("internal")),
		Elem:                baseType.ID,
		Array:               id.NullType,
		InputFunc:           toFuncID("array_in", toInternal("cstring"), toInternal("oid"), toInternal("int4")),
		OutputFunc:          toFuncID("array_out", toInternal("anyarray")),
		ReceiveFunc:         toFuncID("array_recv", toInternal("internal"), toInternal("oid"), toInternal("int4")),
		SendFunc:            toFuncID("array_send", toInternal("anyarray")),
		ModInFunc:           baseType.ModInFunc,
		ModOutFunc:          baseType.ModOutFunc,
		AnalyzeFunc:         toFuncID("array_typanalyze", toInternal("internal")),
		Align:               align,
		Storage:             TypeStorage_Extended,
		NotNull:             false,
		BaseTypeID:          id.NullType,
		TypMod:              -1,
		NDims:               0,
		TypCollation:        baseType.TypCollation,
		DefaulBin:           "",
		Default:             "",
		Acl:                 nil,
		Owner:               baseType.Owner,
		Checks:              nil,
		InternalName:        fmt.Sprintf("%s[]", baseType.Name()), // This will be set to the proper name in ToArrayType
		attTypMod:           baseType.attTypMod,                   // TODO: check
		CompareFunc:         toFuncID("btarraycmp", toInternal("anyarray"), toInternal("anyarray")),
		SerializationFunc:   serializeTypeArray,
		DeserializationFunc: deserializeTypeArray,
	}
}

// LogicalArrayElementTypes is a map of array element types for particular array types where the logical type varies
// from the declared type, as needed. Some types that have a NULL element for pg_catalog compatibility have a logical
// type that we need during analysis for function calls.
var LogicalArrayElementTypes = map[id.Type]*DoltgresType{
	toInternal("anyarray"): AnyElement,
}

// serializeTypeArray handles serialization from the standard representation to our serialized representation that is
// written in Dolt.
func serializeTypeArray(ctx *sql.Context, t *DoltgresType, val any) ([]byte, error) {
	baseType, err := t.ResolveArrayBaseType(ctx)
	if err != nil {
		return nil, err
	}
	vals, ok := ArrayElements(val)
	if !ok {
		return nil, errors.Errorf("expected array value but received %T", val)
	}
	data, err := serializeArray(ctx, vals, baseType)
	if err != nil {
		return nil, err
	}
	lowerBounds := ArrayLowerBounds(val)
	if !ArrayHasNonDefaultLowerBounds(lowerBounds) {
		return data, nil
	}
	var header [8]byte
	binary.LittleEndian.PutUint32(header[0:4], serializedArrayBoundsMarker)
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(lowerBounds)))
	output := bytes.NewBuffer(header[:])
	var bound [4]byte
	for _, lowerBound := range lowerBounds {
		binary.LittleEndian.PutUint32(bound[:], uint32(lowerBound))
		output.Write(bound[:])
	}
	output.Write(data)
	return output.Bytes(), nil
}

// deserializeTypeArray handles deserialization from the Dolt serialized format to our standard representation used by
// expressions and nodes.
func deserializeTypeArray(ctx *sql.Context, t *DoltgresType, data []byte) (any, error) {
	baseType, err := t.ResolveArrayBaseType(ctx)
	if err != nil {
		return nil, err
	}
	if len(data) >= 8 && binary.LittleEndian.Uint32(data[0:4]) == serializedArrayBoundsMarker {
		dimensionCount := binary.LittleEndian.Uint32(data[4:8])
		boundsEnd := 8 + int(dimensionCount)*4
		if len(data) < boundsEnd {
			return nil, errors.Errorf("deserializing array value has invalid bounds header")
		}
		lowerBounds := make([]int32, dimensionCount)
		for i := range lowerBounds {
			lowerBounds[i] = int32(binary.LittleEndian.Uint32(data[8+(i*4):]))
		}
		elements, err := deserializeArray(ctx, data[boundsEnd:], baseType)
		if err != nil {
			return nil, err
		}
		return NewArrayValue(elements, lowerBounds), nil
	}
	return deserializeArray(ctx, data, baseType)
}

// deserializeArray serializes an array of given base type.
func serializeArray(ctx *sql.Context, vals []any, baseType *DoltgresType) ([]byte, error) {
	if arrayContainsNestedValues(vals) {
		return serializeNestedArray(ctx, vals, baseType)
	}
	return serializeFlatArray(ctx, vals, baseType)
}

func arrayContainsNestedValues(vals []any) bool {
	for _, val := range vals {
		if val == nil {
			continue
		}
		if _, ok := ArrayElements(val); ok {
			return true
		}
	}
	return false
}

func serializeFlatArray(ctx *sql.Context, vals []any, baseType *DoltgresType) ([]byte, error) {
	bb := bytes.Buffer{}
	// Write the element count to a buffer. We're using an array since it's stack-allocated, so no need for pooling.
	var elementCount [4]byte
	binary.LittleEndian.PutUint32(elementCount[:], uint32(len(vals)))
	bb.Write(elementCount[:])
	// Create an array that contains the offsets for each value. Since we can't update the offset portion of the buffer
	// as we determine the offsets, we have to track them outside the buffer. We'll overwrite the buffer later with the
	// correct offsets. The last offset represents the end of the slice, which simplifies the logic for reading elements
	// using the "current offset to next offset" strategy. We use a byte slice since the buffer only works with byte
	// slices.
	offsets := make([]byte, (len(vals)+1)*4)
	bb.Write(offsets)
	// The starting offset for the first element is Count(uint32) + (NumberOfElementOffsets * sizeof(uint32))
	currentOffset := uint32(4 + (len(vals)+1)*4)
	for i := range vals {
		// Write the current offset
		binary.LittleEndian.PutUint32(offsets[i*4:], currentOffset)
		// Handle serialization of the value
		serializedVal, err := baseType.SerializeValue(ctx, vals[i])
		if err != nil {
			return nil, err
		}
		// Handle the nil case and non-nil case
		if serializedVal == nil {
			bb.WriteByte(1)
			currentOffset += 1
		} else {
			bb.WriteByte(0)
			bb.Write(serializedVal)
			currentOffset += 1 + uint32(len(serializedVal))
		}
	}
	// Write the final offset, which will equal the length of the serialized slice
	binary.LittleEndian.PutUint32(offsets[len(offsets)-4:], currentOffset)
	// Get the final output, and write the updated offsets to it
	outputBytes := bb.Bytes()
	copy(outputBytes[4:], offsets)
	return outputBytes, nil
}

func serializeNestedArray(ctx *sql.Context, vals []any, baseType *DoltgresType) ([]byte, error) {
	bb := bytes.Buffer{}
	var header [8]byte
	binary.LittleEndian.PutUint32(header[0:4], serializedArrayNestedMarker)
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(vals)))
	bb.Write(header[:])
	offsets := make([]byte, (len(vals)+1)*4)
	bb.Write(offsets)
	currentOffset := uint32(8 + (len(vals)+1)*4)
	for i, val := range vals {
		binary.LittleEndian.PutUint32(offsets[i*4:], currentOffset)
		if val == nil {
			bb.WriteByte(1)
			currentOffset++
			continue
		}
		var serializedVal []byte
		var err error
		if nested, ok := ArrayElements(val); ok {
			bb.WriteByte(2)
			serializedVal, err = serializeNestedArray(ctx, nested, baseType)
		} else {
			bb.WriteByte(0)
			serializedVal, err = baseType.SerializeValue(ctx, val)
		}
		if err != nil {
			return nil, err
		}
		bb.Write(serializedVal)
		currentOffset += 1 + uint32(len(serializedVal))
	}
	binary.LittleEndian.PutUint32(offsets[len(offsets)-4:], currentOffset)
	outputBytes := bb.Bytes()
	copy(outputBytes[8:], offsets)
	return outputBytes, nil
}

// deserializeArray deserializes an array of given base type.
func deserializeArray(ctx *sql.Context, data []byte, baseType *DoltgresType) ([]any, error) {
	// Check for the nil value, then ensure the minimum length of the slice
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) < 4 {
		return nil, errors.Errorf("deserializing non-nil array value has invalid length of %d", len(data))
	}
	if binary.LittleEndian.Uint32(data) == serializedArrayNestedMarker {
		return deserializeNestedArray(ctx, data, baseType)
	}
	// Grab the number of elements and construct an output slice of the appropriate size
	elementCount := binary.LittleEndian.Uint32(data)
	output := make([]any, elementCount)
	// Read all elements
	for i := uint32(0); i < elementCount; i++ {
		// We read from i+1 to account for the element count at the beginning
		offset := binary.LittleEndian.Uint32(data[(i+1)*4:])
		// If the value is null, then we can skip it, since the output slice default initializes all values to nil
		if data[offset] == 1 {
			continue
		}
		// The element data is everything from the offset to the next offset, excluding the null determinant
		nextOffset := binary.LittleEndian.Uint32(data[(i+2)*4:])
		o, err := baseType.DeserializeValue(ctx, data[offset+1:nextOffset])
		if err != nil {
			return nil, err
		}
		output[i] = o
	}
	// Returns all read elements
	return output, nil
}

func deserializeNestedArray(ctx *sql.Context, data []byte, baseType *DoltgresType) ([]any, error) {
	if len(data) < 8 {
		return nil, errors.Errorf("deserializing nested array value has invalid length of %d", len(data))
	}
	if binary.LittleEndian.Uint32(data[0:4]) != serializedArrayNestedMarker {
		return nil, errors.Errorf("deserializing nested array value has invalid marker")
	}
	elementCount := binary.LittleEndian.Uint32(data[4:8])
	offsetBase := 8
	offsetEnd := offsetBase + int(elementCount+1)*4
	if len(data) < offsetEnd {
		return nil, errors.Errorf("deserializing nested array value has invalid offsets")
	}
	output := make([]any, elementCount)
	for i := uint32(0); i < elementCount; i++ {
		offset := binary.LittleEndian.Uint32(data[offsetBase+int(i)*4:])
		nextOffset := binary.LittleEndian.Uint32(data[offsetBase+int(i+1)*4:])
		if offset >= nextOffset || int(nextOffset) > len(data) {
			return nil, errors.Errorf("deserializing nested array value has invalid element offsets")
		}
		tag := data[offset]
		payload := data[offset+1 : nextOffset]
		switch tag {
		case 0:
			val, err := baseType.DeserializeValue(ctx, payload)
			if err != nil {
				return nil, err
			}
			output[i] = val
		case 1:
			output[i] = nil
		case 2:
			nested, err := deserializeNestedArray(ctx, payload, baseType)
			if err != nil {
				return nil, err
			}
			output[i] = nested
		default:
			return nil, errors.Errorf("deserializing nested array value has invalid element tag %d", tag)
		}
	}
	return output, nil
}
