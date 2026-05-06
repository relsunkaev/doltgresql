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

package jsonbgin

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/dolthub/go-mysql-server/sql"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// RowReferenceFormatVersionV1 is the initial ordered JSONB GIN row-reference
// encoding for primary-key tuple references stored inside posting chunks.
const RowReferenceFormatVersionV1 uint8 = 1

const (
	rowReferenceHeaderSize     = 7
	rowReferenceNullMarker     = 0
	rowReferenceNonNullMarker  = 1
	rowReferenceTerminatorByte = 0
	rowReferenceEscapedNulByte = 0xff
)

var rowReferenceMagic = [4]byte{'D', 'G', 'R', 'F'}

// RowReference is an ordered, fetchable reference to a base-table row.
type RowReference struct {
	FormatVersion uint8
	Values        sql.Row
	Bytes         []byte
}

// EncodePrimaryKeyRowReference encodes row's primary-key values in schema
// storage order. The boolean return is false when schema has no primary key.
func EncodePrimaryKeyRowReference(ctx *sql.Context, sch sql.Schema, row sql.Row) (RowReference, bool, error) {
	columnTypes := make([]sql.Type, 0)
	values := make(sql.Row, 0)
	for i, column := range sch {
		if !column.PrimaryKey {
			continue
		}
		if i >= len(row) {
			return RowReference{}, false, fmt.Errorf("JSONB GIN row reference cannot encode missing primary-key column %q", column.Name)
		}
		columnTypes = append(columnTypes, column.Type)
		values = append(values, row[i])
	}
	if len(columnTypes) == 0 {
		return RowReference{}, false, nil
	}
	rowRef, err := EncodeRowReference(ctx, columnTypes, values)
	return rowRef, true, err
}

// EncodeRowReference encodes a tuple of values into bytes whose lexicographic
// order matches the tuple order for the supported column types.
func EncodeRowReference(ctx *sql.Context, columnTypes []sql.Type, values sql.Row) (RowReference, error) {
	if len(columnTypes) != len(values) {
		return RowReference{}, fmt.Errorf("JSONB GIN row reference component count mismatch: %d types for %d values", len(columnTypes), len(values))
	}
	if len(columnTypes) > math.MaxUint16 {
		return RowReference{}, fmt.Errorf("JSONB GIN row reference component count %d exceeds maximum %d", len(columnTypes), uint64(math.MaxUint16))
	}

	encoded := make([]byte, rowReferenceHeaderSize)
	copy(encoded[:4], rowReferenceMagic[:])
	encoded[4] = RowReferenceFormatVersionV1
	binary.BigEndian.PutUint16(encoded[5:rowReferenceHeaderSize], uint16(len(columnTypes)))

	for i, typ := range columnTypes {
		value := values[i]
		if value == nil {
			encoded = append(encoded, rowReferenceNullMarker)
			continue
		}
		component, err := encodeRowReferenceComponent(ctx, typ, value)
		if err != nil {
			return RowReference{}, fmt.Errorf("JSONB GIN row reference component %d: %w", i, err)
		}
		encoded = append(encoded, rowReferenceNonNullMarker)
		encoded = append(encoded, component...)
	}

	return RowReference{
		FormatVersion: RowReferenceFormatVersionV1,
		Values:        cloneRowReferenceValues(values),
		Bytes:         encoded,
	}, nil
}

// DecodeRowReference decodes a row reference using the primary-key column types
// for the table that owns the posting list.
func DecodeRowReference(ctx *sql.Context, columnTypes []sql.Type, encoded []byte) (RowReference, error) {
	if len(encoded) < rowReferenceHeaderSize {
		return RowReference{}, fmt.Errorf("malformed JSONB GIN row reference: too short")
	}
	if !bytes.Equal(encoded[:4], rowReferenceMagic[:]) {
		return RowReference{}, fmt.Errorf("malformed JSONB GIN row reference: invalid magic")
	}
	version := encoded[4]
	switch version {
	case RowReferenceFormatVersionV1:
	default:
		return RowReference{}, fmt.Errorf("unsupported JSONB GIN row reference version %d", version)
	}

	componentCount := int(binary.BigEndian.Uint16(encoded[5:rowReferenceHeaderSize]))
	if componentCount != len(columnTypes) {
		return RowReference{}, fmt.Errorf("malformed JSONB GIN row reference: component count %d does not match %d key types", componentCount, len(columnTypes))
	}

	values := make(sql.Row, componentCount)
	offset := rowReferenceHeaderSize
	for i, typ := range columnTypes {
		if offset >= len(encoded) {
			return RowReference{}, fmt.Errorf("malformed JSONB GIN row reference: missing null marker for component %d", i)
		}
		marker := encoded[offset]
		offset++
		switch marker {
		case rowReferenceNullMarker:
			values[i] = nil
		case rowReferenceNonNullMarker:
			value, nextOffset, err := decodeRowReferenceComponent(ctx, typ, encoded, offset)
			if err != nil {
				return RowReference{}, fmt.Errorf("malformed JSONB GIN row reference component %d: %w", i, err)
			}
			values[i] = value
			offset = nextOffset
		default:
			return RowReference{}, fmt.Errorf("malformed JSONB GIN row reference: invalid null marker %d for component %d", marker, i)
		}
	}
	if offset != len(encoded) {
		return RowReference{}, fmt.Errorf("malformed JSONB GIN row reference: trailing bytes after components")
	}

	return RowReference{
		FormatVersion: version,
		Values:        values,
		Bytes:         append([]byte(nil), encoded...),
	}, nil
}

// CompareRowReferences compares two row references using byte order.
func CompareRowReferences(left []byte, right []byte) int {
	return bytes.Compare(left, right)
}

func encodeRowReferenceComponent(ctx *sql.Context, typ sql.Type, value any) ([]byte, error) {
	dgType, ok := typ.(*pgtypes.DoltgresType)
	if !ok {
		return nil, fmt.Errorf("unsupported row-reference type %T", typ)
	}

	if isStringRowReferenceType(dgType) {
		value, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("unsupported string value type %T", value)
		}
		return encodeComparableBytes([]byte(value)), nil
	}
	if isByteaRowReferenceType(dgType) {
		value, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("unsupported bytea value type %T", value)
		}
		return encodeComparableBytes(value), nil
	}
	if _, ok := fixedWidthRowReferenceType(dgType); !ok {
		return nil, fmt.Errorf("unsupported ordered row-reference type %s", dgType.ID.TypeName())
	}
	encoded, err := dgType.SerializeValue(ctx, value)
	if err != nil {
		return nil, err
	}
	if len(encoded) == 0 {
		return nil, fmt.Errorf("non-null row-reference value serialized to empty bytes")
	}
	return encoded, nil
}

func decodeRowReferenceComponent(ctx *sql.Context, typ sql.Type, encoded []byte, offset int) (any, int, error) {
	dgType, ok := typ.(*pgtypes.DoltgresType)
	if !ok {
		return nil, 0, fmt.Errorf("unsupported row-reference type %T", typ)
	}

	if isStringRowReferenceType(dgType) {
		decoded, nextOffset, err := decodeComparableBytes(encoded, offset)
		if err != nil {
			return nil, 0, err
		}
		return string(decoded), nextOffset, nil
	}
	if isByteaRowReferenceType(dgType) {
		decoded, nextOffset, err := decodeComparableBytes(encoded, offset)
		if err != nil {
			return nil, 0, err
		}
		return decoded, nextOffset, nil
	}
	width, ok := fixedWidthRowReferenceType(dgType)
	if !ok {
		return nil, 0, fmt.Errorf("unsupported ordered row-reference type %s", dgType.ID.TypeName())
	}
	if offset+width > len(encoded) {
		return nil, 0, fmt.Errorf("truncated fixed-width component")
	}
	value, err := dgType.DeserializeValue(ctx, encoded[offset:offset+width])
	if err != nil {
		return nil, 0, err
	}
	return value, offset + width, nil
}

func isStringRowReferenceType(typ *pgtypes.DoltgresType) bool {
	return typ.TypCategory == pgtypes.TypeCategory_StringTypes
}

func isByteaRowReferenceType(typ *pgtypes.DoltgresType) bool {
	return typ.ID.TypeName() == "bytea"
}

func fixedWidthRowReferenceType(typ *pgtypes.DoltgresType) (int, bool) {
	switch typ.ID.TypeName() {
	case "bool":
		return 1, true
	case "int2", "smallserial":
		return 2, true
	case "int4", "serial":
		return 4, true
	case "int8", "bigserial":
		return 8, true
	default:
		return 0, false
	}
}

func encodeComparableBytes(value []byte) []byte {
	encoded := make([]byte, 0, len(value)+2)
	for _, b := range value {
		if b == rowReferenceTerminatorByte {
			encoded = append(encoded, rowReferenceTerminatorByte, rowReferenceEscapedNulByte)
			continue
		}
		encoded = append(encoded, b)
	}
	return append(encoded, rowReferenceTerminatorByte, rowReferenceTerminatorByte)
}

func decodeComparableBytes(encoded []byte, offset int) ([]byte, int, error) {
	decoded := make([]byte, 0)
	for offset < len(encoded) {
		b := encoded[offset]
		offset++
		if b != rowReferenceTerminatorByte {
			decoded = append(decoded, b)
			continue
		}
		if offset >= len(encoded) {
			return nil, 0, fmt.Errorf("unterminated comparable byte sequence")
		}
		next := encoded[offset]
		offset++
		switch next {
		case rowReferenceTerminatorByte:
			return decoded, offset, nil
		case rowReferenceEscapedNulByte:
			decoded = append(decoded, rowReferenceTerminatorByte)
		default:
			return nil, 0, fmt.Errorf("invalid comparable byte escape 0x%02x", next)
		}
	}
	return nil, 0, fmt.Errorf("unterminated comparable byte sequence")
}

func cloneRowReferenceValues(values sql.Row) sql.Row {
	if len(values) == 0 {
		return nil
	}
	copied := make(sql.Row, len(values))
	for i, value := range values {
		if bytesValue, ok := value.([]byte); ok {
			copied[i] = append([]byte(nil), bytesValue...)
			continue
		}
		copied[i] = value
	}
	return copied
}
