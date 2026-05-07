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
	"errors"
	"fmt"
	"math"
	"math/big"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/shopspring/decimal"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

const (
	rowReferenceCurrentFormat     = 2
	rowReferenceKindHeaderSize    = 6
	rowReferenceOrderedHeaderSize = 8
	rowReferenceNullMarker        = 0
	rowReferenceNonNullMarker     = 1
	rowReferenceTerminatorByte    = 0
	rowReferenceEscapedNulByte    = 0xff

	rowReferenceNumericNegativeMarker = 0
	rowReferenceNumericZeroMarker     = 1
	rowReferenceNumericPositiveMarker = 2
)

var rowReferenceMagic = [4]byte{'D', 'G', 'R', 'F'}

// ErrUnsupportedRowReferenceType marks a primary-key type that cannot be
// encoded as an ordered, direct-fetchable row reference.
var ErrUnsupportedRowReferenceType = errors.New("unsupported ordered row-reference type")

// RowReferenceKind describes whether a row reference can be decoded back to a
// primary-key tuple for direct candidate fetch, or is an opaque identity that
// requires scan-and-recheck fallback.
type RowReferenceKind uint8

const (
	RowReferenceKindOrdered RowReferenceKind = 1
	RowReferenceKindOpaque  RowReferenceKind = 2
)

// RowReference is an ordered, fetchable reference to a base-table row.
type RowReference struct {
	FormatVersion uint8
	Kind          RowReferenceKind
	Values        sql.Row
	Identity      string
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

	encoded := make([]byte, rowReferenceOrderedHeaderSize)
	copy(encoded[:4], rowReferenceMagic[:])
	encoded[4] = rowReferenceCurrentFormat
	encoded[5] = byte(RowReferenceKindOrdered)
	binary.BigEndian.PutUint16(encoded[rowReferenceKindHeaderSize:rowReferenceOrderedHeaderSize], uint16(len(columnTypes)))

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
		FormatVersion: rowReferenceCurrentFormat,
		Kind:          RowReferenceKindOrdered,
		Values:        cloneRowReferenceValues(values),
		Bytes:         encoded,
	}, nil
}

// EncodeOpaqueRowReference encodes a deterministic row identity for table
// shapes whose primary key cannot be represented as an ordered row reference.
func EncodeOpaqueRowReference(identity string) (RowReference, error) {
	if identity == "" {
		return RowReference{}, fmt.Errorf("JSONB GIN opaque row reference requires a non-empty identity")
	}
	encoded := make([]byte, rowReferenceKindHeaderSize)
	copy(encoded[:4], rowReferenceMagic[:])
	encoded[4] = rowReferenceCurrentFormat
	encoded[5] = byte(RowReferenceKindOpaque)
	encoded = append(encoded, encodeComparableBytes([]byte(identity))...)
	return RowReference{
		FormatVersion: rowReferenceCurrentFormat,
		Kind:          RowReferenceKindOpaque,
		Identity:      identity,
		Bytes:         encoded,
	}, nil
}

// DecodeRowReference decodes a row reference using the primary-key column types
// for the table that owns the posting list.
func DecodeRowReference(ctx *sql.Context, columnTypes []sql.Type, encoded []byte) (RowReference, error) {
	if len(encoded) < rowReferenceKindHeaderSize {
		return RowReference{}, fmt.Errorf("malformed JSONB GIN row reference: too short")
	}
	if !bytes.Equal(encoded[:4], rowReferenceMagic[:]) {
		return RowReference{}, fmt.Errorf("malformed JSONB GIN row reference: invalid magic")
	}
	version := encoded[4]
	switch version {
	case rowReferenceCurrentFormat:
		return decodeRowReference(ctx, columnTypes, encoded)
	default:
		return RowReference{}, fmt.Errorf("unsupported JSONB GIN row reference version %d", version)
	}
}

func decodeRowReference(ctx *sql.Context, columnTypes []sql.Type, encoded []byte) (RowReference, error) {
	kind := RowReferenceKind(encoded[5])
	switch kind {
	case RowReferenceKindOrdered:
		if len(encoded) < rowReferenceOrderedHeaderSize {
			return RowReference{}, fmt.Errorf("malformed JSONB GIN row reference: too short")
		}
		componentCount := int(binary.BigEndian.Uint16(encoded[rowReferenceKindHeaderSize:rowReferenceOrderedHeaderSize]))
		return decodeOrderedRowReference(ctx, columnTypes, encoded, rowReferenceOrderedHeaderSize, componentCount, rowReferenceCurrentFormat)
	case RowReferenceKindOpaque:
		identity, offset, err := decodeComparableBytes(encoded, rowReferenceKindHeaderSize)
		if err != nil {
			return RowReference{}, fmt.Errorf("malformed JSONB GIN opaque row reference: %w", err)
		}
		if offset != len(encoded) {
			return RowReference{}, fmt.Errorf("malformed JSONB GIN row reference: trailing bytes after opaque identity")
		}
		return RowReference{
			FormatVersion: rowReferenceCurrentFormat,
			Kind:          RowReferenceKindOpaque,
			Identity:      string(identity),
			Bytes:         append([]byte(nil), encoded...),
		}, nil
	default:
		return RowReference{}, fmt.Errorf("unsupported JSONB GIN row reference kind %d", kind)
	}
}

func decodeOrderedRowReference(ctx *sql.Context, columnTypes []sql.Type, encoded []byte, offset int, componentCount int, version uint8) (RowReference, error) {
	if componentCount != len(columnTypes) {
		return RowReference{}, fmt.Errorf("malformed JSONB GIN row reference: component count %d does not match %d key types", componentCount, len(columnTypes))
	}

	values := make(sql.Row, componentCount)
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
		Kind:          RowReferenceKindOrdered,
		Values:        values,
		Bytes:         append([]byte(nil), encoded...),
	}, nil
}

// CompareRowReferences compares two row references using byte order.
func CompareRowReferences(left []byte, right []byte) int {
	return bytes.Compare(left, right)
}

// IsUnsupportedRowReferenceType reports whether err came from an ordered row
// reference type that should fall back to an opaque identity.
func IsUnsupportedRowReferenceType(err error) bool {
	return errors.Is(err, ErrUnsupportedRowReferenceType)
}

func encodeRowReferenceComponent(ctx *sql.Context, typ sql.Type, value any) ([]byte, error) {
	dgType, ok := typ.(*pgtypes.DoltgresType)
	if !ok {
		return nil, fmt.Errorf("%w %T", ErrUnsupportedRowReferenceType, typ)
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
	if isNumericRowReferenceType(dgType) {
		value, ok := value.(decimal.Decimal)
		if !ok {
			return nil, fmt.Errorf("unsupported numeric value type %T", value)
		}
		return encodeNumericRowReferenceComponent(value)
	}
	if _, ok := fixedWidthRowReferenceType(dgType); !ok {
		return nil, fmt.Errorf("%w %s", ErrUnsupportedRowReferenceType, dgType.ID.TypeName())
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
	if isNumericRowReferenceType(dgType) {
		return decodeNumericRowReferenceComponent(encoded, offset)
	}
	width, ok := fixedWidthRowReferenceType(dgType)
	if !ok {
		return nil, 0, fmt.Errorf("%w %s", ErrUnsupportedRowReferenceType, dgType.ID.TypeName())
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

func isNumericRowReferenceType(typ *pgtypes.DoltgresType) bool {
	return typ.ID.TypeName() == "numeric"
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
	case "float4":
		return 4, true
	case "float8":
		return 8, true
	case "uuid":
		return 16, true
	default:
		return 0, false
	}
}

func encodeNumericRowReferenceComponent(value decimal.Decimal) ([]byte, error) {
	sign := value.Sign()
	if sign == 0 {
		return []byte{rowReferenceNumericZeroMarker}, nil
	}

	magnitude := value.Abs()
	coefficient := magnitude.Coefficient()
	exponent := magnitude.Exponent()
	normalizeNumericCoefficient(coefficient, &exponent)

	digits := coefficient.String()
	adjustedExponent := int64(len(digits)) + int64(exponent) - 1
	if adjustedExponent < -1<<31 || adjustedExponent > 1<<31-1 {
		return nil, fmt.Errorf("%w numeric adjusted exponent %d", ErrUnsupportedRowReferenceType, adjustedExponent)
	}

	encoded := make([]byte, 1+4+len(digits)+1)
	if sign < 0 {
		encoded[0] = rowReferenceNumericNegativeMarker
	} else {
		encoded[0] = rowReferenceNumericPositiveMarker
	}
	binary.BigEndian.PutUint32(encoded[1:5], uint32(int32(adjustedExponent))^0x80000000)
	copy(encoded[5:], digits)
	encoded[len(encoded)-1] = rowReferenceTerminatorByte

	if sign < 0 {
		for i := 1; i < len(encoded); i++ {
			encoded[i] = ^encoded[i]
		}
	}
	return encoded, nil
}

func decodeNumericRowReferenceComponent(encoded []byte, offset int) (any, int, error) {
	if offset >= len(encoded) {
		return nil, 0, fmt.Errorf("truncated numeric component")
	}
	marker := encoded[offset]
	offset++
	switch marker {
	case rowReferenceNumericZeroMarker:
		return decimal.Zero, offset, nil
	case rowReferenceNumericPositiveMarker, rowReferenceNumericNegativeMarker:
	default:
		return nil, 0, fmt.Errorf("invalid numeric marker %d", marker)
	}
	if offset+4 >= len(encoded) {
		return nil, 0, fmt.Errorf("truncated numeric component")
	}

	negative := marker == rowReferenceNumericNegativeMarker
	terminator := byte(rowReferenceTerminatorByte)
	if negative {
		terminator = ^terminator
	}
	end := offset + 4
	for end < len(encoded) && encoded[end] != terminator {
		end++
	}
	if end >= len(encoded) {
		return nil, 0, fmt.Errorf("unterminated numeric component")
	}
	if end == offset+4 {
		return nil, 0, fmt.Errorf("numeric component missing coefficient digits")
	}

	sortKey := append([]byte(nil), encoded[offset:end]...)
	if negative {
		for i := range sortKey {
			sortKey[i] = ^sortKey[i]
		}
	}
	adjustedExponent := int32(binary.BigEndian.Uint32(sortKey[:4]) ^ 0x80000000)
	digits := sortKey[4:]
	for _, digit := range digits {
		if digit < '0' || digit > '9' {
			return nil, 0, fmt.Errorf("invalid numeric coefficient digit 0x%02x", digit)
		}
	}
	coefficient, ok := new(big.Int).SetString(string(digits), 10)
	if !ok {
		return nil, 0, fmt.Errorf("invalid numeric coefficient")
	}
	if negative {
		coefficient.Neg(coefficient)
	}
	exponent := adjustedExponent - int32(len(digits)) + 1
	return decimal.NewFromBigInt(coefficient, exponent), end + 1, nil
}

func normalizeNumericCoefficient(coefficient *big.Int, exponent *int32) {
	if coefficient.Sign() == 0 {
		*exponent = 0
		return
	}
	ten := big.NewInt(10)
	quotient := new(big.Int)
	remainder := new(big.Int)
	for {
		quotient.QuoRem(coefficient, ten, remainder)
		if remainder.Sign() != 0 {
			return
		}
		coefficient.Set(quotient)
		*exponent = *exponent + 1
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
