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

package types

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
)

// MaxVectorDimensions is the maximum dimension count accepted by pgvector.
const MaxVectorDimensions = 16000

func newPgvectorUnsupportedType(typeName string) *DoltgresType {
	return &DoltgresType{
		ID:                  toInternal(typeName),
		TypLength:           int16(-1),
		PassedByVal:         false,
		TypType:             TypeType_Base,
		TypCategory:         TypeCategory_UserDefinedTypes,
		IsPreferred:         false,
		IsDefined:           true,
		Delimiter:           ",",
		RelID:               id.Null,
		SubscriptFunc:       toFuncID("-"),
		Elem:                id.NullType,
		Array:               toInternal("_" + typeName),
		InputFunc:           toFuncID(typeName+"_in", toInternal("cstring"), toInternal("oid"), toInternal("int4")),
		OutputFunc:          toFuncID(typeName+"_out", toInternal(typeName)),
		ReceiveFunc:         toFuncID(typeName+"_recv", toInternal("internal"), toInternal("oid"), toInternal("int4")),
		SendFunc:            toFuncID(typeName+"_send", toInternal(typeName)),
		ModInFunc:           toFuncID(typeName+"_typmod_in", toInternal("_cstring")),
		ModOutFunc:          toFuncID(typeName+"_typmod_out", toInternal("int4")),
		AnalyzeFunc:         toFuncID("-"),
		Align:               TypeAlignment_Int,
		Storage:             TypeStorage_External,
		NotNull:             false,
		BaseTypeID:          id.NullType,
		TypMod:              -1,
		NDims:               0,
		TypCollation:        id.NullCollation,
		DefaulBin:           "",
		Default:             "",
		Acl:                 nil,
		Checks:              nil,
		attTypMod:           -1,
		CompareFunc:         toFuncID("-"),
		SerializationFunc:   nil,
		DeserializationFunc: nil,
	}
}

// Vector is a pgvector-compatible float4 vector. Doltgres provides the scalar
// storage, text IO, binary IO, and equality surface needed by PgDog shard keys.
var Vector = &DoltgresType{
	ID:                  toInternal("vector"),
	TypLength:           int16(-1),
	PassedByVal:         false,
	TypType:             TypeType_Base,
	TypCategory:         TypeCategory_UserDefinedTypes,
	IsPreferred:         false,
	IsDefined:           true,
	Delimiter:           ",",
	RelID:               id.Null,
	SubscriptFunc:       toFuncID("-"),
	Elem:                id.NullType,
	Array:               toInternal("_vector"),
	InputFunc:           toFuncID("vector_in", toInternal("cstring"), toInternal("oid"), toInternal("int4")),
	OutputFunc:          toFuncID("vector_out", toInternal("vector")),
	ReceiveFunc:         toFuncID("vector_recv", toInternal("internal"), toInternal("oid"), toInternal("int4")),
	SendFunc:            toFuncID("vector_send", toInternal("vector")),
	ModInFunc:           toFuncID("vector_typmod_in", toInternal("_cstring")),
	ModOutFunc:          toFuncID("vector_typmod_out", toInternal("int4")),
	AnalyzeFunc:         toFuncID("-"),
	Align:               TypeAlignment_Int,
	Storage:             TypeStorage_External,
	NotNull:             false,
	BaseTypeID:          id.NullType,
	TypMod:              -1,
	NDims:               0,
	TypCollation:        id.NullCollation,
	DefaulBin:           "",
	Default:             "",
	Acl:                 nil,
	Checks:              nil,
	attTypMod:           -1,
	CompareFunc:         toFuncID("vector_cmp", toInternal("vector"), toInternal("vector")),
	SerializationFunc:   serializeTypeVector,
	DeserializationFunc: deserializeTypeVector,
}

// Halfvec is a pgvector-compatible type shell. Doltgres accepts schema
// declarations for restore/introspection but does not yet implement values.
var Halfvec = newPgvectorUnsupportedType("halfvec")

// Sparsevec is a pgvector-compatible type shell. Doltgres accepts schema
// declarations for restore/introspection but does not yet implement values.
var Sparsevec = newPgvectorUnsupportedType("sparsevec")

// GetTypmodFromVectorDimensions returns the type modifier for a vector dimension count.
func GetTypmodFromVectorDimensions(dimensions int32) (int32, error) {
	return getTypmodFromPgvectorDimensions("vector", dimensions)
}

// GetTypmodFromHalfvecDimensions returns the type modifier for a halfvec dimension count.
func GetTypmodFromHalfvecDimensions(dimensions int32) (int32, error) {
	return getTypmodFromPgvectorDimensions("halfvec", dimensions)
}

// GetTypmodFromSparsevecDimensions returns the type modifier for a sparsevec dimension count.
func GetTypmodFromSparsevecDimensions(dimensions int32) (int32, error) {
	return getTypmodFromPgvectorDimensions("sparsevec", dimensions)
}

func getTypmodFromPgvectorDimensions(typeName string, dimensions int32) (int32, error) {
	if dimensions < 1 || dimensions > MaxVectorDimensions {
		return 0, errors.Errorf("dimensions for type %s must be between 1 and %d", typeName, MaxVectorDimensions)
	}
	return dimensions, nil
}

// NewVectorType returns a Vector type with a fixed dimension typmod.
func NewVectorType(dimensions int32) (*DoltgresType, error) {
	typmod, err := GetTypmodFromVectorDimensions(dimensions)
	if err != nil {
		return nil, err
	}
	newType := *Vector.WithAttTypMod(typmod)
	return &newType, nil
}

// ParseVector converts pgvector text input into Doltgres' in-memory representation.
func ParseVector(input string, typmod int32) ([]float32, error) {
	trimmed := strings.TrimSpace(input)
	if len(trimmed) < 3 || trimmed[0] != '[' || trimmed[len(trimmed)-1] != ']' {
		return nil, ErrInvalidSyntaxForType.New("vector", input)
	}

	inner := strings.TrimSpace(trimmed[1 : len(trimmed)-1])
	if inner == "" {
		return nil, ErrInvalidSyntaxForType.New("vector", input)
	}

	parts := strings.Split(inner, ",")
	values := make([]float32, len(parts))
	for i, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, ErrInvalidSyntaxForType.New("vector", input)
		}
		parsed, err := strconv.ParseFloat(part, 32)
		if err != nil {
			return nil, ErrInvalidSyntaxForType.New("vector", input)
		}
		if math.IsNaN(parsed) || math.IsInf(parsed, 0) {
			return nil, ErrInvalidSyntaxForType.New("vector", input)
		}
		values[i] = float32(parsed)
	}

	if err := ValidateVectorDimensions(values, typmod); err != nil {
		return nil, err
	}
	return values, nil
}

// FormatVector converts a vector value to pgvector's canonical bracketed text form.
func FormatVector(values []float32) string {
	var sb strings.Builder
	sb.WriteByte('[')
	for i, value := range values {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(strconv.FormatFloat(float64(value), 'g', -1, 32))
	}
	sb.WriteByte(']')
	return sb.String()
}

// ValidateVectorDimensions checks both pgvector's global maximum and an optional vector(n) typmod.
func ValidateVectorDimensions(values []float32, typmod int32) error {
	if len(values) == 0 {
		return ErrInvalidSyntaxForType.New("vector", "[]")
	}
	if len(values) > MaxVectorDimensions {
		return errors.Errorf("dimensions for type vector must be between 1 and %d", MaxVectorDimensions)
	}
	if typmod != -1 && len(values) != int(typmod) {
		return errors.Errorf("expected %d dimensions, not %d", typmod, len(values))
	}
	return nil
}

// ValidateVectorElements checks pgvector's element-level finite-value boundary.
func ValidateVectorElements(values []float32) error {
	for _, value := range values {
		if math.IsNaN(float64(value)) {
			return errors.Errorf("NaN not allowed in vector")
		}
		if math.IsInf(float64(value), 0) {
			return errors.Errorf("infinite value not allowed in vector")
		}
	}
	return nil
}

// VectorFromArrayValues converts a one-dimensional PostgreSQL array value to a dense vector.
func VectorFromArrayValues(values []any, typmod int32, convert func(any) (float32, error)) ([]float32, error) {
	dimensions := len(values)
	if dimensions < 1 {
		return nil, errors.Errorf("vector must have at least 1 dimension")
	}
	if dimensions > MaxVectorDimensions {
		return nil, errors.Errorf("vector cannot have more than %d dimensions", MaxVectorDimensions)
	}
	if typmod != -1 && dimensions != int(typmod) {
		return nil, errors.Errorf("expected %d dimensions, not %d", typmod, dimensions)
	}
	result := make([]float32, dimensions)
	for i, value := range values {
		if value == nil {
			return nil, errors.Errorf("array must not contain nulls")
		}
		converted, err := convert(value)
		if err != nil {
			return nil, err
		}
		result[i] = converted
	}
	if err := ValidateVectorElements(result); err != nil {
		return nil, err
	}
	return result, nil
}

// VectorToFloat32Array converts a dense vector to a PostgreSQL real[] value.
func VectorToFloat32Array(values []float32) []any {
	result := make([]any, len(values))
	for i, value := range values {
		result[i] = value
	}
	return result
}

// CompareVectors lexicographically compares two vectors. This supports Dolt storage keys and equality operators.
func CompareVectors(left, right []float32) int {
	minLength := len(left)
	if len(right) < minLength {
		minLength = len(right)
	}
	for i := 0; i < minLength; i++ {
		if left[i] < right[i] {
			return -1
		}
		if left[i] > right[i] {
			return 1
		}
	}
	if len(left) < len(right) {
		return -1
	}
	if len(left) > len(right) {
		return 1
	}
	return 0
}

// serializeTypeVector handles serialization from the standard representation to our serialized representation that is
// written in Dolt.
func serializeTypeVector(ctx *sql.Context, t *DoltgresType, val any) ([]byte, error) {
	values := val.([]float32)
	if err := ValidateVectorDimensions(values, t.attTypMod); err != nil {
		return nil, err
	}
	output := make([]byte, 4+(len(values)*4))
	binary.BigEndian.PutUint32(output, uint32(len(values)))
	for i, value := range values {
		binary.BigEndian.PutUint32(output[4+(i*4):], math.Float32bits(value))
	}
	return output, nil
}

// deserializeTypeVector handles deserialization from the Dolt serialized format to our standard representation used by
// expressions and nodes.
func deserializeTypeVector(ctx *sql.Context, t *DoltgresType, data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) < 4 {
		return nil, errors.Errorf("deserializing non-nil vector value has invalid length of %d", len(data))
	}
	dimensions := int(binary.BigEndian.Uint32(data))
	if len(data) != 4+(dimensions*4) {
		return nil, errors.Errorf("deserializing vector value has invalid length of %d for %d dimensions", len(data), dimensions)
	}
	values := make([]float32, dimensions)
	for i := range values {
		values[i] = math.Float32frombits(binary.BigEndian.Uint32(data[4+(i*4):]))
	}
	if err := ValidateVectorDimensions(values, t.attTypMod); err != nil {
		return nil, err
	}
	return values, nil
}

// VectorTypmodOut returns vector(n)'s typmod suffix.
func VectorTypmodOut(typmod int32) string {
	if typmod == -1 {
		return ""
	}
	return fmt.Sprintf("(%d)", typmod)
}
