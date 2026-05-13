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

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgtype"
	"github.com/shopspring/decimal"

	"github.com/dolthub/doltgresql/core/id"
)

const (
	MaxUint32 = 4294967295  // MaxUint32 is the largest possible value of Uint32
	MinInt32  = -2147483648 // MinInt32 is the smallest possible value of Int32
)

var (
	NumericValueMaxInt16  = decimal.NewFromInt(32767)                // NumericValueMaxInt16 is the max Int16 value for NUMERIC types
	NumericValueMaxInt32  = decimal.NewFromInt(2147483647)           // NumericValueMaxInt32 is the max Int32 value for NUMERIC types
	NumericValueMaxInt64  = decimal.NewFromInt(9223372036854775807)  // NumericValueMaxInt64 is the max Int64 value for NUMERIC types
	NumericValueMinInt16  = decimal.NewFromInt(-32768)               // NumericValueMinInt16 is the min Int16 value for NUMERIC types
	NumericValueMinInt32  = decimal.NewFromInt(MinInt32)             // NumericValueMinInt32 is the min Int32 value for NUMERIC types
	NumericValueMinInt64  = decimal.NewFromInt(-9223372036854775808) // NumericValueMinInt64 is the min Int64 value for NUMERIC types
	NumericValueMaxUint32 = decimal.NewFromInt(MaxUint32)            // NumericValueMaxUint32 is the max Uint32 value for NUMERIC types

	numericSpecialNaNValue              = decimal.New(1123581301, MinInt32)
	numericSpecialInfinityValue         = decimal.New(1123581302, MinInt32)
	numericSpecialNegativeInfinityValue = decimal.New(1123581303, MinInt32)
)

const (
	numericSpecialNaN byte = iota + 1
	numericSpecialInfinity
	numericSpecialNegativeInfinity
)

var numericSpecialSerializationPrefix = []byte{0x7f, 'D', 'G', 'N', 0}

// Numeric is a precise and unbounded decimal value.
var Numeric = &DoltgresType{
	ID:                  toInternal("numeric"),
	TypLength:           int16(-1),
	PassedByVal:         false,
	TypType:             TypeType_Base,
	TypCategory:         TypeCategory_NumericTypes,
	IsPreferred:         false,
	IsDefined:           true,
	Delimiter:           ",",
	RelID:               id.Null,
	SubscriptFunc:       toFuncID("-"),
	Elem:                id.NullType,
	Array:               toInternal("_numeric"),
	InputFunc:           toFuncID("numeric_in", toInternal("cstring"), toInternal("oid"), toInternal("int4")),
	OutputFunc:          toFuncID("numeric_out", toInternal("numeric")),
	ReceiveFunc:         toFuncID("numeric_recv", toInternal("internal"), toInternal("oid"), toInternal("int4")),
	SendFunc:            toFuncID("numeric_send", toInternal("numeric")),
	ModInFunc:           toFuncID("numerictypmodin", toInternal("_cstring")),
	ModOutFunc:          toFuncID("numerictypmodout", toInternal("int4")),
	AnalyzeFunc:         toFuncID("-"),
	Align:               TypeAlignment_Int,
	Storage:             TypeStorage_Main,
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
	CompareFunc:         toFuncID("numeric_cmp", toInternal("numeric"), toInternal("numeric")),
	SerializationFunc:   serializeTypeNumeric,
	DeserializationFunc: deserializeTypeNumeric,
}

// NewNumericTypeWithPrecisionAndScale returns Numeric type with typmod set.
func NewNumericTypeWithPrecisionAndScale(precision, scale int32) (*DoltgresType, error) {
	typmod, err := GetTypmodFromNumericPrecisionAndScale(precision, scale)
	if err != nil {
		return nil, err
	}
	newType := *Numeric.WithAttTypMod(typmod)
	return &newType, nil
}

// GetTypmodFromNumericPrecisionAndScale takes Numeric type precision and scale and returns the type modifier value.
func GetTypmodFromNumericPrecisionAndScale(precision, scale int32) (int32, error) {
	if precision < 1 || precision > 1000 {
		return 0, errors.Errorf("NUMERIC precision %v must be between 1 and 1000", precision)
	}
	if scale < -1000 || scale > 1000 {
		return 0, errors.Errorf("NUMERIC scale %v must be between -1000 and 1000", scale)
	}
	return ((precision << 16) | (scale & 0xFFFF)) + 4, nil
}

// GetPrecisionAndScaleFromTypmod takes Numeric type modifier and returns precision and scale values.
func GetPrecisionAndScaleFromTypmod(typmod int32) (int32, int32) {
	typmod -= 4
	scale := int32(int16(typmod & 0xFFFF))
	precision := (typmod >> 16) & 0xFFFF
	return precision, scale
}

// GetNumericValueWithTypmod returns either given numeric value or truncated or error
// depending on the precision and scale decoded from given type modifier value.
func GetNumericValueWithTypmod(val decimal.Decimal, typmod int32) (decimal.Decimal, error) {
	if typmod == -1 {
		return val, nil
	}
	precision, scale := GetPrecisionAndScaleFromTypmod(typmod)
	rounded := val.Round(scale)
	limit := decimal.New(1, precision-scale)
	if !rounded.IsZero() && rounded.Abs().GreaterThanOrEqual(limit) {
		// TODO: split error message to ERROR and DETAIL
		return decimal.Decimal{}, errors.Errorf("numeric field overflow - A field with precision %v, scale %v must round to an absolute value less than 10^%v", precision, scale, precision-scale)
	}
	return rounded, nil
}

// GetAnyNumericValueWithTypmod applies a numeric typmod to finite values while
// preserving PostgreSQL's special numeric values.
func GetAnyNumericValueWithTypmod(val any, typmod int32) (any, error) {
	if _, ok := NumericSpecialValueCode(val); ok {
		return val, nil
	}
	if numeric, ok := val.(pgtype.Numeric); ok && (numeric.NaN || numeric.InfinityModifier != pgtype.None) {
		special, _ := NumericSpecialValueFromPgtype(numeric)
		return special, nil
	}
	if typmod == -1 {
		return val, nil
	}
	dec, ok, err := NumericValueAsDecimal(val)
	if err != nil || !ok {
		return val, err
	}
	return GetNumericValueWithTypmod(dec, typmod)
}

// ParseNumericSpecialValue parses PostgreSQL numeric special input values.
func ParseNumericSpecialValue(input string) (decimal.Decimal, bool) {
	switch input {
	case "NaN", "nan", "NAN":
		return numericSpecialNaNValue, true
	case "Infinity", "+Infinity", "inf", "+inf", "INF", "+INF":
		return numericSpecialInfinityValue, true
	case "-Infinity", "-inf", "-INF":
		return numericSpecialNegativeInfinityValue, true
	default:
		return decimal.Decimal{}, false
	}
}

// NumericSpecialValueFromPgtype converts pgtype.Numeric special values to the internal representation.
func NumericSpecialValueFromPgtype(numeric pgtype.Numeric) (decimal.Decimal, bool) {
	if numeric.NaN {
		return numericSpecialNaNValue, true
	}
	switch numeric.InfinityModifier {
	case pgtype.Infinity:
		return numericSpecialInfinityValue, true
	case pgtype.NegativeInfinity:
		return numericSpecialNegativeInfinityValue, true
	default:
		return decimal.Decimal{}, false
	}
}

// NumericSpecialValueCode returns the special value code for internal numeric sentinel values.
func NumericSpecialValueCode(val any) (byte, bool) {
	dec, ok := val.(decimal.Decimal)
	if !ok {
		return 0, false
	}
	switch {
	case numericSentinelEqual(dec, numericSpecialNaNValue):
		return numericSpecialNaN, true
	case numericSentinelEqual(dec, numericSpecialInfinityValue):
		return numericSpecialInfinity, true
	case numericSentinelEqual(dec, numericSpecialNegativeInfinityValue):
		return numericSpecialNegativeInfinity, true
	default:
		return 0, false
	}
}

func numericSentinelEqual(left decimal.Decimal, right decimal.Decimal) bool {
	return left.Exponent() == right.Exponent() && left.Coefficient().Cmp(right.Coefficient()) == 0
}

// NumericValueAsDecimal returns the finite decimal representation of a numeric value.
func NumericValueAsDecimal(val any) (decimal.Decimal, bool, error) {
	switch v := val.(type) {
	case decimal.Decimal:
		if _, ok := NumericSpecialValueCode(v); ok {
			return decimal.Decimal{}, false, nil
		}
		return v, true, nil
	case pgtype.Numeric:
		if v.NaN || v.InfinityModifier != pgtype.None {
			return decimal.Decimal{}, false, nil
		}
		if v.Status != pgtype.Present {
			return decimal.Decimal{}, false, errors.Errorf("unexpected numeric status %v", v.Status)
		}
		if v.Int == nil {
			return decimal.Zero, true, nil
		}
		return decimal.NewFromBigInt(v.Int, v.Exp), true, nil
	default:
		return decimal.Decimal{}, false, errors.Errorf("unexpected numeric value %T", val)
	}
}

// FormatNumericValue returns PostgreSQL text output for finite and special numeric values.
func FormatNumericValue(val any, typmod int32) (string, error) {
	if code, ok := NumericSpecialValueCode(val); ok {
		switch code {
		case numericSpecialNaN:
			return "NaN", nil
		case numericSpecialInfinity:
			return "Infinity", nil
		case numericSpecialNegativeInfinity:
			return "-Infinity", nil
		}
	}
	switch v := val.(type) {
	case pgtype.Numeric:
		if v.NaN {
			return "NaN", nil
		}
		if v.InfinityModifier == pgtype.Infinity {
			return "Infinity", nil
		}
		if v.InfinityModifier == pgtype.NegativeInfinity {
			return "-Infinity", nil
		}
	}
	dec, ok, err := NumericValueAsDecimal(val)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", errors.Errorf("cannot format non-finite numeric value %T", val)
	}
	if typmod == -1 {
		return dec.StringFixed(dec.Exponent() * -1), nil
	}
	_, scale := GetPrecisionAndScaleFromTypmod(typmod)
	return dec.StringFixed(scale), nil
}

// NumericValueToPgtype converts any numeric value into pgtype.Numeric for wire encoding.
func NumericValueToPgtype(val any) (pgtype.Numeric, error) {
	if code, ok := NumericSpecialValueCode(val); ok {
		switch code {
		case numericSpecialNaN:
			return pgtype.Numeric{Status: pgtype.Present, NaN: true}, nil
		case numericSpecialInfinity:
			return pgtype.Numeric{Status: pgtype.Present, InfinityModifier: pgtype.Infinity}, nil
		case numericSpecialNegativeInfinity:
			return pgtype.Numeric{Status: pgtype.Present, InfinityModifier: pgtype.NegativeInfinity}, nil
		}
	}
	if numeric, ok := val.(pgtype.Numeric); ok {
		return numeric, nil
	}
	dec, ok, err := NumericValueAsDecimal(val)
	if err != nil {
		return pgtype.Numeric{}, err
	}
	if !ok {
		return pgtype.Numeric{}, errors.Errorf("cannot convert non-finite numeric value %T", val)
	}
	return pgtype.Numeric{Int: dec.Coefficient(), Exp: dec.Exponent(), Status: pgtype.Present}, nil
}

// CompareNumericValues compares finite and special numeric values with PostgreSQL ordering semantics.
func CompareNumericValues(v1 any, v2 any) (int, error) {
	rank1, dec1, err := numericSortRank(v1)
	if err != nil {
		return 0, err
	}
	rank2, dec2, err := numericSortRank(v2)
	if err != nil {
		return 0, err
	}
	if rank1 != rank2 {
		if rank1 < rank2 {
			return -1, nil
		}
		return 1, nil
	}
	if rank1 != 1 {
		return 0, nil
	}
	return dec1.Cmp(dec2), nil
}

func numericSortRank(val any) (int, decimal.Decimal, error) {
	switch v := val.(type) {
	case decimal.Decimal:
		if code, ok := NumericSpecialValueCode(v); ok {
			switch code {
			case numericSpecialNaN:
				return 3, decimal.Decimal{}, nil
			case numericSpecialInfinity:
				return 2, decimal.Decimal{}, nil
			case numericSpecialNegativeInfinity:
				return 0, decimal.Decimal{}, nil
			}
		}
		return 1, v, nil
	case pgtype.Numeric:
		if v.NaN {
			return 3, decimal.Decimal{}, nil
		}
		switch v.InfinityModifier {
		case pgtype.NegativeInfinity:
			return 0, decimal.Decimal{}, nil
		case pgtype.Infinity:
			return 2, decimal.Decimal{}, nil
		case pgtype.None:
			dec, ok, err := NumericValueAsDecimal(v)
			if err != nil || !ok {
				return 0, decimal.Decimal{}, err
			}
			return 1, dec, nil
		default:
			return 0, decimal.Decimal{}, errors.Errorf("unexpected numeric infinity modifier %v", v.InfinityModifier)
		}
	default:
		return 0, decimal.Decimal{}, errors.Errorf("unexpected numeric value %T", val)
	}
}

// serializeTypeNumeric handles serialization from the standard representation to our serialized representation that is
// written in Dolt.
func serializeTypeNumeric(ctx *sql.Context, t *DoltgresType, val any) ([]byte, error) {
	if code, ok := NumericSpecialValueCode(val); ok {
		return append(append([]byte{}, numericSpecialSerializationPrefix...), code), nil
	}
	if numeric, ok := val.(pgtype.Numeric); ok {
		switch {
		case numeric.NaN:
			return append(append([]byte{}, numericSpecialSerializationPrefix...), numericSpecialNaN), nil
		case numeric.InfinityModifier == pgtype.Infinity:
			return append(append([]byte{}, numericSpecialSerializationPrefix...), numericSpecialInfinity), nil
		case numeric.InfinityModifier == pgtype.NegativeInfinity:
			return append(append([]byte{}, numericSpecialSerializationPrefix...), numericSpecialNegativeInfinity), nil
		}
	}
	dec, ok, err := NumericValueAsDecimal(val)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.Errorf("cannot serialize non-finite numeric value %T", val)
	}
	return dec.MarshalBinary()
}

// deserializeTypeNumeric handles deserialization from the Dolt serialized format to our standard representation used by
// expressions and nodes.
func deserializeTypeNumeric(ctx *sql.Context, t *DoltgresType, data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if bytes.HasPrefix(data, numericSpecialSerializationPrefix) && len(data) == len(numericSpecialSerializationPrefix)+1 {
		switch data[len(numericSpecialSerializationPrefix)] {
		case numericSpecialNaN:
			return numericSpecialNaNValue, nil
		case numericSpecialInfinity:
			return numericSpecialInfinityValue, nil
		case numericSpecialNegativeInfinity:
			return numericSpecialNegativeInfinityValue, nil
		default:
			return nil, errors.Errorf("unknown numeric special serialization code %d", data[len(numericSpecialSerializationPrefix)])
		}
	}
	retVal := decimal.NewFromInt(0)
	err := retVal.UnmarshalBinary(data)
	return retVal, err
}
