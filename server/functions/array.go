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
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
	"github.com/dolthub/doltgresql/utils"
)

// initArray registers the functions to the catalog.
func initArray() {
	framework.RegisterFunction(array_in)
	framework.RegisterFunction(array_out)
	framework.RegisterFunction(array_recv)
	framework.RegisterFunction(array_send)
	framework.RegisterFunction(btarraycmp)
	framework.RegisterFunction(array_subscript_handler)
}

// array_in represents the PostgreSQL function of array type IO input.
var array_in = framework.Function3{
	Name:       "array_in",
	Return:     pgtypes.AnyArray,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Cstring, pgtypes.Oid, pgtypes.Int32},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, val1, val2, val3 any) (any, error) {
		input := val1.(string)
		baseTypeOid := val2.(id.Id)
		baseType := pgtypes.IDToBuiltInDoltgresType[id.Type(baseTypeOid)]
		typmod := val3.(int32)
		baseType = baseType.WithAttTypMod(typmod)
		return parseArrayInput(ctx, input, baseType)
	},
}

func parseArrayInput(ctx *sql.Context, input string, baseType *pgtypes.DoltgresType) (any, error) {
	lowerBounds, expectedDimensions, literal, hasBounds, err := parseArrayBoundsPrefix(input)
	if err != nil {
		return nil, err
	}
	values, err := parseArrayLiteral(ctx, literal, baseType)
	if err != nil {
		return nil, err
	}
	if hasBounds {
		dimensions := arrayDimensions(values)
		if len(expectedDimensions) > len(dimensions) {
			return nil, errors.Errorf(`malformed array literal: "%s"`, input)
		}
		for i, expected := range expectedDimensions {
			if dimensions[i] != expected {
				return nil, errors.Errorf(`malformed array literal: "%s"`, input)
			}
		}
		return pgtypes.NewArrayValue(values, lowerBounds), nil
	}
	return values, nil
}

func parseArrayBoundsPrefix(input string) ([]int32, []int32, string, bool, error) {
	if !strings.HasPrefix(input, "[") {
		return nil, nil, input, false, nil
	}
	pos := 0
	var lowerBounds []int32
	var dimensions []int32
	for pos < len(input) && input[pos] == '[' {
		end := strings.IndexByte(input[pos:], ']')
		if end < 0 {
			return nil, nil, "", false, errors.Errorf(`malformed array literal: "%s"`, input)
		}
		spec := input[pos+1 : pos+end]
		parts := strings.Split(spec, ":")
		if len(parts) != 2 {
			return nil, nil, "", false, errors.Errorf(`malformed array literal: "%s"`, input)
		}
		lower, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 32)
		if err != nil {
			return nil, nil, "", false, errors.Errorf(`malformed array literal: "%s"`, input)
		}
		upper, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 32)
		if err != nil {
			return nil, nil, "", false, errors.Errorf(`malformed array literal: "%s"`, input)
		}
		if upper < lower {
			return nil, nil, "", false, errors.Errorf(`malformed array literal: "%s"`, input)
		}
		lowerBounds = append(lowerBounds, int32(lower))
		dimensions = append(dimensions, int32(upper-lower+1))
		pos += end + 1
	}
	if pos >= len(input) || input[pos] != '=' {
		return nil, nil, "", false, errors.Errorf(`malformed array literal: "%s"`, input)
	}
	return lowerBounds, dimensions, input[pos+1:], true, nil
}

func parseArrayLiteral(ctx *sql.Context, input string, baseType *pgtypes.DoltgresType) ([]any, error) {
	if len(input) < 2 || input[0] != '{' || input[len(input)-1] != '}' {
		return nil, errors.Errorf(`malformed array literal: "%s"`, input)
	}
	input = input[1 : len(input)-1]
	if len(input) == 0 {
		return []any{}, nil
	}

	var values []any
	var err error
	sb := strings.Builder{}
	braceDepth := 0
	inQuotes := false
	tokenQuoted := false
	escaped := false
	for _, r := range input {
		if escaped {
			sb.WriteRune(r)
			escaped = false
			continue
		}
		if inQuotes {
			switch r {
			case '\\':
				if braceDepth > 0 {
					sb.WriteRune(r)
				}
				escaped = true
			case '"':
				if braceDepth > 0 {
					sb.WriteRune(r)
				}
				inQuotes = false
			default:
				sb.WriteRune(r)
			}
			continue
		}
		switch r {
		case ' ', '\t', '\n', '\r':
			continue
		case '\\':
			escaped = true
		case '"':
			inQuotes = true
			if braceDepth == 0 {
				tokenQuoted = true
			} else {
				sb.WriteRune(r)
			}
		case '{':
			braceDepth++
			sb.WriteRune(r)
		case '}':
			if braceDepth == 0 {
				return nil, errors.Errorf(`malformed array literal: "%s"`, input)
			}
			braceDepth--
			sb.WriteRune(r)
		case ',':
			if braceDepth > 0 {
				sb.WriteRune(r)
				continue
			}
			innerValue, nErr := parseArrayValue(ctx, sb.String(), tokenQuoted, baseType)
			if nErr != nil && err == nil {
				err = nErr
			}
			values = append(values, innerValue)
			sb.Reset()
			tokenQuoted = false
		default:
			sb.WriteRune(r)
		}
	}
	if escaped || inQuotes || braceDepth != 0 {
		return nil, errors.Errorf(`malformed array literal: "%s"`, input)
	}
	innerValue, nErr := parseArrayValue(ctx, sb.String(), tokenQuoted, baseType)
	if nErr != nil && err == nil {
		err = nErr
	}
	values = append(values, innerValue)
	return values, err
}

func parseArrayValue(ctx *sql.Context, value string, quoted bool, baseType *pgtypes.DoltgresType) (any, error) {
	if !quoted && strings.EqualFold(value, "null") {
		return nil, nil
	}
	if !quoted && strings.HasPrefix(value, "{") {
		return parseArrayLiteral(ctx, value, baseType)
	}
	return baseType.IoInput(ctx, value)
}

// array_out represents the PostgreSQL function of array type IO output.
var array_out = framework.Function1{
	Name:       "array_out",
	Return:     pgtypes.Cstring,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.AnyArray},
	Strict:     true,
	Callable: func(ctx *sql.Context, t [2]*pgtypes.DoltgresType, val any) (any, error) {
		arrType := t[0]
		baseType, err := arrType.ResolveArrayBaseType(ctx)
		if err != nil {
			return nil, err
		}
		return pgtypes.ArrayToString(ctx, val, baseType, false)
	},
}

// array_recv represents the PostgreSQL function of array type IO receive.
var array_recv = framework.Function3{
	Name:       "array_recv",
	Return:     pgtypes.AnyArray,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Internal, pgtypes.Oid, pgtypes.Int32},
	Strict:     true,
	Callable:   array_recv_callable,
}

// array_recv_callable is the function definition of array_recv.
func array_recv_callable(ctx *sql.Context, t [4]*pgtypes.DoltgresType, val1, val2, val3 any) (any, error) {
	data := val1.([]byte)
	if data == nil {
		return nil, nil
	}
	typeColl, err := core.GetTypesCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	reader := utils.NewWireReader(data)
	dimensions := reader.ReadInt32()
	_ = reader.ReadInt32() // Whether the array has a null, doesn't seem useful
	baseTypeID := id.Type(id.Cache().ToInternal(reader.ReadUint32()))
	baseType, err := typeColl.GetType(ctx, baseTypeID)
	if err != nil {
		return nil, err
	}
	if baseType == nil {
		return nil, pgtypes.ErrTypeDoesNotExist.New(baseTypeID.TypeName())
	}
	// TODO: handle more than 1 dimension
	if dimensions > 1 {
		return nil, errors.Errorf("array dimensions greater than 1 are not yet supported")
	}
	var vals []any
	lowerBounds := make([]int32, 0, dimensions)
	for dimensionIdx := int32(0); dimensionIdx < dimensions; dimensionIdx++ {
		elementsCount := reader.ReadInt32()
		lowerBounds = append(lowerBounds, reader.ReadInt32())
		for i := int32(0); i < elementsCount; i++ {
			elementLen := reader.ReadInt32()
			if elementLen != -1 {
				valBytes := reader.ReadBytes(uint32(elementLen))
				val, err := baseType.CallReceive(ctx, valBytes)
				if err != nil {
					return nil, err
				}
				vals = append(vals, val)
			} else {
				vals = append(vals, nil)
			}
		}
	}
	return pgtypes.NewArrayValue(vals, lowerBounds), nil
}

// array_send represents the PostgreSQL function of array type IO send.
var array_send = framework.Function1{
	Name:       "array_send",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.AnyArray},
	Strict:     true,
	Callable: func(ctx *sql.Context, t [2]*pgtypes.DoltgresType, val any) (any, error) {
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
		vals, ok := pgtypes.ArrayElements(val)
		if !ok {
			return nil, errors.Errorf("expected array value but received %T", val)
		}
		dimensions := arrayBinaryDimensions(vals)
		hasNull, err := validateArrayBinaryShape(vals, dimensions, 0)
		if err != nil {
			return nil, err
		}
		writer := utils.NewWireWriter()
		writer.WriteInt32(int32(len(dimensions))) // Write the number of dimensions
		if hasNull {
			writer.WriteInt32(1)
		} else {
			writer.WriteInt32(0)
		}
		baseType, err := t[0].ResolveArrayBaseType(ctx)
		if err != nil {
			return nil, err
		}
		writer.WriteUint32(id.Cache().ToOID(baseType.ID.AsId())) // Element OID
		lowerBounds := pgtypes.ArrayLowerBounds(val)
		for i, dimension := range dimensions {
			lowerBound := int32(1)
			if i < len(lowerBounds) {
				lowerBound = lowerBounds[i]
			}
			writer.WriteInt32(dimension)  // Elements in this dimension
			writer.WriteInt32(lowerBound) // Lower bound, or what index number we start at
		}
		if err := writeArrayBinaryElements(ctx, writer, vals, dimensions, 0, baseType); err != nil {
			return nil, err
		}
		return writer.BufferData(), nil
	},
}

func arrayBinaryDimensions(vals []any) []int32 {
	if len(vals) == 0 {
		return nil
	}
	dimensions := make([]int32, 0, 1)
	for {
		dimensions = append(dimensions, int32(len(vals)))
		var next []any
		for _, val := range vals {
			if val == nil {
				continue
			}
			nested, ok := val.([]any)
			if !ok {
				return dimensions
			}
			next = nested
			break
		}
		if next == nil {
			return dimensions
		}
		vals = next
	}
}

func validateArrayBinaryShape(vals []any, dimensions []int32, depth int) (bool, error) {
	if len(dimensions) == 0 {
		return false, nil
	}
	if depth >= len(dimensions) || int32(len(vals)) != dimensions[depth] {
		return false, errors.Errorf("multidimensional arrays must have array expressions with matching dimensions")
	}
	hasNull := false
	leafDepth := depth == len(dimensions)-1
	for _, val := range vals {
		if val == nil {
			if !leafDepth {
				return false, errors.Errorf("multidimensional arrays cannot contain null subarrays")
			}
			hasNull = true
			continue
		}
		nested, ok := val.([]any)
		if leafDepth {
			if ok {
				return false, errors.Errorf("multidimensional arrays must have array expressions with matching dimensions")
			}
			continue
		}
		if !ok {
			return false, errors.Errorf("multidimensional arrays must have array expressions with matching dimensions")
		}
		nestedHasNull, err := validateArrayBinaryShape(nested, dimensions, depth+1)
		if err != nil {
			return false, err
		}
		hasNull = hasNull || nestedHasNull
	}
	return hasNull, nil
}

func writeArrayBinaryElements(ctx *sql.Context, writer *utils.WireRW, vals []any, dimensions []int32, depth int, baseType *pgtypes.DoltgresType) error {
	if len(dimensions) == 0 {
		return nil
	}
	if depth == len(dimensions)-1 {
		for _, val := range vals {
			if val == nil {
				writer.WriteInt32(-1)
				continue
			}
			valBytes, err := baseType.CallSend(ctx, val)
			if err != nil {
				return err
			}
			writer.WriteInt32(int32(len(valBytes)))
			writer.WriteBytes(valBytes)
		}
		return nil
	}
	for _, val := range vals {
		if err := writeArrayBinaryElements(ctx, writer, val.([]any), dimensions, depth+1, baseType); err != nil {
			return err
		}
	}
	return nil
}

// btarraycmp represents the PostgreSQL function of array type byte compare.
var btarraycmp = framework.Function2{
	Name:       "btarraycmp",
	Return:     pgtypes.Int32,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.AnyArray, pgtypes.AnyArray},
	Strict:     true,
	Callable: func(ctx *sql.Context, t [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		at := t[0]
		bt := t[1]
		if !at.Equals(bt) {
			// TODO: currently, types should match.
			// Technically, does not have to e.g.: float4 vs float8
			return nil, errors.Errorf("different type comparison is not supported yet")
		}

		ab, ok := pgtypes.ArrayElements(val1)
		if !ok {
			return nil, errors.Errorf("expected array value but received %T", val1)
		}
		bb, ok := pgtypes.ArrayElements(val2)
		if !ok {
			return nil, errors.Errorf("expected array value but received %T", val2)
		}
		minLength := utils.Min(len(ab), len(bb))
		baseType, err := at.ResolveArrayBaseType(ctx)
		if err != nil {
			return nil, err
		}
		for i := 0; i < minLength; i++ {
			res, err := baseType.Compare(ctx, ab[i], bb[i])
			if err != nil {
				return 0, err
			}
			if res != 0 {
				return res, nil
			}
		}
		if len(ab) == len(bb) {
			return int32(0), nil
		} else if len(ab) < len(bb) {
			return int32(-1), nil
		} else {
			return int32(1), nil
		}
	},
}

// array_subscript_handler represents the PostgreSQL function of array type subscript handler.
var array_subscript_handler = framework.Function1{
	Name:       "array_subscript_handler",
	Return:     pgtypes.Internal,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Internal},
	Strict:     true,
	Callable: func(ctx *sql.Context, t [2]*pgtypes.DoltgresType, val any) (any, error) {
		// TODO
		return []byte{}, nil
	},
}
