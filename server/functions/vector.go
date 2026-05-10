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
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/shopspring/decimal"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
	"github.com/dolthub/doltgresql/utils"
)

// initVector registers the functions to the catalog.
func initVector() {
	framework.RegisterFunction(vector_in)
	framework.RegisterFunction(vector_out)
	framework.RegisterFunction(vector_recv)
	framework.RegisterFunction(vector_send)
	framework.RegisterFunction(vector_typmod_in)
	framework.RegisterFunction(vector_typmod_out)
	framework.RegisterFunction(halfvec_in)
	framework.RegisterFunction(halfvec_out)
	framework.RegisterFunction(halfvec_recv)
	framework.RegisterFunction(halfvec_send)
	framework.RegisterFunction(halfvec_typmod_in)
	framework.RegisterFunction(halfvec_typmod_out)
	framework.RegisterFunction(sparsevec_in)
	framework.RegisterFunction(sparsevec_out)
	framework.RegisterFunction(sparsevec_recv)
	framework.RegisterFunction(sparsevec_send)
	framework.RegisterFunction(sparsevec_typmod_in)
	framework.RegisterFunction(sparsevec_typmod_out)
	framework.RegisterFunction(vector_cmp)
	framework.RegisterFunction(array_to_vector_int32)
	framework.RegisterFunction(array_to_vector_float32)
	framework.RegisterFunction(array_to_vector_float64)
	framework.RegisterFunction(array_to_vector_numeric)
	framework.RegisterFunction(vector_to_float4)
}

// vector_in represents the PostgreSQL function of vector type IO input.
var vector_in = framework.Function3{
	Name:       "vector_in",
	Return:     pgtypes.Vector,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Cstring, pgtypes.Oid, pgtypes.Int32},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, val1, val2, val3 any) (any, error) {
		return pgtypes.ParseVector(val1.(string), val3.(int32))
	},
}

// vector_out represents the PostgreSQL function of vector type IO output.
var vector_out = framework.Function1{
	Name:       "vector_out",
	Return:     pgtypes.Cstring,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Vector},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgtypes.FormatVector(val.([]float32)), nil
	},
}

// vector_recv represents the PostgreSQL function of vector type IO receive.
var vector_recv = framework.Function3{
	Name:       "vector_recv",
	Return:     pgtypes.Vector,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Internal, pgtypes.Oid, pgtypes.Int32},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, val1, val2, val3 any) (any, error) {
		data := val1.([]byte)
		if data == nil {
			return nil, nil
		}
		if len(data) < 4 {
			return nil, pgtypes.ErrInvalidSyntaxForType.New("vector", string(data))
		}
		reader := utils.NewWireReader(data)
		dimensions := int(reader.ReadUint16())
		_ = reader.ReadUint16() // pgvector reserves this field for future use.
		if len(data) != 4+(dimensions*4) {
			return nil, pgtypes.ErrInvalidSyntaxForType.New("vector", string(data))
		}
		values := make([]float32, dimensions)
		for i := range values {
			values[i] = reader.ReadFloat32()
		}
		if err := pgtypes.ValidateVectorDimensions(values, val3.(int32)); err != nil {
			return nil, err
		}
		return values, nil
	},
}

// vector_send represents the PostgreSQL function of vector type IO send.
var vector_send = framework.Function1{
	Name:       "vector_send",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Vector},
	Strict:     true,
	Callable: func(ctx *sql.Context, t [2]*pgtypes.DoltgresType, val any) (any, error) {
		values := val.([]float32)
		if err := pgtypes.ValidateVectorDimensions(values, t[0].GetAttTypMod()); err != nil {
			return nil, err
		}
		writer := utils.NewWireWriter()
		writer.WriteUint16(uint16(len(values)))
		writer.WriteUint16(0)
		for _, value := range values {
			writer.WriteFloat32(value)
		}
		return writer.BufferData(), nil
	},
}

var halfvec_in = pgvectorUnsupportedInput("halfvec", pgtypes.Halfvec)
var halfvec_out = pgvectorUnsupportedOutput("halfvec", pgtypes.Halfvec)
var halfvec_recv = pgvectorUnsupportedReceive("halfvec", pgtypes.Halfvec)
var halfvec_send = pgvectorUnsupportedSend("halfvec", pgtypes.Halfvec)
var halfvec_typmod_in = pgvectorTypmodInput("halfvec", pgtypes.GetTypmodFromHalfvecDimensions)
var halfvec_typmod_out = pgvectorTypmodOutput("halfvec")

var sparsevec_in = pgvectorUnsupportedInput("sparsevec", pgtypes.Sparsevec)
var sparsevec_out = pgvectorUnsupportedOutput("sparsevec", pgtypes.Sparsevec)
var sparsevec_recv = pgvectorUnsupportedReceive("sparsevec", pgtypes.Sparsevec)
var sparsevec_send = pgvectorUnsupportedSend("sparsevec", pgtypes.Sparsevec)
var sparsevec_typmod_in = pgvectorTypmodInput("sparsevec", pgtypes.GetTypmodFromSparsevecDimensions)
var sparsevec_typmod_out = pgvectorTypmodOutput("sparsevec")

// vector_typmod_in represents the PostgreSQL function of vector type typmod input.
var vector_typmod_in = framework.Function1{
	Name:       "vector_typmod_in",
	Return:     pgtypes.Int32,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.CstringArray},
	Strict:     true,
	Callable:   pgvectorTypmodInCallable("vector", pgtypes.GetTypmodFromVectorDimensions),
}

func pgvectorUnsupportedValues(typeName string) error {
	return errors.Errorf("pgvector %s values are not yet supported", typeName)
}

func pgvectorUnsupportedInput(typeName string, typ *pgtypes.DoltgresType) framework.Function3 {
	return framework.Function3{
		Name:       typeName + "_in",
		Return:     typ,
		Parameters: [3]*pgtypes.DoltgresType{pgtypes.Cstring, pgtypes.Oid, pgtypes.Int32},
		Strict:     true,
		Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, val1, val2, val3 any) (any, error) {
			return nil, pgvectorUnsupportedValues(typeName)
		},
	}
}

func pgvectorUnsupportedOutput(typeName string, typ *pgtypes.DoltgresType) framework.Function1 {
	return framework.Function1{
		Name:       typeName + "_out",
		Return:     pgtypes.Cstring,
		Parameters: [1]*pgtypes.DoltgresType{typ},
		Strict:     true,
		Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
			return nil, pgvectorUnsupportedValues(typeName)
		},
	}
}

func pgvectorUnsupportedReceive(typeName string, typ *pgtypes.DoltgresType) framework.Function3 {
	return framework.Function3{
		Name:       typeName + "_recv",
		Return:     typ,
		Parameters: [3]*pgtypes.DoltgresType{pgtypes.Internal, pgtypes.Oid, pgtypes.Int32},
		Strict:     true,
		Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, val1, val2, val3 any) (any, error) {
			return nil, pgvectorUnsupportedValues(typeName)
		},
	}
}

func pgvectorUnsupportedSend(typeName string, typ *pgtypes.DoltgresType) framework.Function1 {
	return framework.Function1{
		Name:       typeName + "_send",
		Return:     pgtypes.Bytea,
		Parameters: [1]*pgtypes.DoltgresType{typ},
		Strict:     true,
		Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
			return nil, pgvectorUnsupportedValues(typeName)
		},
	}
}

func pgvectorTypmodInput(typeName string, validate func(int32) (int32, error)) framework.Function1 {
	return framework.Function1{
		Name:       typeName + "_typmod_in",
		Return:     pgtypes.Int32,
		Parameters: [1]*pgtypes.DoltgresType{pgtypes.CstringArray},
		Strict:     true,
		Callable:   pgvectorTypmodInCallable(typeName, validate),
	}
}

func pgvectorTypmodInCallable(typeName string, validate func(int32) (int32, error)) func(*sql.Context, [2]*pgtypes.DoltgresType, any) (any, error) {
	return func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		arr := val.([]any)
		if len(arr) == 0 {
			return nil, pgtypes.ErrTypmodArrayMustBe1D.New()
		} else if len(arr) > 1 {
			return nil, pgtypes.ErrInvalidTypMod.New(typeName)
		}
		dimensions, err := strconv.ParseInt(arr[0].(string), 10, 32)
		if err != nil {
			return nil, err
		}
		return validate(int32(dimensions))
	}
}

func pgvectorTypmodOutput(typeName string) framework.Function1 {
	return framework.Function1{
		Name:       typeName + "_typmod_out",
		Return:     pgtypes.Cstring,
		Parameters: [1]*pgtypes.DoltgresType{pgtypes.Int32},
		Strict:     true,
		Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
			return pgtypes.VectorTypmodOut(val.(int32)), nil
		},
	}
}

// vector_typmod_out represents the PostgreSQL function of vector type typmod output.
var vector_typmod_out = framework.Function1{
	Name:       "vector_typmod_out",
	Return:     pgtypes.Cstring,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Int32},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgtypes.VectorTypmodOut(val.(int32)), nil
	},
}

// vector_cmp represents the PostgreSQL comparator for vector values.
var vector_cmp = framework.Function2{
	Name:       "vector_cmp",
	Return:     pgtypes.Int32,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Vector, pgtypes.Vector},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		return int32(pgtypes.CompareVectors(val1.([]float32), val2.([]float32))), nil
	},
}

var array_to_vector_int32 = framework.Function3{
	Name:       "array_to_vector",
	Return:     pgtypes.Vector,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Int32Array, pgtypes.Int32, pgtypes.Bool},
	Strict:     true,
	Callable: arrayToVectorCallable(func(val any) (float32, error) {
		return float32(val.(int32)), nil
	}),
}

var array_to_vector_float32 = framework.Function3{
	Name:       "array_to_vector",
	Return:     pgtypes.Vector,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Float32Array, pgtypes.Int32, pgtypes.Bool},
	Strict:     true,
	Callable: arrayToVectorCallable(func(val any) (float32, error) {
		return val.(float32), nil
	}),
}

var array_to_vector_float64 = framework.Function3{
	Name:       "array_to_vector",
	Return:     pgtypes.Vector,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Float64Array, pgtypes.Int32, pgtypes.Bool},
	Strict:     true,
	Callable: arrayToVectorCallable(func(val any) (float32, error) {
		return float32(val.(float64)), nil
	}),
}

var array_to_vector_numeric = framework.Function3{
	Name:       "array_to_vector",
	Return:     pgtypes.Vector,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.NumericArray, pgtypes.Int32, pgtypes.Bool},
	Strict:     true,
	Callable: arrayToVectorCallable(func(val any) (float32, error) {
		f, _ := val.(decimal.Decimal).Float64()
		return float32(f), nil
	}),
}

var vector_to_float4 = framework.Function3{
	Name:       "vector_to_float4",
	Return:     pgtypes.Float32Array,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Vector, pgtypes.Int32, pgtypes.Bool},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, val1, val2, val3 any) (any, error) {
		return pgtypes.VectorToFloat32Array(val1.([]float32)), nil
	},
}

func arrayToVectorCallable(convert func(any) (float32, error)) func(*sql.Context, [4]*pgtypes.DoltgresType, any, any, any) (any, error) {
	return func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, val1, val2, val3 any) (any, error) {
		return pgtypes.VectorFromArrayValues(val1.([]any), val2.(int32), convert)
	}
}
