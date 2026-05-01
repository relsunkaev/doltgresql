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

	"github.com/dolthub/go-mysql-server/sql"

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
	framework.RegisterFunction(vector_cmp)
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

// vector_typmod_in represents the PostgreSQL function of vector type typmod input.
var vector_typmod_in = framework.Function1{
	Name:       "vector_typmod_in",
	Return:     pgtypes.Int32,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.CstringArray},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		arr := val.([]any)
		if len(arr) == 0 {
			return nil, pgtypes.ErrTypmodArrayMustBe1D.New()
		} else if len(arr) > 1 {
			return nil, pgtypes.ErrInvalidTypMod.New("vector")
		}
		dimensions, err := strconv.ParseInt(arr[0].(string), 10, 32)
		if err != nil {
			return nil, err
		}
		return pgtypes.GetTypmodFromVectorDimensions(int32(dimensions))
	},
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
