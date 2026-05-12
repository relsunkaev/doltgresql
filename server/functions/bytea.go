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
	"bytes"
	"encoding/hex"
	"hash/crc32"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
	"github.com/dolthub/doltgresql/utils"
)

// initBytea registers the functions to the catalog.
func initBytea() {
	framework.RegisterFunction(byteain)
	framework.RegisterFunction(byteaout)
	framework.RegisterFunction(bytearecv)
	framework.RegisterFunction(byteasend)
	framework.RegisterFunction(byteacmp)
	framework.RegisterFunction(crc32_bytea)
	framework.RegisterFunction(crc32c_bytea)
	framework.MustAddExplicitTypeCast(framework.TypeCast{FromType: pgtypes.Int16, ToType: pgtypes.Bytea, Function: int16ToBytea})
	framework.MustAddExplicitTypeCast(framework.TypeCast{FromType: pgtypes.Int32, ToType: pgtypes.Bytea, Function: int32ToBytea})
	framework.MustAddExplicitTypeCast(framework.TypeCast{FromType: pgtypes.Int64, ToType: pgtypes.Bytea, Function: int64ToBytea})
	framework.MustAddExplicitTypeCast(framework.TypeCast{FromType: pgtypes.Bytea, ToType: pgtypes.Int16, Function: byteaToInt16})
	framework.MustAddExplicitTypeCast(framework.TypeCast{FromType: pgtypes.Bytea, ToType: pgtypes.Int32, Function: byteaToInt32})
	framework.MustAddExplicitTypeCast(framework.TypeCast{FromType: pgtypes.Bytea, ToType: pgtypes.Int64, Function: byteaToInt64})
}

// byteain represents the PostgreSQL function of bytea type IO input.
var byteain = framework.Function1{
	Name:       "byteain",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Cstring},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		input := val.(string)
		if strings.HasPrefix(input, `\x`) {
			return hex.DecodeString(input[2:])
		} else {
			return []byte(input), nil
		}
	},
}

// byteaout represents the PostgreSQL function of bytea type IO output.
var byteaout = framework.Function1{
	Name:       "byteaout",
	Return:     pgtypes.Cstring,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Bytea},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return `\x` + hex.EncodeToString(val.([]byte)), nil
	},
}

// bytearecv represents the PostgreSQL function of bytea type IO receive.
var bytearecv = framework.Function1{
	Name:       "bytearecv",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Internal},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return val, nil
	},
}

// byteasend represents the PostgreSQL function of bytea type IO send.
var byteasend = framework.Function1{
	Name:       "byteasend",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Bytea},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
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
		writer := utils.NewWireWriter()
		writer.WriteBytes(val.([]byte))
		return writer.BufferData(), nil
	},
}

// byteacmp represents the PostgreSQL function of bytea type compare.
var byteacmp = framework.Function2{
	Name:       "byteacmp",
	Return:     pgtypes.Int32,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Bytea, pgtypes.Bytea},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		return int32(bytes.Compare(val1.([]byte), val2.([]byte))), nil
	},
}

var crc32_bytea = framework.Function1{
	Name:       "crc32",
	Return:     pgtypes.Int64,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Bytea},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		data, err := unwrapBytea(ctx, val)
		if err != nil {
			return nil, err
		}
		return int64(crc32.ChecksumIEEE(data)), nil
	},
}

var crc32c_bytea = framework.Function1{
	Name:       "crc32c",
	Return:     pgtypes.Int64,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Bytea},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		data, err := unwrapBytea(ctx, val)
		if err != nil {
			return nil, err
		}
		return int64(crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))), nil
	},
}

func unwrapBytea(ctx *sql.Context, val any) ([]byte, error) {
	if wrapper, ok := val.(sql.AnyWrapper); ok {
		var err error
		val, err = wrapper.UnwrapAny(ctx)
		if err != nil {
			return nil, err
		}
	}
	if val == nil {
		return nil, nil
	}
	data, ok := val.([]byte)
	if !ok {
		return nil, errors.Errorf("expected bytea, got %T", val)
	}
	return data, nil
}

func int16ToBytea(ctx *sql.Context, val any, targetType *pgtypes.DoltgresType) (any, error) {
	writer := utils.NewWireWriter()
	writer.WriteInt16(val.(int16))
	return writer.BufferData(), nil
}

func int32ToBytea(ctx *sql.Context, val any, targetType *pgtypes.DoltgresType) (any, error) {
	writer := utils.NewWireWriter()
	writer.WriteInt32(val.(int32))
	return writer.BufferData(), nil
}

func int64ToBytea(ctx *sql.Context, val any, targetType *pgtypes.DoltgresType) (any, error) {
	writer := utils.NewWireWriter()
	writer.WriteInt64(val.(int64))
	return writer.BufferData(), nil
}

func byteaToInt16(ctx *sql.Context, val any, targetType *pgtypes.DoltgresType) (any, error) {
	data, err := unwrapBytea(ctx, val)
	if err != nil {
		return nil, err
	}
	if len(data) != 2 {
		return nil, errors.Errorf("cannot cast bytea of length %d to smallint", len(data))
	}
	return utils.NewWireReader(data).ReadInt16(), nil
}

func byteaToInt32(ctx *sql.Context, val any, targetType *pgtypes.DoltgresType) (any, error) {
	data, err := unwrapBytea(ctx, val)
	if err != nil {
		return nil, err
	}
	if len(data) != 4 {
		return nil, errors.Errorf("cannot cast bytea of length %d to integer", len(data))
	}
	return utils.NewWireReader(data).ReadInt32(), nil
}

func byteaToInt64(ctx *sql.Context, val any, targetType *pgtypes.DoltgresType) (any, error) {
	data, err := unwrapBytea(ctx, val)
	if err != nil {
		return nil, err
	}
	if len(data) != 8 {
		return nil, errors.Errorf("cannot cast bytea of length %d to bigint", len(data))
	}
	return utils.NewWireReader(data).ReadInt64(), nil
}
