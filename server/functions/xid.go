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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
	"github.com/dolthub/doltgresql/utils"
)

// initXid registers the functions to the catalog.
func initXid() {
	framework.RegisterFunction(xidin)
	framework.RegisterFunction(xidout)
	framework.RegisterFunction(xidrecv)
	framework.RegisterFunction(xidsend)
}

// xidin represents the PostgreSQL function of xid type IO input.
var xidin = framework.Function1{
	Name:       "xidin",
	Return:     pgtypes.Xid,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Cstring},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return parseXidInput(val.(string))
	},
}

func parseXidInput(raw string) (uint32, error) {
	input := strings.TrimSpace(raw)
	if input == "" || strings.HasPrefix(input, "+") {
		return 0, pgtypes.ErrInvalidSyntaxForType.New("xid", raw)
	}
	if strings.HasPrefix(input, "-") {
		sVal, err := strconv.ParseInt(input, 10, 32)
		if err != nil {
			if numErr, ok := err.(*strconv.NumError); ok && numErr.Err == strconv.ErrRange {
				return 0, xidOutOfRangeError(raw)
			}
			return 0, pgtypes.ErrInvalidSyntaxForType.New("xid", raw)
		}
		return uint32(int32(sVal)), nil
	}
	uVal, err := strconv.ParseUint(input, 10, 32)
	if err != nil {
		if numErr, ok := err.(*strconv.NumError); ok && numErr.Err == strconv.ErrRange {
			return 0, xidOutOfRangeError(raw)
		}
		return 0, pgtypes.ErrInvalidSyntaxForType.New("xid", raw)
	}
	return uint32(uVal), nil
}

func xidOutOfRangeError(input string) error {
	return pgerror.Newf(pgcode.NumericValueOutOfRange, "value %q is out of range for type xid", input)
}

// xidout represents the PostgreSQL function of xid type IO output.
var xidout = framework.Function1{
	Name:       "xidout",
	Return:     pgtypes.Cstring,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Xid},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return strconv.FormatUint(uint64(val.(uint32)), 10), nil
	},
}

// xidrecv represents the PostgreSQL function of xid type IO receive.
var xidrecv = framework.Function1{
	Name:       "xidrecv",
	Return:     pgtypes.Xid,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Internal},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		data := val.([]byte)
		if data == nil {
			return nil, nil
		}
		reader := utils.NewWireReader(data)
		return reader.ReadUint32(), nil
	},
}

// xidsend represents the PostgreSQL function of xid type IO send.
var xidsend = framework.Function1{
	Name:       "xidsend",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Xid},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		writer := utils.NewWireWriter()
		writer.WriteUint32(val.(uint32))
		return writer.BufferData(), nil
	},
}
