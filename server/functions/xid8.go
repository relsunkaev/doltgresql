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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
	"github.com/dolthub/doltgresql/utils"
)

// initXid8 registers the functions to the catalog.
func initXid8() {
	framework.RegisterFunction(xid8in)
	framework.RegisterFunction(xid8out)
	framework.RegisterFunction(xid8recv)
	framework.RegisterFunction(xid8send)
}

// xid8in represents the PostgreSQL function of xid8 type IO input.
var xid8in = framework.Function1{
	Name:       "xid8in",
	Return:     pgtypes.Xid8,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Cstring},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		input := strings.TrimSpace(val.(string))
		if input == "" || strings.HasPrefix(input, "+") || strings.HasPrefix(input, "-") {
			return nil, pgtypes.ErrInvalidSyntaxForType.New("xid8", val.(string))
		}
		uVal, err := strconv.ParseUint(input, 10, 64)
		if err != nil {
			return nil, pgtypes.ErrInvalidSyntaxForType.New("xid8", val.(string))
		}
		return uVal, nil
	},
}

// xid8out represents the PostgreSQL function of xid8 type IO output.
var xid8out = framework.Function1{
	Name:       "xid8out",
	Return:     pgtypes.Cstring,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Xid8},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return strconv.FormatUint(val.(uint64), 10), nil
	},
}

// xid8recv represents the PostgreSQL function of xid8 type IO receive.
var xid8recv = framework.Function1{
	Name:       "xid8recv",
	Return:     pgtypes.Xid8,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Internal},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		data := val.([]byte)
		if data == nil {
			return nil, nil
		}
		reader := utils.NewWireReader(data)
		return reader.ReadUint64(), nil
	},
}

// xid8send represents the PostgreSQL function of xid8 type IO send.
var xid8send = framework.Function1{
	Name:       "xid8send",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Xid8},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		writer := utils.NewWireWriter()
		writer.WriteUint64(val.(uint64))
		return writer.BufferData(), nil
	},
}
