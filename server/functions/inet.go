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
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func initInet() {
	framework.RegisterFunction(cidr_in)
	framework.RegisterFunction(cidr_out)
	framework.RegisterFunction(cidr_recv)
	framework.RegisterFunction(cidr_send)
	framework.RegisterFunction(inet_in)
	framework.RegisterFunction(inet_out)
	framework.RegisterFunction(inet_recv)
	framework.RegisterFunction(inet_send)
	framework.RegisterFunction(inet_host)
	framework.RegisterFunction(inet_masklen)
}

var cidr_in = framework.Function1{
	Name:       "cidr_in",
	Return:     pgtypes.Cidr,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Cstring},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgtypes.ParseCidr(val.(string))
	},
}

var cidr_out = framework.Function1{
	Name:       "cidr_out",
	Return:     pgtypes.Cstring,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Cidr},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgtypes.FormatCidr(val.(pgtypes.InetValue)), nil
	},
}

var cidr_recv = framework.Function1{
	Name:       "cidr_recv",
	Return:     pgtypes.Cidr,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Internal},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		data := val.([]byte)
		if data == nil {
			return nil, nil
		}
		return pgtypes.Cidr.DeserializeValue(ctx, data)
	},
}

var cidr_send = framework.Function1{
	Name:       "cidr_send",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Cidr},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgtypes.Cidr.SerializeValue(ctx, val)
	},
}

var inet_in = framework.Function1{
	Name:       "inet_in",
	Return:     pgtypes.Inet,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Cstring},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgtypes.ParseInet(val.(string))
	},
}

var inet_out = framework.Function1{
	Name:       "inet_out",
	Return:     pgtypes.Cstring,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Inet},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgtypes.FormatInet(val.(pgtypes.InetValue)), nil
	},
}

var inet_recv = framework.Function1{
	Name:       "inet_recv",
	Return:     pgtypes.Inet,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Internal},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		data := val.([]byte)
		if data == nil {
			return nil, nil
		}
		return pgtypes.Inet.DeserializeValue(ctx, data)
	},
}

var inet_send = framework.Function1{
	Name:       "inet_send",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Inet},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgtypes.Inet.SerializeValue(ctx, val)
	},
}

var inet_host = framework.Function1{
	Name:       "host",
	Return:     pgtypes.Text,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Inet},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return val.(pgtypes.InetValue).Host(), nil
	},
}

var inet_masklen = framework.Function1{
	Name:       "masklen",
	Return:     pgtypes.Int32,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Inet},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return int32(val.(pgtypes.InetValue).Bits), nil
	},
}
