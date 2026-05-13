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
	"github.com/dolthub/doltgresql/utils"
)

func initTextSearchTypes() {
	framework.RegisterFunction(tsqueryin)
	framework.RegisterFunction(tsqueryout)
	framework.RegisterFunction(tsqueryrecv)
	framework.RegisterFunction(tsquerysend)
	framework.RegisterFunction(tsvectorin)
	framework.RegisterFunction(tsvectorout)
	framework.RegisterFunction(tsvectorrecv)
	framework.RegisterFunction(tsvectorsend)
}

var tsqueryin = framework.Function1{
	Name:       "tsqueryin",
	Return:     pgtypes.TsQuery,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Cstring},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgtypes.CanonicalTSQuery(val.(string)), nil
	},
}

var tsqueryout = framework.Function1{
	Name:       "tsqueryout",
	Return:     pgtypes.Cstring,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.TsQuery},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgtypes.CanonicalTextSearchValue(pgtypes.TsQuery, val.(string)), nil
	},
}

var tsqueryrecv = framework.Function1{
	Name:       "tsqueryrecv",
	Return:     pgtypes.TsQuery,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Internal},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		data := val.([]byte)
		if data == nil {
			return nil, nil
		}
		return pgtypes.CanonicalTSQuery(string(data)), nil
	},
}

var tsquerysend = framework.Function1{
	Name:       "tsquerysend",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.TsQuery},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return sendTextSearchValue(ctx, pgtypes.TsQuery, val)
	},
}

var tsvectorin = framework.Function1{
	Name:       "tsvectorin",
	Return:     pgtypes.TsVector,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Cstring},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgtypes.CanonicalTSVector(val.(string)), nil
	},
}

var tsvectorout = framework.Function1{
	Name:       "tsvectorout",
	Return:     pgtypes.Cstring,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.TsVector},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgtypes.CanonicalTextSearchValue(pgtypes.TsVector, val.(string)), nil
	},
}

var tsvectorrecv = framework.Function1{
	Name:       "tsvectorrecv",
	Return:     pgtypes.TsVector,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Internal},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		data := val.([]byte)
		if data == nil {
			return nil, nil
		}
		return pgtypes.CanonicalTSVector(string(data)), nil
	},
}

var tsvectorsend = framework.Function1{
	Name:       "tsvectorsend",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.TsVector},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return sendTextSearchValue(ctx, pgtypes.TsVector, val)
	},
}

func sendTextSearchValue(ctx *sql.Context, typ *pgtypes.DoltgresType, val any) (any, error) {
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
	writer.WriteString(pgtypes.CanonicalTextSearchValue(typ, val.(string)))
	return writer.BufferData(), nil
}
