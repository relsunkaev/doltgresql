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

func initLseg() {
	framework.RegisterFunction(lseg_in)
	framework.RegisterFunction(lseg_out)
	framework.RegisterFunction(lseg_recv)
	framework.RegisterFunction(lseg_send)
}

var lseg_in = framework.Function1{
	Name:       "lseg_in",
	Return:     pgtypes.Lseg,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Cstring},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgtypes.ParseLseg(val.(string))
	},
}

var lseg_out = framework.Function1{
	Name:       "lseg_out",
	Return:     pgtypes.Cstring,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Lseg},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgtypes.FormatLseg(val.(pgtypes.LsegValue)), nil
	},
}

var lseg_recv = framework.Function1{
	Name:       "lseg_recv",
	Return:     pgtypes.Lseg,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Internal},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		data := val.([]byte)
		if data == nil {
			return nil, nil
		}
		return pgtypes.Lseg.DeserializeValue(ctx, data)
	},
}

var lseg_send = framework.Function1{
	Name:       "lseg_send",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Lseg},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgtypes.Lseg.SerializeValue(ctx, val)
	},
}
