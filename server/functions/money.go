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
	"cmp"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
	"github.com/dolthub/doltgresql/utils"
)

func initMoney() {
	framework.RegisterFunction(cash_in)
	framework.RegisterFunction(cash_out)
	framework.RegisterFunction(cash_recv)
	framework.RegisterFunction(cash_send)
	framework.RegisterFunction(cash_cmp)
}

var cash_in = framework.Function1{
	Name:       "cash_in",
	Return:     pgtypes.Money,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Cstring},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgtypes.ParseMoney(val.(string))
	},
}

var cash_out = framework.Function1{
	Name:       "cash_out",
	Return:     pgtypes.Cstring,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Money},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgtypes.FormatMoney(val.(pgtypes.MoneyValue)), nil
	},
}

var cash_recv = framework.Function1{
	Name:       "cash_recv",
	Return:     pgtypes.Money,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Internal},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		data := val.([]byte)
		if data == nil {
			return nil, nil
		}
		return pgtypes.MoneyValue(utils.NewWireReader(data).ReadInt64()), nil
	},
}

var cash_send = framework.Function1{
	Name:       "cash_send",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Money},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		writer := utils.NewWireWriter()
		writer.WriteInt64(val.(pgtypes.MoneyValue))
		return writer.BufferData(), nil
	},
}

var cash_cmp = framework.Function2{
	Name:       "cash_cmp",
	Return:     pgtypes.Int32,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Money, pgtypes.Money},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		return int32(cmp.Compare(val1.(pgtypes.MoneyValue), val2.(pgtypes.MoneyValue))), nil
	},
}
