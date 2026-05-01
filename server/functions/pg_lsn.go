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
	"math/big"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/shopspring/decimal"

	"github.com/dolthub/doltgresql/server/functions/framework"
	"github.com/dolthub/doltgresql/server/replsource"
	pgtypes "github.com/dolthub/doltgresql/server/types"
	"github.com/dolthub/doltgresql/utils"
)

// initPgLsn registers the functions to the catalog.
func initPgLsn() {
	framework.RegisterFunction(pg_lsn_in)
	framework.RegisterFunction(pg_lsn_out)
	framework.RegisterFunction(pg_lsn_recv)
	framework.RegisterFunction(pg_lsn_send)
	framework.RegisterFunction(pg_lsn_cmp)
	framework.RegisterFunction(pg_wal_lsn_diff)
	framework.RegisterFunction(pg_current_wal_lsn)
	framework.RegisterFunction(pg_last_wal_receive_lsn)
	framework.RegisterFunction(pg_last_wal_replay_lsn)
	framework.RegisterFunction(pg_lsn_larger)
	framework.RegisterFunction(pg_lsn_smaller)
}

// pg_lsn_in represents the PostgreSQL function of pg_lsn type IO input.
var pg_lsn_in = framework.Function1{
	Name:       "pg_lsn_in",
	Return:     pgtypes.PgLsn,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Cstring},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgtypes.ParsePgLsn(val.(string))
	},
}

// pg_lsn_out represents the PostgreSQL function of pg_lsn type IO output.
var pg_lsn_out = framework.Function1{
	Name:       "pg_lsn_out",
	Return:     pgtypes.Cstring,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.PgLsn},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgtypes.FormatPgLsn(val.(uint64)), nil
	},
}

// pg_lsn_recv represents the PostgreSQL function of pg_lsn type IO receive.
var pg_lsn_recv = framework.Function1{
	Name:       "pg_lsn_recv",
	Return:     pgtypes.PgLsn,
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

// pg_lsn_send represents the PostgreSQL function of pg_lsn type IO send.
var pg_lsn_send = framework.Function1{
	Name:       "pg_lsn_send",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.PgLsn},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		writer := utils.NewWireWriter()
		writer.WriteUint64(val.(uint64))
		return writer.BufferData(), nil
	},
}

// pg_lsn_cmp represents the PostgreSQL btree comparator for pg_lsn.
var pg_lsn_cmp = framework.Function2{
	Name:       "pg_lsn_cmp",
	Return:     pgtypes.Int32,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.PgLsn, pgtypes.PgLsn},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		return int32(cmp.Compare(val1.(uint64), val2.(uint64))), nil
	},
}

// pg_wal_lsn_diff represents the PostgreSQL function of the same name.
var pg_wal_lsn_diff = framework.Function2{
	Name:       "pg_wal_lsn_diff",
	Return:     pgtypes.Numeric,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.PgLsn, pgtypes.PgLsn},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		left := new(big.Int).SetUint64(val1.(uint64))
		right := new(big.Int).SetUint64(val2.(uint64))
		return decimal.NewFromBigInt(left.Sub(left, right), 0), nil
	},
}

// pg_current_wal_lsn reports the highest local logical replication source LSN.
var pg_current_wal_lsn = framework.Function0{
	Name:               "pg_current_wal_lsn",
	Return:             pgtypes.PgLsn,
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context) (any, error) {
		return uint64(replsource.CurrentLSN()), nil
	},
}

// pg_last_wal_receive_lsn reports NULL because Doltgres is not in standby recovery mode.
var pg_last_wal_receive_lsn = framework.Function0{
	Name:               "pg_last_wal_receive_lsn",
	Return:             pgtypes.PgLsn,
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context) (any, error) {
		return nil, nil
	},
}

// pg_last_wal_replay_lsn reports NULL because Doltgres is not in standby recovery mode.
var pg_last_wal_replay_lsn = framework.Function0{
	Name:               "pg_last_wal_replay_lsn",
	Return:             pgtypes.PgLsn,
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context) (any, error) {
		return nil, nil
	},
}

// pg_lsn_larger returns the larger pg_lsn.
var pg_lsn_larger = framework.Function2{
	Name:       "pg_lsn_larger",
	Return:     pgtypes.PgLsn,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.PgLsn, pgtypes.PgLsn},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		if val1.(uint64) >= val2.(uint64) {
			return val1, nil
		}
		return val2, nil
	},
}

// pg_lsn_smaller returns the smaller pg_lsn.
var pg_lsn_smaller = framework.Function2{
	Name:       "pg_lsn_smaller",
	Return:     pgtypes.PgLsn,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.PgLsn, pgtypes.PgLsn},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		if val1.(uint64) <= val2.(uint64) {
			return val1, nil
		}
		return val2, nil
	},
}
