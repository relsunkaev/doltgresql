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
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	"github.com/dolthub/doltgresql/server/replsource"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initPgSnapshot registers the functions to the catalog.
func initPgSnapshot() {
	framework.RegisterFunction(pg_snapshot_in)
	framework.RegisterFunction(pg_snapshot_out)
	framework.RegisterFunction(pg_snapshot_recv)
	framework.RegisterFunction(pg_snapshot_send)
	framework.RegisterFunction(pg_current_snapshot)
	framework.RegisterFunction(pg_snapshot_xmin)
	framework.RegisterFunction(pg_snapshot_xmax)
	framework.RegisterFunction(pg_snapshot_xip)
	framework.RegisterFunction(pg_visible_in_snapshot)
}

// pg_snapshot_in represents the PostgreSQL function of pg_snapshot type IO input.
var pg_snapshot_in = framework.Function1{
	Name:       "pg_snapshot_in",
	Return:     pgtypes.PgSnapshot,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Cstring},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgtypes.ParsePgSnapshot(val.(string))
	},
}

// pg_snapshot_out represents the PostgreSQL function of pg_snapshot type IO output.
var pg_snapshot_out = framework.Function1{
	Name:       "pg_snapshot_out",
	Return:     pgtypes.Cstring,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.PgSnapshot},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgtypes.FormatPgSnapshot(val.(pgtypes.PgSnapshotValue)), nil
	},
}

// pg_snapshot_recv represents the PostgreSQL function of pg_snapshot type IO receive.
var pg_snapshot_recv = framework.Function1{
	Name:       "pg_snapshot_recv",
	Return:     pgtypes.PgSnapshot,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Internal},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		data := val.([]byte)
		if data == nil {
			return nil, nil
		}
		return pgtypes.DecodePgSnapshotBinary(data)
	},
}

// pg_snapshot_send represents the PostgreSQL function of pg_snapshot type IO send.
var pg_snapshot_send = framework.Function1{
	Name:       "pg_snapshot_send",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.PgSnapshot},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgtypes.EncodePgSnapshotBinary(val.(pgtypes.PgSnapshotValue)), nil
	},
}

// pg_current_snapshot returns a stable empty snapshot for Doltgres' non-MVCC transaction ID model.
var pg_current_snapshot = framework.Function0{
	Name:               "pg_current_snapshot",
	Return:             pgtypes.PgSnapshot,
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context) (any, error) {
		xmax := uint64(replsource.CurrentLSN()) + 1
		if xmax == 0 {
			xmax = 1
		}
		return pgtypes.PgSnapshotValue{Xmin: 1, Xmax: xmax}, nil
	},
}

// pg_snapshot_xmin returns the snapshot's xmin.
var pg_snapshot_xmin = framework.Function1{
	Name:       "pg_snapshot_xmin",
	Return:     pgtypes.Xid8,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.PgSnapshot},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return val.(pgtypes.PgSnapshotValue).Xmin, nil
	},
}

// pg_snapshot_xmax returns the snapshot's xmax.
var pg_snapshot_xmax = framework.Function1{
	Name:       "pg_snapshot_xmax",
	Return:     pgtypes.Xid8,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.PgSnapshot},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return val.(pgtypes.PgSnapshotValue).Xmax, nil
	},
}

// pg_snapshot_xip returns the snapshot's in-progress transaction IDs.
var pg_snapshot_xip = framework.Function1{
	Name:       "pg_snapshot_xip",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.Xid8),
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.PgSnapshot},
	Strict:     true,
	SRF:        true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		xip := val.(pgtypes.PgSnapshotValue).Xip
		i := 0
		return pgtypes.NewSetReturningFunctionRowIter(func(ctx *sql.Context) (sql.Row, error) {
			if i >= len(xip) {
				return nil, io.EOF
			}
			xid := xip[i]
			i++
			return sql.Row{xid}, nil
		}), nil
	},
}

// pg_visible_in_snapshot returns whether a transaction ID is visible in the supplied snapshot.
var pg_visible_in_snapshot = framework.Function2{
	Name:       "pg_visible_in_snapshot",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Xid8, pgtypes.PgSnapshot},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, xidValue any, snapshotValue any) (any, error) {
		xid := xidValue.(uint64)
		snapshot := snapshotValue.(pgtypes.PgSnapshotValue)
		if xid < snapshot.Xmin {
			return true, nil
		}
		if xid >= snapshot.Xmax {
			return false, nil
		}
		for _, inProgress := range snapshot.Xip {
			if xid == inProgress {
				return false, nil
			}
		}
		return true, nil
	},
}
