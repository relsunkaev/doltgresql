// Copyright 2025 Dolthub, Inc.
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
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/postgres/parser/duration"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initPgSleep registers the functions to the catalog.
func initPgSleep() {
	framework.RegisterFunction(pg_sleep_float64)
	framework.RegisterFunction(pg_sleep_for_interval)
}

// pg_sleep_float64 represents the PostgreSQL function of the same name, taking the same parameters.
var pg_sleep_float64 = framework.Function1{
	Name:               "pg_sleep",
	Return:             pgtypes.Void,
	Parameters:         [1]*pgtypes.DoltgresType{pgtypes.Float64},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return nil, sleepWithContext(ctx, time.Duration(val.(float64)*float64(time.Second)))
	},
}

// pg_sleep_for_interval represents the PostgreSQL function of the same name, taking the same parameters.
var pg_sleep_for_interval = framework.Function1{
	Name:               "pg_sleep_for",
	Return:             pgtypes.Void,
	Parameters:         [1]*pgtypes.DoltgresType{pgtypes.Interval},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return nil, sleepWithContext(ctx, time.Duration(val.(duration.Duration).Nanos()))
	},
}

// sleepWithContext blocks for d, but returns the context error early
// if the session is canceled (e.g. from a CancelRequest startup
// message or a client-side query timeout). Real PG's pg_sleep also
// short-circuits on cancellation; without this the time.Sleep call
// would hold the goroutine open until d expired regardless.
func sleepWithContext(ctx *sql.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
