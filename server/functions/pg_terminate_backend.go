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
	"github.com/dolthub/doltgresql/server/replsource"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initPgTerminateBackend registers the functions to the catalog.
func initPgTerminateBackend() {
	framework.RegisterFunction(pg_terminate_backend_int32)
	framework.RegisterFunction(pg_terminate_backend_int32_int64)
}

// pg_terminate_backend_int32 represents the PostgreSQL function pg_terminate_backend(integer).
var pg_terminate_backend_int32 = framework.Function1{
	Name:               "pg_terminate_backend",
	Return:             pgtypes.Bool,
	Parameters:         [1]*pgtypes.DoltgresType{pgtypes.Int32},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return replsource.TerminateSenderByPID(val.(int32)), nil
	},
}

// pg_terminate_backend_int32_int64 represents pg_terminate_backend(integer, bigint).
var pg_terminate_backend_int32_int64 = framework.Function2{
	Name:               "pg_terminate_backend",
	Return:             pgtypes.Bool,
	Parameters:         [2]*pgtypes.DoltgresType{pgtypes.Int32, pgtypes.Int64},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		var unusedTypes [2]*pgtypes.DoltgresType
		return pg_terminate_backend_int32.Callable(ctx, unusedTypes, val1)
	},
}
