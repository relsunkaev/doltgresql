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

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initPgGetFunctionArgDefault registers the functions to the catalog.
func initPgGetFunctionArgDefault() {
	framework.RegisterFunction(pg_get_function_arg_default_oid_int4)
}

// pg_get_function_arg_default_oid_int4 represents the PostgreSQL system catalog information function.
var pg_get_function_arg_default_oid_int4 = framework.Function2{
	Name:               "pg_get_function_arg_default",
	Return:             pgtypes.Text,
	Parameters:         [2]*pgtypes.DoltgresType{pgtypes.Oid, pgtypes.Int32},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, oidVal any, argNum any) (any, error) {
		return pgGetFunctionArgDefault(ctx, oidVal.(id.Id), argNum.(int32))
	},
}
