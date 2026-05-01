// Copyright 2024 Dolthub, Inc.
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

// initPgTypeof registers the functions to the catalog.
func initPgTypeof() {
	framework.RegisterFunction(pg_typeof_any)
}

// pg_typeof_any represents the PostgreSQL function pg_typeof(any).
var pg_typeof_any = framework.Function1{
	Name:               "pg_typeof",
	Return:             pgtypes.Regtype,
	Parameters:         [1]*pgtypes.DoltgresType{pgtypes.Any},
	IsNonDeterministic: true,
	Strict:             false,
	Callable: func(ctx *sql.Context, t [2]*pgtypes.DoltgresType, val any) (any, error) {
		if t[0] == nil {
			return id.Null, nil
		}
		return t[0].ID.AsId(), nil
	},
}
