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
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/functions/framework"
	"github.com/dolthub/doltgresql/server/rowsecurity"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func initRowSecurity() {
	framework.RegisterFunction(row_security_active_regclass)
}

var row_security_active_regclass = framework.Function1{
	Name:               "row_security_active",
	Return:             pgtypes.Bool,
	Parameters:         [1]*pgtypes.DoltgresType{pgtypes.Regclass},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		relationID := val.(id.Id)
		if relationID.Section() != id.Section_Table {
			return false, nil
		}
		state, ok := rowsecurity.Get(ctx.GetCurrentDatabase(), relationID.Segment(0), relationID.Segment(1))
		if !ok || !state.Enabled {
			return false, nil
		}
		role := auth.GetRole(ctx.Client().User)
		return !role.IsSuperUser && !role.CanBypassRowLevelSecurity, nil
	},
}
