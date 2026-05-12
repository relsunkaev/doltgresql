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

// initCurrentUser registers the functions to the catalog.
func initCurrentUser() {
	framework.RegisterFunction(current_user)
	framework.RegisterFunction(session_user)
	framework.RegisterFunction(system_user)
}

// current_user represents the PostgreSQL current_user/current_role SQL value function.
var current_user = framework.Function0{
	Name:               "current_user",
	Return:             pgtypes.Name,
	IsNonDeterministic: true,
	Callable: func(ctx *sql.Context) (any, error) {
		return currentSQLUser(ctx), nil
	},
}

// session_user represents the PostgreSQL session_user SQL value function.
var session_user = framework.Function0{
	Name:               "session_user",
	Return:             pgtypes.Name,
	IsNonDeterministic: true,
	Callable: func(ctx *sql.Context) (any, error) {
		return currentSQLUser(ctx), nil
	},
}

// system_user represents PostgreSQL's SQL value function of the same name.
var system_user = framework.Function0{
	Name:               "system_user",
	Return:             pgtypes.Name,
	IsNonDeterministic: true,
	Callable: func(ctx *sql.Context) (any, error) {
		return currentSQLUser(ctx), nil
	},
}

func currentSQLUser(ctx *sql.Context) string {
	if ctx != nil && ctx.Client().User != "" {
		return ctx.Client().User
	}
	return "postgres"
}
