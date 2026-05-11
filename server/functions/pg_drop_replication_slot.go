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
	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/functions/framework"
	"github.com/dolthub/doltgresql/server/replsource"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initPgDropReplicationSlot registers the functions to the catalog.
func initPgDropReplicationSlot() {
	framework.RegisterFunction(pg_drop_replication_slot_name)
}

// pg_drop_replication_slot_name represents the PostgreSQL function pg_drop_replication_slot(name).
var pg_drop_replication_slot_name = framework.Function1{
	Name:               "pg_drop_replication_slot",
	Return:             pgtypes.Void,
	Parameters:         [1]*pgtypes.DoltgresType{pgtypes.Name},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		if !auth.CanReplicate(currentSQLUser(ctx)) {
			return nil, errors.Errorf("permission denied to use replication")
		}
		return nil, replsource.DropSlot(val.(string))
	},
}
