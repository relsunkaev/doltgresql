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
	"github.com/dolthub/doltgresql/server/notifications"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initPgNotify registers the functions to the catalog.
func initPgNotify() {
	framework.RegisterFunction(pg_notify_text_text)
}

// pg_notify_text_text queues a PostgreSQL notification for delivery at the
// current transaction boundary.
var pg_notify_text_text = framework.Function2{
	Name:       "pg_notify",
	Return:     pgtypes.Void,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, channel any, payload any) (any, error) {
		if err := notifications.Queue(ctx.Session.ID(), channel.(string), payload.(string)); err != nil {
			return nil, err
		}
		return "", nil
	},
}
