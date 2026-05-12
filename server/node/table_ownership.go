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

package node

import (
	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/tablemetadata"
)

func checkTableOwnership(ctx *sql.Context, tableName doltdb.TableName) error {
	owner, err := tableOwner(ctx, tableName)
	if err != nil {
		return err
	}
	if owner == "" || owner == ctx.Client().User {
		return nil
	}
	var userRole auth.Role
	auth.LockRead(func() {
		userRole = auth.GetRole(ctx.Client().User)
	})
	if userRole.IsValid() && userRole.IsSuperUser {
		return nil
	}
	return errors.Errorf("must be owner of table %s", tableName.Name)
}

func tableOwner(ctx *sql.Context, tableName doltdb.TableName) (string, error) {
	schemaName, err := core.GetSchemaName(ctx, nil, tableName.Schema)
	if err != nil {
		return "", err
	}
	table, err := core.GetSqlTableFromContext(ctx, "", doltdb.TableName{Name: tableName.Name, Schema: schemaName})
	if err != nil {
		return "", err
	}
	owner := tablemetadata.Owner(tableComment(table))
	if owner == "" {
		owner = "postgres"
	}
	return owner, nil
}
