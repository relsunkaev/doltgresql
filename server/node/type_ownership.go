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
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/auth"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func checkTypeOwnership(ctx *sql.Context, typ *pgtypes.DoltgresType) error {
	if typeOwnedByCurrentUser(ctx, typ) {
		return nil
	}
	return errors.Errorf("must be owner of type %s", typ.Name())
}

func typeOwnedByCurrentUser(ctx *sql.Context, typ *pgtypes.DoltgresType) bool {
	if typ == nil {
		return false
	}
	owner := typ.Owner
	if owner == "" {
		owner = "postgres"
	}
	if owner == ctx.Client().User {
		return true
	}
	var userRole auth.Role
	auth.LockRead(func() {
		userRole = auth.GetRole(ctx.Client().User)
	})
	return userRole.IsValid() && userRole.IsSuperUser
}

func currentUserOwnsTypeID(ctx *sql.Context, typeID id.Type) (bool, error) {
	collection, err := core.GetTypesCollectionFromContext(ctx)
	if err != nil {
		return false, err
	}
	typ, err := collection.GetType(ctx, typeID)
	if err != nil {
		return false, err
	}
	return typeOwnedByCurrentUser(ctx, typ), nil
}
