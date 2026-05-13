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

package information_schema

import (
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/tablemetadata"
)

var relationVisibilityPrivileges = []auth.Privilege{
	auth.Privilege_SELECT,
	auth.Privilege_INSERT,
	auth.Privilege_UPDATE,
	auth.Privilege_DELETE,
	auth.Privilege_TRUNCATE,
	auth.Privilege_REFERENCES,
	auth.Privilege_TRIGGER,
}

var columnVisibilityPrivileges = []auth.Privilege{
	auth.Privilege_SELECT,
	auth.Privilege_INSERT,
	auth.Privilege_UPDATE,
	auth.Privilege_REFERENCES,
}

func schemaVisibleToCurrentUser(ctx *sql.Context, schemaName string) bool {
	user := ctx.Client().User
	if user == "" {
		return true
	}

	var visible bool
	auth.LockRead(func() {
		role := auth.GetRole(user)
		public := auth.GetRole("public")
		visible = role.IsValid() && (role.IsSuperUser || schemaOwnedByRole(role, schemaName) || roleHasSchemaPrivilege(role, schemaName))
		if !visible && public.IsValid() {
			visible = roleHasSchemaPrivilege(public, schemaName)
		}
	})
	return visible
}

func relationVisibleToCurrentUser(ctx *sql.Context, schemaName string, relationName string, table sql.Table) bool {
	user := ctx.Client().User
	if user == "" {
		return true
	}
	owner := relationOwner(schemaName, relationName, table)
	relation := doltdb.TableName{Name: relationName, Schema: schemaName}

	var visible bool
	auth.LockRead(func() {
		role := auth.GetRole(user)
		public := auth.GetRole("public")
		if !role.IsValid() {
			return
		}
		if relationOwnedByRole(role, owner) {
			visible = true
			return
		}
		for _, privilege := range relationVisibilityPrivileges {
			if roleHasTablePrivilege(role, relation, "", privilege) || (public.IsValid() && roleHasTablePrivilege(public, relation, "", privilege)) {
				visible = true
				return
			}
		}
		for _, privilege := range columnVisibilityPrivileges {
			if roleHasAnyColumnPrivilege(role, relation, privilege) || (public.IsValid() && roleHasAnyColumnPrivilege(public, relation, privilege)) {
				visible = true
				return
			}
		}
	})
	return visible
}

func columnVisibleToCurrentUser(ctx *sql.Context, schemaName string, relationName string, columnName string, table sql.Table) bool {
	user := ctx.Client().User
	if user == "" {
		return true
	}
	owner := relationOwner(schemaName, relationName, table)
	relation := doltdb.TableName{Name: relationName, Schema: schemaName}

	var visible bool
	auth.LockRead(func() {
		role := auth.GetRole(user)
		public := auth.GetRole("public")
		if !role.IsValid() {
			return
		}
		if relationOwnedByRole(role, owner) {
			visible = true
			return
		}
		for _, privilege := range relationVisibilityPrivileges {
			if roleHasTablePrivilege(role, relation, "", privilege) || (public.IsValid() && roleHasTablePrivilege(public, relation, "", privilege)) {
				visible = true
				return
			}
		}
		for _, privilege := range columnVisibilityPrivileges {
			if roleHasTablePrivilege(role, relation, columnName, privilege) || (public.IsValid() && roleHasTablePrivilege(public, relation, columnName, privilege)) {
				visible = true
				return
			}
		}
	})
	return visible
}

func relationOwner(schemaName string, relationName string, table sql.Table) string {
	if owner := auth.GetRelationOwner(doltdb.TableName{Name: relationName, Schema: schemaName}); owner != "" {
		return owner
	}
	if table != nil {
		if owner := tablemetadata.Owner(tableComment(table)); owner != "" {
			return owner
		}
	}
	return "postgres"
}

func schemaOwnedByRole(role auth.Role, schemaName string) bool {
	if role.IsSuperUser || auth.SchemaOwnedByRole(schemaName, role.Name) {
		return true
	}
	ownerRole := auth.GetRole(auth.GetSchemaOwner(schemaName))
	if !ownerRole.IsValid() {
		return false
	}
	memberID, inheritsPrivileges, _ := auth.IsRoleAMember(role.ID(), ownerRole.ID())
	return memberID.IsValid() && inheritsPrivileges
}

func relationOwnedByRole(role auth.Role, owner string) bool {
	if role.IsSuperUser || role.Name == owner {
		return true
	}
	ownerRole := auth.GetRole(owner)
	if !ownerRole.IsValid() {
		return false
	}
	memberID, inheritsPrivileges, _ := auth.IsRoleAMember(role.ID(), ownerRole.ID())
	return memberID.IsValid() && inheritsPrivileges
}

func roleHasSchemaPrivilege(role auth.Role, schemaName string) bool {
	key := auth.SchemaPrivilegeKey{Role: role.ID(), Schema: schemaName}
	return auth.HasSchemaPrivilege(key, auth.Privilege_USAGE) || auth.HasSchemaPrivilege(key, auth.Privilege_CREATE)
}

func roleHasTablePrivilege(role auth.Role, relation doltdb.TableName, column string, privilege auth.Privilege) bool {
	return auth.HasTablePrivilege(auth.TablePrivilegeKey{
		Role:   role.ID(),
		Table:  relation,
		Column: column,
	}, privilege)
}

func roleHasAnyColumnPrivilege(role auth.Role, relation doltdb.TableName, privilege auth.Privilege) bool {
	return auth.HasAnyColumnPrivilege(auth.TablePrivilegeKey{
		Role:  role.ID(),
		Table: relation,
	}, privilege)
}
