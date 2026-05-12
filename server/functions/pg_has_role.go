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
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initPgHasRole registers the functions to the catalog.
func initPgHasRole() {
	framework.RegisterFunction(pg_has_role_text_text)
	framework.RegisterFunction(pg_has_role_text_oid_text)
	framework.RegisterFunction(pg_has_role_text_text_text)
	framework.RegisterFunction(pg_has_role_regrole_regrole_text)
}

// pg_has_role_text_text represents the PostgreSQL role membership inquiry function.
var pg_has_role_text_text = framework.Function2{
	Name:       "pg_has_role",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, roleName any, privilege any) (any, error) {
		return hasRoleByName(ctx.Client().User, roleName.(string), privilege.(string))
	},
}

// pg_has_role_text_oid_text represents the PostgreSQL role membership inquiry function.
var pg_has_role_text_oid_text = framework.Function3{
	Name:       "pg_has_role",
	Return:     pgtypes.Bool,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Oid, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, userName any, roleOID any, privilege any) (any, error) {
		return hasRoleByOID(userName.(string), roleOID.(id.Id), privilege.(string))
	},
}

// pg_has_role_text_text_text represents the PostgreSQL role membership inquiry function.
var pg_has_role_text_text_text = framework.Function3{
	Name:       "pg_has_role",
	Return:     pgtypes.Bool,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, userName any, roleName any, privilege any) (any, error) {
		return hasRoleByName(userName.(string), roleName.(string), privilege.(string))
	},
}

// pg_has_role_regrole_regrole_text represents the PostgreSQL role membership inquiry function.
var pg_has_role_regrole_regrole_text = framework.Function3{
	Name:       "pg_has_role",
	Return:     pgtypes.Bool,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Regrole, pgtypes.Regrole, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, userRole any, memberRole any, privilege any) (any, error) {
		userName, ok := roleNameFromOID(userRole.(id.Id))
		if !ok {
			return false, nil
		}
		return hasRoleByOID(userName, memberRole.(id.Id), privilege.(string))
	},
}

func hasRoleByOID(userName string, roleOID id.Id, privilege string) (bool, error) {
	roleName, ok := roleNameFromOID(roleOID)
	if !ok {
		return false, nil
	}
	return hasRoleByName(userName, roleName, privilege)
}

func hasRoleByName(userName string, roleName string, privilege string) (bool, error) {
	privilege = strings.ToLower(privilege)
	if privilege != "member" && privilege != "usage" {
		return false, errors.Errorf(`unrecognized privilege type: "%s"`, privilege)
	}

	hasRole := false
	auth.LockRead(func() {
		userRole := auth.GetRole(userName)
		memberRole := auth.GetRole(roleName)
		if !userRole.IsValid() || !memberRole.IsValid() {
			return
		}
		if userRole.IsSuperUser {
			hasRole = true
			return
		}
		groupID, inheritsPrivileges, _ := auth.IsRoleAMember(userRole.ID(), memberRole.ID())
		isMember := groupID.IsValid()
		if privilege == "usage" {
			hasRole = isMember && inheritsPrivileges
		} else {
			hasRole = isMember
		}
	})
	return hasRole, nil
}

func roleNameFromOID(roleOID id.Id) (string, bool) {
	if roleOID.Section() == id.Section_User {
		return roleOID.Segment(0), true
	}
	if id.Cache().ToOID(roleOID) == 10 {
		superUser, _ := auth.GetSuperUserAndPassword()
		return superUser, true
	}
	return "", false
}
