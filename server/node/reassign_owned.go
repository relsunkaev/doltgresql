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
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/server/auth"
)

// ReassignOwned handles REASSIGN OWNED statements.
type ReassignOwned struct {
	OldRoles []string
	NewRole  string
}

var _ sql.ExecSourceRel = (*ReassignOwned)(nil)
var _ vitess.Injectable = (*ReassignOwned)(nil)

// Children implements sql.ExecSourceRel.
func (r *ReassignOwned) Children() []sql.Node {
	return nil
}

// IsReadOnly implements sql.ExecSourceRel.
func (r *ReassignOwned) IsReadOnly() bool {
	return false
}

// Resolved implements sql.ExecSourceRel.
func (r *ReassignOwned) Resolved() bool {
	return true
}

// RowIter implements sql.ExecSourceRel.
func (r *ReassignOwned) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	currentUser := ctx.Client().User
	newRoleName := resolveReassignOwnedRoleSpec(currentUser, r.NewRole)
	var err error
	auth.LockRead(func() {
		userRole := auth.GetRole(currentUser)
		if !userRole.IsValid() {
			err = errors.Errorf(`role "%s" does not exist`, currentUser)
			return
		}
		newRole := auth.GetRole(newRoleName)
		if !newRole.IsValid() {
			err = errors.Errorf(`role "%s" does not exist`, newRoleName)
			return
		}
		if !roleCanOperateAsOwner(userRole, newRoleName) {
			err = errors.Errorf("permission denied to reassign objects")
			return
		}
		for _, oldRoleSpec := range r.OldRoles {
			oldRoleName := resolveReassignOwnedRoleSpec(currentUser, oldRoleSpec)
			oldRole := auth.GetRole(oldRoleName)
			if !oldRole.IsValid() {
				err = errors.Errorf(`role "%s" does not exist`, oldRoleName)
				return
			}
			if !roleCanOperateAsOwner(userRole, oldRoleName) {
				err = errors.Errorf("permission denied to reassign objects")
				return
			}
			if oldRole.ID() != newRole.ID() && auth.RoleHasDependencies(oldRole) {
				err = errors.Errorf(`REASSIGN OWNED is not yet supported for roles with dependent objects`)
				return
			}
		}
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements sql.ExecSourceRel.
func (r *ReassignOwned) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements sql.ExecSourceRel.
func (r *ReassignOwned) String() string {
	return "REASSIGN OWNED"
}

// WithChildren implements sql.ExecSourceRel.
func (r *ReassignOwned) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(r, children...)
}

// WithResolvedChildren implements vitess.Injectable.
func (r *ReassignOwned) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return r, nil
}

func resolveReassignOwnedRoleSpec(currentUser, roleSpec string) string {
	switch strings.ToLower(roleSpec) {
	case "current_role", "current_user", "session_user":
		return currentUser
	default:
		return roleSpec
	}
}
