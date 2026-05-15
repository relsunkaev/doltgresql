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

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/auth"
)

// DropOwned handles DROP OWNED statements.
type DropOwned struct {
	Roles        []string
	DropBehavior tree.DropBehavior
}

var _ sql.ExecSourceRel = (*DropOwned)(nil)
var _ vitess.Injectable = (*DropOwned)(nil)

// Children implements sql.ExecSourceRel.
func (d *DropOwned) Children() []sql.Node {
	return nil
}

// IsReadOnly implements sql.ExecSourceRel.
func (d *DropOwned) IsReadOnly() bool {
	return false
}

// Resolved implements sql.ExecSourceRel.
func (d *DropOwned) Resolved() bool {
	return true
}

// RowIter implements sql.ExecSourceRel.
func (d *DropOwned) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	currentUser := ctx.Client().User
	roles := make([]auth.Role, 0, len(d.Roles))
	var err error
	auth.LockRead(func() {
		userRole := auth.GetRole(currentUser)
		if !userRole.IsValid() {
			err = errors.Errorf(`role "%s" does not exist`, currentUser)
			return
		}
		for _, roleSpec := range d.Roles {
			roleName := resolveDropOwnedRoleSpec(currentUser, roleSpec)
			role := auth.GetRole(roleName)
			if !role.IsValid() {
				err = errors.Errorf(`role "%s" does not exist`, roleName)
				return
			}
			if !roleCanOperateAsOwner(userRole, roleName) {
				err = errors.Errorf("permission denied to drop objects")
				return
			}
			roles = append(roles, role)
		}
	})
	if err != nil {
		return nil, err
	}
	auth.LockWrite(func() {
		for _, role := range roles {
			auth.RemovePrivilegesForRole(role.ID())
		}
		err = auth.PersistChanges()
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements sql.ExecSourceRel.
func (d *DropOwned) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements sql.ExecSourceRel.
func (d *DropOwned) String() string {
	return "DROP OWNED"
}

// WithChildren implements sql.ExecSourceRel.
func (d *DropOwned) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(d, children...)
}

// WithResolvedChildren implements vitess.Injectable.
func (d *DropOwned) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return d, nil
}

func resolveDropOwnedRoleSpec(currentUser, roleSpec string) string {
	switch strings.ToLower(roleSpec) {
	case "current_role", "current_user", "session_user":
		return currentUser
	default:
		return roleSpec
	}
}
