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

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/server/auth"
)

// AlterDefaultPrivileges handles ALTER DEFAULT PRIVILEGES.
type AlterDefaultPrivileges struct {
	OwnerRoles      []string
	Schemas         []string
	Object          auth.PrivilegeObject
	Privileges      []auth.Privilege
	Grantees        []string
	Grant           bool
	WithGrantOption bool
	GrantOptionOnly bool
}

var _ sql.ExecSourceRel = (*AlterDefaultPrivileges)(nil)
var _ vitess.Injectable = (*AlterDefaultPrivileges)(nil)

// Children implements the interface sql.ExecSourceRel.
func (a *AlterDefaultPrivileges) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (a *AlterDefaultPrivileges) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (a *AlterDefaultPrivileges) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (a *AlterDefaultPrivileges) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	schemas, err := a.resolvedSchemas(ctx)
	if err != nil {
		return nil, err
	}
	ownerNames := a.OwnerRoles
	if len(ownerNames) == 0 {
		owner := ctx.Client().User
		if owner == "" {
			owner = "postgres"
		}
		ownerNames = []string{owner}
	}

	auth.LockWrite(func() {
		for _, ownerName := range ownerNames {
			owner := auth.GetRole(ownerName)
			if !owner.IsValid() {
				err = errors.Errorf(`role "%s" does not exist`, ownerName)
				return
			}
			for _, granteeName := range a.Grantees {
				grantee := auth.GetRole(granteeName)
				if !grantee.IsValid() {
					err = errors.Errorf(`role "%s" does not exist`, granteeName)
					return
				}
				for _, schema := range schemas {
					key := auth.DefaultPrivilegeKey{
						Owner:   owner.ID(),
						Schema:  schema,
						Object:  a.Object,
						Grantee: grantee.ID(),
					}
					for _, privilege := range a.Privileges {
						grantedPrivilege := auth.GrantedPrivilege{
							Privilege: privilege,
							GrantedBy: owner.ID(),
						}
						if a.Grant {
							auth.AddDefaultPrivilege(key, grantedPrivilege, a.WithGrantOption)
						} else {
							auth.RemoveDefaultPrivilege(key, grantedPrivilege, a.GrantOptionOnly)
						}
					}
				}
			}
		}
		if err == nil {
			err = auth.PersistChanges()
		}
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (a *AlterDefaultPrivileges) resolvedSchemas(ctx *sql.Context) ([]string, error) {
	if len(a.Schemas) == 0 {
		return []string{""}, nil
	}
	schemas := make([]string, len(a.Schemas))
	for i, schema := range a.Schemas {
		resolved, err := core.GetSchemaName(ctx, nil, schema)
		if err != nil {
			return nil, err
		}
		schemas[i] = resolved
	}
	return schemas, nil
}

// Schema implements the interface sql.ExecSourceRel.
func (a *AlterDefaultPrivileges) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (a *AlterDefaultPrivileges) String() string {
	return "ALTER DEFAULT PRIVILEGES"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (a *AlterDefaultPrivileges) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (a *AlterDefaultPrivileges) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}
