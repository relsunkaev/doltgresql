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

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/accessmethod"
	"github.com/dolthub/doltgresql/server/auth"
)

// CreateAccessMethod implements CREATE ACCESS METHOD.
type CreateAccessMethod struct {
	Name    string
	Type    string
	Handler string
}

var _ sql.ExecSourceRel = (*CreateAccessMethod)(nil)
var _ vitess.Injectable = (*CreateAccessMethod)(nil)

// NewCreateAccessMethod returns a new *CreateAccessMethod.
func NewCreateAccessMethod(name string, typ string, handler string) *CreateAccessMethod {
	return &CreateAccessMethod{
		Name:    name,
		Type:    typ,
		Handler: handler,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (c *CreateAccessMethod) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (c *CreateAccessMethod) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (c *CreateAccessMethod) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (c *CreateAccessMethod) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	if err := requireAccessMethodSuperuser(ctx); err != nil {
		return nil, err
	}
	if err := accessmethod.Register(c.Name, c.Handler, c.Type); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (c *CreateAccessMethod) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (c *CreateAccessMethod) String() string {
	return "CREATE ACCESS METHOD"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (c *CreateAccessMethod) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (c *CreateAccessMethod) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}

// DropAccessMethod implements DROP ACCESS METHOD.
type DropAccessMethod struct {
	Names    []string
	IfExists bool
	Cascade  bool
}

var _ sql.ExecSourceRel = (*DropAccessMethod)(nil)
var _ vitess.Injectable = (*DropAccessMethod)(nil)

// NewDropAccessMethod returns a new *DropAccessMethod.
func NewDropAccessMethod(names []string, ifExists bool, cascade bool) *DropAccessMethod {
	return &DropAccessMethod{
		Names:    names,
		IfExists: ifExists,
		Cascade:  cascade,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (d *DropAccessMethod) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (d *DropAccessMethod) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (d *DropAccessMethod) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (d *DropAccessMethod) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	if err := requireAccessMethodSuperuser(ctx); err != nil {
		return nil, err
	}
	for _, name := range d.Names {
		dropped, err := accessmethod.Drop(name)
		if err != nil {
			return nil, err
		}
		if !dropped && !d.IfExists {
			return nil, errors.Errorf(`access method "%s" does not exist`, name)
		}
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (d *DropAccessMethod) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (d *DropAccessMethod) String() string {
	return "DROP ACCESS METHOD"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (d *DropAccessMethod) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(d, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (d *DropAccessMethod) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return d, nil
}

func requireAccessMethodSuperuser(ctx *sql.Context) error {
	var userRole auth.Role
	auth.LockRead(func() {
		userRole = auth.GetRole(ctx.Client().User)
	})
	if !userRole.IsValid() || !userRole.IsSuperUser {
		return pgerror.New(pgcode.InsufficientPrivilege, "must be superuser")
	}
	return nil
}
