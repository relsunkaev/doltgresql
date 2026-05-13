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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/server/auth"
)

// CreateTablespace records minimal PostgreSQL tablespace catalog metadata.
type CreateTablespace struct {
	Name       string
	Owner      string
	Location   string
	HasOptions bool
}

var _ sql.ExecSourceRel = (*CreateTablespace)(nil)
var _ vitess.Injectable = (*CreateTablespace)(nil)

// NewCreateTablespace returns a new *CreateTablespace.
func NewCreateTablespace(name string, owner string, location string, hasOptions bool) *CreateTablespace {
	return &CreateTablespace{
		Name:       name,
		Owner:      owner,
		Location:   location,
		HasOptions: hasOptions,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (c *CreateTablespace) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (c *CreateTablespace) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (c *CreateTablespace) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (c *CreateTablespace) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	if c.HasOptions {
		return nil, fmt.Errorf("CREATE TABLESPACE storage parameters are not yet supported")
	}
	if c.Location != "" {
		return nil, fmt.Errorf("could not set permissions on directory %q", c.Location)
	}
	value, err := ctx.GetSessionVariable(ctx, "allow_in_place_tablespaces")
	if err != nil {
		return nil, err
	}
	if value != int8(1) {
		return nil, fmt.Errorf("empty tablespace location is not supported without allow_in_place_tablespaces")
	}

	owner := c.Owner
	if owner == "" {
		owner = ctx.Client().User
	}

	auth.LockWrite(func() {
		if _, ok := auth.GetTablespace(c.Name); ok {
			err = fmt.Errorf(`tablespace "%s" already exists`, c.Name)
			return
		}
		if !auth.RoleExists(owner) {
			err = fmt.Errorf(`role "%s" does not exist`, owner)
			return
		}
		auth.SetTablespace(auth.Tablespace{Name: c.Name, Owner: owner})
		err = auth.PersistChanges()
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (c *CreateTablespace) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (c *CreateTablespace) String() string {
	return "CREATE TABLESPACE"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (c *CreateTablespace) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (c *CreateTablespace) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}
