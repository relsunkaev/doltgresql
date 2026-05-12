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
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/server/auth"
)

// CreateDatabase handles PostgreSQL CREATE DATABASE options that GMS does not track.
type CreateDatabase struct {
	Name        string
	IfNotExists bool
	Update      auth.DatabaseMetadataUpdate
}

var _ sql.ExecSourceRel = (*CreateDatabase)(nil)
var _ vitess.Injectable = (*CreateDatabase)(nil)

// Children implements the interface sql.ExecSourceRel.
func (c *CreateDatabase) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (c *CreateDatabase) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (c *CreateDatabase) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (c *CreateDatabase) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	var user auth.Role
	auth.LockRead(func() {
		user = auth.GetRole(ctx.Client().User)
	})
	if !user.IsValid() {
		return nil, errors.Errorf(`role "%s" does not exist`, ctx.Client().User)
	}
	if !user.IsSuperUser && !user.CanCreateDB {
		return nil, errors.Errorf(`permission denied to create database`)
	}
	if c.Update.Owner != nil {
		var ownerExists bool
		auth.LockRead(func() {
			ownerExists = auth.RoleExists(*c.Update.Owner)
		})
		if !ownerExists {
			return nil, errors.Errorf(`role "%s" does not exist`, *c.Update.Owner)
		}
	}

	provider := dsess.DSessFromSess(ctx.Session).Provider()
	if provider.HasDatabase(ctx, c.Name) {
		if c.IfNotExists {
			return sql.RowsToRowIter(), nil
		}
		return nil, sql.ErrDatabaseExists.New(c.Name)
	}
	if err := provider.CreateDatabase(ctx, c.Name); err != nil {
		return nil, err
	}
	var err error
	auth.LockWrite(func() {
		auth.UpdateDatabaseMetadata(c.Name, c.Update)
		err = auth.PersistChanges()
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (c *CreateDatabase) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (c *CreateDatabase) String() string {
	return "CREATE DATABASE"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (c *CreateDatabase) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (c *CreateDatabase) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}
