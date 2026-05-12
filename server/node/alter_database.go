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

// AlterDatabase handles PostgreSQL ALTER DATABASE catalog metadata updates.
type AlterDatabase struct {
	Name      string
	Update    auth.DatabaseMetadataUpdate
	SetName   string
	SetValue  string
	ResetName string
	ResetAll  bool
}

var _ sql.ExecSourceRel = (*AlterDatabase)(nil)
var _ vitess.Injectable = (*AlterDatabase)(nil)

// Children implements the interface sql.ExecSourceRel.
func (a *AlterDatabase) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (a *AlterDatabase) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (a *AlterDatabase) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (a *AlterDatabase) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	if err := databaseExists(ctx, a.Name); err != nil {
		return nil, err
	}
	if a.Update.Owner != nil {
		if err := checkDatabaseOwnership(ctx, a.Name); err != nil {
			return nil, errors.Wrap(err, "permission denied")
		}
	}
	var err error
	auth.LockWrite(func() {
		if a.hasMetadataUpdate() {
			if a.Update.Owner != nil && !auth.RoleExists(*a.Update.Owner) {
				err = errors.Errorf(`role "%s" does not exist`, *a.Update.Owner)
				return
			}
			auth.UpdateDatabaseMetadata(a.Name, a.Update)
		}
		if a.SetName != "" {
			auth.SetDbRoleSetting(a.Name, "", a.SetName, a.SetValue)
		}
		if a.ResetAll {
			auth.ResetDbRoleSetting(a.Name, "", "")
		} else if a.ResetName != "" {
			auth.ResetDbRoleSetting(a.Name, "", a.ResetName)
		}
		err = auth.PersistChanges()
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (a *AlterDatabase) hasMetadataUpdate() bool {
	return a.Update.Owner != nil ||
		a.Update.AllowConnections != nil ||
		a.Update.ConnectionLimit != nil ||
		a.Update.IsTemplate != nil
}

func databaseExists(ctx *sql.Context, name string) error {
	provider := dsess.DSessFromSess(ctx.Session).Provider()
	if !provider.HasDatabase(ctx, name) {
		return sql.ErrDatabaseNotFound.New(name)
	}
	return nil
}

// Schema implements the interface sql.ExecSourceRel.
func (a *AlterDatabase) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (a *AlterDatabase) String() string {
	return "ALTER DATABASE"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (a *AlterDatabase) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (a *AlterDatabase) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}

// RenameDatabase handles ALTER DATABASE ... RENAME TO.
type RenameDatabase struct {
	Name    string
	NewName string
}

var _ sql.ExecSourceRel = (*RenameDatabase)(nil)
var _ vitess.Injectable = (*RenameDatabase)(nil)

// Children implements the interface sql.ExecSourceRel.
func (r *RenameDatabase) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (r *RenameDatabase) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (r *RenameDatabase) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (r *RenameDatabase) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	if err := databaseExists(ctx, r.Name); err != nil {
		return nil, err
	}
	provider := dsess.DSessFromSess(ctx.Session).Provider()
	if provider.HasDatabase(ctx, r.NewName) {
		return nil, sql.ErrDatabaseExists.New(r.NewName)
	}
	renamer, ok := provider.(interface {
		RenameDatabase(ctx *sql.Context, oldName string, newName string) error
	})
	if !ok {
		return nil, errors.New("database provider does not support renaming databases")
	}
	if err := renamer.RenameDatabase(ctx, r.Name, r.NewName); err != nil {
		return nil, err
	}
	var err error
	auth.LockWrite(func() {
		auth.RenameDatabaseMetadata(r.Name, r.NewName)
		err = auth.PersistChanges()
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (r *RenameDatabase) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (r *RenameDatabase) String() string {
	return "ALTER DATABASE RENAME"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (r *RenameDatabase) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(r, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (r *RenameDatabase) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return r, nil
}
