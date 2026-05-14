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
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/datas/pull"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/auth"
)

// CreateDatabase handles PostgreSQL CREATE DATABASE options that GMS does not track.
type CreateDatabase struct {
	Name        string
	IfNotExists bool
	Template    string
	Update      auth.DatabaseMetadataUpdate
	gmsCreateDB *plan.CreateDB
}

var _ sql.ExecSourceRel = (*CreateDatabase)(nil)
var _ sql.ExecBuilderNode = (*CreateDatabase)(nil)
var _ vitess.Injectable = (*CreateDatabase)(nil)

// NewCreateDatabase returns a wrapper around GMS CREATE DATABASE execution.
func NewCreateDatabase(createDB *plan.CreateDB) *CreateDatabase {
	return &CreateDatabase{
		Name:        createDB.DbName,
		IfNotExists: createDB.IfNotExists,
		gmsCreateDB: createDB,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (c *CreateDatabase) Children() []sql.Node {
	if c.gmsCreateDB != nil {
		return c.gmsCreateDB.Children()
	}
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (c *CreateDatabase) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (c *CreateDatabase) Resolved() bool {
	if c.gmsCreateDB != nil {
		return c.gmsCreateDB.Resolved()
	}
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (c *CreateDatabase) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	if err := rejectDatabaseDDLInTransaction(ctx, "CREATE DATABASE"); err != nil {
		return nil, err
	}
	if err := checkCreateDatabasePrivilege(ctx); err != nil {
		return nil, err
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
	if err := c.copyTemplateDatabase(ctx, provider); err != nil {
		_ = provider.DropDatabase(ctx, c.Name)
		return nil, err
	}
	update := c.Update
	if update.Owner == nil {
		owner := ctx.Client().User
		update.Owner = &owner
	}
	var err error
	auth.LockWrite(func() {
		auth.UpdateDatabaseMetadata(c.Name, update)
		err = auth.PersistChanges()
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (c *CreateDatabase) copyTemplateDatabase(ctx *sql.Context, provider dsess.DoltDatabaseProvider) error {
	if c.Template == "" || strings.EqualFold(c.Template, "template0") {
		return nil
	}

	source, ok, err := provider.SessionDatabase(ctx, c.Template)
	if err != nil {
		return err
	}
	if !ok {
		return sql.ErrDatabaseNotFound.New(c.Template)
	}
	target, ok, err := provider.SessionDatabase(ctx, c.Name)
	if err != nil {
		return err
	}
	if !ok {
		return sql.ErrDatabaseNotFound.New(c.Name)
	}

	pull.WithDiscardingStatsCh(func(statsCh chan pull.Stats) {
		err = actions.SyncRoots(
			ctx,
			source.DbData().Ddb,
			target.DbData().Ddb,
			provider.FileSystem().TempDir(),
			actions.SyncRootsDBRelationshipUnrelated,
			statsCh,
		)
	})
	if err != nil && !errors.Is(err, pull.ErrDBUpToDate) {
		return err
	}
	return nil
}

// BuildRowIter implements sql.ExecBuilderNode.
func (c *CreateDatabase) BuildRowIter(ctx *sql.Context, b sql.NodeExecBuilder, r sql.Row) (sql.RowIter, error) {
	if c.gmsCreateDB == nil {
		return c.RowIter(ctx, r)
	}
	if err := rejectDatabaseDDLInTransaction(ctx, "CREATE DATABASE"); err != nil {
		return nil, err
	}
	if err := checkCreateDatabasePrivilege(ctx); err != nil {
		return nil, err
	}
	exists := c.gmsCreateDB.Catalog.HasDatabase(ctx, c.gmsCreateDB.DbName)
	iter, err := b.Build(ctx, c.gmsCreateDB, r)
	if err != nil {
		return nil, err
	}
	if !exists {
		owner := ctx.Client().User
		auth.LockWrite(func() {
			auth.UpdateDatabaseMetadata(c.gmsCreateDB.DbName, auth.DatabaseMetadataUpdate{Owner: &owner})
			err = auth.PersistChanges()
		})
		if err != nil {
			return nil, err
		}
	}
	return iter, nil
}

func checkCreateDatabasePrivilege(ctx *sql.Context) error {
	var user auth.Role
	auth.LockRead(func() {
		user = auth.GetRole(ctx.Client().User)
	})
	if !user.IsValid() {
		return errors.Errorf(`role "%s" does not exist`, ctx.Client().User)
	}
	if !user.IsSuperUser && !user.CanCreateDB {
		return errors.Errorf(`permission denied to create database`)
	}
	return nil
}

func rejectDatabaseDDLInTransaction(ctx *sql.Context, statement string) error {
	if ctx.GetIgnoreAutoCommit() {
		return pgerror.Newf(pgcode.ActiveSQLTransaction, "%s cannot run inside a transaction block", statement)
	}
	return nil
}

// Schema implements the interface sql.ExecSourceRel.
func (c *CreateDatabase) Schema(ctx *sql.Context) sql.Schema {
	if c.gmsCreateDB != nil {
		return c.gmsCreateDB.Schema(ctx)
	}
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (c *CreateDatabase) String() string {
	if c.gmsCreateDB != nil {
		return c.gmsCreateDB.String()
	}
	return "CREATE DATABASE"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (c *CreateDatabase) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if c.gmsCreateDB != nil {
		gmsCreateDB, err := c.gmsCreateDB.WithChildren(ctx, children...)
		if err != nil {
			return nil, err
		}
		return NewCreateDatabase(gmsCreateDB.(*plan.CreateDB)), nil
	}
	return plan.NillaryWithChildren(c, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (c *CreateDatabase) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}
