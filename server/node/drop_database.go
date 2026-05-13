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
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/comments"
)

// DropDatabase is a node that implements PostgreSQL ownership checks before dropping a database.
type DropDatabase struct {
	gmsDropDB *plan.DropDB
}

var _ sql.ExecBuilderNode = (*DropDatabase)(nil)

// NewDropDatabase returns a new *DropDatabase.
func NewDropDatabase(dropDB *plan.DropDB) *DropDatabase {
	return &DropDatabase{
		gmsDropDB: dropDB,
	}
}

// Children implements the interface sql.ExecBuilderNode.
func (d *DropDatabase) Children() []sql.Node {
	return d.gmsDropDB.Children()
}

// IsReadOnly implements the interface sql.ExecBuilderNode.
func (d *DropDatabase) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecBuilderNode.
func (d *DropDatabase) Resolved() bool {
	return d.gmsDropDB != nil && d.gmsDropDB.Resolved()
}

// BuildRowIter implements the interface sql.ExecBuilderNode.
func (d *DropDatabase) BuildRowIter(ctx *sql.Context, b sql.NodeExecBuilder, r sql.Row) (sql.RowIter, error) {
	if err := rejectDatabaseDDLInTransaction(ctx, "DROP DATABASE"); err != nil {
		return nil, err
	}
	if strings.EqualFold(d.gmsDropDB.DbName, ctx.GetCurrentDatabase()) {
		return nil, errors.New("cannot drop the currently open database")
	}
	if d.gmsDropDB.Catalog.HasDatabase(ctx, d.gmsDropDB.DbName) {
		if err := checkDatabaseOwnership(ctx, d.gmsDropDB.DbName); err != nil {
			return nil, errors.Wrap(err, "permission denied")
		}
	}
	iter, err := b.Build(ctx, d.gmsDropDB, r)
	if err != nil {
		return nil, err
	}
	comments.RemoveObject(id.NewDatabase(d.gmsDropDB.DbName).AsId(), "pg_database")
	auth.LockWrite(func() {
		auth.RemoveDatabaseMetadata(d.gmsDropDB.DbName)
		auth.RemoveAllDatabasePrivileges(d.gmsDropDB.DbName)
		err = auth.PersistChanges()
	})
	if err != nil {
		return nil, err
	}
	return iter, nil
}

func checkDatabaseOwnership(ctx *sql.Context, dbName string) error {
	var owner string
	auth.LockRead(func() {
		owner = auth.GetDatabaseMetadata(dbName).Owner
	})
	if owner == "" || owner == ctx.Client().User {
		return nil
	}
	var userRole auth.Role
	auth.LockRead(func() {
		userRole = auth.GetRole(ctx.Client().User)
	})
	if userRole.IsValid() && userRole.IsSuperUser {
		return nil
	}
	return errors.Errorf("must be owner of database %s", dbName)
}

// Schema implements the interface sql.ExecBuilderNode.
func (d *DropDatabase) Schema(ctx *sql.Context) sql.Schema {
	return d.gmsDropDB.Schema(ctx)
}

// String implements the interface sql.ExecBuilderNode.
func (d *DropDatabase) String() string {
	return d.gmsDropDB.String()
}

// WithChildren implements the interface sql.ExecBuilderNode.
func (d *DropDatabase) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	gmsDropDB, err := d.gmsDropDB.WithChildren(ctx, children...)
	if err != nil {
		return nil, err
	}
	return &DropDatabase{
		gmsDropDB: gmsDropDB.(*plan.DropDB),
	}, nil
}
