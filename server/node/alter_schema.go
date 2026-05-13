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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/server/auth"
)

// AlterSchema implements ALTER SCHEMA.
type AlterSchema struct {
	SchemaName string
	Owner      string
	NewName    string
}

var _ sql.ExecSourceRel = (*AlterSchema)(nil)
var _ vitess.Injectable = (*AlterSchema)(nil)

// NewAlterSchemaOwner returns a new *AlterSchema for ALTER SCHEMA ... OWNER TO.
func NewAlterSchemaOwner(schema string, owner string) *AlterSchema {
	return &AlterSchema{SchemaName: schema, Owner: owner}
}

// NewAlterSchemaRename returns a new *AlterSchema for ALTER SCHEMA ... RENAME TO.
func NewAlterSchemaRename(schema string, newName string) *AlterSchema {
	return &AlterSchema{SchemaName: schema, NewName: newName}
}

// Children implements the interface sql.ExecSourceRel.
func (a *AlterSchema) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (a *AlterSchema) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (a *AlterSchema) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (a *AlterSchema) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	sdb, err := currentSchemaDDLDatabase(ctx)
	if err != nil {
		return nil, err
	}
	databaseSchema, exists, err := sdb.GetSchema(ctx, a.SchemaName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, sql.ErrDatabaseSchemaNotFound.New(a.SchemaName)
	}
	if a.NewName != "" {
		if err = a.renameSchema(ctx, sdb, databaseSchema.SchemaName()); err != nil {
			return nil, err
		}
		return sql.RowsToRowIter(sql.Row{types.OkResult{RowsAffected: 1}}), nil
	}

	auth.LockWrite(func() {
		err = a.checkOwnership(ctx, true)
		if err != nil {
			return
		}
		auth.SetSchemaOwner(a.SchemaName, a.Owner)
		err = auth.PersistChanges()
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(sql.Row{types.OkResult{RowsAffected: 1}}), nil
}

type rootSchemaDatabase interface {
	sql.SchemaDatabase
	GetRoot(ctx *sql.Context) (doltdb.RootValue, error)
	SetRoot(ctx *sql.Context, newRoot doltdb.RootValue) error
}

func (a *AlterSchema) renameSchema(ctx *sql.Context, sdb sql.SchemaDatabase, schemaName string) error {
	if _, exists, err := sdb.GetSchema(ctx, a.NewName); err != nil {
		return err
	} else if exists {
		return sql.ErrDatabaseSchemaExists.New(a.NewName)
	}
	db, ok := sdb.(rootSchemaDatabase)
	if !ok {
		return errors.Errorf("database %s does not support schema rename", ctx.GetCurrentDatabase())
	}
	if err := a.checkOwnership(ctx, false); err != nil {
		return err
	}
	root, err := db.GetRoot(ctx)
	if err != nil {
		return err
	}
	tableNames, err := root.GetTableNames(ctx, schemaName, true)
	if err != nil {
		return err
	}
	root, err = root.CreateDatabaseSchema(ctx, schema.DatabaseSchema{Name: a.NewName})
	if err != nil {
		return err
	}
	for _, tableName := range tableNames {
		root, err = root.RenameTable(ctx,
			doltdb.TableName{Schema: schemaName, Name: tableName},
			doltdb.TableName{Schema: a.NewName, Name: tableName},
		)
		if err != nil {
			return err
		}
	}
	root, err = root.DropDatabaseSchema(ctx, schema.DatabaseSchema{Name: schemaName})
	if err != nil {
		return err
	}
	if err = db.SetRoot(ctx, root); err != nil {
		return err
	}
	auth.LockWrite(func() {
		auth.RenameSchemaOwner(schemaName, a.NewName)
		auth.RenameSchemaPrivileges(schemaName, a.NewName)
		auth.RenameTableSchemaPrivileges(schemaName, a.NewName)
		for _, tableName := range tableNames {
			auth.RenameRelationOwner(
				doltdb.TableName{Schema: schemaName, Name: tableName},
				doltdb.TableName{Schema: a.NewName, Name: tableName},
			)
		}
		err = auth.PersistChanges()
	})
	return err
}

func (a *AlterSchema) checkOwnership(ctx *sql.Context, validateOwner bool) error {
	if validateOwner && !auth.RoleExists(a.Owner) {
		return errors.Errorf(`role "%s" does not exist`, a.Owner)
	}
	role := auth.GetRole(ctx.Client().User)
	if role.IsValid() && role.IsSuperUser {
		return nil
	}
	if auth.SchemaOwnedByRole(a.SchemaName, ctx.Client().User) {
		return nil
	}
	return errors.Errorf("must be owner of schema %s", a.SchemaName)
}

// Schema implements the interface sql.ExecSourceRel.
func (a *AlterSchema) Schema(ctx *sql.Context) sql.Schema {
	return types.OkResultSchema
}

// String implements the interface sql.ExecSourceRel.
func (a *AlterSchema) String() string {
	return "ALTER SCHEMA"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (a *AlterSchema) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (a *AlterSchema) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}
