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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// CreateForeignKey wraps GMS foreign key creation so PostgreSQL NOT VALID
// foreign keys can skip the existing-row scan while still enforcing new rows.
type CreateForeignKey struct {
	gmsCreateForeignKey    *plan.CreateForeignKey
	skipExistingValidation bool
}

var _ sql.ExecBuilderNode = (*CreateForeignKey)(nil)
var _ sql.MultiDatabaser = (*CreateForeignKey)(nil)
var _ sql.Databaseable = (*CreateForeignKey)(nil)

// NewCreateForeignKey returns a new *CreateForeignKey.
func NewCreateForeignKey(createForeignKey *plan.CreateForeignKey, skipExistingValidation bool) *CreateForeignKey {
	return &CreateForeignKey{
		gmsCreateForeignKey:    createForeignKey,
		skipExistingValidation: skipExistingValidation,
	}
}

// Children implements sql.ExecBuilderNode.
func (c *CreateForeignKey) Children() []sql.Node {
	return nil
}

// Database implements sql.Databaseable.
func (c *CreateForeignKey) Database() string {
	return c.gmsCreateForeignKey.Database()
}

// DatabaseProvider implements sql.MultiDatabaser.
func (c *CreateForeignKey) DatabaseProvider() sql.DatabaseProvider {
	return c.gmsCreateForeignKey.DatabaseProvider()
}

// IsReadOnly implements sql.ExecBuilderNode.
func (c *CreateForeignKey) IsReadOnly() bool {
	return false
}

// Resolved implements sql.ExecBuilderNode.
func (c *CreateForeignKey) Resolved() bool {
	return c.gmsCreateForeignKey != nil && c.gmsCreateForeignKey.Resolved()
}

// BuildRowIter implements sql.ExecBuilderNode.
func (c *CreateForeignKey) BuildRowIter(ctx *sql.Context, _ sql.NodeExecBuilder, _ sql.Row) (sql.RowIter, error) {
	fkDef := c.gmsCreateForeignKey.FkDef
	db, err := c.gmsCreateForeignKey.DbProvider.Database(ctx, fkDef.Database)
	if err != nil {
		return nil, err
	}
	if fkDef.SchemaName != "" {
		db, err = databaseForSchema(ctx, db, fkDef.SchemaName)
		if err != nil {
			return nil, err
		}
	}
	table, ok, err := db.GetTableInsensitive(ctx, fkDef.Table)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableNotFound.New(fkDef.Table)
	}

	refDb, err := c.gmsCreateForeignKey.DbProvider.Database(ctx, fkDef.ParentDatabase)
	if err != nil {
		return nil, err
	}
	if fkDef.ParentSchema != "" {
		refDb, err = databaseForSchema(ctx, refDb, fkDef.ParentSchema)
		if err != nil {
			return nil, err
		}
	}
	refTable, ok, err := refDb.GetTableInsensitive(ctx, fkDef.ParentTable)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableNotFound.New(fkDef.ParentTable)
	}

	if fkDef.ParentSchema == "" {
		if dst, ok := refTable.(sql.DatabaseSchemaTable); ok {
			fkDef.ParentSchema = dst.DatabaseSchema().SchemaName()
		}
	}

	fkTable, ok := typedTableForeignKeyTable(table)
	if !ok {
		return nil, sql.ErrNoForeignKeySupport.New(fkDef.Table)
	}
	refFkTable, ok := typedTableForeignKeyTable(refTable)
	if !ok {
		return nil, sql.ErrNoForeignKeySupport.New(fkDef.ParentTable)
	}

	fkChecks, err := ctx.GetSessionVariable(ctx, "foreign_key_checks")
	if err != nil {
		return nil, err
	}
	if err = plan.ResolveForeignKey(ctx, fkTable, refFkTable, *fkDef, true, fkChecks.(int8) == 1, !c.skipExistingValidation); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(sql.NewRow(types.NewOkResult(0))), nil
}

// Schema implements sql.ExecBuilderNode.
func (c *CreateForeignKey) Schema(ctx *sql.Context) sql.Schema {
	return c.gmsCreateForeignKey.Schema(ctx)
}

// String implements sql.ExecBuilderNode.
func (c *CreateForeignKey) String() string {
	return c.gmsCreateForeignKey.String()
}

// WithChildren implements sql.ExecBuilderNode.
func (c *CreateForeignKey) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

// WithDatabaseProvider implements sql.MultiDatabaser.
func (c *CreateForeignKey) WithDatabaseProvider(provider sql.DatabaseProvider) (sql.Node, error) {
	gmsCreateForeignKey, err := c.gmsCreateForeignKey.WithDatabaseProvider(provider)
	if err != nil {
		return nil, err
	}
	return &CreateForeignKey{
		gmsCreateForeignKey:    gmsCreateForeignKey.(*plan.CreateForeignKey),
		skipExistingValidation: c.skipExistingValidation,
	}, nil
}
