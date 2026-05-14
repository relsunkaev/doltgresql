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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/comments"
	"github.com/dolthub/doltgresql/server/replicaidentity"
)

// DropSchema wraps GMS DROP SCHEMA execution to clean Doltgres auth metadata.
type DropSchema struct {
	gmsDropSchema *plan.DropSchema
	Name          string
	IfExists      bool
	Cascade       bool
}

var _ sql.ExecBuilderNode = (*DropSchema)(nil)
var _ sql.ExecSourceRel = (*DropSchema)(nil)
var _ vitess.Injectable = (*DropSchema)(nil)

// NewDropSchema returns a new *DropSchema.
func NewDropSchema(dropSchema *plan.DropSchema) *DropSchema {
	return &DropSchema{gmsDropSchema: dropSchema, Name: dropSchema.DbName, IfExists: dropSchema.IfExists}
}

// NewDropSchemaStatement returns a new PostgreSQL DROP SCHEMA node.
func NewDropSchemaStatement(name string, ifExists bool, cascade bool) *DropSchema {
	return &DropSchema{Name: name, IfExists: ifExists, Cascade: cascade}
}

// Children implements sql.ExecBuilderNode.
func (d *DropSchema) Children() []sql.Node {
	if d.gmsDropSchema == nil {
		return nil
	}
	return d.gmsDropSchema.Children()
}

// IsReadOnly implements sql.ExecBuilderNode.
func (d *DropSchema) IsReadOnly() bool {
	return false
}

// Resolved implements sql.ExecBuilderNode.
func (d *DropSchema) Resolved() bool {
	return d.gmsDropSchema == nil || d.gmsDropSchema.Resolved()
}

// BuildRowIter implements sql.ExecBuilderNode.
func (d *DropSchema) BuildRowIter(ctx *sql.Context, b sql.NodeExecBuilder, r sql.Row) (sql.RowIter, error) {
	if d.gmsDropSchema == nil {
		return d.RowIter(ctx, r)
	}
	iter, err := b.Build(ctx, d.gmsDropSchema, r)
	if err != nil {
		return nil, err
	}
	auth.LockWrite(func() {
		err = removeDroppedSchemaAuthMetadata(d.gmsDropSchema.DbName, nil)
	})
	if err != nil {
		return nil, err
	}
	comments.RemoveObject(id.NewNamespace(d.gmsDropSchema.DbName).AsId(), "pg_namespace")
	return iter, nil
}

// RowIter implements sql.ExecSourceRel.
func (d *DropSchema) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	sdb, err := currentSchemaDDLDatabase(ctx)
	if err != nil {
		return nil, err
	}
	schema, exists, err := sdb.GetSchema(ctx, d.Name)
	if err != nil {
		return nil, err
	}
	rows := []sql.Row{{types.OkResult{RowsAffected: 1}}}
	if !exists {
		if d.IfExists {
			return sql.RowsToRowIter(rows...), nil
		}
		return nil, sql.ErrDatabaseSchemaNotFound.New(d.Name)
	}

	var relations []doltdb.TableName
	if d.Cascade {
		relations, err = dropSchemaContents(ctx, schema)
		if err != nil {
			return nil, err
		}
	}

	if err = sdb.DropSchema(ctx, d.Name); err != nil {
		return nil, err
	}

	if err = removeDroppedSchemaMetadata(ctx, d.Name, relations); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(rows...), nil
}

func dropSchemaContents(ctx *sql.Context, schema sql.DatabaseSchema) ([]doltdb.TableName, error) {
	relations := make([]doltdb.TableName, 0)
	if viewDB, ok := schema.(sql.ViewDatabase); ok {
		views, err := viewDB.AllViews(ctx)
		if err != nil {
			return nil, err
		}
		for _, view := range views {
			if err = viewDB.DropView(ctx, view.Name); err != nil {
				return nil, err
			}
			relation := doltdb.TableName{Schema: schema.SchemaName(), Name: view.Name}
			relations = append(relations, relation)
			comments.RemoveObject(id.NewView(relation.Schema, relation.Name).AsId(), "pg_class")
		}
	}

	tableNames, err := schema.GetTableNames(ctx)
	if err != nil {
		return nil, err
	}
	if len(tableNames) == 0 {
		return relations, nil
	}
	dropper, ok := schema.(sql.TableDropper)
	if !ok {
		return nil, sql.ErrDropTableNotSupported.New(schema.Name())
	}
	for _, tableName := range tableNames {
		if err = dropper.DropTable(ctx, tableName); err != nil {
			return nil, err
		}
		relation := doltdb.TableName{Schema: schema.SchemaName(), Name: tableName}
		relations = append(relations, relation)
		comments.RemoveObject(id.NewTable(relation.Schema, relation.Name).AsId(), "pg_class")
	}
	return relations, nil
}

func removeDroppedSchemaMetadata(ctx *sql.Context, schemaName string, relations []doltdb.TableName) error {
	for _, relation := range relations {
		if err := replicaidentity.DropTable(ctx.GetCurrentDatabase(), relation.Schema, relation.Name); err != nil {
			return err
		}
	}
	var err error
	auth.LockWrite(func() {
		err = removeDroppedSchemaAuthMetadata(schemaName, relations)
	})
	if err != nil {
		return err
	}
	comments.RemoveObject(id.NewNamespace(schemaName).AsId(), "pg_namespace")
	return nil
}

func removeDroppedSchemaAuthMetadata(schemaName string, relations []doltdb.TableName) error {
	auth.RemoveSchemaOwner(schemaName)
	auth.RemoveAllSchemaPrivileges(schemaName)
	auth.RemoveDefaultPrivilegesForSchema(schemaName)
	for _, relation := range relations {
		auth.RemoveRelationOwner(relation)
		auth.RemoveAllTablePrivileges(relation)
	}
	return auth.PersistChanges()
}

// Schema implements sql.ExecBuilderNode.
func (d *DropSchema) Schema(ctx *sql.Context) sql.Schema {
	if d.gmsDropSchema == nil {
		return types.OkResultSchema
	}
	return d.gmsDropSchema.Schema(ctx)
}

// String implements sql.ExecBuilderNode.
func (d *DropSchema) String() string {
	if d.gmsDropSchema == nil {
		return "DROP SCHEMA"
	}
	return d.gmsDropSchema.String()
}

// WithChildren implements sql.ExecBuilderNode.
func (d *DropSchema) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if d.gmsDropSchema == nil {
		if len(children) != 0 {
			return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 0)
		}
		return d, nil
	}
	gmsDropSchema, err := d.gmsDropSchema.WithChildren(ctx, children...)
	if err != nil {
		return nil, err
	}
	return &DropSchema{gmsDropSchema: gmsDropSchema.(*plan.DropSchema)}, nil
}

// WithResolvedChildren implements vitess.Injectable.
func (d *DropSchema) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, errors.Errorf("expected 0 children, found %d", len(children))
	}
	return d, nil
}
