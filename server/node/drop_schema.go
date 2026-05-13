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

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/comments"
)

// DropSchema wraps GMS DROP SCHEMA execution to clean Doltgres auth metadata.
type DropSchema struct {
	gmsDropSchema *plan.DropSchema
}

var _ sql.ExecBuilderNode = (*DropSchema)(nil)

// NewDropSchema returns a new *DropSchema.
func NewDropSchema(dropSchema *plan.DropSchema) *DropSchema {
	return &DropSchema{gmsDropSchema: dropSchema}
}

// Children implements sql.ExecBuilderNode.
func (d *DropSchema) Children() []sql.Node {
	return d.gmsDropSchema.Children()
}

// IsReadOnly implements sql.ExecBuilderNode.
func (d *DropSchema) IsReadOnly() bool {
	return false
}

// Resolved implements sql.ExecBuilderNode.
func (d *DropSchema) Resolved() bool {
	return d.gmsDropSchema != nil && d.gmsDropSchema.Resolved()
}

// BuildRowIter implements sql.ExecBuilderNode.
func (d *DropSchema) BuildRowIter(ctx *sql.Context, b sql.NodeExecBuilder, r sql.Row) (sql.RowIter, error) {
	iter, err := b.Build(ctx, d.gmsDropSchema, r)
	if err != nil {
		return nil, err
	}
	auth.LockWrite(func() {
		auth.RemoveSchemaOwner(d.gmsDropSchema.DbName)
		auth.RemoveAllSchemaPrivileges(d.gmsDropSchema.DbName)
		err = auth.PersistChanges()
	})
	if err != nil {
		return nil, err
	}
	comments.RemoveObject(id.NewNamespace(d.gmsDropSchema.DbName).AsId(), "pg_namespace")
	return iter, nil
}

// Schema implements sql.ExecBuilderNode.
func (d *DropSchema) Schema(ctx *sql.Context) sql.Schema {
	return d.gmsDropSchema.Schema(ctx)
}

// String implements sql.ExecBuilderNode.
func (d *DropSchema) String() string {
	return d.gmsDropSchema.String()
}

// WithChildren implements sql.ExecBuilderNode.
func (d *DropSchema) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	gmsDropSchema, err := d.gmsDropSchema.WithChildren(ctx, children...)
	if err != nil {
		return nil, err
	}
	return &DropSchema{gmsDropSchema: gmsDropSchema.(*plan.DropSchema)}, nil
}
