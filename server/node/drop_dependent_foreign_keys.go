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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/doltgresql/core"
)

// DropDependentForeignKeys removes foreign-key constraints that depend on a
// parent constraint being dropped with CASCADE.
type DropDependentForeignKeys struct {
	foreignKeys []sql.ForeignKeyConstraint
}

var _ sql.ExecSourceRel = (*DropDependentForeignKeys)(nil)

// NewDropDependentForeignKeys returns a new *DropDependentForeignKeys.
func NewDropDependentForeignKeys(foreignKeys []sql.ForeignKeyConstraint) *DropDependentForeignKeys {
	copied := make([]sql.ForeignKeyConstraint, len(foreignKeys))
	copy(copied, foreignKeys)
	return &DropDependentForeignKeys{foreignKeys: copied}
}

// Children implements sql.ExecSourceRel.
func (d *DropDependentForeignKeys) Children() []sql.Node {
	return nil
}

// IsReadOnly implements sql.ExecSourceRel.
func (d *DropDependentForeignKeys) IsReadOnly() bool {
	return false
}

// Resolved implements sql.ExecSourceRel.
func (d *DropDependentForeignKeys) Resolved() bool {
	return true
}

// RowIter implements sql.ExecSourceRel.
func (d *DropDependentForeignKeys) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	if err := DropForeignKeys(ctx, d.foreignKeys); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// DropForeignKeys removes the given foreign-key constraints.
func DropForeignKeys(ctx *sql.Context, foreignKeys []sql.ForeignKeyConstraint) error {
	for _, foreignKey := range foreignKeys {
		table, err := core.GetSqlTableFromContext(ctx, foreignKey.Database, doltdb.TableName{
			Name:   foreignKey.Table,
			Schema: foreignKey.SchemaName,
		})
		if err != nil {
			return err
		}
		if table == nil {
			return sql.ErrTableNotFound.New(foreignKey.Table)
		}
		foreignKeyTable, ok := sql.GetUnderlyingTable(table).(sql.ForeignKeyTable)
		if !ok {
			return sql.ErrNoForeignKeySupport.New(foreignKey.Name)
		}
		if err = foreignKeyTable.DropForeignKey(ctx, foreignKey.Name, foreignKey.Table, foreignKey.SchemaName); err != nil {
			return err
		}
	}
	return nil
}

// Schema implements sql.ExecSourceRel.
func (d *DropDependentForeignKeys) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements sql.ExecSourceRel.
func (d *DropDependentForeignKeys) String() string {
	return "DROP DEPENDENT FOREIGN KEYS"
}

// WithChildren implements sql.ExecSourceRel.
func (d *DropDependentForeignKeys) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(d, children...)
}
