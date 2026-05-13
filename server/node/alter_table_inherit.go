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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/server/tablemetadata"
)

// AlterTableInherit handles ALTER TABLE ... INHERIT/NO INHERIT.
type AlterTableInherit struct {
	child  alterTableStorageTarget
	parent alterTableStorageTarget
	attach bool
}

var _ sql.ExecSourceRel = (*AlterTableInherit)(nil)
var _ vitess.Injectable = (*AlterTableInherit)(nil)

// NewAlterTableInherit returns a new *AlterTableInherit.
func NewAlterTableInherit(ifExists bool, schema string, table string, parentSchema string, parentTable string, attach bool) *AlterTableInherit {
	return &AlterTableInherit{
		child: alterTableStorageTarget{
			ifExists: ifExists,
			schema:   schema,
			table:    table,
		},
		parent: alterTableStorageTarget{
			schema: parentSchema,
			table:  parentTable,
		},
		attach: attach,
	}
}

func (a *AlterTableInherit) Children() []sql.Node {
	return nil
}

func (a *AlterTableInherit) IsReadOnly() bool {
	return false
}

func (a *AlterTableInherit) Resolved() bool {
	return true
}

func (a *AlterTableInherit) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	child, childSchema, err := resolveAlterTableInheritTable(ctx, a.child)
	if err != nil {
		return nil, err
	}
	if child == nil {
		return sql.RowsToRowIter(), nil
	}
	parent, parentSchema, err := resolveAlterTableInheritTable(ctx, a.parent)
	if err != nil {
		return nil, err
	}
	if parent == nil {
		return nil, sql.ErrTableNotFound.New(a.parent.table)
	}
	commented, ok := child.(sql.CommentedTable)
	if !ok {
		return nil, sql.ErrAlterTableCommentNotSupported.New(child.Name())
	}
	alterable, ok := child.(sql.CommentAlterableTable)
	if !ok {
		return nil, sql.ErrAlterTableCommentNotSupported.New(child.Name())
	}
	parentRef := tablemetadata.InheritedTable{Schema: parentSchema, Name: parent.Name()}
	parents := tablemetadata.Inherits(commented.Comment())
	if a.attach {
		if err = validateInheritedTableCompatibility(ctx, child, parent); err != nil {
			return nil, err
		}
		if !containsInheritedParent(parents, parentRef, childSchema) {
			parents = append(parents, parentRef)
		}
	} else {
		var removed bool
		parents, removed = removeInheritedParent(parents, parentRef, childSchema)
		if !removed {
			return nil, errors.Errorf(`relation "%s" is not a parent of relation "%s"`, parent.Name(), child.Name())
		}
	}
	return sql.RowsToRowIter(), alterable.ModifyComment(ctx, tablemetadata.SetInherits(commented.Comment(), parents))
}

func (a *AlterTableInherit) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

func (a *AlterTableInherit) String() string {
	if a.attach {
		return "ALTER TABLE INHERIT"
	}
	return "ALTER TABLE NO INHERIT"
}

func (a *AlterTableInherit) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

func (a *AlterTableInherit) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}

func resolveAlterTableInheritTable(ctx *sql.Context, target alterTableStorageTarget) (sql.Table, string, error) {
	if target.schema != "" {
		table, err := core.GetSqlTableFromContext(ctx, "", doltdb.TableName{Name: target.table, Schema: target.schema})
		if err != nil {
			return nil, "", err
		}
		if table == nil && !target.ifExists {
			return nil, "", sql.ErrTableNotFound.New(target.table)
		}
		return table, target.schema, nil
	}
	searchPaths, err := core.SearchPath(ctx)
	if err != nil {
		return nil, "", err
	}
	for _, schema := range searchPaths {
		table, err := core.GetSqlTableFromContext(ctx, "", doltdb.TableName{Name: target.table, Schema: schema})
		if err != nil {
			return nil, "", err
		}
		if table != nil {
			return table, schema, nil
		}
	}
	if target.ifExists {
		return nil, "", nil
	}
	return nil, "", errors.Errorf(`relation "%s" does not exist`, target.table)
}

func validateInheritedTableCompatibility(ctx *sql.Context, child sql.Table, parent sql.Table) error {
	childColumns := make(map[string]*sql.Column)
	for _, column := range child.Schema(ctx) {
		if column.HiddenSystem {
			continue
		}
		childColumns[strings.ToLower(column.Name)] = column
	}
	for _, parentColumn := range parent.Schema(ctx) {
		if parentColumn.HiddenSystem {
			continue
		}
		childColumn, ok := childColumns[strings.ToLower(parentColumn.Name)]
		if !ok {
			return errors.Errorf(`child table is missing column "%s"`, parentColumn.Name)
		}
		if !childColumn.Type.Equals(parentColumn.Type) {
			return errors.Errorf(`child table column "%s" has a different type`, parentColumn.Name)
		}
	}
	return nil
}
