// Copyright 2024 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/comments"
	"github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/rowsecurity"
	"github.com/dolthub/doltgresql/server/tablemetadata"
)

// DropTable is a node that implements functionality specifically relevant to Doltgres' table dropping needs.
type DropTable struct {
	gmsDropTable *plan.DropTable
	cascade      bool
}

var _ sql.ExecBuilderNode = (*DropTable)(nil)

// NewDropTable returns a new *DropTable.
func NewDropTable(dropTable *plan.DropTable, cascade bool) *DropTable {
	return &DropTable{
		gmsDropTable: dropTable,
		cascade:      cascade,
	}
}

// Children implements the interface sql.ExecBuilderNode.
func (c *DropTable) Children() []sql.Node {
	return c.gmsDropTable.Children()
}

// IsReadOnly implements the interface sql.ExecBuilderNode.
func (c *DropTable) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecBuilderNode.
func (c *DropTable) Resolved() bool {
	return c.gmsDropTable != nil && c.gmsDropTable.Resolved()
}

// BuildRowIter implements the interface sql.ExecBuilderNode.
func (c *DropTable) BuildRowIter(ctx *sql.Context, b sql.NodeExecBuilder, r sql.Row) (sql.RowIter, error) {
	dropTables, err := c.inheritanceExpandedDropTables(ctx)
	if err != nil {
		return nil, err
	}

	targets := make([]dropTableTarget, 0, len(dropTables))
	for _, table := range dropTables {
		var dbName string
		var schemaName string
		var tableName string
		switch table := table.(type) {
		case *plan.ResolvedTable:
			schemaName, err = core.GetSchemaName(ctx, table.Database(), "")
			if err != nil {
				return nil, err
			}
			tableName = table.Name()
			dbName = table.Database().Name()
		default:
			return nil, errors.Errorf("encountered unexpected table type `%T` during DROP TABLE", table)
		}

		doltTableName := doltdb.TableName{Schema: schemaName, Name: tableName}
		if err := checkTableOwnership(ctx, doltTableName); err != nil {
			return nil, errors.Wrap(err, "permission denied")
		}

		tableID := id.NewTable(doltTableName.Schema, doltTableName.Name).AsId()
		if err := id.ValidateOperation(ctx, id.Section_Table, id.Operation_Delete, dbName, tableID, id.Null); err != nil {
			return nil, err
		}
		targets = append(targets, dropTableTarget{dbName: dbName, tableID: tableID, relation: doltTableName})
	}

	rewritten := *c.gmsDropTable
	rewritten.Tables = dropTables
	gmsDropTable := &rewritten
	dropTableIter, err := b.Build(ctx, gmsDropTable, r)
	if err != nil {
		return nil, err
	}

	for _, target := range targets {
		if err = id.PerformOperation(ctx, id.Section_Table, id.Operation_Delete, target.dbName, target.tableID, id.Null); err != nil {
			return nil, err
		}
		comments.RemoveObject(target.tableID, "pg_class")
		rowsecurity.DropTable(uint32(ctx.Session.ID()), target.dbName, target.relation.Schema, target.relation.Name)
	}
	if len(targets) > 0 {
		var persistErr error
		auth.LockWrite(func() {
			for _, target := range targets {
				auth.RemoveRelationOwner(target.relation)
				auth.RemoveAllTablePrivileges(target.relation)
			}
			persistErr = auth.PersistChanges()
		})
		if persistErr != nil {
			return nil, persistErr
		}
	}
	return dropTableIter, err
}

func (c *DropTable) inheritanceExpandedDropTables(ctx *sql.Context) ([]sql.Node, error) {
	explicitKeys := make(map[string]struct{}, len(c.gmsDropTable.Tables))
	for _, table := range c.gmsDropTable.Tables {
		resolved, ok := table.(*plan.ResolvedTable)
		if !ok {
			continue
		}
		key, err := dropTableKey(ctx, resolved)
		if err != nil {
			return nil, err
		}
		explicitKeys[key] = struct{}{}
	}

	seen := make(map[string]struct{}, len(c.gmsDropTable.Tables))
	ret := make([]sql.Node, 0, len(c.gmsDropTable.Tables))
	for _, table := range c.gmsDropTable.Tables {
		resolved, ok := table.(*plan.ResolvedTable)
		if !ok {
			ret = append(ret, table)
			continue
		}
		key, err := dropTableKey(ctx, resolved)
		if err != nil {
			return nil, err
		}
		children, err := inheritedDropTableDescendants(ctx, resolved, map[string]struct{}{key: {}})
		if err != nil {
			return nil, err
		}
		for _, child := range children {
			childKey, err := dropTableKey(ctx, child)
			if err != nil {
				return nil, err
			}
			if _, explicit := explicitKeys[childKey]; !c.cascade && !explicit {
				return nil, errors.Errorf("cannot drop table %s because table %s depends on table %s", resolved.Name(), child.Name(), resolved.Name())
			}
			if _, ok := seen[childKey]; ok {
				continue
			}
			seen[childKey] = struct{}{}
			ret = append(ret, child)
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		ret = append(ret, resolved)
	}
	return ret, nil
}

func inheritedDropTableDescendants(ctx *sql.Context, parent *plan.ResolvedTable, seen map[string]struct{}) ([]*plan.ResolvedTable, error) {
	parentSchema, err := core.GetSchemaName(ctx, parent.Database(), "")
	if err != nil {
		return nil, err
	}
	parentRef := tablemetadata.InheritedTable{Schema: parentSchema, Name: parent.Name()}
	var children []*plan.ResolvedTable
	err = functions.IterateCurrentDatabase(ctx, functions.Callbacks{
		Table: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable) (cont bool, err error) {
			childRef := tablemetadata.InheritedTable{Schema: schema.Item.SchemaName(), Name: table.Item.Name()}
			childKey := inheritedDropTableKey(childRef)
			if _, ok := seen[childKey]; ok {
				return true, nil
			}
			for _, inheritedParent := range tablemetadata.Inherits(inheritedDropTableComment(table.Item)) {
				if inheritedParent.Schema == "" {
					inheritedParent.Schema = schema.Item.SchemaName()
				}
				if !inheritedDropTableParentMatches(inheritedParent, parentRef) {
					continue
				}
				seen[childKey] = struct{}{}
				child := plan.NewResolvedTable(table.Item, schema.Item, nil)
				grandchildren, err := inheritedDropTableDescendants(ctx, child, seen)
				if err != nil {
					return false, err
				}
				children = append(children, grandchildren...)
				children = append(children, child)
				break
			}
			return true, nil
		},
	})
	return children, err
}

func dropTableKey(ctx *sql.Context, table *plan.ResolvedTable) (string, error) {
	schemaName, err := core.GetSchemaName(ctx, table.Database(), "")
	if err != nil {
		return "", err
	}
	return inheritedDropTableKey(tablemetadata.InheritedTable{Schema: schemaName, Name: table.Name()}), nil
}

func inheritedDropTableComment(table sql.Table) string {
	for table != nil {
		if commented, ok := table.(sql.CommentedTable); ok {
			return commented.Comment()
		}
		wrapper, ok := table.(sql.TableWrapper)
		if !ok {
			return ""
		}
		table = wrapper.Underlying()
	}
	return ""
}

func inheritedDropTableParentMatches(left tablemetadata.InheritedTable, right tablemetadata.InheritedTable) bool {
	return strings.EqualFold(left.Schema, right.Schema) && strings.EqualFold(left.Name, right.Name)
}

func inheritedDropTableKey(table tablemetadata.InheritedTable) string {
	return strings.ToLower(table.Schema) + "." + strings.ToLower(table.Name)
}

type dropTableTarget struct {
	dbName   string
	tableID  id.Id
	relation doltdb.TableName
}

// Schema implements the interface sql.ExecBuilderNode.
func (c *DropTable) Schema(ctx *sql.Context) sql.Schema {
	return c.gmsDropTable.Schema(ctx)
}

// String implements the interface sql.ExecBuilderNode.
func (c *DropTable) String() string {
	return c.gmsDropTable.String()
}

// WithChildren implements the interface sql.ExecBuilderNode.
func (c *DropTable) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	gmsDropTable, err := c.gmsDropTable.WithChildren(ctx, children...)
	if err != nil {
		return nil, err
	}
	return &DropTable{
		gmsDropTable: gmsDropTable.(*plan.DropTable),
		cascade:      c.cascade,
	}, nil
}
