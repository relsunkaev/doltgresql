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
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/comments"
	"github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/replicaidentity"
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

	relations := make([]doltdb.TableName, len(targets))
	for i, target := range targets {
		relations[i] = target.relation
	}
	if c.cascade {
		if err = dropCascadeDependentForeignKeys(ctx, relations); err != nil {
			return nil, err
		}
		dependentViews, dependentTables, err := dropCascadeDependentViews(ctx, relations)
		if err != nil {
			return nil, err
		}
		if err = cleanupDroppedViewTargets(ctx, dependentViews); err != nil {
			return nil, err
		}
		targets = append(targets, dependentTables...)
	} else if err = rejectDependentViews(ctx, relations); err != nil {
		return nil, err
	}

	rewritten := *c.gmsDropTable
	rewritten.Tables = dropTables
	gmsDropTable := &rewritten

	restoreTempShadows := c.hideTemporaryShadowsForPersistentDrops(ctx, dropTables)
	defer restoreTempShadows()
	dropTableIter, err := b.Build(ctx, gmsDropTable, r)
	if err != nil {
		return nil, err
	}

	if err = cleanupDroppedTableTargets(ctx, targets); err != nil {
		return nil, err
	}
	return dropTableIter, err
}

func dropCascadeDependentForeignKeys(ctx *sql.Context, relations []doltdb.TableName) error {
	if len(relations) == 0 {
		return nil
	}
	dropSet := make(map[string]struct{}, len(relations))
	for _, relation := range relations {
		dropSet[dropTableForeignKeyRelationKey(relation)] = struct{}{}
	}

	session, root, err := core.GetRootFromContext(ctx)
	if err != nil {
		return err
	}
	fkc, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return err
	}

	var dependentForeignKeys []doltdb.ForeignKey
	for _, fk := range fkc.AllKeys() {
		if _, dropsParent := dropSet[dropTableForeignKeyRelationKey(fk.ReferencedTableName)]; !dropsParent {
			continue
		}
		if _, dropsChild := dropSet[dropTableForeignKeyRelationKey(fk.TableName)]; dropsChild {
			continue
		}
		dependentForeignKeys = append(dependentForeignKeys, fk)
	}
	if len(dependentForeignKeys) == 0 {
		return nil
	}

	fkc.RemoveKeys(dependentForeignKeys...)
	newRoot, err := root.PutForeignKeyCollection(ctx, fkc)
	if err != nil {
		return err
	}
	if err = dropCascadeDependentForeignKeyMetadata(ctx, dependentForeignKeys); err != nil {
		return err
	}
	return session.SetWorkingRoot(ctx, ctx.GetCurrentDatabase(), newRoot)
}

func dropCascadeDependentForeignKeyMetadata(ctx *sql.Context, foreignKeys []doltdb.ForeignKey) error {
	collection, err := core.GetForeignKeyMetadataCollectionFromContext(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}
	for _, fk := range foreignKeys {
		fkID := id.NewForeignKey(fk.TableName.Schema, fk.TableName.Name, fk.Name)
		if err = collection.DeleteMetadata(ctx, fkID); err != nil {
			return err
		}
	}
	return nil
}

func dropTableForeignKeyRelationKey(relation doltdb.TableName) string {
	return strings.ToLower(relation.Schema) + "." + strings.ToLower(relation.Name)
}

func (c *DropTable) hideTemporaryShadowsForPersistentDrops(ctx *sql.Context, dropTables []sql.Node) func() {
	session := dsess.DSessFromSess(ctx.Session)
	type hiddenTempTable struct {
		database string
		table    sql.Table
	}
	hidden := make([]hiddenTempTable, 0)
	seen := make(map[string]struct{}, len(dropTables))
	for _, table := range dropTables {
		resolved, ok := table.(*plan.ResolvedTable)
		if !ok {
			continue
		}
		if tempTarget := temporaryTableForDropTarget(resolved); tempTarget != nil && tempTarget.IsTemporary() {
			continue
		}
		databaseName := resolved.Database().Name()
		tableName := resolved.Name()
		key := strings.ToLower(databaseName) + "." + strings.ToLower(tableName)
		if _, ok = seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		if tempTable, ok := session.GetTemporaryTable(ctx, databaseName, tableName); ok {
			session.DropTemporaryTable(ctx, databaseName, tableName)
			hidden = append(hidden, hiddenTempTable{
				database: databaseName,
				table:    tempTable,
			})
		}
	}
	return func() {
		for i := len(hidden) - 1; i >= 0; i-- {
			session.AddTemporaryTable(ctx, hidden[i].database, hidden[i].table)
		}
	}
}

func temporaryTableForDropTarget(table sql.Table) sql.TemporaryTable {
	switch table := table.(type) {
	case sql.TemporaryTable:
		return table
	case sql.TableWrapper:
		return temporaryTableForDropTarget(table.Underlying())
	case *plan.ResolvedTable:
		return temporaryTableForDropTarget(table.Table)
	default:
		return nil
	}
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
				return nil, pgerror.Newf(pgcode.DependentObjectsStillExist, "cannot drop table %s because table %s depends on table %s", resolved.Name(), child.Name(), resolved.Name())
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

func cleanupDroppedTableTargets(ctx *sql.Context, targets []dropTableTarget) error {
	for _, target := range targets {
		if err := id.PerformOperation(ctx, id.Section_Table, id.Operation_Delete, target.dbName, target.tableID, id.Null); err != nil {
			return err
		}
		comments.RemoveObject(target.tableID, "pg_class")
		if err := replicaidentity.DropTable(target.dbName, target.relation.Schema, target.relation.Name); err != nil {
			return err
		}
		if err := removeDroppedTablePublicationMetadata(ctx, target.relation); err != nil {
			return err
		}
		rowsecurity.DropTable(uint32(ctx.Session.ID()), target.dbName, target.relation.Schema, target.relation.Name)
	}
	if len(targets) == 0 {
		return nil
	}
	var persistErr error
	auth.LockWrite(func() {
		for _, target := range targets {
			auth.RemoveRelationOwner(target.relation)
			auth.RemoveAllTablePrivileges(target.relation)
		}
		persistErr = auth.PersistChanges()
	})
	return persistErr
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
