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
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	pgfunctions "github.com/dolthub/doltgresql/core/functions"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/types"
)

// AlterTypeRenameAttribute executes ALTER TYPE ... RENAME ATTRIBUTE ... TO ...
// against a composite type stored in the doltgres TypeCollection. The collection
// caches the loaded *DoltgresType in its accessedMap, so mutating CompositeAttrs
// in place causes the rename to be persisted at the next writeCache (which
// happens automatically during transaction commit).
type AlterTypeRenameAttribute struct {
	database string
	schName  string
	typName  string
	oldAttr  string
	newAttr  string
	cascade  bool
}

var _ sql.ExecSourceRel = (*AlterTypeRenameAttribute)(nil)
var _ vitess.Injectable = (*AlterTypeRenameAttribute)(nil)

// NewAlterTypeRenameAttribute returns a new *AlterTypeRenameAttribute.
func NewAlterTypeRenameAttribute(db, sch, typ, oldAttr, newAttr string, cascade bool) *AlterTypeRenameAttribute {
	return &AlterTypeRenameAttribute{
		database: db,
		schName:  sch,
		typName:  typ,
		oldAttr:  oldAttr,
		newAttr:  newAttr,
		cascade:  cascade,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (c *AlterTypeRenameAttribute) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (c *AlterTypeRenameAttribute) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (c *AlterTypeRenameAttribute) Resolved() bool {
	return true
}

// Schema implements the interface sql.ExecSourceRel.
func (c *AlterTypeRenameAttribute) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (c *AlterTypeRenameAttribute) String() string {
	return "ALTER TYPE RENAME ATTRIBUTE"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (c *AlterTypeRenameAttribute) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (c *AlterTypeRenameAttribute) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}

// RowIter implements the interface sql.ExecSourceRel.
func (c *AlterTypeRenameAttribute) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	currentDb := ctx.GetCurrentDatabase()
	if len(c.database) > 0 && c.database != currentDb {
		return nil, errors.Errorf("ALTER TYPE is currently only supported for the current database")
	}
	schema, err := core.GetSchemaName(ctx, nil, c.schName)
	if err != nil {
		return nil, err
	}
	collection, err := core.GetTypesCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	typeID := id.NewType(schema, c.typName)
	typ, err := collection.GetType(ctx, typeID)
	if err != nil {
		return nil, err
	}
	if typ == nil {
		return nil, errors.Errorf(`type "%s" does not exist`, c.typName)
	}
	if typ.TypType != types.TypeType_Composite {
		return nil, errors.Errorf(`"%s" is not a composite type`, c.typName)
	}
	if _, isBuiltIn := types.IDToBuiltInDoltgresType[typ.ID]; isBuiltIn {
		return nil, errors.Errorf(`cannot alter type "%s" because it is a built-in type`, c.typName)
	}
	if err = checkTypeOwnership(ctx, typ); err != nil {
		return nil, err
	}

	oldIdx := -1
	for i, attr := range typ.CompositeAttrs {
		if attr.Name == c.oldAttr {
			oldIdx = i
		}
		if attr.Name == c.newAttr {
			return nil, errors.Errorf(`column "%s" of relation "%s" already exists`, c.newAttr, c.typName)
		}
	}
	if oldIdx < 0 {
		return nil, errors.Errorf(`column "%s" of relation "%s" does not exist`, c.oldAttr, c.typName)
	}

	// Mutating the cached entry persists at the next writeCache, which is
	// invoked automatically during transaction commit. The slice is owned by
	// the cached DoltgresType, so this mutation is visible to subsequent
	// catalog reads in the same session as well.
	typ.CompositeAttrs[oldIdx].Name = c.newAttr
	if err = core.MarkTypesCollectionDirty(ctx, ""); err != nil {
		return nil, err
	}
	if err = c.renameDependentTableColumns(ctx, typ); err != nil {
		return nil, err
	}
	if err = c.renameDependentFunctions(ctx, typ.ID); err != nil {
		return nil, err
	}
	if err = c.renameDependentViews(ctx, typ.ID); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (c *AlterTypeRenameAttribute) renameDependentTableColumns(ctx *sql.Context, updatedType *types.DoltgresType) error {
	_, root, err := core.GetRootFromContext(ctx)
	if err != nil {
		return err
	}
	tableNames, err := root.GetAllTableNames(ctx, false)
	if err != nil {
		return err
	}

	for _, tableName := range tableNames {
		if doltdb.IsSystemTable(tableName) {
			continue
		}
		sqlTable, err := core.GetSqlTableFromContext(ctx, c.database, tableName)
		if err != nil {
			return err
		}
		if sqlTable == nil {
			continue
		}
		alterable, ok := sqlTable.(sql.AlterableTable)
		if !ok {
			alterable, ok = sql.GetUnderlyingTable(sqlTable).(sql.AlterableTable)
		}
		if !ok {
			continue
		}
		originalComment, isMaterializedView, commentChanged, err := rewriteMaterializedViewCommentCompositeAttributeReferences(tableComment(sqlTable), updatedType.ID, c.oldAttr, c.newAttr)
		if err != nil {
			return err
		}
		tableChanged := false
		compositeColumns := compositeColumnNames(sqlTable.Schema(ctx), updatedType.ID)
		for _, col := range sqlTable.Schema(ctx) {
			updatedCol := *col
			columnChanged := false
			if doltgresType, ok := col.Type.(*types.DoltgresType); ok && doltgresType.ID == updatedType.ID {
				updatedCol.Type = updatedType.WithAttTypMod(doltgresType.GetAttTypMod())
				columnChanged = true
			}
			if newDefault, changed, err := rewriteColumnDefaultCompositeAttributeReferences(col.Default, updatedType.ID, c.oldAttr, c.newAttr, compositeColumns); err != nil {
				return err
			} else if changed {
				updatedCol.Default = newDefault
				columnChanged = true
			}
			if newGenerated, changed, err := rewriteColumnDefaultCompositeAttributeReferences(col.Generated, updatedType.ID, c.oldAttr, c.newAttr, compositeColumns); err != nil {
				return err
			} else if changed {
				updatedCol.Generated = newGenerated
				columnChanged = true
			}
			if newOnUpdate, changed, err := rewriteColumnDefaultCompositeAttributeReferences(col.OnUpdate, updatedType.ID, c.oldAttr, c.newAttr, compositeColumns); err != nil {
				return err
			} else if changed {
				updatedCol.OnUpdate = newOnUpdate
				columnChanged = true
			}
			if !columnChanged {
				continue
			}
			if err = alterable.ModifyColumn(ctx, col.Name, &updatedCol, nil); err != nil {
				return err
			}
			tableChanged = true
		}
		if isMaterializedView && commentChanged {
			db, err := (&AlterTypeRename{DatabaseName: c.database}).databaseForTableName(ctx, tableName)
			if err != nil {
				return err
			}
			if err = modifyTableComment(ctx, db, tableName.Name, originalComment); err != nil {
				return err
			}
		} else if tableChanged && originalComment != "" {
			commentAlterable, ok := sqlTable.(sql.CommentAlterableTable)
			if !ok {
				commentAlterable, ok = sql.GetUnderlyingTable(sqlTable).(sql.CommentAlterableTable)
			}
			if ok {
				if err = commentAlterable.ModifyComment(ctx, originalComment); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (c *AlterTypeRenameAttribute) renameDependentViews(ctx *sql.Context, typeID id.Type) error {
	databases, err := (&AlterTypeRename{DatabaseName: c.database}).schemaDatabases(ctx)
	if err != nil {
		return err
	}
	for _, database := range databases {
		viewDatabase, ok := database.(sql.ViewDatabase)
		if !ok {
			continue
		}
		views, err := viewDatabase.AllViews(ctx)
		if err != nil {
			return err
		}
		for _, view := range views {
			createViewStatement, changed, err := rewriteSQLCompositeAttributeReferences(view.CreateViewStatement, typeID, c.oldAttr, c.newAttr)
			if err != nil {
				return err
			}
			if !changed {
				continue
			}
			textDefinition := view.TextDefinition
			if textDefinition != "" {
				if rewrittenTextDefinition, textChanged, err := rewriteSQLCompositeAttributeReferences(textDefinition, typeID, c.oldAttr, c.newAttr); err != nil {
					return err
				} else if textChanged {
					textDefinition = rewrittenTextDefinition
				}
			}
			if err = viewDatabase.DropView(ctx, view.Name); err != nil {
				return err
			}
			if err = viewDatabase.CreateView(ctx, view.Name, textDefinition, createViewStatement); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *AlterTypeRenameAttribute) renameDependentFunctions(ctx *sql.Context, typeID id.Type) error {
	collection, err := core.GetFunctionsCollectionFromContextForDatabase(ctx, c.database)
	if err != nil {
		return err
	}
	var updates []functionRename
	if err = collection.IterateFunctions(ctx, func(function pgfunctions.Function) (bool, error) {
		if function.SQLDefinition == "" {
			return false, nil
		}
		sqlDefinition, changed, err := rewriteSQLCompositeAttributeReferences(function.SQLDefinition, typeID, c.oldAttr, c.newAttr)
		if err != nil || !changed {
			return false, err
		}
		updated := function
		updated.SQLDefinition = sqlDefinition
		updated.Definition = function.ReplaceDefinition(sqlDefinition)
		updates = append(updates, functionRename{oldID: function.ID, updated: updated})
		return false, nil
	}); err != nil {
		return err
	}
	for _, update := range updates {
		if err = collection.DropFunction(ctx, update.oldID); err != nil {
			return err
		}
		if err = collection.AddFunction(ctx, update.updated); err != nil {
			return err
		}
	}
	return nil
}

func compositeColumnNames(schema sql.Schema, typeID id.Type) map[string]struct{} {
	columns := make(map[string]struct{})
	for _, col := range schema {
		if doltgresType, ok := col.Type.(*types.DoltgresType); ok && doltgresType.ID == typeID {
			columns[col.Name] = struct{}{}
		}
	}
	return columns
}
