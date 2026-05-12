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
		originalComment := tableComment(sqlTable)
		tableChanged := false
		for _, col := range sqlTable.Schema(ctx) {
			doltgresType, ok := col.Type.(*types.DoltgresType)
			if !ok || doltgresType.ID != updatedType.ID {
				continue
			}
			updatedCol := *col
			updatedCol.Type = updatedType.WithAttTypMod(doltgresType.GetAttTypMod())
			if err = alterable.ModifyColumn(ctx, col.Name, &updatedCol, nil); err != nil {
				return err
			}
			tableChanged = true
		}
		if tableChanged && originalComment != "" {
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
