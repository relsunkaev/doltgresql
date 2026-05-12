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

// AlterTypeRename handles ALTER TYPE/DOMAIN ... RENAME TO.
type AlterTypeRename struct {
	DatabaseName string
	SchemaName   string
	TypeName     string
	NewName      string
	DomainOnly   bool
}

var _ sql.ExecSourceRel = (*AlterTypeRename)(nil)
var _ vitess.Injectable = (*AlterTypeRename)(nil)

// NewAlterTypeRename returns a new *AlterTypeRename.
func NewAlterTypeRename(databaseName, schemaName, typeName, newName string, domainOnly bool) *AlterTypeRename {
	return &AlterTypeRename{
		DatabaseName: databaseName,
		SchemaName:   schemaName,
		TypeName:     typeName,
		NewName:      newName,
		DomainOnly:   domainOnly,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (a *AlterTypeRename) Children() []sql.Node { return nil }

// IsReadOnly implements the interface sql.ExecSourceRel.
func (a *AlterTypeRename) IsReadOnly() bool { return false }

// Resolved implements the interface sql.ExecSourceRel.
func (a *AlterTypeRename) Resolved() bool { return true }

// RowIter implements the interface sql.ExecSourceRel.
func (a *AlterTypeRename) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	schema, err := core.GetSchemaName(ctx, nil, a.SchemaName)
	if err != nil {
		return nil, err
	}
	collection, err := core.GetTypesCollectionFromContextForDatabase(ctx, a.DatabaseName)
	if err != nil {
		return nil, err
	}
	oldTypeID := id.NewType(schema, a.TypeName)
	newTypeID := id.NewType(schema, a.NewName)
	typ, err := collection.GetType(ctx, oldTypeID)
	if err != nil {
		return nil, err
	}
	if typ == nil {
		return nil, types.ErrTypeDoesNotExist.New(a.TypeName)
	}
	if a.DomainOnly && typ.TypType != types.TypeType_Domain {
		return nil, errors.Errorf(`type "%s" is not a domain`, a.TypeName)
	}
	if _, ok := types.IDToBuiltInDoltgresType[typ.ID]; ok {
		return nil, errors.Errorf(`cannot alter type "%s" because it is a system type`, a.TypeName)
	}
	if collection.HasType(ctx, newTypeID) {
		return nil, types.ErrTypeAlreadyExists.New(a.NewName)
	}
	if err = checkTypeOwnership(ctx, typ); err != nil {
		return nil, err
	}

	updatedType := *typ
	updatedType.ID = newTypeID
	if typ.Array.IsValid() {
		updatedType.Array = id.NewType(schema, "_"+a.NewName)
		if collection.HasType(ctx, updatedType.Array) {
			return nil, types.ErrTypeAlreadyExists.New(updatedType.Array.TypeName())
		}
	}
	updatedType.EnumLabels = renameEnumLabelParents(typ.EnumLabels, newTypeID)

	dropIDs := []id.Type{oldTypeID}
	if typ.Array.IsValid() {
		dropIDs = append(dropIDs, typ.Array)
	}
	if err = collection.DropType(ctx, dropIDs...); err != nil {
		return nil, err
	}
	if err = collection.CreateType(ctx, &updatedType); err != nil {
		return nil, err
	}
	if updatedType.Array.IsValid() {
		if err = collection.CreateType(ctx, types.CreateArrayTypeFromBaseType(&updatedType)); err != nil {
			return nil, err
		}
	}
	if err = core.MarkTypesCollectionDirty(ctx, a.DatabaseName); err != nil {
		return nil, err
	}
	if err = a.renameDependentTableColumns(ctx, oldTypeID, typ.Array, &updatedType); err != nil {
		return nil, err
	}
	if err = a.renameDependentFunctions(ctx, oldTypeID, typ.Array, updatedType.ID, updatedType.Array); err != nil {
		return nil, err
	}
	if err = a.renameDependentViews(ctx, oldTypeID, typ.Array, updatedType.ID, updatedType.Array); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (a *AlterTypeRename) renameDependentTableColumns(ctx *sql.Context, oldTypeID id.Type, oldArrayID id.Type, updatedType *types.DoltgresType) error {
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
		sqlTable, err := core.GetSqlTableFromContext(ctx, a.DatabaseName, tableName)
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
		comment, isMaterializedView, commentChanged, err := rewriteMaterializedViewCommentTypeReferences(tableComment(sqlTable), oldTypeID, oldArrayID, updatedType.ID, updatedType.Array)
		if err != nil {
			return err
		}
		tableChanged := false
		for _, col := range sqlTable.Schema(ctx) {
			updatedCol := *col
			columnChanged := false
			if doltgresType, ok := col.Type.(*types.DoltgresType); ok {
				newType, ok, err := a.renamedColumnType(ctx, doltgresType, oldTypeID, oldArrayID, updatedType)
				if err != nil {
					return err
				}
				if ok {
					updatedCol.Type = newType
					columnChanged = true
				}
			}
			if newDefault, changed, err := rewriteColumnDefaultTypeReferences(col.Default, oldTypeID, oldArrayID, updatedType.ID, updatedType.Array); err != nil {
				return err
			} else if changed {
				updatedCol.Default = newDefault
				columnChanged = true
			}
			if newGenerated, changed, err := rewriteColumnDefaultTypeReferences(col.Generated, oldTypeID, oldArrayID, updatedType.ID, updatedType.Array); err != nil {
				return err
			} else if changed {
				updatedCol.Generated = newGenerated
				columnChanged = true
			}
			if newOnUpdate, changed, err := rewriteColumnDefaultTypeReferences(col.OnUpdate, oldTypeID, oldArrayID, updatedType.ID, updatedType.Array); err != nil {
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
		if err = a.renameDependentCheckConstraints(ctx, sqlTable, oldTypeID, oldArrayID, updatedType.ID, updatedType.Array); err != nil {
			return err
		}
		if isMaterializedView && (tableChanged || commentChanged) {
			db, err := a.databaseForTableName(ctx, tableName)
			if err != nil {
				return err
			}
			if err = modifyTableComment(ctx, db, tableName.Name, comment); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *AlterTypeRename) renameDependentCheckConstraints(ctx *sql.Context, table sql.Table, oldTypeID id.Type, oldArrayID id.Type, newTypeID id.Type, newArrayID id.Type) error {
	checkTable, ok := table.(sql.CheckTable)
	if !ok {
		checkTable, ok = sql.GetUnderlyingTable(table).(sql.CheckTable)
	}
	if !ok {
		return nil
	}
	checkAlterable, ok := table.(sql.CheckAlterableTable)
	if !ok {
		checkAlterable, ok = sql.GetUnderlyingTable(table).(sql.CheckAlterableTable)
	}
	if !ok {
		return nil
	}
	checks, err := checkTable.GetChecks(ctx)
	if err != nil {
		return err
	}
	for _, check := range checks {
		rewritten, changed, err := rewriteCheckConstraintTypeReferences(check, oldTypeID, oldArrayID, newTypeID, newArrayID)
		if err != nil {
			return err
		}
		if !changed {
			continue
		}
		if err = checkAlterable.DropCheck(ctx, check.Name); err != nil {
			return err
		}
		if err = checkAlterable.CreateCheck(ctx, &rewritten); err != nil {
			return err
		}
	}
	return nil
}

func (a *AlterTypeRename) renamedColumnType(ctx *sql.Context, colType *types.DoltgresType, oldTypeID id.Type, oldArrayID id.Type, updatedType *types.DoltgresType) (*types.DoltgresType, bool, error) {
	switch colType.ID {
	case oldTypeID:
		return updatedType.WithAttTypMod(colType.GetAttTypMod()), true, nil
	case oldArrayID:
		if !oldArrayID.IsValid() {
			return nil, false, nil
		}
		collection, err := core.GetTypesCollectionFromContextForDatabase(ctx, a.DatabaseName)
		if err != nil {
			return nil, false, err
		}
		newArrayType, err := collection.GetType(ctx, updatedType.Array)
		if err != nil {
			return nil, false, err
		}
		if newArrayType == nil {
			return nil, false, types.ErrTypeDoesNotExist.New(updatedType.Array.TypeName())
		}
		return newArrayType.WithAttTypMod(colType.GetAttTypMod()), true, nil
	default:
		return nil, false, nil
	}
}

type functionRename struct {
	oldID   id.Function
	updated pgfunctions.Function
}

func (a *AlterTypeRename) renameDependentFunctions(ctx *sql.Context, oldTypeID id.Type, oldArrayID id.Type, newTypeID id.Type, newArrayID id.Type) error {
	collection, err := core.GetFunctionsCollectionFromContextForDatabase(ctx, a.DatabaseName)
	if err != nil {
		return err
	}
	var updates []functionRename
	if err = collection.IterateFunctions(ctx, func(function pgfunctions.Function) (bool, error) {
		updated, changed := renameFunctionTypes(function, oldTypeID, oldArrayID, newTypeID, newArrayID)
		if changed {
			updates = append(updates, functionRename{
				oldID:   function.ID,
				updated: updated,
			})
		}
		return false, nil
	}); err != nil {
		return err
	}
	for _, update := range updates {
		if update.oldID != update.updated.ID && collection.HasFunction(ctx, update.updated.ID) {
			return errors.Errorf(`function "%s" already exists with renamed argument types`, update.updated.ID.FunctionName())
		}
		if err = collection.DropFunction(ctx, update.oldID); err != nil {
			return err
		}
		if err = collection.AddFunction(ctx, update.updated); err != nil {
			return err
		}
	}
	return nil
}

func renameFunctionTypes(function pgfunctions.Function, oldTypeID id.Type, oldArrayID id.Type, newTypeID id.Type, newArrayID id.Type) (pgfunctions.Function, bool) {
	updated := function
	changed := false
	if newID, ok := renameFunctionIDTypes(function.ID, oldTypeID, oldArrayID, newTypeID, newArrayID); ok {
		updated.ID = newID
		changed = true
	}
	if newReturnType, ok := renameTypeID(function.ReturnType, oldTypeID, oldArrayID, newTypeID, newArrayID); ok {
		updated.ReturnType = newReturnType
		changed = true
	}
	if len(function.ParameterTypes) > 0 {
		updatedParams := make([]id.Type, len(function.ParameterTypes))
		copy(updatedParams, function.ParameterTypes)
		for i, param := range updatedParams {
			if newParam, ok := renameTypeID(param, oldTypeID, oldArrayID, newTypeID, newArrayID); ok {
				updatedParams[i] = newParam
				changed = true
			}
		}
		if changed {
			updated.ParameterTypes = updatedParams
		}
	}
	if newAggregateStateType, ok := renameTypeID(function.AggregateStateType, oldTypeID, oldArrayID, newTypeID, newArrayID); ok {
		updated.AggregateStateType = newAggregateStateType
		changed = true
	}
	if newAggregateSFunc, ok := renameFunctionIDTypes(function.AggregateSFunc, oldTypeID, oldArrayID, newTypeID, newArrayID); ok {
		updated.AggregateSFunc = newAggregateSFunc
		changed = true
	}
	return updated, changed
}

func renameFunctionIDTypes(functionID id.Function, oldTypeID id.Type, oldArrayID id.Type, newTypeID id.Type, newArrayID id.Type) (id.Function, bool) {
	if !functionID.IsValid() {
		return functionID, false
	}
	params := functionID.Parameters()
	changed := false
	for i, param := range params {
		if newParam, ok := renameTypeID(param, oldTypeID, oldArrayID, newTypeID, newArrayID); ok {
			params[i] = newParam
			changed = true
		}
	}
	if !changed {
		return functionID, false
	}
	return id.NewFunction(functionID.SchemaName(), functionID.FunctionName(), params...), true
}

func renameTypeID(typeID id.Type, oldTypeID id.Type, oldArrayID id.Type, newTypeID id.Type, newArrayID id.Type) (id.Type, bool) {
	switch typeID {
	case oldTypeID:
		return newTypeID, true
	case oldArrayID:
		if oldArrayID.IsValid() {
			return newArrayID, true
		}
	}
	return typeID, false
}

func renameEnumLabelParents(labels map[string]types.EnumLabel, parent id.Type) map[string]types.EnumLabel {
	if len(labels) == 0 {
		return labels
	}
	renamed := make(map[string]types.EnumLabel, len(labels))
	for label, enumLabel := range labels {
		enumLabel.ID = id.NewEnumLabel(parent, label)
		renamed[label] = enumLabel
	}
	return renamed
}

// Schema implements the interface sql.ExecSourceRel.
func (a *AlterTypeRename) Schema(ctx *sql.Context) sql.Schema { return nil }

// String implements the interface sql.ExecSourceRel.
func (a *AlterTypeRename) String() string { return "ALTER TYPE RENAME" }

// WithChildren implements the interface sql.ExecSourceRel.
func (a *AlterTypeRename) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (a *AlterTypeRename) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}
