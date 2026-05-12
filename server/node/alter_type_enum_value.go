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
	"io"
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/types"
)

// AlterTypeAddValue handles ALTER TYPE ... ADD VALUE.
type AlterTypeAddValue struct {
	database    string
	schema      string
	typeName    string
	newValue    string
	ifNotExists bool
	before      bool
	existingVal string
}

var _ sql.ExecSourceRel = (*AlterTypeAddValue)(nil)
var _ vitess.Injectable = (*AlterTypeAddValue)(nil)

// NewAlterTypeAddValue returns a new *AlterTypeAddValue.
func NewAlterTypeAddValue(database, schema, typeName, newValue string, ifNotExists bool, before bool, existingVal string) *AlterTypeAddValue {
	return &AlterTypeAddValue{
		database:    database,
		schema:      schema,
		typeName:    typeName,
		newValue:    newValue,
		ifNotExists: ifNotExists,
		before:      before,
		existingVal: existingVal,
	}
}

// AlterTypeRenameValue handles ALTER TYPE ... RENAME VALUE.
type AlterTypeRenameValue struct {
	database string
	schema   string
	typeName string
	oldValue string
	newValue string
}

var _ sql.ExecSourceRel = (*AlterTypeRenameValue)(nil)
var _ vitess.Injectable = (*AlterTypeRenameValue)(nil)

// NewAlterTypeRenameValue returns a new *AlterTypeRenameValue.
func NewAlterTypeRenameValue(database, schema, typeName, oldValue, newValue string) *AlterTypeRenameValue {
	return &AlterTypeRenameValue{
		database: database,
		schema:   schema,
		typeName: typeName,
		oldValue: oldValue,
		newValue: newValue,
	}
}

func (a *AlterTypeAddValue) Children() []sql.Node                  { return nil }
func (a *AlterTypeAddValue) IsReadOnly() bool                      { return false }
func (a *AlterTypeAddValue) Resolved() bool                        { return true }
func (a *AlterTypeAddValue) Schema(ctx *sql.Context) sql.Schema    { return nil }
func (a *AlterTypeAddValue) String() string                        { return "ALTER TYPE ADD VALUE" }
func (a *AlterTypeRenameValue) Children() []sql.Node               { return nil }
func (a *AlterTypeRenameValue) IsReadOnly() bool                   { return false }
func (a *AlterTypeRenameValue) Resolved() bool                     { return true }
func (a *AlterTypeRenameValue) Schema(ctx *sql.Context) sql.Schema { return nil }
func (a *AlterTypeRenameValue) String() string                     { return "ALTER TYPE RENAME VALUE" }
func (a *AlterTypeAddValue) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}
func (a *AlterTypeRenameValue) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}
func (a *AlterTypeAddValue) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}
func (a *AlterTypeRenameValue) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}

func (a *AlterTypeAddValue) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	typ, err := enumTypeForAlter(ctx, a.database, a.schema, a.typeName)
	if err != nil {
		return nil, err
	}
	if err = checkTypeOwnership(ctx, typ); err != nil {
		return nil, err
	}
	if _, exists := typ.EnumLabels[a.newValue]; exists {
		if a.ifNotExists {
			return sql.RowsToRowIter(), nil
		}
		return nil, errors.Errorf(`enum label "%s" already exists`, a.newValue)
	}
	sortOrder, err := enumSortOrderForAdd(typ, a.before, a.existingVal)
	if err != nil {
		return nil, err
	}
	if typ.EnumLabels == nil {
		typ.EnumLabels = make(map[string]types.EnumLabel)
	}
	typ.EnumLabels[a.newValue] = types.NewEnumLabel(ctx, id.NewEnumLabel(typ.ID, a.newValue), sortOrder)
	if err = core.MarkTypesCollectionDirty(ctx, a.database); err != nil {
		return nil, err
	}
	if err = refreshDependentEnumColumnTypes(ctx, a.database, typ); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (a *AlterTypeRenameValue) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	typ, err := enumTypeForAlter(ctx, a.database, a.schema, a.typeName)
	if err != nil {
		return nil, err
	}
	if err = checkTypeOwnership(ctx, typ); err != nil {
		return nil, err
	}
	label, exists := typ.EnumLabels[a.oldValue]
	if !exists {
		return nil, errors.Errorf(`enum label "%s" does not exist`, a.oldValue)
	}
	if _, exists = typ.EnumLabels[a.newValue]; exists {
		return nil, errors.Errorf(`enum label "%s" already exists`, a.newValue)
	}
	if err = rewriteStoredEnumValues(ctx, a.database, typ.ID, a.oldValue, a.newValue); err != nil {
		return nil, err
	}
	delete(typ.EnumLabels, a.oldValue)
	label.ID = id.NewEnumLabel(typ.ID, a.newValue)
	typ.EnumLabels[a.newValue] = label
	if err = core.MarkTypesCollectionDirty(ctx, a.database); err != nil {
		return nil, err
	}
	if err = refreshDependentEnumColumnTypes(ctx, a.database, typ); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func enumTypeForAlter(ctx *sql.Context, database, schemaName, typeName string) (*types.DoltgresType, error) {
	if database != "" && database != ctx.GetCurrentDatabase() {
		return nil, errors.Errorf("ALTER TYPE is currently only supported for the current database")
	}
	schema, err := core.GetSchemaName(ctx, nil, schemaName)
	if err != nil {
		return nil, err
	}
	collection, err := core.GetTypesCollectionFromContextForDatabase(ctx, database)
	if err != nil {
		return nil, err
	}
	typ, err := collection.GetType(ctx, id.NewType(schema, typeName))
	if err != nil {
		return nil, err
	}
	if typ == nil {
		return nil, types.ErrTypeDoesNotExist.New(typeName)
	}
	if typ.TypType != types.TypeType_Enum {
		return nil, errors.Errorf(`"%s" is not an enum type`, typeName)
	}
	if _, isBuiltIn := types.IDToBuiltInDoltgresType[typ.ID]; isBuiltIn {
		return nil, errors.Errorf(`cannot alter type "%s" because it is a built-in type`, typeName)
	}
	return typ, nil
}

func enumSortOrderForAdd(typ *types.DoltgresType, before bool, existingVal string) (float32, error) {
	labels := sortedEnumLabels(typ)
	if existingVal == "" {
		if len(labels) == 0 {
			return 1, nil
		}
		return labels[len(labels)-1].SortOrder + 1, nil
	}
	for i, label := range labels {
		if label.ID.Label() != existingVal {
			continue
		}
		if before {
			if i == 0 {
				return label.SortOrder - 1, nil
			}
			return (labels[i-1].SortOrder + label.SortOrder) / 2, nil
		}
		if i == len(labels)-1 {
			return label.SortOrder + 1, nil
		}
		return (label.SortOrder + labels[i+1].SortOrder) / 2, nil
	}
	return 0, errors.Errorf(`enum label "%s" does not exist`, existingVal)
}

func sortedEnumLabels(typ *types.DoltgresType) []types.EnumLabel {
	labels := make([]types.EnumLabel, 0, len(typ.EnumLabels))
	for _, label := range typ.EnumLabels {
		labels = append(labels, label)
	}
	sort.Slice(labels, func(i, j int) bool {
		if labels[i].SortOrder == labels[j].SortOrder {
			return labels[i].ID.Label() < labels[j].ID.Label()
		}
		return labels[i].SortOrder < labels[j].SortOrder
	})
	return labels
}

func rewriteStoredEnumValues(ctx *sql.Context, database string, typeID id.Type, oldValue string, newValue string) error {
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
		table, err := core.GetSqlTableFromContext(ctx, database, tableName)
		if err != nil {
			return err
		}
		if table == nil {
			continue
		}
		if err = rewriteStoredEnumValuesInTable(ctx, table, typeID, oldValue, newValue); err != nil {
			return err
		}
	}
	return nil
}

func rewriteStoredEnumValuesInTable(ctx *sql.Context, table sql.Table, typeID id.Type, oldValue string, newValue string) error {
	enumOrdinals := enumColumnOrdinals(table.Schema(ctx), typeID)
	if len(enumOrdinals) == 0 {
		return nil
	}
	updatable, ok := table.(sql.UpdatableTable)
	if !ok {
		updatable, ok = sql.GetUnderlyingTable(table).(sql.UpdatableTable)
	}
	if !ok {
		return errors.Errorf(`table "%s" does not support rewriting enum values`, table.Name())
	}
	updater := updatable.Updater(ctx)
	updater.StatementBegin(ctx)
	completed := false
	defer func() {
		if !completed {
			_ = updater.DiscardChanges(ctx, errors.New("enum value rewrite failed"))
		}
		_ = updater.Close(ctx)
	}()
	partitions, err := table.Partitions(ctx)
	if err != nil {
		return err
	}
	defer partitions.Close(ctx)
	for {
		partition, err := partitions.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		rows, err := table.PartitionRows(ctx, partition)
		if err != nil {
			return err
		}
		for {
			row, err := rows.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = rows.Close(ctx)
				return err
			}
			updated := row.Copy()
			changed := false
			for _, ordinal := range enumOrdinals {
				if value, ok := updated[ordinal].(string); ok && value == oldValue {
					updated[ordinal] = newValue
					changed = true
				}
			}
			if changed {
				if err = updater.Update(ctx, row, updated); err != nil {
					_ = rows.Close(ctx)
					return err
				}
			}
		}
		if err = rows.Close(ctx); err != nil {
			return err
		}
	}
	if err = updater.StatementComplete(ctx); err != nil {
		return err
	}
	completed = true
	return nil
}

func enumColumnOrdinals(schema sql.Schema, typeID id.Type) []int {
	var ordinals []int
	for i, col := range schema {
		if typ, ok := col.Type.(*types.DoltgresType); ok && typ.ID == typeID {
			ordinals = append(ordinals, i)
		}
	}
	return ordinals
}

func refreshDependentEnumColumnTypes(ctx *sql.Context, database string, updatedType *types.DoltgresType) error {
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
		table, err := core.GetSqlTableFromContext(ctx, database, tableName)
		if err != nil {
			return err
		}
		if table == nil {
			continue
		}
		alterable, ok := table.(sql.AlterableTable)
		if !ok {
			alterable, ok = sql.GetUnderlyingTable(table).(sql.AlterableTable)
		}
		if !ok {
			continue
		}
		for _, col := range table.Schema(ctx) {
			colType, ok := col.Type.(*types.DoltgresType)
			if !ok || colType.ID != updatedType.ID {
				continue
			}
			updatedCol := *col
			updatedCol.Type = updatedType.WithAttTypMod(colType.GetAttTypMod())
			if err = alterable.ModifyColumn(ctx, col.Name, &updatedCol, nil); err != nil {
				return err
			}
		}
	}
	return nil
}
