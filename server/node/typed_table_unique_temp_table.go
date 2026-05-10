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
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

type typedTableUniqueTempTable struct {
	sql.Table
	constraints []TypedTableUniqueConstraint
	checks      []TypedTableCheckConstraint
}

var _ sql.TableWrapper = (*typedTableUniqueTempTable)(nil)
var _ sql.TemporaryTable = (*typedTableUniqueTempTable)(nil)
var _ sql.InsertableTable = (*typedTableUniqueTempTable)(nil)
var _ sql.UpdatableTable = (*typedTableUniqueTempTable)(nil)
var _ sql.DeletableTable = (*typedTableUniqueTempTable)(nil)
var _ sql.PrimaryKeyTable = (*typedTableUniqueTempTable)(nil)
var _ sql.IndexAddressableTable = (*typedTableUniqueTempTable)(nil)
var _ sql.IndexAlterableTable = (*typedTableUniqueTempTable)(nil)
var _ sql.CheckTable = (*typedTableUniqueTempTable)(nil)
var _ sql.CheckAlterableTable = (*typedTableUniqueTempTable)(nil)

func newTypedTableUniqueTempTable(table sql.Table, constraints []TypedTableUniqueConstraint, checks []TypedTableCheckConstraint) *typedTableUniqueTempTable {
	return &typedTableUniqueTempTable{
		Table:       table,
		constraints: append([]TypedTableUniqueConstraint(nil), constraints...),
		checks:      append([]TypedTableCheckConstraint(nil), checks...),
	}
}

func (t *typedTableUniqueTempTable) Underlying() sql.Table {
	return t.Table
}

func (t *typedTableUniqueTempTable) IsTemporary() bool {
	if temporaryTable, ok := t.Table.(sql.TemporaryTable); ok {
		return temporaryTable.IsTemporary()
	}
	return true
}

func (t *typedTableUniqueTempTable) Inserter(ctx *sql.Context) sql.RowInserter {
	return &typedTableUniqueTempInserter{
		table:    t,
		inserter: t.Table.(sql.InsertableTable).Inserter(ctx),
	}
}

func (t *typedTableUniqueTempTable) Updater(ctx *sql.Context) sql.RowUpdater {
	return &typedTableUniqueTempUpdater{
		table:   t,
		updater: t.Table.(sql.UpdatableTable).Updater(ctx),
	}
}

func (t *typedTableUniqueTempTable) Deleter(ctx *sql.Context) sql.RowDeleter {
	return t.Table.(sql.DeletableTable).Deleter(ctx)
}

func (t *typedTableUniqueTempTable) PrimaryKeySchema(ctx *sql.Context) sql.PrimaryKeySchema {
	return t.Table.(sql.PrimaryKeyTable).PrimaryKeySchema(ctx)
}

func (t *typedTableUniqueTempTable) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	if indexed, ok := t.Table.(sql.IndexAddressable); ok {
		return indexed.IndexedAccess(ctx, lookup)
	}
	return nil
}

func (t *typedTableUniqueTempTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	if indexed, ok := t.Table.(sql.IndexAddressable); ok {
		return indexed.GetIndexes(ctx)
	}
	return nil, nil
}

func (t *typedTableUniqueTempTable) PreciseMatch() bool {
	if indexed, ok := t.Table.(sql.IndexAddressable); ok {
		return indexed.PreciseMatch()
	}
	return false
}

func (t *typedTableUniqueTempTable) CreateIndex(ctx *sql.Context, indexDef sql.IndexDef) error {
	return t.Table.(sql.IndexAlterableTable).CreateIndex(ctx, indexDef)
}

func (t *typedTableUniqueTempTable) DropIndex(ctx *sql.Context, indexName string) error {
	return t.Table.(sql.IndexAlterableTable).DropIndex(ctx, indexName)
}

func (t *typedTableUniqueTempTable) RenameIndex(ctx *sql.Context, fromIndexName string, toIndexName string) error {
	return t.Table.(sql.IndexAlterableTable).RenameIndex(ctx, fromIndexName, toIndexName)
}

func (t *typedTableUniqueTempTable) GetChecks(ctx *sql.Context) ([]sql.CheckDefinition, error) {
	definitions := make([]sql.CheckDefinition, 0, len(t.checks))
	if checkTable, ok := t.Table.(sql.CheckTable); ok {
		checks, err := checkTable.GetChecks(ctx)
		if err != nil {
			return nil, err
		}
		definitions = append(definitions, checks...)
	}
	for _, check := range t.checks {
		definitions = append(definitions, sql.CheckDefinition{
			Name:            check.Name,
			CheckExpression: check.Expression,
			Enforced:        true,
		})
	}
	return definitions, nil
}

func (t *typedTableUniqueTempTable) CreateCheck(ctx *sql.Context, check *sql.CheckDefinition) error {
	t.checks = append(t.checks, TypedTableCheckConstraint{
		Name:       check.Name,
		Expression: check.CheckExpression,
	})
	return nil
}

func (t *typedTableUniqueTempTable) DropCheck(ctx *sql.Context, chName string) error {
	for i, check := range t.checks {
		if strings.EqualFold(check.Name, chName) {
			t.checks = append(t.checks[:i], t.checks[i+1:]...)
			return nil
		}
	}
	checkAlterable, ok := typedTableCheckAlterable(t.Table)
	if !ok {
		return fmt.Errorf("CREATE TABLE OF CHECK constraints are not supported by this table")
	}
	return checkAlterable.DropCheck(ctx, chName)
}

func (t *typedTableUniqueTempTable) checkUniqueConstraints(ctx *sql.Context, candidate sql.Row, ignore sql.Row, pending []sql.Row) error {
	schema := t.Schema(ctx)
	for _, constraint := range t.constraints {
		indexes, err := typedTableUniqueColumnIndexes(schema, constraint)
		if err != nil {
			return err
		}
		if !constraint.NullsNotDistinct && typedTableUniqueHasNull(candidate, indexes) {
			continue
		}
		for _, pendingRow := range pending {
			conflict, err := typedTableUniqueRowsEqual(ctx, schema, indexes, candidate, pendingRow)
			if err != nil {
				return err
			}
			if conflict {
				return sql.NewUniqueKeyErr(typedTableUniqueKeyString(candidate, indexes), false, pendingRow)
			}
		}
		duplicate, err := t.findExistingUniqueConflict(ctx, schema, indexes, candidate, ignore)
		if err != nil {
			return err
		}
		if duplicate != nil {
			return sql.NewUniqueKeyErr(typedTableUniqueKeyString(candidate, indexes), false, duplicate)
		}
	}
	return nil
}

func (t *typedTableUniqueTempTable) findExistingUniqueConflict(ctx *sql.Context, schema sql.Schema, indexes []int, candidate sql.Row, ignore sql.Row) (sql.Row, error) {
	partitionIter, err := t.Table.Partitions(ctx)
	if err != nil {
		return nil, err
	}
	defer partitionIter.Close(ctx)
	for {
		partition, err := partitionIter.Next(ctx)
		if err == io.EOF {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		rowIter, err := t.Table.PartitionRows(ctx, partition)
		if err != nil {
			return nil, err
		}
		for {
			row, err := rowIter.Next(ctx)
			if err == io.EOF {
				if closeErr := rowIter.Close(ctx); closeErr != nil {
					return nil, closeErr
				}
				break
			}
			if err != nil {
				_ = rowIter.Close(ctx)
				return nil, err
			}
			if ignore != nil {
				sameIgnoredRow, err := typedTableRowsEqual(ctx, schema, row, ignore)
				if err != nil {
					_ = rowIter.Close(ctx)
					return nil, err
				}
				if sameIgnoredRow {
					continue
				}
			}
			conflict, err := typedTableUniqueRowsEqual(ctx, schema, indexes, candidate, row)
			if err != nil {
				_ = rowIter.Close(ctx)
				return nil, err
			}
			if conflict {
				_ = rowIter.Close(ctx)
				return row, nil
			}
		}
	}
}

type typedTableUniqueTempInserter struct {
	table    *typedTableUniqueTempTable
	inserter sql.RowInserter
	pending  []sql.Row
}

func (i *typedTableUniqueTempInserter) StatementBegin(ctx *sql.Context) {
	i.pending = nil
	i.inserter.StatementBegin(ctx)
}

func (i *typedTableUniqueTempInserter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	i.pending = nil
	return i.inserter.DiscardChanges(ctx, errorEncountered)
}

func (i *typedTableUniqueTempInserter) StatementComplete(ctx *sql.Context) error {
	i.pending = nil
	return i.inserter.StatementComplete(ctx)
}

func (i *typedTableUniqueTempInserter) Insert(ctx *sql.Context, row sql.Row) error {
	if err := i.table.checkUniqueConstraints(ctx, row, nil, i.pending); err != nil {
		return err
	}
	if err := i.inserter.Insert(ctx, row); err != nil {
		return err
	}
	i.pending = append(i.pending, append(sql.Row(nil), row...))
	return nil
}

func (i *typedTableUniqueTempInserter) Close(ctx *sql.Context) error {
	i.pending = nil
	return i.inserter.Close(ctx)
}

type typedTableUniqueTempUpdater struct {
	table   *typedTableUniqueTempTable
	updater sql.RowUpdater
	pending []sql.Row
}

func (u *typedTableUniqueTempUpdater) StatementBegin(ctx *sql.Context) {
	u.pending = nil
	u.updater.StatementBegin(ctx)
}

func (u *typedTableUniqueTempUpdater) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	u.pending = nil
	return u.updater.DiscardChanges(ctx, errorEncountered)
}

func (u *typedTableUniqueTempUpdater) StatementComplete(ctx *sql.Context) error {
	u.pending = nil
	return u.updater.StatementComplete(ctx)
}

func (u *typedTableUniqueTempUpdater) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
	if err := u.table.checkUniqueConstraints(ctx, newRow, oldRow, u.pending); err != nil {
		return err
	}
	if err := u.updater.Update(ctx, oldRow, newRow); err != nil {
		return err
	}
	u.pending = append(u.pending, append(sql.Row(nil), newRow...))
	return nil
}

func (u *typedTableUniqueTempUpdater) Close(ctx *sql.Context) error {
	u.pending = nil
	return u.updater.Close(ctx)
}

func typedTableUniqueColumnIndexes(schema sql.Schema, constraint TypedTableUniqueConstraint) ([]int, error) {
	indexes := make([]int, len(constraint.Columns))
	for i, column := range constraint.Columns {
		idx := typedTableColumnIndex(schema, column)
		if idx < 0 {
			return nil, sql.ErrColumnNotFound.New(column)
		}
		indexes[i] = idx
	}
	return indexes, nil
}

func typedTableUniqueHasNull(row sql.Row, indexes []int) bool {
	for _, idx := range indexes {
		if row[idx] == nil {
			return true
		}
	}
	return false
}

func typedTableRowsEqual(ctx *sql.Context, schema sql.Schema, left sql.Row, right sql.Row) (bool, error) {
	if len(left) != len(right) {
		return false, nil
	}
	indexes := make([]int, len(schema))
	for i := range schema {
		indexes[i] = i
	}
	return typedTableUniqueRowsEqual(ctx, schema, indexes, left, right)
}

func typedTableUniqueRowsEqual(ctx *sql.Context, schema sql.Schema, indexes []int, left sql.Row, right sql.Row) (bool, error) {
	for _, idx := range indexes {
		if left[idx] == nil || right[idx] == nil {
			if left[idx] != right[idx] {
				return false, nil
			}
			continue
		}
		compare, err := schema[idx].Type.Compare(ctx, left[idx], right[idx])
		if err != nil {
			return false, err
		}
		if compare != 0 {
			return false, nil
		}
	}
	return true, nil
}

func typedTableUniqueKeyString(row sql.Row, indexes []int) string {
	values := make([]string, len(indexes))
	for i, idx := range indexes {
		values[i] = fmt.Sprint(row[idx])
	}
	return "[" + strings.Join(values, ",") + "]"
}
