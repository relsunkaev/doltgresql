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
	"io"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/hash"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/tablemetadata"
)

// InheritedScanTable scans a parent table and its inherited children, and routes
// parent UPDATE, DELETE, and TRUNCATE operations to the concrete child tables.
type InheritedScanTable struct {
	underlying sql.Table
	comment    string
	parent     tablemetadata.InheritedTable
	children   []sql.Table
	editState  *inheritedScanEditState
}

var _ sql.Table = (*InheritedScanTable)(nil)
var _ sql.TableWrapper = (*InheritedScanTable)(nil)
var _ sql.MutableTableWrapper = (*InheritedScanTable)(nil)
var _ sql.CommentedTable = (*InheritedScanTable)(nil)
var _ sql.DatabaseSchemaTable = (*InheritedScanTable)(nil)
var _ sql.ProjectedTable = (*InheritedScanTable)(nil)
var _ sql.UpdatableTable = (*InheritedScanTable)(nil)
var _ sql.DeletableTable = (*InheritedScanTable)(nil)
var _ sql.TruncateableTable = (*InheritedScanTable)(nil)

// WrapInheritedScanTable wraps a parent table when table metadata records one
// or more child tables inheriting from it.
func WrapInheritedScanTable(ctx *sql.Context, table sql.Table) (sql.Table, bool, error) {
	if _, ok := table.(*InheritedScanTable); ok {
		return table, false, nil
	}
	parentID, ok, err := id.GetFromTable(ctx, table)
	if err != nil || !ok {
		return table, false, nil
	}
	parent := tablemetadata.InheritedTable{Schema: parentID.SchemaName(), Name: parentID.TableName()}
	children, err := inheritedScanChildren(ctx, parent)
	if err != nil {
		return table, false, err
	}
	if len(children) == 0 {
		return table, false, nil
	}
	return &InheritedScanTable{
		underlying: table,
		comment:    unwrappedTableComment(table),
		parent:     parent,
		children:   children,
		editState:  newInheritedScanEditState(),
	}, true, nil
}

func (t *InheritedScanTable) Underlying() sql.Table {
	return t.underlying
}

func (t *InheritedScanTable) WithUnderlying(table sql.Table) sql.Table {
	out := *t
	out.underlying = table
	return &out
}

func (t *InheritedScanTable) Name() string {
	return t.underlying.Name()
}

func (t *InheritedScanTable) String() string {
	return t.underlying.String()
}

func (t *InheritedScanTable) Schema(ctx *sql.Context) sql.Schema {
	return t.underlying.Schema(ctx)
}

func (t *InheritedScanTable) Collation() sql.CollationID {
	return t.underlying.Collation()
}

func (t *InheritedScanTable) Comment() string {
	return t.comment
}

func (t *InheritedScanTable) DatabaseSchema() sql.DatabaseSchema {
	if schemaTable, ok := t.underlying.(sql.DatabaseSchemaTable); ok {
		return schemaTable.DatabaseSchema()
	}
	if wrapper, ok := t.underlying.(sql.TableWrapper); ok {
		if schemaTable, ok := sql.GetUnderlyingTable(wrapper.Underlying()).(sql.DatabaseSchemaTable); ok {
			return schemaTable.DatabaseSchema()
		}
	}
	return nil
}

func (t *InheritedScanTable) WithProjections(ctx *sql.Context, colNames []string) (sql.Table, error) {
	projected, ok := t.underlying.(sql.ProjectedTable)
	if !ok {
		return nil, errors.Errorf("table %s does not support projections", t.Name())
	}
	table, err := projected.WithProjections(ctx, colNames)
	if err != nil {
		return nil, err
	}
	return &InheritedScanTable{
		underlying: table,
		comment:    t.comment,
		parent:     t.parent,
		children:   t.children,
		editState:  newInheritedScanEditState(),
	}, nil
}

func (t *InheritedScanTable) Projections() []string {
	if projected, ok := t.underlying.(sql.ProjectedTable); ok {
		return projected.Projections()
	}
	return nil
}

func (t *InheritedScanTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	var partitions []sql.Partition
	if err := appendInheritedScanPartitions(ctx, &partitions, t.underlying, nil); err != nil {
		return nil, err
	}
	parentSchema := t.Schema(ctx)
	for _, child := range t.children {
		mapping, err := inheritedColumnMapping(parentSchema, child.Schema(ctx))
		if err != nil {
			return nil, err
		}
		if err = appendInheritedScanPartitions(ctx, &partitions, child, mapping); err != nil {
			return nil, err
		}
	}
	return sql.PartitionsToPartitionIter(partitions...), nil
}

func (t *InheritedScanTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	inheritedPartition, ok := partition.(*inheritedScanPartition)
	if !ok {
		return nil, errors.Errorf("unexpected inherited partition type %T", partition)
	}
	iter, err := inheritedPartition.table.PartitionRows(ctx, inheritedPartition.partition)
	if err != nil {
		return nil, err
	}
	return &inheritedScanRowIter{
		iter:     iter,
		mapping:  inheritedPartition.mapping,
		state:    t.getEditState(),
		schema:   t.Schema(ctx),
		tableKey: inheritedScanEditTableKey(ctx, inheritedPartition.table),
	}, nil
}

func (t *InheritedScanTable) Updater(ctx *sql.Context) sql.RowUpdater {
	parent, ok := inheritedScanUpdatableTable(t.underlying)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not updatable", t.Name()))
	}
	editors := map[string]sql.RowUpdater{
		inheritedScanEditTableKey(ctx, t.underlying): parent.Updater(ctx),
	}
	for _, child := range t.children {
		updatable, ok := inheritedScanUpdatableTable(child)
		if !ok {
			return sqlutil.NewStaticErrorEditor(planErr("table %s is not updatable", child.Name()))
		}
		editors[inheritedScanEditTableKey(ctx, child)] = updatable.Updater(ctx)
	}
	return &inheritedScanUpdater{
		table:   t,
		schema:  t.Schema(ctx),
		editors: editors,
	}
}

func (t *InheritedScanTable) Deleter(ctx *sql.Context) sql.RowDeleter {
	parent, ok := inheritedScanDeletableTable(t.underlying)
	if !ok {
		return sqlutil.NewStaticErrorEditor(planErr("table %s is not deletable", t.Name()))
	}
	editors := map[string]sql.RowDeleter{
		inheritedScanEditTableKey(ctx, t.underlying): parent.Deleter(ctx),
	}
	for _, child := range t.children {
		deletable, ok := inheritedScanDeletableTable(child)
		if !ok {
			return sqlutil.NewStaticErrorEditor(planErr("table %s is not deletable", child.Name()))
		}
		editors[inheritedScanEditTableKey(ctx, child)] = deletable.Deleter(ctx)
	}
	return &inheritedScanDeleter{
		table:   t,
		schema:  t.Schema(ctx),
		editors: editors,
	}
}

func (t *InheritedScanTable) Truncate(ctx *sql.Context) (int, error) {
	parent, ok := inheritedScanTruncateableTable(t.underlying)
	if !ok {
		return 0, planErr("table %s is not truncateable", t.Name())
	}
	rowsAffected, err := parent.Truncate(ctx)
	if err != nil {
		return 0, err
	}
	for _, child := range t.children {
		truncateable, ok := inheritedScanTruncateableTable(child)
		if !ok {
			return rowsAffected, planErr("table %s is not truncateable", child.Name())
		}
		childRowsAffected, err := truncateable.Truncate(ctx)
		if err != nil {
			return rowsAffected, err
		}
		rowsAffected += childRowsAffected
	}
	return rowsAffected, nil
}

func (t *InheritedScanTable) getEditState() *inheritedScanEditState {
	if t.editState == nil {
		t.editState = newInheritedScanEditState()
	}
	return t.editState
}

func inheritedScanChildren(ctx *sql.Context, parent tablemetadata.InheritedTable) ([]sql.Table, error) {
	var children []sql.Table
	err := functions.IterateCurrentDatabase(ctx, functions.Callbacks{
		Table: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable) (cont bool, err error) {
			for _, inheritedParent := range tablemetadata.Inherits(unwrappedTableComment(table.Item)) {
				if inheritedParent.Schema == "" {
					inheritedParent.Schema = schema.Item.SchemaName()
				}
				if inheritedParentMatches(inheritedParent, parent) {
					children = append(children, table.Item)
					break
				}
			}
			return true, nil
		},
	})
	return children, err
}

func appendInheritedScanPartitions(ctx *sql.Context, partitions *[]sql.Partition, table sql.Table, mapping []int) error {
	iter, err := table.Partitions(ctx)
	if err != nil {
		return err
	}
	defer iter.Close(ctx)
	for {
		partition, err := iter.Next(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		*partitions = append(*partitions, &inheritedScanPartition{table: table, partition: partition, mapping: mapping})
	}
}

func inheritedColumnMapping(parentSchema sql.Schema, childSchema sql.Schema) ([]int, error) {
	childColumns := make(map[string]int, len(childSchema))
	for idx, column := range childSchema {
		childColumns[strings.ToLower(column.Name)] = idx
	}
	mapping := make([]int, len(parentSchema))
	for idx, column := range parentSchema {
		childIdx, ok := childColumns[strings.ToLower(column.Name)]
		if !ok {
			return nil, errors.Errorf(`child table is missing inherited column "%s"`, column.Name)
		}
		mapping[idx] = childIdx
	}
	return mapping, nil
}

func containsInheritedParent(parents []tablemetadata.InheritedTable, parent tablemetadata.InheritedTable, childSchema string) bool {
	for _, existing := range parents {
		if existing.Schema == "" {
			existing.Schema = childSchema
		}
		if inheritedParentMatches(existing, parent) {
			return true
		}
	}
	return false
}

func removeInheritedParent(parents []tablemetadata.InheritedTable, parent tablemetadata.InheritedTable, childSchema string) ([]tablemetadata.InheritedTable, bool) {
	var removed bool
	ret := parents[:0]
	for _, existing := range parents {
		comparable := existing
		if comparable.Schema == "" {
			comparable.Schema = childSchema
		}
		if inheritedParentMatches(comparable, parent) {
			removed = true
			continue
		}
		ret = append(ret, existing)
	}
	return ret, removed
}

func inheritedParentMatches(left tablemetadata.InheritedTable, right tablemetadata.InheritedTable) bool {
	return strings.EqualFold(left.Schema, right.Schema) && strings.EqualFold(left.Name, right.Name)
}

func inheritedScanUpdatableTable(table sql.Table) (sql.UpdatableTable, bool) {
	switch table := table.(type) {
	case sql.UpdatableTable:
		return table, true
	case sql.TableWrapper:
		return inheritedScanUpdatableTable(table.Underlying())
	default:
		return nil, false
	}
}

func inheritedScanDeletableTable(table sql.Table) (sql.DeletableTable, bool) {
	switch table := table.(type) {
	case sql.DeletableTable:
		return table, true
	case sql.TableWrapper:
		return inheritedScanDeletableTable(table.Underlying())
	default:
		return nil, false
	}
}

func inheritedScanTruncateableTable(table sql.Table) (sql.TruncateableTable, bool) {
	switch table := table.(type) {
	case sql.TruncateableTable:
		return table, true
	case sql.TableWrapper:
		return inheritedScanTruncateableTable(table.Underlying())
	default:
		return nil, false
	}
}

func inheritedScanEditTableKey(ctx *sql.Context, table sql.Table) string {
	tableID, ok, err := id.GetFromTable(ctx, table)
	if err == nil && ok {
		return strings.ToLower(tableID.SchemaName() + "." + tableID.TableName())
	}
	return strings.ToLower(table.Name())
}

func inheritedScanRowToChildRow(target inheritedScanEditTarget, newParentRow sql.Row) sql.Row {
	if len(target.mapping) == 0 {
		return newParentRow
	}
	newChildRow := target.row.Copy()
	for parentIdx, childIdx := range target.mapping {
		newChildRow[childIdx] = newParentRow[parentIdx]
	}
	return newChildRow
}

type inheritedScanEditTarget struct {
	tableKey  string
	row       sql.Row
	projected sql.Row
	mapping   []int
}

type inheritedScanEditState struct {
	mu   sync.Mutex
	rows map[uint64][]inheritedScanEditTarget
}

func newInheritedScanEditState() *inheritedScanEditState {
	return &inheritedScanEditState{rows: make(map[uint64][]inheritedScanEditTarget)}
}

func (s *inheritedScanEditState) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = make(map[uint64][]inheritedScanEditTarget)
}

func (s *inheritedScanEditState) record(ctx *sql.Context, schema sql.Schema, projected sql.Row, target inheritedScanEditTarget) error {
	key, err := hash.HashOf(ctx, schema, projected)
	if err != nil {
		return err
	}
	target.projected = projected.Copy()
	target.row = target.row.Copy()
	if len(target.mapping) > 0 {
		target.mapping = append([]int(nil), target.mapping...)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows[key] = append(s.rows[key], target)
	return nil
}

func (s *inheritedScanEditState) pop(ctx *sql.Context, schema sql.Schema, projected sql.Row) (inheritedScanEditTarget, bool, error) {
	key, err := hash.HashOf(ctx, schema, projected)
	if err != nil {
		return inheritedScanEditTarget{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	targets := s.rows[key]
	for idx, target := range targets {
		equal, err := target.projected.Equals(ctx, projected, schema)
		if err != nil {
			return inheritedScanEditTarget{}, false, err
		}
		if !equal {
			continue
		}
		copy(targets[idx:], targets[idx+1:])
		targets = targets[:len(targets)-1]
		if len(targets) == 0 {
			delete(s.rows, key)
		} else {
			s.rows[key] = targets
		}
		return target, true, nil
	}
	return inheritedScanEditTarget{}, false, nil
}

type inheritedScanRowEditor interface {
	sql.EditOpenerCloser
	sql.Closer
}

type inheritedScanUpdater struct {
	table   *InheritedScanTable
	schema  sql.Schema
	editors map[string]sql.RowUpdater
}

var _ sql.RowUpdater = (*inheritedScanUpdater)(nil)

func (u *inheritedScanUpdater) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
	target, ok, err := u.table.getEditState().pop(ctx, u.schema, oldRow)
	if err != nil {
		return err
	}
	if !ok {
		target = inheritedScanEditTarget{
			tableKey: inheritedScanEditTableKey(ctx, u.table.underlying),
			row:      oldRow,
		}
	}
	editor, ok := u.editors[target.tableKey]
	if !ok {
		return errors.Errorf("missing inherited update editor for table %s", target.tableKey)
	}
	return editor.Update(ctx, target.row, inheritedScanRowToChildRow(target, newRow))
}

func (u *inheritedScanUpdater) StatementBegin(ctx *sql.Context) {
	u.table.getEditState().clear()
	for _, editor := range u.editors {
		editor.StatementBegin(ctx)
	}
}

func (u *inheritedScanUpdater) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	u.table.getEditState().clear()
	return inheritedScanDiscardChanges(ctx, errorEncountered, u.editors)
}

func (u *inheritedScanUpdater) StatementComplete(ctx *sql.Context) error {
	u.table.getEditState().clear()
	return inheritedScanStatementComplete(ctx, u.editors)
}

func (u *inheritedScanUpdater) Close(ctx *sql.Context) error {
	u.table.getEditState().clear()
	return inheritedScanClose(ctx, u.editors)
}

type inheritedScanDeleter struct {
	table   *InheritedScanTable
	schema  sql.Schema
	editors map[string]sql.RowDeleter
}

var _ sql.RowDeleter = (*inheritedScanDeleter)(nil)

func (d *inheritedScanDeleter) Delete(ctx *sql.Context, row sql.Row) error {
	target, ok, err := d.table.getEditState().pop(ctx, d.schema, row)
	if err != nil {
		return err
	}
	if !ok {
		target = inheritedScanEditTarget{
			tableKey: inheritedScanEditTableKey(ctx, d.table.underlying),
			row:      row,
		}
	}
	editor, ok := d.editors[target.tableKey]
	if !ok {
		return errors.Errorf("missing inherited delete editor for table %s", target.tableKey)
	}
	return editor.Delete(ctx, target.row)
}

func (d *inheritedScanDeleter) StatementBegin(ctx *sql.Context) {
	d.table.getEditState().clear()
	for _, editor := range d.editors {
		editor.StatementBegin(ctx)
	}
}

func (d *inheritedScanDeleter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	d.table.getEditState().clear()
	return inheritedScanDiscardChanges(ctx, errorEncountered, d.editors)
}

func (d *inheritedScanDeleter) StatementComplete(ctx *sql.Context) error {
	d.table.getEditState().clear()
	return inheritedScanStatementComplete(ctx, d.editors)
}

func (d *inheritedScanDeleter) Close(ctx *sql.Context) error {
	d.table.getEditState().clear()
	return inheritedScanClose(ctx, d.editors)
}

func inheritedScanDiscardChanges[T inheritedScanRowEditor](ctx *sql.Context, errorEncountered error, editors map[string]T) error {
	var firstErr error
	for _, editor := range editors {
		if err := editor.DiscardChanges(ctx, errorEncountered); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func inheritedScanStatementComplete[T inheritedScanRowEditor](ctx *sql.Context, editors map[string]T) error {
	var firstErr error
	for _, editor := range editors {
		if err := editor.StatementComplete(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func inheritedScanClose[T inheritedScanRowEditor](ctx *sql.Context, editors map[string]T) error {
	var firstErr error
	for _, editor := range editors {
		if err := editor.Close(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

type inheritedScanPartition struct {
	table     sql.Table
	partition sql.Partition
	mapping   []int
}

func (p *inheritedScanPartition) Key() []byte {
	return p.partition.Key()
}

type inheritedScanRowIter struct {
	iter     sql.RowIter
	mapping  []int
	state    *inheritedScanEditState
	schema   sql.Schema
	tableKey string
}

var _ sql.RowIter = (*inheritedScanRowIter)(nil)

func (i *inheritedScanRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	row, err := i.iter.Next(ctx)
	if err != nil {
		return nil, err
	}
	mapped := row
	if len(i.mapping) > 0 {
		mapped = make(sql.Row, len(i.mapping))
		for idx, childIdx := range i.mapping {
			mapped[idx] = row[childIdx]
		}
	}
	if i.state != nil {
		if err = i.state.record(ctx, i.schema, mapped, inheritedScanEditTarget{
			tableKey: i.tableKey,
			row:      row,
			mapping:  i.mapping,
		}); err != nil {
			return nil, err
		}
	}
	return mapped, nil
}

func (i *inheritedScanRowIter) Close(ctx *sql.Context) error {
	return i.iter.Close(ctx)
}
