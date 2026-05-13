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

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/tablemetadata"
)

// InheritedScanTable scans a parent table and its inherited children for
// read-only SELECT plans, matching PostgreSQL's default inheritance scan.
type InheritedScanTable struct {
	underlying sql.Table
	comment    string
	parent     tablemetadata.InheritedTable
	children   []sql.Table
}

var _ sql.Table = (*InheritedScanTable)(nil)
var _ sql.TableWrapper = (*InheritedScanTable)(nil)
var _ sql.MutableTableWrapper = (*InheritedScanTable)(nil)
var _ sql.CommentedTable = (*InheritedScanTable)(nil)
var _ sql.DatabaseSchemaTable = (*InheritedScanTable)(nil)
var _ sql.ProjectedTable = (*InheritedScanTable)(nil)

// WrapInheritedScanTable wraps a parent table when table metadata records one
// or more child tables inheriting from it.
func WrapInheritedScanTable(ctx *sql.Context, table sql.Table) (sql.Table, bool, error) {
	if _, ok := table.(*InheritedScanTable); ok {
		return table, false, nil
	}
	parentID, ok, err := id.GetFromTable(ctx, table)
	if err != nil || !ok {
		return table, false, err
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
	children := make([]sql.Table, 0, len(t.children))
	for _, child := range t.children {
		projectedChild, err := projectInheritedChild(ctx, child, colNames)
		if err != nil {
			return nil, err
		}
		children = append(children, projectedChild)
	}
	return &InheritedScanTable{
		underlying: table,
		comment:    t.comment,
		parent:     t.parent,
		children:   children,
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
	if err := appendInheritedScanPartitions(ctx, &partitions, t.underlying); err != nil {
		return nil, err
	}
	parentSchema := t.Schema(ctx)
	for _, child := range t.children {
		projectedChild, err := projectInheritedChild(ctx, child, columnNames(parentSchema))
		if err != nil {
			return nil, err
		}
		if err = appendInheritedScanPartitions(ctx, &partitions, projectedChild); err != nil {
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
	if inheritedPartition.table == t.underlying {
		return iter, nil
	}
	mapping, err := inheritedColumnMapping(t.Schema(ctx), inheritedPartition.table.Schema(ctx))
	if err != nil {
		_ = iter.Close(ctx)
		return nil, err
	}
	return &inheritedScanRowIter{iter: iter, mapping: mapping}, nil
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

func appendInheritedScanPartitions(ctx *sql.Context, partitions *[]sql.Partition, table sql.Table) error {
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
		*partitions = append(*partitions, &inheritedScanPartition{table: table, partition: partition})
	}
}

func projectInheritedChild(ctx *sql.Context, child sql.Table, colNames []string) (sql.Table, error) {
	projected, ok := child.(sql.ProjectedTable)
	if !ok {
		return child, nil
	}
	return projected.WithProjections(ctx, colNames)
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

func columnNames(schema sql.Schema) []string {
	names := make([]string, 0, len(schema))
	for _, column := range schema {
		names = append(names, column.Name)
	}
	return names
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

type inheritedScanPartition struct {
	table     sql.Table
	partition sql.Partition
}

func (p *inheritedScanPartition) Key() []byte {
	return p.partition.Key()
}

type inheritedScanRowIter struct {
	iter    sql.RowIter
	mapping []int
}

var _ sql.RowIter = (*inheritedScanRowIter)(nil)

func (i *inheritedScanRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	row, err := i.iter.Next(ctx)
	if err != nil {
		return nil, err
	}
	mapped := make(sql.Row, len(i.mapping))
	for idx, childIdx := range i.mapping {
		mapped[idx] = row[childIdx]
	}
	return mapped, nil
}

func (i *inheritedScanRowIter) Close(ctx *sql.Context) error {
	return i.iter.Close(ctx)
}
