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
	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/tablemetadata"
)

// PartitionedTable rejects direct inserts into a partitioned table parent.
// Doltgres does not route rows into PostgreSQL partitions yet, so a direct
// insert must fail instead of storing the row in the parent backing table.
type PartitionedTable struct {
	underlying sql.Table
	comment    string
}

var _ sql.Table = (*PartitionedTable)(nil)
var _ sql.TableWrapper = (*PartitionedTable)(nil)
var _ sql.MutableTableWrapper = (*PartitionedTable)(nil)
var _ sql.CommentedTable = (*PartitionedTable)(nil)
var _ sql.DatabaseSchemaTable = (*PartitionedTable)(nil)
var _ sql.InsertableTable = (*PartitionedTable)(nil)
var _ sql.ProjectedTable = (*PartitionedTable)(nil)
var _ sql.IndexAddressableTable = (*PartitionedTable)(nil)
var _ sql.IndexedTable = (*PartitionedTable)(nil)

// WrapPartitionedTable wraps table when Doltgres metadata marks it as a
// PostgreSQL partitioned table.
func WrapPartitionedTable(table sql.Table) (sql.Table, bool) {
	if _, ok := table.(*PartitionedTable); ok {
		return table, false
	}
	comment := unwrappedTableComment(table)
	if tablemetadata.PartitionKeyDef(comment) == "" {
		return table, false
	}
	return &PartitionedTable{
		underlying: table,
		comment:    comment,
	}, true
}

func (t *PartitionedTable) Underlying() sql.Table {
	return t.underlying
}

func (t *PartitionedTable) WithUnderlying(table sql.Table) sql.Table {
	out := *t
	out.underlying = table
	return &out
}

func (t *PartitionedTable) Name() string {
	return t.underlying.Name()
}

func (t *PartitionedTable) String() string {
	return t.underlying.String()
}

func (t *PartitionedTable) Schema(ctx *sql.Context) sql.Schema {
	return t.underlying.Schema(ctx)
}

func (t *PartitionedTable) Collation() sql.CollationID {
	return t.underlying.Collation()
}

func (t *PartitionedTable) Comment() string {
	return t.comment
}

func (t *PartitionedTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return t.underlying.Partitions(ctx)
}

func (t *PartitionedTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return t.underlying.PartitionRows(ctx, partition)
}

func (t *PartitionedTable) DatabaseSchema() sql.DatabaseSchema {
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

func (t *PartitionedTable) WithProjections(ctx *sql.Context, colNames []string) (sql.Table, error) {
	projected, ok := t.underlying.(sql.ProjectedTable)
	if !ok {
		return nil, errors.Errorf("table %s does not support projections", t.Name())
	}
	table, err := projected.WithProjections(ctx, colNames)
	if err != nil {
		return nil, err
	}
	return &PartitionedTable{underlying: table, comment: t.comment}, nil
}

func (t *PartitionedTable) Projections() []string {
	if projected, ok := t.underlying.(sql.ProjectedTable); ok {
		return projected.Projections()
	}
	return nil
}

func (t *PartitionedTable) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	indexAddressable, ok := t.underlying.(sql.IndexAddressable)
	if !ok {
		return nil
	}
	inner := indexAddressable.IndexedAccess(ctx, lookup)
	if inner == nil {
		return nil
	}
	return &PartitionedTable{underlying: inner, comment: t.comment}
}

func (t *PartitionedTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	indexAddressable, ok := t.underlying.(sql.IndexAddressable)
	if !ok {
		return nil, nil
	}
	return indexAddressable.GetIndexes(ctx)
}

func (t *PartitionedTable) PreciseMatch() bool {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.PreciseMatch()
	}
	return false
}

func (t *PartitionedTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	indexed, ok := t.underlying.(sql.IndexedTable)
	if !ok {
		return nil, errors.Errorf("table %s is not indexed", t.Name())
	}
	return indexed.LookupPartitions(ctx, lookup)
}

func (t *PartitionedTable) Inserter(ctx *sql.Context) sql.RowInserter {
	var primary sql.RowInserter
	if insertable, ok := t.underlying.(sql.InsertableTable); ok {
		primary = insertable.Inserter(ctx)
	}
	return &partitionedTableInserter{
		primary: primary,
		name:    t.Name(),
	}
}

type partitionedTableInserter struct {
	primary sql.RowInserter
	name    string
}

var _ sql.RowInserter = (*partitionedTableInserter)(nil)

func (i *partitionedTableInserter) StatementBegin(ctx *sql.Context) {
	if i.primary != nil {
		i.primary.StatementBegin(ctx)
	}
}

func (i *partitionedTableInserter) DiscardChanges(ctx *sql.Context, err error) error {
	if i.primary != nil {
		return i.primary.DiscardChanges(ctx, err)
	}
	return nil
}

func (i *partitionedTableInserter) StatementComplete(ctx *sql.Context) error {
	if i.primary != nil {
		return i.primary.StatementComplete(ctx)
	}
	return nil
}

func (i *partitionedTableInserter) Insert(ctx *sql.Context, row sql.Row) error {
	return sql.ErrCheckConstraintViolated.New(i.name + "_partition_check")
}

func (i *partitionedTableInserter) Close(ctx *sql.Context) error {
	if i.primary != nil {
		return i.primary.Close(ctx)
	}
	return nil
}
