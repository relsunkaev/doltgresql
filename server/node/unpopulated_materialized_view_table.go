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

// UnpopulatedMaterializedViewTable rejects row scans for materialized views
// created or refreshed WITH NO DATA. DDL/DML helpers unwrap TableWrapper values,
// so refresh, truncate, and index maintenance can still operate on the backing
// table storage.
type UnpopulatedMaterializedViewTable struct {
	underlying sql.Table
	comment    string
}

var _ sql.Table = (*UnpopulatedMaterializedViewTable)(nil)
var _ sql.TableWrapper = (*UnpopulatedMaterializedViewTable)(nil)
var _ sql.MutableTableWrapper = (*UnpopulatedMaterializedViewTable)(nil)
var _ sql.CommentedTable = (*UnpopulatedMaterializedViewTable)(nil)
var _ sql.DatabaseSchemaTable = (*UnpopulatedMaterializedViewTable)(nil)
var _ sql.ProjectedTable = (*UnpopulatedMaterializedViewTable)(nil)
var _ sql.IndexAddressableTable = (*UnpopulatedMaterializedViewTable)(nil)
var _ sql.IndexedTable = (*UnpopulatedMaterializedViewTable)(nil)

// WrapUnpopulatedMaterializedViewTable wraps table when its Doltgres metadata
// marks it as an unpopulated materialized view.
func WrapUnpopulatedMaterializedViewTable(table sql.Table) (sql.Table, bool) {
	if _, ok := table.(*UnpopulatedMaterializedViewTable); ok {
		return table, false
	}
	comment := unwrappedTableComment(table)
	if !tablemetadata.IsMaterializedView(comment) || tablemetadata.IsMaterializedViewPopulated(comment) {
		return table, false
	}
	return &UnpopulatedMaterializedViewTable{
		underlying: table,
		comment:    comment,
	}, true
}

func (t *UnpopulatedMaterializedViewTable) Underlying() sql.Table {
	return t.underlying
}

func (t *UnpopulatedMaterializedViewTable) WithUnderlying(table sql.Table) sql.Table {
	out := *t
	out.underlying = table
	return &out
}

func (t *UnpopulatedMaterializedViewTable) Name() string {
	return t.underlying.Name()
}

func (t *UnpopulatedMaterializedViewTable) String() string {
	return t.underlying.String()
}

func (t *UnpopulatedMaterializedViewTable) Schema(ctx *sql.Context) sql.Schema {
	return t.underlying.Schema(ctx)
}

func (t *UnpopulatedMaterializedViewTable) Collation() sql.CollationID {
	return t.underlying.Collation()
}

func (t *UnpopulatedMaterializedViewTable) Comment() string {
	return t.comment
}

func (t *UnpopulatedMaterializedViewTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return t.underlying.Partitions(ctx)
}

func (t *UnpopulatedMaterializedViewTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return nil, errors.Errorf(`materialized view "%s" has not been populated`, t.Name())
}

func (t *UnpopulatedMaterializedViewTable) DatabaseSchema() sql.DatabaseSchema {
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

func (t *UnpopulatedMaterializedViewTable) WithProjections(ctx *sql.Context, colNames []string) (sql.Table, error) {
	projected, ok := t.underlying.(sql.ProjectedTable)
	if !ok {
		return nil, errors.Errorf("table %s does not support projections", t.Name())
	}
	table, err := projected.WithProjections(ctx, colNames)
	if err != nil {
		return nil, err
	}
	return &UnpopulatedMaterializedViewTable{underlying: table, comment: t.comment}, nil
}

func (t *UnpopulatedMaterializedViewTable) Projections() []string {
	if projected, ok := t.underlying.(sql.ProjectedTable); ok {
		return projected.Projections()
	}
	return nil
}

func (t *UnpopulatedMaterializedViewTable) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	indexAddressable, ok := t.underlying.(sql.IndexAddressable)
	if !ok {
		return nil
	}
	inner := indexAddressable.IndexedAccess(ctx, lookup)
	if inner == nil {
		return nil
	}
	return &UnpopulatedMaterializedViewTable{underlying: inner, comment: t.comment}
}

func (t *UnpopulatedMaterializedViewTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	indexAddressable, ok := t.underlying.(sql.IndexAddressable)
	if !ok {
		return nil, nil
	}
	return indexAddressable.GetIndexes(ctx)
}

func (t *UnpopulatedMaterializedViewTable) PreciseMatch() bool {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.PreciseMatch()
	}
	return false
}

func (t *UnpopulatedMaterializedViewTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	indexed, ok := t.underlying.(sql.IndexedTable)
	if !ok {
		return nil, errors.Errorf("table %s is not indexed", t.Name())
	}
	return indexed.LookupPartitions(ctx, lookup)
}

func unwrappedTableComment(table sql.Table) string {
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
