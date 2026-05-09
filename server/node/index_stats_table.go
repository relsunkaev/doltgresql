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

	"github.com/dolthub/doltgresql/server/pgstats"
)

type IndexStatsTable struct {
	underlying sql.IndexedTable
	indexOID   uint32
}

var _ sql.IndexedTable = (*IndexStatsTable)(nil)
var _ sql.ProjectedTable = (*IndexStatsTable)(nil)
var _ sql.PrimaryKeyTable = (*IndexStatsTable)(nil)
var _ sql.DatabaseSchemaTable = (*IndexStatsTable)(nil)

// WrapIndexStatsTable instruments executor-level index access for
// pg_stat_*_indexes counters.
func WrapIndexStatsTable(table sql.IndexedTable, indexOID uint32) (sql.IndexedTable, bool) {
	if table == nil || indexOID == 0 {
		return table, false
	}
	if _, ok := table.(*IndexStatsTable); ok {
		return table, false
	}
	return &IndexStatsTable{
		underlying: table,
		indexOID:   indexOID,
	}, true
}

func (t *IndexStatsTable) Name() string {
	return t.underlying.Name()
}

func (t *IndexStatsTable) String() string {
	return t.underlying.String()
}

func (t *IndexStatsTable) Schema(ctx *sql.Context) sql.Schema {
	return t.underlying.Schema(ctx)
}

func (t *IndexStatsTable) Collation() sql.CollationID {
	return t.underlying.Collation()
}

func (t *IndexStatsTable) WithProjections(ctx *sql.Context, colNames []string) (sql.Table, error) {
	projected, ok := t.underlying.(sql.ProjectedTable)
	if !ok {
		return nil, errors.Errorf("table %s does not support projections", t.Name())
	}
	table, err := projected.WithProjections(ctx, colNames)
	if err != nil {
		return nil, err
	}
	indexedTable, ok := table.(sql.IndexedTable)
	if !ok {
		return nil, errors.Errorf("projected table %s is not indexed", t.Name())
	}
	return &IndexStatsTable{underlying: indexedTable, indexOID: t.indexOID}, nil
}

func (t *IndexStatsTable) Projections() []string {
	if projected, ok := t.underlying.(sql.ProjectedTable); ok {
		return projected.Projections()
	}
	return nil
}

func (t *IndexStatsTable) PrimaryKeySchema(ctx *sql.Context) sql.PrimaryKeySchema {
	if primaryKeyTable, ok := t.underlying.(sql.PrimaryKeyTable); ok {
		return primaryKeyTable.PrimaryKeySchema(ctx)
	}
	return sql.NewPrimaryKeySchema(t.Schema(ctx))
}

func (t *IndexStatsTable) DatabaseSchema() sql.DatabaseSchema {
	if schemaTable, ok := t.underlying.(sql.DatabaseSchemaTable); ok {
		return schemaTable.DatabaseSchema()
	}
	return nil
}

func (t *IndexStatsTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return t.underlying.Partitions(ctx)
}

func (t *IndexStatsTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	partitions, err := t.underlying.LookupPartitions(ctx, lookup)
	if err != nil {
		return nil, err
	}
	pgstats.RecordIndexScan(t.indexOID)
	return partitions, nil
}

func (t *IndexStatsTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	iter, err := t.underlying.PartitionRows(ctx, partition)
	if err != nil {
		return nil, err
	}
	return &indexStatsRowIter{
		underlying: iter,
		indexOID:   t.indexOID,
	}, nil
}

type indexStatsRowIter struct {
	underlying sql.RowIter
	indexOID   uint32
}

var _ sql.RowIter = (*indexStatsRowIter)(nil)

func (i *indexStatsRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	row, err := i.underlying.Next(ctx)
	if err == nil {
		pgstats.RecordIndexRows(i.indexOID, 1)
	}
	return row, err
}

func (i *indexStatsRowIter) Close(ctx *sql.Context) error {
	if i.underlying == nil {
		return nil
	}
	err := i.underlying.Close(ctx)
	i.underlying = nil
	return err
}
