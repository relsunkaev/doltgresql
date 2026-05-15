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
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/indexmetadata"
)

const (
	maxBatchedIndexLookupCacheEntries = 256
	maxBatchedIndexLookupCachedRows   = 256
)

type batchedIndexLookupIndexedTable struct {
	underlying sql.IndexedTable

	mu    sync.Mutex
	cache map[string][]sql.Row
	order []string
}

var _ sql.IndexedTable = (*batchedIndexLookupIndexedTable)(nil)
var _ sql.TableWrapper = (*batchedIndexLookupIndexedTable)(nil)
var _ sql.ProjectedTable = (*batchedIndexLookupIndexedTable)(nil)
var _ sql.PrimaryKeyTable = (*batchedIndexLookupIndexedTable)(nil)

func newBatchedIndexLookupIndexedTable(table sql.IndexedTable) sql.IndexedTable {
	if _, ok := table.(*batchedIndexLookupIndexedTable); ok {
		return table
	}
	return &batchedIndexLookupIndexedTable{
		underlying: table,
		cache:      make(map[string][]sql.Row),
	}
}

// WrapBatchedIndexLookupIndexedTable wraps btree lookup-join indexed tables so
// repeated dynamic lookups can be served from an execution-local cache.
func WrapBatchedIndexLookupIndexedTable(table sql.IndexedTable, index sql.Index) (sql.IndexedTable, bool) {
	if table == nil || !batchedIndexLookupCacheableIndex(index) {
		return table, false
	}
	if _, ok := table.(*batchedIndexLookupIndexedTable); ok {
		return table, false
	}
	return newBatchedIndexLookupIndexedTable(table), true
}

func (t *batchedIndexLookupIndexedTable) Name() string {
	return t.underlying.Name()
}

func (t *batchedIndexLookupIndexedTable) Underlying() sql.Table {
	return t.underlying
}

func (t *batchedIndexLookupIndexedTable) String() string {
	return t.underlying.String()
}

func (t *batchedIndexLookupIndexedTable) Schema(ctx *sql.Context) sql.Schema {
	return t.underlying.Schema(ctx)
}

func (t *batchedIndexLookupIndexedTable) Collation() sql.CollationID {
	return t.underlying.Collation()
}

func (t *batchedIndexLookupIndexedTable) WithProjections(ctx *sql.Context, colNames []string) (sql.Table, error) {
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
	return newBatchedIndexLookupIndexedTable(indexedTable), nil
}

func (t *batchedIndexLookupIndexedTable) Projections() []string {
	if projected, ok := t.underlying.(sql.ProjectedTable); ok {
		return projected.Projections()
	}
	return nil
}

func (t *batchedIndexLookupIndexedTable) PrimaryKeySchema(ctx *sql.Context) sql.PrimaryKeySchema {
	if primaryKeyTable, ok := t.underlying.(sql.PrimaryKeyTable); ok {
		return primaryKeyTable.PrimaryKeySchema(ctx)
	}
	return sql.NewPrimaryKeySchema(t.Schema(ctx))
}

func (t *batchedIndexLookupIndexedTable) DatabaseSchema() sql.DatabaseSchema {
	if schemaTable, ok := t.underlying.(sql.DatabaseSchemaTable); ok {
		return schemaTable.DatabaseSchema()
	}
	return nil
}

func (t *batchedIndexLookupIndexedTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return t.underlying.Partitions(ctx)
}

func (t *batchedIndexLookupIndexedTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	if lookupPartition, ok := partition.(*batchedIndexLookupPartition); ok {
		if lookupPartition.cached {
			return sql.RowsToRowIter(cloneBatchedLookupRows(lookupPartition.cachedRows)...), nil
		}
		return &batchedIndexLookupRowIter{
			table:      t,
			partitions: lookupPartition.source,
			cacheKey:   lookupPartition.cacheKey,
			cacheable:  true,
		}, nil
	}
	return t.underlying.PartitionRows(ctx, partition)
}

func (t *batchedIndexLookupIndexedTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	cacheKey, ok := batchedIndexLookupCacheKey(ctx, lookup)
	if !ok {
		return t.underlying.LookupPartitions(ctx, lookup)
	}
	if rows, ok := t.cachedRows(cacheKey); ok {
		return sql.PartitionsToPartitionIter(&batchedIndexLookupPartition{
			cacheKey:   cacheKey,
			cachedRows: rows,
			cached:     true,
		}), nil
	}
	partitions, err := t.underlying.LookupPartitions(ctx, lookup)
	if err != nil {
		return nil, err
	}
	return sql.PartitionsToPartitionIter(&batchedIndexLookupPartition{
		cacheKey: cacheKey,
		source:   partitions,
	}), nil
}

func (t *batchedIndexLookupIndexedTable) cachedRows(cacheKey string) ([]sql.Row, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	rows, ok := t.cache[cacheKey]
	if !ok {
		return nil, false
	}
	return cloneBatchedLookupRows(rows), true
}

func (t *batchedIndexLookupIndexedTable) storeRows(cacheKey string, rows []sql.Row) {
	if len(rows) > maxBatchedIndexLookupCachedRows {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.cache[cacheKey]; ok {
		return
	}
	if len(t.cache) >= maxBatchedIndexLookupCacheEntries && len(t.order) > 0 {
		evictKey := t.order[0]
		t.order = t.order[1:]
		delete(t.cache, evictKey)
	}
	t.cache[cacheKey] = cloneBatchedLookupRows(rows)
	t.order = append(t.order, cacheKey)
}

type batchedIndexLookupPartition struct {
	cacheKey   string
	cachedRows []sql.Row
	cached     bool
	source     sql.PartitionIter
}

func (p *batchedIndexLookupPartition) Key() []byte {
	return []byte(p.cacheKey)
}

type batchedIndexLookupRowIter struct {
	table      *batchedIndexLookupIndexedTable
	partitions sql.PartitionIter
	cacheKey   string
	cacheable  bool
	rows       []sql.Row
	current    sql.RowIter
	complete   bool
}

func (i *batchedIndexLookupRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		if i.current != nil {
			row, err := i.current.Next(ctx)
			if err == nil {
				i.remember(row)
				return row, nil
			}
			if err != io.EOF {
				return nil, err
			}
			if err := i.current.Close(ctx); err != nil {
				return nil, err
			}
			i.current = nil
		}

		partition, err := i.partitions.Next(ctx)
		if err == io.EOF {
			i.complete = true
			if i.cacheable {
				i.table.storeRows(i.cacheKey, i.rows)
			}
			return nil, io.EOF
		}
		if err != nil {
			return nil, err
		}
		i.current, err = i.table.underlying.PartitionRows(ctx, partition)
		if err != nil {
			return nil, err
		}
	}
}

func (i *batchedIndexLookupRowIter) Close(ctx *sql.Context) error {
	var closeErr error
	if i.current != nil {
		closeErr = i.current.Close(ctx)
		i.current = nil
	}
	if err := i.partitions.Close(ctx); closeErr == nil {
		closeErr = err
	}
	return closeErr
}

func (i *batchedIndexLookupRowIter) remember(row sql.Row) {
	if !i.cacheable {
		return
	}
	if len(i.rows) >= maxBatchedIndexLookupCachedRows {
		i.cacheable = false
		i.rows = nil
		return
	}
	i.rows = append(i.rows, cloneBatchedLookupRow(row))
}

func batchedIndexLookupCacheKey(ctx *sql.Context, lookup sql.IndexLookup) (string, bool) {
	if lookup.Index == nil ||
		lookup.Ranges == nil ||
		lookup.IsEmptyRange ||
		lookup.IsSpatialLookup ||
		lookup.VectorOrderAndLimit.OrderBy != nil ||
		!batchedIndexLookupCacheableIndex(lookup.Index) {
		return "", false
	}
	ranges, ok := lookup.Ranges.(sql.MySQLRangeCollection)
	if !ok || len(ranges) == 0 {
		return "", false
	}
	return fmt.Sprintf("%s\x00reverse=%t\x00%s", lookup.Index.ID(), lookup.IsReverse, ranges.DebugString(ctx)), true
}

func batchedIndexLookupCacheableIndex(index sql.Index) bool {
	if index == nil || index.IsSpatial() || index.IsFullText() || index.IsVector() {
		return false
	}
	return indexmetadata.AccessMethod(index.IndexType(), index.Comment()) == indexmetadata.AccessMethodBtree
}

func cloneBatchedLookupRows(rows []sql.Row) []sql.Row {
	if len(rows) == 0 {
		return nil
	}
	cloned := make([]sql.Row, len(rows))
	for i, row := range rows {
		cloned[i] = cloneBatchedLookupRow(row)
	}
	return cloned
}

func cloneBatchedLookupRow(row sql.Row) sql.Row {
	if row == nil {
		return nil
	}
	return append(sql.Row(nil), row...)
}
