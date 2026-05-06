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
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"
)

func TestBatchedIndexLookupCachesDuplicateBtreeLookups(t *testing.T) {
	ctx := sql.NewEmptyContext()
	idx := fakeBatchedLookupIndex{id: "btree_idx", indexType: "BTREE"}
	base := newFakeBatchedLookupIndexedTable()
	base.rowsByLookup[batchedLookupTestKey(ctx, batchedLookupTestLookup(idx, 7))] = []sql.Row{{int64(7), "first"}, {int64(7), "second"}}

	indexed, wrapped := WrapBatchedIndexLookupIndexedTable(base, idx)
	require.True(t, wrapped)

	require.Equal(t, []sql.Row{{int64(7), "first"}, {int64(7), "second"}}, readBatchedLookupRows(t, ctx, indexed, batchedLookupTestLookup(idx, 7)))
	require.Equal(t, []sql.Row{{int64(7), "first"}, {int64(7), "second"}}, readBatchedLookupRows(t, ctx, indexed, batchedLookupTestLookup(idx, 7)))
	require.Equal(t, 1, base.lookupCalls)
	require.Equal(t, 1, base.partitionRowsCalls)

	require.Empty(t, readBatchedLookupRows(t, ctx, indexed, batchedLookupTestLookup(idx, 404)))
	require.Empty(t, readBatchedLookupRows(t, ctx, indexed, batchedLookupTestLookup(idx, 404)))
	require.Equal(t, 2, base.lookupCalls)
	require.Equal(t, 2, base.partitionRowsCalls)
}

func TestBatchedIndexLookupCacheSeparatesReverseLookups(t *testing.T) {
	ctx := sql.NewEmptyContext()
	idx := fakeBatchedLookupIndex{id: "btree_idx", indexType: "BTREE"}
	base := newFakeBatchedLookupIndexedTable()
	forwardLookup := batchedLookupTestLookup(idx, 7)
	reverseLookup := batchedLookupTestReverseLookup(idx, 7)
	base.rowsByLookup[batchedLookupTestKey(ctx, forwardLookup)] = []sql.Row{{int64(7), "forward"}}
	base.rowsByLookup[batchedLookupTestKey(ctx, reverseLookup)] = []sql.Row{{int64(7), "reverse"}}

	indexed, wrapped := WrapBatchedIndexLookupIndexedTable(base, idx)
	require.True(t, wrapped)

	require.Equal(t, []sql.Row{{int64(7), "forward"}}, readBatchedLookupRows(t, ctx, indexed, forwardLookup))
	require.Equal(t, []sql.Row{{int64(7), "reverse"}}, readBatchedLookupRows(t, ctx, indexed, reverseLookup))
	require.Equal(t, []sql.Row{{int64(7), "forward"}}, readBatchedLookupRows(t, ctx, indexed, forwardLookup))
	require.Equal(t, []sql.Row{{int64(7), "reverse"}}, readBatchedLookupRows(t, ctx, indexed, reverseLookup))
	require.Equal(t, 2, base.lookupCalls)
	require.Equal(t, 2, base.partitionRowsCalls)
}

func TestBatchedIndexLookupCacheKeepsMixedHitMissBatchesCorrect(t *testing.T) {
	ctx := sql.NewEmptyContext()
	idx := fakeBatchedLookupIndex{id: "btree_idx", indexType: "BTREE"}
	base := newFakeBatchedLookupIndexedTable()
	base.rowsByLookup[batchedLookupTestKey(ctx, batchedLookupTestLookup(idx, 1))] = []sql.Row{{int64(1), "one"}}
	base.rowsByLookup[batchedLookupTestKey(ctx, batchedLookupTestLookup(idx, 3))] = []sql.Row{{int64(3), "three-a"}, {int64(3), "three-b"}}

	indexed, wrapped := WrapBatchedIndexLookupIndexedTable(base, idx)
	require.True(t, wrapped)

	require.Equal(t, []sql.Row{{int64(1), "one"}}, readBatchedLookupRows(t, ctx, indexed, batchedLookupTestLookup(idx, 1)))
	require.Empty(t, readBatchedLookupRows(t, ctx, indexed, batchedLookupTestLookup(idx, 2)))
	require.Equal(t, []sql.Row{{int64(3), "three-a"}, {int64(3), "three-b"}}, readBatchedLookupRows(t, ctx, indexed, batchedLookupTestLookup(idx, 3)))
	require.Empty(t, readBatchedLookupRows(t, ctx, indexed, batchedLookupTestLookup(idx, 2)))
	require.Equal(t, []sql.Row{{int64(1), "one"}}, readBatchedLookupRows(t, ctx, indexed, batchedLookupTestLookup(idx, 1)))
	require.Equal(t, []sql.Row{{int64(3), "three-a"}, {int64(3), "three-b"}}, readBatchedLookupRows(t, ctx, indexed, batchedLookupTestLookup(idx, 3)))
	require.Equal(t, 3, base.lookupCalls)
	require.Equal(t, 3, base.partitionRowsCalls)
}

func TestBatchedIndexLookupCacheSeparatesNullRanges(t *testing.T) {
	ctx := sql.NewEmptyContext()
	idx := fakeBatchedLookupIndex{id: "btree_idx", indexType: "BTREE"}
	base := newFakeBatchedLookupIndexedTable()
	nullLookup := batchedLookupTestNullLookup(idx)
	zeroLookup := batchedLookupTestLookup(idx, 0)
	nullKey := batchedLookupTestKey(ctx, nullLookup)
	zeroKey := batchedLookupTestKey(ctx, zeroLookup)
	require.NotEqual(t, zeroKey, nullKey)
	base.rowsByLookup[zeroKey] = []sql.Row{{int64(0), "zero"}}

	indexed, wrapped := WrapBatchedIndexLookupIndexedTable(base, idx)
	require.True(t, wrapped)

	require.Empty(t, readBatchedLookupRows(t, ctx, indexed, nullLookup))
	require.Equal(t, []sql.Row{{int64(0), "zero"}}, readBatchedLookupRows(t, ctx, indexed, zeroLookup))
	require.Empty(t, readBatchedLookupRows(t, ctx, indexed, nullLookup))
	require.Equal(t, []sql.Row{{int64(0), "zero"}}, readBatchedLookupRows(t, ctx, indexed, zeroLookup))
	require.Equal(t, 2, base.lookupCalls)
	require.Equal(t, 2, base.partitionRowsCalls)
}

func TestBatchedIndexLookupDoesNotCacheBroadResults(t *testing.T) {
	ctx := sql.NewEmptyContext()
	idx := fakeBatchedLookupIndex{id: "btree_idx", indexType: "BTREE"}
	base := newFakeBatchedLookupIndexedTable()
	rows := make([]sql.Row, maxBatchedIndexLookupCachedRows+1)
	for i := range rows {
		rows[i] = sql.Row{int64(i)}
	}
	base.rowsByLookup[batchedLookupTestKey(ctx, batchedLookupTestLookup(idx, 9))] = rows

	indexed, wrapped := WrapBatchedIndexLookupIndexedTable(base, idx)
	require.True(t, wrapped)

	require.Len(t, readBatchedLookupRows(t, ctx, indexed, batchedLookupTestLookup(idx, 9)), maxBatchedIndexLookupCachedRows+1)
	require.Len(t, readBatchedLookupRows(t, ctx, indexed, batchedLookupTestLookup(idx, 9)), maxBatchedIndexLookupCachedRows+1)
	require.Equal(t, 2, base.lookupCalls)
	require.Equal(t, 2, base.partitionRowsCalls)
}

func TestBatchedIndexLookupSkipsNonBtreeIndexes(t *testing.T) {
	base := newFakeBatchedLookupIndexedTable()

	wrappedTable, wrapped := WrapBatchedIndexLookupIndexedTable(base, fakeBatchedLookupIndex{id: "gin_idx", indexType: "GIN"})
	require.False(t, wrapped)
	require.Same(t, base, wrappedTable)
}

func readBatchedLookupRows(t *testing.T, ctx *sql.Context, table sql.IndexedTable, lookup sql.IndexLookup) []sql.Row {
	t.Helper()
	partitions, err := table.LookupPartitions(ctx, lookup)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, partitions.Close(ctx))
	}()

	var rows []sql.Row
	for {
		partition, err := partitions.Next(ctx)
		if err == io.EOF {
			return rows
		}
		require.NoError(t, err)
		rowIter, err := table.PartitionRows(ctx, partition)
		require.NoError(t, err)
		for {
			row, err := rowIter.Next(ctx)
			if err == io.EOF {
				require.NoError(t, rowIter.Close(ctx))
				break
			}
			require.NoError(t, err)
			rows = append(rows, row)
		}
	}
}

func batchedLookupTestLookup(index sql.Index, value int64) sql.IndexLookup {
	return sql.NewIndexLookup(index, sql.MySQLRangeCollection{{
		sql.ClosedRangeColumnExpr(value, value, types.Int64),
	}}, true, false, false, false)
}

func batchedLookupTestReverseLookup(index sql.Index, value int64) sql.IndexLookup {
	return sql.NewIndexLookup(index, sql.MySQLRangeCollection{{
		sql.ClosedRangeColumnExpr(value, value, types.Int64),
	}}, true, false, false, true)
}

func batchedLookupTestNullLookup(index sql.Index) sql.IndexLookup {
	return sql.NewIndexLookup(index, sql.MySQLRangeCollection{{
		sql.NullRangeColumnExpr(types.Int64),
	}}, true, false, false, false)
}

func batchedLookupTestKey(ctx *sql.Context, lookup sql.IndexLookup) string {
	key, ok := batchedIndexLookupCacheKey(ctx, lookup)
	if !ok {
		panic("test lookup should be cacheable")
	}
	return key
}

type fakeBatchedLookupIndexedTable struct {
	rowsByLookup       map[string][]sql.Row
	lookupCalls        int
	partitionRowsCalls int
}

func newFakeBatchedLookupIndexedTable() *fakeBatchedLookupIndexedTable {
	return &fakeBatchedLookupIndexedTable{
		rowsByLookup: make(map[string][]sql.Row),
	}
}

var _ sql.IndexedTable = (*fakeBatchedLookupIndexedTable)(nil)

func (t *fakeBatchedLookupIndexedTable) Name() string {
	return "fake_lookup"
}

func (t *fakeBatchedLookupIndexedTable) String() string {
	return t.Name()
}

func (t *fakeBatchedLookupIndexedTable) Schema(ctx *sql.Context) sql.Schema {
	return sql.Schema{{Name: "id", Type: types.Int64}, {Name: "label", Type: types.Text}}
}

func (t *fakeBatchedLookupIndexedTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

func (t *fakeBatchedLookupIndexedTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return sql.PartitionsToPartitionIter(), nil
}

func (t *fakeBatchedLookupIndexedTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	t.partitionRowsCalls++
	key := string(partition.Key())
	return sql.RowsToRowIter(cloneBatchedLookupRows(t.rowsByLookup[key])...), nil
}

func (t *fakeBatchedLookupIndexedTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	t.lookupCalls++
	key, ok := batchedIndexLookupCacheKey(ctx, lookup)
	if !ok {
		return sql.PartitionsToPartitionIter(), nil
	}
	return sql.PartitionsToPartitionIter(fakeBatchedLookupPartition(key)), nil
}

type fakeBatchedLookupPartition string

func (p fakeBatchedLookupPartition) Key() []byte {
	return []byte(p)
}

type fakeBatchedLookupIndex struct {
	id        string
	indexType string
	comment   string
}

var _ sql.Index = fakeBatchedLookupIndex{}

func (i fakeBatchedLookupIndex) ID() string {
	return i.id
}

func (i fakeBatchedLookupIndex) Database() string {
	return "postgres"
}

func (i fakeBatchedLookupIndex) Table() string {
	return "fake_lookup"
}

func (i fakeBatchedLookupIndex) Expressions() []string {
	return []string{"id"}
}

func (i fakeBatchedLookupIndex) IsUnique() bool {
	return false
}

func (i fakeBatchedLookupIndex) IsSpatial() bool {
	return false
}

func (i fakeBatchedLookupIndex) IsFullText() bool {
	return false
}

func (i fakeBatchedLookupIndex) IsVector() bool {
	return false
}

func (i fakeBatchedLookupIndex) Comment() string {
	return i.comment
}

func (i fakeBatchedLookupIndex) IndexType() string {
	return i.indexType
}

func (i fakeBatchedLookupIndex) IsGenerated() bool {
	return false
}

func (i fakeBatchedLookupIndex) ColumnExpressionTypes(ctx *sql.Context) []sql.ColumnExpressionType {
	return []sql.ColumnExpressionType{{Expression: "id", Type: types.Int64}}
}

func (i fakeBatchedLookupIndex) CanSupport(ctx *sql.Context, ranges ...sql.Range) bool {
	return true
}

func (i fakeBatchedLookupIndex) CanSupportOrderBy(expr sql.Expression) bool {
	return false
}

func (i fakeBatchedLookupIndex) PrefixLengths() []uint16 {
	return nil
}
