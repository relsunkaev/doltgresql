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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/indexmetadata"
	"github.com/dolthub/doltgresql/server/jsonbgin"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func TestJsonbGinPostingTokenLookupUsesIndex(t *testing.T) {
	ctx := sql.NewEmptyContext()
	wanted := jsonbgin.EncodeToken(jsonbgin.Token{
		OpClass: indexmetadata.OpClassJsonbOps,
		Kind:    jsonbgin.TokenKindKey,
		Value:   "vip",
	})
	other := jsonbgin.EncodeToken(jsonbgin.Token{
		OpClass: indexmetadata.OpClassJsonbOps,
		Kind:    jsonbgin.TokenKindKey,
		Value:   "draft",
	})

	table := &fakePostingTable{
		rows: []sql.Row{
			{wanted, "row/1"},
			{wanted, "row/2"},
			{other, "row/3"},
		},
	}

	rowIDs, err := lookupPostingTokenRowIDs(ctx, table, wanted)
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{"row/1": {}, "row/2": {}}, rowIDs)
	require.Equal(t, 1, table.indexedAccesses)
	require.Zero(t, table.fullScans)
}

type fakePostingTable struct {
	rows            []sql.Row
	indexedAccesses int
	fullScans       int
}

var _ sql.IndexAddressableTable = (*fakePostingTable)(nil)

func (t *fakePostingTable) Name() string {
	return "postings"
}

func (t *fakePostingTable) String() string {
	return "postings"
}

func (t *fakePostingTable) Schema(*sql.Context) sql.Schema {
	return sql.Schema{
		{Name: "token", Source: "postings", Type: pgtypes.Text, PrimaryKey: true, Nullable: false},
		{Name: "row_id", Source: "postings", Type: pgtypes.Text, PrimaryKey: true, Nullable: false},
	}
}

func (t *fakePostingTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

func (t *fakePostingTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	t.fullScans++
	return nil, errors.New("unexpected full posting table scan")
}

func (t *fakePostingTable) PartitionRows(*sql.Context, sql.Partition) (sql.RowIter, error) {
	return nil, errors.New("unexpected full posting table rows")
}

func (t *fakePostingTable) IndexedAccess(_ *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	t.indexedAccesses++
	return &fakePostingIndexedTable{
		table:  t,
		lookup: lookup,
	}
}

func (t *fakePostingTable) GetIndexes(*sql.Context) ([]sql.Index, error) {
	return []sql.Index{fakePostingIndex{}}, nil
}

func (t *fakePostingTable) PreciseMatch() bool {
	return true
}

type fakePostingIndexedTable struct {
	table  *fakePostingTable
	lookup sql.IndexLookup
}

var _ sql.IndexedTable = (*fakePostingIndexedTable)(nil)

func (t *fakePostingIndexedTable) Name() string {
	return t.table.Name()
}

func (t *fakePostingIndexedTable) String() string {
	return t.table.String()
}

func (t *fakePostingIndexedTable) Schema(ctx *sql.Context) sql.Schema {
	return t.table.Schema(ctx)
}

func (t *fakePostingIndexedTable) Collation() sql.CollationID {
	return t.table.Collation()
}

func (t *fakePostingIndexedTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return t.LookupPartitions(ctx, t.lookup)
}

func (t *fakePostingIndexedTable) LookupPartitions(_ *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	token, ok := tokenFromPostingLookup(lookup)
	if !ok {
		return nil, fmt.Errorf("expected exact token lookup, got %s", lookup.Ranges.String())
	}
	return sql.PartitionsToPartitionIter(fakePostingPartition{token: token}), nil
}

func (t *fakePostingIndexedTable) PartitionRows(_ *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	postingPartition, ok := partition.(fakePostingPartition)
	if !ok {
		return nil, fmt.Errorf("unexpected partition %T", partition)
	}
	var rows []sql.Row
	for _, row := range t.table.rows {
		if row[0] == postingPartition.token {
			rows = append(rows, row)
		}
	}
	return sql.RowsToRowIter(rows...), nil
}

type fakePostingPartition struct {
	token string
}

func (p fakePostingPartition) Key() []byte {
	return []byte(p.token)
}

type fakePostingIndex struct{}

var _ sql.Index = fakePostingIndex{}

func (fakePostingIndex) ID() string {
	return "PRIMARY"
}

func (fakePostingIndex) Database() string {
	return ""
}

func (fakePostingIndex) Table() string {
	return "postings"
}

func (fakePostingIndex) Expressions() []string {
	return []string{"postings.token", "postings.row_id"}
}

func (fakePostingIndex) IsUnique() bool {
	return true
}

func (fakePostingIndex) IsSpatial() bool {
	return false
}

func (fakePostingIndex) IsFullText() bool {
	return false
}

func (fakePostingIndex) IsVector() bool {
	return false
}

func (fakePostingIndex) Comment() string {
	return ""
}

func (fakePostingIndex) IndexType() string {
	return "BTREE"
}

func (fakePostingIndex) IsGenerated() bool {
	return false
}

func (fakePostingIndex) ColumnExpressionTypes(*sql.Context) []sql.ColumnExpressionType {
	return []sql.ColumnExpressionType{
		{Expression: "postings.token", Type: pgtypes.Text},
		{Expression: "postings.row_id", Type: pgtypes.Text},
	}
}

func (fakePostingIndex) CanSupport(*sql.Context, ...sql.Range) bool {
	return true
}

func (fakePostingIndex) CanSupportOrderBy(sql.Expression) bool {
	return false
}

func (fakePostingIndex) PrefixLengths() []uint16 {
	return nil
}

func tokenFromPostingLookup(lookup sql.IndexLookup) (string, bool) {
	ranges, ok := lookup.Ranges.(sql.MySQLRangeCollection)
	if !ok || len(ranges) != 1 || len(ranges[0]) != 1 {
		return "", false
	}
	lower, ok := ranges[0][0].LowerBound.(sql.Below)
	if !ok {
		return "", false
	}
	upper, ok := ranges[0][0].UpperBound.(sql.Above)
	if !ok || lower.Key != upper.Key {
		return "", false
	}
	token, ok := lower.Key.(string)
	return token, ok
}
