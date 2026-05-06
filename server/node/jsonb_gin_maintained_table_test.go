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
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	doltschema "github.com/dolthub/dolt/go/libraries/doltcore/schema"
	doltindex "github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	dolttypes "github.com/dolthub/dolt/go/store/types"
	gmsexpression "github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
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

func TestJsonbGinPostingTokenBatchLookupUsesSingleIndexAccess(t *testing.T) {
	ctx := sql.NewEmptyContext()
	vip := jsonbgin.EncodeToken(jsonbgin.Token{
		OpClass: indexmetadata.OpClassJsonbOps,
		Kind:    jsonbgin.TokenKindKey,
		Value:   "vip",
	})
	draft := jsonbgin.EncodeToken(jsonbgin.Token{
		OpClass: indexmetadata.OpClassJsonbOps,
		Kind:    jsonbgin.TokenKindKey,
		Value:   "draft",
	})
	other := jsonbgin.EncodeToken(jsonbgin.Token{
		OpClass: indexmetadata.OpClassJsonbOps,
		Kind:    jsonbgin.TokenKindKey,
		Value:   "archived",
	})

	table := &fakePostingTable{
		rows: []sql.Row{
			{vip, "row/1", int32(1)},
			{draft, "row/2", int32(2)},
			{vip, "row/3", int32(3)},
			{other, "row/4", int32(4)},
		},
	}

	rowIDs, candidates, err := lookupPostingTokensRowIDsAndCandidates(ctx, table, []string{vip, draft})
	require.NoError(t, err)
	require.Equal(t, []map[string]struct{}{
		{"row/1": {}, "row/3": {}},
		{"row/2": {}},
	}, rowIDs)
	require.Equal(t, map[string]jsonbGinPostingCandidate{
		"row/1": {rowID: "row/1", key: sql.Row{int32(1)}},
		"row/3": {rowID: "row/3", key: sql.Row{int32(3)}},
	}, candidates[0])
	require.Equal(t, map[string]jsonbGinPostingCandidate{
		"row/2": {rowID: "row/2", key: sql.Row{int32(2)}},
	}, candidates[1])
	require.Equal(t, 1, table.indexedAccesses)
	require.Zero(t, table.fullScans)
}

func TestJsonbGinPostingRowCompaction(t *testing.T) {
	oldRows := []sql.Row{
		{"token/a", "row/1", int32(1)},
		{"token/b", "row/1", int32(1)},
		{"token/c", "row/1", int32(1)},
	}
	newRows := []sql.Row{
		{"token/b", "row/1", int32(1)},
		{"token/c", "row/1", int32(1)},
		{"token/d", "row/1", int32(1)},
	}

	require.Equal(t, []sql.Row{{"token/a", "row/1", int32(1)}}, compactPostingRowsToDelete(oldRows, newRows))
	require.Equal(t, []sql.Row{{"token/d", "row/1", int32(1)}}, compactPostingRowsToInsert(oldRows, newRows))
	require.Empty(t, compactPostingRowsToDelete(oldRows, oldRows))
	require.Empty(t, compactPostingRowsToInsert(oldRows, oldRows))
}

func TestJsonbGinPostingEditorBatchesAndCancelsStatementRows(t *testing.T) {
	ctx := sql.NewEmptyContext()
	editor := &recordingPostingEditor{}
	posting := jsonbGinPostingEditor{
		editor:  editor,
		pending: make(map[string]jsonbGinPendingPosting),
	}

	posting.stageInsert(sql.Row{"token/a", "row/1", int32(1)})
	posting.stageDelete(sql.Row{"token/a", "row/1", int32(1)})
	posting.stageDelete(sql.Row{"token/b", "row/2", int32(2)})
	posting.stageInsert(sql.Row{"token/c", "row/3", int32(3)})

	require.NoError(t, posting.flush(ctx))
	require.Equal(t, []sql.Row{{"token/b", "row/2", int32(2)}}, editor.deleted)
	require.Equal(t, []sql.Row{{"token/c", "row/3", int32(3)}}, editor.inserted)
	require.Empty(t, posting.pending)
}

func TestJsonbGinLookupTokenCacheCopiesTokens(t *testing.T) {
	ctx := sql.NewEmptyContext()
	literal := gmsexpression.NewLiteral(`{"tenant":8,"status":"open"}`, pgtypes.JsonB)

	tokens, mode, ok, err := jsonbGinLookupTokens(ctx, indexmetadata.OpClassJsonbOps, framework.Operator_BinaryJSONContainsRight, literal)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, jsonbGinLookupIntersect, mode)
	require.NotEmpty(t, tokens)

	tokens[0] = "mutated"
	tokensAgain, modeAgain, ok, err := jsonbGinLookupTokens(ctx, indexmetadata.OpClassJsonbOps, framework.Operator_BinaryJSONContainsRight, literal)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, jsonbGinLookupIntersect, modeAgain)
	require.NotEqual(t, "mutated", tokensAgain[0])
}

func TestJsonbGinLookupTokensDeduplicatesTopLevelKeys(t *testing.T) {
	ctx := sql.NewEmptyContext()
	literal := gmsexpression.NewLiteral([]any{"vip", "draft", "vip"}, pgtypes.TextArray)
	expected := []string{
		jsonbgin.EncodeToken(jsonbgin.Token{OpClass: indexmetadata.OpClassJsonbOps, Kind: jsonbgin.TokenKindKey, Value: "draft"}),
		jsonbgin.EncodeToken(jsonbgin.Token{OpClass: indexmetadata.OpClassJsonbOps, Kind: jsonbgin.TokenKindKey, Value: "vip"}),
	}
	expected = normalizeJsonbGinLookupTokens(expected)

	tokens, mode, ok, err := jsonbGinLookupTokens(ctx, indexmetadata.OpClassJsonbOps, framework.Operator_BinaryJSONTopLevelAny, literal)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, jsonbGinLookupUnion, mode)
	require.Equal(t, expected, tokens)

	tokens, mode, ok, err = jsonbGinLookupTokens(ctx, indexmetadata.OpClassJsonbOps, framework.Operator_BinaryJSONTopLevelAll, literal)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, jsonbGinLookupIntersect, mode)
	require.Equal(t, expected, tokens)
}

func TestJsonbGinMaintainedTableProjectionRemapsColumnIndex(t *testing.T) {
	ctx := sql.NewEmptyContext()
	underlying := &fakeProjectedTable{
		schema: sql.Schema{
			{Name: "id", Type: pgtypes.Int32, PrimaryKey: true, Nullable: false},
			{Name: "label", Type: pgtypes.Text, Nullable: false},
			{Name: "doc", Type: pgtypes.JsonB, Nullable: false},
		},
	}
	table := &JsonbGinMaintainedTable{
		underlying: underlying,
		indexes: []JsonbGinMaintainedIndex{{
			ColumnName:  "doc",
			ColumnIndex: 2,
			OpClass:     indexmetadata.OpClassJsonbOps,
		}},
	}

	projected, err := table.WithProjections(ctx, []string{"id", "doc"})
	require.NoError(t, err)

	maintained := projected.(*JsonbGinMaintainedTable)
	require.Equal(t, []string{"id", "doc"}, maintained.Projections())
	require.Equal(t, 1, maintained.indexes[0].ColumnIndex)
	require.Equal(t, sql.Schema{
		{Name: "id", Type: pgtypes.Int32, PrimaryKey: true, Nullable: false},
		{Name: "doc", Type: pgtypes.JsonB, Nullable: false},
	}, maintained.Schema(ctx))
}

func TestJsonbGinPostingRowBufferSortsRows(t *testing.T) {
	ctx := sql.NewEmptyContext()
	editor := &recordingPostingEditor{}
	buffer := newJsonbGinPostingRowBuffer(editor, 0)

	require.NoError(t, buffer.Add(ctx, sql.Row{"token/c", "row/2", int32(2)}))
	require.NoError(t, buffer.Add(ctx, sql.Row{"token/a", "row/3", int32(3)}))
	require.NoError(t, buffer.Add(ctx, sql.Row{"token/a", "row/1", int32(1)}))
	require.NoError(t, buffer.Flush(ctx))

	require.Equal(t, []sql.Row{
		{"token/a", "row/1", int32(1)},
		{"token/a", "row/3", int32(3)},
		{"token/c", "row/2", int32(2)},
	}, editor.inserted)
	require.Empty(t, buffer.rows)
}

func TestJsonbGinPostingRowBufferFlushesChunks(t *testing.T) {
	ctx := sql.NewEmptyContext()
	editor := &recordingPostingEditor{}
	buffer := newJsonbGinPostingRowBuffer(editor, 2)

	require.NoError(t, buffer.Add(ctx, sql.Row{"token/b", "row/2"}))
	require.Empty(t, editor.inserted)
	require.NoError(t, buffer.Add(ctx, sql.Row{"token/a", "row/1"}))
	require.Equal(t, []sql.Row{
		{"token/a", "row/1"},
		{"token/b", "row/2"},
	}, editor.inserted)
	require.Empty(t, buffer.rows)

	require.NoError(t, buffer.Add(ctx, sql.Row{"token/d", "row/4"}))
	require.NoError(t, buffer.Flush(ctx))
	require.Equal(t, []sql.Row{
		{"token/a", "row/1"},
		{"token/b", "row/2"},
		{"token/d", "row/4"},
	}, editor.inserted)
}

func TestBuildSortedPrimaryRowIndexSortsAndMaterializesRows(t *testing.T) {
	ctx := sql.NewEmptyContext()
	ns := tree.NewTestNodeStore()
	sqlSch := sql.Schema{
		{Name: "token", Source: "postings", Type: pgtypes.Text, PrimaryKey: true, Nullable: false},
		{Name: "row_id", Source: "postings", Type: pgtypes.Text, PrimaryKey: true, Nullable: false},
		{Name: "pk_2", Source: "postings", Type: pgtypes.Text, Nullable: true},
	}
	doltSch := doltschema.MustSchemaFromCols(doltschema.NewColCollection(
		doltschema.NewColumn("token", 1, dolttypes.StringKind, true, doltschema.NotNullConstraint{}),
		doltschema.NewColumn("row_id", 2, dolttypes.StringKind, true, doltschema.NotNullConstraint{}),
		doltschema.NewColumn("pk_2", 3, dolttypes.StringKind, false),
	))
	rows := []sql.Row{
		{"token/c", "row/2", "pk-2"},
		{"token/a", "row/3", nil},
		{"token/a", "row/1", "pk-1"},
	}

	rowData, err := buildSortedPrimaryRowIndex(ctx, ns, doltSch, sqlSch, rows, jsonbGinPostingRowLess)
	require.NoError(t, err)
	rowMap, err := durable.ProllyMapFromIndex(rowData)
	require.NoError(t, err)
	iter, err := rowMap.IterAll(ctx)
	require.NoError(t, err)
	rowIter := doltindex.NewProllyRowIterForSchema(doltSch, iter, doltSch.GetKeyDescriptor(ns), doltSch.GetValueDescriptor(ns), doltSch.GetAllCols().Tags, ns)
	defer rowIter.Close(ctx)

	var got []sql.Row
	for {
		row, err := rowIter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		got = append(got, row)
	}
	require.Equal(t, []sql.Row{
		{"token/a", "row/1", "pk-1"},
		{"token/a", "row/3", nil},
		{"token/c", "row/2", "pk-2"},
	}, got)
}

func BenchmarkJsonbGinBackfillPartitionEncodedTokens(b *testing.B) {
	ctx := sql.NewEmptyContext()
	sch := benchmarkJsonbGinSchema()
	rows := benchmarkJsonbGinRows(128)

	for _, opClass := range []string{indexmetadata.OpClassJsonbOps, indexmetadata.OpClassJsonbPathOps} {
		b.Run(opClass, func(b *testing.B) {
			create := &CreateJsonbGinIndex{opClass: opClass}
			inserter := &countingRowInserter{}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				inserter.count = 0
				iter := &benchmarkRowIter{rows: rows}
				buffer := newJsonbGinPostingRowBuffer(inserter, jsonbGinPostingBackfillChunkRows)
				if err := create.backfillPartition(ctx, sch, iter, buffer, 1); err != nil {
					b.Fatal(err)
				}
				if err := buffer.Flush(ctx); err != nil {
					b.Fatal(err)
				}
				if inserter.count == 0 {
					b.Fatal("expected posting rows")
				}
			}
		})
	}
}

func BenchmarkJsonbGinPostingRowsEncodedTokens(b *testing.B) {
	ctx := sql.NewEmptyContext()
	sch := benchmarkJsonbGinSchema()
	row := sql.Row{int32(1), benchmarkJsonbGinDocument()}
	editor := jsonbGinMaintainingEditor{
		tableSchema:        sch,
		primaryKeyOrdinals: primaryKeyOrdinals(sch),
	}

	for _, opClass := range []string{indexmetadata.OpClassJsonbOps, indexmetadata.OpClassJsonbPathOps} {
		b.Run(opClass, func(b *testing.B) {
			index := JsonbGinMaintainedIndex{ColumnName: "doc", ColumnIndex: 1, OpClass: opClass}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				rows, err := editor.postingRows(ctx, index, row)
				if err != nil {
					b.Fatal(err)
				}
				if len(rows) == 0 {
					b.Fatal("expected posting rows")
				}
			}
		})
	}
}

func BenchmarkBuildSortedPrimaryRowIndexPostingRows(b *testing.B) {
	ctx := sql.NewEmptyContext()
	ns := tree.NewTestNodeStore()
	sqlSch, doltSch := benchmarkJsonbGinPostingStorageSchemas()
	rows := benchmarkJsonbGinPostingStorageRows(4096)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rowData, err := buildSortedPrimaryRowIndex(ctx, ns, doltSch, sqlSch, rows, jsonbGinPostingRowLess)
		if err != nil {
			b.Fatal(err)
		}
		count, err := rowData.Count()
		if err != nil {
			b.Fatal(err)
		}
		if count != uint64(len(rows)) {
			b.Fatalf("expected %d rows, found %d", len(rows), count)
		}
	}
}

func BenchmarkJsonbGinLookupTokensEncoded(b *testing.B) {
	ctx := sql.NewEmptyContext()
	literal := gmsexpression.NewLiteral(benchmarkJsonbGinDocument(), pgtypes.JsonB)

	for _, opClass := range []string{indexmetadata.OpClassJsonbOps, indexmetadata.OpClassJsonbPathOps} {
		b.Run(opClass+"/contains_uncached", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				jsonbGinLiteralTokenCache = sync.Map{}
				tokens, mode, ok, err := jsonbGinLookupTokens(ctx, opClass, framework.Operator_BinaryJSONContainsRight, literal)
				if err != nil {
					b.Fatal(err)
				}
				if !ok || mode != jsonbGinLookupIntersect || len(tokens) == 0 {
					b.Fatalf("unexpected lookup tokens: ok=%v mode=%s count=%d", ok, mode, len(tokens))
				}
			}
		})

		b.Run(opClass+"/contains_cached", func(b *testing.B) {
			jsonbGinLiteralTokenCache = sync.Map{}
			tokens, mode, ok, err := jsonbGinLookupTokens(ctx, opClass, framework.Operator_BinaryJSONContainsRight, literal)
			if err != nil {
				b.Fatal(err)
			}
			if !ok || mode != jsonbGinLookupIntersect || len(tokens) == 0 {
				b.Fatalf("unexpected lookup tokens: ok=%v mode=%s count=%d", ok, mode, len(tokens))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tokens, mode, ok, err := jsonbGinLookupTokens(ctx, opClass, framework.Operator_BinaryJSONContainsRight, literal)
				if err != nil {
					b.Fatal(err)
				}
				if !ok || mode != jsonbGinLookupIntersect || len(tokens) == 0 {
					b.Fatalf("unexpected lookup tokens: ok=%v mode=%s count=%d", ok, mode, len(tokens))
				}
			}
		})
	}
}

type recordingPostingEditor struct {
	inserted []sql.Row
	deleted  []sql.Row
}

var _ sql.RowReplacer = (*recordingPostingEditor)(nil)

func (e *recordingPostingEditor) StatementBegin(*sql.Context) {}

func (e *recordingPostingEditor) DiscardChanges(*sql.Context, error) error {
	e.inserted = nil
	e.deleted = nil
	return nil
}

func (e *recordingPostingEditor) StatementComplete(*sql.Context) error {
	return nil
}

func (e *recordingPostingEditor) Insert(_ *sql.Context, row sql.Row) error {
	e.inserted = append(e.inserted, row)
	return nil
}

func (e *recordingPostingEditor) Delete(_ *sql.Context, row sql.Row) error {
	e.deleted = append(e.deleted, row)
	return nil
}

func (e *recordingPostingEditor) Close(*sql.Context) error {
	return nil
}

type countingRowInserter struct {
	count int
}

var _ sql.RowInserter = (*countingRowInserter)(nil)

func (i *countingRowInserter) StatementBegin(*sql.Context) {}

func (i *countingRowInserter) DiscardChanges(*sql.Context, error) error {
	i.count = 0
	return nil
}

func (i *countingRowInserter) StatementComplete(*sql.Context) error {
	return nil
}

func (i *countingRowInserter) Insert(*sql.Context, sql.Row) error {
	i.count++
	return nil
}

func (i *countingRowInserter) Close(*sql.Context) error {
	return nil
}

type benchmarkRowIter struct {
	rows []sql.Row
	pos  int
}

var _ sql.RowIter = (*benchmarkRowIter)(nil)

func (i *benchmarkRowIter) Next(*sql.Context) (sql.Row, error) {
	if i.pos >= len(i.rows) {
		return nil, io.EOF
	}
	row := i.rows[i.pos]
	i.pos++
	return row, nil
}

func (i *benchmarkRowIter) Close(*sql.Context) error {
	return nil
}

type fakeProjectedTable struct {
	schema      sql.Schema
	projections []string
}

var _ sql.ProjectedTable = (*fakeProjectedTable)(nil)

func (t *fakeProjectedTable) Name() string {
	return "projected"
}

func (t *fakeProjectedTable) String() string {
	return "projected"
}

func (t *fakeProjectedTable) Schema(*sql.Context) sql.Schema {
	return t.schema
}

func (t *fakeProjectedTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

func (t *fakeProjectedTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return nil, errors.New("unexpected partitions")
}

func (t *fakeProjectedTable) PartitionRows(*sql.Context, sql.Partition) (sql.RowIter, error) {
	return nil, errors.New("unexpected partition rows")
}

func (t *fakeProjectedTable) WithProjections(_ *sql.Context, colNames []string) (sql.Table, error) {
	projectedSchema := make(sql.Schema, 0, len(colNames))
	for _, colName := range colNames {
		idx := t.schema.IndexOfColName(colName)
		if idx < 0 {
			return nil, errors.New("unknown projection column")
		}
		projectedSchema = append(projectedSchema, t.schema[idx])
	}
	return &fakeProjectedTable{
		schema:      projectedSchema,
		projections: append([]string(nil), colNames...),
	}, nil
}

func (t *fakeProjectedTable) Projections() []string {
	return append([]string(nil), t.projections...)
}

func benchmarkJsonbGinSchema() sql.Schema {
	return sql.Schema{
		{Name: "id", Type: pgtypes.Int32, PrimaryKey: true, Nullable: false},
		{Name: "doc", Type: pgtypes.JsonB, Nullable: false},
	}
}

func benchmarkJsonbGinRows(rowCount int) []sql.Row {
	doc := benchmarkJsonbGinDocument()
	rows := make([]sql.Row, rowCount)
	for i := range rows {
		rows[i] = sql.Row{int32(i), doc}
	}
	return rows
}

func benchmarkJsonbGinDocument() string {
	var sb strings.Builder
	sb.WriteString(`{"accounts":[`)
	for i := 0; i < 100; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `{"id":%d,"name":"account-%d","active":%t,"tags":["vip","region-%d","vip"],"metadata":{"score":%d,"empty":{}}}`,
			i, i, i%2 == 0, i%10, i*7)
	}
	sb.WriteString(`],"summary":{"count":100,"empty":[]}}`)
	return sb.String()
}

func benchmarkJsonbGinPostingStorageSchemas() (sql.Schema, doltschema.Schema) {
	sqlSch := sql.Schema{
		{Name: "token", Source: "postings", Type: pgtypes.Text, PrimaryKey: true, Nullable: false},
		{Name: "row_id", Source: "postings", Type: pgtypes.Text, PrimaryKey: true, Nullable: false},
		{Name: "pk_2", Source: "postings", Type: pgtypes.Text, Nullable: true},
	}
	doltSch := doltschema.MustSchemaFromCols(doltschema.NewColCollection(
		doltschema.NewColumn("token", 1, dolttypes.StringKind, true, doltschema.NotNullConstraint{}),
		doltschema.NewColumn("row_id", 2, dolttypes.StringKind, true, doltschema.NotNullConstraint{}),
		doltschema.NewColumn("pk_2", 3, dolttypes.StringKind, false),
	))
	return sqlSch, doltSch
}

func benchmarkJsonbGinPostingStorageRows(rowCount int) []sql.Row {
	rows := make([]sql.Row, rowCount)
	for i := range rows {
		rows[i] = sql.Row{
			fmt.Sprintf("token/%04d", rowCount-i),
			fmt.Sprintf("row/%04d", i),
			fmt.Sprintf("pk/%04d", i),
		}
	}
	return rows
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
	tokens, ok := tokensFromPostingLookup(lookup)
	if !ok {
		return nil, fmt.Errorf("expected exact token lookup, got %s", lookup.Ranges.String())
	}
	return sql.PartitionsToPartitionIter(fakePostingPartition{tokens: tokens}), nil
}

func (t *fakePostingIndexedTable) PartitionRows(_ *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	postingPartition, ok := partition.(fakePostingPartition)
	if !ok {
		return nil, fmt.Errorf("unexpected partition %T", partition)
	}
	var rows []sql.Row
	tokens := make(map[string]struct{}, len(postingPartition.tokens))
	for _, token := range postingPartition.tokens {
		tokens[token] = struct{}{}
	}
	for _, row := range t.table.rows {
		if _, ok := tokens[row[0].(string)]; ok {
			rows = append(rows, row)
		}
	}
	return sql.RowsToRowIter(rows...), nil
}

type fakePostingPartition struct {
	tokens []string
}

func (p fakePostingPartition) Key() []byte {
	return []byte(strings.Join(p.tokens, "\x00"))
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
	tokens, ok := tokensFromPostingLookup(lookup)
	if !ok || len(tokens) != 1 {
		return "", false
	}
	return tokens[0], true
}

func tokensFromPostingLookup(lookup sql.IndexLookup) ([]string, bool) {
	ranges, ok := lookup.Ranges.(sql.MySQLRangeCollection)
	if !ok || len(ranges) == 0 {
		return nil, false
	}
	tokens := make([]string, len(ranges))
	for i, lookupRange := range ranges {
		if len(lookupRange) != 1 {
			return nil, false
		}
		lower, ok := lookupRange[0].LowerBound.(sql.Below)
		if !ok {
			return nil, false
		}
		upper, ok := lookupRange[0].UpperBound.(sql.Above)
		if !ok || lower.Key != upper.Key {
			return nil, false
		}
		token, ok := lower.Key.(string)
		if !ok {
			return nil, false
		}
		tokens[i] = token
	}
	return tokens, true
}
