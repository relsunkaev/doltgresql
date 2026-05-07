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
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	doltschema "github.com/dolthub/dolt/go/libraries/doltcore/schema"
	doltindex "github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	dolttypes "github.com/dolthub/dolt/go/store/types"
	gmsexpression "github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	"github.com/dolthub/doltgresql/server/indexmetadata"
	"github.com/dolthub/doltgresql/server/jsonbgin"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func TestPostingChunkPayloadBytesUnwrapsByteWrappers(t *testing.T) {
	ctx := sql.NewEmptyContext()
	payload := []byte("posting-chunk-payload")

	got, err := postingChunkPayloadBytes(ctx, payload)
	require.NoError(t, err)
	require.Equal(t, payload, got)

	got, err = postingChunkPayloadBytes(ctx, testPostingChunkBytesWrapper{payload: payload})
	require.NoError(t, err)
	require.Equal(t, payload, got)

	_, err = postingChunkPayloadBytes(ctx, "not bytes")
	require.ErrorContains(t, err, "unexpected JSONB GIN posting chunk payload type string")
}

type testPostingChunkBytesWrapper struct {
	payload []byte
}

func (w testPostingChunkBytesWrapper) Unwrap(ctx context.Context) ([]byte, error) {
	return append([]byte(nil), w.payload...), nil
}

func (w testPostingChunkBytesWrapper) UnwrapAny(ctx context.Context) (interface{}, error) {
	return w.Unwrap(ctx)
}

func (w testPostingChunkBytesWrapper) IsExactLength() bool {
	return true
}

func (w testPostingChunkBytesWrapper) MaxByteLength() int64 {
	return int64(len(w.payload))
}

func (w testPostingChunkBytesWrapper) Compare(ctx context.Context, other interface{}) (int, bool, error) {
	return 0, false, nil
}

func (w testPostingChunkBytesWrapper) Hash() interface{} {
	return string(w.payload)
}

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

func TestJsonbGinPostingChunkTokenBatchLookupUsesSingleIndexAccess(t *testing.T) {
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
	keyTypes := []sql.ColumnExpressionType{{Expression: "docs.id", Type: pgtypes.Int32}}
	vipRow, vipRefs := jsonbGinPostingChunkTestRow(t, ctx, vip, 0, []int32{1, 3})
	draftRow, draftRefs := jsonbGinPostingChunkTestRow(t, ctx, draft, 0, []int32{2})
	otherRow, _ := jsonbGinPostingChunkTestRow(t, ctx, other, 0, []int32{4})
	table := &fakePostingTable{
		rows: []sql.Row{vipRow, draftRow, otherRow},
	}

	rowIDs, candidates, err := lookupPostingChunkTokensRowIDsAndCandidates(ctx, table, []string{vip, draft}, keyTypes)
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{string(vipRefs[0]): {}, string(vipRefs[1]): {}}, rowIDs[0])
	require.Equal(t, map[string]struct{}{string(draftRefs[0]): {}}, rowIDs[1])
	require.Equal(t, sql.Row{int32(1)}, candidates[0][string(vipRefs[0])].key)
	require.Equal(t, sql.Row{int32(3)}, candidates[0][string(vipRefs[1])].key)
	require.Equal(t, sql.Row{int32(2)}, candidates[1][string(draftRefs[0])].key)
	require.Equal(t, 1, table.indexedAccesses)
	require.Zero(t, table.fullScans)
}

func TestJsonbGinPostingChunkTokenCountUsesIndex(t *testing.T) {
	ctx := sql.NewEmptyContext()
	vip := jsonbgin.EncodeToken(jsonbgin.Token{
		OpClass: indexmetadata.OpClassJsonbOps,
		Kind:    jsonbgin.TokenKindKey,
		Value:   "vip",
	})
	other := jsonbgin.EncodeToken(jsonbgin.Token{
		OpClass: indexmetadata.OpClassJsonbOps,
		Kind:    jsonbgin.TokenKindKey,
		Value:   "archived",
	})
	vipChunkA, _ := jsonbGinPostingChunkTestRow(t, ctx, vip, 0, []int32{1, 2})
	vipChunkB, _ := jsonbGinPostingChunkTestRow(t, ctx, vip, 1, []int32{3})
	otherChunk, _ := jsonbGinPostingChunkTestRow(t, ctx, other, 0, []int32{4, 5, 6})
	table := &fakePostingTable{
		rows: []sql.Row{vipChunkA, vipChunkB, otherChunk},
	}

	exceeds, err := postingChunkTokenRowRefCountExceeds(ctx, table, vip, 2)
	require.NoError(t, err)
	require.True(t, exceeds)
	require.Equal(t, 1, table.indexedAccesses)
	require.Zero(t, table.fullScans)
}

func TestJsonbGinPostingChunkTokenCountUsesRowCountMetadata(t *testing.T) {
	ctx := sql.NewEmptyContext()
	vip := jsonbgin.EncodeToken(jsonbgin.Token{
		OpClass: indexmetadata.OpClassJsonbOps,
		Kind:    jsonbgin.TokenKindKey,
		Value:   "vip",
	})
	vipChunk, _ := jsonbGinPostingChunkTestRow(t, ctx, vip, 0, []int32{1, 2, 3})
	payload := append([]byte(nil), vipChunk[6].([]byte)...)
	payload[len(payload)-5] ^= 0xff
	vipChunk[6] = payload
	table := &fakePostingTable{
		rows: []sql.Row{vipChunk},
	}

	exceeds, err := postingChunkTokenRowRefCountExceeds(ctx, table, vip, 2)
	require.NoError(t, err)
	require.True(t, exceeds)
	require.Equal(t, 1, table.indexedAccesses)
	require.Zero(t, table.fullScans)
}

func TestJsonbGinPostingChunkTokenCountFallsBackToPayloadMetadata(t *testing.T) {
	ctx := sql.NewEmptyContext()
	vip := jsonbgin.EncodeToken(jsonbgin.Token{
		OpClass: indexmetadata.OpClassJsonbOps,
		Kind:    jsonbgin.TokenKindKey,
		Value:   "vip",
	})

	testCases := []struct {
		name      string
		mutateRow func(sql.Row)
	}{
		{
			name: "missing row count",
			mutateRow: func(row sql.Row) {
				row[3] = nil
			},
		},
		{
			name: "stale row count",
			mutateRow: func(row sql.Row) {
				row[3] = int32(1)
			},
		},
		{
			name: "stale checksum",
			mutateRow: func(row sql.Row) {
				row[7] = []byte{0, 0, 0, 0}
			},
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			vipChunk, _ := jsonbGinPostingChunkTestRow(t, ctx, vip, 0, []int32{1, 2, 3})
			test.mutateRow(vipChunk)
			table := &fakePostingTable{
				rows: []sql.Row{vipChunk},
			}

			exceeds, err := postingChunkTokenRowRefCountExceeds(ctx, table, vip, 2)
			require.NoError(t, err)
			require.True(t, exceeds)
			require.Equal(t, 1, table.indexedAccesses)
			require.Zero(t, table.fullScans)
		})
	}
}

func TestJsonbGinPostingCandidateFromRowReference(t *testing.T) {
	ctx := sql.NewEmptyContext()
	keyTypes := []sql.ColumnExpressionType{
		{Expression: "docs.tenant", Type: pgtypes.Text},
		{Expression: "docs.id", Type: pgtypes.Int32},
	}
	rowRef, err := jsonbgin.EncodeRowReference(ctx, []sql.Type{pgtypes.Text, pgtypes.Int32}, sql.Row{"east", int32(42)})
	require.NoError(t, err)

	candidate, err := jsonbGinPostingCandidateFromRowReference(ctx, "row/ref", keyTypes, rowRef.Bytes)
	require.NoError(t, err)
	require.Equal(t, "row/ref", candidate.rowID)
	require.Equal(t, sql.Row{"east", int32(42)}, candidate.key)

	ranges := primaryKeyRanges(candidate.key, keyTypes)
	require.Len(t, ranges, 2)

	numericKeyTypes := []sql.ColumnExpressionType{
		{Expression: "docs.id", Type: pgtypes.Numeric},
	}
	numericRef, err := jsonbGinPostingRowReference(ctx, sql.Schema{
		{Name: "id", Type: pgtypes.Numeric, PrimaryKey: true},
		{Name: "doc", Type: pgtypes.JsonB},
	}, sql.Row{decimal.RequireFromString("42.50"), pgtypes.JsonDocument{}})
	require.NoError(t, err)
	require.Equal(t, jsonbgin.RowReferenceKindOrdered, numericRef.Kind)

	numericCandidate, err := jsonbGinPostingCandidateFromRowReference(ctx, "numeric/ref", numericKeyTypes, numericRef.Bytes)
	require.NoError(t, err)
	require.Equal(t, "numeric/ref", numericCandidate.rowID)
	require.Len(t, numericCandidate.key, 1)
	cmp, err := pgtypes.Numeric.Compare(ctx, decimal.RequireFromString("42.5"), numericCandidate.key[0])
	require.NoError(t, err)
	require.Zero(t, cmp)
}

func TestJsonbGinPostingCandidateFromOpaqueRowReference(t *testing.T) {
	rowRef, err := jsonbgin.EncodeOpaqueRowReference("opaque-row-id")
	require.NoError(t, err)

	candidate, err := jsonbGinPostingCandidateFromRowReference(sql.NewEmptyContext(), "ignored", nil, rowRef.Bytes)
	require.NoError(t, err)
	require.Equal(t, "opaque-row-id", candidate.rowID)
	require.Empty(t, candidate.key)
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

func TestJsonbGinPostingChunkEditorAppendsDMLChunksWithoutRewritingExisting(t *testing.T) {
	ctx := sql.NewEmptyContext()
	token := "token/shared"
	existingRow, _ := jsonbGinPostingChunkTestRow(t, ctx, token, 0, []int32{1, 2})
	_, insertedRefs := jsonbGinPostingChunkTestRow(t, ctx, token, 0, []int32{3})
	table := &fakePostingTable{rows: []sql.Row{existingRow}}
	editor := &recordingPostingEditor{}
	posting := jsonbGinPostingChunkEditor{
		table:   table,
		editor:  editor,
		pending: make(map[string]map[string]jsonbGinPendingPostingChunk),
	}

	posting.stageInsert(insertedRefs[0], []string{token})

	require.NoError(t, posting.flush(ctx))
	require.Empty(t, editor.deleted)
	require.Len(t, editor.inserted, 1)
	require.Zero(t, table.indexedAccesses)
	require.Zero(t, table.fullScans)
	chunkNo, ok, err := postingChunkRowChunkNo(editor.inserted[0])
	require.NoError(t, err)
	require.True(t, ok)
	require.GreaterOrEqual(t, chunkNo, jsonbGinPostingChunkDMLChunkNoBase)
	requirePostingChunkRow(t, ctx, editor.inserted[0], chunkNo, []int32{3})
	require.Empty(t, posting.pending)
}

func TestJsonbGinPostingChunkEditorDeletesOnlyTouchedChunks(t *testing.T) {
	ctx := sql.NewEmptyContext()
	token := "token/shared"
	existingRow, existingRefs := jsonbGinPostingChunkTestRow(t, ctx, token, 0, []int32{1, 2})
	appendedRow, _ := jsonbGinPostingChunkTestRow(t, ctx, token, jsonbGinPostingChunkDMLChunkNoBase, []int32{3})
	table := &fakePostingTable{rows: []sql.Row{existingRow, appendedRow}}
	editor := &recordingPostingEditor{}
	posting := jsonbGinPostingChunkEditor{
		table:   table,
		editor:  editor,
		pending: make(map[string]map[string]jsonbGinPendingPostingChunk),
	}

	posting.stageDelete(existingRefs[1], []string{token})

	require.NoError(t, posting.flush(ctx))
	require.Equal(t, []sql.Row{existingRow}, editor.deleted)
	require.Len(t, editor.inserted, 1)
	requirePostingChunkRow(t, ctx, editor.inserted[0], 0, []int32{1})
	require.Empty(t, posting.pending)
}

func TestJsonbGinPostingChunkEditorSkipsDeleteChunksOutsideRowRefRange(t *testing.T) {
	ctx := sql.NewEmptyContext()
	token := "token/shared"
	existingRow, existingRefs := jsonbGinPostingChunkTestRow(t, ctx, token, 0, []int32{1, 2})
	farFirst, err := jsonbgin.EncodeRowReference(ctx, []sql.Type{pgtypes.Int32}, sql.Row{int32(100)})
	require.NoError(t, err)
	farLast, err := jsonbgin.EncodeRowReference(ctx, []sql.Type{pgtypes.Int32}, sql.Row{int32(101)})
	require.NoError(t, err)
	malformedFarRow := sql.Row{
		token,
		int64(1),
		int16(jsonbgin.PostingChunkFormatVersion),
		int32(2),
		farFirst.Bytes,
		farLast.Bytes,
		[]byte("not a posting chunk"),
		[]byte{0, 0, 0, 0},
	}
	table := &fakePostingTable{rows: []sql.Row{existingRow, malformedFarRow}}
	editor := &recordingPostingEditor{}
	posting := jsonbGinPostingChunkEditor{
		table:   table,
		editor:  editor,
		pending: make(map[string]map[string]jsonbGinPendingPostingChunk),
	}

	posting.stageDelete(existingRefs[0], []string{token})

	require.NoError(t, posting.flush(ctx))
	require.Equal(t, []sql.Row{existingRow}, editor.deleted)
	require.Len(t, editor.inserted, 1)
	requirePostingChunkRow(t, ctx, editor.inserted[0], 0, []int32{2})
	require.Empty(t, posting.pending)
}

func TestJsonbGinPostingChunkEditorCompactsMultiRowDMLInserts(t *testing.T) {
	ctx := sql.NewEmptyContext()
	token := "token/shared"
	_, insertedRefs := jsonbGinPostingChunkTestRow(t, ctx, token, 0, []int32{1, 2, 3, 4, 5})
	table := &fakePostingTable{}
	editor := &recordingPostingEditor{}
	posting := jsonbGinPostingChunkEditor{
		table:   table,
		editor:  editor,
		pending: make(map[string]map[string]jsonbGinPendingPostingChunk),
	}

	for _, rowRef := range insertedRefs {
		posting.stageInsert(rowRef, []string{token})
	}

	require.NoError(t, posting.flush(ctx))
	require.Zero(t, table.indexedAccesses)
	require.Zero(t, table.fullScans)
	require.Empty(t, editor.deleted)
	require.Len(t, editor.inserted, 1)
	chunkNo, ok, err := postingChunkRowChunkNo(editor.inserted[0])
	require.NoError(t, err)
	require.True(t, ok)
	require.GreaterOrEqual(t, chunkNo, jsonbGinPostingChunkDMLChunkNoBase)
	requirePostingChunkRow(t, ctx, editor.inserted[0], chunkNo, []int32{1, 2, 3, 4, 5})
	require.Empty(t, posting.pending)
}

func TestMaterializePostingChunkRowsForAppendedDMLSplitsAndDeduplicates(t *testing.T) {
	ctx := sql.NewEmptyContext()
	token := "token/shared"
	_, rowRefs := jsonbGinPostingChunkTestRow(t, ctx, token, 0, []int32{1, 2, 3, 4, 5})
	rowRefs = append(rowRefs, rowRefs[1])

	rows, err := materializePostingChunkRowsForAppendedDML(token, rowRefs, nil, 2)

	require.NoError(t, err)
	require.Len(t, rows, 3)
	for _, row := range rows {
		chunkNo, ok, err := postingChunkRowChunkNo(row)
		require.NoError(t, err)
		require.True(t, ok)
		require.GreaterOrEqual(t, chunkNo, jsonbGinPostingChunkDMLChunkNoBase)
	}
	requirePostingChunkRow(t, ctx, rows[0], rows[0][1].(int64), []int32{1, 2})
	requirePostingChunkRow(t, ctx, rows[1], rows[1][1].(int64), []int32{3, 4})
	requirePostingChunkRow(t, ctx, rows[2], rows[2][1].(int64), []int32{5})
}

func TestJsonbGinMaintainingEditorMaintainsMixedLegacyAndChunkIndexes(t *testing.T) {
	ctx := sql.NewEmptyContext()
	tableSchema := jsonbGinPostingStorageBaseSchema()
	primary := &countingRowInserter{}
	legacyEditor := &recordingPostingEditor{}
	chunkEditor := &recordingPostingEditor{}
	editor := &jsonbGinMaintainingEditor{
		tableSchema:        tableSchema,
		primaryKeyOrdinals: primaryKeyOrdinals(tableSchema),
		primary:            primary,
		postings: []jsonbGinPostingEditor{{
			index: JsonbGinMaintainedIndex{
				Name:                  "docs_doc_legacy_idx",
				ColumnName:            "doc",
				ColumnIndex:           1,
				OpClass:               indexmetadata.OpClassJsonbOps,
				PostingTable:          "docs_doc_legacy_postings",
				PostingStorageVersion: indexmetadata.GinPostingStorageLegacy,
			},
			editor: legacyEditor,
		}},
		postingChunks: []jsonbGinPostingChunkEditor{{
			index: JsonbGinMaintainedIndex{
				Name:                  "docs_doc_chunk_idx",
				ColumnName:            "doc",
				ColumnIndex:           1,
				OpClass:               indexmetadata.OpClassJsonbOps,
				PostingChunkTable:     "docs_doc_chunk_posting_chunks",
				PostingStorageVersion: indexmetadata.GinPostingStorageChunked,
			},
			table:  &fakePostingTable{},
			editor: chunkEditor,
		}},
	}

	editor.StatementBegin(ctx)
	require.NoError(t, editor.Insert(ctx, sql.Row{int32(1), `{"tags":["vip"],"status":"open"}`}))
	require.NoError(t, editor.StatementComplete(ctx))

	require.Equal(t, 1, primary.count)
	require.NotEmpty(t, legacyEditor.inserted)
	require.NotEmpty(t, chunkEditor.inserted)
	require.Len(t, chunkEditor.inserted[0], 8)
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

func TestCreateJsonbGinIndexDefaultPostingStorageMetadata(t *testing.T) {
	create := NewCreateJsonbGinIndex(false, "public", "docs", "docs_doc_idx", "doc", indexmetadata.OpClassJsonbOps)

	metadata := create.indexMetadata()
	require.NotNil(t, metadata.Gin)
	require.Equal(t, indexmetadata.GinPostingStorageChunked, metadata.Gin.PostingStorageVersion)
	require.Empty(t, metadata.Gin.PostingTable)
	require.Equal(t, jsonbgin.PostingChunkTableName("docs", "docs_doc_idx"), metadata.Gin.PostingChunkTable)
}

func TestCreateJsonbGinIndexCreatesPostingChunkStorageByDefault(t *testing.T) {
	ctx := sql.NewEmptyContext()
	create := NewCreateJsonbGinIndex(false, "public", "docs", "docs_doc_idx", "doc", indexmetadata.OpClassJsonbOps)
	creator := &recordingTableCreator{}

	require.NoError(t, create.createPostingStorageTables(ctx, creator, jsonbGinPostingStorageBaseSchema()))
	require.Len(t, creator.created, 1)
	require.Equal(t, jsonbgin.PostingChunkTableName("docs", "docs_doc_idx"), creator.created[0].name)
	require.Equal(t, jsonbGinPostingChunkTableComment, creator.created[0].comment)
	for _, columnName := range []string{"token", "chunk_no", "format_version", "row_count", "first_row_ref", "last_row_ref", "payload", "checksum"} {
		require.NotEqual(t, -1, creator.created[0].schema.Schema.IndexOfColName(columnName), columnName)
	}
	require.Equal(t, -1, creator.created[0].schema.Schema.IndexOfColName("row_id"))
}

func TestCreateJsonbGinIndexBuildsPostingChunkRowsJsonbOps(t *testing.T) {
	ctx := sql.NewEmptyContext()
	create := NewCreateJsonbGinIndex(false, "public", "docs", "docs_doc_idx", "doc", indexmetadata.OpClassJsonbOps)
	create.postingChunkRowsPerChunk = 2
	baseSchema := jsonbGinPostingStorageBaseSchema()
	rows := sql.RowsToRowIter(
		sql.Row{int32(1), `{"tags":["vip"],"status":"open"}`},
		sql.Row{int32(2), `{"tags":["vip","archived"],"status":"open"}`},
		sql.Row{int32(3), `{"tags":["standard"],"status":"closed"}`},
		sql.Row{int32(4), `{"tags":["vip"],"status":"closed"}`},
	)

	chunkRows, err := create.buildPostingChunkRows(ctx, baseSchema, rows, 1)
	require.NoError(t, err)

	vipRows := postingChunkRowsForToken(t, chunkRows, jsonbgin.EncodeToken(jsonbgin.Token{
		OpClass: indexmetadata.OpClassJsonbOps,
		Kind:    jsonbgin.TokenKindKey,
		Value:   "vip",
	}))
	require.Len(t, vipRows, 2)
	requirePostingChunkRow(t, ctx, vipRows[0], 0, []int32{1, 2})
	requirePostingChunkRow(t, ctx, vipRows[1], 1, []int32{4})
}

func TestCreateJsonbGinIndexBuildsPostingChunkRowsJsonbPathOps(t *testing.T) {
	ctx := sql.NewEmptyContext()
	create := NewCreateJsonbGinIndex(false, "public", "docs", "docs_doc_idx", "doc", indexmetadata.OpClassJsonbPathOps)
	create.postingChunkRowsPerChunk = 2
	baseSchema := jsonbGinPostingStorageBaseSchema()
	rows := sql.RowsToRowIter(
		sql.Row{int32(1), `{"payload":{"category":"cat-1"}}`},
		sql.Row{int32(2), `{"payload":{"category":"cat-1"}}`},
		sql.Row{int32(3), `{"payload":{"category":"cat-2"}}`},
	)

	chunkRows, err := create.buildPostingChunkRows(ctx, baseSchema, rows, 1)
	require.NoError(t, err)
	require.NotEmpty(t, chunkRows)
	token, err := jsonbgin.DecodeToken(chunkRows[0][0].(string))
	require.NoError(t, err)
	require.Equal(t, indexmetadata.OpClassJsonbPathOps, token.OpClass)
	require.Equal(t, jsonbgin.TokenKindPathValue, token.Kind)
}

func TestCreateJsonbGinIndexBuildsPostingChunkRowsWithSpill(t *testing.T) {
	ctx := sql.NewEmptyContext()
	baseSchema := jsonbGinPostingStorageBaseSchema()
	rows := []sql.Row{
		{int32(1), `{"tags":["vip"],"status":"open","payload":{"category":"cat-1"}}`},
		{int32(2), `{"tags":["vip","archived"],"status":"open","payload":{"category":"cat-2"}}`},
		{int32(3), `{"tags":["standard"],"status":"closed","payload":{"category":"cat-1"}}`},
		{int32(4), `{"tags":["vip"],"status":"closed","payload":{"category":"cat-3"}}`},
	}
	create := NewCreateJsonbGinIndex(false, "public", "docs", "docs_doc_idx", "doc", indexmetadata.OpClassJsonbOps)
	create.postingChunkRowsPerChunk = 2
	expected, err := create.buildPostingChunkRows(ctx, baseSchema, &benchmarkRowIter{rows: rows}, 1)
	require.NoError(t, err)

	spilled := NewCreateJsonbGinIndex(false, "public", "docs", "docs_doc_idx", "doc", indexmetadata.OpClassJsonbOps)
	spilled.postingChunkRowsPerChunk = 2
	spilled.postingChunkBuildSpillEntries = 3
	got, err := spilled.buildPostingChunkRows(ctx, baseSchema, &benchmarkRowIter{rows: rows}, 1)
	require.NoError(t, err)
	require.Equal(t, expected, got)
}

func TestCreateJsonbGinIndexBuildsPostingChunkRowsWithParallelWorkers(t *testing.T) {
	ctx := sql.NewEmptyContext()
	testCases := []struct {
		name    string
		schema  sql.Schema
		rows    []sql.Row
		opClass string
	}{
		{
			name:    "jsonb_ops_primary_key",
			schema:  jsonbGinPostingStorageBaseSchema(),
			opClass: indexmetadata.OpClassJsonbOps,
			rows: []sql.Row{
				{int32(1), `{"tags":["vip","hot"],"status":"open","payload":{"category":"cat-1","skew":"hot"}}`},
				{int32(2), `{"tags":["vip"],"status":"open","payload":{"category":"cat-1","skew":"hot"}}`},
				{int32(3), `{"tags":["standard"],"status":"closed","payload":{"category":"cat-2","skew":"rare"}}`},
				{int32(4), `{"tags":["vip","archived"],"status":"closed","payload":{"category":"cat-3","skew":"hot"}}`},
				{int32(5), `{"tags":["standard"],"status":"open","payload":{"category":"cat-2","skew":"hot"}}`},
			},
		},
		{
			name: "jsonb_path_ops_opaque_fallback",
			schema: sql.Schema{
				{Name: "id", Type: pgtypes.Numeric, Nullable: false},
				{Name: "doc", Type: pgtypes.JsonB, Nullable: false},
			},
			opClass: indexmetadata.OpClassJsonbPathOps,
			rows: []sql.Row{
				{decimal.RequireFromString("1.1"), `{"payload":{"category":"cat-1","skew":"hot"},"tags":["vip"]}`},
				{decimal.RequireFromString("2.2"), `{"payload":{"category":"cat-1","skew":"hot"},"tags":["vip","vip"]}`},
				{decimal.RequireFromString("3.3"), `{"payload":{"category":"cat-2","skew":"rare"},"tags":["standard"]}`},
				{decimal.RequireFromString("4.4"), `{"payload":{"category":"cat-3","skew":"hot"},"tags":["vip","archived"]}`},
			},
		},
	}
	workerCounts := []int{1, 2, runtime.GOMAXPROCS(0) + 1}
	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var expected []sql.Row
			for _, workers := range workerCounts {
				create := NewCreateJsonbGinIndex(false, "public", "docs", "docs_doc_idx", "doc", test.opClass)
				create.postingChunkRowsPerChunk = 2
				create.postingChunkBuildSpillEntries = 3
				create.postingChunkBuildWorkers = workers
				got, err := create.buildPostingChunkRows(ctx, test.schema, &benchmarkRowIter{rows: test.rows}, 1)
				require.NoError(t, err)
				require.NotEmpty(t, got)
				if expected == nil {
					expected = got
					continue
				}
				require.Equal(t, expected, got, "worker count %d should match serial output", workers)
			}
		})
	}
}

func TestCreateJsonbGinIndexParallelBuildHonorsCanceledContext(t *testing.T) {
	baseCtx, cancel := context.WithCancel(context.Background())
	ctx := sql.NewContext(baseCtx)
	cancel()
	tempDir := t.TempDir()
	create := NewCreateJsonbGinIndex(false, "public", "docs", "docs_doc_idx", "doc", indexmetadata.OpClassJsonbOps)
	create.postingChunkBuildSpillEntries = 1
	create.postingChunkBuildWorkers = 2
	create.postingChunkBuildTempDir = tempDir

	_, err := create.buildPostingChunkRows(ctx, jsonbGinPostingStorageBaseSchema(), &benchmarkRowIter{rows: []sql.Row{
		{int32(1), `{"tags":["vip"],"status":"open"}`},
		{int32(2), `{"tags":["vip"],"status":"closed"}`},
	}}, 1)
	require.ErrorIs(t, err, context.Canceled)
	requireJsonbGinPostingRunDirEmpty(t, tempDir)
}

func TestCreateJsonbGinIndexParallelBuildCleansRunsOnScanError(t *testing.T) {
	ctx := sql.NewEmptyContext()
	tempDir := t.TempDir()
	create := NewCreateJsonbGinIndex(false, "public", "docs", "docs_doc_idx", "doc", indexmetadata.OpClassJsonbOps)
	create.postingChunkBuildSpillEntries = 1
	create.postingChunkBuildWorkers = 2
	create.postingChunkBuildTempDir = tempDir

	_, err := create.buildPostingChunkRows(ctx, jsonbGinPostingStorageBaseSchema(), &errorAfterRowsIter{
		rows: []sql.Row{
			{int32(1), `{"tags":["vip"],"status":"open","payload":{"category":"cat-1"}}`},
			{int32(2), `{"tags":["vip"],"status":"closed","payload":{"category":"cat-2"}}`},
		},
		err: errors.New("scan failed"),
	}, 1)
	require.ErrorContains(t, err, "scan failed")
	requireJsonbGinPostingRunDirEmpty(t, tempDir)
}

func TestCreateJsonbGinIndexParallelBuildCleansRunsOnWorkerError(t *testing.T) {
	ctx := sql.NewEmptyContext()
	tempDir := t.TempDir()
	create := NewCreateJsonbGinIndex(false, "public", "docs", "docs_doc_idx", "doc", indexmetadata.OpClassJsonbOps)
	create.postingChunkBuildSpillEntries = 1
	create.postingChunkBuildWorkers = 2
	create.postingChunkBuildTempDir = tempDir

	_, err := create.buildPostingChunkRows(ctx, jsonbGinPostingStorageBaseSchema(), &benchmarkRowIter{rows: []sql.Row{
		{int32(1), `{"tags":["vip"],"status":"open","payload":{"category":"cat-1"}}`},
		{int32(2), `{"tags":["vip"],"status":"closed","payload":{"category":"cat-2"}}`},
		{int32(3), `{"tags":[`},
	}}, 1)
	require.Error(t, err)
	requireJsonbGinPostingRunDirEmpty(t, tempDir)
}

func TestCreateJsonbGinIndexParallelBuildCleansRunsOnSuccess(t *testing.T) {
	ctx := sql.NewEmptyContext()
	tempDir := t.TempDir()
	create := NewCreateJsonbGinIndex(false, "public", "docs", "docs_doc_idx", "doc", indexmetadata.OpClassJsonbOps)
	create.postingChunkBuildSpillEntries = 1
	create.postingChunkBuildWorkers = 2
	create.postingChunkBuildTempDir = tempDir

	chunkRows, err := create.buildPostingChunkRows(ctx, jsonbGinPostingStorageBaseSchema(), &benchmarkRowIter{rows: []sql.Row{
		{int32(1), `{"tags":["vip"],"status":"open","payload":{"category":"cat-1"}}`},
		{int32(2), `{"tags":["vip"],"status":"closed","payload":{"category":"cat-2"}}`},
	}}, 1)
	require.NoError(t, err)
	require.NotEmpty(t, chunkRows)
	requireJsonbGinPostingRunDirEmpty(t, tempDir)
}

func TestCreateJsonbGinIndexWritePostingChunkRowsHonorsCanceledContextAndCleansRuns(t *testing.T) {
	baseCtx, cancel := context.WithCancel(context.Background())
	ctx := sql.NewContext(baseCtx)
	tempDir := t.TempDir()
	sorter := newJsonbGinPostingChunkEntrySorterInDir(1, tempDir)
	require.NoError(t, sorter.Add("token/a", []byte("row/1")))
	require.NoError(t, sorter.Add("token/b", []byte("row/2")))
	require.NotEmpty(t, sorter.runs)
	cancel()

	create := &CreateJsonbGinIndex{postingChunkRowsPerChunk: 10}
	err := create.writePostingChunkRowsFromEntries(ctx, sorter, &jsonbGinPostingChunkRowCollector{})
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, sorter.Close())
	requireJsonbGinPostingRunDirEmpty(t, tempDir)
}

func TestCreateJsonbGinIndexWritePostingChunkRowsCleansRunsOnSinkError(t *testing.T) {
	ctx := sql.NewEmptyContext()
	tempDir := t.TempDir()
	sorter := newJsonbGinPostingChunkEntrySorterInDir(1, tempDir)
	require.NoError(t, sorter.Add("token/a", []byte("row/1")))
	require.NoError(t, sorter.Add("token/b", []byte("row/2")))
	require.NotEmpty(t, sorter.runs)

	create := &CreateJsonbGinIndex{postingChunkRowsPerChunk: 1}
	err := create.writePostingChunkRowsFromEntries(ctx, sorter, &failingPostingRowAppender{err: errors.New("sidecar write failed")})
	require.ErrorContains(t, err, "sidecar write failed")
	require.NoError(t, sorter.Close())
	requireJsonbGinPostingRunDirEmpty(t, tempDir)
}

func TestJsonbGinPostingChunkSpillDeduplicatesEntries(t *testing.T) {
	ctx := sql.NewEmptyContext()
	sorter := newJsonbGinPostingChunkEntrySorter(1)
	defer sorter.Close()
	require.NoError(t, sorter.Add("token/a", []byte("row/1")))
	require.NoError(t, sorter.Add("token/a", []byte("row/1")))
	require.NoError(t, sorter.Add("token/a", []byte("row/2")))

	collector := &jsonbGinPostingChunkRowCollector{}
	create := &CreateJsonbGinIndex{postingChunkRowsPerChunk: 10}
	require.NoError(t, create.writePostingChunkRowsFromEntries(ctx, sorter, collector))
	require.Len(t, collector.rows, 1)
	require.Equal(t, int32(2), collector.rows[0][3])
	payload, ok := collector.rows[0][6].([]byte)
	require.True(t, ok)
	chunk, err := jsonbgin.DecodePostingChunk(payload)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("row/1"), []byte("row/2")}, chunk.RowRefs)
}

func TestJsonbGinPostingChunkEntrySorterAddRowTokensCopiesRowRefAndMatchesSpill(t *testing.T) {
	ctx := sql.NewEmptyContext()
	buildRows := func(t *testing.T, spillEntries int) []sql.Row {
		t.Helper()
		sorter := newJsonbGinPostingChunkEntrySorter(spillEntries)
		defer sorter.Close()
		sourceRowRef := []byte("row/1")
		require.NoError(t, sorter.AddRowTokens([]string{"token/b", "token/a", "token/a"}, sourceRowRef))
		copy(sourceRowRef, []byte("bad/1"))
		require.NoError(t, sorter.AddRowTokens([]string{"token/a"}, []byte("row/2")))

		collector := &jsonbGinPostingChunkRowCollector{}
		create := &CreateJsonbGinIndex{postingChunkRowsPerChunk: 10}
		require.NoError(t, create.writePostingChunkRowsFromEntries(ctx, sorter, collector))
		return collector.rows
	}

	memoryRows := buildRows(t, 100)
	spillRows := buildRows(t, 2)
	require.Equal(t, memoryRows, spillRows)
	require.Len(t, memoryRows, 2)
	require.Equal(t, "token/a", memoryRows[0][0])
	require.Equal(t, "token/b", memoryRows[1][0])

	pathPayload, ok := memoryRows[0][6].([]byte)
	require.True(t, ok)
	pathChunk, err := jsonbgin.DecodePostingChunk(pathPayload)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("row/1"), []byte("row/2")}, pathChunk.RowRefs)

	keyPayload, ok := memoryRows[1][6].([]byte)
	require.True(t, ok)
	keyChunk, err := jsonbgin.DecodePostingChunk(keyPayload)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("row/1")}, keyChunk.RowRefs)
}

func TestJsonbGinPostingChunkEntrySorterAddRowTokensValidatesInput(t *testing.T) {
	sorter := newJsonbGinPostingChunkEntrySorter(10)
	defer sorter.Close()
	require.NoError(t, sorter.AddRowTokens(nil, nil))
	require.ErrorContains(t, sorter.AddRowTokens([]string{""}, []byte("row/1")), "posting token")
	require.ErrorContains(t, sorter.AddRowTokens([]string{"token/a"}, nil), "row reference")

	iter, err := sorter.Iterator()
	require.NoError(t, err)
	_, err = iter.Next()
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, iter.Close())
}

func TestJsonbGinExtractEncodedTokensFromSQLValueMatchesConversion(t *testing.T) {
	ctx := sql.NewEmptyContext()
	input := `{"bbb":1,"a":[{"z":true},{"z":true}],"tags":["vip","vip"],"empty":{},"none":null}`
	doc, err := pgtypes.UnmarshalToJsonDocument([]byte(input))
	require.NoError(t, err)

	for _, opClass := range []string{indexmetadata.OpClassJsonbOps, indexmetadata.OpClassJsonbPathOps} {
		t.Run(opClass+"/string", func(t *testing.T) {
			expectedDoc, err := pgtypes.JsonDocumentFromSQLValue(ctx, pgtypes.JsonB, input)
			require.NoError(t, err)
			expected, err := jsonbgin.ExtractEncoded(expectedDoc, opClass)
			require.NoError(t, err)

			got, err := jsonbGinExtractEncodedTokensFromSQLValue(ctx, input, opClass)
			require.NoError(t, err)
			require.Equal(t, expected, got)
		})

		t.Run(opClass+"/document", func(t *testing.T) {
			expectedDoc, err := pgtypes.JsonDocumentFromSQLValue(ctx, pgtypes.JsonB, doc)
			require.NoError(t, err)
			expected, err := jsonbgin.ExtractEncoded(expectedDoc, opClass)
			require.NoError(t, err)

			got, err := jsonbGinExtractEncodedTokensFromSQLValue(ctx, doc, opClass)
			require.NoError(t, err)
			require.Equal(t, expected, got)
		})

		t.Run(opClass+"/value", func(t *testing.T) {
			expected, err := jsonbgin.ExtractValueEncoded(doc.Value, opClass)
			require.NoError(t, err)

			got, err := jsonbGinExtractEncodedTokensFromSQLValue(ctx, doc.Value, opClass)
			require.NoError(t, err)
			require.Equal(t, expected, got)
		})
	}

	_, err = jsonbGinExtractEncodedTokensFromSQLValue(ctx, doc, "text_ops")
	require.ErrorContains(t, err, "unsupported JSONB GIN opclass")
}

func TestJsonbGinPostingChunkEntrySorterMergeOrdersAndCleansRuns(t *testing.T) {
	sorter := newJsonbGinPostingChunkEntrySorter(2)
	defer sorter.Close()
	require.NoError(t, sorter.Add("token/b", []byte("row/2")))
	require.NoError(t, sorter.Add("token/a", []byte("row/3")))
	require.NoError(t, sorter.Add("token/a", []byte("row/1")))
	require.NoError(t, sorter.Add("token/b", []byte("row/1")))
	require.NoError(t, sorter.Add("token/a", []byte("row/2")))

	iter, err := sorter.Iterator()
	require.NoError(t, err)
	var got []jsonbGinPostingChunkBuildEntry
	for {
		entry, err := iter.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		got = append(got, entry)
	}
	require.NoError(t, iter.Close())
	require.Equal(t, []jsonbGinPostingChunkBuildEntry{
		{token: "token/a", rowRef: []byte("row/1")},
		{token: "token/a", rowRef: []byte("row/2")},
		{token: "token/a", rowRef: []byte("row/3")},
		{token: "token/b", rowRef: []byte("row/1")},
		{token: "token/b", rowRef: []byte("row/2")},
	}, got)

	runPaths := append([]string(nil), sorter.runs...)
	require.NotEmpty(t, runPaths)
	require.NoError(t, sorter.Close())
	for _, runPath := range runPaths {
		_, err = os.Stat(runPath)
		require.True(t, os.IsNotExist(err), "expected spill run %s to be removed", runPath)
	}
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

func TestSortedPrimaryRowIndexBuilderStreamsSortedRows(t *testing.T) {
	ctx := sql.NewEmptyContext()
	ns := tree.NewTestNodeStore()
	sqlSch := sql.Schema{
		{Name: "token", Source: "postings", Type: pgtypes.Text, PrimaryKey: true, Nullable: false},
		{Name: "chunk_no", Source: "postings", Type: pgtypes.Int64, PrimaryKey: true, Nullable: false},
		{Name: "payload", Source: "postings", Type: pgtypes.Bytea, Nullable: false},
	}
	doltSch := doltschema.MustSchemaFromCols(doltschema.NewColCollection(
		doltschema.NewColumn("token", 1, dolttypes.StringKind, true, doltschema.NotNullConstraint{}),
		doltschema.NewColumn("chunk_no", 2, dolttypes.IntKind, true, doltschema.NotNullConstraint{}),
		doltschema.NewColumn("payload", 3, dolttypes.InlineBlobKind, false, doltschema.NotNullConstraint{}),
	))
	rows := []sql.Row{
		{"token/a", int64(0), []byte("a0")},
		{"token/a", int64(1), []byte("a1")},
		{"token/b", int64(0), []byte("b0")},
	}

	builder, err := newSortedPrimaryRowIndexBuilder(ctx, ns, doltSch, sqlSch)
	require.NoError(t, err)
	for _, row := range rows {
		require.NoError(t, builder.Add(ctx, row))
	}
	rowData, err := builder.Complete(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(len(rows)), mustIndexCount(t, rowData))
}

func TestSortedPrimaryRowIndexBuilderRejectsUnsortedRows(t *testing.T) {
	ctx := sql.NewEmptyContext()
	ns := tree.NewTestNodeStore()
	sqlSch, doltSch := benchmarkJsonbGinPostingStorageSchemas()
	builder, err := newSortedPrimaryRowIndexBuilder(ctx, ns, doltSch, sqlSch)
	require.NoError(t, err)
	require.NoError(t, builder.Add(ctx, sql.Row{"token/b", "row/2", "pk/2"}))

	err = builder.Add(ctx, sql.Row{"token/a", "row/1", "pk/1"})
	require.ErrorContains(t, err, "sorted primary row builder received rows out of order")
}

func mustIndexCount(t *testing.T, idx durable.Index) uint64 {
	t.Helper()
	count, err := idx.Count()
	require.NoError(t, err)
	return count
}

func TestJsonbGinPostingChunkRowLessSortsRows(t *testing.T) {
	rows := []sql.Row{
		{"token/b", int64(1)},
		{"token/a", int64(2)},
		{"token/a", int64(0)},
		{"token/b", int64(0)},
	}

	sort.Slice(rows, func(i, j int) bool {
		return jsonbGinPostingChunkRowLess(rows[i], rows[j])
	})

	require.Equal(t, []sql.Row{
		{"token/a", int64(0)},
		{"token/a", int64(2)},
		{"token/b", int64(0)},
		{"token/b", int64(1)},
	}, rows)
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

func BenchmarkJsonbGinPostingChunkRowsToSink(b *testing.B) {
	ctx := sql.NewEmptyContext()
	sch := benchmarkJsonbGinSchema()
	doc := mustBenchmarkJsonDocument(b, benchmarkJsonbGinDocument())
	rowSets := []struct {
		name string
		rows []sql.Row
	}{
		{name: "string", rows: benchmarkJsonbGinRows(512)},
		{name: "document", rows: benchmarkJsonbGinRowsWithValue(512, doc)},
	}
	spillCases := []struct {
		name         string
		spillEntries int
	}{
		{name: "memory", spillEntries: 0},
		{name: "spill", spillEntries: 64},
	}
	for _, rowSet := range rowSets {
		rowSet := rowSet
		b.Run(rowSet.name, func(b *testing.B) {
			for _, opClass := range []string{indexmetadata.OpClassJsonbOps, indexmetadata.OpClassJsonbPathOps} {
				opClass := opClass
				b.Run(opClass, func(b *testing.B) {
					for _, rowsPerChunk := range []int{64, 128, 256, 512} {
						rowsPerChunk := rowsPerChunk
						b.Run(fmt.Sprintf("chunk_%d", rowsPerChunk), func(b *testing.B) {
							for _, test := range spillCases {
								test := test
								b.Run(test.name, func(b *testing.B) {
									create := &CreateJsonbGinIndex{
										opClass:                       opClass,
										postingChunkRowsPerChunk:      rowsPerChunk,
										postingChunkBuildSpillEntries: test.spillEntries,
									}
									b.ReportAllocs()
									var lastChunkRows int
									var lastPayloadBytes int
									var lastAvgRefs float64
									var lastMaxRefs int
									for i := 0; i < b.N; i++ {
										collector := &jsonbGinPostingChunkRowCollector{}
										sorter := newJsonbGinPostingChunkEntrySorter(create.postingChunkBuildSpillEntryLimit())
										if err := create.addPostingChunkEntries(ctx, sch, &benchmarkRowIter{rows: rowSet.rows}, 1, sorter); err != nil {
											b.Fatal(err)
										}
										if err := create.writePostingChunkRowsFromEntries(ctx, sorter, collector); err != nil {
											_ = sorter.Close()
											b.Fatal(err)
										}
										if err := sorter.Close(); err != nil {
											b.Fatal(err)
										}
										if len(collector.rows) == 0 {
											b.Fatal("expected chunk rows")
										}
										payloadBytes, totalRefs, maxRefs := postingChunkRowsPayloadStats(collector.rows)
										lastChunkRows = len(collector.rows)
										lastPayloadBytes = payloadBytes
										lastMaxRefs = maxRefs
										lastAvgRefs = float64(totalRefs) / float64(len(collector.rows))
									}
									b.ReportMetric(float64(lastChunkRows), "chunk_rows/op")
									b.ReportMetric(float64(lastPayloadBytes), "payload_bytes/op")
									b.ReportMetric(lastAvgRefs, "avg_refs/chunk")
									b.ReportMetric(float64(lastMaxRefs), "max_refs/chunk")
								})
							}
						})
					}
				})
			}
		})
	}
}

func postingChunkRowsPayloadStats(rows []sql.Row) (payloadBytes int, totalRefs int, maxRefs int) {
	for _, row := range rows {
		if len(row) > 6 {
			if payload, ok := row[6].([]byte); ok {
				payloadBytes += len(payload)
			}
		}
		if len(row) > 3 {
			if rowCount, ok := integralInt64(row[3]); ok {
				totalRefs += int(rowCount)
				if int(rowCount) > maxRefs {
					maxRefs = int(rowCount)
				}
			}
		}
	}
	return payloadBytes, totalRefs, maxRefs
}

func BenchmarkJsonbGinExtractEncodedTokensFromSQLValue(b *testing.B) {
	ctx := sql.NewEmptyContext()
	docText := benchmarkJsonbGinDocument()
	doc := mustBenchmarkJsonDocument(b, docText)
	values := []struct {
		name  string
		value any
	}{
		{name: "string", value: docText},
		{name: "document", value: doc},
		{name: "value", value: doc.Value},
	}
	for _, opClass := range []string{indexmetadata.OpClassJsonbOps, indexmetadata.OpClassJsonbPathOps} {
		opClass := opClass
		b.Run(opClass, func(b *testing.B) {
			for _, test := range values {
				test := test
				b.Run(test.name, func(b *testing.B) {
					b.ReportAllocs()
					var tokenCount int
					for i := 0; i < b.N; i++ {
						tokens, err := jsonbGinExtractEncodedTokensFromSQLValue(ctx, test.value, opClass)
						if err != nil {
							b.Fatal(err)
						}
						tokenCount = len(tokens)
					}
					if tokenCount == 0 {
						b.Fatal("expected tokens")
					}
					b.ReportMetric(float64(tokenCount), "tokens/op")
				})
			}
		})
	}
}

func BenchmarkJsonbGinPostingChunkRowsToSinkWorkers(b *testing.B) {
	ctx := sql.NewEmptyContext()
	sch := benchmarkJsonbGinSchema()
	doc := mustBenchmarkJsonDocument(b, benchmarkJsonbGinDocument())
	rows := benchmarkJsonbGinRowsWithValue(512, doc)
	for _, workers := range []int{1, 2, 4} {
		workers := workers
		b.Run(fmt.Sprintf("workers_%d", workers), func(b *testing.B) {
			create := &CreateJsonbGinIndex{
				opClass:                       indexmetadata.OpClassJsonbOps,
				postingChunkRowsPerChunk:      128,
				postingChunkBuildSpillEntries: 64,
				postingChunkBuildWorkers:      workers,
			}
			sink := &countingPostingRowSink{}
			b.ReportAllocs()
			var lastChunkRows int
			for i := 0; i < b.N; i++ {
				sink.count = 0
				sorter, err := create.buildPostingChunkEntrySorterFromRows(ctx, sch, &benchmarkRowIter{rows: rows}, 1)
				if err != nil {
					b.Fatal(err)
				}
				if err = create.writePostingChunkRowsFromEntries(ctx, sorter, sink); err != nil {
					_ = sorter.Close()
					b.Fatal(err)
				}
				if err = sorter.Close(); err != nil {
					b.Fatal(err)
				}
				if sink.count == 0 {
					b.Fatal("expected chunk rows")
				}
				lastChunkRows = sink.count
			}
			b.ReportMetric(float64(lastChunkRows), "chunk_rows/op")
			b.ReportMetric(float64(workers), "workers/op")
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
				rows, err := editor.postingRows(ctx, index, row, &editor.tokenScratch)
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
	b.ReportMetric(float64(len(rows)), "sidecar_rows/op")
}

func BenchmarkSortedPrimaryRowIndexBuilderPostingRows(b *testing.B) {
	ctx := sql.NewEmptyContext()
	ns := tree.NewTestNodeStore()
	sqlSch, doltSch := benchmarkJsonbGinPostingStorageSchemas()
	rows := benchmarkJsonbGinPostingStorageRows(4096)
	sort.Slice(rows, func(i, j int) bool {
		return jsonbGinPostingRowLess(rows[i], rows[j])
	})

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		builder, err := newSortedPrimaryRowIndexBuilder(ctx, ns, doltSch, sqlSch)
		if err != nil {
			b.Fatal(err)
		}
		for _, row := range rows {
			if err = builder.Add(ctx, row); err != nil {
				b.Fatal(err)
			}
		}
		rowData, err := builder.Complete(ctx)
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
	b.ReportMetric(float64(len(rows)), "sidecar_rows/op")
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

type countingPostingRowSink struct {
	count int
}

var _ jsonbGinPostingRowSink = (*countingPostingRowSink)(nil)

func (s *countingPostingRowSink) Add(*sql.Context, sql.Row) error {
	s.count++
	return nil
}

func (s *countingPostingRowSink) Complete(*sql.Context) error {
	return nil
}

func (s *countingPostingRowSink) Discard(*sql.Context, error) error {
	s.count = 0
	return nil
}

func (s *countingPostingRowSink) Close(*sql.Context) error {
	return nil
}

type failingPostingRowAppender struct {
	err error
}

var _ jsonbGinPostingRowAppender = (*failingPostingRowAppender)(nil)

func (s *failingPostingRowAppender) Add(*sql.Context, sql.Row) error {
	return s.err
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

type errorAfterRowsIter struct {
	rows []sql.Row
	pos  int
	err  error
}

var _ sql.RowIter = (*errorAfterRowsIter)(nil)

func (i *errorAfterRowsIter) Next(*sql.Context) (sql.Row, error) {
	if i.pos >= len(i.rows) {
		if i.err != nil {
			return nil, i.err
		}
		return nil, io.EOF
	}
	row := i.rows[i.pos]
	i.pos++
	return row, nil
}

func (i *errorAfterRowsIter) Close(*sql.Context) error {
	return nil
}

func requireJsonbGinPostingRunDirEmpty(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
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
	return benchmarkJsonbGinRowsWithValue(rowCount, doc)
}

func benchmarkJsonbGinRowsWithValue(rowCount int, doc any) []sql.Row {
	rows := make([]sql.Row, rowCount)
	for i := range rows {
		rows[i] = sql.Row{int32(i), doc}
	}
	return rows
}

func mustBenchmarkJsonDocument(tb testing.TB, input string) pgtypes.JsonDocument {
	tb.Helper()
	doc, err := pgtypes.UnmarshalToJsonDocument([]byte(input))
	require.NoError(tb, err)
	return doc
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

func jsonbGinPostingStorageBaseSchema() sql.Schema {
	return sql.Schema{
		{Name: "id", Type: pgtypes.Int32, PrimaryKey: true, Nullable: false},
		{Name: "doc", Type: pgtypes.JsonB, Nullable: false},
	}
}

type recordingCreatedTable struct {
	name      string
	schema    sql.PrimaryKeySchema
	collation sql.CollationID
	comment   string
}

type recordingTableCreator struct {
	created []recordingCreatedTable
}

var _ sql.TableCreator = (*recordingTableCreator)(nil)

func (c *recordingTableCreator) Name() string {
	return "recording"
}

func (c *recordingTableCreator) GetTableInsensitive(*sql.Context, string) (sql.Table, bool, error) {
	return nil, false, nil
}

func (c *recordingTableCreator) GetTableNames(*sql.Context) ([]string, error) {
	names := make([]string, len(c.created))
	for i, table := range c.created {
		names[i] = table.name
	}
	return names, nil
}

func (c *recordingTableCreator) CreateTable(_ *sql.Context, name string, schema sql.PrimaryKeySchema, collation sql.CollationID, comment string) error {
	c.created = append(c.created, recordingCreatedTable{
		name:      name,
		schema:    schema,
		collation: collation,
		comment:   comment,
	})
	return nil
}

func postingChunkRowsForToken(t *testing.T, rows []sql.Row, token string) []sql.Row {
	t.Helper()
	var tokenRows []sql.Row
	for _, row := range rows {
		require.Len(t, row, 8)
		if row[0] == token {
			tokenRows = append(tokenRows, row)
		}
	}
	return tokenRows
}

func requirePostingChunkRow(t *testing.T, ctx *sql.Context, row sql.Row, chunkNo int64, ids []int32) {
	t.Helper()
	require.Equal(t, chunkNo, row[1])
	require.Equal(t, int16(jsonbgin.PostingChunkFormatVersion), row[2])
	require.Equal(t, int32(len(ids)), row[3])
	require.NotEmpty(t, row[4])
	require.NotEmpty(t, row[5])
	payload, ok := row[6].([]byte)
	require.True(t, ok)
	checksum, ok := row[7].([]byte)
	require.True(t, ok)
	require.Len(t, checksum, 4)

	chunk, err := jsonbgin.DecodePostingChunk(payload)
	require.NoError(t, err)
	require.Equal(t, uint32(len(ids)), chunk.RowCount)
	require.Equal(t, row[4], chunk.FirstRowRef)
	require.Equal(t, row[5], chunk.LastRowRef)
	require.Len(t, chunk.RowRefs, len(ids))
	for i, rowRef := range chunk.RowRefs {
		decoded, err := jsonbgin.DecodeRowReference(ctx, []sql.Type{pgtypes.Int32}, rowRef)
		require.NoError(t, err)
		require.Equal(t, sql.Row{ids[i]}, decoded.Values)
	}
}

func jsonbGinPostingChunkTestRow(t *testing.T, ctx *sql.Context, token string, chunkNo int64, ids []int32) (sql.Row, [][]byte) {
	t.Helper()
	rowRefs := make([][]byte, len(ids))
	for i, id := range ids {
		rowRef, err := jsonbgin.EncodeRowReference(ctx, []sql.Type{pgtypes.Int32}, sql.Row{id})
		require.NoError(t, err)
		rowRefs[i] = rowRef.Bytes
	}
	chunk, err := jsonbgin.EncodePostingChunk(rowRefs)
	require.NoError(t, err)
	return sql.Row{
		token,
		chunkNo,
		int16(chunk.FormatVersion),
		int32(chunk.RowCount),
		chunk.FirstRowRef,
		chunk.LastRowRef,
		chunk.Payload,
		postingChunkChecksumBytes(chunk.Checksum),
	}, rowRefs
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
