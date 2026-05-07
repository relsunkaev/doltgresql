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

package jsonbgin

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/doltgresql/server/indexmetadata"
)

func TestChunkedPostingStoreMatchesPostingStoreLookups(t *testing.T) {
	baseline, chunked := buildComparablePostingStores(t, 3, []string{
		`{"tenant":1,"status":"open","tags":["vip","west"],"payload":{"category":"cat-1"}}`,
		`{"tenant":1,"status":"open","tags":["standard","west"],"payload":{"category":"cat-2"}}`,
		`{"tenant":2,"status":"closed","tags":["vip","east"],"payload":{"category":"cat-1"}}`,
		`{"tenant":2,"status":"open","tags":["archived","east"],"payload":{"category":"cat-3"}}`,
		`{"tenant":3,"status":"open","tags":["vip","archived"],"payload":{"category":"cat-3"}}`,
		`{"tenant":3,"status":"closed","tags":["standard"],"payload":{"category":"cat-2"}}`,
		`{"tenant":4,"status":"open","tags":["vip"],"payload":{"category":"cat-1"}}`,
	})

	vip := Token{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "vip"}
	archived := Token{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "archived"}
	open := Token{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindString, Value: "open"}
	categoryCat1 := Token{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindString, Value: "cat-1"}
	tenantKey := Token{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "tenant"}

	chunks, err := chunked.Chunks(vip)
	require.NoError(t, err)
	require.Greater(t, len(chunks), 1)

	require.Equal(t, baseline.Lookup(vip), rowRefStrings(chunked.Lookup(vip)))
	require.Equal(t, baseline.Lookup(tenantKey), rowRefStrings(chunked.Lookup(tenantKey)))
	require.Equal(t, baseline.Lookup(categoryCat1), rowRefStrings(chunked.Lookup(categoryCat1)))
	require.Equal(t, baseline.Union(vip, archived), rowRefStrings(chunked.Union(vip, archived)))
	require.Equal(t, baseline.Intersect(vip, open), rowRefStrings(chunked.Intersect(vip, open)))
	require.Equal(t, baseline.Intersect(vip, open, categoryCat1), rowRefStrings(chunked.Intersect(vip, open, categoryCat1)))
}

func TestChunkedPostingStoreHandlesSkewedAndDuplicateTokens(t *testing.T) {
	baseline := NewPostingStore()
	chunked := NewChunkedPostingStore(4)
	common := Token{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "common"}
	rare := Token{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "rare"}

	for i := 0; i < 25; i++ {
		rowID := fmt.Sprintf("row/%04d", i)
		tokens := []Token{common, common}
		if i%7 == 0 {
			tokens = append(tokens, rare, rare)
		}
		require.NoError(t, baseline.Add(rowID, tokens))
		require.NoError(t, chunked.Add([]byte(rowID), tokens))
	}

	require.Equal(t, baseline.Lookup(common), rowRefStrings(chunked.Lookup(common)))
	require.Equal(t, baseline.Lookup(rare), rowRefStrings(chunked.Lookup(rare)))
	require.Equal(t, baseline.Intersect(common, rare), rowRefStrings(chunked.Intersect(common, rare)))

	chunks, err := chunked.Chunks(common)
	require.NoError(t, err)
	require.Greater(t, len(chunks), 1)
	for _, chunk := range chunks {
		require.LessOrEqual(t, int(chunk.RowCount), 4)
	}
}

func TestChunkedPostingStoreMutationsMatchPostingStore(t *testing.T) {
	baseline := NewPostingStore()
	chunked := NewChunkedPostingStore(2)
	oldTokens, err := Extract(mustJsonDocument(t, `{"tags":["old","vip"],"status":"draft"}`), indexmetadata.OpClassJsonbOps)
	require.NoError(t, err)
	newTokens, err := Extract(mustJsonDocument(t, `{"tags":["new","vip"],"status":"posted"}`), indexmetadata.OpClassJsonbOps)
	require.NoError(t, err)

	require.NoError(t, baseline.Add("row/1", oldTokens))
	require.NoError(t, chunked.Add([]byte("row/1"), oldTokens))
	require.NoError(t, baseline.Replace("row/1", oldTokens, newTokens))
	require.NoError(t, chunked.Replace([]byte("row/1"), oldTokens, newTokens))

	oldToken := Token{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "old"}
	newToken := Token{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "new"}
	require.Equal(t, baseline.Lookup(oldToken), rowRefStrings(chunked.Lookup(oldToken)))
	require.Equal(t, baseline.Lookup(newToken), rowRefStrings(chunked.Lookup(newToken)))

	require.NoError(t, baseline.Delete("row/1", newTokens))
	require.NoError(t, chunked.Delete([]byte("row/1"), newTokens))
	require.Empty(t, chunked.Lookup(newToken))
}

func TestChunkedPostingStoreChunkEntriesAreDeterministic(t *testing.T) {
	chunked := NewChunkedPostingStore(2)
	vip := Token{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "vip"}
	archived := Token{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "archived"}

	require.NoError(t, chunked.Add([]byte("row/0003"), []Token{vip, archived}))
	require.NoError(t, chunked.Add([]byte("row/0001"), []Token{vip}))
	require.NoError(t, chunked.Add([]byte("row/0002"), []Token{vip, archived}))

	entries, err := chunked.ChunkEntries()
	require.NoError(t, err)
	require.Len(t, entries, 3)
	require.Equal(t, entries[0].Token, entries[1].Token)
	require.Less(t, entries[1].Token, entries[2].Token)
	require.Equal(t, int64(0), entries[0].ChunkNo)
	require.Equal(t, int64(1), entries[1].ChunkNo)
	require.Equal(t, int64(0), entries[2].ChunkNo)
	require.Equal(t, []byte("row/0001"), entries[0].Chunk.FirstRowRef)
	require.Equal(t, []byte("row/0002"), entries[0].Chunk.LastRowRef)
	require.Equal(t, []byte("row/0003"), entries[1].Chunk.FirstRowRef)
}

func BenchmarkPostingChunkEncodeDecode(b *testing.B) {
	rowRefs := benchmarkChunkedRowRefs(512)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		chunk, err := EncodePostingChunk(rowRefs)
		if err != nil {
			b.Fatal(err)
		}
		decoded, err := DecodePostingChunk(chunk.Payload)
		if err != nil {
			b.Fatal(err)
		}
		if int(decoded.RowCount) != len(rowRefs) {
			b.Fatalf("expected %d row refs, got %d", len(rowRefs), decoded.RowCount)
		}
	}
}

func BenchmarkChunkedPostingStoreUnionIntersect(b *testing.B) {
	_, chunked := buildComparablePostingStores(b, 128, benchmarkChunkedDocuments(2048))
	vip := Token{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "vip"}
	archived := Token{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "archived"}
	open := Token{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindString, Value: "open"}

	b.ReportAllocs()
	b.Run("union", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows := chunked.Union(vip, archived)
			if len(rows) == 0 {
				b.Fatal("expected union rows")
			}
		}
	})
	b.Run("intersect", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows := chunked.Intersect(vip, open)
			if len(rows) == 0 {
				b.Fatal("expected intersect rows")
			}
		}
	})
}

func buildComparablePostingStores(tb testing.TB, rowsPerChunk int, documents []string) (*PostingStore, *ChunkedPostingStore) {
	tb.Helper()
	baseline := NewPostingStore()
	chunked := NewChunkedPostingStore(rowsPerChunk)
	for i, input := range documents {
		rowID := fmt.Sprintf("row/%04d", i)
		tokens, err := Extract(mustJsonDocument(tb, input), indexmetadata.OpClassJsonbOps)
		require.NoError(tb, err)
		require.NoError(tb, baseline.Add(rowID, tokens))
		require.NoError(tb, chunked.Add([]byte(rowID), tokens))
	}
	return baseline, chunked
}

func rowRefStrings(rowRefs [][]byte) []string {
	if len(rowRefs) == 0 {
		return nil
	}
	rows := make([]string, len(rowRefs))
	for i, rowRef := range rowRefs {
		rows[i] = string(rowRef)
	}
	return rows
}

func benchmarkChunkedRowRefs(count int) [][]byte {
	rowRefs := make([][]byte, count)
	for i := range rowRefs {
		rowRefs[i] = []byte(fmt.Sprintf("row/%08d", i))
	}
	return rowRefs
}

func benchmarkChunkedDocuments(count int) []string {
	docs := make([]string, count)
	for i := range docs {
		tag := "standard"
		if i%3 == 0 {
			tag = "vip"
		}
		archived := "active"
		if i%11 == 0 {
			archived = "archived"
		}
		status := "closed"
		if i%2 == 0 {
			status = "open"
		}
		docs[i] = fmt.Sprintf(`{"tenant":%d,"status":"%s","tags":["%s","%s"],"payload":{"category":"cat-%d"}}`, i%32, status, tag, archived, i%16)
	}
	return docs
}
