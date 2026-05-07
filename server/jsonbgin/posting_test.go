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

func TestEncodeTokenRoundTrip(t *testing.T) {
	token := Token{
		OpClass: indexmetadata.OpClassJsonbPathOps,
		Kind:    TokenKindPathValue,
		Path:    []string{"odd:key", "line\nbreak", "nul\x00byte"},
		Value:   "string:value:with:separators",
	}

	encoded := EncodeToken(token)
	decoded, err := DecodeToken(encoded)
	require.NoError(t, err)
	require.Equal(t, token, decoded)
	require.Equal(t, encoded, EncodeToken(decoded))
}

func TestDecodeTokenRejectsMalformedStorageKeys(t *testing.T) {
	for _, encoded := range []string{
		"",
		"3:key",
		"4:json",
		"999:short",
		"1:a1:b1:c",
	} {
		t.Run(encoded, func(t *testing.T) {
			_, err := DecodeToken(encoded)
			require.Error(t, err)
		})
	}
}

func TestPostingChunkTableName(t *testing.T) {
	require.Equal(t,
		"dg_gin_jsonb_gin_backfill_jsonb_gin_backfill_idx_posting_chunks",
		PostingChunkTableName("jsonb_gin_backfill", "jsonb_gin_backfill_idx"))
}

func TestPostingStoreMutations(t *testing.T) {
	store := NewPostingStore()
	doc := mustJsonDocument(t, `{"a": {"b": "x"}, "tags": ["vip", "vip"]}`)
	tokens, err := Extract(doc, indexmetadata.OpClassJsonbOps)
	require.NoError(t, err)

	require.NoError(t, store.Add("row/2", tokens))
	require.NoError(t, store.Add("row/1", tokens))
	require.NoError(t, store.Add("row/1", tokens))

	require.Equal(t, []string{"row/1", "row/2"}, store.Lookup(Token{
		OpClass: indexmetadata.OpClassJsonbOps,
		Kind:    TokenKindKey,
		Value:   "vip",
	}))
	require.Equal(t, []string{"row/1", "row/2"}, store.Lookup(Token{
		OpClass: indexmetadata.OpClassJsonbOps,
		Kind:    TokenKindString,
		Value:   "x",
	}))

	require.NoError(t, store.Delete("row/1", tokens))
	require.Equal(t, []string{"row/2"}, store.Lookup(Token{
		OpClass: indexmetadata.OpClassJsonbOps,
		Kind:    TokenKindKey,
		Value:   "vip",
	}))
}

func TestPostingStoreReplace(t *testing.T) {
	store := NewPostingStore()
	oldTokens, err := Extract(mustJsonDocument(t, `{"tags": ["old"], "status": "draft"}`), indexmetadata.OpClassJsonbOps)
	require.NoError(t, err)
	newTokens, err := Extract(mustJsonDocument(t, `{"tags": ["new"], "status": "posted"}`), indexmetadata.OpClassJsonbOps)
	require.NoError(t, err)

	require.NoError(t, store.Add("row/1", oldTokens))
	require.NoError(t, store.Replace("row/1", oldTokens, newTokens))

	require.Empty(t, store.Lookup(Token{
		OpClass: indexmetadata.OpClassJsonbOps,
		Kind:    TokenKindKey,
		Value:   "old",
	}))
	require.Equal(t, []string{"row/1"}, store.Lookup(Token{
		OpClass: indexmetadata.OpClassJsonbOps,
		Kind:    TokenKindKey,
		Value:   "new",
	}))
	require.Equal(t, []string{"row/1"}, store.Lookup(Token{
		OpClass: indexmetadata.OpClassJsonbOps,
		Kind:    TokenKindString,
		Value:   "posted",
	}))
}

func TestPostingStoreUnionAndIntersection(t *testing.T) {
	store := NewPostingStore()
	for rowID, input := range map[string]string{
		"row/1": `{"status": "posted", "tags": ["vip", "west"]}`,
		"row/2": `{"status": "draft", "tags": ["vip"]}`,
		"row/3": `{"status": "posted", "tags": ["east"]}`,
	} {
		tokens, err := Extract(mustJsonDocument(t, input), indexmetadata.OpClassJsonbOps)
		require.NoError(t, err)
		require.NoError(t, store.Add(rowID, tokens))
	}

	vip := Token{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "vip"}
	posted := Token{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindString, Value: "posted"}
	draft := Token{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindString, Value: "draft"}

	require.Equal(t, []string{"row/1", "row/2", "row/3"}, store.Union(vip, posted))
	require.Equal(t, []string{"row/1"}, store.Intersect(vip, posted))
	require.Equal(t, []string{"row/2"}, store.Intersect(vip, draft))
	require.Empty(t, store.Intersect(posted, draft))
}

func TestPostingStoreRejectsEmptyRowID(t *testing.T) {
	store := NewPostingStore()
	err := store.Add("", []Token{{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "a"}})
	require.Error(t, err)
}

func BenchmarkPostingStoreLookup(b *testing.B) {
	store := NewPostingStore()
	for i := 0; i < 1000; i++ {
		doc := mustJsonDocument(b, fmt.Sprintf(`{"id": %d, "tags": ["vip", "region-%d"], "status": "posted"}`, i, i%10))
		tokens, err := Extract(doc, indexmetadata.OpClassJsonbOps)
		require.NoError(b, err)
		require.NoError(b, store.Add(fmt.Sprintf("row/%04d", i), tokens))
	}

	token := Token{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "vip"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rows := store.Lookup(token)
		if len(rows) != 1000 {
			b.Fatalf("expected 1000 rows, got %d", len(rows))
		}
	}
}
