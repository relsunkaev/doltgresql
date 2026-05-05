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
	"strings"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/doltgresql/server/indexmetadata"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func TestExtractJsonbOpsTokens(t *testing.T) {
	doc := mustJsonDocument(t, `{
		"n": null,
		"s": "text",
		"b": 2,
		"z": [],
		"a": {
			"flag": true,
			"empty": {},
			"tags": ["vip", "vip", null, false, 3, ""]
		}
	}`)

	tokens, err := Extract(doc, indexmetadata.OpClassJsonbOps)
	require.NoError(t, err)
	require.Equal(t, []Token{
		{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindBoolean, Value: "false"},
		{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindBoolean, Value: "true"},
		{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindEmptyArray},
		{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindEmptyObject},
		{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: ""},
		{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "a"},
		{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "b"},
		{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "empty"},
		{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "flag"},
		{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "n"},
		{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "s"},
		{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "tags"},
		{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "vip"},
		{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "z"},
		{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindNull, Value: "null"},
		{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindNumber, Value: "2"},
		{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindNumber, Value: "3"},
		{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindString, Value: "text"},
	}, tokens)
}

func TestExtractJsonbOpsDeduplicatesCanonicalJsonbTokens(t *testing.T) {
	value := pgtypes.JsonObjectFromItems([]pgtypes.JsonValueObjectItem{
		{Key: "dupe", Value: pgtypes.JsonValueNumber(decimal.NewFromInt(1))},
		{Key: "dupe", Value: pgtypes.JsonValueNumber(decimal.NewFromInt(2))},
		{
			Key: "arr",
			Value: pgtypes.JsonValueArray{
				pgtypes.JsonValueString("same"),
				pgtypes.JsonValueString("same"),
			},
		},
	}, true)

	tokens, err := ExtractValue(value, indexmetadata.OpClassJsonbOps)
	require.NoError(t, err)
	require.Equal(t, []Token{
		{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "arr"},
		{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "dupe"},
		{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "same"},
		{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindNumber, Value: "2"},
	}, tokens)
}

func TestExtractJsonbPathOpsTokens(t *testing.T) {
	doc := mustJsonDocument(t, `{
		"foo": {"bar": "baz"},
		"arr": [{"bar": "baz"}],
		"tags": ["vip"],
		"empty": {},
		"bool": true,
		"null": null,
		"n": 7
	}`)

	tokens, err := Extract(doc, indexmetadata.OpClassJsonbPathOps)
	require.NoError(t, err)
	require.Equal(t, []Token{
		{OpClass: indexmetadata.OpClassJsonbPathOps, Kind: TokenKindPathValue, Path: []string{"arr", "bar"}, Value: "string:baz"},
		{OpClass: indexmetadata.OpClassJsonbPathOps, Kind: TokenKindPathValue, Path: []string{"bool"}, Value: "boolean:true"},
		{OpClass: indexmetadata.OpClassJsonbPathOps, Kind: TokenKindPathValue, Path: []string{"foo", "bar"}, Value: "string:baz"},
		{OpClass: indexmetadata.OpClassJsonbPathOps, Kind: TokenKindPathValue, Path: []string{"n"}, Value: "number:7"},
		{OpClass: indexmetadata.OpClassJsonbPathOps, Kind: TokenKindPathValue, Path: []string{"null"}, Value: "null:null"},
		{OpClass: indexmetadata.OpClassJsonbPathOps, Kind: TokenKindPathValue, Path: []string{"tags"}, Value: "string:vip"},
	}, tokens)
}

func TestExtractJsonbPathOpsDiffersFromJsonbOps(t *testing.T) {
	doc := mustJsonDocument(t, `{"foo": {"bar": "baz"}, "tags": ["vip"], "empty": {}}`)

	opsTokens, err := Extract(doc, indexmetadata.OpClassJsonbOps)
	require.NoError(t, err)
	require.Contains(t, opsTokens, Token{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "foo"})
	require.Contains(t, opsTokens, Token{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindKey, Value: "vip"})
	require.Contains(t, opsTokens, Token{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindEmptyObject})

	pathOpsTokens, err := Extract(doc, indexmetadata.OpClassJsonbPathOps)
	require.NoError(t, err)
	require.NotContains(t, pathOpsTokens, Token{OpClass: indexmetadata.OpClassJsonbPathOps, Kind: TokenKindKey, Value: "foo"})
	require.NotContains(t, pathOpsTokens, Token{OpClass: indexmetadata.OpClassJsonbPathOps, Kind: TokenKindKey, Value: "vip"})
	require.NotContains(t, pathOpsTokens, Token{OpClass: indexmetadata.OpClassJsonbPathOps, Kind: TokenKindEmptyObject})
	require.Contains(t, pathOpsTokens, Token{OpClass: indexmetadata.OpClassJsonbPathOps, Kind: TokenKindPathValue, Path: []string{"foo", "bar"}, Value: "string:baz"})
}

func TestExtractDeterministicOrder(t *testing.T) {
	left := mustJsonDocument(t, `{"bbb": 1, "a": [{"z": true}, {"z": true}], "empty": {}}`)
	right := mustJsonDocument(t, `{"empty": {}, "a": [{"z": true}, {"z": true}], "bbb": 1}`)

	for _, opClass := range []string{indexmetadata.OpClassJsonbOps, indexmetadata.OpClassJsonbPathOps} {
		leftTokens, err := Extract(left, opClass)
		require.NoError(t, err)
		rightTokens, err := Extract(right, opClass)
		require.NoError(t, err)
		require.Equal(t, leftTokens, rightTokens, opClass)
	}
}

func TestExtractRejectsUnsupportedOpClass(t *testing.T) {
	doc := mustJsonDocument(t, `{"a": 1}`)
	_, err := Extract(doc, "text_ops")
	require.Error(t, err)
}

func TestExtractEmptyContainers(t *testing.T) {
	for _, test := range []struct {
		name    string
		input   string
		opClass string
		want    []Token
	}{
		{
			name:    "jsonb_ops_empty_object",
			input:   `{}`,
			opClass: indexmetadata.OpClassJsonbOps,
			want: []Token{
				{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindEmptyObject},
			},
		},
		{
			name:    "jsonb_ops_empty_array",
			input:   `[]`,
			opClass: indexmetadata.OpClassJsonbOps,
			want: []Token{
				{OpClass: indexmetadata.OpClassJsonbOps, Kind: TokenKindEmptyArray},
			},
		},
		{
			name:    "jsonb_path_ops_empty_containers",
			input:   `{"a": {}, "b": []}`,
			opClass: indexmetadata.OpClassJsonbPathOps,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			tokens, err := Extract(mustJsonDocument(t, test.input), test.opClass)
			require.NoError(t, err)
			require.Equal(t, test.want, tokens)
		})
	}
}

func BenchmarkExtractLargeDocument(b *testing.B) {
	doc := mustJsonDocument(b, largeJsonDocument())

	for _, opClass := range []string{indexmetadata.OpClassJsonbOps, indexmetadata.OpClassJsonbPathOps} {
		b.Run(opClass, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				tokens, err := Extract(doc, opClass)
				if err != nil {
					b.Fatal(err)
				}
				if len(tokens) == 0 {
					b.Fatal("expected extracted tokens")
				}
			}
		})
	}
}

func mustJsonDocument(tb testing.TB, input string) pgtypes.JsonDocument {
	tb.Helper()
	doc, err := pgtypes.UnmarshalToJsonDocument([]byte(input))
	require.NoError(tb, err)
	return doc
}

func largeJsonDocument() string {
	var sb strings.Builder
	sb.WriteString(`{"accounts":[`)
	for i := 0; i < 100; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `{"id":%d,"name":"account-%d","active":%t,"tags":["vip","region-%d"],"metadata":{"score":%d,"empty":{}}}`,
			i, i, i%2 == 0, i%10, i*7)
	}
	sb.WriteString(`],"summary":{"count":100,"empty":[]}}`)
	return sb.String()
}
