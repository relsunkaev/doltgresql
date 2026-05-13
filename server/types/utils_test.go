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

package types

import (
	"reflect"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestQuoteStringEscapesArraySpecialCharacters(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "plain value",
			in:   "plain",
			want: "plain",
		},
		{
			name: "backslash",
			in:   `\x6869`,
			want: `"\\x6869"`,
		},
		{
			name: "quote",
			in:   `a"b`,
			want: `"a\"b"`,
		},
		{
			name: "backslash and quote",
			in:   `a\b"c`,
			want: `"a\\b\"c"`,
		},
		{
			name: "null spelling",
			in:   "NULL",
			want: `"NULL"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := quoteString(tt.in); got != tt.want {
				t.Fatalf("quoteString(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestArrayDimensionsTreatArrayValuesAsNestedArrays(t *testing.T) {
	got := arrayStringDimensions([]any{
		ArrayValue{Elements: []any{int32(1), int32(2)}},
		[]any{int32(3), int32(4)},
	})
	want := []int32{2, 2}
	if len(got) != len(want) {
		t.Fatalf("arrayStringDimensions() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("arrayStringDimensions() = %v, want %v", got, want)
		}
	}
}

func TestSerializeArrayRoundTripsNestedArrayValues(t *testing.T) {
	ctx := sql.NewEmptyContext()
	data, err := serializeArray(ctx, []any{
		ArrayValue{Elements: []any{int32(1), int32(2)}},
		[]any{int32(3), int32(4)},
	}, Int32)
	if err != nil {
		t.Fatal(err)
	}
	got, err := deserializeArray(ctx, data, Int32)
	if err != nil {
		t.Fatal(err)
	}
	want := []any{
		[]any{int32(1), int32(2)},
		[]any{int32(3), int32(4)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("deserializeArray() = %#v, want %#v", got, want)
	}
}
