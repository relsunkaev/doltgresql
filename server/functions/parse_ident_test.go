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

package functions

import "testing"

func TestParseIdent(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		strict bool
		want   []any
	}{
		{
			name:   "folds unquoted identifiers",
			input:  "Schemax.Tabley",
			strict: true,
			want:   []any{"schemax", "tabley"},
		},
		{
			name:   "preserves quoted identifiers",
			input:  `"SchemaX"."TableY"`,
			strict: true,
			want:   []any{"SchemaX", "TableY"},
		},
		{
			name:   "unescapes embedded quotes",
			input:  `"has""quote".child`,
			strict: true,
			want:   []any{`has"quote`, "child"},
		},
		{
			name:   "ignores trailing text when not strict",
			input:  "foo.boo[]",
			strict: false,
			want:   []any{"foo", "boo"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseIdent(tt.input, tt.strict)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %#v, want %#v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("got %#v, want %#v", got, tt.want)
				}
			}
		})
	}
}

func TestParseIdentErrors(t *testing.T) {
	for _, input := range []string{"", ".", "foo.", `"unterminated`, `""`} {
		t.Run(input, func(t *testing.T) {
			if _, err := parseIdent(input, true); err == nil {
				t.Fatal("expected parse_ident error")
			}
		})
	}
}
