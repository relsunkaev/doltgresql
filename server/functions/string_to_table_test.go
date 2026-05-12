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

func TestStringToTableRows(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		delimiter  any
		nullString any
		want       []any
	}{
		{
			name:      "splits on delimiter",
			input:     "1|2|3",
			delimiter: "|",
			want:      []any{"1", "2", "3"},
		},
		{
			name:      "empty delimiter returns whole string",
			input:     "abc",
			delimiter: "",
			want:      []any{"abc"},
		},
		{
			name:      "nil delimiter splits characters",
			input:     "abc",
			delimiter: nil,
			want:      []any{"a", "b", "c"},
		},
		{
			name:       "null string maps fields to sql null",
			input:      "a,,c",
			delimiter:  ",",
			nullString: "",
			want:       []any{"a", nil, "c"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stringToTableRows(tt.input, tt.delimiter, tt.nullString)
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
