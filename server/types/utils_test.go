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

import "testing"

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
