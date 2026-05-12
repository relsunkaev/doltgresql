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

import (
	"testing"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func TestSubstringSimilarPattern(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		pattern string
		escape  string
		want    any
		wantErr bool
	}{
		{
			name:    "full match without capture markers",
			input:   "hello.",
			pattern: "hello#.",
			escape:  "#",
			want:    "hello.",
		},
		{
			name:    "capture marked section",
			input:   "Thomas",
			pattern: `%#"o_a#"_`,
			escape:  "#",
			want:    "oma",
		},
		{
			name:    "no match returns null",
			input:   "Thomas",
			pattern: `x#"o_a#"_`,
			escape:  "#",
			want:    nil,
		},
		{
			name:    "invalid escape",
			input:   "hello",
			pattern: "hello",
			escape:  "##",
			wantErr: true,
		},
		{
			name:    "unbalanced capture marker",
			input:   "hello",
			pattern: `#"hello`,
			escape:  "#",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := substring_text_text_text.Callable(nil, [4]*pgtypes.DoltgresType{}, tt.input, tt.pattern, tt.escape)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}
