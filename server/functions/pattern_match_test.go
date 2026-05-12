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

	"github.com/dolthub/go-mysql-server/sql"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func TestDoltgresILike(t *testing.T) {
	ctx := sql.NewEmptyContext()
	params := [3]*pgtypes.DoltgresType{}
	tests := []struct {
		value   string
		pattern string
		want    bool
	}{
		{"Alpha", "a%", true},
		{"Alpha", "ALP_A", true},
		{"Alpha", "b%", false},
		{"100%", `100\%`, true},
	}
	for _, tt := range tests {
		got, err := doltgres_ilike_text_text.Callable(ctx, params, tt.value, tt.pattern)
		if err != nil {
			t.Fatal(err)
		}
		if got != tt.want {
			t.Fatalf("%q ILIKE %q: got %v, want %v", tt.value, tt.pattern, got, tt.want)
		}
	}
}

func TestDoltgresSimilarTo(t *testing.T) {
	ctx := sql.NewEmptyContext()
	params := [3]*pgtypes.DoltgresType{}
	tests := []struct {
		value   string
		pattern string
		want    bool
	}{
		{"abc", "a%(b|c)", true},
		{"adc", "a%(b|c)", true},
		{"abx", "a%(b|c)", false},
	}
	for _, tt := range tests {
		got, err := doltgres_similar_to_text_text.Callable(ctx, params, tt.value, tt.pattern)
		if err != nil {
			t.Fatal(err)
		}
		if got != tt.want {
			t.Fatalf("%q SIMILAR TO %q: got %v, want %v", tt.value, tt.pattern, got, tt.want)
		}
	}
}

func TestDoltgresRegexMatchCI(t *testing.T) {
	ctx := sql.NewEmptyContext()
	params := [3]*pgtypes.DoltgresType{}
	got, err := doltgres_regex_match_ci_text_text.Callable(ctx, params, "Alpha", "^a")
	if err != nil {
		t.Fatal(err)
	}
	if got != true {
		t.Fatalf("got %v, want true", got)
	}
}
