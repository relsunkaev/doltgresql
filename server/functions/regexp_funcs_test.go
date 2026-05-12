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

func TestCompilePGRegexSupportedFlags(t *testing.T) {
	re, _, err := compilePGRegex("a b", "x")
	if err != nil {
		t.Fatal(err)
	}
	if got := re.FindString("ab"); got != "ab" {
		t.Fatalf("got %q, want ab", got)
	}

	re, _, err = compilePGRegex("^b", "n")
	if err != nil {
		t.Fatal(err)
	}
	if got := re.FindString("a\nb"); got != "b" {
		t.Fatalf("got %q, want b", got)
	}

	if _, _, err = compilePGRegex("a", "q"); err == nil {
		t.Fatal("expected unsupported flag to error")
	}
}

func TestRegexpReplace(t *testing.T) {
	re, _, err := compilePGRegex("b..", "")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := regexpReplace("foobarbaz", re, "X", false), "fooXbaz"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if got, want := regexpReplace("foobarbaz", re, "X", true), "fooXX"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}

	re, _, err = compilePGRegex(`(\w+)`, "")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := regexpReplace("abc", re, `[\1]`, false), "[abc]"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestRegexpSplitArray(t *testing.T) {
	re, _, err := compilePGRegex(",", "")
	if err != nil {
		t.Fatal(err)
	}
	got := regexpSplitArray("a,b,c", re)
	want := []any{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got %#v, want %#v", got, want)
		}
	}
}

func TestRegexpCountCallable(t *testing.T) {
	got, err := regexp_count_text_text.Callable(sql.NewEmptyContext(), [3]*pgtypes.DoltgresType{}, "abcabc", "a")
	if err != nil {
		t.Fatal(err)
	}
	if got != int32(2) {
		t.Fatalf("got %#v, want 2", got)
	}
}
