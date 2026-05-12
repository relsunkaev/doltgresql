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
	"reflect"
	"testing"
)

func TestSimpleTSPhraseQuery(t *testing.T) {
	if got, want := simpleTSPhraseQuery("fat cats"), "'fat' <-> 'cats'"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestSimpleTSHeadline(t *testing.T) {
	got := simpleTSHeadline("fat cats ate rats", "'cats'")
	if want := "fat <b>cats</b> ate rats"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestTSVectorLexemes(t *testing.T) {
	got := tsVectorLexemes("'cats':2 'fat':1 'cats':3")
	want := []string{"cats", "fat"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestSimpleTSStrip(t *testing.T) {
	if got, want := simpleTSStrip("'cats':2 'fat':1"), "'cats' 'fat'"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestSimpleTSDelete(t *testing.T) {
	if got, want := simpleTSDelete("'cats':2 'fat':1", "cats"), "'fat':1"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestSimpleTSSetWeight(t *testing.T) {
	if got, want := simpleTSSetWeight("'cats':2 'fat':1", "A"), "'cats':2A 'fat':1A"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestSimpleTSNumNode(t *testing.T) {
	if got, want := simpleTSNumNode("'fat' & 'cats'"), 3; got != want {
		t.Fatalf("got %d, want %d", got, want)
	}
}

func TestSimpleTSMatches(t *testing.T) {
	if !simpleTSMatches("'cats':2 'fat':1", "'cats'") {
		t.Fatal("expected query term to match vector lexeme")
	}
	if simpleTSMatches("'cats':2 'fat':1", "'rats'") {
		t.Fatal("expected missing query term not to match")
	}
}
