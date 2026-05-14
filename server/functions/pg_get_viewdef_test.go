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

func TestPgGetViewdefKeepsVisibleRelationNamesUnqualified(t *testing.T) {
	got, err := pgGetViewdefDefinition("CREATE VIEW test_view AS SELECT name FROM test", "public")
	if err != nil {
		t.Fatal(err)
	}
	got = ensureTrailingSemicolon(formatPgGetViewdefDefinition(got))
	want := " SELECT name\n   FROM test;"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestSchemaQualifiedViewDefinitionStillBindsDefaultSchema(t *testing.T) {
	got, err := SchemaQualifiedViewDefinition("CREATE VIEW test_view AS SELECT name FROM test", "public")
	if err != nil {
		t.Fatal(err)
	}
	want := "SELECT name FROM public.test"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
