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

package server

import "testing"

func TestRewriteXmlConstructors(t *testing.T) {
	got, ok := rewriteXmlConstructors(`SELECT xmlelement(name foo)::text;`)
	if !ok {
		t.Fatal("expected rewrite")
	}
	if want := `SELECT xmlelement('foo')::text;`; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}

	got, ok = rewriteXmlConstructors(`SELECT xmlforest('abc' AS foo, 123 AS bar)::text;`)
	if !ok {
		t.Fatal("expected rewrite")
	}
	if want := `SELECT xmlforest('foo', 'abc', 'bar', 123)::text;`; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestContainsSecurityLabel(t *testing.T) {
	if !containsSecurityLabel(`SECURITY LABEL ON TABLE t IS 'x';`) {
		t.Fatal("expected SECURITY LABEL to be recognized")
	}
	if containsSecurityLabel(`SELECT 'security label';`) {
		t.Fatal("did not expect ordinary SELECT to match")
	}
}

func TestRewriteDMLReturningTableOID(t *testing.T) {
	got, ok := rewriteDMLReturningTableOID(`INSERT INTO returning_tableoid_items VALUES (1, 10) RETURNING tableoid::regclass::text, id;`)
	if !ok {
		t.Fatal("expected rewrite")
	}
	if want := `INSERT INTO returning_tableoid_items VALUES (1, 10) RETURNING 'returning_tableoid_items'::regclass::text, id;`; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
