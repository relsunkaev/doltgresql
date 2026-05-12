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

func TestRewritePostgres16IntegerLiterals(t *testing.T) {
	tests := []struct {
		query string
		want  string
		ok    bool
	}{
		{`SELECT 1_000::text;`, `SELECT 1000::text;`, true},
		{`SELECT 0x10::text, 0o10::text, 0b1010::text;`, `SELECT 16::text, 8::text, 10::text;`, true},
		{`SELECT '0x10', 1;`, ``, false},
		{`SELECT col1_000 FROM t;`, ``, false},
	}
	for _, tt := range tests {
		got, ok := rewritePostgres16IntegerLiterals(tt.query)
		if ok != tt.ok {
			t.Fatalf("%q: got ok %v, want %v", tt.query, ok, tt.ok)
		}
		if ok && got != tt.want {
			t.Fatalf("%q: got %q, want %q", tt.query, got, tt.want)
		}
	}
}

func TestRewriteTemporalOverlaps(t *testing.T) {
	got, ok := rewriteTemporalOverlaps(`SELECT (DATE '2024-01-01', DATE '2024-01-10') OVERLAPS (DATE '2024-01-05', DATE '2024-01-20');`)
	if !ok {
		t.Fatal("expected rewrite")
	}
	if want := `SELECT __doltgres_overlaps(DATE '2024-01-01', DATE '2024-01-10', DATE '2024-01-05', DATE '2024-01-20');`; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestRewritePostgres16BuiltinSyntax(t *testing.T) {
	got, ok := rewritePostgres16BuiltinSyntax(`SELECT (pg_input_error_info('42000000000', 'integer')).sql_error_code, system_user IS NOT NULL, any_value(v) FROM t;`)
	if !ok {
		t.Fatal("expected rewrite")
	}
	want := `SELECT pg_input_error_info_sql_error_code('42000000000', 'integer'), system_user() IS NOT NULL, min(v) FROM t;`
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
