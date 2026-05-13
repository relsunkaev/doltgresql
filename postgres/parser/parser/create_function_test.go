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

package parser

import "testing"

func TestParseRoutineArgNameBeforeOutMode(t *testing.T) {
	statements, err := Parse(`CREATE FUNCTION pgp_armor_headers(text, key OUT text, value OUT text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pgp_armor_headers'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;`)
	if err != nil {
		t.Fatal(err)
	}
	if len(statements) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(statements))
	}
}

func TestParseCopyDefaultOption(t *testing.T) {
	statements, err := Parse(`COPY t (a, b) FROM STDIN WITH (FORMAT csv, DEFAULT 'DEFAULT');`)
	if err != nil {
		t.Fatal(err)
	}
	if len(statements) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(statements))
	}
}

func TestParseVacuumAnalyzeRecentOptions(t *testing.T) {
	queries := []string{
		`VACUUM (BUFFER_USAGE_LIMIT '128 kB') t;`,
		`ANALYZE (BUFFER_USAGE_LIMIT '128 kB') t;`,
		`VACUUM ONLY t;`,
		`ANALYZE ONLY t;`,
	}
	for _, query := range queries {
		statements, err := Parse(query)
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		if len(statements) != 1 {
			t.Fatalf("%s: expected 1 statement, got %d", query, len(statements))
		}
	}
}

func TestParsePointTypeCastExpression(t *testing.T) {
	queries := []string{
		`SELECT ('(1,2)'::point + '(3,4)'::point)::text;`,
		`CREATE TABLE point_items (id INT, p point);`,
		`SELECT NULL::geometry(POINT);`,
	}
	for _, query := range queries {
		statements, err := Parse(query)
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		if len(statements) != 1 {
			t.Fatalf("%s: expected 1 statement, got %d", query, len(statements))
		}
	}
}
