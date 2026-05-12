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
