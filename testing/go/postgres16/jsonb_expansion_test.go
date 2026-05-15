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

package postgres16

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"
)

// TestJsonbExpansionWorkload pins the JSONB navigation operators and SRFs
// real PG views and reporting queries use to project nested JSON state
// into relational shapes. Per the View/query TODO in
// docs/app-compatibility-checklist.md.
func TestJsonbExpansionWorkload(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "-> and ->> against object keys",
			SetUpScript: []string{
				`CREATE TABLE events (id INT PRIMARY KEY, payload JSONB);`,
				`INSERT INTO events VALUES
					(1, '{"user": {"id": 100, "name": "Alice"}, "kind": "click", "ts": 1234}'),
					(2, '{"user": {"id": 200, "name": "Bob"},   "kind": "view",  "ts": 1235}');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// ->> returns text; -> returns jsonb. Top-level keys.
					Query: `SELECT id, payload->>'kind' AS kind, payload->>'ts' AS ts
						FROM events
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "jsonb-expansion-test-testjsonbexpansionworkload-0001-select-id-payload->>-kind-as"},
				},
				{
					// Chained -> followed by ->> for nested object access.
					Query: `SELECT id,
						payload->'user'->>'name' AS user_name,
						payload->'user'->>'id'   AS user_id
						FROM events
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "jsonb-expansion-test-testjsonbexpansionworkload-0002-select-id-payload->-user->>"},
				},
			},
		},
		{
			Name: "#> and #>> path operators",
			SetUpScript: []string{
				`CREATE TABLE docs (id INT PRIMARY KEY, doc JSONB);`,
				`INSERT INTO docs VALUES
					(1, '{"a": {"b": {"c": "deep"}}, "list": [10, 20, 30]}');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// #>> returns text along a key path.
					Query: `SELECT doc#>>'{a,b,c}' AS deep FROM docs WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "jsonb-expansion-test-testjsonbexpansionworkload-0003-select-doc#>>-{a-b-c}"},
				},
				{
					// Indexed array access via path.
					Query: `SELECT doc#>>'{list,1}' AS second FROM docs WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "jsonb-expansion-test-testjsonbexpansionworkload-0004-select-doc#>>-{list-1}-as"},
				},
			},
		},
		{
			Name: "jsonb_array_elements expands array values",
			SetUpScript: []string{
				`CREATE TABLE bags (id INT PRIMARY KEY, items JSONB);`,
				`INSERT INTO bags VALUES
					(1, '[1, 2, 3]'),
					(2, '[10, 20]');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*)::text FROM (SELECT jsonb_array_elements(items) FROM bags) t;`, PostgresOracle: ScriptTestPostgresOracle{ID: "jsonb-expansion-test-testjsonbexpansionworkload-0005-select-count-*-::text-from"},
				},
			},
		},
		{
			Name:        "jsonb_object_keys lists object keys",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT k FROM jsonb_object_keys('{"a": 1, "b": 2, "c": 3}'::jsonb) k
						ORDER BY k;`, PostgresOracle: ScriptTestPostgresOracle{ID: "jsonb-expansion-test-testjsonbexpansionworkload-0006-select-k-from-jsonb_object_keys-{"},
				},
			},
		},
	})
}
