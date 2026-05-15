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

// TestSearchPathResolvesViewsInLaterSchemasRepro reproduces a query
// correctness bug: PostgreSQL resolves unqualified view names through every
// schema in search_path, but Doltgres misses views in later schemas.
func TestSearchPathResolvesViewsInLaterSchemasRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "search_path resolves views in later schemas",
			SetUpScript: []string{
				`CREATE SCHEMA first_schema;`,
				`CREATE SCHEMA second_schema;`,
				`CREATE TABLE second_schema.search_path_source (id INT PRIMARY KEY, v INT);`,
				`INSERT INTO second_schema.search_path_source VALUES (1, 4), (2, 5);`,
				`CREATE VIEW second_schema.search_path_view AS
					SELECT id, v FROM second_schema.search_path_source;`,
				`SET search_path = first_schema, second_schema;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT v FROM search_path_view ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "view-search-path-repro-test-testsearchpathresolvesviewsinlaterschemasrepro-0001-select-v-from-search_path_view-order"},
				},
			},
		},
	})
}
