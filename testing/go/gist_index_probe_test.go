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

package _go

import (
	"testing"
)

// TestGistIndexProbe pins the PostgreSQL behavior for GiST index DDL.
// Per the Index/planner TODO in docs/app-compatibility-checklist.md.
func TestGistIndexProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE INDEX USING gist follows PostgreSQL",
			SetUpScript: []string{
				`CREATE TABLE shapes (id INT PRIMARY KEY, geom TEXT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE INDEX shapes_geom_gist_idx ON shapes USING gist (geom);`,

					PostgresOracle: ScriptTestPostgresOracle{ID: "gist-index-probe-test-testgistindexprobe-0001-create-index-shapes_geom_gist_idx-on-shapes"},
				},
			},
		},
	})
}
