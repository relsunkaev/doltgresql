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

// TestPgMatviewsProbe pins the empty pg_matviews catalog surface dump
// tools query before a schema creates any materialized views. Populated
// pg_matviews rows are covered by TestMaterializedViewProbe.
func TestPgMatviewsProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_matviews exists and returns zero rows when no matviews are defined",
			SetUpScript: []string{
				`CREATE SCHEMA matviews_probe_schema;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*)::text FROM pg_matviews WHERE schemaname = 'matviews_probe_schema';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-matviews-probe-test-testpgmatviewsprobe-0001-select-count-*-::text-from"},
				},
				{
					// dump tools issue this exact shape to discover
					// matviews: column-name selection plus a
					// schema filter. Must not blow up.
					Query: `SELECT schemaname, matviewname, matviewowner,
							tablespace, hasindexes, ispopulated, definition
						FROM pg_matviews
						WHERE schemaname = 'matviews_probe_schema'
						ORDER BY schemaname, matviewname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-matviews-probe-test-testpgmatviewsprobe-0002-select-schemaname-matviewname-matviewowner-tablespace"},
				},
			},
		},
	})
}
