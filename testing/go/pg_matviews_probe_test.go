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

	"github.com/dolthub/go-mysql-server/sql"
)

// TestPgMatviewsProbe pins the empty pg_matviews catalog surface dump
// tools query before a schema creates any materialized views. Populated
// pg_matviews rows are covered by TestMaterializedViewProbe.
func TestPgMatviewsProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "pg_matviews exists and returns zero rows when no matviews are defined",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT count(*)::text FROM pg_matviews;`,
					Expected: []sql.Row{{"0"}},
				},
				{
					// dump tools issue this exact shape to discover
					// matviews: column-name selection plus a
					// schema filter. Must not blow up.
					Query: `SELECT schemaname, matviewname, matviewowner,
							tablespace, hasindexes, ispopulated, definition
						FROM pg_matviews
						WHERE schemaname = 'public'
						ORDER BY schemaname, matviewname;`,
					ExpectedColNames: []string{
						"schemaname",
						"matviewname",
						"matviewowner",
						"tablespace",
						"hasindexes",
						"ispopulated",
						"definition",
					},
					Expected: []sql.Row{},
				},
			},
		},
	})
}
