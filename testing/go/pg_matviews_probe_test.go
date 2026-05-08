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

// TestPgMatviewsProbe pins the pg_matviews catalog surface dump tools
// query during matview repair. Materialized views themselves are not
// yet supported (separate item in the Schema/DDL TODO), so the
// expected shape today is "the catalog view exists and returns zero
// rows" — that's what dump tools need to skip the matview repair
// branch cleanly. Per the Dump/admin/tooling TODO in
// docs/app-compatibility-checklist.md.
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
