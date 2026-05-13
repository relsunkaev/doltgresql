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

// TestCreateTablespaceInPlaceCatalogRepro reproduces a tablespace DDL/catalog
// gap: PostgreSQL can create an in-place tablespace when the developer GUC is
// enabled, then exposes it through pg_tablespace.
func TestCreateTablespaceInPlaceCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLESPACE records pg_tablespace row",
			SetUpScript: []string{
				`SET allow_in_place_tablespaces = true;`,
				`CREATE TABLESPACE repro_tblspace LOCATION '';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT spcname
						FROM pg_catalog.pg_tablespace
						WHERE spcname = 'repro_tblspace';`, PostgresOracle: ScriptTestPostgresOracle{ID: "tablespace-correctness-repro-test-testcreatetablespaceinplacecatalogrepro-0001-select-spcname-from-pg_catalog.pg_tablespace-where"},
				},
			},
		},
	})
}
