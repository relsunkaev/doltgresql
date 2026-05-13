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

// TestAlterMaterializedViewRenamePreservesSelectPrivilegeRepro reproduces an
// ACL persistence bug: ALTER TABLE RENAME on a materialized view leaves the
// SELECT grant behind, so existing grantees lose access to the same object.
func TestAlterMaterializedViewRenamePreservesSelectPrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE RENAME preserves materialized view SELECT privilege",
			SetUpScript: []string{
				`CREATE USER rename_mv_reader PASSWORD 'reader';`,
				`CREATE TABLE rename_mv_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO rename_mv_source VALUES (1, 'visible after rename');`,
				`CREATE MATERIALIZED VIEW rename_mv_target AS
					SELECT id, label FROM rename_mv_source;`,
				`GRANT USAGE ON SCHEMA public TO rename_mv_reader;`,
				`GRANT SELECT ON rename_mv_target TO rename_mv_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label
						FROM rename_mv_target;`,
					Expected: []sql.Row{{1, "visible after rename"}},
					Username: `rename_mv_reader`,
					Password: `reader`,
				},
				{
					Query: `ALTER TABLE rename_mv_target
						RENAME TO rename_mv_target_new;`,
				},
				{
					Query: `SELECT id, label
						FROM rename_mv_target_new;`,
					Expected: []sql.Row{{1, "visible after rename"}},
					Username: `rename_mv_reader`,
					Password: `reader`,
				},
			},
		},
	})
}
