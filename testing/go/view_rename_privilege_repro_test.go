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

// TestAlterViewRenamePreservesSelectPrivilegeRepro reproduces an ACL
// persistence bug: ALTER TABLE RENAME on a view leaves the view's SELECT grant
// behind, so existing grantees lose access to the same view.
func TestAlterViewRenamePreservesSelectPrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE RENAME preserves view SELECT privilege",
			SetUpScript: []string{
				`CREATE USER rename_view_reader PASSWORD 'reader';`,
				`CREATE TABLE rename_view_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO rename_view_source VALUES (1, 'visible after rename');`,
				`CREATE VIEW rename_view_target AS
					SELECT id, label FROM rename_view_source;`,
				`GRANT USAGE ON SCHEMA public TO rename_view_reader;`,
				`GRANT SELECT ON rename_view_source TO rename_view_reader;`,
				`GRANT SELECT ON rename_view_target TO rename_view_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label
						FROM rename_view_target;`,

					Username: `rename_view_reader`,
					Password: `reader`, PostgresOracle: ScriptTestPostgresOracle{ID: "view-rename-privilege-repro-test-testalterviewrenamepreservesselectprivilegerepro-0001-select-id-label-from-rename_view_target"},
				},
				{
					Query: `ALTER TABLE rename_view_target
						RENAME TO rename_view_target_new;`,
				},
				{
					Query: `SELECT id, label
						FROM rename_view_target_new;`,

					Username: `rename_view_reader`,
					Password: `reader`, PostgresOracle: ScriptTestPostgresOracle{ID: "view-rename-privilege-repro-test-testalterviewrenamepreservesselectprivilegerepro-0002-select-id-label-from-rename_view_target_new"},
				},
			},
		},
	})
}
