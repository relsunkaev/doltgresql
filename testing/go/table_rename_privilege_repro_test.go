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

// TestAlterTableRenamePreservesTablePrivilegesRepro reproduces an ACL
// persistence bug: ALTER TABLE RENAME changes the table name but leaves table
// privileges behind, so existing grantees lose access to the same relation.
func TestAlterTableRenamePreservesTablePrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE RENAME preserves table privileges",
			SetUpScript: []string{
				`CREATE USER rename_priv_reader PASSWORD 'reader';`,
				`CREATE TABLE rename_priv_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO rename_priv_items VALUES (1, 'visible after rename');`,
				`GRANT USAGE ON SCHEMA public TO rename_priv_reader;`,
				`GRANT SELECT ON rename_priv_items TO rename_priv_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label
						FROM rename_priv_items;`,
					Expected: []sql.Row{{1, "visible after rename"}},
					Username: `rename_priv_reader`,
					Password: `reader`,
				},
				{
					Query: `ALTER TABLE rename_priv_items
						RENAME TO rename_priv_items_renamed;`,
				},
				{
					Query: `SELECT id, label
						FROM rename_priv_items_renamed;`,
					Expected: []sql.Row{{1, "visible after rename"}},
					Username: `rename_priv_reader`,
					Password: `reader`,
				},
			},
		},
	})
}
