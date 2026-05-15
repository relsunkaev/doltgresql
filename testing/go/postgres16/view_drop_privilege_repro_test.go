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

// TestDropViewClearsViewPrivilegesRepro reproduces an ACL persistence bug:
// dropping a view does not clear its privileges, so a later view with the same
// name inherits access granted to the dropped view.
func TestDropViewClearsViewPrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP VIEW clears view privileges before recreate",
			SetUpScript: []string{
				`CREATE USER drop_recreate_view_reader PASSWORD 'reader';`,
				`CREATE TABLE drop_recreate_view_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO drop_recreate_view_source VALUES (1, 'new sensitive');`,
				`CREATE VIEW drop_recreate_view_target AS
					SELECT id, label FROM drop_recreate_view_source;`,
				`GRANT USAGE ON SCHEMA public TO drop_recreate_view_reader;`,
				`GRANT SELECT ON drop_recreate_view_source TO drop_recreate_view_reader;`,
				`GRANT SELECT ON drop_recreate_view_target TO drop_recreate_view_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label
						FROM drop_recreate_view_target;`,

					Username: `drop_recreate_view_reader`,
					Password: `reader`, PostgresOracle: ScriptTestPostgresOracle{ID: "view-drop-privilege-repro-test-testdropviewclearsviewprivilegesrepro-0001-select-id-label-from-drop_recreate_view_target"},
				},
				{
					Query: `DROP VIEW drop_recreate_view_target;`,
				},
				{
					Query: `CREATE VIEW drop_recreate_view_target AS
						SELECT id, label FROM drop_recreate_view_source;`,
				},
				{
					Query: `SELECT id, label FROM drop_recreate_view_target;`,

					Username: `drop_recreate_view_reader`,
					Password: `reader`, PostgresOracle: ScriptTestPostgresOracle{ID: "view-drop-privilege-repro-test-testdropviewclearsviewprivilegesrepro-0002-select-id-label-from-drop_recreate_view_target", Compare: "sqlstate"},
				},
			},
		},
	})
}
