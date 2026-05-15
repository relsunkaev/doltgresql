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

// TestDropMaterializedViewClearsPrivilegesRepro reproduces an ACL persistence
// bug: dropping a materialized view does not clear its privileges, so a later
// materialized view with the same name inherits access granted to the dropped
// object.
func TestDropMaterializedViewClearsPrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP MATERIALIZED VIEW clears privileges before recreate",
			SetUpScript: []string{
				`CREATE USER drop_recreate_mv_reader PASSWORD 'reader';`,
				`CREATE TABLE drop_recreate_mv_source (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO drop_recreate_mv_source VALUES (1, 'new sensitive');`,
				`CREATE MATERIALIZED VIEW drop_recreate_mv_target AS
					SELECT id, label FROM drop_recreate_mv_source;`,
				`GRANT USAGE ON SCHEMA public TO drop_recreate_mv_reader;`,
				`GRANT SELECT ON drop_recreate_mv_target TO drop_recreate_mv_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label
						FROM drop_recreate_mv_target;`,

					Username: `drop_recreate_mv_reader`,
					Password: `reader`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-drop-privilege-repro-test-testdropmaterializedviewclearsprivilegesrepro-0001-select-id-label-from-drop_recreate_mv_target"},
				},
				{
					Query: `DROP MATERIALIZED VIEW drop_recreate_mv_target;`,
				},
				{
					Query: `CREATE MATERIALIZED VIEW drop_recreate_mv_target AS
						SELECT id, label FROM drop_recreate_mv_source;`,
				},
				{
					Query: `SELECT id, label FROM drop_recreate_mv_target;`,

					Username: `drop_recreate_mv_reader`,
					Password: `reader`, PostgresOracle: ScriptTestPostgresOracle{ID: "materialized-view-drop-privilege-repro-test-testdropmaterializedviewclearsprivilegesrepro-0002-select-id-label-from-drop_recreate_mv_target", Compare: "sqlstate"},
				},
			},
		},
	})
}
