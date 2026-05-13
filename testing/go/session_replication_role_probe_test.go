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

// TestSessionReplicationRoleProbe pins `SET session_replication_role`
// behavior. pg_dump and many ORM data-import paths flip this to
// 'replica' to suppress trigger and FK firing during bulk load. Per the
// Schema/DDL TODO in docs/app-compatibility-checklist.md.
func TestSessionReplicationRoleProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "SET session_replication_role keyword acceptance",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SET session_replication_role = 'replica';`,
				},
				{
					Query: `SHOW session_replication_role;`, PostgresOracle: ScriptTestPostgresOracle{ID: "session-replication-role-probe-test-testsessionreplicationroleprobe-0001-show-session_replication_role"},
				},
				{
					Query: `SET session_replication_role = 'origin';`,
				},
				{
					Query: `SHOW session_replication_role;`, PostgresOracle: ScriptTestPostgresOracle{ID: "session-replication-role-probe-test-testsessionreplicationroleprobe-0002-show-session_replication_role"},
				},
			},
		},
		{
			Name: "session_replication_role = replica suppresses FK enforcement",
			SetUpScript: []string{
				`CREATE TABLE p (id INT PRIMARY KEY);`,
				`CREATE TABLE c (id INT PRIMARY KEY, pid INT REFERENCES p(id));`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SET session_replication_role = 'replica';`,
				},
				{
					Query: `INSERT INTO c VALUES (1, 999);`,
				},
				{
					Query: `SET session_replication_role = 'origin';`,
				},
				{
					Query: `INSERT INTO c VALUES (2, 999);`, PostgresOracle: ScriptTestPostgresOracle{ID: "session-replication-role-probe-test-testsessionreplicationroleprobe-0003-insert-into-c-values-2", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "session_replication_role = replica suppresses trigger firing",
			SetUpScript: []string{
				`CREATE TABLE main (id INT PRIMARY KEY, label TEXT);`,
				`CREATE TABLE audit_log (main_id INT, label TEXT);`,
				`CREATE FUNCTION log_main_insert() RETURNS trigger AS $$
BEGIN
	INSERT INTO audit_log VALUES (NEW.id, NEW.label);
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER tg_log_main_insert AFTER INSERT ON main
					FOR EACH ROW EXECUTE FUNCTION log_main_insert();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SET session_replication_role = 'replica';`,
				},
				{
					Query: `INSERT INTO main VALUES (1, 'suppressed');`,
				},
				{
					Query: `SELECT count(*)::TEXT FROM audit_log;`, PostgresOracle: ScriptTestPostgresOracle{ID: "session-replication-role-probe-test-testsessionreplicationroleprobe-0004-select-count-*-::text-from"},
				},
				{
					Query: `SET session_replication_role = 'origin';`,
				},
				{
					Query: `INSERT INTO main VALUES (2, 'logged');`,
				},
				{
					Query: `SELECT count(*)::TEXT FROM audit_log;`, PostgresOracle: ScriptTestPostgresOracle{ID: "session-replication-role-probe-test-testsessionreplicationroleprobe-0005-select-count-*-::text-from"},
				},
			},
		},
	})
}
