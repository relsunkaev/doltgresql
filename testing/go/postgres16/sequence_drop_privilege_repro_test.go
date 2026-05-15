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

// TestDropSequenceClearsSequencePrivilegesRepro reproduces an ACL persistence
// bug: dropping a sequence does not clear its privileges, so a later sequence
// with the same name inherits access granted to the dropped sequence.
func TestDropSequenceClearsSequencePrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP SEQUENCE clears sequence privileges before recreate",
			SetUpScript: []string{
				`CREATE USER drop_recreate_sequence_user PASSWORD 'sequence';`,
				`CREATE SEQUENCE drop_recreate_acl_seq;`,
				`GRANT USAGE ON SCHEMA public TO drop_recreate_sequence_user;`,
				`GRANT USAGE ON SEQUENCE drop_recreate_acl_seq TO drop_recreate_sequence_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT nextval('drop_recreate_acl_seq');`,

					Username: `drop_recreate_sequence_user`,
					Password: `sequence`, PostgresOracle: ScriptTestPostgresOracle{ID: "sequence-drop-privilege-repro-test-testdropsequenceclearssequenceprivilegesrepro-0001-select-nextval-drop_recreate_acl_seq"},
				},
				{
					Query: `DROP SEQUENCE drop_recreate_acl_seq;`,
				},
				{
					Query: `CREATE SEQUENCE drop_recreate_acl_seq;`,
				},
				{
					Query: `SELECT nextval('drop_recreate_acl_seq');`,

					Username: `drop_recreate_sequence_user`,
					Password: `sequence`, PostgresOracle: ScriptTestPostgresOracle{ID: "sequence-drop-privilege-repro-test-testdropsequenceclearssequenceprivilegesrepro-0002-select-nextval-drop_recreate_acl_seq", Compare: "sqlstate"},
				},
			},
		},
	})
}
