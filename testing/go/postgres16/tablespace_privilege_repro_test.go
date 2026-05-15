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

// TestTablespaceCreatePrivilegeCanBeGrantedAndRevokedRepro reproduces an ACL
// correctness bug: PostgreSQL supports CREATE privileges on tablespaces.
func TestTablespaceCreatePrivilegeCanBeGrantedAndRevokedRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "tablespace CREATE can be granted and revoked",
			SetUpScript: []string{
				`CREATE USER tablespace_create_acl_user PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT CREATE ON TABLESPACE pg_default TO tablespace_create_acl_user;`, PostgresOracle: ScriptTestPostgresOracle{ID: "tablespace-privilege-repro-test-testtablespacecreateprivilegecanbegrantedandrevokedrepro-0001-grant-create-on-tablespace-pg_default"},
				},
				{
					Query: `REVOKE CREATE ON TABLESPACE pg_default FROM tablespace_create_acl_user;`, PostgresOracle: ScriptTestPostgresOracle{ID: "tablespace-privilege-repro-test-testtablespacecreateprivilegecanbegrantedandrevokedrepro-0002-revoke-create-on-tablespace-pg_default"},
				},
			},
		},
	})
}
