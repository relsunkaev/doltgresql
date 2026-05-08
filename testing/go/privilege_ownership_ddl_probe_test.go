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

// TestPrivilegeOwnershipDDLProbe pins the privilege/ownership DDL
// shapes pg_dump emits. The contract for dump-restore is "the
// statement is accepted (or no-ops cleanly) so a dump replay
// doesn't error" — full ownership and ACL semantics are tracked
// against the auth surface separately. Per the Schema/DDL TODO in
// docs/app-compatibility-checklist.md.
func TestPrivilegeOwnershipDDLProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE OWNER TO is accepted",
			SetUpScript: []string{
				`CREATE TABLE owned (id INT PRIMARY KEY);`,
				`CREATE ROLE app_role;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE owned OWNER TO app_role;`,
				},
			},
		},
		{
			Name: "GRANT/REVOKE SELECT on a table is accepted",
			SetUpScript: []string{
				`CREATE TABLE shared (id INT PRIMARY KEY, v TEXT);`,
				`CREATE ROLE reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT SELECT ON shared TO reader;`,
				},
				{
					Query: `REVOKE SELECT ON shared FROM reader;`,
				},
			},
		},
		{
			// pg_dump emits this for schemas that have non-default
			// ACL inheritance. Doltgres does not enforce default ACLs,
			// but accepting the statement as a no-op keeps dump replay
			// from failing.
			Name: "ALTER DEFAULT PRIVILEGES is accepted as a no-op",
			SetUpScript: []string{
				`CREATE ROLE deployer;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER DEFAULT PRIVILEGES IN SCHEMA public
						GRANT SELECT ON TABLES TO deployer;`,
				},
			},
		},
	})
}
