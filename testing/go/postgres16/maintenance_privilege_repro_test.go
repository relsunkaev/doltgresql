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

// TestVacuumTableRequiresOwnershipRepro reproduces a security bug: Doltgres
// accepts VACUUM on another role's table, while PostgreSQL requires table
// ownership or an equivalent maintenance privilege.
func TestVacuumTableRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "VACUUM requires table ownership",
			SetUpScript: []string{
				`CREATE TABLE vacuum_private (id INT PRIMARY KEY, secret TEXT);`,
				`INSERT INTO vacuum_private VALUES (1, 'hidden');`,
				`CREATE USER vacuum_intruder PASSWORD 'pw';`,
				`GRANT USAGE ON SCHEMA public TO vacuum_intruder;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `VACUUM vacuum_private;`,

					Username: `vacuum_intruder`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						ID:      "maintenance-privilege-repro-test-testvacuumtablerequiresownershiprepro-0001-vacuum-vacuum_private",
						Compare: "sqlstate"},
				},
			},
		},
	})
}

// TestPgMaintainRoleIsAbsentInPostgres16Repro keeps the PostgreSQL 16 boundary
// explicit. pg_maintain is a PostgreSQL 17 predefined role, while PostgreSQL 16
// has table-level MAINTAIN privilege coverage below.
func TestPgMaintainRoleIsAbsentInPostgres16Repro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_maintain is absent in PostgreSQL 16",
			SetUpScript: []string{
				`CREATE USER maintain_vacuum_user PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `GRANT pg_maintain TO maintain_vacuum_user;`,
					ExpectedErr: `role "pg_maintain" does not exist`,
				},
			},
		},
	})
}

// TestTableMaintainPrivilegeAllowsVacuumRepro reproduces a table-privilege
// security gap: PostgreSQL's MAINTAIN privilege allows VACUUM on a specific
// table without requiring table ownership or broad pg_maintain membership.
func TestTableMaintainPrivilegeAllowsVacuumRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "table MAINTAIN privilege allows VACUUM",
			SetUpScript: []string{
				`CREATE USER table_maintain_user PASSWORD 'pw';`,
				`CREATE TABLE table_maintain_private (id INT PRIMARY KEY);`,
				`INSERT INTO table_maintain_private VALUES (1);`,
				`GRANT USAGE ON SCHEMA public TO table_maintain_user;`,
				`GRANT MAINTAIN ON table_maintain_private TO table_maintain_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `VACUUM table_maintain_private;`,
					Username: `table_maintain_user`,
					Password: `pw`,
				},
			},
		},
	})
}

// TestVacuumCannotRunInsideTransactionBlockRepro reproduces a PostgreSQL
// compatibility gap: VACUUM is a top-level utility command and must reject
// execution inside an explicit transaction block.
func TestVacuumCannotRunInsideTransactionBlockRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "VACUUM rejects explicit transaction block",
			SetUpScript: []string{
				`CREATE TABLE vacuum_transaction_target (id INT PRIMARY KEY);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `VACUUM vacuum_transaction_target;`, PostgresOracle: ScriptTestPostgresOracle{ID: "maintenance-privilege-repro-test-testvacuumcannotruninsidetransactionblockrepro-0001-vacuum-vacuum_transaction_target",

						// TestPostgres16VacuumAnalyzeBufferUsageLimitRepro reproduces a PostgreSQL 16
						// compatibility gap: VACUUM and ANALYZE accept BUFFER_USAGE_LIMIT in their
						// parenthesized option lists.
						Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK;`,
				},
			},
		},
	})
}

func TestPostgres16VacuumAnalyzeBufferUsageLimitRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "VACUUM and ANALYZE accept BUFFER_USAGE_LIMIT",
			SetUpScript: []string{
				`CREATE TABLE maintenance_buffer_limit_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO maintenance_buffer_limit_items VALUES
					(1, 'one'),
					(2, 'two');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `VACUUM (BUFFER_USAGE_LIMIT '128 kB') maintenance_buffer_limit_items;`,
				},
				{
					Query: `ANALYZE (BUFFER_USAGE_LIMIT '128 kB') maintenance_buffer_limit_items;`,
				},
			},
		},
	})
}
