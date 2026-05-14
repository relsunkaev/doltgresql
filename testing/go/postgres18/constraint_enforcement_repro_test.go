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

package postgres18

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"
)

// TestPostgres18CheckConstraintNotEnforcedRepro reproduces a PostgreSQL 18
// compatibility gap: NOT ENFORCED check constraints are metadata only and do
// not reject violating rows.
func TestPostgres18CheckConstraintNotEnforcedRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "table CHECK NOT ENFORCED allows violating rows",
			SetUpScript: []string{
				`CREATE TABLE check_not_enforced_items (
					id INT PRIMARY KEY,
					qty INT,
					CONSTRAINT qty_positive CHECK (qty > 0) NOT ENFORCED
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO check_not_enforced_items VALUES (1, -5);`,
				},
				{
					Query: `SELECT qty FROM check_not_enforced_items WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/constraint-enforcement-repro-test-testpostgres18checkconstraintnotenforcedrepro-0001-select-qty-from-check_not_enforced_items-where"},
				},
				{
					Query: `SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'qty_positive';`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/constraint-enforcement-repro-test-testpostgres18checkconstraintnotenforcedrepro-0002-select-pg_get_constraintdef-oid-from-pg_constraint"},
				},
			},
		},
		{
			Name: "NOT ENFORCED check does not disable enforced checks",
			SetUpScript: []string{
				`CREATE TABLE mixed_check_enforcement_items (
					id INT PRIMARY KEY,
					qty INT,
					CONSTRAINT qty_positive_metadata CHECK (qty > 0) NOT ENFORCED,
					CONSTRAINT qty_floor CHECK (qty > -10)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO mixed_check_enforcement_items VALUES (1, -5);`,
				},
				{
					Query: `INSERT INTO mixed_check_enforcement_items VALUES (2, -20);`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/constraint-enforcement-repro-test-testpostgres18checkconstraintnotenforcedrepro-0003-insert-into-mixed_check_enforcement_items-values-2", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "ALTER TABLE ADD CHECK NOT ENFORCED allows violating rows",
			SetUpScript: []string{
				`CREATE TABLE alter_check_not_enforced_items (
					id INT PRIMARY KEY,
					qty INT
				);`,
				`INSERT INTO alter_check_not_enforced_items VALUES (1, -5);`,
				`ALTER TABLE alter_check_not_enforced_items
					ADD CONSTRAINT alter_qty_positive CHECK (qty > 0) NOT ENFORCED;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO alter_check_not_enforced_items VALUES (2, -10);`,
				},
				{
					Query: `SELECT qty FROM alter_check_not_enforced_items ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/constraint-enforcement-repro-test-testpostgres18checkconstraintnotenforcedrepro-0004-select-qty-from-alter_check_not_enforced_items-order"},
				},
				{
					Query: `SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'alter_qty_positive';`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/constraint-enforcement-repro-test-testpostgres18checkconstraintnotenforcedrepro-0005-select-pg_get_constraintdef-oid-from-pg_constraint"},
				},
			},
		},
	})
}
