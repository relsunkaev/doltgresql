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

// TestDataModifyingCtesRepro reproduces a PostgreSQL write-query correctness
// gap: data-modifying statements in WITH can INSERT, UPDATE, or DELETE rows and
// feed their RETURNING output to the outer query.
func TestDataModifyingCtesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "INSERT CTE returns inserted rows",
			SetUpScript: []string{
				`CREATE TABLE insert_cte_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `WITH inserted AS (
							INSERT INTO insert_cte_items VALUES (1, 'one'), (2, 'two')
							RETURNING id, label
						)
						SELECT id, label FROM inserted ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "data-modifying-cte-correctness-repro-test-testdatamodifyingctesrepro-0001-with-inserted-as-insert-into"},
				},
				{
					Query: `SELECT id, label
						FROM insert_cte_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "data-modifying-cte-correctness-repro-test-testdatamodifyingctesrepro-0002-select-id-label-from-insert_cte_items"},
				},
			},
		},
		{
			Name: "UPDATE CTE returns updated rows",
			SetUpScript: []string{
				`CREATE TABLE update_cte_items (
					id INT PRIMARY KEY,
					qty INT
				);`,
				`INSERT INTO update_cte_items VALUES (1, 10), (2, 20);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `WITH updated AS (
							UPDATE update_cte_items
							SET qty = qty + 5
							WHERE id = 1
							RETURNING id, qty
						)
						SELECT id, qty FROM updated;`, PostgresOracle: ScriptTestPostgresOracle{ID: "data-modifying-cte-correctness-repro-test-testdatamodifyingctesrepro-0003-with-updated-as-update-update_cte_items"},
				},
				{
					Query: `SELECT id, qty
						FROM update_cte_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "data-modifying-cte-correctness-repro-test-testdatamodifyingctesrepro-0004-select-id-qty-from-update_cte_items"},
				},
			},
		},
		{
			Name: "DELETE CTE returns deleted rows",
			SetUpScript: []string{
				`CREATE TABLE delete_cte_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO delete_cte_items VALUES (1, 'remove'), (2, 'keep');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `WITH deleted AS (
							DELETE FROM delete_cte_items
							WHERE id = 1
							RETURNING id, label
						)
						SELECT id, label FROM deleted;`, PostgresOracle: ScriptTestPostgresOracle{ID: "data-modifying-cte-correctness-repro-test-testdatamodifyingctesrepro-0005-with-deleted-as-delete-from"},
				},
				{
					Query: `SELECT id, label
						FROM delete_cte_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "data-modifying-cte-correctness-repro-test-testdatamodifyingctesrepro-0006-select-id-label-from-delete_cte_items"},
				},
			},
		},
	})
}

// TestReadOnlyCteFeedsOuterDataModificationRepro guards PostgreSQL's support
// for a read-only WITH query attached to an outer data-modifying statement.
func TestReadOnlyCteFeedsOuterDataModificationRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "read-only CTE feeds outer INSERT",
			SetUpScript: []string{
				`CREATE TABLE cte_insert_source (id INT PRIMARY KEY, qty INT);`,
				`CREATE TABLE cte_insert_target (id INT PRIMARY KEY, qty INT);`,
				`INSERT INTO cte_insert_source VALUES (1, 10), (2, 20);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `WITH source_rows AS (
							SELECT id, qty FROM cte_insert_source WHERE qty >= 10
						)
						INSERT INTO cte_insert_target
						SELECT id, qty + 1 FROM source_rows
						RETURNING id, qty;`, PostgresOracle: ScriptTestPostgresOracle{ID: "data-modifying-cte-correctness-repro-test-testreadonlyctefeedsouterdatamodificationrepro-0001-with-source_rows-as-select-id"},
				},
				{
					Query: `SELECT id, qty FROM cte_insert_target ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "data-modifying-cte-correctness-repro-test-testreadonlyctefeedsouterdatamodificationrepro-0002-select-id-qty-from-cte_insert_target"},
				},
			},
		},
		{
			Name: "read-only CTE feeds outer UPDATE",
			SetUpScript: []string{
				`CREATE TABLE cte_update_source (id INT PRIMARY KEY, delta INT);`,
				`CREATE TABLE cte_update_target (id INT PRIMARY KEY, qty INT);`,
				`INSERT INTO cte_update_source VALUES (1, 5), (2, 7);`,
				`INSERT INTO cte_update_target VALUES (1, 10), (2, 20);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `WITH deltas AS (
							SELECT id, delta FROM cte_update_source
						)
						UPDATE cte_update_target AS t
						SET qty = qty + d.delta
						FROM deltas AS d
						WHERE t.id = d.id
						RETURNING t.id, t.qty;`, PostgresOracle: ScriptTestPostgresOracle{ID: "data-modifying-cte-correctness-repro-test-testreadonlyctefeedsouterdatamodificationrepro-0003-with-deltas-as-select-id"},
				},
				{
					Query: `SELECT id, qty FROM cte_update_target ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "data-modifying-cte-correctness-repro-test-testreadonlyctefeedsouterdatamodificationrepro-0004-select-id-qty-from-cte_update_target"},
				},
			},
		},
		{
			Name: "read-only CTE feeds outer DELETE",
			SetUpScript: []string{
				`CREATE TABLE cte_delete_source (id INT PRIMARY KEY);`,
				`CREATE TABLE cte_delete_target (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO cte_delete_source VALUES (2), (3);`,
				`INSERT INTO cte_delete_target VALUES (1, 'keep'), (2, 'delete-a'), (3, 'delete-b');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `WITH doomed AS (
							SELECT id FROM cte_delete_source
						)
						DELETE FROM cte_delete_target
						WHERE id IN (SELECT id FROM doomed)
						RETURNING id, label;`, PostgresOracle: ScriptTestPostgresOracle{ID: "data-modifying-cte-correctness-repro-test-testreadonlyctefeedsouterdatamodificationrepro-0005-with-doomed-as-select-id"},
				},
				{
					Query: `SELECT id, label FROM cte_delete_target ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "data-modifying-cte-correctness-repro-test-testreadonlyctefeedsouterdatamodificationrepro-0006-select-id-label-from-cte_delete_target"},
				},
			},
		},
	})
}
