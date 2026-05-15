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

// TestDynamicViewRebuild pins the CREATE OR REPLACE VIEW rebuild path
// real applications use to redeploy a view's body without dropping
// dependent objects. Per the View/query TODO in
// docs/app-compatibility-checklist.md.
func TestDynamicViewRebuild(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE OR REPLACE VIEW with same column shape",
			SetUpScript: []string{
				`CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);`,
				`INSERT INTO orders VALUES (1, 100, 'paid'), (2, 50, 'pending'), (3, 200, 'paid');`,
				`CREATE VIEW paid_orders AS SELECT id, amount FROM orders WHERE status = 'paid';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, amount FROM paid_orders ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "view-rebuild-test-testdynamicviewrebuild-0001-select-id-amount-from-paid_orders"},
				},
				{
					// Rebuild the view body — same projection shape,
					// different filter. This is the common rebuild
					// flow when application logic changes the
					// definition.
					Query: `CREATE OR REPLACE VIEW paid_orders AS SELECT id, amount FROM orders WHERE amount >= 100;`, PostgresOracle: ScriptTestPostgresOracle{ID: "view-rebuild-test-testdynamicviewrebuild-0002-create-or-replace-view-paid_orders"},
				},
				{
					Query: `SELECT id, amount FROM paid_orders ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "view-rebuild-test-testdynamicviewrebuild-0003-select-id-amount-from-paid_orders"},
				},
			},
		},
		{
			Name: "DROP VIEW + CREATE VIEW rebuild flow",
			SetUpScript: []string{
				`CREATE TABLE t (id INT PRIMARY KEY, v INT);`,
				`INSERT INTO t VALUES (1, 10), (2, 20);`,
				`CREATE VIEW v_old AS SELECT id, v FROM t;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP VIEW v_old;`, PostgresOracle: ScriptTestPostgresOracle{ID: "view-rebuild-test-testdynamicviewrebuild-0004-drop-view-v_old"},
				},
				{
					Query: `CREATE VIEW v_old AS SELECT id, v * 2 AS doubled FROM t;`, PostgresOracle: ScriptTestPostgresOracle{ID: "view-rebuild-test-testdynamicviewrebuild-0005-create-view-v_old-as-select"},
				},
				{
					Query: `SELECT id, doubled FROM v_old ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "view-rebuild-test-testdynamicviewrebuild-0006-select-id-doubled-from-v_old"},
				},
			},
		},
		{
			Name: "view depending on another view",
			SetUpScript: []string{
				`CREATE TABLE sales (id INT PRIMARY KEY, amount INT, region TEXT);`,
				`INSERT INTO sales VALUES (1, 100, 'us'), (2, 200, 'eu'), (3, 50, 'us');`,
				`CREATE VIEW us_sales AS SELECT id, amount FROM sales WHERE region = 'us';`,
				`CREATE VIEW us_sales_total AS SELECT sum(amount)::text AS total FROM us_sales;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT total FROM us_sales_total;`, PostgresOracle: ScriptTestPostgresOracle{ID: "view-rebuild-test-testdynamicviewrebuild-0007-select-total-from-us_sales_total"},
				},
				{
					// Replace the underlying view; dependent view
					// should reflect the new shape on next read.
					Query: `CREATE OR REPLACE VIEW us_sales AS SELECT id, amount FROM sales WHERE region IN ('us', 'eu');`, PostgresOracle: ScriptTestPostgresOracle{ID: "view-rebuild-test-testdynamicviewrebuild-0008-create-or-replace-view-us_sales"},
				},
				{
					Query: `SELECT total FROM us_sales_total;`, PostgresOracle: ScriptTestPostgresOracle{ID: "view-rebuild-test-testdynamicviewrebuild-0009-select-total-from-us_sales_total"},
				},
			},
		},
		{
			Name: "view body CASE/COALESCE expressions survive rebuild",
			SetUpScript: []string{
				`CREATE TABLE accounts (id INT PRIMARY KEY, balance INT);`,
				`INSERT INTO accounts VALUES (1, 50), (2, NULL), (3, 200);`,
				`CREATE VIEW account_status AS
					SELECT id,
						CASE WHEN balance IS NULL THEN COALESCE('unknown', 'missing')
						     WHEN balance < 100 THEN COALESCE('low', 'small')
						     ELSE COALESCE('ok', 'healthy') END AS status
					FROM accounts;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, status FROM account_status ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "view-rebuild-test-testdynamicviewrebuild-0010-select-id-status-from-account_status"},
				},
				{
					Query: `CREATE OR REPLACE VIEW account_status AS
						SELECT id,
							CASE WHEN balance IS NULL THEN COALESCE('missing', 'unknown')
							     WHEN balance < 100 THEN COALESCE('small', 'low')
							     ELSE COALESCE('healthy', 'ok') END AS status
						FROM accounts;`, PostgresOracle: ScriptTestPostgresOracle{ID: "view-rebuild-test-testdynamicviewrebuild-0011-create-or-replace-view-account_status"},
				},
				{
					Query: `SELECT id, status FROM account_status ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "view-rebuild-test-testdynamicviewrebuild-0012-select-id-status-from-account_status"},
				},
			},
		},
	})
}
