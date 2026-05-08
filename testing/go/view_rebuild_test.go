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

	"github.com/dolthub/go-mysql-server/sql"
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
					Query:    `SELECT id, amount FROM paid_orders ORDER BY id;`,
					Expected: []sql.Row{{int32(1), int32(100)}, {int32(3), int32(200)}},
				},
				{
					// Rebuild the view body — same projection shape,
					// different filter. This is the common rebuild
					// flow when application logic changes the
					// definition.
					Query:    `CREATE OR REPLACE VIEW paid_orders AS SELECT id, amount FROM orders WHERE amount >= 100;`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT id, amount FROM paid_orders ORDER BY id;`,
					Expected: []sql.Row{{int32(1), int32(100)}, {int32(3), int32(200)}},
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
					Query:    `DROP VIEW v_old;`,
					Expected: []sql.Row{},
				},
				{
					Query:    `CREATE VIEW v_old AS SELECT id, v * 2 AS doubled FROM t;`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT id, doubled FROM v_old ORDER BY id;`,
					Expected: []sql.Row{{int32(1), int32(20)}, {int32(2), int32(40)}},
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
					Query:    `SELECT total FROM us_sales_total;`,
					Expected: []sql.Row{{"150"}},
				},
				{
					// Replace the underlying view; dependent view
					// should reflect the new shape on next read.
					Query:    `CREATE OR REPLACE VIEW us_sales AS SELECT id, amount FROM sales WHERE region IN ('us', 'eu');`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT total FROM us_sales_total;`,
					Expected: []sql.Row{{"350"}},
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
						CASE WHEN balance IS NULL THEN 'unknown'
						     WHEN balance < 100 THEN 'low'
						     ELSE 'ok' END AS status
					FROM accounts;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, status FROM account_status ORDER BY id;`,
					Expected: []sql.Row{
						{int32(1), "low"},
						{int32(2), "unknown"},
						{int32(3), "ok"},
					},
				},
				{
					Query: `CREATE OR REPLACE VIEW account_status AS
						SELECT id, COALESCE(balance, 0) AS effective_balance
						FROM accounts;`,
					Expected: []sql.Row{},
				},
				{
					Query: `SELECT id, effective_balance FROM account_status ORDER BY id;`,
					Expected: []sql.Row{
						{int32(1), int32(50)},
						{int32(2), int32(0)},
						{int32(3), int32(200)},
					},
				},
			},
		},
	})
}
