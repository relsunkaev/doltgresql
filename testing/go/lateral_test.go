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

// TestLateralJoins pins the LATERAL join shapes real PG views use to
// project per-row computations: LEFT JOIN LATERAL (...) ON true and
// CROSS JOIN LATERAL (...). Per the View/query TODO in
// docs/app-compatibility-checklist.md.
func TestLateralJoins(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			// LEFT JOIN LATERAL preserves outer rows whose lateral
			// subquery returns no rows, projecting NULLs on the inner side.
			Name: "LEFT JOIN LATERAL",
			SetUpScript: []string{
				`CREATE TABLE customers (id INT PRIMARY KEY, name TEXT);`,
				`CREATE TABLE orders (
					id INT PRIMARY KEY,
					customer_id INT,
					placed_at INT,
					amount INT
				);`,
				`INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Cara');`,
				`INSERT INTO orders VALUES
					(10, 1, 100, 50),
					(11, 1, 200, 75),
					(12, 1, 300, 200),
					(13, 2, 150, 30);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT c.id, c.name, o.amount AS latest_amount
						FROM customers c
						LEFT JOIN LATERAL (
							SELECT amount FROM orders
							WHERE customer_id = c.id
							ORDER BY placed_at DESC
							LIMIT 1
						) o ON true
						ORDER BY c.id;`,
					Expected: []sql.Row{
						{int32(1), "Alice", int32(200)},
						{int32(2), "Bob", int32(30)},
						{int32(3), "Cara", nil},
					},
				},
			},
		},
		{
			Name: "CROSS JOIN LATERAL: top-N per group",
			SetUpScript: []string{
				`CREATE TABLE departments (id INT PRIMARY KEY, name TEXT);`,
				`CREATE TABLE employees (
					id INT PRIMARY KEY,
					department_id INT,
					name TEXT,
					salary INT
				);`,
				`INSERT INTO departments VALUES (1, 'Eng'), (2, 'Sales');`,
				`INSERT INTO employees VALUES
					(10, 1, 'A', 100),
					(11, 1, 'B', 90),
					(12, 1, 'C', 80),
					(13, 2, 'D', 70),
					(14, 2, 'E', 60);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// Top 2 employees per department by salary.
					Query: `SELECT d.name AS dept, e.name AS emp, e.salary
						FROM departments d
						CROSS JOIN LATERAL (
							SELECT name, salary FROM employees
							WHERE department_id = d.id
							ORDER BY salary DESC
							LIMIT 2
						) e
						ORDER BY d.id, e.salary DESC;`,
					Expected: []sql.Row{
						{"Eng", "A", int32(100)},
						{"Eng", "B", int32(90)},
						{"Sales", "D", int32(70)},
						{"Sales", "E", int32(60)},
					},
				},
			},
		},
		{
			Name: "LATERAL projects computed columns referencing outer row",
			SetUpScript: []string{
				`CREATE TABLE products (
					id INT PRIMARY KEY,
					price INT,
					qty INT
				);`,
				`INSERT INTO products VALUES (1, 100, 5), (2, 50, 10), (3, 200, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT p.id, p.price, p.qty, t.total
						FROM products p
						CROSS JOIN LATERAL (SELECT p.price * p.qty AS total) t
						ORDER BY p.id;`,
					Expected: []sql.Row{
						{int32(1), int32(100), int32(5), int32(500)},
						{int32(2), int32(50), int32(10), int32(500)},
						{int32(3), int32(200), int32(1), int32(200)},
					},
				},
			},
		},
	})
}
