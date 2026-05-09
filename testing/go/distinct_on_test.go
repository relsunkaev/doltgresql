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

// TestDistinctOn pins the SELECT DISTINCT ON (...) shapes real views use
// for "latest row per group" patterns. Per the View/query TODO in
// docs/app-compatibility-checklist.md.
//
// PostgreSQL semantics:
//
//	SELECT DISTINCT ON (k1, k2, ...) ...
//	  ORDER BY k1, k2, ..., tiebreak
//
// returns the first row encountered for each distinct combination of
// (k1, k2, ...) in the ORDER BY ordering. The leading ORDER BY columns
// must match the DISTINCT ON columns; remaining ORDER BY columns
// determine which row is the "first" within a group.
func TestDistinctOn(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DISTINCT ON: latest row per customer",
			SetUpScript: []string{
				`CREATE TABLE orders (
					id INT PRIMARY KEY,
					customer_id INT,
					placed_at TIMESTAMP,
					amount INT
				);`,
				`INSERT INTO orders VALUES
					(1, 100, '2026-01-01 09:00:00', 50),
					(2, 100, '2026-01-02 10:00:00', 75),
					(3, 100, '2026-01-03 11:00:00', 200),
					(4, 200, '2026-01-01 09:30:00', 30),
					(5, 200, '2026-01-04 12:00:00', 90);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// Latest order per customer.
					Query: `SELECT DISTINCT ON (customer_id) customer_id, id, amount
						FROM orders
						ORDER BY customer_id, placed_at DESC;`,
					Expected: []sql.Row{
						{int32(100), int32(3), int32(200)},
						{int32(200), int32(5), int32(90)},
					},
				},
			},
		},
		{
			Name: "DISTINCT ON multi-column key",
			SetUpScript: []string{
				`CREATE TABLE events (
					id INT PRIMARY KEY,
					tenant TEXT,
					stream TEXT,
					seq INT,
					payload TEXT
				);`,
				`INSERT INTO events VALUES
					(1, 'a', 'x', 1, 'a-x-1'),
					(2, 'a', 'x', 5, 'a-x-5'),
					(3, 'a', 'y', 2, 'a-y-2'),
					(4, 'b', 'x', 3, 'b-x-3'),
					(5, 'b', 'x', 4, 'b-x-4');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// Latest event per (tenant, stream).
					Query: `SELECT DISTINCT ON (tenant, stream) tenant, stream, seq, payload
						FROM events
						ORDER BY tenant, stream, seq DESC;`,
					Expected: []sql.Row{
						{"a", "x", int32(5), "a-x-5"},
						{"a", "y", int32(2), "a-y-2"},
						{"b", "x", int32(4), "b-x-4"},
					},
				},
			},
		},
		{
			Name: "DISTINCT ON with WHERE filter",
			SetUpScript: []string{
				`CREATE TABLE prices (
					id INT PRIMARY KEY,
					sku TEXT,
					price INT,
					region TEXT
				);`,
				`INSERT INTO prices VALUES
					(1, 'a', 10, 'us'),
					(2, 'a', 12, 'eu'),
					(3, 'a', 9, 'us'),
					(4, 'b', 5, 'us'),
					(5, 'b', 7, 'eu');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// Lowest US price per sku.
					Query: `SELECT DISTINCT ON (sku) sku, price
						FROM prices
						WHERE region = 'us'
						ORDER BY sku, price ASC;`,
					Expected: []sql.Row{
						{"a", int32(9)},
						{"b", int32(5)},
					},
				},
			},
		},
		{
			Name: "DISTINCT ON returns NULL groups",
			SetUpScript: []string{
				`CREATE TABLE t (k INT, v INT, ts INT);`,
				`INSERT INTO t VALUES
					(1, 10, 100),
					(1, 11, 200),
					(NULL, 20, 50),
					(NULL, 21, 60);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// NULL forms its own DISTINCT ON group. PostgreSQL
					// default ASC ordering places that group after
					// non-NULL keys.
					Query: `SELECT DISTINCT ON (k) k, v
						FROM t
						ORDER BY k, ts DESC;`,
					Expected: []sql.Row{
						{int32(1), int32(11)},
						{nil, int32(21)},
					},
				},
			},
		},
	})
}
