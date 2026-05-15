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

func TestPgDogCompatibilityBoundary(t *testing.T) {
	RunScripts(
		t,
		[]ScriptTest{
			{
				Name: "PgDog schema split catalog and qualified DML",
				SetUpScript: []string{
					"CREATE SCHEMA shared;",
					"CREATE SCHEMA customer;",
					"CREATE TABLE shared.accounts (id INT PRIMARY KEY, label TEXT NOT NULL);",
					"CREATE TABLE customer.orders (customer_id BIGINT NOT NULL, order_id BIGINT NOT NULL, status TEXT NOT NULL, amount INT NOT NULL, note TEXT, PRIMARY KEY (customer_id, order_id));",
					"INSERT INTO shared.accounts VALUES (1, 'shared-one');",
					"INSERT INTO customer.orders VALUES (42, 1, 'open', 420, 'customer-one');",
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: "SELECT label FROM shared.accounts WHERE id = 1;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgdog-compat-test-testpgdogcompatibilityboundary-0016-select-label-from-shared.accounts-where"},
					},
					{
						Query: "UPDATE customer.orders SET status = 'updated', amount = amount + 1 WHERE customer_id = 42 AND order_id = 1;",
					},
					{
						Query: "SELECT status, amount FROM customer.orders WHERE customer_id = 42 AND order_id = 1;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgdog-compat-test-testpgdogcompatibilityboundary-0017-select-status-amount-from-customer.orders"},
					},
					{
						Query: `SELECT table_schema, table_name
			FROM information_schema.tables
			WHERE table_schema IN ('shared', 'customer')
			ORDER BY table_schema, table_name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgdog-compat-test-testpgdogcompatibilityboundary-0018-select-table_schema-table_name-from-information_schema.tables"},
					},
					{
						Query: `SELECT n.nspname, c.relname
			FROM pg_catalog.pg_class c
			JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname IN ('shared', 'customer') AND c.relkind = 'r' AND c.relname NOT LIKE 'dolt_%'
			ORDER BY n.nspname, c.relname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pgdog-compat-test-testpgdogcompatibilityboundary-0019-select-n.nspname-c.relname-from-pg_catalog.pg_class"},
					},
					{
						Query: "SELECT 'customer.orders'::regclass::text, 'shared.accounts'::regclass::text;", PostgresOracle: ScriptTestPostgresOracle{ID: "pgdog-compat-test-testpgdogcompatibilityboundary-0020-select-customer.orders-::regclass::text-shared.accounts-::regclass::text"},
					},
				},
			},
		},
	)
}
