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

// TestPgConstraintIntrospectionForORMs pins the exact PK and unique
// constraint introspection queries drizzle-kit (and by extension Prisma,
// Sequelize, anything sitting on the `pg` Node driver) issues during
// `introspect` / `db pull`. Until both the per-column workhorse query
// (information_schema.table_constraints + constraint_column_usage) and
// the composite-PK lookup against pg_constraint return the right rows,
// drizzle skips the primaryKey({ ... }) / .unique() blocks in the
// generated schema.ts.
//
// See docs/app-compatibility-checklist.md, "pg_constraint completeness"
// and the two `if false` blocks in drizzle_kit_introspect_test.go.
func TestPgConstraintIntrospectionForORMs(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "table_constraints + constraint_column_usage exposes simple PK",
			SetUpScript: []string{
				`CREATE TABLE customers (id INT PRIMARY KEY, email TEXT UNIQUE);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// drizzle-kit's tableConstraints workhorse query.
					Query: `SELECT c.column_name, c.data_type, constraint_type, constraint_name, constraint_schema
FROM information_schema.table_constraints tc
JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)
JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
  AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
WHERE tc.table_name = 'customers' and constraint_schema = 'public'
ORDER BY constraint_type, constraint_name, c.column_name;`,
					Expected: []sql.Row{
						{"id", "integer", "PRIMARY KEY", "customers_pkey", "public"},
						{"email", "text", "UNIQUE", "customers_email_key", "public"},
					},
				},
			},
		},
		{
			Name: "table_constraints + constraint_column_usage exposes composite PK",
			SetUpScript: []string{
				`CREATE TABLE order_items (order_id INT, line_no INT, qty INT, PRIMARY KEY (order_id, line_no));`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT c.column_name, c.data_type, constraint_type, constraint_name, constraint_schema
FROM information_schema.table_constraints tc
JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)
JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
  AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
WHERE tc.table_name = 'order_items' and constraint_schema = 'public'
ORDER BY c.column_name;`,
					Expected: []sql.Row{
						{"line_no", "integer", "PRIMARY KEY", "order_items_pkey", "public"},
						{"order_id", "integer", "PRIMARY KEY", "order_items_pkey", "public"},
					},
				},
				{
					// drizzle-kit's composite-PK lookup against pg_constraint.
					Query: `SELECT conname AS primary_key
FROM pg_constraint join pg_class on (pg_class.oid = conrelid)
WHERE contype = 'p' AND connamespace = 'public'::regnamespace AND pg_class.relname = 'order_items';`,
					Expected: []sql.Row{
						{"order_items_pkey"},
					},
				},
			},
		},
		{
			Name: "table_constraints exposes single-column UNIQUE",
			SetUpScript: []string{
				`CREATE TABLE customers (id INT PRIMARY KEY, email TEXT UNIQUE);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT c.column_name, c.data_type, constraint_type, constraint_name, constraint_schema
FROM information_schema.table_constraints tc
JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)
JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
  AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
WHERE tc.table_name = 'customers' and constraint_schema = 'public' AND constraint_type = 'UNIQUE'
ORDER BY c.column_name;`,
					Expected: []sql.Row{
						{"email", "text", "UNIQUE", "customers_email_key", "public"},
					},
				},
			},
		},
		{
			Name: "table_constraints exposes Sequelize FK catalog columns",
			SetUpScript: []string{
				`CREATE TABLE seq_accounts (id INT PRIMARY KEY);`,
				`CREATE TABLE seq_items (
					id INT PRIMARY KEY,
					account_id INT NOT NULL,
					CONSTRAINT seq_items_account_id_fkey
						FOREIGN KEY (account_id)
						REFERENCES seq_accounts(id)
						DEFERRABLE INITIALLY DEFERRED
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT DISTINCT
  tc.constraint_name,
  tc.constraint_schema,
  tc.constraint_catalog,
  tc.table_name,
  tc.table_schema,
  tc.table_catalog,
  tc.initially_deferred,
  tc.is_deferrable,
  kcu.column_name,
  ccu.table_schema AS referenced_table_schema,
  ccu.table_catalog AS referenced_table_catalog,
  ccu.table_name AS referenced_table_name,
  ccu.column_name AS referenced_column_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
WHERE constraint_type = 'FOREIGN KEY'
  AND tc.table_name = 'seq_items'
  AND tc.table_catalog = 'postgres'
ORDER BY tc.constraint_name, kcu.column_name;`,
					Expected: []sql.Row{{
						"seq_items_account_id_fkey",
						"public",
						"postgres",
						"seq_items",
						"public",
						"postgres",
						"YES",
						"YES",
						"account_id",
						"public",
						"postgres",
						"seq_accounts",
						"id",
					}},
				},
			},
		},
	})
}
