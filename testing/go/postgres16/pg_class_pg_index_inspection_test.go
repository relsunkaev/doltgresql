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

// TestPgClassPgIndexInspection pins low-level catalog inspection that
// admin scripts and migration tools issue against pg_class and
// pg_index. The patterns covered here are the ones that surface in
// "is the table there?", "what indexes exist on it?", and "is this
// index unique / primary?" workflows. Per the Dump/admin/tooling TODO
// in docs/app-compatibility-checklist.md.
func TestPgClassPgIndexInspection(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_class table existence and relkind",
			SetUpScript: []string{
				`CREATE TABLE public.widgets (id INT PRIMARY KEY, label TEXT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// "Does the table exist?" - the canonical PG
					// pattern uses pg_class joined to pg_namespace.
					Query: `SELECT count(*)::text
						FROM pg_class c
						JOIN pg_namespace n ON c.relnamespace = n.oid
						WHERE c.relname = 'widgets'
							AND n.nspname = 'public'
							AND c.relkind = 'r';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-pg-index-inspection-test-testpgclasspgindexinspection-0001-select-count-*-::text-from"},
				},
			},
		},
		{
			Name: "pg_index reports primary-key and unique flags",
			SetUpScript: []string{
				`CREATE TABLE accounts (
					id INT PRIMARY KEY,
					email TEXT,
					phone TEXT
				);`,
				`CREATE UNIQUE INDEX accounts_email_uidx ON accounts (email);`,
				`CREATE INDEX accounts_phone_idx ON accounts (phone);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// PK index: indisprimary=true, indisunique=true.
					Query: `SELECT count(*)::text
						FROM pg_index i
						JOIN pg_class c ON i.indexrelid = c.oid
						WHERE c.relname = 'accounts_pkey'
							AND i.indisprimary = true
							AND i.indisunique = true;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-pg-index-inspection-test-testpgclasspgindexinspection-0002-select-count-*-::text-from"},
				},
				{
					// Non-PK unique index: indisprimary=false,
					// indisunique=true.
					Query: `SELECT count(*)::text
						FROM pg_index i
						JOIN pg_class c ON i.indexrelid = c.oid
						WHERE c.relname = 'accounts_email_uidx'
							AND i.indisprimary = false
							AND i.indisunique = true;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-pg-index-inspection-test-testpgclasspgindexinspection-0003-select-count-*-::text-from"},
				},
				{
					// Non-unique secondary: indisunique=false.
					Query: `SELECT count(*)::text
						FROM pg_index i
						JOIN pg_class c ON i.indexrelid = c.oid
						WHERE c.relname = 'accounts_phone_idx'
							AND i.indisunique = false;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-pg-index-inspection-test-testpgclasspgindexinspection-0004-select-count-*-::text-from"},
				},
			},
		},
		{
			Name: "pg_class -> pg_index join enumerates indexes per table",
			SetUpScript: []string{
				`CREATE TABLE orders (
					id INT PRIMARY KEY,
					customer_id INT,
					placed_at TIMESTAMP
				);`,
				`CREATE INDEX orders_customer_idx ON orders (customer_id);`,
				`CREATE INDEX orders_placed_idx ON orders (placed_at);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// Three indexes total: PK + two secondary.
					Query: `SELECT count(*)::text
						FROM pg_index i
						JOIN pg_class t ON i.indrelid = t.oid
						WHERE t.relname = 'orders';`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-pg-index-inspection-test-testpgclasspgindexinspection-0005-select-count-*-::text-from"},
				},
				{
					// Index names round-trip via pg_class for the
					// indexrelid side.
					Query: `SELECT ic.relname
						FROM pg_index i
						JOIN pg_class t  ON i.indrelid  = t.oid
						JOIN pg_class ic ON i.indexrelid = ic.oid
						WHERE t.relname = 'orders'
						ORDER BY ic.relname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-class-pg-index-inspection-test-testpgclasspgindexinspection-0006-select-ic.relname-from-pg_index-i"},
				},
			},
		},
	})
}
