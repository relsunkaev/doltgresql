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

// TestIndexOpclassesAndNullOrdering pins how far explicit opclass
// declarations and ASC NULLS LAST / DESC NULLS FIRST DDL go today.
// Both are emitted by pg_dump for non-trivial btree definitions and
// by ORM migration tools for sortable indexes. Per the Index/planner
// TODO in docs/app-compatibility-checklist.md.
func TestIndexOpclassesAndNullOrdering(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "explicit text_ops opclass on a text column",
			SetUpScript: []string{
				`CREATE TABLE accounts (id INT PRIMARY KEY, email TEXT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE INDEX accounts_email_text_ops_idx
						ON accounts (email text_ops);`,
				},
				{
					Query:    `SELECT count(*)::text FROM pg_indexes WHERE indexname = 'accounts_email_text_ops_idx';`,
					Expected: []sql.Row{{"1"}},
				},
			},
		},
		{
			Name: "int4_ops opclass on an int column",
			SetUpScript: []string{
				`CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE INDEX orders_customer_int4_ops_idx
						ON orders (customer_id int4_ops);`,
				},
				{
					Query:    `SELECT count(*)::text FROM pg_indexes WHERE indexname = 'orders_customer_int4_ops_idx';`,
					Expected: []sql.Row{{"1"}},
				},
			},
		},
		{
			// PG planner refers to the column-level NULLS LAST /
			// NULLS FIRST ordering when picking a covering index for
			// a sort plan. Pin DDL acceptance so migration tools
			// don't trip; planner-usage parity is a separate gap.
			Name: "NULLS LAST / NULLS FIRST DDL acceptance",
			SetUpScript: []string{
				`CREATE TABLE events (id INT PRIMARY KEY, ts TIMESTAMP);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE INDEX events_ts_desc_idx
						ON events (ts DESC NULLS LAST);`,
				},
				{
					Query: `CREATE INDEX events_ts_asc_idx
						ON events (ts ASC NULLS FIRST);`,
				},
			},
		},
	})
}
