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

// TestInfoSchemaColumnsOrdering pins the column-order queries pg_dump
// and ORM introspection tools (drizzle-kit, prisma db pull, Alembic
// autogenerate) issue against information_schema.columns to recover
// physical column ordering, nullability, default expressions, and
// data-type descriptions. Per the Dump/admin/tooling TODO in
// docs/app-compatibility-checklist.md.
func TestInfoSchemaColumnsOrdering(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ordinal_position reflects DDL column order",
			SetUpScript: []string{
				`CREATE TABLE invoices (
					id INT PRIMARY KEY,
					customer TEXT,
					amount NUMERIC(12,2),
					currency VARCHAR(3),
					due_date DATE
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// pg_dump-style ordering query: rebuild the
					// CREATE TABLE shape by walking
					// information_schema.columns in ordinal_position
					// order.
					Query: `SELECT column_name, ordinal_position::text
						FROM information_schema.columns
						WHERE table_name = 'invoices'
						ORDER BY ordinal_position;`,
					Expected: []sql.Row{
						{"id", "1"},
						{"customer", "2"},
						{"amount", "3"},
						{"currency", "4"},
						{"due_date", "5"},
					},
				},
			},
		},
		{
			Name: "is_nullable reports YES/NO accurately",
			SetUpScript: []string{
				`CREATE TABLE accounts (
					id INT PRIMARY KEY,
					email TEXT NOT NULL,
					nickname TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT column_name, is_nullable
						FROM information_schema.columns
						WHERE table_name = 'accounts'
						ORDER BY ordinal_position;`,
					Expected: []sql.Row{
						{"id", "NO"},
						{"email", "NO"},
						{"nickname", "YES"},
					},
				},
			},
		},
		{
			Name: "data_type column reports PG type names",
			SetUpScript: []string{
				`CREATE TABLE shapes (
					id INT PRIMARY KEY,
					label TEXT,
					qty INT,
					price NUMERIC(10,2),
					ts TIMESTAMP,
					code VARCHAR(8)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT column_name, data_type
						FROM information_schema.columns
						WHERE table_name = 'shapes'
						ORDER BY ordinal_position;`,
					Expected: []sql.Row{
						{"id", "integer"},
						{"label", "text"},
						{"qty", "integer"},
						{"price", "numeric"},
						{"ts", "timestamp without time zone"},
						{"code", "character varying"},
					},
				},
			},
		},
		{
			Name: "array columns report ARRAY data type and array udt name",
			SetUpScript: []string{
				`CREATE TABLE array_shapes (
					id INT PRIMARY KEY,
					tags TEXT[],
					scores INT[]
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT column_name, data_type, udt_name
						FROM information_schema.columns
						WHERE table_name = 'array_shapes'
						ORDER BY ordinal_position;`,
					Expected: []sql.Row{
						{"id", "integer", "int4"},
						{"tags", "ARRAY", "_text"},
						{"scores", "ARRAY", "_int4"},
					},
				},
			},
		},
		{
			// pg_dump and Alembic autogenerate inspect column_default
			// to reconstruct DEFAULT clauses. Constants and
			// expression defaults must be visible.
			Name: "column_default surfaces literal and expression defaults",
			SetUpScript: []string{
				`CREATE TABLE flags (
					id INT PRIMARY KEY,
					active BOOL DEFAULT true,
					ttl INT DEFAULT 7,
					created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT column_name,
						(column_default IS NOT NULL)::text AS has_default
						FROM information_schema.columns
						WHERE table_name = 'flags'
						ORDER BY ordinal_position;`,
					Expected: []sql.Row{
						{"id", "false"},
						{"active", "true"},
						{"ttl", "true"},
						{"created_at", "true"},
					},
				},
			},
		},
	})
}
