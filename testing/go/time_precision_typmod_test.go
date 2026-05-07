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
	"github.com/dolthub/go-mysql-server/sql"
	"testing"
)

// TestTimePrecisionTypmod asserts that TIMESTAMP(p), TIMESTAMPTZ(p),
// TIME(p), and TIMETZ(p) round-trip through pg_attribute.atttypmod.
// Strict-typed ORM bindings (SQLAlchemy, Hibernate) read atttypmod
// to enforce client-side precision validation; introspection tools
// (Drizzle Kit, Prisma db pull) compare it against the migration's
// declared precision when diffing schemas. Without it, a fresh
// migration would always think the column "drifted" and emit
// pointless ALTER TYPE statements.
//
// PostgreSQL stores typmod for these types as 4 + (precision << 16)
// in some encodings and as just the precision in others; what
// pg_attribute returns is the precision directly (per
// information_schema_constraints in real PG). Doltgres' typmod
// helpers in server/types/time.go encode it as the raw precision,
// so that's what we assert against.
func TestTimePrecisionTypmod(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "TIMESTAMP(p) and friends preserve precision in atttypmod",
			SetUpScript: []string{
				`CREATE TABLE prec_t (
					id   INT PRIMARY KEY,
					ts0  TIMESTAMP(0),
					ts3  TIMESTAMP(3),
					ts6  TIMESTAMP(6),
					tsd  TIMESTAMP,
					tz3  TIMESTAMPTZ(3),
					tz6  TIMESTAMPTZ(6),
					t0   TIME(0),
					t3   TIME(3),
					t6   TIME(6)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// Each column's atttypmod must be the precision the
					// user requested. -1 means no precision was given.
					Query: `SELECT attname, atttypmod
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname = 'prec_t' AND n.nspname = 'public' AND a.attnum > 0
ORDER BY a.attnum;`,
					Expected: []sql.Row{
						{"id", int32(-1)},
						{"ts0", int32(0)},
						{"ts3", int32(3)},
						{"ts6", int32(6)},
						{"tsd", int32(-1)},
						{"tz3", int32(3)},
						{"tz6", int32(6)},
						{"t0", int32(0)},
						{"t3", int32(3)},
						{"t6", int32(6)},
					},
				},
			},
		},
		{
			Name: "format_type renders the precision back into the type name",
			SetUpScript: []string{
				`CREATE TABLE prec_fmt (id INT PRIMARY KEY, ts3 TIMESTAMP(3), t6 TIME(6));`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// information_schema.columns / pg_get_expr would
					// rebuild the original DDL from atttypid +
					// atttypmod via format_type. If our typmod is
					// right, format_type should round-trip the
					// precision into the rendered type name.
					Query: `SELECT a.attname, format_type(a.atttypid, a.atttypmod)
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname = 'prec_fmt' AND n.nspname = 'public' AND a.attname IN ('ts3', 't6')
ORDER BY a.attname;`,
					Expected: []sql.Row{
						{"t6", "time(6) without time zone"},
						{"ts3", "timestamp(3) without time zone"},
					},
				},
			},
		},
	})
}
