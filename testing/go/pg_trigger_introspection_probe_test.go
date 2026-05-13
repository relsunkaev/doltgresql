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

// TestPgTriggerIntrospectionProbe pins what `pg_trigger` and
// `information_schema.triggers` return for triggers defined via
// CREATE TRIGGER. Migration tools (drizzle-kit, prisma db pull,
// Alembic autogenerate) read these views to recover trigger
// definitions during introspection. Per the Schema/DDL TODO in
// docs/app-compatibility-checklist.md.
func TestPgTriggerIntrospectionProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_trigger surfaces a CREATE TRIGGER row",
			SetUpScript: []string{
				`CREATE TABLE main (id INT PRIMARY KEY, v INT);`,
				`CREATE TABLE audit_log (id SERIAL PRIMARY KEY, main_id INT);`,
				`CREATE FUNCTION audit_main_insert() RETURNS trigger AS $$
					BEGIN
						INSERT INTO audit_log (main_id) VALUES (NEW.id);
						RETURN NEW;
					END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER tg_audit_main
					AFTER INSERT ON main
					FOR EACH ROW EXECUTE FUNCTION audit_main_insert();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT tgname, tgtype::int, tgenabled, tgisinternal,
							tgnargs::int, pg_get_triggerdef(oid) <> ''
						FROM pg_trigger
						WHERE tgname = 'tg_audit_main';`,
					Expected: []sql.Row{{
						"tg_audit_main",
						int32(5), // row-level AFTER INSERT
						"O",
						"f",
						int32(0),
						"t",
					}},
				},
				{
					Query: `SELECT trigger_name, event_manipulation,
							(event_object_schema = current_schema())::text, event_object_table,
							action_timing, action_orientation
						FROM information_schema.triggers
						WHERE trigger_name = 'tg_audit_main';`,
					Expected: []sql.Row{{
						"tg_audit_main",
						"INSERT",
						"true",
						"main",
						"AFTER",
						"ROW",
					}},
				},
				{
					Query: `SELECT relhastriggers
						FROM pg_class
						WHERE relname = 'main';`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query: `SELECT hastriggers
						FROM pg_tables
						WHERE schemaname = current_schema()
							AND tablename = 'main';`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
	})
}
