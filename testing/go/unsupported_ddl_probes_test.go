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
)

// TestUnsupportedDdlProbes pins the rejection contracts for DDL
// shapes that real PG dumps and migrations emit but are not yet
// supported in doltgresql. Pinning the rejection contracts means
// that dump-rewrite tooling has a stable error string to filter on,
// and that any future incidental support that "starts working"
// without the engineering to back it surfaces as a test break.
// Per the Schema/DDL TODO in
// docs/app-compatibility-checklist.md.
func TestUnsupportedDdlProbes(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "CREATE AGGREGATE is rejected",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE AGGREGATE my_sum (int) (
						sfunc = int4pl,
						stype = int
					);`,
					ExpectedErr: "CREATE AGGREGATE is not yet supported",
				},
			},
		},
		{
			// EXCLUDE constraints on a table (e.g. EXCLUDE USING gist)
			// are how PG enforces non-overlapping ranges; not
			// supported today.
			Name:        "EXCLUDE constraint via CREATE TABLE is rejected",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE bookings (
						id INT PRIMARY KEY,
						room_id INT,
						period TEXT,
						EXCLUDE USING gist (room_id WITH =, period WITH &&)
					);`,
					ExpectedErr: "EXCLUDE constraints are not yet supported",
				},
			},
		},
		{
			// Transition tables are PostgreSQL-only AFTER-trigger
			// state. BEFORE triggers must still reject REFERENCING.
			Name: "BEFORE trigger with REFERENCING NEW TABLE is rejected",
			SetUpScript: []string{
				`CREATE TABLE t (id INT PRIMARY KEY, v INT);`,
				`CREATE FUNCTION audit_fn() RETURNS trigger AS $$
					BEGIN
						RETURN NULL;
					END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TRIGGER tg
						BEFORE INSERT ON t
						REFERENCING NEW TABLE AS new_rows
						FOR EACH STATEMENT EXECUTE FUNCTION audit_fn();`,
					ExpectedErr: "transition tables are only supported for AFTER triggers",
				},
			},
		},
		{
			// Event triggers fire on DDL statements.
			Name:        "CREATE EVENT TRIGGER is rejected",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE EVENT TRIGGER ddl_audit
						ON ddl_command_end
						EXECUTE FUNCTION audit_fn();`,
					ExpectedErr: "permission denied to create event trigger",
				},
			},
		},
	})
}
