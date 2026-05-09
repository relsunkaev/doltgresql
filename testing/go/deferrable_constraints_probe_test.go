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

// TestDeferrableConstraintsProbe pins how DEFERRABLE FK DDL behaves
// today so we know exactly what shapes silently fall through to
// immediate-check semantics versus what hard-rejects. PG semantics:
// `DEFERRABLE INITIALLY DEFERRED` defers FK validation to commit time;
// `SET CONSTRAINTS ALL DEFERRED` toggles at runtime. Per the
// Schema/DDL TODO in docs/app-compatibility-checklist.md.
func TestDeferrableConstraintsProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "DEFERRABLE keyword acceptance probe",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					// We expect this to either land cleanly or reject
					// with a clear error. ExpectedErr matches a
					// substring; if both work the test must be
					// updated to assert the correct semantics.
					Query: `CREATE TABLE parent (id INT PRIMARY KEY);`,
				},
				{
					Query: `CREATE TABLE child (
						id INT PRIMARY KEY,
						parent_id INT REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED
					);`,
				},
			},
		},
		{
			// Today: DEFERRABLE INITIALLY DEFERRED is parsed and the
			// table is created, but FK enforcement is still immediate
			// — the violating row is rejected at INSERT time, not at
			// COMMIT. Pin the silent-immediate behavior so the gap is
			// visible. PG-correct semantics would defer enforcement
			// to commit and only error there.
			Name: "DEFERRED FK enforces immediately (residual gap)",
			SetUpScript: []string{
				`CREATE TABLE p (id INT PRIMARY KEY);`,
				`CREATE TABLE c (
					id INT PRIMARY KEY,
					pid INT REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// PG would accept this (deferred until commit)
					// and only reject at COMMIT. Doltgres rejects
					// here, immediate-style. Pin the rejection so the
					// gap stays visible.
					Query:       `INSERT INTO c VALUES (1, 999);`,
					ExpectedErr: "Foreign key violation",
				},
			},
		},
		{
			// SET CONSTRAINTS ALL DEFERRED is the runtime toggle
			// applications use to switch deferrable constraints
			// on/off mid-transaction. Doltgres accepts the statement
			// for dump and migration compatibility, but the
			// enforcement mode remains immediate as pinned above.
			Name:        "SET CONSTRAINTS ALL DEFERRED is accepted as no-op",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SET CONSTRAINTS ALL DEFERRED;`,
				},
			},
		},
	})
}
