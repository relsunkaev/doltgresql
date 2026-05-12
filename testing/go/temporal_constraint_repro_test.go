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

import "testing"

// TestPostgres18WithoutOverlapsTemporalUniqueRepro reproduces a PostgreSQL 18
// data-consistency gap: temporal UNIQUE constraints reject overlapping ranges.
func TestPostgres18WithoutOverlapsTemporalUniqueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UNIQUE WITHOUT OVERLAPS rejects overlapping ranges",
			SetUpScript: []string{
				`CREATE TABLE temporal_unique_bookings (
					booked daterange NOT NULL,
					UNIQUE (booked WITHOUT OVERLAPS)
				);`,
				`INSERT INTO temporal_unique_bookings VALUES
					('[2026-01-01,2026-02-01)'::daterange),
					('[2026-02-01,2026-03-01)'::daterange);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `INSERT INTO temporal_unique_bookings VALUES ('[2026-01-15,2026-01-20)'::daterange);`,
					ExpectedErr: `conflicting key value`,
				},
			},
		},
	})
}

// TestPostgres18TemporalForeignKeyPeriodRepro reproduces a PostgreSQL 18
// referential-integrity gap: PERIOD foreign keys require the referenced
// temporal key to cover the referencing row's full period.
func TestPostgres18TemporalForeignKeyPeriodRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "FOREIGN KEY PERIOD requires full temporal coverage",
			SetUpScript: []string{
				`CREATE EXTENSION btree_gist;`,
				`CREATE TABLE temporal_fk_parent (
					product_id INT NOT NULL,
					available daterange NOT NULL,
					PRIMARY KEY (product_id, available WITHOUT OVERLAPS)
				);`,
				`CREATE TABLE temporal_fk_child (
					product_id INT NOT NULL,
					requested daterange NOT NULL,
					FOREIGN KEY (product_id, PERIOD requested)
						REFERENCES temporal_fk_parent (product_id, PERIOD available)
				);`,
				`INSERT INTO temporal_fk_parent VALUES
					(1, '[2026-01-01,2026-02-01)'::daterange),
					(1, '[2026-02-01,2026-03-01)'::daterange);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO temporal_fk_child VALUES
						(1, '[2026-01-15,2026-02-15)'::daterange);`,
				},
				{
					Query:       `INSERT INTO temporal_fk_child VALUES (1, '[2026-03-01,2026-04-01)'::daterange);`,
					ExpectedErr: `violates foreign key constraint`,
				},
			},
		},
	})
}
