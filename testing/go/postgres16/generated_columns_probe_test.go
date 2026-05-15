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

// TestGeneratedColumnsProbe pins how far PG `GENERATED ALWAYS AS (...)
// STORED` generated-column DDL gets today. Per the Schema/DDL TODO in
// docs/app-compatibility-checklist.md.
func TestGeneratedColumnsProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GENERATED ALWAYS AS ... STORED computes on insert",
			SetUpScript: []string{
				`CREATE TABLE rectangles (
						id INT PRIMARY KEY,
						width INT,
						height INT,
						area INT GENERATED ALWAYS AS (width * height) STORED
					);`,
				`INSERT INTO rectangles (id, width, height) VALUES (1, 4, 5), (2, 2, 3);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, area FROM rectangles ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "generated-columns-probe-test-testgeneratedcolumnsprobe-0001-select-id-area-from-rectangles"},
				},
			},
		},
		{
			// information_schema.columns must surface generated
			// columns so dump tools can reconstruct the DDL.
			Name: "information_schema reports is_generated",
			SetUpScript: []string{
				`CREATE TABLE box (
					id INT PRIMARY KEY,
					side INT,
					area INT GENERATED ALWAYS AS (side * side) STORED
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT column_name, is_generated
						FROM information_schema.columns
						WHERE table_name = 'box'
						ORDER BY ordinal_position;`, PostgresOracle: ScriptTestPostgresOracle{ID: "generated-columns-probe-test-testgeneratedcolumnsprobe-0002-select-column_name-is_generated-from-information_schema.columns"},
				},
			},
		},
		{
			Name: "generated column updates when source columns change",
			SetUpScript: []string{
				`CREATE TABLE prices (
						id INT PRIMARY KEY,
						subtotal INT,
						tax_pct INT,
						total INT GENERATED ALWAYS AS (subtotal + (subtotal * tax_pct) / 100) STORED
					);`,
				`INSERT INTO prices (id, subtotal, tax_pct) VALUES (1, 100, 10);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT total FROM prices WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "generated-columns-probe-test-testgeneratedcolumnsprobe-0003-select-total-from-prices-where"},
				},
				{
					Query: `UPDATE prices SET tax_pct = 20 WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "generated-columns-probe-test-testgeneratedcolumnsprobe-0004-update-prices-set-tax_pct-="},
				},
				{
					Query: `SELECT total FROM prices WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "generated-columns-probe-test-testgeneratedcolumnsprobe-0005-select-total-from-prices-where"},
				},
			},
		},
	})
}
