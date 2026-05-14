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

// TestInformationSchemaColumnsCollationName mirrors PostgreSQL's
// behavior for information_schema.columns.collation_name:
//
//   - Numeric / temporal / boolean / unspecified-collation string
//     columns report NULL — matching PG, which only populates
//     collation_name when an explicit COLLATE clause was given.
//   - Columns with an explicit COLLATE record the collation name.
//
// The earlier audit asserted that doltgres "returns NULL when real
// PG returns en_US.utf8 etc." — that's only true when the user
// declared an explicit collation. This test pins both the
// always-NULL-for-default case and the populated-for-explicit case
// so a regression in either direction breaks loudly.
func TestInformationSchemaColumnsCollationName(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "collation_name is NULL for default-collated strings and non-strings",
			SetUpScript: []string{
				`CREATE TABLE coll_default (
					id INT PRIMARY KEY,
					s_text TEXT,
					s_varchar VARCHAR(40),
					n NUMERIC,
					ts TIMESTAMP,
					b BOOL
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT column_name, collation_name
FROM information_schema.columns
WHERE table_name = 'coll_default' AND table_schema = 'public'
ORDER BY ordinal_position;`, PostgresOracle: ScriptTestPostgresOracle{ID: "info-schema-collation-test-testinformationschemacolumnscollationname-0001-select-column_name-collation_name-from-information_schema.columns"},
				},
			},
		},
		{
			Name: "explicit COLLATE surfaces collation_name in information_schema.columns",
			SetUpScript: []string{
				`CREATE TABLE coll_explicit (
					id   INT PRIMARY KEY,
					a    TEXT COLLATE "C",
					b    TEXT COLLATE "POSIX",
					def  TEXT COLLATE "default",
					plain TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// Real PG: explicit COLLATE populates
					// collation_name; the catalog default and the
					// no-COLLATE column report NULL.
					Query: `SELECT column_name, collation_name
FROM information_schema.columns
WHERE table_name = 'coll_explicit' AND column_name IN ('a', 'b', 'def', 'plain')
ORDER BY column_name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "info-schema-collation-test-testinformationschemacolumnscollationname-0002-select-column_name-collation_name-from-information_schema.columns"},
				},
			},
		},
	})
}
