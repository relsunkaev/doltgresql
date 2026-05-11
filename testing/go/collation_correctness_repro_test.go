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

// TestIcuCollationUniqueConstraintUsesCollationEqualityRepro reproduces a
// collation correctness bug: PostgreSQL supports nondeterministic ICU
// collations, and unique constraints over those collations use collation
// equality rather than bytewise text equality.
func TestIcuCollationUniqueConstraintUsesCollationEqualityRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ICU collation unique constraint uses collation equality",
			SetUpScript: []string{
				`CREATE COLLATION case_insensitive_unique (
					provider = icu,
					locale = 'und-u-ks-level2',
					deterministic = false
				);`,
				`CREATE TABLE collation_unique_items (
					label TEXT COLLATE case_insensitive_unique UNIQUE
				);`,
				`INSERT INTO collation_unique_items VALUES ('abc');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `INSERT INTO collation_unique_items VALUES ('ABC');`,
					ExpectedErr: `duplicate key value violates unique constraint`,
				},
			},
		},
	})
}
