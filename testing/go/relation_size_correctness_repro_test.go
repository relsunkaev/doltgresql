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

// TestPgRelationSizeRejectsInvalidForkRepro reproduces an admin/runtime helper
// compatibility gap: PostgreSQL rejects unknown relation fork names instead of
// silently returning a size.
func TestPgRelationSizeRejectsInvalidForkRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_relation_size rejects invalid fork names",
			SetUpScript: []string{
				`CREATE TABLE relation_size_fork_items (id INT PRIMARY KEY);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `SELECT pg_relation_size('relation_size_fork_items'::regclass, 'badfork');`,
					ExpectedErr: `invalid fork name`,
				},
			},
		},
	})
}
