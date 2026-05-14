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

package postgres18

import (
	. "github.com/dolthub/doltgresql/testing/go"
	"testing"
)

// TestPostgres18VacuumAnalyzeOnlyInheritanceTargetRepro reproduces a
// PostgreSQL 18 compatibility gap: VACUUM and ANALYZE can target only the
// named inheritance parent with ONLY.
func TestPostgres18VacuumAnalyzeOnlyInheritanceTargetRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "VACUUM and ANALYZE accept ONLY inheritance targets",
			SetUpScript: []string{
				`CREATE TABLE maintenance_only_parent (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE TABLE maintenance_only_child (
					extra TEXT
				) INHERITS (maintenance_only_parent);`,
				`INSERT INTO maintenance_only_parent VALUES (1, 'parent');`,
				`INSERT INTO maintenance_only_child VALUES (2, 'child', 'extra');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `VACUUM ONLY maintenance_only_parent;`,
				},
				{
					Query: `ANALYZE ONLY maintenance_only_parent;`,
				},
			},
		},
	})
}
