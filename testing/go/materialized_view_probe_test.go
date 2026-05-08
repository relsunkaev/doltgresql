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

// TestMaterializedViewProbe pins where CREATE MATERIALIZED VIEW
// stands in doltgresql today. Per the Schema/DDL TODO in
// docs/app-compatibility-checklist.md.
func TestMaterializedViewProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE MATERIALIZED VIEW probe",
			SetUpScript: []string{
				`CREATE TABLE source (id INT PRIMARY KEY, v INT);`,
				`INSERT INTO source VALUES (1, 100), (2, 200);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// PG: creates a populated matview. Today:
					// SQLSTATE 0A000 feature_not_supported.
					Query:       `CREATE MATERIALIZED VIEW source_mv AS SELECT id, v FROM source;`,
					ExpectedErr: "CREATE MATERIALIZED VIEW is not yet supported",
				},
			},
		},
	})
}
