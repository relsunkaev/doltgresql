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

// TestGistIndexProbe pins the GiST index DDL boundary today.
// Per the Index/planner TODO in
// docs/app-compatibility-checklist.md.
func TestGistIndexProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE INDEX USING gist probe",
			SetUpScript: []string{
				`CREATE TABLE shapes (id INT PRIMARY KEY, geom TEXT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// Today: SQLSTATE 0A000 'index method gist is
					// not yet supported'. Apps that need GiST
					// (geometry, range non-overlap, btree_gist
					// composite uniqueness) must rewrite to btree
					// with a custom unique key or strip the
					// USING gist suffix from the dump.
					Query:       `CREATE INDEX shapes_geom_gist_idx ON shapes USING gist (geom);`,
					ExpectedErr: "index method gist is not yet supported",
				},
			},
		},
	})
}
