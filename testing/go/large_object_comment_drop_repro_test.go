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

// TestLargeObjectUnlinkClearsCommentRepro reproduces a metadata persistence
// bug: lo_unlink does not clear COMMENT ON LARGE OBJECT metadata, so a later
// large object that reuses the same OID inherits the dropped object's comment.
func TestLargeObjectUnlinkClearsCommentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "lo_unlink clears large-object comment before OID reuse",
			SetUpScript: []string{
				`SELECT lo_create(424260);`,
				`COMMENT ON LARGE OBJECT 424260 IS 'old private large object';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT lo_unlink(424260);`, PostgresOracle: ScriptTestPostgresOracle{ID: "large-object-comment-drop-repro-test-testlargeobjectunlinkclearscommentrepro-0001-select-lo_unlink-424260"},
				},
				{
					Query: `SELECT lo_create(424260);`, PostgresOracle: ScriptTestPostgresOracle{ID: "large-object-comment-drop-repro-test-testlargeobjectunlinkclearscommentrepro-0002-select-lo_create-424260"},
				},
				{
					Query: `SELECT description
						FROM pg_catalog.pg_description
						WHERE objoid = 424260
							AND classoid = 'pg_largeobject_metadata'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "large-object-comment-drop-repro-test-testlargeobjectunlinkclearscommentrepro-0003-select-description-from-pg_catalog.pg_description-where"},
				},
			},
		},
	})
}
