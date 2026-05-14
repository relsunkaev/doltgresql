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

// TestHasLargeObjectPrivilegeHelperRepro reproduces a PostgreSQL 18 ACL helper
// parity gap: has_largeobject_privilege should be registered for checking
// SELECT and UPDATE privileges on large objects.
func TestHasLargeObjectPrivilegeHelperRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "has_largeobject_privilege helper is registered",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) >= 1
						FROM pg_catalog.pg_proc
						WHERE proname = 'has_largeobject_privilege';`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/acl-catalog-repro-test-testhaslargeobjectprivilegehelperrepro-0001-select-count-*->=-1"},
				},
			},
		},
	})
}
