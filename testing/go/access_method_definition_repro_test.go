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

// TestCreateAccessMethodPersistsPgAmRepro reproduces a DDL/catalog correctness
// bug: PostgreSQL supports defining table access methods backed by a handler
// function and persists the new method in pg_am.
func TestCreateAccessMethodPersistsPgAmRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE ACCESS METHOD persists pg_am row",
			SetUpScript: []string{
				`CREATE ACCESS METHOD heap_repro_am TYPE TABLE HANDLER heap_tableam_handler;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT amname, amtype
						FROM pg_catalog.pg_am
						WHERE amname = 'heap_repro_am';`, PostgresOracle: ScriptTestPostgresOracle{ID: "access-method-definition-repro-test-testcreateaccessmethodpersistspgamrepro-0001-select-amname-amtype-from-pg_catalog.pg_am"},
				},
			},
		},
	})
}

// TestDropAccessMethodIfExistsMissingNoopsRepro reproduces a DDL correctness
// bug: PostgreSQL accepts DROP ACCESS METHOD IF EXISTS for a missing access
// method as a no-op, which migration tools can emit defensively.
func TestDropAccessMethodIfExistsMissingNoopsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP ACCESS METHOD IF EXISTS missing method no-ops",
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP ACCESS METHOD IF EXISTS missing_repro_am;`,
				},
			},
		},
	})
}
