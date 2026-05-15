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

// TestAlterSystemInsideTransactionRejectedRepro reproduces an admin correctness
// bug: PostgreSQL parses ALTER SYSTEM and rejects it inside transaction blocks
// before writing postgresql.auto.conf.
func TestAlterSystemInsideTransactionRejectedRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SYSTEM rejects inside transaction block",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `ALTER SYSTEM SET work_mem = '64kB';`,

					PostgresOracle: ScriptTestPostgresOracle{ID: "alter-system-repro-test-testaltersysteminsidetransactionrejectedrepro-0001-alter-system-set-work_mem-=", Compare: "sqlstate", Cleanup: []string{"ROLLBACK;"}},
				},
				{
					Query: `ROLLBACK;`,
				},
			},
		},
		{
			Name: "ALTER SYSTEM RESET rejects inside transaction block",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `ALTER SYSTEM RESET work_mem;`,

					PostgresOracle: ScriptTestPostgresOracle{ID: "alter-system-repro-test-testaltersysteminsidetransactionrejectedrepro-0002-alter-system-reset-work_mem", Compare: "sqlstate", Cleanup: []string{"ROLLBACK;"}},
				},
				{
					Query: `ROLLBACK;`,
				},
			},
		},
	})
}
