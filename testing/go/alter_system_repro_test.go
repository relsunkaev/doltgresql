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
					Query:       `ALTER SYSTEM SET work_mem = '64kB';`,
					ExpectedErr: `ALTER SYSTEM cannot run inside a transaction block`,
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
					Query:       `ALTER SYSTEM RESET work_mem;`,
					ExpectedErr: `ALTER SYSTEM cannot run inside a transaction block`,
				},
				{
					Query: `ROLLBACK;`,
				},
			},
		},
	})
}
