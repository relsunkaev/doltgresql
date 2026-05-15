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

// TestCopyFromProgramRequiresPrivilegeRepro reproduces a COPY privilege
// semantics bug: PostgreSQL parses COPY FROM PROGRAM and requires
// pg_execute_server_program or superuser privileges before executing a server
// program.
func TestCopyFromProgramRequiresPrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COPY FROM PROGRAM requires server-program privilege",
			SetUpScript: []string{
				`CREATE TABLE copy_program_private (id INT PRIMARY KEY, label TEXT);`,
				`CREATE USER copy_program_reader PASSWORD 'pw';`,
				`GRANT USAGE ON SCHEMA public TO copy_program_reader;`,
				`GRANT INSERT ON copy_program_private TO copy_program_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `COPY copy_program_private (id, label) FROM PROGRAM 'printf ''1,program\n''' WITH (FORMAT CSV);`,

					Username: "copy_program_reader",
					Password: "pw", PostgresOracle: ScriptTestPostgresOracle{ID: "copy-server-file-privilege-repro-test-testcopyfromprogramrequiresprivilegerepro-0001-copy-copy_program_private-id-label-from", Compare: "sqlstate"},

					// TestCopyToProgramRequiresPrivilegeRepro reproduces a COPY privilege
					// semantics bug: PostgreSQL parses COPY TO PROGRAM and requires
					// pg_execute_server_program or superuser privileges before executing a server
					// program.

				},
			},
		},
	})
}
