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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCopyFromServerFileRequiresPrivilegeRepro reproduces a security bug:
// Doltgres lets a normal role read a server-side file through COPY FROM when
// PostgreSQL requires elevated server-file privileges.
func TestCopyFromServerFileRequiresPrivilegeRepro(t *testing.T) {
	copyPath := filepath.Join(t.TempDir(), "copy_server_file.csv")
	require.NoError(t, os.WriteFile(copyPath, []byte("1,loaded\n"), 0644))
	escapedPath := strings.ReplaceAll(copyPath, "'", "''")

	RunScripts(t, []ScriptTest{
		{
			Name: "COPY FROM server file requires server-file privilege",
			SetUpScript: []string{
				`CREATE TABLE copy_file_private (id INT PRIMARY KEY, label TEXT);`,
				`CREATE USER copy_file_reader PASSWORD 'pw';`,
				`GRANT USAGE ON SCHEMA public TO copy_file_reader;`,
				`GRANT INSERT ON copy_file_private TO copy_file_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: fmt.Sprintf(
						`COPY copy_file_private (id, label) FROM '%s' (FORMAT CSV);`,
						escapedPath,
					),
					ExpectedErr: "pg_read_server_files",
					Username:    "copy_file_reader",
					Password:    "pw",
				},
			},
		},
	})
}

// TestCopyFromServerFileRejectsRelativePathRepro reproduces a server-file
// security/correctness bug: PostgreSQL requires COPY FROM server files to use
// an absolute path instead of resolving relative paths against the server
// process working directory.
func TestCopyFromServerFileRejectsRelativePathRepro(t *testing.T) {
	relativePath := "copy_relative_server_file_repro.csv"
	require.NoError(t, os.WriteFile(relativePath, []byte("1,relative\n"), 0644))
	t.Cleanup(func() {
		_ = os.Remove(relativePath)
	})

	RunScripts(t, []ScriptTest{
		{
			Name: "COPY FROM server file rejects relative path",
			SetUpScript: []string{
				`CREATE TABLE copy_relative_file_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       fmt.Sprintf(`COPY copy_relative_file_items FROM '%s' WITH (FORMAT CSV);`, relativePath),
					ExpectedErr: `relative path not allowed`,
				},
				{
					Query: `SELECT count(*) FROM copy_relative_file_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "copy-server-file-privilege-repro-test-testcopyfromserverfilerejectsrelativepathrepro-0002-select-count-*-from-copy_relative_file_items"},
				},
			},
		},
	})
}

// TestCopyToServerFileRequiresPrivilegeRepro reproduces a COPY privilege
// semantics bug: PostgreSQL parses server-file COPY TO and then requires
// pg_write_server_files or superuser privileges before a role can write files.
func TestCopyToServerFileRequiresPrivilegeRepro(t *testing.T) {
	copyPath := filepath.Join(t.TempDir(), "copy_server_file_out.csv")
	escapedPath := strings.ReplaceAll(copyPath, "'", "''")

	RunScripts(t, []ScriptTest{
		{
			Name: "COPY TO server file requires server-file privilege",
			SetUpScript: []string{
				`CREATE TABLE copy_file_write_private (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO copy_file_write_private VALUES (1, 'secret');`,
				`CREATE USER copy_file_writer PASSWORD 'pw';`,
				`GRANT USAGE ON SCHEMA public TO copy_file_writer;`,
				`GRANT SELECT ON copy_file_write_private TO copy_file_writer;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: fmt.Sprintf(
						`COPY copy_file_write_private (id, label) TO '%s' WITH (FORMAT CSV);`,
						escapedPath,
					),
					ExpectedErr: "pg_write_server_files",
					Username:    "copy_file_writer",
					Password:    "pw",
				},
			},
		},
	})
}

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
					Password: "pw", PostgresOracle: ScriptTestPostgresOracle{

						// TestCopyToProgramRequiresPrivilegeRepro reproduces a COPY privilege
						// semantics bug: PostgreSQL parses COPY TO PROGRAM and requires
						// pg_execute_server_program or superuser privileges before executing a server
						// program.
						ID: "copy-server-file-privilege-repro-test-testcopyfromprogramrequiresprivilegerepro-0001-copy-copy_program_private-id-label-from", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCopyToProgramRequiresPrivilegeRepro(t *testing.T) {
	copyPath := filepath.Join(t.TempDir(), "copy_program_out.csv")
	escapedPath := strings.ReplaceAll(copyPath, "'", "'\"'\"'")

	RunScripts(t, []ScriptTest{
		{
			Name: "COPY TO PROGRAM requires server-program privilege",
			SetUpScript: []string{
				`CREATE TABLE copy_program_write_private (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO copy_program_write_private VALUES (1, 'secret');`,
				`CREATE USER copy_program_writer PASSWORD 'pw';`,
				`GRANT USAGE ON SCHEMA public TO copy_program_writer;`,
				`GRANT SELECT ON copy_program_write_private TO copy_program_writer;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: fmt.Sprintf(
						`COPY copy_program_write_private (id, label) TO PROGRAM 'cat > ''%s''' WITH (FORMAT CSV);`,
						escapedPath,
					),
					ExpectedErr: "pg_execute_server_program",
					Username:    "copy_program_writer",
					Password:    "pw",
				},
			},
		},
	})
}
