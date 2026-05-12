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

	"github.com/dolthub/go-mysql-server/sql"
)

// TestCreateConversionPersistsPgConversionRepro reproduces a catalog
// persistence gap: PostgreSQL persists CREATE CONVERSION metadata in
// pg_conversion.
func TestCreateConversionPersistsPgConversionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE CONVERSION populates pg_conversion",
			SetUpScript: []string{
				`CREATE CONVERSION custom_latin1_to_utf8
					FOR 'LATIN1' TO 'UTF8'
					FROM iso8859_1_to_utf8;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT conname
						FROM pg_catalog.pg_conversion
						WHERE conname = 'custom_latin1_to_utf8';`,
					Expected: []sql.Row{{"custom_latin1_to_utf8"}},
				},
			},
		},
	})
}

// TestCreateConversionRequiresSchemaCreatePrivilegeRepro reproduces a security
// bug: PostgreSQL requires CREATE privilege on the target schema for CREATE
// CONVERSION.
func TestCreateConversionRequiresSchemaCreatePrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE CONVERSION requires schema CREATE privilege",
			SetUpScript: []string{
				`CREATE USER conversion_creator PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE CONVERSION unauthorized_latin1_to_utf8
						FOR 'LATIN1' TO 'UTF8'
						FROM iso8859_1_to_utf8;`,
					ExpectedErr: `permission denied`,
					Username:    `conversion_creator`,
					Password:    `pw`,
				},
			},
		},
	})
}

// TestDropConversionRequiresOwnershipRepro reproduces a security bug:
// PostgreSQL requires conversion ownership to drop a conversion.
func TestDropConversionRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP CONVERSION requires ownership",
			SetUpScript: []string{
				`CREATE USER conversion_dropper PASSWORD 'pw';`,
				`CREATE CONVERSION owner_private_latin1_to_utf8
					FOR 'LATIN1' TO 'UTF8'
					FROM iso8859_1_to_utf8;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP CONVERSION owner_private_latin1_to_utf8;`,
					ExpectedErr: `must be owner`,
					Username:    `conversion_dropper`,
					Password:    `pw`,
				},
				{
					Query: `SELECT conname
						FROM pg_catalog.pg_conversion
						WHERE conname = 'owner_private_latin1_to_utf8';`,
					Expected: []sql.Row{{"owner_private_latin1_to_utf8"}},
				},
			},
		},
	})
}

// TestDropConversionIfExistsMissingRepro reproduces a compatibility gap:
// PostgreSQL accepts DROP CONVERSION IF EXISTS for absent conversions.
func TestDropConversionIfExistsMissingRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP CONVERSION IF EXISTS missing conversion succeeds",
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP CONVERSION IF EXISTS missing_conversion_repro;`,
				},
			},
		},
	})
}

// TestAlterConversionMissingReachesValidationRepro reproduces a compatibility
// gap: ALTER CONVERSION should parse and validate that the target conversion
// exists.
func TestAlterConversionMissingReachesValidationRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER CONVERSION missing target reaches validation",
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER CONVERSION missing_conversion_repro RENAME TO renamed_conversion_repro;`,
					ExpectedErr: `does not exist`,
				},
			},
		},
	})
}
