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

// TestPgReadAllDataRoleIncludesSchemaUsageRepro reproduces a predefined-role
// privilege bug: pg_read_all_data should include USAGE on every schema, so a
// member can read private-schema tables without a separate schema grant.
func TestPgReadAllDataRoleIncludesSchemaUsageRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_read_all_data includes schema usage",
			SetUpScript: []string{
				`CREATE USER read_all_schema_user PASSWORD 'pw';`,
				`CREATE SCHEMA read_all_private_schema;`,
				`CREATE TABLE read_all_private_schema.private_items (
					id INT PRIMARY KEY,
					secret TEXT
				);`,
				`INSERT INTO read_all_private_schema.private_items VALUES (1, 'visible without schema grant');`,
				`GRANT pg_read_all_data TO read_all_schema_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, secret
						FROM read_all_private_schema.private_items;`,
					Expected: []sql.Row{{1, "visible without schema grant"}},
					Username: `read_all_schema_user`,
					Password: `pw`,
				},
			},
		},
	})
}

// TestPgWriteAllDataRoleIncludesSchemaUsageRepro reproduces a predefined-role
// privilege bug: pg_write_all_data should include USAGE on every schema, so a
// member can write private-schema tables without a separate schema grant.
func TestPgWriteAllDataRoleIncludesSchemaUsageRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_write_all_data includes schema usage",
			SetUpScript: []string{
				`CREATE USER write_all_schema_user PASSWORD 'pw';`,
				`CREATE SCHEMA write_all_private_schema;`,
				`CREATE TABLE write_all_private_schema.private_items (
					id INT PRIMARY KEY,
					secret TEXT
				);`,
				`GRANT pg_write_all_data TO write_all_schema_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO write_all_private_schema.private_items
						VALUES (1, 'written without schema grant');`,
					Username: `write_all_schema_user`,
					Password: `pw`,
				},
				{
					Query: `SELECT id, secret
						FROM write_all_private_schema.private_items;`,
					Expected: []sql.Row{{1, "written without schema grant"}},
				},
			},
		},
	})
}

// TestPgReadAllDataRoleAllowsSequenceReadsRepro reproduces a predefined-role
// privilege bug: pg_read_all_data should grant SELECT-style access to sequences
// as well as tables.
func TestPgReadAllDataRoleAllowsSequenceReadsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_read_all_data allows sequence reads",
			SetUpScript: []string{
				`CREATE USER read_all_sequence_user PASSWORD 'pw';`,
				`CREATE SCHEMA read_all_sequence_schema;`,
				`CREATE SEQUENCE read_all_sequence_schema.private_seq START 50;`,
				`GRANT pg_read_all_data TO read_all_sequence_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT last_value::text
						FROM read_all_sequence_schema.private_seq;`,
					Expected: []sql.Row{{"50"}},
					Username: `read_all_sequence_user`,
					Password: `pw`,
				},
			},
		},
	})
}

// TestPgWriteAllDataRoleAllowsSequenceWritesRepro reproduces a predefined-role
// privilege bug: pg_write_all_data should grant write access to sequences, so a
// member can advance a private sequence without a per-sequence grant.
func TestPgWriteAllDataRoleAllowsSequenceWritesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_write_all_data allows sequence writes",
			SetUpScript: []string{
				`CREATE USER write_all_sequence_user PASSWORD 'pw';`,
				`CREATE SCHEMA write_all_sequence_schema;`,
				`CREATE SEQUENCE write_all_sequence_schema.private_seq START 70;`,
				`GRANT pg_write_all_data TO write_all_sequence_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT nextval('write_all_sequence_schema.private_seq')::text;`,
					Expected: []sql.Row{{"70"}},
					Username: `write_all_sequence_user`,
					Password: `pw`,
				},
			},
		},
	})
}
