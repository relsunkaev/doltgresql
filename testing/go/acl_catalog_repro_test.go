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

// TestGrantTablePrivilegesPopulatePgClassRelaclRepro reproduces a catalog
// persistence bug: explicit table grants should be reflected in pg_class.relacl.
func TestGrantTablePrivilegesPopulatePgClassRelaclRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT table privileges populate pg_class relacl",
			SetUpScript: []string{
				`CREATE ROLE acl_catalog_reader;`,
				`CREATE TABLE acl_catalog_target (id INT PRIMARY KEY);`,
				`GRANT SELECT ON acl_catalog_target TO acl_catalog_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT relacl IS NOT NULL
						FROM pg_catalog.pg_class
						WHERE oid = 'acl_catalog_target'::regclass;`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
	})
}

// TestGrantSchemaPrivilegesPopulatePgNamespaceNspaclRepro reproduces a catalog
// persistence bug: explicit schema grants should be reflected in
// pg_namespace.nspacl.
func TestGrantSchemaPrivilegesPopulatePgNamespaceNspaclRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT schema privileges populate pg_namespace nspacl",
			SetUpScript: []string{
				`CREATE ROLE acl_schema_reader;`,
				`CREATE SCHEMA acl_schema_target;`,
				`GRANT USAGE ON SCHEMA acl_schema_target TO acl_schema_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT nspacl IS NOT NULL
						FROM pg_catalog.pg_namespace
						WHERE nspname = 'acl_schema_target';`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
	})
}

// TestGrantDatabasePrivilegesPopulatePgDatabaseDataclRepro reproduces a catalog
// persistence bug: explicit database grants should be reflected in
// pg_database.datacl.
func TestGrantDatabasePrivilegesPopulatePgDatabaseDataclRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT database privileges populate pg_database datacl",
			SetUpScript: []string{
				`CREATE ROLE acl_database_reader;`,
				`GRANT CONNECT ON DATABASE postgres TO acl_database_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT datacl IS NOT NULL
						FROM pg_catalog.pg_database
						WHERE datname = 'postgres';`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
	})
}

// TestGrantSequencePrivilegesPopulatePgClassRelaclRepro reproduces a catalog
// persistence bug: explicit sequence grants should be reflected in
// pg_class.relacl.
func TestGrantSequencePrivilegesPopulatePgClassRelaclRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT sequence privileges populate pg_class relacl",
			SetUpScript: []string{
				`CREATE ROLE acl_sequence_reader;`,
				`CREATE SEQUENCE acl_catalog_sequence;`,
				`GRANT USAGE ON SEQUENCE acl_catalog_sequence TO acl_sequence_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT relacl IS NOT NULL
						FROM pg_catalog.pg_class
						WHERE oid = 'acl_catalog_sequence'::regclass;`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
	})
}

// TestPrivilegeInquiryFunctionsReflectColumnGrantsRepro reproduces an ACL
// correctness bug: PostgreSQL privilege inquiry helpers report table and
// column privileges according to explicit grants.
func TestPrivilegeInquiryFunctionsReflectColumnGrantsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "privilege inquiry functions reflect column grants",
			SetUpScript: []string{
				`CREATE ROLE privilege_inquiry_reader;`,
				`CREATE TABLE privilege_inquiry_private (
					id INT,
					secret TEXT
				);`,
				`GRANT SELECT (id) ON privilege_inquiry_private TO privilege_inquiry_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
							has_table_privilege('privilege_inquiry_reader', 'privilege_inquiry_private', 'SELECT'),
							has_column_privilege('privilege_inquiry_reader', 'privilege_inquiry_private', 'id', 'SELECT'),
							has_column_privilege('privilege_inquiry_reader', 'privilege_inquiry_private', 'secret', 'SELECT');`,
					Expected: []sql.Row{{false, true, false}},
				},
			},
		},
	})
}

// TestPrivilegeInquiryFunctionsReflectObjectGrantsRepro reproduces an ACL
// correctness bug: PostgreSQL privilege inquiry helpers report explicit grants
// across database, schema, sequence, and function object types.
func TestPrivilegeInquiryFunctionsReflectObjectGrantsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "privilege inquiry functions reflect object grants",
			SetUpScript: []string{
				`CREATE ROLE privilege_inquiry_object_user;`,
				`CREATE SCHEMA privilege_inquiry_schema;`,
				`CREATE SEQUENCE privilege_inquiry_sequence;`,
				`CREATE FUNCTION privilege_inquiry_function() RETURNS INT
					LANGUAGE SQL AS 'SELECT 1';`,
				`GRANT CONNECT ON DATABASE postgres TO privilege_inquiry_object_user;`,
				`GRANT USAGE ON SCHEMA privilege_inquiry_schema TO privilege_inquiry_object_user;`,
				`GRANT USAGE ON SEQUENCE privilege_inquiry_sequence TO privilege_inquiry_object_user;`,
				`GRANT EXECUTE ON FUNCTION privilege_inquiry_function() TO privilege_inquiry_object_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
							has_database_privilege('privilege_inquiry_object_user', 'postgres', 'CONNECT'),
							has_schema_privilege('privilege_inquiry_object_user', 'privilege_inquiry_schema', 'USAGE'),
							has_sequence_privilege('privilege_inquiry_object_user', 'privilege_inquiry_sequence', 'USAGE'),
							has_function_privilege('privilege_inquiry_object_user', 'privilege_inquiry_function()', 'EXECUTE');`,
					Expected: []sql.Row{{true, true, true, true}},
				},
			},
		},
	})
}
