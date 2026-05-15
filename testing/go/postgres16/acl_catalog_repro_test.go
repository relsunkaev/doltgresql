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
						WHERE oid = 'acl_catalog_target'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "acl-catalog-repro-test-testgranttableprivilegespopulatepgclassrelaclrepro-0001-select-relacl-is-not-null"},
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
						WHERE nspname = 'acl_schema_target';`, PostgresOracle: ScriptTestPostgresOracle{ID: "acl-catalog-repro-test-testgrantschemaprivilegespopulatepgnamespacenspaclrepro-0001-select-nspacl-is-not-null"},
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
						WHERE datname = 'postgres';`, PostgresOracle: ScriptTestPostgresOracle{ID: "acl-catalog-repro-test-testgrantdatabaseprivilegespopulatepgdatabasedataclrepro-0001-select-datacl-is-not-null"},
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
						WHERE oid = 'acl_catalog_sequence'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "acl-catalog-repro-test-testgrantsequenceprivilegespopulatepgclassrelaclrepro-0001-select-relacl-is-not-null"},
				},
			},
		},
	})
}

// TestGrantLanguagePrivilegesPopulatePgLanguageLanaclRepro reproduces a catalog
// persistence bug: explicit procedural-language grants should be reflected in
// pg_language.lanacl.
func TestGrantLanguagePrivilegesPopulatePgLanguageLanaclRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT language privileges populate pg_language lanacl",
			SetUpScript: []string{
				`CREATE ROLE acl_language_user;`,
				`GRANT USAGE ON LANGUAGE plpgsql TO acl_language_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT lanacl::text LIKE '%acl_language_user=U/%'
						FROM pg_catalog.pg_language
						WHERE lanname = 'plpgsql';`, PostgresOracle: ScriptTestPostgresOracle{ID: "acl-catalog-repro-test-testgrantlanguageprivilegespopulatepglanguagelanaclrepro-0001-select-lanacl::text-like-%acl_language_user=u/%-from"},
				},
			},
		},
	})
}

// TestGrantFunctionPrivilegesPopulatePgProcProaclRepro reproduces a catalog
// persistence bug: explicit routine grants should be reflected in pg_proc.proacl.
func TestGrantFunctionPrivilegesPopulatePgProcProaclRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT function privileges populate pg_proc proacl",
			SetUpScript: []string{
				`CREATE ROLE acl_function_user;`,
				`CREATE FUNCTION acl_function_target() RETURNS INT
					LANGUAGE SQL AS 'SELECT 42';`,
				`GRANT EXECUTE ON FUNCTION acl_function_target() TO acl_function_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT proacl::text LIKE '%acl_function_user=X/%'
						FROM pg_catalog.pg_proc
						WHERE proname = 'acl_function_target';`, PostgresOracle: ScriptTestPostgresOracle{ID: "acl-catalog-repro-test-testgrantfunctionprivilegespopulatepgprocproaclrepro-0001-select-proacl::text-like-%acl_function_user=x/%-from"},
				},
			},
		},
	})
}

// TestGrantTypePrivilegesPopulatePgTypeTypaclRepro reproduces a type privilege
// compatibility gap: explicit type grants should be accepted and reflected in
// pg_type.typacl.
func TestGrantTypePrivilegesPopulatePgTypeTypaclRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT type privileges populate pg_type typacl",
			SetUpScript: []string{
				`CREATE ROLE acl_type_user;`,
				`CREATE TYPE acl_type_mood AS ENUM ('ok', 'sad');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT USAGE ON TYPE acl_type_mood TO acl_type_user;`,
				},
				{
					Query: `SELECT typacl::text LIKE '%acl_type_user=U/%'
						FROM pg_catalog.pg_type
						WHERE typname = 'acl_type_mood';`, PostgresOracle: ScriptTestPostgresOracle{ID: "acl-catalog-repro-test-testgranttypeprivilegespopulatepgtypetypaclrepro-0001-select-typacl::text-like-%acl_type_user=u/%-from"},
				},
			},
		},
	})
}

// TestPgGetAclReflectsTableGrantRepro reproduces an ACL helper parity gap:
// pg_get_acl should expose the effective ACL for a catalog object.
func TestPgGetAclReflectsTableGrantRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_get_acl reflects table grants",
			SetUpScript: []string{
				`CREATE ROLE pg_get_acl_reader;`,
				`CREATE TABLE pg_get_acl_target (id INT PRIMARY KEY);`,
				`GRANT SELECT ON pg_get_acl_target TO pg_get_acl_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_get_acl(
							'pg_class'::regclass,
							'pg_get_acl_target'::regclass,
							0
						) IS NOT NULL;`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
	})
}

// TestHasTypePrivilegeHelperRepro reproduces an ACL helper parity gap:
// PostgreSQL exposes has_type_privilege for checking type USAGE privileges.
func TestHasTypePrivilegeHelperRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "has_type_privilege reports built-in type usage",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT has_type_privilege('pg_catalog.int4', 'USAGE');`, PostgresOracle: ScriptTestPostgresOracle{ID: "acl-catalog-repro-test-testhastypeprivilegehelperrepro-0001-select-has_type_privilege-pg_catalog.int4-usage"},
				},
			},
		},
	})
}

// TestHasAnyColumnPrivilegeHelperRepro reproduces an ACL helper parity gap:
// PostgreSQL exposes has_any_column_privilege for checking whether any column
// on a relation has a requested privilege.
func TestHasAnyColumnPrivilegeHelperRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "has_any_column_privilege reflects column grants",
			SetUpScript: []string{
				`CREATE ROLE any_column_privilege_reader;`,
				`CREATE TABLE any_column_privilege_private (
					id INT,
					secret TEXT
				);`,
				`GRANT SELECT (id) ON any_column_privilege_private
					TO any_column_privilege_reader;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT has_any_column_privilege(
							'any_column_privilege_reader',
							'any_column_privilege_private',
							'SELECT'
						);`, PostgresOracle: ScriptTestPostgresOracle{ID: "acl-catalog-repro-test-testhasanycolumnprivilegehelperrepro-0001-select-has_any_column_privilege-any_column_privilege_reader-any_column_privilege_private-select"},
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
							has_column_privilege('privilege_inquiry_reader', 'privilege_inquiry_private', 'secret', 'SELECT');`, PostgresOracle: ScriptTestPostgresOracle{ID: "acl-catalog-repro-test-testprivilegeinquiryfunctionsreflectcolumngrantsrepro-0001-select-has_table_privilege-privilege_inquiry_reader-privilege_inquiry_private-select"},
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
							has_function_privilege('privilege_inquiry_object_user', 'privilege_inquiry_function()', 'EXECUTE');`, PostgresOracle: ScriptTestPostgresOracle{ID: "acl-catalog-repro-test-testprivilegeinquiryfunctionsreflectobjectgrantsrepro-0001-select-has_database_privilege-privilege_inquiry_object_user-postgres-connect"},
				},
			},
		},
	})
}
