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

// TestCreateForeignDataWrapperPersistsCatalogRepro reproduces an FDW metadata
// persistence bug: PostgreSQL stores created foreign-data wrappers in
// pg_foreign_data_wrapper.
func TestCreateForeignDataWrapperPersistsCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE FOREIGN DATA WRAPPER populates pg_foreign_data_wrapper",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE FOREIGN DATA WRAPPER fdw_catalog_repro;`,
				},
				{
					Query: `SELECT fdwname
						FROM pg_catalog.pg_foreign_data_wrapper
						WHERE fdwname = 'fdw_catalog_repro';`, PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-data-wrapper-repro-test-testcreateforeigndatawrapperpersistscatalogrepro-0001-select-fdwname-from-pg_catalog.pg_foreign_data_wrapper-where"},
				},
			},
		},
	})
}

// TestCreateForeignServerRequiresExistingWrapperRepro reproduces an FDW
// catalog consistency bug: PostgreSQL supports CREATE SERVER and validates that
// the referenced foreign-data wrapper exists.
func TestCreateForeignServerRequiresExistingWrapperRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE SERVER validates referenced foreign data wrapper",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE SERVER server_missing_fdw_repro FOREIGN DATA WRAPPER missing_server_fdw_repro;`, PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-data-wrapper-repro-test-testcreateforeignserverrequiresexistingwrapperrepro-0001-create-server-server_missing_fdw_repro-foreign-data", Compare: "sqlstate"},
				},
			},
		},
	})
}

// TestAlterAndDropForeignServerRequireExistingServerRepro reproduces an FDW
// catalog consistency bug: PostgreSQL supports ALTER/DROP SERVER and validates
// that the target server exists.
func TestAlterAndDropForeignServerRequireExistingServerRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER and DROP SERVER validate target server",
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SERVER missing_alter_server_repro VERSION '2';`, PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-data-wrapper-repro-test-testalteranddropforeignserverrequireexistingserverrepro-0001-alter-server-missing_alter_server_repro-version-2", Compare: "sqlstate"},
				},
				{
					Query: `DROP SERVER missing_drop_server_repro;`, PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-data-wrapper-repro-test-testalteranddropforeignserverrequireexistingserverrepro-0002-drop-server-missing_drop_server_repro", Compare: "sqlstate"},
				},
			},
		},
	})
}

// TestCreateUserMappingRequiresExistingServerRepro reproduces an FDW catalog
// consistency bug: PostgreSQL supports CREATE USER MAPPING and validates that
// the referenced foreign server exists.
func TestCreateUserMappingRequiresExistingServerRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE USER MAPPING validates referenced foreign server",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE USER MAPPING FOR CURRENT_USER SERVER missing_mapping_server_repro;`, PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-data-wrapper-repro-test-testcreateusermappingrequiresexistingserverrepro-0001-create-user-mapping-for-current_user", Compare: "sqlstate"},
				},
			},
		},
	})
}

// TestCreateForeignTableRequiresExistingServerRepro reproduces an FDW catalog
// consistency bug: PostgreSQL supports CREATE FOREIGN TABLE and validates that
// the referenced foreign server exists.
func TestCreateForeignTableRequiresExistingServerRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE FOREIGN TABLE validates referenced foreign server",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE FOREIGN TABLE foreign_table_missing_server_repro (id integer) SERVER missing_foreign_table_server_repro;`, PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-data-wrapper-repro-test-testcreateforeigntablerequiresexistingserverrepro-0001-create-foreign-table-foreign_table_missing_server_repro-id", Compare: "sqlstate"},
				},
			},
		},
	})
}

// TestDropForeignTableRequiresExistingRelationRepro reproduces an FDW catalog
// consistency bug: PostgreSQL supports DROP FOREIGN TABLE and validates that
// the target foreign table exists.
func TestDropForeignTableRequiresExistingRelationRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP FOREIGN TABLE validates target relation",
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP FOREIGN TABLE missing_foreign_table_repro;`, PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-data-wrapper-repro-test-testdropforeigntablerequiresexistingrelationrepro-0001-drop-foreign-table-missing_foreign_table_repro", Compare: "sqlstate"},
				},
			},
		},
	})
}

// TestImportForeignSchemaRequiresExistingServerRepro reproduces an FDW catalog
// consistency bug: PostgreSQL supports IMPORT FOREIGN SCHEMA and validates that
// the referenced foreign server exists.
func TestImportForeignSchemaRequiresExistingServerRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "IMPORT FOREIGN SCHEMA validates referenced foreign server",
			Assertions: []ScriptTestAssertion{
				{
					Query: `IMPORT FOREIGN SCHEMA remote_schema FROM SERVER missing_import_schema_server_repro INTO public;`, PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-data-wrapper-repro-test-testimportforeignschemarequiresexistingserverrepro-0001-import-foreign-schema-remote_schema-from", Compare: "sqlstate"},
				},
			},
		},
	})
}

// TestAlterAndDropForeignDataWrapperRequireExistingWrapperRepro reproduces an
// FDW catalog consistency bug: PostgreSQL supports ALTER/DROP FOREIGN DATA
// WRAPPER and validates that the target wrapper exists.
func TestAlterAndDropForeignDataWrapperRequireExistingWrapperRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER and DROP FOREIGN DATA WRAPPER validate target wrapper",
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER FOREIGN DATA WRAPPER missing_alter_fdw_repro OPTIONS (ADD host 'localhost');`, PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-data-wrapper-repro-test-testalteranddropforeigndatawrapperrequireexistingwrapperrepro-0001-alter-foreign-data-wrapper-missing_alter_fdw_repro", Compare: "sqlstate"},
				},
				{
					Query: `DROP FOREIGN DATA WRAPPER missing_drop_fdw_repro;`, PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-data-wrapper-repro-test-testalteranddropforeigndatawrapperrequireexistingwrapperrepro-0002-drop-foreign-data-wrapper-missing_drop_fdw_repro", Compare: "sqlstate"},
				},
			},
		},
	})
}

// TestAlterAndDropUserMappingRequireExistingServerRepro reproduces an FDW
// catalog consistency bug: PostgreSQL supports ALTER/DROP USER MAPPING and
// validates that the referenced server exists.
func TestAlterAndDropUserMappingRequireExistingServerRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER and DROP USER MAPPING validate referenced server",
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER USER MAPPING FOR CURRENT_USER SERVER missing_alter_mapping_server_repro OPTIONS (ADD user 'u');`, PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-data-wrapper-repro-test-testalteranddropusermappingrequireexistingserverrepro-0001-alter-user-mapping-for-current_user", Compare: "sqlstate"},
				},
				{
					Query: `DROP USER MAPPING FOR CURRENT_USER SERVER missing_drop_mapping_server_repro;`, PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-data-wrapper-repro-test-testalteranddropusermappingrequireexistingserverrepro-0002-drop-user-mapping-for-current_user", Compare: "sqlstate"},
				},
			},
		},
	})
}

// TestAlterForeignTableRequiresExistingRelationRepro reproduces an FDW catalog
// consistency bug: PostgreSQL supports ALTER FOREIGN TABLE and validates that
// the target foreign table exists.
func TestAlterForeignTableRequiresExistingRelationRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER FOREIGN TABLE validates target relation",
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER FOREIGN TABLE missing_alter_foreign_table_repro OPTIONS (ADD host 'localhost');`, PostgresOracle: ScriptTestPostgresOracle{ID: "foreign-data-wrapper-repro-test-testalterforeigntablerequiresexistingrelationrepro-0001-alter-foreign-table-missing_alter_foreign_table_repro-options", Compare: "sqlstate"},
				},
			},
		},
	})
}
