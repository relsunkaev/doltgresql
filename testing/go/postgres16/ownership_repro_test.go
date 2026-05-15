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

// TestCreateTableOwnerUpdatesCatalogRepro reproduces a security/catalog bug:
// Doltgres creates tables for non-superuser roles but pg_class.relowner remains
// postgres instead of the creating role.
func TestCreateTableOwnerUpdatesCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE updates pg_class relowner",
			SetUpScript: []string{
				`CREATE USER table_catalog_creator PASSWORD 'pw';`,
				`GRANT USAGE, CREATE ON SCHEMA public TO table_catalog_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `CREATE TABLE created_table_catalog (id INT PRIMARY KEY);`,
					Username: `table_catalog_creator`,
					Password: `pw`,
				},
				{
					Query: `SELECT pg_get_userbyid(relowner)
						FROM pg_class
						WHERE relname = 'created_table_catalog';`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testcreatetableownerupdatescatalogrepro-0001-select-pg_get_userbyid-relowner-from-pg_class"},
				},
			},
		},
	})
}

// TestTableOwnerCanUseCreatedTableRepro reproduces an ownership privilege bug:
// a role that creates a table owns it and should be able to use it without
// explicit table grants.
func TestTableOwnerCanUseCreatedTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "table owner can use created table",
			SetUpScript: []string{
				`CREATE USER table_owner_user PASSWORD 'pw';`,
				`GRANT USAGE, CREATE ON SCHEMA public TO table_owner_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `CREATE TABLE owner_created_table (id INT PRIMARY KEY, label TEXT);`,
					Username: `table_owner_user`,
					Password: `pw`,
				},
				{
					Query:    `INSERT INTO owner_created_table VALUES (1, 'owned');`,
					Username: `table_owner_user`,
					Password: `pw`,
				},
				{
					Query: `SELECT id, label FROM owner_created_table;`,

					Username: `table_owner_user`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestCreateSequenceOwnerUpdatesCatalogRepro reproduces a security/catalog bug:
						// Doltgres creates sequences for non-superuser roles but pg_class.relowner
						// remains postgres instead of the creating role.
						ID: "ownership-repro-test-testtableownercanusecreatedtablerepro-0001-select-id-label-from-owner_created_table", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCreateSequenceOwnerUpdatesCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE SEQUENCE updates pg_class relowner",
			SetUpScript: []string{
				`CREATE USER sequence_catalog_creator PASSWORD 'pw';`,
				`GRANT USAGE, CREATE ON SCHEMA public TO sequence_catalog_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `CREATE SEQUENCE created_sequence_catalog;`,
					Username: `sequence_catalog_creator`,
					Password: `pw`,
				},
				{
					Query: `SELECT pg_get_userbyid(relowner)
						FROM pg_class
						WHERE relname = 'created_sequence_catalog';`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testcreatesequenceownerupdatescatalogrepro-0001-select-pg_get_userbyid-relowner-from-pg_class"},
				},
			},
		},
	})
}

// TestCreateViewOwnerUpdatesCatalogRepro reproduces a security/catalog bug:
// Doltgres creates views for non-superuser roles but pg_class.relowner remains
// postgres instead of the creating role.
func TestCreateViewOwnerUpdatesCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE VIEW updates pg_class relowner",
			SetUpScript: []string{
				`CREATE USER view_catalog_creator PASSWORD 'pw';`,
				`CREATE TABLE view_catalog_source (id INT PRIMARY KEY);`,
				`GRANT USAGE, CREATE ON SCHEMA public TO view_catalog_creator;`,
				`GRANT SELECT ON view_catalog_source TO view_catalog_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `CREATE VIEW created_view_catalog AS SELECT id FROM view_catalog_source;`,
					Username: `view_catalog_creator`,
					Password: `pw`,
				},
				{
					Query: `SELECT pg_get_userbyid(relowner)
						FROM pg_class
						WHERE relname = 'created_view_catalog';`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testcreateviewownerupdatescatalogrepro-0001-select-pg_get_userbyid-relowner-from-pg_class"},
				},
			},
		},
	})
}

// TestViewOwnerCanUseCreatedViewRepro reproduces a view ownership privilege bug:
// a role that creates a view owns it and should be able to select from it.
func TestViewOwnerCanUseCreatedViewRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "view owner can use created view",
			SetUpScript: []string{
				`CREATE USER view_owner_user PASSWORD 'pw';`,
				`CREATE TABLE view_owner_source (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO view_owner_source VALUES (1, 'visible');`,
				`GRANT USAGE, CREATE ON SCHEMA public TO view_owner_user;`,
				`GRANT SELECT ON view_owner_source TO view_owner_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE VIEW owner_created_view AS
						SELECT id, label FROM view_owner_source;`,
					Username: `view_owner_user`,
					Password: `pw`,
				},
				{
					Query: `SELECT id, label FROM owner_created_view;`,

					Username: `view_owner_user`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestCreateMaterializedViewOwnerUpdatesCatalogRepro reproduces a
						// security/catalog bug: Doltgres creates materialized views for non-superuser
						// roles but pg_class.relowner remains postgres instead of the creating role.
						ID: "ownership-repro-test-testviewownercanusecreatedviewrepro-0001-select-id-label-from-owner_created_view", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCreateMaterializedViewOwnerUpdatesCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE MATERIALIZED VIEW updates pg_class relowner",
			SetUpScript: []string{
				`CREATE USER materialized_view_catalog_creator PASSWORD 'pw';`,
				`CREATE TABLE materialized_view_catalog_source (id INT PRIMARY KEY);`,
				`INSERT INTO materialized_view_catalog_source VALUES (1);`,
				`GRANT USAGE, CREATE ON SCHEMA public TO materialized_view_catalog_creator;`,
				`GRANT SELECT ON materialized_view_catalog_source TO materialized_view_catalog_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `CREATE MATERIALIZED VIEW created_materialized_view_catalog AS SELECT id FROM materialized_view_catalog_source;`,
					Username: `materialized_view_catalog_creator`,
					Password: `pw`,
				},
				{
					Query: `SELECT pg_get_userbyid(relowner)
						FROM pg_class
						WHERE relname = 'created_materialized_view_catalog';`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testcreatematerializedviewownerupdatescatalogrepro-0001-select-pg_get_userbyid-relowner-from-pg_class"},
				},
			},
		},
	})
}

// TestMaterializedViewOwnerCanUseCreatedMaterializedViewRepro reproduces a
// materialized-view ownership privilege bug: a role that creates a materialized
// view owns it and should be able to read and refresh it.
func TestMaterializedViewOwnerCanUseCreatedMaterializedViewRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "materialized view owner can use created materialized view",
			SetUpScript: []string{
				`CREATE USER materialized_view_owner_user PASSWORD 'pw';`,
				`CREATE TABLE materialized_view_owner_source (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO materialized_view_owner_source VALUES (1, 'initial');`,
				`GRANT USAGE, CREATE ON SCHEMA public TO materialized_view_owner_user;`,
				`GRANT SELECT ON materialized_view_owner_source TO materialized_view_owner_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE MATERIALIZED VIEW owner_created_materialized_view AS
						SELECT id, label FROM materialized_view_owner_source;`,
					Username: `materialized_view_owner_user`,
					Password: `pw`,
				},
				{
					Query: `SELECT id, label FROM owner_created_materialized_view;`,

					Username: `materialized_view_owner_user`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testmaterializedviewownercanusecreatedmaterializedviewrepro-0001-select-id-label-from-owner_created_materialized_view", Compare:

					// TestCreateSchemaOwnerUpdatesCatalogRepro reproduces a security/catalog bug:
					// Doltgres creates schemas for non-superuser roles but pg_namespace.nspowner
					// remains postgres instead of the creating role.
					"sqlstate"},
				},
				{
					Query:    `REFRESH MATERIALIZED VIEW owner_created_materialized_view;`,
					Username: `materialized_view_owner_user`,
					Password: `pw`,
				},
			},
		},
	})
}

func TestCreateSchemaOwnerUpdatesCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE SCHEMA updates pg_namespace nspowner",
			SetUpScript: []string{
				`CREATE USER schema_catalog_creator PASSWORD 'pw';`,
				`GRANT CREATE ON DATABASE postgres TO schema_catalog_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `CREATE SCHEMA created_schema_catalog;`,
					Username: `schema_catalog_creator`,
					Password: `pw`,
				},
				{
					Query: `SELECT pg_get_userbyid(nspowner)
						FROM pg_namespace
						WHERE nspname = 'created_schema_catalog';`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testcreateschemaownerupdatescatalogrepro-0001-select-pg_get_userbyid-nspowner-from-pg_namespace"},
				},
			},
		},
	})
}

// TestSchemaOwnerCanUseCreatedSchemaRepro reproduces a schema ownership
// privilege bug: a role that creates a schema owns it and should be able to
// create objects in it.
func TestSchemaOwnerCanUseCreatedSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "schema owner can use created schema",
			SetUpScript: []string{
				`CREATE USER schema_owner_user PASSWORD 'pw';`,
				`GRANT CREATE ON DATABASE postgres TO schema_owner_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `CREATE SCHEMA owner_created_schema;`,
					Username: `schema_owner_user`,
					Password: `pw`,
				},
				{
					Query:    `CREATE TABLE owner_created_schema.owned_schema_table (id INT PRIMARY KEY);`,
					Username: `schema_owner_user`,
					Password: `pw`,
				},
				{
					Query:    `INSERT INTO owner_created_schema.owned_schema_table VALUES (1);`,
					Username: `schema_owner_user`,
					Password: `pw`,
				},
				{
					Query: `SELECT id FROM owner_created_schema.owned_schema_table;`,

					Username: `schema_owner_user`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestCreateTypeOwnerUpdatesCatalogRepro reproduces a security/catalog bug:
						// Doltgres creates types for non-superuser roles but pg_type.typowner remains
						// postgres instead of the creating role.
						ID: "ownership-repro-test-testschemaownercanusecreatedschemarepro-0001-select-id-from-owner_created_schema.owned_schema_table", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCreateTypeOwnerUpdatesCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TYPE updates pg_type typowner",
			SetUpScript: []string{
				`CREATE USER type_catalog_creator PASSWORD 'pw';`,
				`GRANT USAGE, CREATE ON SCHEMA public TO type_catalog_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `CREATE TYPE created_enum_catalog AS ENUM ('one', 'two');`,
					Username: `type_catalog_creator`,
					Password: `pw`,
				},
				{
					Query: `SELECT pg_get_userbyid(typowner)
						FROM pg_type
						WHERE typname = 'created_enum_catalog';`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testcreatetypeownerupdatescatalogrepro-0001-select-pg_get_userbyid-typowner-from-pg_type"},
				},
			},
		},
	})
}

// TestTypeOwnerCanUseCreatedTypeGuard covers type ownership privileges: a role
// that creates an enum type owns it and should be able to use it.
func TestTypeOwnerCanUseCreatedTypeGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "type owner can use created type",
			SetUpScript: []string{
				`CREATE USER type_owner_user PASSWORD 'pw';`,
				`GRANT USAGE, CREATE ON SCHEMA public TO type_owner_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `CREATE TYPE owner_created_enum AS ENUM ('one', 'two');`,
					Username: `type_owner_user`,
					Password: `pw`,
				},
				{
					Query: `SELECT 'one'::owner_created_enum::text;`,

					Username: `type_owner_user`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestCreateDomainOwnerUpdatesCatalogRepro reproduces a security/catalog bug:
						// Doltgres creates domains for non-superuser roles but pg_type.typowner remains
						// postgres instead of the creating role.
						ID: "ownership-repro-test-testtypeownercanusecreatedtypeguard-0001-select-one-::owner_created_enum::text"},
				},
			},
		},
	})
}

func TestCreateDomainOwnerUpdatesCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE DOMAIN updates pg_type typowner",
			SetUpScript: []string{
				`CREATE USER domain_catalog_creator PASSWORD 'pw';`,
				`GRANT USAGE, CREATE ON SCHEMA public TO domain_catalog_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `CREATE DOMAIN created_domain_catalog AS TEXT;`,
					Username: `domain_catalog_creator`,
					Password: `pw`,
				},
				{
					Query: `SELECT pg_get_userbyid(typowner)
						FROM pg_type
						WHERE typname = 'created_domain_catalog';`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testcreatedomainownerupdatescatalogrepro-0001-select-pg_get_userbyid-typowner-from-pg_type"},
				},
			},
		},
	})
}

// TestDomainOwnerCanUseCreatedDomainGuard covers domain ownership privileges:
// a role that creates a domain owns it and should be able to use it without an
// explicit type USAGE grant.
func TestDomainOwnerCanUseCreatedDomainGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "domain owner can use created domain",
			SetUpScript: []string{
				`CREATE USER domain_owner_user PASSWORD 'pw';`,
				`GRANT USAGE, CREATE ON SCHEMA public TO domain_owner_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `CREATE DOMAIN owner_created_domain AS INT CHECK (VALUE > 0);`,
					Username: `domain_owner_user`,
					Password: `pw`,
				},
				{
					Query: `SELECT 7::owner_created_domain;`,

					Username: `domain_owner_user`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestAlterTableOwnerUpdatesCatalogRepro reproduces a security/catalog bug:
						// Doltgres accepts ALTER TABLE ... OWNER TO, but pg_class.relowner remains the
						// original owner instead of the requested role.
						ID: "ownership-repro-test-testdomainownercanusecreateddomainguard-0001-select-7::owner_created_domain"},
				},
			},
		},
	})
}

func TestAlterTableOwnerUpdatesCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE OWNER TO updates pg_class relowner",
			SetUpScript: []string{
				`CREATE ROLE new_owner;`,
				`CREATE TABLE owned_catalog (id INT PRIMARY KEY);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE owned_catalog OWNER TO new_owner;`,
				},
				{
					Query: `SELECT pg_get_userbyid(relowner)
						FROM pg_class
						WHERE relname = 'owned_catalog';`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testaltertableownerupdatescatalogrepro-0001-select-pg_get_userbyid-relowner-from-pg_class"},
				},
			},
		},
	})
}

// TestAlterTableOwnerCanUseTransferredTableRepro reproduces an ownership
// privilege bug: after ALTER TABLE ... OWNER TO, the transferred owner should
// be able to use the table without explicit table grants.
func TestAlterTableOwnerCanUseTransferredTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE OWNER TO lets transferred owner use table",
			SetUpScript: []string{
				`CREATE USER transferred_table_owner PASSWORD 'pw';`,
				`CREATE TABLE transferred_table_runtime (id INT PRIMARY KEY, label TEXT);`,
				`GRANT USAGE ON SCHEMA public TO transferred_table_owner;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE transferred_table_runtime OWNER TO transferred_table_owner;`,
				},
				{
					Query:    `INSERT INTO transferred_table_runtime VALUES (1, 'owned');`,
					Username: `transferred_table_owner`,
					Password: `pw`,
				},
				{
					Query: `SELECT id, label FROM transferred_table_runtime;`,

					Username: `transferred_table_owner`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestCreateDatabaseOwnerUpdatesCatalogRepro reproduces a security/catalog bug:
						// Doltgres accepts CREATE DATABASE ... OWNER, but pg_database.datdba does not
						// record the requested owner.
						ID: "ownership-repro-test-testaltertableownercanusetransferredtablerepro-0001-select-id-label-from-transferred_table_runtime"},
				},
			},
		},
	})
}

func TestCreateDatabaseOwnerUpdatesCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE DATABASE OWNER updates pg_database datdba",
			SetUpScript: []string{
				`CREATE ROLE db_catalog_owner;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE DATABASE owned_database_catalog OWNER = db_catalog_owner;`,
				},
				{
					Query: `SELECT pg_get_userbyid(datdba)
						FROM pg_database
						WHERE datname = 'owned_database_catalog';`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testcreatedatabaseownerupdatescatalogrepro-0001-select-pg_get_userbyid-datdba-from-pg_database"},
				},
			},
		},
	})
}

// TestCreateDatabaseOwnerRequiresExistingRoleRepro reproduces a
// security/catalog bug: Doltgres accepts CREATE DATABASE ... OWNER for a role
// that does not exist.
func TestCreateDatabaseOwnerRequiresExistingRoleRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE DATABASE OWNER requires existing role",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE DATABASE missing_owner_database OWNER = missing_database_owner;`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testcreatedatabaseownerrequiresexistingrolerepro-0001-create-database-missing_owner_database-owner-=",

						// TestAlterDatabaseOwnerUpdatesCatalogRepro reproduces a security/catalog bug:
						// Doltgres accepts ALTER DATABASE ... OWNER TO, but pg_database.datdba does not
						// record the requested owner.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestAlterDatabaseOwnerUpdatesCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER DATABASE OWNER TO updates pg_database datdba",
			SetUpScript: []string{
				`CREATE ROLE db_alter_catalog_owner;`,
				`CREATE DATABASE alter_owned_database_catalog;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER DATABASE alter_owned_database_catalog OWNER TO db_alter_catalog_owner;`,
				},
				{
					Query: `SELECT pg_get_userbyid(datdba)
						FROM pg_database
						WHERE datname = 'alter_owned_database_catalog';`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testalterdatabaseownerupdatescatalogrepro-0001-select-pg_get_userbyid-datdba-from-pg_database"},
				},
			},
		},
	})
}

// TestAlterViewOwnerUpdatesCatalogRepro reproduces a security/catalog bug:
// Doltgres accepts ALTER VIEW ... OWNER TO, but pg_class.relowner remains the
// original owner instead of the requested role.
func TestAlterViewOwnerUpdatesCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER VIEW OWNER TO updates pg_class relowner",
			SetUpScript: []string{
				`CREATE ROLE view_owner;`,
				`CREATE TABLE view_owner_base (id INT PRIMARY KEY);`,
				`CREATE VIEW owned_view_catalog AS SELECT id FROM view_owner_base;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER VIEW owned_view_catalog OWNER TO view_owner;`,
				},
				{
					Query: `SELECT pg_get_userbyid(relowner)
						FROM pg_class
						WHERE relname = 'owned_view_catalog';`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testalterviewownerupdatescatalogrepro-0001-select-pg_get_userbyid-relowner-from-pg_class"},
				},
			},
		},
	})
}

// TestAlterViewOwnerCanUseTransferredViewRepro reproduces an ownership
// privilege bug: after ALTER VIEW ... OWNER TO, the transferred owner should
// be able to select from the view without explicit view grants.
func TestAlterViewOwnerCanUseTransferredViewRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER VIEW OWNER TO lets transferred owner use view",
			SetUpScript: []string{
				`CREATE USER transferred_view_owner PASSWORD 'pw';`,
				`CREATE TABLE transferred_view_source (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO transferred_view_source VALUES (1, 'visible');`,
				`CREATE VIEW transferred_view_runtime AS
					SELECT id, label FROM transferred_view_source;`,
				`GRANT USAGE ON SCHEMA public TO transferred_view_owner;`,
				`GRANT SELECT ON transferred_view_source TO transferred_view_owner;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER VIEW transferred_view_runtime OWNER TO transferred_view_owner;`,
				},
				{
					Query: `SELECT id, label FROM transferred_view_runtime;`,

					Username: `transferred_view_owner`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestAlterOwnerRequiresExistingRoleRepro reproduces owner-validation bugs:
						// PostgreSQL rejects ALTER ... OWNER TO when the target role does not exist.
						ID: "ownership-repro-test-testalterviewownercanusetransferredviewrepro-0001-select-id-label-from-transferred_view_runtime"},
				},
			},
		},
	})
}

func TestAlterOwnerRequiresExistingRoleRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER object OWNER TO requires existing role",
			SetUpScript: []string{
				`CREATE TABLE missing_owner_table (id INT PRIMARY KEY);`,
				`CREATE VIEW missing_owner_view AS SELECT id FROM missing_owner_table;`,
				`CREATE SEQUENCE missing_owner_sequence;`,
				`CREATE SCHEMA missing_owner_schema;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE missing_owner_table OWNER TO missing_alter_owner;`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testalterownerrequiresexistingrolerepro-0001-alter-table-missing_owner_table-owner-to", Compare: "sqlstate"},
				},
				{
					Query: `ALTER VIEW missing_owner_view OWNER TO missing_alter_owner;`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testalterownerrequiresexistingrolerepro-0002-alter-view-missing_owner_view-owner-to", Compare: "sqlstate"},
				},
				{
					Query: `ALTER SEQUENCE missing_owner_sequence OWNER TO missing_alter_owner;`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testalterownerrequiresexistingrolerepro-0003-alter-sequence-missing_owner_sequence-owner-to", Compare: "sqlstate"},
				},
				{
					Query: `ALTER SCHEMA missing_owner_schema OWNER TO missing_alter_owner;`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testalterownerrequiresexistingrolerepro-0004-alter-schema-missing_owner_schema-owner-to",

						// TestDropOwnedRevokesGrantedPrivilegesRepro reproduces a privilege-cleanup
						// bug: PostgreSQL's DROP OWNED BY also revokes privileges granted to the target
						// role on objects it does not own.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestDropOwnedRevokesGrantedPrivilegesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP OWNED revokes granted table privileges",
			SetUpScript: []string{
				`CREATE USER drop_owned_grantee PASSWORD 'pw';`,
				`CREATE TABLE public.drop_owned_grants (id INT PRIMARY KEY);`,
				`INSERT INTO public.drop_owned_grants VALUES (1);`,
				`GRANT SELECT ON public.drop_owned_grants TO drop_owned_grantee;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id FROM public.drop_owned_grants;`,

					Username: `drop_owned_grantee`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testdropownedrevokesgrantedprivilegesrepro-0001-select-id-from-drop_owned_grants", Compare: "sqlstate"},
				},
				{
					Query: `DROP OWNED BY drop_owned_grantee;`,
				},
				{
					Query: `SELECT id FROM public.drop_owned_grants;`,

					Username: `drop_owned_grantee`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestReassignOwnedByEmptyRoleRepro reproduces an ownership-admin correctness
						// bug: PostgreSQL accepts REASSIGN OWNED BY even when the source role currently
						// owns no objects.
						ID: "ownership-repro-test-testdropownedrevokesgrantedprivilegesrepro-0002-select-id-from-drop_owned_grants", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestReassignOwnedByEmptyRoleRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REASSIGN OWNED accepts empty source role",
			SetUpScript: []string{
				`CREATE ROLE reassign_empty_old;`,
				`CREATE ROLE reassign_empty_new;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `REASSIGN OWNED BY reassign_empty_old TO reassign_empty_new;`,
				},
			},
		},
	})
}

// TestAlterMaterializedViewOwnerUpdatesCatalogRepro reproduces a
// security/catalog bug: PostgreSQL accepts ALTER MATERIALIZED VIEW ... OWNER TO
// and records the requested owner in pg_class.relowner.
func TestAlterMaterializedViewOwnerUpdatesCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER MATERIALIZED VIEW OWNER TO updates pg_class relowner",
			SetUpScript: []string{
				`CREATE ROLE materialized_view_owner;`,
				`CREATE TABLE materialized_view_owner_base (id INT PRIMARY KEY);`,
				`INSERT INTO materialized_view_owner_base VALUES (1);`,
				`CREATE MATERIALIZED VIEW owned_materialized_view_catalog AS
					SELECT id FROM materialized_view_owner_base;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER MATERIALIZED VIEW owned_materialized_view_catalog
						OWNER TO materialized_view_owner;`,
				},
				{
					Query: `SELECT pg_get_userbyid(relowner)
						FROM pg_class
						WHERE relname = 'owned_materialized_view_catalog';`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testaltermaterializedviewownerupdatescatalogrepro-0001-select-pg_get_userbyid-relowner-from-pg_class"},
				},
			},
		},
	})
}

// TestAlterSequenceOwnerUpdatesCatalogRepro reproduces a security/catalog bug:
// Doltgres accepts ALTER SEQUENCE ... OWNER TO, but pg_class.relowner remains
// the original owner instead of the requested role.
func TestAlterSequenceOwnerUpdatesCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SEQUENCE OWNER TO updates pg_class relowner",
			SetUpScript: []string{
				`CREATE ROLE sequence_owner;`,
				`CREATE SEQUENCE owned_sequence_catalog;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SEQUENCE owned_sequence_catalog OWNER TO sequence_owner;`,
				},
				{
					Query: `SELECT pg_get_userbyid(relowner)
						FROM pg_class
						WHERE relname = 'owned_sequence_catalog';`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testaltersequenceownerupdatescatalogrepro-0001-select-pg_get_userbyid-relowner-from-pg_class"},
				},
			},
		},
	})
}

// TestAlterSequenceOwnerCanUseTransferredSequenceRepro reproduces an ownership
// privilege bug: after ALTER SEQUENCE ... OWNER TO, the transferred owner
// should be able to use the sequence without explicit sequence grants.
func TestAlterSequenceOwnerCanUseTransferredSequenceRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SEQUENCE OWNER TO lets transferred owner use sequence",
			SetUpScript: []string{
				`CREATE USER transferred_sequence_owner PASSWORD 'pw';`,
				`CREATE SEQUENCE transferred_sequence_runtime;`,
				`GRANT USAGE ON SCHEMA public TO transferred_sequence_owner;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SEQUENCE transferred_sequence_runtime OWNER TO transferred_sequence_owner;`,
				},
				{
					Query: `SELECT nextval('transferred_sequence_runtime');`,

					Username: `transferred_sequence_owner`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestAlterSchemaOwnerUpdatesCatalogRepro reproduces a security/catalog bug:
						// Doltgres accepts ALTER SCHEMA ... OWNER TO, but pg_namespace.nspowner remains
						// the original owner instead of the requested role.
						ID: "ownership-repro-test-testaltersequenceownercanusetransferredsequencerepro-0001-select-nextval-transferred_sequence_runtime"},
				},
			},
		},
	})
}

func TestAlterSchemaOwnerUpdatesCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SCHEMA OWNER TO updates pg_namespace nspowner",
			SetUpScript: []string{
				`CREATE ROLE schema_owner;`,
				`CREATE SCHEMA owned_schema_catalog;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SCHEMA owned_schema_catalog OWNER TO schema_owner;`,
				},
				{
					Query: `SELECT pg_get_userbyid(nspowner)
						FROM pg_namespace
						WHERE nspname = 'owned_schema_catalog';`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testalterschemaownerupdatescatalogrepro-0001-select-pg_get_userbyid-nspowner-from-pg_namespace"},
				},
			},
		},
	})
}

// TestAlterSchemaOwnerCanUseTransferredSchemaRepro reproduces an ownership
// privilege bug: after ALTER SCHEMA ... OWNER TO, the transferred owner should
// be able to create objects in that schema without explicit schema grants.
func TestAlterSchemaOwnerCanUseTransferredSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SCHEMA OWNER TO lets transferred owner use schema",
			SetUpScript: []string{
				`CREATE USER transferred_schema_owner PASSWORD 'pw';`,
				`CREATE SCHEMA transferred_schema_runtime;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SCHEMA transferred_schema_runtime OWNER TO transferred_schema_owner;`,
				},
				{
					Query:    `CREATE TABLE transferred_schema_runtime.owned_schema_table (id INT PRIMARY KEY);`,
					Username: `transferred_schema_owner`,
					Password: `pw`,
				},
				{
					Query:    `INSERT INTO transferred_schema_runtime.owned_schema_table VALUES (1);`,
					Username: `transferred_schema_owner`,
					Password: `pw`,
				},
				{
					Query: `SELECT id FROM transferred_schema_runtime.owned_schema_table;`,

					Username: `transferred_schema_owner`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestCreateSchemaAuthorizationUpdatesCatalogRepro reproduces a
						// security/catalog bug: Doltgres accepts CREATE SCHEMA AUTHORIZATION, but
						// pg_namespace.nspowner remains postgres instead of the authorized role.
						ID: "ownership-repro-test-testalterschemaownercanusetransferredschemarepro-0001-select-id-from", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCreateSchemaAuthorizationUpdatesCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE SCHEMA AUTHORIZATION updates pg_namespace nspowner",
			SetUpScript: []string{
				`CREATE ROLE schema_auth_owner;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE SCHEMA AUTHORIZATION schema_auth_owner;`,
				},
				{
					Query: `SELECT pg_get_userbyid(nspowner)
						FROM pg_namespace
						WHERE nspname = 'schema_auth_owner';`,
					Expected: []sql.Row{{"schema_auth_owner"}},
				},
			},
		},
	})
}

// TestCreateSchemaAuthorizationRequiresExistingRoleRepro reproduces a
// security/catalog bug: Doltgres accepts CREATE SCHEMA AUTHORIZATION for a role
// that does not exist.
func TestCreateSchemaAuthorizationRequiresExistingRoleRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE SCHEMA AUTHORIZATION requires existing role",
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE SCHEMA AUTHORIZATION missing_schema_owner;`,
					ExpectedErr: `does not exist`,
				},
			},
		},
	})
}

// TestAlterTypeOwnerUpdatesCatalogRepro reproduces a security/catalog bug:
// Doltgres accepts ALTER TYPE ... OWNER TO, but pg_type.typowner remains the
// original owner instead of the requested role.
func TestAlterTypeOwnerUpdatesCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TYPE OWNER TO updates pg_type typowner",
			SetUpScript: []string{
				`CREATE ROLE type_owner;`,
				`CREATE TYPE owned_enum_catalog AS ENUM ('one', 'two');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TYPE owned_enum_catalog OWNER TO type_owner;`,
				},
				{
					Query: `SELECT pg_get_userbyid(typowner)
						FROM pg_type
						WHERE typname = 'owned_enum_catalog';`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testaltertypeownerupdatescatalogrepro-0001-select-pg_get_userbyid-typowner-from-pg_type"},
				},
			},
		},
	})
}

// TestAlterDomainOwnerUpdatesCatalogRepro reproduces a security/catalog bug:
// Doltgres accepts ALTER DOMAIN ... OWNER TO, but pg_type.typowner remains the
// original owner instead of the requested role.
func TestAlterDomainOwnerUpdatesCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER DOMAIN OWNER TO updates pg_type typowner",
			SetUpScript: []string{
				`CREATE ROLE domain_owner;`,
				`CREATE DOMAIN owned_domain_catalog AS INT;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER DOMAIN owned_domain_catalog OWNER TO domain_owner;`,
				},
				{
					Query: `SELECT pg_get_userbyid(typowner)
						FROM pg_type
						WHERE typname = 'owned_domain_catalog';`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testalterdomainownerupdatescatalogrepro-0001-select-pg_get_userbyid-typowner-from-pg_type"},
				},
			},
		},
	})
}

// TestFunctionOwnerCatalogEntryRepro reproduces a security/catalog bug:
// Doltgres accepts CREATE FUNCTION and ALTER FUNCTION ... OWNER TO, but the
// function has no visible pg_proc ownership row.
func TestFunctionOwnerCatalogEntryRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER FUNCTION OWNER TO exposes pg_proc proowner",
			SetUpScript: []string{
				`CREATE ROLE function_owner;`,
				`CREATE FUNCTION owned_function_catalog() RETURNS INT AS $$ SELECT 1 $$ LANGUAGE SQL;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER FUNCTION owned_function_catalog() OWNER TO function_owner;`,
				},
				{
					Query: `SELECT pg_get_userbyid(proowner)
						FROM pg_proc
						WHERE proname = 'owned_function_catalog';`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testfunctionownercatalogentryrepro-0001-select-pg_get_userbyid-proowner-from-pg_proc"},
				},
			},
		},
	})
}

// TestFunctionOwnerCanExecuteCreatedFunctionRepro reproduces a function
// ownership privilege bug: a role that creates a function owns it and should be
// able to execute it without an explicit EXECUTE grant.
func TestFunctionOwnerCanExecuteCreatedFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "function owner can execute created function",
			SetUpScript: []string{
				`CREATE USER function_owner_user PASSWORD 'pw';`,
				`GRANT USAGE, CREATE ON SCHEMA public TO function_owner_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE FUNCTION owner_created_function()
						RETURNS INT AS $$ SELECT 7 $$ LANGUAGE SQL;`,
					Username: `function_owner_user`,
					Password: `pw`,
				},
				{
					Query: `SELECT owner_created_function();`,

					Username: `function_owner_user`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestCreateOrReplaceFunctionPreservesOwnerRepro reproduces an ownership
						// persistence bug: PostgreSQL preserves the existing function owner during
						// CREATE OR REPLACE FUNCTION.
						ID: "ownership-repro-test-testfunctionownercanexecutecreatedfunctionrepro-0001-select-owner_created_function"},
				},
			},
		},
	})
}

func TestCreateOrReplaceFunctionPreservesOwnerRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE OR REPLACE FUNCTION preserves pg_proc proowner",
			SetUpScript: []string{
				`CREATE USER replace_function_owner PASSWORD 'pw';`,
				`GRANT USAGE, CREATE ON SCHEMA public TO replace_function_owner;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE FUNCTION replace_owner_function()
						RETURNS INT AS $$ SELECT 1 $$ LANGUAGE SQL;`,
					Username: `replace_function_owner`,
					Password: `pw`,
				},
				{
					Query: `CREATE OR REPLACE FUNCTION replace_owner_function()
						RETURNS INT AS $$ SELECT 2 $$ LANGUAGE SQL;`,
				},
				{
					Query: `SELECT pg_get_userbyid(proowner)
						FROM pg_catalog.pg_proc
						WHERE proname = 'replace_owner_function';`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testcreateorreplacefunctionpreservesownerrepro-0001-select-pg_get_userbyid-proowner-from-pg_catalog.pg_proc"},
				},
			},
		},
	})
}

// TestProcedureOwnerCatalogEntryRepro reproduces a security/catalog bug:
// Doltgres accepts CREATE PROCEDURE and ALTER PROCEDURE ... OWNER TO, but the
// procedure has no visible pg_proc ownership row.
func TestProcedureOwnerCatalogEntryRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER PROCEDURE OWNER TO exposes pg_proc proowner",
			SetUpScript: []string{
				`CREATE ROLE procedure_owner;`,
				`CREATE PROCEDURE owned_procedure_catalog()
					AS $$
					BEGIN
						NULL;
					END;
					$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER PROCEDURE owned_procedure_catalog() OWNER TO procedure_owner;`,
				},
				{
					Query: `SELECT pg_get_userbyid(proowner)
						FROM pg_proc
						WHERE proname = 'owned_procedure_catalog';`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testprocedureownercatalogentryrepro-0001-select-pg_get_userbyid-proowner-from-pg_proc"},
				},
			},
		},
	})
}

// TestProcedureOwnerCanCallCreatedProcedureRepro reproduces a procedure
// ownership privilege bug: a role that creates a procedure owns it and should
// be able to call it without an explicit EXECUTE grant.
func TestProcedureOwnerCanCallCreatedProcedureRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "procedure owner can call created procedure",
			SetUpScript: []string{
				`CREATE USER procedure_owner_user PASSWORD 'pw';`,
				`GRANT USAGE, CREATE ON SCHEMA public TO procedure_owner_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PROCEDURE owner_created_procedure()
						AS $$
						BEGIN
							NULL;
						END;
						$$ LANGUAGE plpgsql;`,
					Username: `procedure_owner_user`,
					Password: `pw`,
				},
				{
					Query: `CALL owner_created_procedure();`,

					Username: `procedure_owner_user`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestCreateOrReplaceProcedurePreservesOwnerRepro reproduces an ownership
						// persistence bug: PostgreSQL preserves the existing procedure owner during
						// CREATE OR REPLACE PROCEDURE.
						ID: "ownership-repro-test-testprocedureownercancallcreatedprocedurerepro-0001-call-owner_created_procedure"},
				},
			},
		},
	})
}

func TestCreateOrReplaceProcedurePreservesOwnerRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE OR REPLACE PROCEDURE preserves pg_proc proowner",
			SetUpScript: []string{
				`CREATE USER replace_procedure_owner PASSWORD 'pw';`,
				`GRANT USAGE, CREATE ON SCHEMA public TO replace_procedure_owner;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE PROCEDURE replace_owner_procedure()
						AS $$
						BEGIN
							NULL;
						END;
						$$ LANGUAGE plpgsql;`,
					Username: `replace_procedure_owner`,
					Password: `pw`,
				},
				{
					Query: `CREATE OR REPLACE PROCEDURE replace_owner_procedure()
						AS $$
						BEGIN
							NULL;
						END;
						$$ LANGUAGE plpgsql;`,
				},
				{
					Query: `SELECT pg_get_userbyid(proowner)
						FROM pg_catalog.pg_proc
						WHERE proname = 'replace_owner_procedure';`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testcreateorreplaceprocedurepreservesownerrepro-0001-select-pg_get_userbyid-proowner-from-pg_catalog.pg_proc"},
				},
			},
		},
	})
}

// TestCreateOrReplaceAggregatePreservesOwnerRepro reproduces an ownership
// persistence bug: PostgreSQL preserves the existing aggregate owner during
// CREATE OR REPLACE AGGREGATE.
func TestCreateOrReplaceAggregatePreservesOwnerRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE OR REPLACE AGGREGATE preserves pg_proc proowner",
			SetUpScript: []string{
				`CREATE USER replace_aggregate_owner PASSWORD 'pw';`,
				`GRANT USAGE, CREATE ON SCHEMA public TO replace_aggregate_owner;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE FUNCTION replace_aggregate_sfunc(state INT, next_value INT)
						RETURNS INT
						LANGUAGE SQL
						IMMUTABLE
						AS $$ SELECT COALESCE(state, 0) + COALESCE(next_value, 0) $$;`,
					Username: `replace_aggregate_owner`,
					Password: `pw`,
				},
				{
					Query: `CREATE AGGREGATE replace_owner_aggregate(INT) (
						SFUNC = replace_aggregate_sfunc,
						STYPE = INT,
						INITCOND = '0'
					);`,
					Username: `replace_aggregate_owner`,
					Password: `pw`,
				},
				{
					Query: `CREATE OR REPLACE AGGREGATE replace_owner_aggregate(INT) (
						SFUNC = replace_aggregate_sfunc,
						STYPE = INT,
						INITCOND = '1'
					);`,
				},
				{
					Query: `SELECT pg_get_userbyid(proowner)
						FROM pg_catalog.pg_proc
						WHERE proname = 'replace_owner_aggregate';`, PostgresOracle: ScriptTestPostgresOracle{ID: "ownership-repro-test-testcreateorreplaceaggregatepreservesownerrepro-0001-select-pg_get_userbyid-proowner-from-pg_catalog.pg_proc"},
				},
			},
		},
	})
}

// TestCreateExtensionOwnerUpdatesCatalogRepro reproduces a
// security/catalog bug: extensions installed by a non-superuser role should
// record that role in pg_extension.extowner, but Doltgres reports postgres.
func TestCreateExtensionOwnerUpdatesCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE EXTENSION updates pg_extension extowner",
			SetUpScript: []string{
				`CREATE USER extension_catalog_creator PASSWORD 'pw';`,
				`GRANT CREATE ON DATABASE postgres TO extension_catalog_creator;`,
				`GRANT USAGE, CREATE ON SCHEMA public TO extension_catalog_creator;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `CREATE EXTENSION hstore WITH SCHEMA public;`,
					Username: `extension_catalog_creator`,
					Password: `pw`,
				},
				{
					Query: `SELECT pg_get_userbyid(extowner)
						FROM pg_catalog.pg_extension
						WHERE extname = 'hstore';`,
					Expected: []sql.Row{{"extension_catalog_creator"}},
				},
			},
		},
	})
}
