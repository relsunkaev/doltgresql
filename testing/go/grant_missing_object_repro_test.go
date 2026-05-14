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

// TestGrantOnMissingTableRequiresExistingRelationRepro reproduces an ACL
// consistency bug: Doltgres accepts GRANT on a missing table.
func TestGrantOnMissingTableRequiresExistingRelationRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT on missing table requires existing relation",
			SetUpScript: []string{
				`CREATE USER missing_table_grantee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT SELECT ON TABLE missing_grant_table TO missing_table_grantee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-missing-object-repro-test-testgrantonmissingtablerequiresexistingrelationrepro-0001-grant-select-on-table-missing_grant_table",

						// TestGrantOnMissingSchemaRequiresExistingSchemaRepro reproduces an ACL
						// consistency bug: Doltgres accepts GRANT on a missing schema.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestGrantOnMissingSchemaRequiresExistingSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT on missing schema requires existing schema",
			SetUpScript: []string{
				`CREATE USER missing_schema_grantee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT USAGE ON SCHEMA missing_grant_schema TO missing_schema_grantee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-missing-object-repro-test-testgrantonmissingschemarequiresexistingschemarepro-0001-grant-usage-on-schema-missing_grant_schema",

						// TestGrantOnMissingDatabaseRequiresExistingDatabaseRepro reproduces an ACL
						// consistency bug: Doltgres accepts GRANT on a missing database.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestGrantOnMissingDatabaseRequiresExistingDatabaseRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT on missing database requires existing database",
			SetUpScript: []string{
				`CREATE USER missing_database_grantee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT CONNECT ON DATABASE missing_grant_database TO missing_database_grantee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-missing-object-repro-test-testgrantonmissingdatabaserequiresexistingdatabaserepro-0001-grant-connect-on-database-missing_grant_database",

						// TestGrantOnMissingSequenceRequiresExistingSequenceRepro reproduces an ACL
						// consistency bug: Doltgres accepts GRANT on a missing sequence.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestGrantOnMissingSequenceRequiresExistingSequenceRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT on missing sequence requires existing relation",
			SetUpScript: []string{
				`CREATE USER missing_sequence_grantee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT USAGE ON SEQUENCE missing_grant_sequence TO missing_sequence_grantee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-missing-object-repro-test-testgrantonmissingsequencerequiresexistingsequencerepro-0001-grant-usage-on-sequence-missing_grant_sequence",

						// TestGrantOnMissingFunctionRequiresExistingRoutineRepro reproduces an ACL
						// consistency bug: Doltgres accepts GRANT on a missing function.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestGrantOnMissingFunctionRequiresExistingRoutineRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT on missing function requires existing routine",
			SetUpScript: []string{
				`CREATE USER missing_function_grantee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT EXECUTE ON FUNCTION missing_grant_function() TO missing_function_grantee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-missing-object-repro-test-testgrantonmissingfunctionrequiresexistingroutinerepro-0001-grant-execute-on-function-missing_grant_function",

						// TestGrantOnMissingProcedureRequiresExistingRoutineRepro reproduces an ACL
						// consistency bug: Doltgres accepts GRANT on a missing procedure.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestGrantOnMissingProcedureRequiresExistingRoutineRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT on missing procedure requires existing routine",
			SetUpScript: []string{
				`CREATE USER missing_procedure_grantee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT EXECUTE ON PROCEDURE missing_grant_procedure() TO missing_procedure_grantee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-missing-object-repro-test-testgrantonmissingprocedurerequiresexistingroutinerepro-0001-grant-execute-on-procedure-missing_grant_procedure",

						// TestGrantOnMissingRoutineRequiresExistingRoutineRepro reproduces an ACL
						// consistency bug: Doltgres accepts GRANT on a missing routine.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestGrantOnMissingRoutineRequiresExistingRoutineRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT on missing routine requires existing routine",
			SetUpScript: []string{
				`CREATE USER missing_routine_grantee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT EXECUTE ON ROUTINE missing_grant_routine() TO missing_routine_grantee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-missing-object-repro-test-testgrantonmissingroutinerequiresexistingroutinerepro-0001-grant-execute-on-routine-missing_grant_routine",

						// TestGrantOnMissingForeignDataWrapperRequiresExistingWrapperRepro reproduces
						// an ACL consistency bug: PostgreSQL validates that the named foreign-data
						// wrapper exists before granting privileges on it.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestGrantOnMissingForeignDataWrapperRequiresExistingWrapperRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT on missing foreign data wrapper requires existing wrapper",
			SetUpScript: []string{
				`CREATE USER missing_fdw_grantee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT USAGE ON FOREIGN DATA WRAPPER missing_grant_fdw TO missing_fdw_grantee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-missing-object-repro-test-testgrantonmissingforeigndatawrapperrequiresexistingwrapperrepro-0001-grant-usage-on-foreign-data",

						// TestGrantOnMissingForeignServerRequiresExistingServerRepro reproduces an ACL
						// consistency bug: PostgreSQL validates that the named foreign server exists
						// before granting privileges on it.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestGrantOnMissingForeignServerRequiresExistingServerRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT on missing foreign server requires existing server",
			SetUpScript: []string{
				`CREATE USER missing_foreign_server_grantee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT USAGE ON FOREIGN SERVER missing_grant_server TO missing_foreign_server_grantee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-missing-object-repro-test-testgrantonmissingforeignserverrequiresexistingserverrepro-0001-grant-usage-on-foreign-server",

						// TestRevokeOnMissingTableRequiresExistingRelationRepro reproduces an ACL
						// consistency bug: Doltgres accepts REVOKE on a missing table.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestRevokeOnMissingTableRequiresExistingRelationRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE on missing table requires existing relation",
			SetUpScript: []string{
				`CREATE USER missing_table_revokee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `REVOKE SELECT ON TABLE missing_revoke_table FROM missing_table_revokee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-missing-object-repro-test-testrevokeonmissingtablerequiresexistingrelationrepro-0001-revoke-select-on-table-missing_revoke_table",

						// TestRevokeOnMissingSchemaRequiresExistingSchemaRepro reproduces an ACL
						// consistency bug: Doltgres accepts REVOKE on a missing schema.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestRevokeOnMissingSchemaRequiresExistingSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE on missing schema requires existing schema",
			SetUpScript: []string{
				`CREATE USER missing_schema_revokee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `REVOKE USAGE ON SCHEMA missing_revoke_schema FROM missing_schema_revokee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-missing-object-repro-test-testrevokeonmissingschemarequiresexistingschemarepro-0001-revoke-usage-on-schema-missing_revoke_schema",

						// TestRevokeOnMissingDatabaseRequiresExistingDatabaseRepro reproduces an ACL
						// consistency bug: Doltgres accepts REVOKE on a missing database.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestRevokeOnMissingDatabaseRequiresExistingDatabaseRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE on missing database requires existing database",
			SetUpScript: []string{
				`CREATE USER missing_database_revokee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `REVOKE CONNECT ON DATABASE missing_revoke_database FROM missing_database_revokee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-missing-object-repro-test-testrevokeonmissingdatabaserequiresexistingdatabaserepro-0001-revoke-connect-on-database-missing_revoke_database",

						// TestRevokeOnMissingSequenceRequiresExistingSequenceRepro reproduces an ACL
						// consistency bug: Doltgres accepts REVOKE on a missing sequence.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestRevokeOnMissingSequenceRequiresExistingSequenceRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE on missing sequence requires existing relation",
			SetUpScript: []string{
				`CREATE USER missing_sequence_revokee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `REVOKE USAGE ON SEQUENCE missing_revoke_sequence FROM missing_sequence_revokee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-missing-object-repro-test-testrevokeonmissingsequencerequiresexistingsequencerepro-0001-revoke-usage-on-sequence-missing_revoke_sequence",

						// TestRevokeOnMissingFunctionRequiresExistingRoutineRepro reproduces an ACL
						// consistency bug: Doltgres accepts REVOKE on a missing function.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestRevokeOnMissingFunctionRequiresExistingRoutineRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE on missing function requires existing routine",
			SetUpScript: []string{
				`CREATE USER missing_function_revokee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `REVOKE EXECUTE ON FUNCTION missing_revoke_function() FROM missing_function_revokee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-missing-object-repro-test-testrevokeonmissingfunctionrequiresexistingroutinerepro-0001-revoke-execute-on-function-missing_revoke_function",

						// TestRevokeOnMissingProcedureRequiresExistingRoutineRepro reproduces an ACL
						// consistency bug: Doltgres accepts REVOKE on a missing procedure.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestRevokeOnMissingProcedureRequiresExistingRoutineRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE on missing procedure requires existing routine",
			SetUpScript: []string{
				`CREATE USER missing_procedure_revokee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `REVOKE EXECUTE ON PROCEDURE missing_revoke_procedure() FROM missing_procedure_revokee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-missing-object-repro-test-testrevokeonmissingprocedurerequiresexistingroutinerepro-0001-revoke-execute-on-procedure-missing_revoke_procedure",

						// TestRevokeOnMissingRoutineRequiresExistingRoutineRepro reproduces an ACL
						// consistency bug: Doltgres accepts REVOKE on a missing routine.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestRevokeOnMissingRoutineRequiresExistingRoutineRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE on missing routine requires existing routine",
			SetUpScript: []string{
				`CREATE USER missing_routine_revokee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `REVOKE EXECUTE ON ROUTINE missing_revoke_routine() FROM missing_routine_revokee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-missing-object-repro-test-testrevokeonmissingroutinerequiresexistingroutinerepro-0001-revoke-execute-on-routine-missing_revoke_routine",

						// TestRevokeOnMissingForeignDataWrapperRequiresExistingWrapperRepro reproduces
						// an ACL consistency bug: PostgreSQL validates that the named foreign-data
						// wrapper exists before revoking privileges on it.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestRevokeOnMissingForeignDataWrapperRequiresExistingWrapperRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE on missing foreign data wrapper requires existing wrapper",
			SetUpScript: []string{
				`CREATE USER missing_fdw_revokee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `REVOKE USAGE ON FOREIGN DATA WRAPPER missing_revoke_fdw FROM missing_fdw_revokee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-missing-object-repro-test-testrevokeonmissingforeigndatawrapperrequiresexistingwrapperrepro-0001-revoke-usage-on-foreign-data",

						// TestRevokeOnMissingForeignServerRequiresExistingServerRepro reproduces an ACL
						// consistency bug: PostgreSQL validates that the named foreign server exists
						// before revoking privileges on it.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestRevokeOnMissingForeignServerRequiresExistingServerRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE on missing foreign server requires existing server",
			SetUpScript: []string{
				`CREATE USER missing_foreign_server_revokee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `REVOKE USAGE ON FOREIGN SERVER missing_revoke_server FROM missing_foreign_server_revokee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-missing-object-repro-test-testrevokeonmissingforeignserverrequiresexistingserverrepro-0001-revoke-usage-on-foreign-server",

						// TestGrantOnAllTablesInMissingSchemaRequiresExistingSchemaRepro reproduces an
						// ACL consistency bug: Doltgres accepts GRANT ON ALL TABLES IN SCHEMA for a
						// missing schema.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestGrantOnAllTablesInMissingSchemaRequiresExistingSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT on all tables in missing schema requires existing schema",
			SetUpScript: []string{
				`CREATE USER missing_all_tables_grantee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT SELECT ON ALL TABLES IN SCHEMA missing_all_tables_schema TO missing_all_tables_grantee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-missing-object-repro-test-testgrantonalltablesinmissingschemarequiresexistingschemarepro-0001-grant-select-on-all-tables",

						// TestGrantOnAllSequencesInMissingSchemaRequiresExistingSchemaRepro reproduces
						// an ACL consistency bug: Doltgres accepts GRANT ON ALL SEQUENCES IN SCHEMA for
						// a missing schema.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestGrantOnAllSequencesInMissingSchemaRequiresExistingSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT on all sequences in missing schema requires existing schema",
			SetUpScript: []string{
				`CREATE USER missing_all_sequences_grantee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT USAGE ON ALL SEQUENCES IN SCHEMA missing_all_sequences_schema TO missing_all_sequences_grantee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-missing-object-repro-test-testgrantonallsequencesinmissingschemarequiresexistingschemarepro-0001-grant-usage-on-all-sequences",

						// TestGrantOnAllFunctionsInMissingSchemaRequiresExistingSchemaRepro reproduces
						// an ACL consistency bug: Doltgres accepts GRANT ON ALL FUNCTIONS IN SCHEMA for
						// a missing schema.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestGrantOnAllFunctionsInMissingSchemaRequiresExistingSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT on all functions in missing schema requires existing schema",
			SetUpScript: []string{
				`CREATE USER missing_all_functions_grantee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA missing_all_functions_schema TO missing_all_functions_grantee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-missing-object-repro-test-testgrantonallfunctionsinmissingschemarequiresexistingschemarepro-0001-grant-execute-on-all-functions",

						// TestRevokeOnAllTablesInMissingSchemaRequiresExistingSchemaRepro reproduces an
						// ACL consistency bug: Doltgres accepts REVOKE ON ALL TABLES IN SCHEMA for a
						// missing schema.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestRevokeOnAllTablesInMissingSchemaRequiresExistingSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE on all tables in missing schema requires existing schema",
			SetUpScript: []string{
				`CREATE USER missing_all_tables_revokee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `REVOKE SELECT ON ALL TABLES IN SCHEMA missing_revoke_all_tables_schema FROM missing_all_tables_revokee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-missing-object-repro-test-testrevokeonalltablesinmissingschemarequiresexistingschemarepro-0001-revoke-select-on-all-tables",

						// TestRevokeOnAllSequencesInMissingSchemaRequiresExistingSchemaRepro reproduces
						// an ACL consistency bug: Doltgres accepts REVOKE ON ALL SEQUENCES IN SCHEMA
						// for a missing schema.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestRevokeOnAllSequencesInMissingSchemaRequiresExistingSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE on all sequences in missing schema requires existing schema",
			SetUpScript: []string{
				`CREATE USER missing_all_sequences_revokee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `REVOKE USAGE ON ALL SEQUENCES IN SCHEMA missing_revoke_all_sequences_schema FROM missing_all_sequences_revokee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-missing-object-repro-test-testrevokeonallsequencesinmissingschemarequiresexistingschemarepro-0001-revoke-usage-on-all-sequences",

						// TestRevokeOnAllFunctionsInMissingSchemaRequiresExistingSchemaRepro reproduces
						// an ACL consistency bug: Doltgres accepts REVOKE ON ALL FUNCTIONS IN SCHEMA
						// for a missing schema.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestRevokeOnAllFunctionsInMissingSchemaRequiresExistingSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE on all functions in missing schema requires existing schema",
			SetUpScript: []string{
				`CREATE USER missing_all_functions_revokee PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA missing_revoke_all_functions_schema FROM missing_all_functions_revokee;`, PostgresOracle: ScriptTestPostgresOracle{ID: "grant-missing-object-repro-test-testrevokeonallfunctionsinmissingschemarequiresexistingschemarepro-0001-revoke-execute-on-all-functions",

						// TestRevokeAllTablesInSchemaDoesNotAffectOtherSchemasRepro reproduces an ACL
						// consistency bug: REVOKE ON ALL TABLES IN SCHEMA should remove only the named
						// schema's all-tables privilege.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestRevokeAllTablesInSchemaDoesNotAffectOtherSchemasRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "REVOKE on all tables in one schema preserves other schema privileges",
			SetUpScript: []string{
				`CREATE USER revoke_all_tables_user PASSWORD 'pw';`,
				`CREATE SCHEMA revoke_other_schema;`,
				`CREATE TABLE revoke_public_acl (id INT PRIMARY KEY);`,
				`CREATE TABLE revoke_other_schema.revoke_other_acl (id INT PRIMARY KEY);`,
				`INSERT INTO revoke_public_acl VALUES (1);`,
				`INSERT INTO revoke_other_schema.revoke_other_acl VALUES (2);`,
				`GRANT USAGE ON SCHEMA public TO revoke_all_tables_user;`,
				`GRANT USAGE ON SCHEMA revoke_other_schema TO revoke_all_tables_user;`,
				`GRANT SELECT ON ALL TABLES IN SCHEMA public TO revoke_all_tables_user;`,
				`GRANT SELECT ON ALL TABLES IN SCHEMA revoke_other_schema TO revoke_all_tables_user;`,
				`REVOKE SELECT ON ALL TABLES IN SCHEMA revoke_other_schema FROM revoke_all_tables_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT id FROM revoke_public_acl;`,
					Expected: []sql.Row{{int32(1)}},
					Username: `revoke_all_tables_user`,
					Password: `pw`,
				},
				{
					Query:       `SELECT id FROM revoke_other_schema.revoke_other_acl;`,
					ExpectedErr: `permission denied for table`,
					Username:    `revoke_all_tables_user`,
					Password:    `pw`,
				},
			},
		},
	})
}
