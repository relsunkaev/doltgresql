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

// TestCommentOnColumnPersistsDescriptionRepro reproduces a metadata
// persistence bug: Doltgres accepts COMMENT ON COLUMN, but col_description does
// not return the stored comment.
func TestCommentOnColumnPersistsDescriptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON COLUMN persists col_description metadata",
			SetUpScript: []string{
				`CREATE TABLE comment_target (id INT PRIMARY KEY, label TEXT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON COLUMN comment_target.label IS 'visible label comment';`,
				},
				{
					Query: `SELECT col_description('comment_target'::regclass, 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentoncolumnpersistsdescriptionrepro-0001-select-col_description-comment_target-::regclass-2"},
				},
			},
		},
	})
}

// TestCommentOnTablePersistsDescriptionRepro reproduces a persistence bug:
// Doltgres accepts COMMENT ON TABLE but does not persist the table description.
func TestCommentOnTablePersistsDescriptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON TABLE persists obj_description metadata",
			SetUpScript: []string{
				`CREATE TABLE table_comment_target (id INT PRIMARY KEY);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON TABLE table_comment_target IS 'visible table comment';`,
				},
				{
					Query: `SELECT obj_description('table_comment_target'::regclass);`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentontablepersistsdescriptionrepro-0001-select-obj_description-table_comment_target-::regclass"},
				},
			},
		},
	})
}

// TestCommentOnTablePopulatesPgDescriptionRepro reproduces the same comment
// persistence gap through the underlying pg_description catalog table.
func TestCommentOnTablePopulatesPgDescriptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON TABLE populates pg_description",
			SetUpScript: []string{
				`CREATE TABLE pg_description_table_target (id INT PRIMARY KEY);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON TABLE pg_description_table_target
						IS 'visible pg_description comment';`,
				},
				{
					Query: `SELECT description
						FROM pg_catalog.pg_description
						WHERE objoid = 'pg_description_table_target'::regclass
							AND classoid = 'pg_class'::regclass
							AND objsubid = 0;`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentontablepopulatespgdescriptionrepro-0001-select-description-from-pg_catalog.pg_description-where"},
				},
			},
		},
	})
}

// TestCommentOnRelationKindsPersistsDescriptionRepro reproduces the same
// persistence bug for other relation kinds: Doltgres accepts COMMENT ON VIEW,
// COMMENT ON MATERIALIZED VIEW, and COMMENT ON SEQUENCE but does not persist
// their descriptions.
func TestCommentOnRelationKindsPersistsDescriptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON relation kinds persists obj_description metadata",
			SetUpScript: []string{
				`CREATE VIEW view_comment_target AS SELECT 1 AS id;`,
				`CREATE TABLE matview_comment_source (id INT PRIMARY KEY);`,
				`INSERT INTO matview_comment_source VALUES (1);`,
				`CREATE MATERIALIZED VIEW matview_comment_target AS
					SELECT id FROM matview_comment_source;`,
				`CREATE SEQUENCE sequence_comment_target;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON VIEW view_comment_target IS 'visible view comment';`,
				},
				{
					Query: `COMMENT ON MATERIALIZED VIEW matview_comment_target
						IS 'visible materialized view comment';`,
				},
				{
					Query: `COMMENT ON SEQUENCE sequence_comment_target
						IS 'visible sequence comment';`,
				},
				{
					Query: `SELECT
							obj_description('view_comment_target'::regclass),
							obj_description('matview_comment_target'::regclass),
							obj_description('sequence_comment_target'::regclass);`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonrelationkindspersistsdescriptionrepro-0001-select-obj_description-view_comment_target-::regclass-obj_description"},
				},
			},
		},
	})
}

// TestCommentOnIndexConstraintTriggerPersistsDescriptionRepro reproduces the
// same persistence bug for indexes, constraints, and triggers.
func TestCommentOnIndexConstraintTriggerPersistsDescriptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON index constraint and trigger persists metadata",
			SetUpScript: []string{
				`CREATE TABLE comment_metadata_target (
					id INT PRIMARY KEY,
					v INT CONSTRAINT comment_metadata_v_positive CHECK (v > 0)
				);`,
				`CREATE INDEX comment_metadata_v_idx ON comment_metadata_target (v);`,
				`CREATE FUNCTION comment_metadata_trigger_func() RETURNS TRIGGER AS $$
				BEGIN
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER comment_metadata_before_insert
					BEFORE INSERT ON comment_metadata_target
					FOR EACH ROW EXECUTE FUNCTION comment_metadata_trigger_func();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON INDEX comment_metadata_v_idx IS 'visible index comment';`,
				},
				{
					Query: `COMMENT ON CONSTRAINT comment_metadata_v_positive
						ON comment_metadata_target IS 'visible constraint comment';`,
				},
				{
					Query: `COMMENT ON TRIGGER comment_metadata_before_insert
						ON comment_metadata_target IS 'visible trigger comment';`,
				},
				{
					Query: `SELECT
							obj_description('comment_metadata_v_idx'::regclass),
							obj_description(
								(SELECT oid FROM pg_constraint
								 WHERE conname = 'comment_metadata_v_positive'),
								'pg_constraint'),
							obj_description(
								(SELECT oid FROM pg_trigger
								 WHERE tgname = 'comment_metadata_before_insert'),
								'pg_trigger');`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonindexconstrainttriggerpersistsdescriptionrepro-0001-select-obj_description-comment_metadata_v_idx-::regclass-obj_description"},
				},
			},
		},
	})
}

// TestCommentOnSchemaPersistsDescriptionRepro reproduces a persistence bug:
// Doltgres accepts COMMENT ON SCHEMA but does not persist the schema
// description.
func TestCommentOnSchemaPersistsDescriptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON SCHEMA persists obj_description metadata",
			SetUpScript: []string{
				`CREATE SCHEMA schema_comment_target;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON SCHEMA schema_comment_target IS 'visible schema comment';`,
				},
				{
					Query: `SELECT obj_description(
						(SELECT oid FROM pg_namespace WHERE nspname = 'schema_comment_target'),
						'pg_namespace');`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonschemapersistsdescriptionrepro-0001-select-obj_description-select-oid-from"},
				},
			},
		},
	})
}

// TestCommentOnDatabasePersistsDescriptionRepro reproduces a persistence bug:
// Doltgres accepts COMMENT ON DATABASE but does not persist the shared database
// description.
func TestCommentOnDatabasePersistsDescriptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON DATABASE persists shobj_description metadata",
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON DATABASE postgres IS 'visible database comment';`,
				},
				{
					Query: `SELECT shobj_description(
						(SELECT oid FROM pg_database WHERE datname = 'postgres'),
						'pg_database');`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentondatabasepersistsdescriptionrepro-0001-select-shobj_description-select-oid-from"},
				},
			},
		},
	})
}

// TestCommentOnDatabasePopulatesPgShdescriptionRepro reproduces the same
// shared-comment persistence gap through the pg_shdescription catalog table.
func TestCommentOnDatabasePopulatesPgShdescriptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON DATABASE populates pg_shdescription",
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON DATABASE postgres
						IS 'visible pg_shdescription comment';`,
				},
				{
					Query: `SELECT description
						FROM pg_catalog.pg_shdescription
						WHERE objoid = (SELECT oid FROM pg_database WHERE datname = 'postgres')
							AND classoid = 'pg_database'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentondatabasepopulatespgshdescriptionrepro-0001-select-description-from-pg_catalog.pg_shdescription-where"},
				},
			},
		},
	})
}

// TestCommentOnFunctionPersistsDescriptionRepro reproduces a persistence bug:
// Doltgres accepts COMMENT ON FUNCTION but does not persist the function
// description.
func TestCommentOnFunctionPersistsDescriptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON FUNCTION persists obj_description metadata",
			SetUpScript: []string{
				`CREATE FUNCTION comment_function_target() RETURNS INT LANGUAGE SQL AS $$ SELECT 7 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON FUNCTION comment_function_target() IS 'visible function comment';`,
				},
				{
					Query: `SELECT obj_description(
						(SELECT oid FROM pg_proc WHERE proname = 'comment_function_target'),
						'pg_proc');`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonfunctionpersistsdescriptionrepro-0001-select-obj_description-select-oid-from"},
				},
			},
		},
	})
}

// TestCommentOnAggregatePersistsDescriptionRepro reproduces a persistence gap:
// PostgreSQL stores aggregate comments as pg_proc descriptions, just like
// function comments.
func TestCommentOnAggregatePersistsDescriptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON AGGREGATE persists obj_description metadata",
			SetUpScript: []string{
				`CREATE FUNCTION comment_aggregate_sfunc(state INT, next_value INT)
					RETURNS INT
					LANGUAGE SQL
					IMMUTABLE
					AS $$ SELECT COALESCE(state, 0) + COALESCE(next_value, 0) $$;`,
				`CREATE AGGREGATE comment_aggregate_target(INT) (
					SFUNC = comment_aggregate_sfunc,
					STYPE = INT,
					INITCOND = '0'
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON AGGREGATE comment_aggregate_target(INT)
						IS 'visible aggregate comment';`,
				},
				{
					Query: `SELECT obj_description(
						(SELECT oid FROM pg_proc
						 WHERE proname = 'comment_aggregate_target'),
						'pg_proc');`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonaggregatepersistsdescriptionrepro-0001-select-obj_description-select-oid-from"},
				},
			},
		},
	})
}

// TestCommentOnTypePersistsDescriptionRepro reproduces a persistence bug:
// Doltgres accepts COMMENT ON TYPE but does not persist the type description.
func TestCommentOnTypePersistsDescriptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON TYPE persists obj_description metadata",
			SetUpScript: []string{
				`CREATE TYPE comment_enum_target AS ENUM ('one', 'two');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON TYPE comment_enum_target IS 'visible type comment';`,
				},
				{
					Query: `SELECT obj_description(
						(SELECT oid FROM pg_type WHERE typname = 'comment_enum_target'),
						'pg_type');`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentontypepersistsdescriptionrepro-0001-select-obj_description-select-oid-from"},
				},
			},
		},
	})
}

// TestCommentOnRoleAndExtensionPersistsDescriptionRepro reproduces the same
// persistence bug for roles and extensions.
func TestCommentOnRoleAndExtensionPersistsDescriptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON role and extension persists description metadata",
			SetUpScript: []string{
				`CREATE ROLE role_comment_target;`,
				`CREATE EXTENSION hstore WITH SCHEMA public;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON ROLE role_comment_target IS 'visible role comment';`,
				},
				{
					Query: `COMMENT ON EXTENSION hstore IS 'visible extension comment';`,
				},
				{
					Query: `SELECT
							shobj_description(
								(SELECT oid FROM pg_authid
								 WHERE rolname = 'role_comment_target'),
								'pg_authid'),
							obj_description(
								(SELECT oid FROM pg_extension
								 WHERE extname = 'hstore'),
								'pg_extension');`,
					Expected: []sql.Row{{
						"visible role comment",
						"visible extension comment",
					}},
				},
			},
		},
	})
}

func TestCommentOnPreinstalledPlpgsqlExtensionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON preinstalled plpgsql extension persists description metadata",
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON EXTENSION plpgsql IS 'visible plpgsql extension comment';`,
				},
				{
					Query: `SELECT obj_description(
						(SELECT oid FROM pg_extension WHERE extname = 'plpgsql'),
						'pg_extension');`,
					Expected: []sql.Row{{
						"visible plpgsql extension comment",
					}},
				},
			},
		},
	})
}

// TestCommentOnProcedureRoutineDomainLanguagePersistsDescriptionRepro
// reproduces the same persistence bug for procedures, routines, domains, and
// procedural languages.
func TestCommentOnProcedureRoutineDomainLanguagePersistsDescriptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON procedure routine domain and language persists metadata",
			SetUpScript: []string{
				`CREATE PROCEDURE comment_procedure_target()
					LANGUAGE SQL
					AS $$ SELECT 1 $$;`,
				`CREATE FUNCTION comment_routine_target() RETURNS INT
					LANGUAGE SQL
					AS $$ SELECT 2 $$;`,
				`CREATE DOMAIN comment_domain_target AS INT;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON PROCEDURE comment_procedure_target()
						IS 'visible procedure comment';`,
				},
				{
					Query: `COMMENT ON ROUTINE comment_routine_target()
						IS 'visible routine comment';`,
				},
				{
					Query: `COMMENT ON DOMAIN comment_domain_target
						IS 'visible domain comment';`,
				},
				{
					Query: `COMMENT ON LANGUAGE plpgsql IS 'visible language comment';`,
				},
				{
					Query: `SELECT
							obj_description(
								(SELECT oid FROM pg_proc
								 WHERE proname = 'comment_procedure_target'),
								'pg_proc'),
							obj_description(
								(SELECT oid FROM pg_proc
								 WHERE proname = 'comment_routine_target'),
								'pg_proc'),
							obj_description(
								(SELECT oid FROM pg_type
								 WHERE typname = 'comment_domain_target'),
								'pg_type'),
							obj_description(
								(SELECT oid FROM pg_language
								 WHERE lanname = 'plpgsql'),
								'pg_language');`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonprocedureroutinedomainlanguagepersistsdescriptionrepro-0001-select-obj_description-select-oid-from"},
				},
			},
		},
	})
}

// TestCommentOnCollationAndOperatorPersistsDescriptionRepro reproduces the
// same persistence bug for built-in collations and operators.
func TestCommentOnCollationAndOperatorPersistsDescriptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON collation and operator persists metadata",
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON COLLATION pg_catalog."C"
						IS 'visible collation comment';`,
				},
				{
					Query: `COMMENT ON OPERATOR + (integer, integer)
						IS 'visible operator comment';`,
				},
				{
					Query: `SELECT
							obj_description(
								(SELECT oid FROM pg_collation
								 WHERE collname = 'C'
								   AND collnamespace = 'pg_catalog'::regnamespace),
								'pg_collation'),
							obj_description(
								(SELECT oid FROM pg_operator
								 WHERE oprname = '+'
								   AND oprleft = 'integer'::regtype
								   AND oprright = 'integer'::regtype
								   AND oprnamespace = 'pg_catalog'::regnamespace),
								'pg_operator');`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentoncollationandoperatorpersistsdescriptionrepro-0001-select-obj_description-select-oid-from"},
				},
			},
		},
	})
}

// TestCommentOnAccessMethodPersistsDescriptionRepro reproduces the same
// persistence bug for built-in access methods.
func TestCommentOnAccessMethodPersistsDescriptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON access method persists metadata",
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON ACCESS METHOD btree
						IS 'visible access method comment';`,
				},
				{
					Query: `SELECT obj_description(
						(SELECT oid FROM pg_am WHERE amname = 'btree'),
						'pg_am');`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonaccessmethodpersistsdescriptionrepro-0001-select-obj_description-select-oid-from"},
				},
			},
		},
	})
}

// TestCommentOnPublicationPersistsDescriptionRepro reproduces the same
// persistence bug for publications.
func TestCommentOnPublicationPersistsDescriptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON publication persists metadata",
			SetUpScript: []string{
				`CREATE PUBLICATION comment_publication_target;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON PUBLICATION comment_publication_target
						IS 'visible publication comment';`,
				},
				{
					Query: `SELECT obj_description(
						(SELECT oid FROM pg_publication WHERE pubname = 'comment_publication_target'),
						'pg_publication');`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonpublicationpersistsdescriptionrepro-0001-select-obj_description-select-oid-from"},
				},
			},
		},
	})
}

// TestCommentOnSubscriptionPersistsDescriptionRepro reproduces the same
// persistence bug for subscriptions.
func TestCommentOnSubscriptionPersistsDescriptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON subscription persists metadata",
			SetUpScript: []string{
				`CREATE PUBLICATION comment_subscription_pub;`,
				`CREATE SUBSCRIPTION comment_subscription_target
					CONNECTION 'dbname=regress_doesnotexist'
					PUBLICATION comment_subscription_pub
					WITH (connect = false, enabled = false, create_slot = false, slot_name = NONE);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON SUBSCRIPTION comment_subscription_target
						IS 'visible subscription comment';`,
				},
				{
					Query: `SELECT obj_description(
						(SELECT oid FROM pg_subscription WHERE subname = 'comment_subscription_target'),
						'pg_subscription');`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonsubscriptionpersistsdescriptionrepro-0001-select-obj_description-select-oid-from"},
				},
			},
		},
	})
}

// TestCommentOnTextSearchObjectsPersistsDescriptionRepro reproduces the same
// persistence bug for text-search catalog objects.
func TestCommentOnTextSearchObjectsPersistsDescriptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON text-search objects persists metadata",
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON TEXT SEARCH CONFIGURATION simple
						IS 'visible text search config comment';`,
				},
				{
					Query: `COMMENT ON TEXT SEARCH DICTIONARY simple
						IS 'visible text search dictionary comment';`,
				},
				{
					Query: `COMMENT ON TEXT SEARCH PARSER "default"
						IS 'visible text search parser comment';`,
				},
				{
					Query: `COMMENT ON TEXT SEARCH TEMPLATE simple
						IS 'visible text search template comment';`,
				},
				{
					Query: `SELECT
							obj_description(
								(SELECT oid FROM pg_ts_config
								 WHERE cfgname = 'simple'
								   AND cfgnamespace = 'pg_catalog'::regnamespace),
								'pg_ts_config'),
							obj_description(
								(SELECT oid FROM pg_ts_dict
								 WHERE dictname = 'simple'
								   AND dictnamespace = 'pg_catalog'::regnamespace),
								'pg_ts_dict'),
							obj_description(
								(SELECT oid FROM pg_ts_parser
								 WHERE prsname = 'default'
								   AND prsnamespace = 'pg_catalog'::regnamespace),
								'pg_ts_parser'),
							obj_description(
								(SELECT oid FROM pg_ts_template
								 WHERE tmplname = 'simple'
								   AND tmplnamespace = 'pg_catalog'::regnamespace),
								'pg_ts_template');`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentontextsearchobjectspersistsdescriptionrepro-0001-select-obj_description-select-oid-from"},
				},
			},
		},
	})
}

// TestCommentOnMissingTargetsRequiresExistingObjectRepro reproduces a
// correctness bug: Doltgres accepts COMMENT ON statements without validating
// that the target object exists.
func TestCommentOnMissingTargetsRequiresExistingObjectRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON missing targets errors",
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON TABLE missing_comment_table IS 'ghost table';`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonmissingtargetsrequiresexistingobjectrepro-0001-comment-on-table-missing_comment_table-is", Compare: "sqlstate"},
				},
				{
					Query: `COMMENT ON COLUMN missing_comment_table.value IS 'ghost column';`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonmissingtargetsrequiresexistingobjectrepro-0002-comment-on-column-missing_comment_table.value-is", Compare: "sqlstate"},
				},
				{
					Query: `COMMENT ON FUNCTION missing_comment_function() IS 'ghost function';`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonmissingtargetsrequiresexistingobjectrepro-0003-comment-on-function-missing_comment_function-is", Compare: "sqlstate"},
				},
				{
					Query: `COMMENT ON ROLE missing_comment_role IS 'ghost role';`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonmissingtargetsrequiresexistingobjectrepro-0004-comment-on-role-missing_comment_role-is", Compare: "sqlstate"},
				},
				{
					Query: `COMMENT ON EXTENSION missing_comment_extension IS 'ghost extension';`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonmissingtargetsrequiresexistingobjectrepro-0005-comment-on-extension-missing_comment_extension-is", Compare: "sqlstate"},
				},
				{
					Query: `COMMENT ON PROCEDURE missing_comment_procedure() IS 'ghost procedure';`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonmissingtargetsrequiresexistingobjectrepro-0006-comment-on-procedure-missing_comment_procedure-is", Compare: "sqlstate"},
				},
				{
					Query: `COMMENT ON ROUTINE missing_comment_routine() IS 'ghost routine';`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonmissingtargetsrequiresexistingobjectrepro-0007-comment-on-routine-missing_comment_routine-is", Compare: "sqlstate"},
				},
				{
					Query: `COMMENT ON DOMAIN missing_comment_domain IS 'ghost domain';`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonmissingtargetsrequiresexistingobjectrepro-0008-comment-on-domain-missing_comment_domain-is", Compare: "sqlstate"},
				},
				{
					Query: `COMMENT ON LANGUAGE missing_comment_language IS 'ghost language';`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonmissingtargetsrequiresexistingobjectrepro-0009-comment-on-language-missing_comment_language-is", Compare: "sqlstate"},
				},
				{
					Query: `COMMENT ON COLLATION missing_comment_collation IS 'ghost collation';`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonmissingtargetsrequiresexistingobjectrepro-0010-comment-on-collation-missing_comment_collation-is", Compare: "sqlstate"},
				},
				{
					Query: `COMMENT ON OPERATOR + (integer, boolean) IS 'ghost operator';`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonmissingtargetsrequiresexistingobjectrepro-0011-comment-on-operator-+-integer", Compare: "sqlstate"},
				},
				{
					Query: `COMMENT ON TEXT SEARCH CONFIGURATION missing_comment_ts_config
						IS 'ghost text search config';`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonmissingtargetsrequiresexistingobjectrepro-0012-comment-on-text-search-configuration", Compare: "sqlstate"},
				},
				{
					Query: `COMMENT ON TEXT SEARCH DICTIONARY missing_comment_ts_dict
						IS 'ghost text search dictionary';`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonmissingtargetsrequiresexistingobjectrepro-0013-comment-on-text-search-dictionary", Compare: "sqlstate"},
				},
				{
					Query: `COMMENT ON TEXT SEARCH PARSER missing_comment_ts_parser
						IS 'ghost text search parser';`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonmissingtargetsrequiresexistingobjectrepro-0014-comment-on-text-search-parser", Compare: "sqlstate"},
				},
				{
					Query: `COMMENT ON TEXT SEARCH TEMPLATE missing_comment_ts_template
						IS 'ghost text search template';`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonmissingtargetsrequiresexistingobjectrepro-0015-comment-on-text-search-template",

						// TestCommentOnMissingAccessMethodRequiresExistingObjectRepro reproduces a
						// correctness bug: COMMENT ON ACCESS METHOD should validate that the named
						// access method exists.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCommentOnMissingAccessMethodRequiresExistingObjectRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON missing access method errors",
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON ACCESS METHOD missing_comment_am
						IS 'ghost access method';`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonmissingaccessmethodrequiresexistingobjectrepro-0001-comment-on-access-method-missing_comment_am",

						// TestCommentOnMissingPublicationRequiresExistingObjectRepro reproduces a
						// correctness bug: COMMENT ON PUBLICATION should validate that the named
						// publication exists.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCommentOnMissingPublicationRequiresExistingObjectRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON missing publication errors",
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON PUBLICATION missing_comment_publication
						IS 'ghost publication';`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonmissingpublicationrequiresexistingobjectrepro-0001-comment-on-publication-missing_comment_publication-is",

						// TestCommentOnMissingSubscriptionRequiresExistingObjectRepro reproduces a
						// correctness bug: COMMENT ON SUBSCRIPTION should validate that the named
						// subscription exists.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCommentOnMissingSubscriptionRequiresExistingObjectRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON missing subscription errors",
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON SUBSCRIPTION missing_comment_subscription
						IS 'ghost subscription';`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonmissingsubscriptionrequiresexistingobjectrepro-0001-comment-on-subscription-missing_comment_subscription-is",

						// TestCommentOnMissingPolicyRequiresExistingObjectRepro reproduces a
						// correctness bug: COMMENT ON POLICY should validate that the named policy
						// exists on the target table.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCommentOnMissingPolicyRequiresExistingObjectRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON missing policy errors",
			SetUpScript: []string{
				`CREATE TABLE comment_policy_target (id INT PRIMARY KEY);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON POLICY missing_comment_policy
						ON comment_policy_target IS 'ghost policy';`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonmissingpolicyrequiresexistingobjectrepro-0001-comment-on-policy-missing_comment_policy-on",

						// TestCommentOnMissingLargeObjectRequiresExistingObjectRepro reproduces a
						// correctness bug: COMMENT ON LARGE OBJECT should validate that the large
						// object exists.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCommentOnMissingLargeObjectRequiresExistingObjectRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON missing large object errors",
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON LARGE OBJECT 987654321 IS 'ghost large object';`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonmissinglargeobjectrequiresexistingobjectrepro-0001-comment-on-large-object-987654321",

						// TestCommentOnMissingTablespaceRequiresExistingObjectRepro reproduces a
						// correctness bug: COMMENT ON TABLESPACE should validate that the named
						// tablespace exists.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCommentOnMissingTablespaceRequiresExistingObjectRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON missing tablespace errors",
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON TABLESPACE missing_comment_tablespace IS 'ghost tablespace';`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonmissingtablespacerequiresexistingobjectrepro-0001-comment-on-tablespace-missing_comment_tablespace-is",

						// TestCommentOnTableRequiresOwnershipRepro reproduces a security bug:
						// Doltgres accepts COMMENT ON TABLE from a role that does not own the table.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCommentOnTableRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON TABLE requires table ownership",
			SetUpScript: []string{
				`CREATE USER comment_intruder PASSWORD 'intruder';`,
				`CREATE TABLE comment_private (id INT PRIMARY KEY);`,
				`GRANT USAGE ON SCHEMA public TO comment_intruder;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON TABLE comment_private IS 'unauthorized comment';`,

					Username: `comment_intruder`,
					Password: `intruder`, PostgresOracle: ScriptTestPostgresOracle{

						// TestCommentOnColumnRequiresOwnershipRepro reproduces a security bug:
						// Doltgres accepts COMMENT ON COLUMN from a role that does not own the table.
						ID: "comment-persistence-repro-test-testcommentontablerequiresownershiprepro-0001-comment-on-table-comment_private-is", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCommentOnColumnRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON COLUMN requires table ownership",
			SetUpScript: []string{
				`CREATE USER column_comment_intruder PASSWORD 'intruder';`,
				`CREATE TABLE comment_column_private (id INT PRIMARY KEY, secret TEXT);`,
				`GRANT USAGE ON SCHEMA public TO column_comment_intruder;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON COLUMN comment_column_private.secret IS 'unauthorized column comment';`,

					Username: `column_comment_intruder`,
					Password: `intruder`, PostgresOracle: ScriptTestPostgresOracle{

						// TestCommentOnRelationKindsRequiresOwnershipRepro reproduces the same
						// security bug for other relation kinds: Doltgres accepts COMMENT ON VIEW,
						// COMMENT ON MATERIALIZED VIEW, and COMMENT ON SEQUENCE from a non-owner.
						ID: "comment-persistence-repro-test-testcommentoncolumnrequiresownershiprepro-0001-comment-on-column-comment_column_private.secret-is", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCommentOnRelationKindsRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON relation kinds requires ownership",
			SetUpScript: []string{
				`CREATE USER relation_comment_intruder PASSWORD 'intruder';`,
				`CREATE VIEW comment_private_view AS SELECT 1 AS id;`,
				`CREATE TABLE comment_private_matview_source (id INT PRIMARY KEY);`,
				`INSERT INTO comment_private_matview_source VALUES (1);`,
				`CREATE MATERIALIZED VIEW comment_private_matview AS
					SELECT id FROM comment_private_matview_source;`,
				`CREATE SEQUENCE comment_private_sequence;`,
				`GRANT USAGE ON SCHEMA public TO relation_comment_intruder;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON VIEW comment_private_view IS 'unauthorized view comment';`,

					Username: `relation_comment_intruder`,
					Password: `intruder`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonrelationkindsrequiresownershiprepro-0001-comment-on-view-comment_private_view-is", Compare: "sqlstate"},
				},
				{
					Query: `COMMENT ON MATERIALIZED VIEW comment_private_matview
						IS 'unauthorized materialized view comment';`,

					Username: `relation_comment_intruder`,
					Password: `intruder`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonrelationkindsrequiresownershiprepro-0002-comment-on-materialized-view-comment_private_matview", Compare: "sqlstate"},
				},
				{
					Query: `COMMENT ON SEQUENCE comment_private_sequence IS 'unauthorized sequence comment';`,

					Username: `relation_comment_intruder`,
					Password: `intruder`, PostgresOracle: ScriptTestPostgresOracle{

						// TestCommentOnIndexConstraintTriggerRequiresOwnershipRepro reproduces the same
						// security bug for indexes, constraints, and triggers.
						ID: "comment-persistence-repro-test-testcommentonrelationkindsrequiresownershiprepro-0003-comment-on-sequence-comment_private_sequence-is", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCommentOnIndexConstraintTriggerRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON index constraint and trigger requires ownership",
			SetUpScript: []string{
				`CREATE USER metadata_comment_intruder PASSWORD 'intruder';`,
				`CREATE TABLE comment_metadata_private (
					id INT PRIMARY KEY,
					v INT CONSTRAINT comment_metadata_private_v_positive CHECK (v > 0)
				);`,
				`CREATE INDEX comment_metadata_private_v_idx ON comment_metadata_private (v);`,
				`CREATE FUNCTION comment_metadata_private_trigger_func() RETURNS TRIGGER AS $$
				BEGIN
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER comment_metadata_private_before_insert
					BEFORE INSERT ON comment_metadata_private
					FOR EACH ROW EXECUTE FUNCTION comment_metadata_private_trigger_func();`,
				`GRANT USAGE ON SCHEMA public TO metadata_comment_intruder;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON INDEX comment_metadata_private_v_idx IS 'unauthorized index comment';`,

					Username: `metadata_comment_intruder`,
					Password: `intruder`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonindexconstrainttriggerrequiresownershiprepro-0001-comment-on-index-comment_metadata_private_v_idx-is", Compare: "sqlstate"},
				},
				{
					Query: `COMMENT ON CONSTRAINT comment_metadata_private_v_positive
						ON comment_metadata_private IS 'unauthorized constraint comment';`,

					Username: `metadata_comment_intruder`,
					Password: `intruder`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonindexconstrainttriggerrequiresownershiprepro-0002-comment-on-constraint-comment_metadata_private_v_positive-on", Compare: "sqlstate"},
				},
				{
					Query: `COMMENT ON TRIGGER comment_metadata_private_before_insert
						ON comment_metadata_private IS 'unauthorized trigger comment';`,

					Username: `metadata_comment_intruder`,
					Password: `intruder`, PostgresOracle: ScriptTestPostgresOracle{

						// TestCommentOnSchemaRequiresOwnershipRepro reproduces a security bug:
						// Doltgres accepts COMMENT ON SCHEMA from a role that does not own the schema.
						ID: "comment-persistence-repro-test-testcommentonindexconstrainttriggerrequiresownershiprepro-0003-comment-on-trigger-comment_metadata_private_before_insert-on", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCommentOnSchemaRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON SCHEMA requires schema ownership",
			SetUpScript: []string{
				`CREATE USER schema_comment_intruder PASSWORD 'intruder';`,
				`CREATE SCHEMA comment_private_schema;`,
				`GRANT USAGE ON SCHEMA comment_private_schema TO schema_comment_intruder;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON SCHEMA comment_private_schema IS 'unauthorized schema comment';`,

					Username: `schema_comment_intruder`,
					Password: `intruder`, PostgresOracle: ScriptTestPostgresOracle{

						// TestCommentOnDatabaseRequiresOwnershipRepro reproduces a security bug:
						// Doltgres accepts COMMENT ON DATABASE from a role that does not own the
						// database.
						ID: "comment-persistence-repro-test-testcommentonschemarequiresownershiprepro-0001-comment-on-schema-comment_private_schema-is", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCommentOnDatabaseRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON DATABASE requires database ownership",
			SetUpScript: []string{
				`CREATE USER database_comment_intruder PASSWORD 'intruder';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON DATABASE postgres IS 'unauthorized database comment';`,

					Username: `database_comment_intruder`,
					Password: `intruder`, PostgresOracle: ScriptTestPostgresOracle{

						// TestCommentOnFunctionRequiresOwnershipRepro reproduces a security bug:
						// Doltgres accepts COMMENT ON FUNCTION from a role that does not own the
						// function.
						ID: "comment-persistence-repro-test-testcommentondatabaserequiresownershiprepro-0001-comment-on-database-postgres-is", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCommentOnFunctionRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON FUNCTION requires function ownership",
			SetUpScript: []string{
				`CREATE USER function_comment_intruder PASSWORD 'intruder';`,
				`CREATE FUNCTION comment_private_function() RETURNS INT LANGUAGE SQL AS $$ SELECT 7 $$;`,
				`GRANT USAGE ON SCHEMA public TO function_comment_intruder;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON FUNCTION comment_private_function() IS 'unauthorized function comment';`,

					Username: `function_comment_intruder`,
					Password: `intruder`, PostgresOracle: ScriptTestPostgresOracle{

						// TestCommentOnTypeRequiresOwnershipRepro reproduces a security bug: Doltgres
						// accepts COMMENT ON TYPE from a role that does not own the type.
						ID: "comment-persistence-repro-test-testcommentonfunctionrequiresownershiprepro-0001-comment-on-function-comment_private_function-is", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCommentOnTypeRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON TYPE requires type ownership",
			SetUpScript: []string{
				`CREATE USER type_comment_intruder PASSWORD 'intruder';`,
				`CREATE TYPE comment_private_type AS ENUM ('one', 'two');`,
				`GRANT USAGE ON SCHEMA public TO type_comment_intruder;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON TYPE comment_private_type IS 'unauthorized type comment';`,

					Username: `type_comment_intruder`,
					Password: `intruder`, PostgresOracle: ScriptTestPostgresOracle{

						// TestCommentOnRoleAndExtensionRequiresPrivilegeRepro reproduces the same
						// security bug for roles and extensions.
						ID: "comment-persistence-repro-test-testcommentontyperequiresownershiprepro-0001-comment-on-type-comment_private_type-is", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCommentOnRoleAndExtensionRequiresPrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON role and extension requires privileges",
			SetUpScript: []string{
				`CREATE ROLE comment_role_private;`,
				`CREATE USER comment_role_intruder PASSWORD 'intruder';`,
				`CREATE EXTENSION hstore WITH SCHEMA public;`,
				`CREATE USER comment_extension_intruder PASSWORD 'intruder';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `COMMENT ON ROLE comment_role_private IS 'unauthorized role comment';`,
					ExpectedErr: `permission denied`,
					Username:    `comment_role_intruder`,
					Password:    `intruder`,
				},
				{
					Query:       `COMMENT ON EXTENSION hstore IS 'unauthorized extension comment';`,
					ExpectedErr: `must be owner`,
					Username:    `comment_extension_intruder`,
					Password:    `intruder`,
				},
			},
		},
	})
}

// TestCommentOnProcedureRoutineDomainLanguageRequiresOwnershipRepro reproduces
// the same security bug for procedures, routines, domains, and procedural
// languages.
func TestCommentOnProcedureRoutineDomainLanguageRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON procedure routine domain and language requires ownership",
			SetUpScript: []string{
				`CREATE USER routine_comment_intruder PASSWORD 'intruder';`,
				`CREATE PROCEDURE comment_private_procedure()
					LANGUAGE SQL
					AS $$ SELECT 1 $$;`,
				`CREATE FUNCTION comment_private_routine() RETURNS INT
					LANGUAGE SQL
					AS $$ SELECT 2 $$;`,
				`CREATE DOMAIN comment_private_domain AS INT;`,
				`GRANT USAGE ON SCHEMA public TO routine_comment_intruder;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON PROCEDURE comment_private_procedure()
						IS 'unauthorized procedure comment';`,

					Username: `routine_comment_intruder`,
					Password: `intruder`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonprocedureroutinedomainlanguagerequiresownershiprepro-0001-comment-on-procedure-comment_private_procedure-is", Compare: "sqlstate"},
				},
				{
					Query: `COMMENT ON ROUTINE comment_private_routine()
						IS 'unauthorized routine comment';`,

					Username: `routine_comment_intruder`,
					Password: `intruder`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonprocedureroutinedomainlanguagerequiresownershiprepro-0002-comment-on-routine-comment_private_routine-is", Compare: "sqlstate"},
				},
				{
					Query: `COMMENT ON DOMAIN comment_private_domain
						IS 'unauthorized domain comment';`,

					Username: `routine_comment_intruder`,
					Password: `intruder`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentonprocedureroutinedomainlanguagerequiresownershiprepro-0003-comment-on-domain-comment_private_domain-is", Compare: "sqlstate"},
				},
				{
					Query: `COMMENT ON LANGUAGE plpgsql IS 'unauthorized language comment';`,

					Username: `routine_comment_intruder`,
					Password: `intruder`, PostgresOracle: ScriptTestPostgresOracle{

						// TestCommentOnCollationAndOperatorRequiresOwnershipRepro reproduces the same
						// security bug for built-in collations and operators.
						ID: "comment-persistence-repro-test-testcommentonprocedureroutinedomainlanguagerequiresownershiprepro-0004-comment-on-language-plpgsql-is", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCommentOnCollationAndOperatorRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON collation and operator requires ownership",
			SetUpScript: []string{
				`CREATE USER catalog_comment_intruder PASSWORD 'intruder';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON COLLATION pg_catalog."C"
						IS 'unauthorized collation comment';`,

					Username: `catalog_comment_intruder`,
					Password: `intruder`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentoncollationandoperatorrequiresownershiprepro-0001-comment-on-collation-pg_catalog.-c", Compare: "sqlstate"},
				},
				{
					Query: `COMMENT ON OPERATOR + (integer, integer)
						IS 'unauthorized operator comment';`,

					Username: `catalog_comment_intruder`,
					Password: `intruder`, PostgresOracle: ScriptTestPostgresOracle{

						// TestCommentOnTextSearchObjectsRequiresOwnershipRepro reproduces the same
						// security bug for text-search catalog objects.
						ID: "comment-persistence-repro-test-testcommentoncollationandoperatorrequiresownershiprepro-0002-comment-on-operator-+-integer", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestCommentOnTextSearchObjectsRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON text-search objects requires ownership",
			SetUpScript: []string{
				`CREATE USER text_search_comment_intruder PASSWORD 'intruder';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON TEXT SEARCH CONFIGURATION simple
						IS 'unauthorized text search config comment';`,

					Username: `text_search_comment_intruder`,
					Password: `intruder`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentontextsearchobjectsrequiresownershiprepro-0001-comment-on-text-search-configuration", Compare: "sqlstate"},
				},
				{
					Query: `COMMENT ON TEXT SEARCH DICTIONARY simple
						IS 'unauthorized text search dictionary comment';`,

					Username: `text_search_comment_intruder`,
					Password: `intruder`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentontextsearchobjectsrequiresownershiprepro-0002-comment-on-text-search-dictionary", Compare: "sqlstate"},
				},
				{
					Query: `COMMENT ON TEXT SEARCH PARSER "default"
						IS 'unauthorized text search parser comment';`,

					Username: `text_search_comment_intruder`,
					Password: `intruder`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentontextsearchobjectsrequiresownershiprepro-0003-comment-on-text-search-parser", Compare: "sqlstate"},
				},
				{
					Query: `COMMENT ON TEXT SEARCH TEMPLATE simple
						IS 'unauthorized text search template comment';`,

					Username: `text_search_comment_intruder`,
					Password: `intruder`, PostgresOracle: ScriptTestPostgresOracle{ID: "comment-persistence-repro-test-testcommentontextsearchobjectsrequiresownershiprepro-0004-comment-on-text-search-template", Compare: "sqlstate"},
				},
			},
		},
	})
}
