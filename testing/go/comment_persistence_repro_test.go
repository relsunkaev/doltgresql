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
					Query:    `SELECT col_description('comment_target'::regclass, 2);`,
					Expected: []sql.Row{{"visible label comment"}},
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
					Query:    `SELECT obj_description('table_comment_target'::regclass);`,
					Expected: []sql.Row{{"visible table comment"}},
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
							obj_description('sequence_comment_target'::regclass);`,
					Expected: []sql.Row{{
						"visible view comment",
						"visible materialized view comment",
						"visible sequence comment",
					}},
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
								'pg_trigger');`,
					Expected: []sql.Row{{
						"visible index comment",
						"visible constraint comment",
						"visible trigger comment",
					}},
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
						'pg_namespace');`,
					Expected: []sql.Row{{"visible schema comment"}},
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
						'pg_database');`,
					Expected: []sql.Row{{"visible database comment"}},
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
						'pg_proc');`,
					Expected: []sql.Row{{"visible function comment"}},
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
						'pg_type');`,
					Expected: []sql.Row{{"visible type comment"}},
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
								'pg_language');`,
					Expected: []sql.Row{{
						"visible procedure comment",
						"visible routine comment",
						"visible domain comment",
						"visible language comment",
					}},
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
								'pg_operator');`,
					Expected: []sql.Row{{
						"visible collation comment",
						"visible operator comment",
					}},
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
						'pg_am');`,
					Expected: []sql.Row{{"visible access method comment"}},
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
						'pg_publication');`,
					Expected: []sql.Row{{"visible publication comment"}},
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
						'pg_subscription');`,
					Expected: []sql.Row{{"visible subscription comment"}},
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
								'pg_ts_template');`,
					Expected: []sql.Row{{
						"visible text search config comment",
						"visible text search dictionary comment",
						"visible text search parser comment",
						"visible text search template comment",
					}},
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
					Query:       `COMMENT ON TABLE missing_comment_table IS 'ghost table';`,
					ExpectedErr: `does not exist`,
				},
				{
					Query:       `COMMENT ON COLUMN missing_comment_table.value IS 'ghost column';`,
					ExpectedErr: `does not exist`,
				},
				{
					Query:       `COMMENT ON FUNCTION missing_comment_function() IS 'ghost function';`,
					ExpectedErr: `does not exist`,
				},
				{
					Query:       `COMMENT ON ROLE missing_comment_role IS 'ghost role';`,
					ExpectedErr: `does not exist`,
				},
				{
					Query:       `COMMENT ON EXTENSION missing_comment_extension IS 'ghost extension';`,
					ExpectedErr: `does not exist`,
				},
				{
					Query:       `COMMENT ON PROCEDURE missing_comment_procedure() IS 'ghost procedure';`,
					ExpectedErr: `does not exist`,
				},
				{
					Query:       `COMMENT ON ROUTINE missing_comment_routine() IS 'ghost routine';`,
					ExpectedErr: `does not exist`,
				},
				{
					Query:       `COMMENT ON DOMAIN missing_comment_domain IS 'ghost domain';`,
					ExpectedErr: `does not exist`,
				},
				{
					Query:       `COMMENT ON LANGUAGE missing_comment_language IS 'ghost language';`,
					ExpectedErr: `does not exist`,
				},
				{
					Query:       `COMMENT ON COLLATION missing_comment_collation IS 'ghost collation';`,
					ExpectedErr: `does not exist`,
				},
				{
					Query:       `COMMENT ON OPERATOR + (integer, boolean) IS 'ghost operator';`,
					ExpectedErr: `does not exist`,
				},
				{
					Query: `COMMENT ON TEXT SEARCH CONFIGURATION missing_comment_ts_config
						IS 'ghost text search config';`,
					ExpectedErr: `does not exist`,
				},
				{
					Query: `COMMENT ON TEXT SEARCH DICTIONARY missing_comment_ts_dict
						IS 'ghost text search dictionary';`,
					ExpectedErr: `does not exist`,
				},
				{
					Query: `COMMENT ON TEXT SEARCH PARSER missing_comment_ts_parser
						IS 'ghost text search parser';`,
					ExpectedErr: `does not exist`,
				},
				{
					Query: `COMMENT ON TEXT SEARCH TEMPLATE missing_comment_ts_template
						IS 'ghost text search template';`,
					ExpectedErr: `does not exist`,
				},
			},
		},
	})
}

// TestCommentOnMissingAccessMethodRequiresExistingObjectRepro reproduces a
// correctness bug: COMMENT ON ACCESS METHOD should validate that the named
// access method exists.
func TestCommentOnMissingAccessMethodRequiresExistingObjectRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON missing access method errors",
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON ACCESS METHOD missing_comment_am
						IS 'ghost access method';`,
					ExpectedErr: `does not exist`,
				},
			},
		},
	})
}

// TestCommentOnMissingPublicationRequiresExistingObjectRepro reproduces a
// correctness bug: COMMENT ON PUBLICATION should validate that the named
// publication exists.
func TestCommentOnMissingPublicationRequiresExistingObjectRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON missing publication errors",
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON PUBLICATION missing_comment_publication
						IS 'ghost publication';`,
					ExpectedErr: `does not exist`,
				},
			},
		},
	})
}

// TestCommentOnMissingSubscriptionRequiresExistingObjectRepro reproduces a
// correctness bug: COMMENT ON SUBSCRIPTION should validate that the named
// subscription exists.
func TestCommentOnMissingSubscriptionRequiresExistingObjectRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON missing subscription errors",
			Assertions: []ScriptTestAssertion{
				{
					Query: `COMMENT ON SUBSCRIPTION missing_comment_subscription
						IS 'ghost subscription';`,
					ExpectedErr: `does not exist`,
				},
			},
		},
	})
}

// TestCommentOnMissingPolicyRequiresExistingObjectRepro reproduces a
// correctness bug: COMMENT ON POLICY should validate that the named policy
// exists on the target table.
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
						ON comment_policy_target IS 'ghost policy';`,
					ExpectedErr: `does not exist`,
				},
			},
		},
	})
}

// TestCommentOnMissingLargeObjectRequiresExistingObjectRepro reproduces a
// correctness bug: COMMENT ON LARGE OBJECT should validate that the large
// object exists.
func TestCommentOnMissingLargeObjectRequiresExistingObjectRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON missing large object errors",
			Assertions: []ScriptTestAssertion{
				{
					Query:       `COMMENT ON LARGE OBJECT 987654321 IS 'ghost large object';`,
					ExpectedErr: `does not exist`,
				},
			},
		},
	})
}

// TestCommentOnMissingTablespaceRequiresExistingObjectRepro reproduces a
// correctness bug: COMMENT ON TABLESPACE should validate that the named
// tablespace exists.
func TestCommentOnMissingTablespaceRequiresExistingObjectRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON missing tablespace errors",
			Assertions: []ScriptTestAssertion{
				{
					Query:       `COMMENT ON TABLESPACE missing_comment_tablespace IS 'ghost tablespace';`,
					ExpectedErr: `does not exist`,
				},
			},
		},
	})
}

// TestCommentOnTableRequiresOwnershipRepro reproduces a security bug:
// Doltgres accepts COMMENT ON TABLE from a role that does not own the table.
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
					Query:       `COMMENT ON TABLE comment_private IS 'unauthorized comment';`,
					ExpectedErr: `permission denied`,
					Username:    `comment_intruder`,
					Password:    `intruder`,
				},
			},
		},
	})
}

// TestCommentOnColumnRequiresOwnershipRepro reproduces a security bug:
// Doltgres accepts COMMENT ON COLUMN from a role that does not own the table.
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
					Query:       `COMMENT ON COLUMN comment_column_private.secret IS 'unauthorized column comment';`,
					ExpectedErr: `permission denied`,
					Username:    `column_comment_intruder`,
					Password:    `intruder`,
				},
			},
		},
	})
}

// TestCommentOnRelationKindsRequiresOwnershipRepro reproduces the same
// security bug for other relation kinds: Doltgres accepts COMMENT ON VIEW,
// COMMENT ON MATERIALIZED VIEW, and COMMENT ON SEQUENCE from a non-owner.
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
					Query:       `COMMENT ON VIEW comment_private_view IS 'unauthorized view comment';`,
					ExpectedErr: `permission denied`,
					Username:    `relation_comment_intruder`,
					Password:    `intruder`,
				},
				{
					Query: `COMMENT ON MATERIALIZED VIEW comment_private_matview
						IS 'unauthorized materialized view comment';`,
					ExpectedErr: `permission denied`,
					Username:    `relation_comment_intruder`,
					Password:    `intruder`,
				},
				{
					Query:       `COMMENT ON SEQUENCE comment_private_sequence IS 'unauthorized sequence comment';`,
					ExpectedErr: `permission denied`,
					Username:    `relation_comment_intruder`,
					Password:    `intruder`,
				},
			},
		},
	})
}

// TestCommentOnIndexConstraintTriggerRequiresOwnershipRepro reproduces the same
// security bug for indexes, constraints, and triggers.
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
					Query:       `COMMENT ON INDEX comment_metadata_private_v_idx IS 'unauthorized index comment';`,
					ExpectedErr: `permission denied`,
					Username:    `metadata_comment_intruder`,
					Password:    `intruder`,
				},
				{
					Query: `COMMENT ON CONSTRAINT comment_metadata_private_v_positive
						ON comment_metadata_private IS 'unauthorized constraint comment';`,
					ExpectedErr: `permission denied`,
					Username:    `metadata_comment_intruder`,
					Password:    `intruder`,
				},
				{
					Query: `COMMENT ON TRIGGER comment_metadata_private_before_insert
						ON comment_metadata_private IS 'unauthorized trigger comment';`,
					ExpectedErr: `permission denied`,
					Username:    `metadata_comment_intruder`,
					Password:    `intruder`,
				},
			},
		},
	})
}

// TestCommentOnSchemaRequiresOwnershipRepro reproduces a security bug:
// Doltgres accepts COMMENT ON SCHEMA from a role that does not own the schema.
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
					Query:       `COMMENT ON SCHEMA comment_private_schema IS 'unauthorized schema comment';`,
					ExpectedErr: `permission denied`,
					Username:    `schema_comment_intruder`,
					Password:    `intruder`,
				},
			},
		},
	})
}

// TestCommentOnDatabaseRequiresOwnershipRepro reproduces a security bug:
// Doltgres accepts COMMENT ON DATABASE from a role that does not own the
// database.
func TestCommentOnDatabaseRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON DATABASE requires database ownership",
			SetUpScript: []string{
				`CREATE USER database_comment_intruder PASSWORD 'intruder';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `COMMENT ON DATABASE postgres IS 'unauthorized database comment';`,
					ExpectedErr: `permission denied`,
					Username:    `database_comment_intruder`,
					Password:    `intruder`,
				},
			},
		},
	})
}

// TestCommentOnFunctionRequiresOwnershipRepro reproduces a security bug:
// Doltgres accepts COMMENT ON FUNCTION from a role that does not own the
// function.
func TestCommentOnFunctionRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON FUNCTION requires function ownership",
			SetUpScript: []string{
				`CREATE USER function_comment_intruder PASSWORD 'intruder';`,
				`CREATE FUNCTION comment_private_function() RETURNS INT LANGUAGE SQL AS $$ SELECT 7 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `COMMENT ON FUNCTION comment_private_function() IS 'unauthorized function comment';`,
					ExpectedErr: `permission denied`,
					Username:    `function_comment_intruder`,
					Password:    `intruder`,
				},
			},
		},
	})
}

// TestCommentOnTypeRequiresOwnershipRepro reproduces a security bug: Doltgres
// accepts COMMENT ON TYPE from a role that does not own the type.
func TestCommentOnTypeRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COMMENT ON TYPE requires type ownership",
			SetUpScript: []string{
				`CREATE USER type_comment_intruder PASSWORD 'intruder';`,
				`CREATE TYPE comment_private_type AS ENUM ('one', 'two');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `COMMENT ON TYPE comment_private_type IS 'unauthorized type comment';`,
					ExpectedErr: `permission denied`,
					Username:    `type_comment_intruder`,
					Password:    `intruder`,
				},
			},
		},
	})
}

// TestCommentOnRoleAndExtensionRequiresPrivilegeRepro reproduces the same
// security bug for roles and extensions.
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
					ExpectedErr: `must be owner`,
					Username:    `routine_comment_intruder`,
					Password:    `intruder`,
				},
				{
					Query: `COMMENT ON ROUTINE comment_private_routine()
						IS 'unauthorized routine comment';`,
					ExpectedErr: `must be owner`,
					Username:    `routine_comment_intruder`,
					Password:    `intruder`,
				},
				{
					Query: `COMMENT ON DOMAIN comment_private_domain
						IS 'unauthorized domain comment';`,
					ExpectedErr: `must be owner`,
					Username:    `routine_comment_intruder`,
					Password:    `intruder`,
				},
				{
					Query:       `COMMENT ON LANGUAGE plpgsql IS 'unauthorized language comment';`,
					ExpectedErr: `must be owner`,
					Username:    `routine_comment_intruder`,
					Password:    `intruder`,
				},
			},
		},
	})
}

// TestCommentOnCollationAndOperatorRequiresOwnershipRepro reproduces the same
// security bug for built-in collations and operators.
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
					ExpectedErr: `must be owner`,
					Username:    `catalog_comment_intruder`,
					Password:    `intruder`,
				},
				{
					Query: `COMMENT ON OPERATOR + (integer, integer)
						IS 'unauthorized operator comment';`,
					ExpectedErr: `must be owner`,
					Username:    `catalog_comment_intruder`,
					Password:    `intruder`,
				},
			},
		},
	})
}

// TestCommentOnTextSearchObjectsRequiresOwnershipRepro reproduces the same
// security bug for text-search catalog objects.
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
					ExpectedErr: `must be owner`,
					Username:    `text_search_comment_intruder`,
					Password:    `intruder`,
				},
				{
					Query: `COMMENT ON TEXT SEARCH DICTIONARY simple
						IS 'unauthorized text search dictionary comment';`,
					ExpectedErr: `must be owner`,
					Username:    `text_search_comment_intruder`,
					Password:    `intruder`,
				},
				{
					Query: `COMMENT ON TEXT SEARCH PARSER "default"
						IS 'unauthorized text search parser comment';`,
					ExpectedErr: `must be superuser`,
					Username:    `text_search_comment_intruder`,
					Password:    `intruder`,
				},
				{
					Query: `COMMENT ON TEXT SEARCH TEMPLATE simple
						IS 'unauthorized text search template comment';`,
					ExpectedErr: `must be superuser`,
					Username:    `text_search_comment_intruder`,
					Password:    `intruder`,
				},
			},
		},
	})
}
