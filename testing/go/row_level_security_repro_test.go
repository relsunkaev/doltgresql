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
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestRowLevelSecurityDefaultDenyRepro reproduces a security bug: Doltgres
// accepts ALTER TABLE ... ENABLE ROW LEVEL SECURITY but does not enforce the
// PostgreSQL default-deny behavior for non-owners when no policies exist.
func TestRowLevelSecurityDefaultDenyRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RLS enabled without policies hides rows from granted non-owner",
			SetUpScript: []string{
				`CREATE USER rls_reader PASSWORD 'reader';`,
				`CREATE TABLE rls_secrets (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO rls_secrets VALUES (1, 'alpha'), (2, 'beta');`,
				`GRANT USAGE ON SCHEMA public TO rls_reader;`,
				`GRANT SELECT ON rls_secrets TO rls_reader;`,
				`ALTER TABLE rls_secrets ENABLE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label FROM rls_secrets ORDER BY id;`,

					Username: `rls_reader`,
					Password: `reader`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-repro-test-testrowlevelsecuritydefaultdenyrepro-0001-select-id-label-from-rls_secrets"},

					// TestRowLevelSecuritySelectPolicyFiltersRowsRepro reproduces a security bug:
					// PostgreSQL applies SELECT policies to rows visible to granted non-owners.

				},
			},
		},
	})
}

func TestRowLevelSecuritySelectPolicyFiltersRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RLS SELECT policy filters visible rows",
			SetUpScript: []string{
				`CREATE USER rls_policy_reader PASSWORD 'reader';`,
				`CREATE TABLE rls_policy_docs (
					id INT PRIMARY KEY,
					owner_name TEXT,
					label TEXT
				);`,
				`INSERT INTO rls_policy_docs VALUES
					(1, 'rls_policy_reader', 'visible'),
					(2, 'other_user', 'hidden');`,
				`GRANT USAGE ON SCHEMA public TO rls_policy_reader;`,
				`GRANT SELECT ON rls_policy_docs TO rls_policy_reader;`,
				`CREATE POLICY rls_policy_docs_owner_select
					ON rls_policy_docs
					FOR SELECT
					USING (owner_name = current_user);`,
				`ALTER TABLE rls_policy_docs ENABLE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label
						FROM rls_policy_docs
						ORDER BY id;`,

					Username: `rls_policy_reader`,
					Password: `reader`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-repro-test-testrowlevelsecurityselectpolicyfiltersrowsrepro-0001-select-id-label-from-rls_policy_docs"},

					// TestRowSecurityOffDoesNotBypassPoliciesRepro reproduces a security bug:
					// PostgreSQL's row_security=off mode does not bypass RLS for ordinary users;
					// it errors when a query would be affected by row-level security.

				},
			},
		},
	})
}

func TestRowSecurityOffDoesNotBypassPoliciesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "row_security off does not bypass policies",
			SetUpScript: []string{
				`CREATE USER rls_guc_reader PASSWORD 'reader';`,
				`CREATE TABLE rls_guc_docs (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO rls_guc_docs VALUES
					(1, 'alpha'),
					(2, 'beta');`,
				`GRANT USAGE ON SCHEMA public TO rls_guc_reader;`,
				`GRANT SELECT ON rls_guc_docs TO rls_guc_reader;`,
				`ALTER TABLE rls_guc_docs ENABLE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SET row_security = off;`,
					Username: `rls_guc_reader`,
					Password: `reader`,
				},
				{
					Query: `SELECT id, label
						FROM rls_guc_docs
						ORDER BY id;`,

					Username: `rls_guc_reader`,
					Password: `reader`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-repro-test-testrowsecurityoffdoesnotbypasspoliciesrepro-0001-select-id-label-from-rls_guc_docs", Compare: "sqlstate"},

					// TestDropPolicyIfExistsMissingRepro reproduces a PostgreSQL compatibility gap:
					// DROP POLICY IF EXISTS should no-op when the named policy is absent on an
					// existing table.

				},
			},
		},
	})
}

func TestDropPolicyIfExistsMissingRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP POLICY IF EXISTS missing policy no-ops",
			SetUpScript: []string{
				`CREATE TABLE rls_drop_docs (
					id INT PRIMARY KEY,
					label TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP POLICY IF EXISTS missing_rls_drop_policy
						ON rls_drop_docs;`,
				},
			},
		},
	})
}

// TestRowLevelSecurityDefaultDenyInsertRepro reproduces a security bug:
// PostgreSQL default-deny RLS blocks INSERT for granted non-owners when no
// INSERT policy exists.
func TestRowLevelSecurityDefaultDenyInsertRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RLS enabled without policies blocks inserts from granted non-owner",
			SetUpScript: []string{
				`CREATE USER rls_insert_user PASSWORD 'writer';`,
				`CREATE TABLE rls_insert_secrets (id INT PRIMARY KEY, label TEXT);`,
				`GRANT USAGE ON SCHEMA public TO rls_insert_user;`,
				`GRANT INSERT ON rls_insert_secrets TO rls_insert_user;`,
				`ALTER TABLE rls_insert_secrets ENABLE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO rls_insert_secrets VALUES (1, 'leaked');`,

					Username: `rls_insert_user`,
					Password: `writer`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-repro-test-testrowlevelsecuritydefaultdenyinsertrepro-0001-insert-into-rls_insert_secrets-values-1", Compare: "sqlstate"},
				},
				{
					Query: `SELECT count(*) FROM rls_insert_secrets;`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-repro-test-testrowlevelsecuritydefaultdenyinsertrepro-0002-select-count-*-from-rls_insert_secrets"},
				},
			},
		},
	})
}

// TestRowLevelSecurityInsertPolicyWithCheckRepro reproduces a security bug:
// PostgreSQL enforces INSERT policy WITH CHECK expressions for granted
// non-owners.
func TestRowLevelSecurityInsertPolicyWithCheckRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RLS INSERT WITH CHECK rejects invalid rows",
			SetUpScript: []string{
				`CREATE USER rls_insert_check_user PASSWORD 'writer';`,
				`CREATE TABLE rls_insert_check_docs (
					id INT PRIMARY KEY,
					owner_name TEXT,
					label TEXT
				);`,
				`GRANT USAGE ON SCHEMA public TO rls_insert_check_user;`,
				`GRANT INSERT, SELECT ON rls_insert_check_docs TO rls_insert_check_user;`,
				`CREATE POLICY rls_insert_check_owner_insert
					ON rls_insert_check_docs
					FOR INSERT
					WITH CHECK (owner_name = current_user);`,
				`ALTER TABLE rls_insert_check_docs ENABLE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO rls_insert_check_docs
						VALUES (1, 'rls_insert_check_user', 'allowed');`,
					Username: `rls_insert_check_user`,
					Password: `writer`,
				},
				{
					Query: `INSERT INTO rls_insert_check_docs
						VALUES (2, 'other_user', 'blocked');`,

					Username: `rls_insert_check_user`,
					Password: `writer`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-repro-test-testrowlevelsecurityinsertpolicywithcheckrepro-0001-insert-into-rls_insert_check_docs-values-2", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, owner_name, label
						FROM rls_insert_check_docs
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-repro-test-testrowlevelsecurityinsertpolicywithcheckrepro-0002-select-id-owner_name-label-from"},
				},
			},
		},
	})
}

// TestRowLevelSecurityUpdatePolicyWithCheckRepro reproduces an RLS security
// gap: PostgreSQL accepts UPDATE policies with USING and WITH CHECK
// expressions and applies them to granted non-owners.
func TestRowLevelSecurityUpdatePolicyWithCheckRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RLS UPDATE policy filters rows and rejects invalid new rows",
			SetUpScript: []string{
				`CREATE USER rls_update_policy_user PASSWORD 'writer';`,
				`CREATE TABLE rls_update_policy_docs (
					id INT PRIMARY KEY,
					owner_name TEXT,
					label TEXT
				);`,
				`INSERT INTO rls_update_policy_docs VALUES
					(1, 'rls_update_policy_user', 'visible'),
					(2, 'other_user', 'hidden');`,
				`GRANT USAGE ON SCHEMA public TO rls_update_policy_user;`,
				`GRANT SELECT, UPDATE ON rls_update_policy_docs TO rls_update_policy_user;`,
				`CREATE POLICY rls_update_policy_docs_owner_update
					ON rls_update_policy_docs
					FOR UPDATE
					USING (owner_name = current_user)
					WITH CHECK (owner_name = current_user);`,
				`ALTER TABLE rls_update_policy_docs ENABLE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE rls_update_policy_docs
						SET label = 'changed'
						WHERE id = 1
						RETURNING id;`,

					Username: `rls_update_policy_user`,
					Password: `writer`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-repro-test-testrowlevelsecurityupdatepolicywithcheckrepro-0001-update-rls_update_policy_docs-set-label-=", Compare: "sqlstate"},
				},
				{
					Query: `UPDATE rls_update_policy_docs
						SET label = 'blocked'
						WHERE id = 2
						RETURNING id;`,

					Username: `rls_update_policy_user`,
					Password: `writer`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-repro-test-testrowlevelsecurityupdatepolicywithcheckrepro-0002-update-rls_update_policy_docs-set-label-=", Compare: "sqlstate"},
				},
				{
					Query: `UPDATE rls_update_policy_docs
						SET owner_name = 'other_user'
						WHERE id = 1;`,

					Username: `rls_update_policy_user`,
					Password: `writer`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-repro-test-testrowlevelsecurityupdatepolicywithcheckrepro-0003-update-rls_update_policy_docs-set-owner_name-=", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, owner_name, label
						FROM rls_update_policy_docs
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-repro-test-testrowlevelsecurityupdatepolicywithcheckrepro-0004-select-id-owner_name-label-from"},
				},
			},
		},
	})
}

// TestRowLevelSecurityDefaultDenyUpdateRepro reproduces a security bug:
// PostgreSQL default-deny RLS hides target rows from UPDATE for granted
// non-owners when no UPDATE policy exists.
func TestRowLevelSecurityDefaultDenyUpdateRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RLS enabled without policies blocks updates from granted non-owner",
			SetUpScript: []string{
				`CREATE USER rls_update_user PASSWORD 'writer';`,
				`CREATE TABLE rls_update_secrets (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO rls_update_secrets VALUES (1, 'original');`,
				`GRANT USAGE ON SCHEMA public TO rls_update_user;`,
				`GRANT SELECT, UPDATE ON rls_update_secrets TO rls_update_user;`,
				`ALTER TABLE rls_update_secrets ENABLE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE rls_update_secrets
						SET label = 'changed'
						WHERE id = 1
						RETURNING id;`,

					Username: `rls_update_user`,
					Password: `writer`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-repro-test-testrowlevelsecuritydefaultdenyupdaterepro-0001-update-rls_update_secrets-set-label-=", Compare: "sqlstate"},
				},
				{
					Query: `SELECT label FROM rls_update_secrets WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-repro-test-testrowlevelsecuritydefaultdenyupdaterepro-0002-select-label-from-rls_update_secrets-where"},
				},
			},
		},
	})
}

// TestRowLevelSecurityDeletePolicyFiltersRowsRepro reproduces an RLS security
// gap: PostgreSQL accepts DELETE policies and applies their USING expressions
// to granted non-owners.
func TestRowLevelSecurityDeletePolicyFiltersRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RLS DELETE policy filters target rows",
			SetUpScript: []string{
				`CREATE USER rls_delete_policy_user PASSWORD 'writer';`,
				`CREATE TABLE rls_delete_policy_docs (
					id INT PRIMARY KEY,
					owner_name TEXT,
					label TEXT
				);`,
				`INSERT INTO rls_delete_policy_docs VALUES
					(1, 'rls_delete_policy_user', 'delete me'),
					(2, 'other_user', 'keep me');`,
				`GRANT USAGE ON SCHEMA public TO rls_delete_policy_user;`,
				`GRANT SELECT, DELETE ON rls_delete_policy_docs TO rls_delete_policy_user;`,
				`CREATE POLICY rls_delete_policy_docs_owner_delete
					ON rls_delete_policy_docs
					FOR DELETE
					USING (owner_name = current_user);`,
				`ALTER TABLE rls_delete_policy_docs ENABLE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DELETE FROM rls_delete_policy_docs
						RETURNING id;`,

					Username: `rls_delete_policy_user`,
					Password: `writer`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-repro-test-testrowlevelsecuritydeletepolicyfiltersrowsrepro-0001-delete-from-rls_delete_policy_docs-returning-id", Compare: "sqlstate"},
				},
				{
					Query: `SELECT id, owner_name, label
						FROM rls_delete_policy_docs
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-repro-test-testrowlevelsecuritydeletepolicyfiltersrowsrepro-0002-select-id-owner_name-label-from"},
				},
			},
		},
	})
}

// TestRowLevelSecurityDefaultDenyDeleteRepro reproduces a security bug:
// PostgreSQL default-deny RLS hides target rows from DELETE for granted
// non-owners when no DELETE policy exists.
func TestRowLevelSecurityDefaultDenyDeleteRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RLS enabled without policies blocks deletes from granted non-owner",
			SetUpScript: []string{
				`CREATE USER rls_delete_user PASSWORD 'writer';`,
				`CREATE TABLE rls_delete_secrets (id INT PRIMARY KEY, label TEXT);`,
				`INSERT INTO rls_delete_secrets VALUES (1, 'keep');`,
				`GRANT USAGE ON SCHEMA public TO rls_delete_user;`,
				`GRANT SELECT, DELETE ON rls_delete_secrets TO rls_delete_user;`,
				`ALTER TABLE rls_delete_secrets ENABLE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DELETE FROM rls_delete_secrets
						WHERE id = 1
						RETURNING id;`,

					Username: `rls_delete_user`,
					Password: `writer`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-repro-test-testrowlevelsecuritydefaultdenydeleterepro-0001-delete-from-rls_delete_secrets-where-id", Compare: "sqlstate"},
				},
				{
					Query: `SELECT count(*) FROM rls_delete_secrets;`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-repro-test-testrowlevelsecuritydefaultdenydeleterepro-0002-select-count-*-from-rls_delete_secrets"},
				},
			},
		},
	})
}

// TestRowLevelSecurityBlocksCopyFromRepro reproduces a security bug:
// PostgreSQL does not support COPY FROM on tables with row-level security,
// because the COPY path cannot apply INSERT policies row-by-row.
func TestRowLevelSecurityBlocksCopyFromRepro(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, connection, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	for _, stmt := range []string{
		`CREATE USER rls_copy_user PASSWORD 'writer';`,
		`CREATE TABLE rls_copy_secrets (id INT PRIMARY KEY, label TEXT);`,
		`GRANT USAGE ON SCHEMA public TO rls_copy_user;`,
		`GRANT INSERT ON rls_copy_secrets TO rls_copy_user;`,
		`ALTER TABLE rls_copy_secrets ENABLE ROW LEVEL SECURITY;`,
	} {
		_, err = connection.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	writerConn, err := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://rls_copy_user:writer@127.0.0.1:%d/postgres?sslmode=disable",
		port,
	))
	require.NoError(t, err)
	defer writerConn.Close(context.Background())

	tag, err := writerConn.PgConn().CopyFrom(
		ctx,
		strings.NewReader("1\tcopied\n"),
		`COPY rls_copy_secrets (id, label) FROM STDIN;`,
	)
	if err == nil {
		var count int64
		require.NoError(t, connection.Default.QueryRow(
			context.Background(),
			`SELECT count(*) FROM rls_copy_secrets;`,
		).Scan(&count))
		require.Equal(t, int64(0), count, "COPY FROM should reject RLS-enabled tables; tag=%s", tag.String())
	}
	require.ErrorContains(t, err, "row-level security")
}

// TestRowLevelSecurityFiltersCopyToRepro reproduces a security bug: PostgreSQL
// applies RLS visibility rules to COPY TO just like SELECT.
func TestRowLevelSecurityFiltersCopyToRepro(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, connection, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	for _, stmt := range []string{
		`CREATE USER rls_copy_reader PASSWORD 'reader';`,
		`CREATE TABLE rls_copy_visible (id INT PRIMARY KEY, label TEXT);`,
		`INSERT INTO rls_copy_visible VALUES (1, 'hidden');`,
		`GRANT USAGE ON SCHEMA public TO rls_copy_reader;`,
		`GRANT SELECT ON rls_copy_visible TO rls_copy_reader;`,
		`ALTER TABLE rls_copy_visible ENABLE ROW LEVEL SECURITY;`,
	} {
		_, err = connection.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	readerConn, err := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://rls_copy_reader:reader@127.0.0.1:%d/postgres?sslmode=disable",
		port,
	))
	require.NoError(t, err)
	defer readerConn.Close(context.Background())

	var out bytes.Buffer
	tag, err := readerConn.PgConn().CopyTo(
		ctx,
		&out,
		`COPY rls_copy_visible (id, label) TO STDOUT;`,
	)
	require.NoError(t, err)
	require.Equal(t, "COPY 0", tag.String())
	require.Equal(t, "", out.String())
}

// TestRowLevelSecurityPgClassMetadataRepro reproduces a catalog persistence
// bug: ALTER TABLE row-level security settings should update pg_class metadata.
func TestRowLevelSecurityPgClassMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RLS settings update pg_class metadata",
			SetUpScript: []string{
				`CREATE TABLE rls_catalog_target (id INT PRIMARY KEY);`,
				`ALTER TABLE rls_catalog_target ENABLE ROW LEVEL SECURITY;`,
				`ALTER TABLE rls_catalog_target FORCE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT relrowsecurity, relforcerowsecurity
						FROM pg_catalog.pg_class
						WHERE oid = 'rls_catalog_target'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-repro-test-testrowlevelsecuritypgclassmetadatarepro-0001-select-relrowsecurity-relforcerowsecurity-from-pg_catalog.pg_class"},
				},
			},
		},
	})
}

// TestRowLevelSecurityNoForcePgClassMetadataRepro reproduces a catalog
// persistence bug: ALTER TABLE ... NO FORCE ROW LEVEL SECURITY should clear
// the forced-RLS table mode without disabling RLS.
func TestRowLevelSecurityNoForcePgClassMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "NO FORCE RLS clears pg_class forced metadata",
			SetUpScript: []string{
				`CREATE TABLE rls_no_force_catalog_target (id INT PRIMARY KEY);`,
				`ALTER TABLE rls_no_force_catalog_target ENABLE ROW LEVEL SECURITY;`,
				`ALTER TABLE rls_no_force_catalog_target FORCE ROW LEVEL SECURITY;`,
				`ALTER TABLE rls_no_force_catalog_target NO FORCE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT relrowsecurity, relforcerowsecurity
						FROM pg_catalog.pg_class
						WHERE oid = 'rls_no_force_catalog_target'::regclass;`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-repro-test-testrowlevelsecuritynoforcepgclassmetadatarepro-0001-select-relrowsecurity-relforcerowsecurity-from-pg_catalog.pg_class"},
				},
			},
		},
	})
}

// TestRowLevelSecurityTableOwnerBypassesPolicyUnlessForcedRepro reproduces a
// row-level security bug: PostgreSQL lets the table owner bypass RLS unless the
// table is marked FORCE ROW LEVEL SECURITY, but Doltgres applies default-deny
// RLS to the owner.
func TestRowLevelSecurityTableOwnerBypassesPolicyUnlessForcedRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "table owner bypasses non-forced RLS",
			SetUpScript: []string{
				`CREATE USER rls_owner_bypass_user PASSWORD 'owner';`,
				`CREATE TABLE rls_owner_bypass_docs (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO rls_owner_bypass_docs VALUES (1, 'visible to owner');`,
				`ALTER TABLE rls_owner_bypass_docs OWNER TO rls_owner_bypass_user;`,
				`GRANT USAGE ON SCHEMA public TO rls_owner_bypass_user;`,
				`GRANT SELECT ON rls_owner_bypass_docs TO rls_owner_bypass_user;`,
				`ALTER TABLE rls_owner_bypass_docs ENABLE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label
						FROM rls_owner_bypass_docs
						ORDER BY id;`,

					Username: `rls_owner_bypass_user`,
					Password: `owner`, PostgresOracle: ScriptTestPostgresOracle{ID: "rls-table-owner-bypasses-unforced-rls"},

					// TestForcedRowLevelSecurityAppliesToTableOwnerRepro reproduces a security bug:
					// PostgreSQL FORCE ROW LEVEL SECURITY applies policies to the table owner.

				},
			},
		},
	})
}

func TestForcedRowLevelSecurityAppliesToTableOwnerRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "FORCE RLS applies policy to table owner",
			SetUpScript: []string{
				`CREATE USER rls_forced_owner PASSWORD 'owner';`,
				`CREATE TABLE rls_force_owner_docs (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO rls_force_owner_docs VALUES (1, 'hidden from owner');`,
				`ALTER TABLE rls_force_owner_docs OWNER TO rls_forced_owner;`,
				`GRANT USAGE ON SCHEMA public TO rls_forced_owner;`,
				`GRANT SELECT ON rls_force_owner_docs TO rls_forced_owner;`,
				`ALTER TABLE rls_force_owner_docs ENABLE ROW LEVEL SECURITY;`,
				`ALTER TABLE rls_force_owner_docs FORCE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label
						FROM rls_force_owner_docs
						ORDER BY id;`,

					Username: `rls_forced_owner`,
					Password: `owner`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-repro-test-testforcedrowlevelsecurityappliestotableownerrepro-0001-select-id-label-from-rls_force_owner_docs", Compare: "sqlstate"},

					// TestRowSecurityActiveReportsPolicyStateRepro reproduces an RLS catalog
					// correctness bug: PostgreSQL exposes whether row-level security is active for
					// the current user on a relation.

				},
			},
		},
	})
}

func TestRowSecurityActiveReportsPolicyStateRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "row_security_active reports active RLS",
			SetUpScript: []string{
				`CREATE USER rls_active_reader PASSWORD 'reader';`,
				`CREATE TABLE rls_active_docs (id INT PRIMARY KEY);`,
				`GRANT USAGE ON SCHEMA public TO rls_active_reader;`,
				`GRANT SELECT ON rls_active_docs TO rls_active_reader;`,
				`ALTER TABLE rls_active_docs ENABLE ROW LEVEL SECURITY;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT row_security_active('rls_active_docs'::regclass);`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-repro-test-testrowsecurityactivereportspolicystaterepro-0001-select-row_security_active-rls_active_docs-::regclass"},
				},
				{
					Query: `SELECT row_security_active('rls_active_docs'::regclass);`,

					Username: `rls_active_reader`,
					Password: `reader`, PostgresOracle: ScriptTestPostgresOracle{ID: "row-level-security-repro-test-testrowsecurityactivereportspolicystaterepro-0002-select-row_security_active-rls_active_docs-::regclass"},
				},
			},
		},
	})
}
