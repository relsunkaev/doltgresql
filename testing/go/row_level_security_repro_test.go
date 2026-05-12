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
					Query:    `SELECT id, label FROM rls_secrets ORDER BY id;`,
					Expected: []sql.Row{},
					Username: `rls_reader`,
					Password: `reader`,
				},
			},
		},
	})
}

// TestRowLevelSecuritySelectPolicyFiltersRowsRepro reproduces a security bug:
// PostgreSQL applies SELECT policies to rows visible to granted non-owners.
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
					Expected: []sql.Row{{1, "visible"}},
					Username: `rls_policy_reader`,
					Password: `reader`,
				},
			},
		},
	})
}

// TestDropPolicyIfExistsMissingRepro reproduces a PostgreSQL compatibility gap:
// DROP POLICY IF EXISTS should no-op when the named policy is absent on an
// existing table.
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
					Query:       `INSERT INTO rls_insert_secrets VALUES (1, 'leaked');`,
					ExpectedErr: `row-level security`,
					Username:    `rls_insert_user`,
					Password:    `writer`,
				},
				{
					Query:    `SELECT count(*) FROM rls_insert_secrets;`,
					Expected: []sql.Row{{int64(0)}},
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
					ExpectedErr: `row-level security`,
					Username:    `rls_insert_check_user`,
					Password:    `writer`,
				},
				{
					Query: `SELECT id, owner_name, label
						FROM rls_insert_check_docs
						ORDER BY id;`,
					Expected: []sql.Row{{1, "rls_insert_check_user", "allowed"}},
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
					Expected: []sql.Row{{1}},
					Username: `rls_update_policy_user`,
					Password: `writer`,
				},
				{
					Query: `UPDATE rls_update_policy_docs
						SET label = 'blocked'
						WHERE id = 2
						RETURNING id;`,
					Expected: []sql.Row{},
					Username: `rls_update_policy_user`,
					Password: `writer`,
				},
				{
					Query: `UPDATE rls_update_policy_docs
						SET owner_name = 'other_user'
						WHERE id = 1;`,
					ExpectedErr: `row-level security`,
					Username:    `rls_update_policy_user`,
					Password:    `writer`,
				},
				{
					Query: `SELECT id, owner_name, label
						FROM rls_update_policy_docs
						ORDER BY id;`,
					Expected: []sql.Row{
						{1, "rls_update_policy_user", "changed"},
						{2, "other_user", "hidden"},
					},
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
					Expected: []sql.Row{},
					Username: `rls_update_user`,
					Password: `writer`,
				},
				{
					Query:    `SELECT label FROM rls_update_secrets WHERE id = 1;`,
					Expected: []sql.Row{{"original"}},
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
					Expected: []sql.Row{{1}},
					Username: `rls_delete_policy_user`,
					Password: `writer`,
				},
				{
					Query: `SELECT id, owner_name, label
						FROM rls_delete_policy_docs
						ORDER BY id;`,
					Expected: []sql.Row{{2, "other_user", "keep me"}},
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
					Expected: []sql.Row{},
					Username: `rls_delete_user`,
					Password: `writer`,
				},
				{
					Query:    `SELECT count(*) FROM rls_delete_secrets;`,
					Expected: []sql.Row{{int64(1)}},
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
						WHERE oid = 'rls_catalog_target'::regclass;`,
					Expected: []sql.Row{{"t", "t"}},
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
						WHERE oid = 'rls_no_force_catalog_target'::regclass;`,
					Expected: []sql.Row{{"t", "f"}},
				},
			},
		},
	})
}

// TestForcedRowLevelSecurityAppliesToTableOwnerRepro reproduces a security bug:
// PostgreSQL FORCE ROW LEVEL SECURITY applies policies to the table owner.
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
					Expected: []sql.Row{},
					Username: `rls_forced_owner`,
					Password: `owner`,
				},
			},
		},
	})
}

// TestRowSecurityActiveReportsPolicyStateRepro reproduces an RLS catalog
// correctness bug: PostgreSQL exposes whether row-level security is active for
// the current user on a relation.
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
					Query:    `SELECT row_security_active('rls_active_docs'::regclass);`,
					Expected: []sql.Row{{false}},
				},
				{
					Query:    `SELECT row_security_active('rls_active_docs'::regclass);`,
					Expected: []sql.Row{{true}},
					Username: `rls_active_reader`,
					Password: `reader`,
				},
			},
		},
	})
}
