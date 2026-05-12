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

// TestLanguageUsagePrivilegeCanBeRevokedAndGrantedRepro reproduces a language
// ACL security bug: PostgreSQL lets language owners revoke default PUBLIC
// usage and grant it back to selected roles.
func TestLanguageUsagePrivilegeCanBeRevokedAndGrantedRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "language USAGE can be revoked from PUBLIC and granted to a role",
			SetUpScript: []string{
				`CREATE USER language_usage_acl_user PASSWORD 'pw';`,
				`GRANT USAGE, CREATE ON SCHEMA public TO language_usage_acl_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `REVOKE USAGE ON LANGUAGE plpgsql FROM PUBLIC;`,
					ExpectedTag: `REVOKE`,
				},
				{
					Query: `CREATE FUNCTION language_acl_denied() RETURNS INT
						LANGUAGE plpgsql
						AS $$ BEGIN RETURN 7; END; $$;`,
					ExpectedErr: `permission denied`,
					Username:    `language_usage_acl_user`,
					Password:    `pw`,
				},
				{
					Query:       `GRANT USAGE ON LANGUAGE plpgsql TO language_usage_acl_user;`,
					ExpectedTag: `GRANT`,
				},
				{
					Query: `CREATE FUNCTION language_acl_allowed() RETURNS INT
						LANGUAGE plpgsql
						AS $$ BEGIN RETURN 7; END; $$;`,
					Username: `language_usage_acl_user`,
					Password: `pw`,
				},
				{
					Query:       `REVOKE USAGE ON LANGUAGE plpgsql FROM language_usage_acl_user;`,
					ExpectedTag: `REVOKE`,
				},
				{
					Query: `CREATE FUNCTION language_acl_denied_again() RETURNS INT
						LANGUAGE plpgsql
						AS $$ BEGIN RETURN 7; END; $$;`,
					ExpectedErr: `permission denied`,
					Username:    `language_usage_acl_user`,
					Password:    `pw`,
				},
			},
		},
	})
}

// TestCreateUntrustedLanguageRequiresSuperuserRepro reproduces a language DDL
// security bug: PostgreSQL restricts untrusted language creation to superusers.
func TestCreateUntrustedLanguageRequiresSuperuserRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE LANGUAGE for untrusted language requires superuser",
			SetUpScript: []string{
				`CREATE USER untrusted_language_creator PASSWORD 'pw';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE LANGUAGE untrusted_user_lang HANDLER plpgsql_call_handler;`,
					ExpectedErr: `superuser`,
					Username:    `untrusted_language_creator`,
					Password:    `pw`,
				},
			},
		},
	})
}

// TestAlterLanguageOwnerUpdatesPgLanguageRepro reproduces a procedural-language
// catalog persistence gap: ALTER LANGUAGE OWNER TO should update pg_language.
func TestAlterLanguageOwnerUpdatesPgLanguageRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER LANGUAGE OWNER TO updates pg_language",
			SetUpScript: []string{
				`CREATE ROLE language_owner_target;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER LANGUAGE plpgsql OWNER TO language_owner_target;`,
				},
				{
					Query: `SELECT pg_get_userbyid(lanowner)
						FROM pg_catalog.pg_language
						WHERE lanname = 'plpgsql';`,
					Expected: []sql.Row{{"language_owner_target"}},
				},
			},
		},
	})
}

// TestAlterLanguageOwnerToRequiresOwnershipRepro reproduces a PostgreSQL
// privilege incompatibility: a normal login role can run ALTER LANGUAGE OWNER
// TO against a language it does not own.
func TestAlterLanguageOwnerToRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER LANGUAGE OWNER TO requires language ownership",
			SetUpScript: []string{
				`CREATE USER language_owner_hijacker PASSWORD 'hijacker';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER LANGUAGE plpgsql OWNER TO language_owner_hijacker;`,
					ExpectedErr: `must be owner`,
					Username:    `language_owner_hijacker`,
					Password:    `hijacker`,
				},
			},
		},
	})
}

// TestDropLanguageRequiresOwnershipRepro reproduces a language DDL security
// bug: a normal login role can drop a procedural language it does not own.
func TestDropLanguageRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP LANGUAGE requires language ownership",
			SetUpScript: []string{
				`CREATE USER language_dropper PASSWORD 'dropper';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP LANGUAGE plpgsql;`,
					ExpectedErr: `must be owner`,
					Username:    `language_dropper`,
					Password:    `dropper`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_language
						WHERE lanname = 'plpgsql';`,
					Expected: []sql.Row{{int64(1)}},
				},
			},
		},
	})
}

// TestCreateLanguagePopulatesPgLanguageRepro reproduces a procedural-language
// catalog persistence gap: CREATE LANGUAGE should add the new language to
// pg_language.
func TestCreateLanguagePopulatesPgLanguageRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE LANGUAGE populates pg_language",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE LANGUAGE compat_lang HANDLER plpgsql_call_handler;`,
				},
				{
					Query: `SELECT lanname
						FROM pg_catalog.pg_language
						WHERE lanname = 'compat_lang';`,
					Expected: []sql.Row{{"compat_lang"}},
				},
			},
		},
	})
}

// TestDropLanguageIfExistsMissingRepro reproduces a procedural-language DDL
// compatibility gap: PostgreSQL accepts DROP LANGUAGE IF EXISTS for absent
// languages as a dump-safe no-op.
func TestDropLanguageIfExistsMissingRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP LANGUAGE IF EXISTS missing language succeeds",
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP LANGUAGE IF EXISTS missing_language;`,
				},
			},
		},
	})
}
