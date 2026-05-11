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

import "testing"

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
