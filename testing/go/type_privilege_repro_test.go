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

// TestTypeUsagePrivilegeCanBeRevokedAndGrantedRepro reproduces a type ACL
// security bug: PostgreSQL lets type owners revoke default PUBLIC usage and
// grant it back to selected roles.
func TestTypeUsagePrivilegeCanBeRevokedAndGrantedRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "type USAGE can be revoked from PUBLIC and granted to a role",
			SetUpScript: []string{
				`CREATE USER type_usage_acl_user PASSWORD 'pw';`,
				`CREATE TYPE type_usage_acl_mood AS ENUM ('ok', 'sad');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `REVOKE USAGE ON TYPE type_usage_acl_mood FROM PUBLIC;`,
					ExpectedTag: `REVOKE`,
				},
				{
					Query:       `SELECT 'ok'::type_usage_acl_mood::text;`,
					ExpectedErr: `permission denied`,
					Username:    `type_usage_acl_user`,
					Password:    `pw`,
				},
				{
					Query:       `GRANT USAGE ON TYPE type_usage_acl_mood TO type_usage_acl_user;`,
					ExpectedTag: `GRANT`,
				},
				{
					Query:    `SELECT 'ok'::type_usage_acl_mood::text;`,
					Expected: []sql.Row{{"ok"}},
					Username: `type_usage_acl_user`,
					Password: `pw`,
				},
				{
					Query:       `REVOKE USAGE ON TYPE type_usage_acl_mood FROM type_usage_acl_user;`,
					ExpectedTag: `REVOKE`,
				},
				{
					Query:       `SELECT 'ok'::type_usage_acl_mood::text;`,
					ExpectedErr: `permission denied`,
					Username:    `type_usage_acl_user`,
					Password:    `pw`,
				},
			},
		},
	})
}
