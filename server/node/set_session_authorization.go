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

package node

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/server/auth"
)

const originalSessionAuthorizationVar = "__doltgres_original_session_authorization"

// SetSessionAuthorization implements PostgreSQL's SET SESSION AUTHORIZATION and
// RESET SESSION AUTHORIZATION statements for the current connection.
type SetSessionAuthorization struct {
	User string
}

var _ sql.ExecSourceRel = (*SetSessionAuthorization)(nil)
var _ vitess.Injectable = (*SetSessionAuthorization)(nil)

// NewSetSessionAuthorization returns a new *SetSessionAuthorization. An empty
// user represents RESET SESSION AUTHORIZATION / SET SESSION AUTHORIZATION DEFAULT.
func NewSetSessionAuthorization(user string) *SetSessionAuthorization {
	return &SetSessionAuthorization{User: user}
}

// Children implements the interface sql.ExecSourceRel.
func (s *SetSessionAuthorization) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (s *SetSessionAuthorization) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (s *SetSessionAuthorization) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (s *SetSessionAuthorization) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	if ctx == nil || ctx.Session == nil {
		return nil, errors.Errorf("SET SESSION AUTHORIZATION requires an active session")
	}
	if ctx.GetIgnoreAutoCommit() {
		return nil, errors.Errorf("SET SESSION AUTHORIZATION cannot run inside a transaction block")
	}
	if s.User == "" {
		return s.reset(ctx)
	}

	targetRole := auth.GetRole(s.User)
	if !targetRole.IsValid() {
		return nil, errors.Errorf(`role "%s" does not exist`, s.User)
	}

	currentRole := auth.GetRole(ctx.Client().User)
	if !currentRole.IsValid() {
		return nil, errors.Errorf(`role "%s" does not exist`, ctx.Client().User)
	}
	if currentRole.Name != targetRole.Name && !currentRole.IsSuperUser {
		return nil, errors.Errorf(`permission denied to set session authorization "%s"`, targetRole.Name)
	}

	if original := originalSessionAuthorization(ctx); original == "" {
		if err := ctx.Session.SetUserVariable(ctx, originalSessionAuthorizationVar, ctx.Client().User, gmstypes.Text); err != nil {
			return nil, err
		}
	}
	setSessionUser(ctx, targetRole.Name)
	return sql.RowsToRowIter(), nil
}

func (s *SetSessionAuthorization) reset(ctx *sql.Context) (sql.RowIter, error) {
	original := originalSessionAuthorization(ctx)
	if original == "" {
		original = ctx.Client().User
	}
	if !auth.RoleExists(original) {
		return nil, errors.Errorf(`role "%s" does not exist`, original)
	}
	setSessionUser(ctx, original)
	return sql.RowsToRowIter(), ctx.Session.SetUserVariable(ctx, originalSessionAuthorizationVar, "", gmstypes.Text)
}

func originalSessionAuthorization(ctx *sql.Context) string {
	_, value, err := ctx.Session.GetUserVariable(ctx, originalSessionAuthorizationVar)
	if err != nil {
		return ""
	}
	if original, ok := value.(string); ok {
		return original
	}
	return ""
}

func setSessionUser(ctx *sql.Context, user string) {
	client := ctx.Client()
	client.User = user
	ctx.Session.SetClient(client)
	ctx.Session.SetPrivilegeSet(nil, 0)
}

// Schema implements the interface sql.ExecSourceRel.
func (s *SetSessionAuthorization) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (s *SetSessionAuthorization) String() string {
	if s.User == "" {
		return "RESET SESSION AUTHORIZATION"
	}
	return fmt.Sprintf("SET SESSION AUTHORIZATION %s", s.User)
}

// WithChildren implements the interface sql.ExecSourceRel.
func (s *SetSessionAuthorization) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return s, nil
}

// WithResolvedChildren implements the interface sql.ExecSourceRel.
func (s *SetSessionAuthorization) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	return s, nil
}
