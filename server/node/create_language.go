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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/server/auth"
)

// CreateLanguage implements CREATE LANGUAGE.
type CreateLanguage struct {
	Name         string
	Replace      bool
	Trusted      bool
	IsProcedural bool
	Handler      string
	Inline       string
	Validator    string
}

var _ sql.ExecSourceRel = (*CreateLanguage)(nil)
var _ vitess.Injectable = (*CreateLanguage)(nil)

// NewCreateLanguage returns a new *CreateLanguage.
func NewCreateLanguage(name string, replace bool, trusted bool, isProcedural bool, handler string, inline string, validator string) *CreateLanguage {
	return &CreateLanguage{
		Name:         name,
		Replace:      replace,
		Trusted:      trusted,
		IsProcedural: isProcedural,
		Handler:      handler,
		Inline:       inline,
		Validator:    validator,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (c *CreateLanguage) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (c *CreateLanguage) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (c *CreateLanguage) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (c *CreateLanguage) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	var err error
	auth.LockWrite(func() {
		err = auth.CreateLanguage(auth.Language{
			Name:         c.Name,
			Owner:        ctx.Client().User,
			IsProcedural: c.IsProcedural,
			Trusted:      c.Trusted,
			Handler:      c.Handler,
			Inline:       c.Inline,
			Validator:    c.Validator,
		}, c.Replace)
		if err != nil {
			return
		}
		if c.Trusted {
			publicRole := auth.GetRole("public")
			ownerRole := auth.GetRole(ctx.Client().User)
			if publicRole.IsValid() && ownerRole.IsValid() {
				auth.AddLanguagePrivilege(auth.LanguagePrivilegeKey{Role: publicRole.ID(), Name: c.Name}, auth.GrantedPrivilege{
					Privilege: auth.Privilege_USAGE,
					GrantedBy: ownerRole.ID(),
				}, false)
			}
		}
		err = auth.PersistChanges()
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (c *CreateLanguage) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (c *CreateLanguage) String() string {
	return "CREATE LANGUAGE"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (c *CreateLanguage) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (c *CreateLanguage) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}
