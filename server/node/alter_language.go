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

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/server/auth"
)

// AlterLanguage implements ALTER LANGUAGE.
type AlterLanguage struct {
	Name    string
	NewName string
	Owner   string
}

var _ sql.ExecSourceRel = (*AlterLanguage)(nil)
var _ vitess.Injectable = (*AlterLanguage)(nil)

// NewAlterLanguageRename returns a new *AlterLanguage for ALTER LANGUAGE ... RENAME TO.
func NewAlterLanguageRename(name string, newName string) *AlterLanguage {
	return &AlterLanguage{Name: name, NewName: newName}
}

// NewAlterLanguageOwner returns a new *AlterLanguage for ALTER LANGUAGE ... OWNER TO.
func NewAlterLanguageOwner(name string, owner string) *AlterLanguage {
	return &AlterLanguage{Name: name, Owner: owner}
}

// Children implements the interface sql.ExecSourceRel.
func (a *AlterLanguage) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (a *AlterLanguage) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (a *AlterLanguage) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (a *AlterLanguage) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	var err error
	auth.LockWrite(func() {
		err = checkLanguageOwnership(ctx, a.Name)
		if err != nil {
			return
		}
		if a.NewName != "" {
			err = auth.RenameLanguage(a.Name, a.NewName)
		} else {
			err = auth.AlterLanguageOwner(a.Name, a.Owner)
		}
		if err != nil {
			return
		}
		err = auth.PersistChanges()
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func checkLanguageOwnership(ctx *sql.Context, name string) error {
	language, ok := auth.GetLanguage(name)
	if !ok {
		return errors.Errorf(`language "%s" does not exist`, name)
	}
	if language.Owner == "" || language.Owner == ctx.Client().User {
		return nil
	}
	userRole := auth.GetRole(ctx.Client().User)
	if userRole.IsValid() && userRole.IsSuperUser {
		return nil
	}
	return errors.Errorf("must be owner of language %s", name)
}

// Schema implements the interface sql.ExecSourceRel.
func (a *AlterLanguage) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (a *AlterLanguage) String() string {
	return "ALTER LANGUAGE"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (a *AlterLanguage) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (a *AlterLanguage) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}
