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

// DropLanguage implements DROP LANGUAGE.
type DropLanguage struct {
	Name     string
	IfExists bool
	Cascade  bool
}

var _ sql.ExecSourceRel = (*DropLanguage)(nil)
var _ vitess.Injectable = (*DropLanguage)(nil)

// NewDropLanguage returns a new *DropLanguage.
func NewDropLanguage(name string, ifExists bool, cascade bool) *DropLanguage {
	return &DropLanguage{Name: name, IfExists: ifExists, Cascade: cascade}
}

// Children implements the interface sql.ExecSourceRel.
func (d *DropLanguage) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (d *DropLanguage) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (d *DropLanguage) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (d *DropLanguage) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	if d.Cascade {
		return nil, errors.New("DROP LANGUAGE does not yet support CASCADE")
	}
	var err error
	auth.LockWrite(func() {
		if _, ok := auth.GetLanguage(d.Name); !ok {
			if !d.IfExists {
				err = errors.Errorf(`language "%s" does not exist`, d.Name)
			}
			return
		}
		if err = checkLanguageOwnership(ctx, d.Name); err != nil {
			return
		}
		if ok := auth.DropLanguage(d.Name); !ok {
			err = errors.Errorf(`language "%s" does not exist`, d.Name)
			return
		}
		err = auth.PersistChanges()
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (d *DropLanguage) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (d *DropLanguage) String() string {
	return "DROP LANGUAGE"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (d *DropLanguage) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(d, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (d *DropLanguage) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return d, nil
}
