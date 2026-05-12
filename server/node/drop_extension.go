// Copyright 2025 Dolthub, Inc.
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
	"slices"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	corefunctions "github.com/dolthub/doltgresql/core/functions"
	"github.com/dolthub/doltgresql/core/id"
)

// DropExtension implements DROP EXTENSION.
type DropExtension struct {
	Names    []string
	IfExists bool
	Cascade  bool
}

var _ sql.ExecSourceRel = (*DropExtension)(nil)
var _ vitess.Injectable = (*DropExtension)(nil)

// NewDropExtension returns a new *DropExtension.
func NewDropExtension(names []string, ifExists bool, cascade bool) *DropExtension {
	return &DropExtension{
		Names:    names,
		IfExists: ifExists,
		Cascade:  cascade,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (c *DropExtension) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (c *DropExtension) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (c *DropExtension) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (c *DropExtension) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	extCollection, err := core.GetExtensionsCollectionFromContext(ctx, "")
	if err != nil {
		return nil, err
	}
	funcCollection, err := core.GetFunctionsCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	extensionsToDrop := make([]id.Extension, 0, len(c.Names))
	for _, name := range c.Names {
		extID := id.NewExtension(name)
		if !extCollection.HasLoadedExtension(ctx, extID) {
			if c.IfExists {
				continue
			}
			return nil, errors.Errorf(`extension "%s" does not exist`, name)
		}
		extensionsToDrop = append(extensionsToDrop, extID)
	}
	functionsToDrop := make([]id.Function, 0)
	err = funcCollection.IterateFunctions(ctx, func(f corefunctions.Function) (stop bool, err error) {
		for _, extID := range extensionsToDrop {
			if slices.Contains(f.ExtensionDeps, extID.Name()) {
				if !c.Cascade {
					return true, errors.Errorf(`cannot drop extension "%s" because other objects depend on it`, extID.Name())
				}
				functionsToDrop = append(functionsToDrop, f.ID)
				break
			}
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	if err = funcCollection.DropFunction(ctx, functionsToDrop...); err != nil {
		return nil, err
	}
	if err = extCollection.DropLoadedExtension(ctx, extensionsToDrop...); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (c *DropExtension) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (c *DropExtension) String() string {
	return "DROP EXTENSION"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (c *DropExtension) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (c *DropExtension) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}
