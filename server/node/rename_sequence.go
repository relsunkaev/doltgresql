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
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/auth"
)

// RenameSequence handles ALTER SEQUENCE ... RENAME TO.
type RenameSequence struct {
	schema      string
	sequence    string
	newSchema   string
	newSequence string
	ifExists    bool
}

var _ sql.ExecSourceRel = (*RenameSequence)(nil)
var _ vitess.Injectable = (*RenameSequence)(nil)

// NewRenameSequence returns a new *RenameSequence.
func NewRenameSequence(schema, sequence, newSchema, newSequence string, ifExists bool) *RenameSequence {
	return &RenameSequence{
		schema:      schema,
		sequence:    sequence,
		newSchema:   newSchema,
		newSequence: newSequence,
		ifExists:    ifExists,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (r *RenameSequence) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (r *RenameSequence) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (r *RenameSequence) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (r *RenameSequence) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	schema, err := core.GetSchemaName(ctx, nil, r.schema)
	if err != nil {
		return nil, err
	}
	newSchema := schema
	if r.newSchema != "" {
		newSchema, err = core.GetSchemaName(ctx, nil, r.newSchema)
		if err != nil {
			return nil, err
		}
		if newSchema != schema {
			return nil, errors.New("cannot change sequence schema with RENAME")
		}
	}

	collection, err := core.GetSequencesCollectionFromContext(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return nil, err
	}
	oldID := id.NewSequence(schema, r.sequence)
	if !collection.HasSequence(ctx, oldID) {
		if r.ifExists {
			return sql.RowsToRowIter(), nil
		}
		return nil, errors.Errorf(`sequence "%s" does not exist`, r.sequence)
	}
	seq, err := collection.GetSequence(ctx, oldID)
	if err != nil {
		return nil, err
	}
	if err = checkSequenceOwnership(ctx, seq); err != nil {
		return nil, errors.Wrap(err, "permission denied")
	}

	relationType, err := core.GetRelationType(ctx, newSchema, r.newSequence)
	if err != nil {
		return nil, err
	}
	if relationType != core.RelationType_DoesNotExist {
		return nil, pgerror.Newf(pgcode.DuplicateRelation, `relation "%s" already exists`, r.newSequence)
	}
	newID := id.NewSequence(newSchema, r.newSequence)
	if err = collection.RenameRootObject(ctx, oldID.AsId(), newID.AsId()); err != nil {
		return nil, err
	}

	var persistErr error
	auth.LockWrite(func() {
		auth.RenameSequencePrivileges(schema, r.sequence, newSchema, r.newSequence)
		persistErr = auth.PersistChanges()
	})
	if persistErr != nil {
		return nil, persistErr
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (r *RenameSequence) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (r *RenameSequence) String() string {
	return "RENAME SEQUENCE"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (r *RenameSequence) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 0)
	}
	return r, nil
}

// WithResolvedChildren implements the vitess.Injectable interface.
func (r *RenameSequence) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 0)
	}
	return r, nil
}
