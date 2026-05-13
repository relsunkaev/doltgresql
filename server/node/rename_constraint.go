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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/comments"
)

// RenameConstraint handles ALTER TABLE ... RENAME CONSTRAINT for table check constraints.
type RenameConstraint struct {
	ifExists bool
	schema   string
	table    string
	oldName  string
	newName  string
}

var _ sql.ExecSourceRel = (*RenameConstraint)(nil)
var _ vitess.Injectable = (*RenameConstraint)(nil)

// NewRenameConstraint returns a new *RenameConstraint.
func NewRenameConstraint(ifExists bool, schema string, table string, oldName string, newName string) *RenameConstraint {
	return &RenameConstraint{
		ifExists: ifExists,
		schema:   schema,
		table:    table,
		oldName:  oldName,
		newName:  newName,
	}
}

// Children implements sql.ExecSourceRel.
func (r *RenameConstraint) Children() []sql.Node {
	return nil
}

// IsReadOnly implements sql.ExecSourceRel.
func (r *RenameConstraint) IsReadOnly() bool {
	return false
}

// Resolved implements sql.ExecSourceRel.
func (r *RenameConstraint) Resolved() bool {
	return true
}

// RowIter implements sql.ExecSourceRel.
func (r *RenameConstraint) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	resolver := AlterRelationOwner{
		kind:     relationOwnerKindTable,
		ifExists: r.ifExists,
		schema:   r.schema,
		name:     r.table,
	}
	relation, err := resolver.resolveTable(ctx)
	if err != nil {
		return nil, err
	}
	if relation.Name == "" {
		return sql.RowsToRowIter(), nil
	}
	if err = resolver.checkOwnership(ctx, relation); err != nil {
		return nil, errors.Wrap(err, "permission denied")
	}
	table, err := core.GetSqlTableFromContext(ctx, "", doltdb.TableName{Name: r.table, Schema: relation.Schema})
	if err != nil {
		return nil, err
	}
	if table == nil {
		if r.ifExists {
			return sql.RowsToRowIter(), nil
		}
		return nil, errors.Errorf(`relation "%s" does not exist`, r.table)
	}
	checkTable, ok := table.(sql.CheckTable)
	if !ok {
		checkTable, ok = sql.GetUnderlyingTable(table).(sql.CheckTable)
	}
	if !ok {
		return nil, errors.Errorf(`relation "%s" does not support check constraints`, r.table)
	}
	checkAlterable, ok := typedTableCheckAlterable(table)
	if !ok {
		return nil, errors.Errorf(`relation "%s" does not support altering check constraints`, r.table)
	}
	checks, err := checkTable.GetChecks(ctx)
	if err != nil {
		return nil, err
	}
	var target *sql.CheckDefinition
	for i := range checks {
		if checks[i].Name == r.newName {
			return nil, errors.Errorf(`constraint "%s" already exists`, r.newName)
		}
		if checks[i].Name == r.oldName {
			check := checks[i]
			target = &check
		}
	}
	if target == nil {
		return nil, errors.Errorf(`constraint "%s" does not exist`, r.oldName)
	}
	renamed := *target
	renamed.Name = r.newName
	if err = checkAlterable.CreateCheck(ctx, &renamed); err != nil {
		return nil, err
	}
	if err = checkAlterable.DropCheck(ctx, r.oldName); err != nil {
		return nil, err
	}
	moveCheckConstraintComment(relation, r.oldName, r.newName)
	return sql.RowsToRowIter(), nil
}

// Schema implements sql.ExecSourceRel.
func (r *RenameConstraint) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements sql.ExecSourceRel.
func (r *RenameConstraint) String() string {
	return "RENAME CONSTRAINT"
}

// WithChildren implements sql.ExecSourceRel.
func (r *RenameConstraint) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(r, children...)
}

// WithResolvedChildren implements vitess.Injectable.
func (r *RenameConstraint) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return r, nil
}

func moveCheckConstraintComment(relation doltdb.TableName, oldName string, newName string) {
	oldID := id.NewCheck(relation.Schema, relation.Name, oldName).AsId()
	newID := id.NewCheck(relation.Schema, relation.Name, newName).AsId()
	oldOID := id.Cache().ToOID(oldID)
	newOID := id.Cache().ToOID(newID)
	classOID := comments.ClassOID("pg_constraint")
	for _, entry := range comments.Entries() {
		if entry.ObjOID != oldOID || entry.ClassOID != classOID {
			continue
		}
		description := entry.Description
		comments.Set(comments.Key{ObjOID: newOID, ClassOID: classOID, ObjSubID: entry.ObjSubID}, &description)
		comments.Set(entry.Key, nil)
	}
}
