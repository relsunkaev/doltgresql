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
	"math"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/core/sequences"
	"github.com/dolthub/doltgresql/server/auth"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// AlterSequence handles the ALTER SEQUENCE statement.
type AlterSequence struct {
	ifExists       bool
	targetSchema   string
	targetSequence string
	owner          string
	ownedBy        AlterSequenceOwnedBy
	options        []AlterSequenceOption
	warnings       []string
}

// AlterSequenceOwnedBy is an option in AlterSequence to represent OWNED BY.
type AlterSequenceOwnedBy struct {
	IsSet  bool
	Table  string
	Column string
}

// AlterSequenceOption is a supported option in an ALTER SEQUENCE statement.
type AlterSequenceOption struct {
	Name   string
	IntVal *int64
}

const (
	AlterSequenceOptionRestart   = "RESTART"
	AlterSequenceOptionStart     = "START WITH"
	AlterSequenceOptionIncrement = "INCREMENT BY"
	AlterSequenceOptionMinValue  = "MINVALUE"
	AlterSequenceOptionMaxValue  = "MAXVALUE"
	AlterSequenceOptionCycle     = "CYCLE"
	AlterSequenceOptionNoCycle   = "NO CYCLE"
)

var _ sql.ExecSourceRel = (*AlterSequence)(nil)
var _ vitess.Injectable = (*AlterSequence)(nil)

// NewAlterSequence returns a new *AlterSequence.
func NewAlterSequence(ifExists bool, targetSchema string, targetSequence string, owner string, ownedBy AlterSequenceOwnedBy, options []AlterSequenceOption, warnings ...string) *AlterSequence {
	return &AlterSequence{
		ifExists:       ifExists,
		targetSchema:   targetSchema,
		targetSequence: targetSequence,
		owner:          owner,
		ownedBy:        ownedBy,
		options:        options,
		warnings:       warnings,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (c *AlterSequence) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (c *AlterSequence) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (c *AlterSequence) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (c *AlterSequence) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	targetSchema, err := core.GetSchemaName(ctx, nil, c.targetSchema)
	if err != nil {
		return nil, err
	}
	target := id.NewSequence(targetSchema, c.targetSequence)
	collection, err := core.GetSequencesCollectionFromContext(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return nil, err
	}
	if !collection.HasSequence(ctx, target) {
		if c.ifExists {
			return sql.RowsToRowIter(), nil
		}
		return nil, errors.Errorf(`relation "%s" does not exist`, c.targetSequence)
	}
	seq, err := collection.GetSequence(ctx, target)
	if err != nil {
		return nil, err
	}
	if c.owner != "" || c.ownedBy.IsSet {
		if err = checkSequenceOwnership(ctx, seq); err != nil {
			return nil, errors.Wrap(err, "permission denied")
		}
	}

	if c.owner != "" {
		if !auth.RoleExists(c.owner) {
			return nil, errors.Errorf(`role "%s" does not exist`, c.owner)
		}
		seq.Owner = c.owner
	}

	if c.ownedBy.IsSet {
		if len(c.ownedBy.Table) > 0 {
			relationType, err := core.GetRelationType(ctx, targetSchema, c.ownedBy.Table)
			if err != nil {
				return nil, err
			}
			if relationType == core.RelationType_DoesNotExist {
				return nil, errors.Errorf(`relation "%s" does not exist`, c.ownedBy.Table)
			} else if relationType != core.RelationType_Table {
				return nil, errors.Errorf(`sequence cannot be owned by relation "%s"`, c.ownedBy.Table)
			}
			if err = checkTableOwnership(ctx, doltdb.TableName{Name: c.ownedBy.Table, Schema: targetSchema}); err != nil {
				return nil, errors.Wrap(err, "permission denied")
			}

			table, err := core.GetSqlTableFromContext(ctx, "", doltdb.TableName{Name: c.ownedBy.Table, Schema: targetSchema})
			if err != nil {
				return nil, err
			}
			if table == nil {
				return nil, errors.Errorf(`table "%s" cannot be found but says it exists`, c.ownedBy.Table)
			}
			var tableColumn *sql.Column
			for _, col := range table.Schema(ctx) {
				if col.Name == c.ownedBy.Column {
					tableColumn = col.Copy()
					break
				}
			}
			if tableColumn == nil {
				return nil, errors.Errorf(`column "%s" of relation "%s" does not exist`,
					c.ownedBy.Column, c.ownedBy.Table)
			}
			// We've verified the existence of the table's column, so we can assign it now
			seq.OwnerTable = id.NewTable(targetSchema, c.ownedBy.Table)
			seq.OwnerColumn = c.ownedBy.Column
		} else {
			seq.OwnerTable = ""
			seq.OwnerColumn = ""
		}
	}
	if len(c.options) > 0 {
		if err = applyAlterSequenceOptions(seq, c.options); err != nil {
			return nil, err
		}
	}
	// Display any warnings that were encountered during parsing
	for _, warning := range c.warnings {
		noticeResponse := &pgproto3.NoticeResponse{
			Severity: "WARNING",
			Message:  warning,
		}
		sess := dsess.DSessFromSess(ctx.Session)
		sess.Notice(noticeResponse)
	}
	// Any changes made to the sequence will be persisted at the end of the transaction, so we can just return now
	return sql.RowsToRowIter(), nil
}

func applyAlterSequenceOptions(seq *sequences.Sequence, options []AlterSequenceOption) error {
	for _, option := range options {
		switch option.Name {
		case AlterSequenceOptionRestart:
			restart := seq.Start
			if option.IntVal != nil {
				restart = *option.IntVal
			}
			seq.Current = restart
			seq.IsAtEnd = false
			seq.IsCalled = false
		case AlterSequenceOptionStart:
			seq.Start = *option.IntVal
		case AlterSequenceOptionIncrement:
			if *option.IntVal == 0 {
				return errors.Errorf("INCREMENT must not be zero")
			}
			seq.Increment = *option.IntVal
			seq.IsAtEnd = false
		case AlterSequenceOptionMinValue:
			if option.IntVal == nil {
				seq.Minimum = defaultSequenceMinimum(seq)
			} else {
				seq.Minimum = *option.IntVal
			}
			seq.IsAtEnd = false
		case AlterSequenceOptionMaxValue:
			if option.IntVal == nil {
				seq.Maximum = defaultSequenceMaximum(seq)
			} else {
				seq.Maximum = *option.IntVal
			}
			seq.IsAtEnd = false
		case AlterSequenceOptionCycle:
			seq.Cycle = true
		case AlterSequenceOptionNoCycle:
			seq.Cycle = false
		default:
			return errors.Errorf(`unsupported ALTER SEQUENCE option "%s"`, option.Name)
		}
	}
	if seq.Minimum > seq.Maximum {
		return errors.Errorf("MINVALUE must be less than or equal to MAXVALUE")
	}
	if seq.Current < seq.Minimum || seq.Current > seq.Maximum {
		return errors.Errorf(`RESTART value %d is out of bounds for sequence "%s" (%d..%d)`,
			seq.Current, seq.Id, seq.Minimum, seq.Maximum)
	}
	return nil
}

func defaultSequenceMinimum(seq *sequences.Sequence) int64 {
	if seq.Increment < 0 {
		return math.MinInt64
	}
	return 1
}

func defaultSequenceMaximum(seq *sequences.Sequence) int64 {
	switch seq.DataTypeID {
	case pgtypes.Int16.ID:
		return math.MaxInt16
	case pgtypes.Int32.ID:
		return math.MaxInt32
	default:
		return math.MaxInt64
	}
}

// Schema implements the interface sql.ExecSourceRel.
func (c *AlterSequence) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (c *AlterSequence) String() string {
	return "ALTER SEQUENCE"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (c *AlterSequence) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (c *AlterSequence) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}
