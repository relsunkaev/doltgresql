// Copyright 2024 Dolthub, Inc.
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
	"regexp"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/core/sequences"
	"github.com/dolthub/doltgresql/postgres/parser/parser"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/comments"
	serverfunctions "github.com/dolthub/doltgresql/server/functions"
)

// DropSequence handles the DROP SEQUENCE statement.
type DropSequence struct {
	schema   string
	sequence string
	ifExists bool
	cascade  bool
}

var _ sql.ExecSourceRel = (*DropSequence)(nil)
var _ vitess.Injectable = (*DropSequence)(nil)

// NewDropSequence returns a new *DropSequence.
func NewDropSequence(ifExists bool, schema string, sequence string, cascade bool) *DropSequence {
	return &DropSequence{
		schema:   schema,
		sequence: sequence,
		ifExists: ifExists,
		cascade:  cascade,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (c *DropSequence) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (c *DropSequence) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (c *DropSequence) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (c *DropSequence) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	schema, err := core.GetSchemaName(ctx, nil, c.schema)
	if err != nil {
		return nil, err
	}
	relationType, err := core.GetRelationType(ctx, schema, c.sequence)
	if err != nil {
		return nil, err
	}
	if relationType == core.RelationType_DoesNotExist {
		if c.ifExists {
			// TODO: issue a notice
			return sql.RowsToRowIter(), nil
		}
		return nil, errors.Errorf(`sequence "%s" does not exist`, c.sequence)
	}
	// TODO: we always use the current database for this operation, but it should also be possible drop a sequence in
	//  a different DB (e.g. on a different branch)
	collection, err := core.GetSequencesCollectionFromContext(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return nil, err
	}
	sequenceID := id.NewSequence(schema, c.sequence)
	sequence, err := collection.GetSequence(ctx, sequenceID)
	if err != nil {
		return nil, err
	}
	if err = checkSequenceOwnership(ctx, sequence); err != nil {
		return nil, errors.Wrap(err, "permission denied")
	}
	dependentDefaults, err := sequenceDefaultDependencies(ctx, sequenceID)
	if err != nil {
		return nil, err
	}
	if len(dependentDefaults) > 0 {
		if !c.cascade {
			return nil, pgerror.Newf(pgcode.DependentObjectsStillExist,
				`cannot drop sequence %s because another object depends on it`, c.sequence)
		}
		if err = clearSequenceDefaultDependencies(ctx, dependentDefaults); err != nil {
			return nil, err
		}
	}
	if err = collection.DropSequence(ctx, sequenceID); err != nil {
		return nil, err
	}
	comments.RemoveObject(sequenceID.AsId(), "pg_class")
	comments.RemoveObject(id.NewTable(schema, c.sequence).AsId(), "pg_class")
	var persistErr error
	auth.LockWrite(func() {
		auth.RemoveAllSequencePrivileges(schema, c.sequence)
		persistErr = auth.PersistChanges()
	})
	if persistErr != nil {
		return nil, persistErr
	}
	return sql.RowsToRowIter(), nil
}

func checkSequenceOwnership(ctx *sql.Context, sequence *sequences.Sequence) error {
	owner := sequence.Owner
	if owner == "" {
		owner = "postgres"
	}
	if owner == "" || owner == ctx.Client().User {
		return nil
	}
	var userRole auth.Role
	auth.LockRead(func() {
		userRole = auth.GetRole(ctx.Client().User)
	})
	if userRole.IsValid() && userRole.IsSuperUser {
		return nil
	}
	return errors.Errorf("must be owner of sequence %s", sequence.Id.SequenceName())
}

type sequenceDefaultDependency struct {
	table          sql.Table
	column         *sql.Column
	clearDefault   bool
	clearGenerated bool
}

func sequenceDefaultDependencies(ctx *sql.Context, sequenceID id.Sequence) ([]sequenceDefaultDependency, error) {
	dependencies := make([]sequenceDefaultDependency, 0)
	err := serverfunctions.IterateCurrentDatabase(ctx, serverfunctions.Callbacks{
		ColumnDefault: func(ctx *sql.Context, schema serverfunctions.ItemSchema, table serverfunctions.ItemTable, columnDefault serverfunctions.ItemColumnDefault) (cont bool, err error) {
			column := columnDefault.Item.Column
			dependency := sequenceDefaultDependency{
				table:          table.Item,
				column:         column.Copy(),
				clearDefault:   columnDefaultValueReferencesSequence(column.Default, schema.Item.SchemaName(), sequenceID),
				clearGenerated: columnDefaultValueReferencesSequence(column.Generated, schema.Item.SchemaName(), sequenceID),
			}
			if dependency.clearDefault || dependency.clearGenerated {
				dependencies = append(dependencies, dependency)
			}
			return true, nil
		},
	})
	return dependencies, err
}

func clearSequenceDefaultDependencies(ctx *sql.Context, dependencies []sequenceDefaultDependency) error {
	for _, dependency := range dependencies {
		alterableTable, ok := dependency.table.(*sqle.AlterableDoltTable)
		if !ok {
			return errors.Errorf(`expected a Dolt table but received "%T"`, dependency.table)
		}
		updatedColumn := dependency.column.Copy()
		if dependency.clearDefault {
			updatedColumn.Default = nil
		}
		if dependency.clearGenerated {
			updatedColumn.Generated = nil
		}
		if err := alterableTable.ModifyColumn(ctx, dependency.column.Name, updatedColumn, nil); err != nil {
			return err
		}
	}
	return nil
}

var (
	nextvalQuotedArgPattern = regexp.MustCompile(`(?i)nextval\s*\(\s*'([^']+)'`)
	nextvalBareArgPattern   = regexp.MustCompile(`(?i)nextval\s*\(\s*([A-Za-z0-9_."']+)`)
)

func columnDefaultValueReferencesSequence(defaultValue *sql.ColumnDefaultValue, defaultSchema string, sequenceID id.Sequence) bool {
	if defaultValue == nil {
		return false
	}
	expressions := []string{defaultValue.String()}
	if defaultValue.Expr != nil && defaultValue.Expr.String() != defaultValue.String() {
		expressions = append(expressions, defaultValue.Expr.String())
	}
	for _, expr := range expressions {
		for _, sequenceName := range nextvalSequenceNamesFromDefaultText(expr) {
			if sequenceReferenceMatches(sequenceName, defaultSchema, sequenceID) {
				return true
			}
		}
	}
	return false
}

func nextvalSequenceNamesFromDefaultText(defaultText string) []string {
	matches := nextvalQuotedArgPattern.FindAllStringSubmatch(defaultText, -1)
	if len(matches) == 0 {
		matches = nextvalBareArgPattern.FindAllStringSubmatch(defaultText, -1)
	}
	sequenceNames := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		if sequenceName := cleanNextvalSequenceName(match[1]); sequenceName != "" {
			sequenceNames = append(sequenceNames, sequenceName)
		}
	}
	return sequenceNames
}

func cleanNextvalSequenceName(name string) string {
	name = strings.TrimSpace(name)
	if beforeCast, _, ok := strings.Cut(name, "::"); ok {
		name = beforeCast
	}
	name = strings.TrimSpace(name)
	name = strings.Trim(name, "'")
	return strings.TrimSpace(name)
}

func sequenceReferenceMatches(sequenceName string, defaultSchema string, sequenceID id.Sequence) bool {
	sequenceName = cleanNextvalSequenceName(sequenceName)
	if sequenceName == "" {
		return false
	}
	tableName, err := parser.ParseQualifiedTableName(sequenceName)
	if err != nil {
		return defaultSchema == sequenceID.SchemaName() && sequenceName == sequenceID.SequenceName()
	}
	schema := defaultSchema
	if tableName.ExplicitSchema {
		schema = string(tableName.SchemaName)
	}
	return schema == sequenceID.SchemaName() && tableName.Table() == sequenceID.SequenceName()
}

// Schema implements the interface sql.ExecSourceRel.
func (c *DropSequence) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (c *DropSequence) String() string {
	return "DROP SEQUENCE"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (c *DropSequence) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (c *DropSequence) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}
