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
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	pgexprs "github.com/dolthub/doltgresql/server/expression"
)

// ValidateCheckConstraint implements ALTER TABLE ... VALIDATE CONSTRAINT for
// table CHECK and foreign key constraints.
type ValidateCheckConstraint struct {
	ifExists   bool
	schema     string
	table      string
	constraint string
	Runner     pgexprs.StatementRunner
}

var _ sql.ExecSourceRel = (*ValidateCheckConstraint)(nil)
var _ sql.Expressioner = (*ValidateCheckConstraint)(nil)
var _ vitess.Injectable = (*ValidateCheckConstraint)(nil)

// NewValidateCheckConstraint returns a new *ValidateCheckConstraint.
func NewValidateCheckConstraint(ifExists bool, schema string, table string, constraint string) *ValidateCheckConstraint {
	return &ValidateCheckConstraint{
		ifExists:   ifExists,
		schema:     schema,
		table:      table,
		constraint: constraint,
	}
}

// Children implements sql.ExecSourceRel.
func (v *ValidateCheckConstraint) Children() []sql.Node {
	return nil
}

// Expressions implements sql.Expressioner.
func (v *ValidateCheckConstraint) Expressions() []sql.Expression {
	return []sql.Expression{v.Runner}
}

// IsReadOnly implements sql.ExecSourceRel.
func (v *ValidateCheckConstraint) IsReadOnly() bool {
	return false
}

// Resolved implements sql.ExecSourceRel.
func (v *ValidateCheckConstraint) Resolved() bool {
	return true
}

// RowIter implements sql.ExecSourceRel.
func (v *ValidateCheckConstraint) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	resolver := AlterRelationOwner{
		kind:     relationOwnerKindTable,
		ifExists: v.ifExists,
		schema:   v.schema,
		name:     v.table,
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

	table, err := core.GetSqlTableFromContext(ctx, "", doltdb.TableName{Name: v.table, Schema: relation.Schema})
	if err != nil {
		return nil, err
	}
	if table == nil {
		if v.ifExists {
			return sql.RowsToRowIter(), nil
		}
		return nil, errors.Errorf(`relation "%s" does not exist`, v.table)
	}
	if rowIter, found, err := v.validateCheckConstraint(ctx, relation, table); err != nil || found {
		return rowIter, err
	}
	if rowIter, found, err := v.validateForeignKeyConstraint(ctx, relation, table); err != nil || found {
		return rowIter, err
	}
	return nil, errors.Errorf(`constraint "%s" of relation "%s" does not exist`, core.DecodePhysicalConstraintName(v.constraint), v.table)
}

func (v *ValidateCheckConstraint) validateCheckConstraint(ctx *sql.Context, relation doltdb.TableName, table sql.Table) (sql.RowIter, bool, error) {
	checkTable, ok := table.(sql.CheckTable)
	if !ok && table != nil {
		checkTable, ok = sql.GetUnderlyingTable(table).(sql.CheckTable)
	}
	if !ok {
		return nil, false, nil
	}
	checks, err := checkTable.GetChecks(ctx)
	if err != nil {
		return nil, false, err
	}
	var target *sql.CheckDefinition
	for i := range checks {
		if constraintNameMatches(checks[i].Name, v.constraint) {
			check := checks[i]
			target = &check
			break
		}
	}
	if target == nil {
		return nil, false, nil
	}

	query := fmt.Sprintf(
		"SELECT 1 FROM %s WHERE NOT (%s) LIMIT 1",
		qualifiedCheckTableName(relation.Schema, relation.Name),
		target.CheckExpression,
	)
	hasViolation, err := v.hasViolation(ctx, query)
	if err != nil {
		return nil, true, err
	}
	if hasViolation {
		return nil, true, sql.ErrCheckConstraintViolated.New(core.DecodePhysicalConstraintName(target.Name))
	}
	return sql.RowsToRowIter(), true, nil
}

func (v *ValidateCheckConstraint) validateForeignKeyConstraint(ctx *sql.Context, relation doltdb.TableName, table sql.Table) (sql.RowIter, bool, error) {
	fkTable, ok := typedTableForeignKeyTable(table)
	if !ok {
		return nil, false, nil
	}
	foreignKeys, err := fkTable.GetDeclaredForeignKeys(ctx)
	if err != nil {
		return nil, false, err
	}
	var target *sql.ForeignKeyConstraint
	for i := range foreignKeys {
		if constraintNameMatches(foreignKeys[i].Name, v.constraint) {
			foreignKey := foreignKeys[i]
			target = &foreignKey
			break
		}
	}
	if target == nil {
		return nil, false, nil
	}

	parentSchema := target.ParentSchema
	if parentSchema == "" {
		parentSchema = relation.Schema
	}
	parentTable, err := core.GetSqlTableFromContext(ctx, target.ParentDatabase, doltdb.TableName{Name: target.ParentTable, Schema: parentSchema})
	if err != nil {
		return nil, true, err
	}
	parentForeignKeyTable, ok := typedTableForeignKeyTable(parentTable)
	if !ok {
		return nil, true, sql.ErrNoForeignKeySupport.New(target.ParentTable)
	}

	foreignKey := *target
	foreignKey.IsResolved = false
	if err = plan.ResolveForeignKey(ctx, fkTable, parentForeignKeyTable, foreignKey, false, true, true); err != nil {
		return nil, true, err
	}
	return sql.RowsToRowIter(), true, nil
}

// Schema implements sql.ExecSourceRel.
func (v *ValidateCheckConstraint) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements sql.ExecSourceRel.
func (v *ValidateCheckConstraint) String() string {
	return "VALIDATE CHECK CONSTRAINT"
}

// WithChildren implements sql.ExecSourceRel.
func (v *ValidateCheckConstraint) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(v, children...)
}

// WithExpressions implements sql.Expressioner.
func (v *ValidateCheckConstraint) WithExpressions(ctx *sql.Context, expressions ...sql.Expression) (sql.Node, error) {
	if len(expressions) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(v, len(expressions), 1)
	}
	newV := *v
	newV.Runner = expressions[0].(pgexprs.StatementRunner)
	return &newV, nil
}

// WithResolvedChildren implements vitess.Injectable.
func (v *ValidateCheckConstraint) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return v, nil
}

func (v *ValidateCheckConstraint) hasViolation(ctx *sql.Context, query string) (bool, error) {
	if v.Runner.Runner == nil {
		return false, errors.Errorf("statement runner is not available")
	}
	rows, err := sql.RunInterpreted(ctx, func(subCtx *sql.Context) ([]sql.Row, error) {
		_, rowIter, _, err := v.Runner.Runner.QueryWithBindings(subCtx, query, nil, nil, nil)
		if err != nil {
			return nil, err
		}
		return sql.RowIterToRows(subCtx, rowIter)
	})
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}

func qualifiedCheckTableName(schema string, table string) string {
	if schema == "" {
		return quoteCheckIdentifier(table)
	}
	return quoteCheckIdentifier(schema) + "." + quoteCheckIdentifier(table)
}

func quoteCheckIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func constraintNameMatches(stored string, requested string) bool {
	if stored == requested {
		return true
	}
	return core.DecodePhysicalConstraintName(stored) == core.DecodePhysicalConstraintName(requested)
}
