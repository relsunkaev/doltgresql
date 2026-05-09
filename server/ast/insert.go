// Copyright 2023 Dolthub, Inc.
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

package ast

import (
	"github.com/cockroachdb/errors"

	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/auth"
)

// nodeInsert handles *tree.Insert nodes.
func nodeInsert(ctx *Context, node *tree.Insert) (insert *vitess.Insert, err error) {
	if node == nil {
		return nil, nil
	}
	ctx.Auth().PushAuthType(auth.AuthType_INSERT)
	defer ctx.Auth().PopAuthType()

	var returningExprs vitess.SelectExprs
	if returning, ok := node.Returning.(*tree.ReturningExprs); ok {
		// TODO: PostgreSQL will apply any triggers before returning the value; need to test this.
		returningExprs, err = nodeSelectExprs(ctx, tree.SelectExprs(*returning))
		if err != nil {
			return nil, err
		}
	}
	var ignore string
	var onDuplicate vitess.OnDup

	if node.OnConflict != nil {
		if isIgnore(node.OnConflict) {
			ignore = vitess.IgnoreStr
		} else if supportedOnConflictClause(node.OnConflict) {
			// TODO: we are ignoring the column names, which are used to infer which index under conflict is to be checked
			var updateExprs vitess.AssignmentExprs
			var whereExpr vitess.Expr
			if err := ctx.WithExcludedRefs(func() error {
				converted, convertErr := nodeUpdateExprs(ctx, node.OnConflict.Exprs)
				if convertErr != nil {
					return convertErr
				}
				updateExprs = converted
				if node.OnConflict.Where != nil {
					whereExpr, convertErr = nodeExpr(ctx, node.OnConflict.Where.Expr)
					if convertErr != nil {
						return convertErr
					}
				}
				return nil
			}); err != nil {
				return nil, err
			}
			updateExprs = applyOnConflictUpdateWhere(updateExprs, whereExpr)
			for _, updateExpr := range updateExprs {
				onDuplicate = append(onDuplicate, updateExpr)
			}
		} else {
			return nil, errors.Errorf("the ON CONFLICT clause provided is not yet supported")
		}
	}
	var tableName vitess.TableName
	switch node := node.Table.(type) {
	case *tree.AliasedTableExpr:
		return nil, errors.Errorf("aliased inserts are not yet supported")
	case *tree.TableName:
		var err error
		tableName, err = nodeTableName(ctx, node)
		if err != nil {
			return nil, err
		}
	case *tree.TableRef:
		return nil, errors.Errorf("table refs are not yet supported")
	default:
		return nil, errors.Errorf("unknown table name type in INSERT: `%T`", node)
	}
	var columns []vitess.ColIdent
	if len(node.Columns) > 0 {
		columns = make([]vitess.ColIdent, len(node.Columns))
		for i := range node.Columns {
			columns[i] = vitess.NewColIdent(string(node.Columns[i]))
		}
	}
	with, err := nodeWith(ctx, node.With)
	if err != nil {
		return nil, err
	}
	var rows vitess.InsertRows
	rows, err = nodeSelect(ctx, node.Rows)
	if err != nil {
		return nil, err
	}

	// For a ValuesStatement with simple rows, GMS expects AliasedValues
	if vSelect, ok := rows.(*vitess.Select); ok && len(vSelect.From) == 1 {
		if aliasedStmt, ok := vSelect.From[0].(*vitess.AliasedTableExpr); ok {
			if valsStmt, ok := aliasedStmt.Expr.(*vitess.ValuesStatement); ok {
				var vals vitess.Values
				if len(valsStmt.Rows) == 0 {
					vals = []vitess.ValTuple{{}}
				} else {
					vals = valsStmt.Rows
				}
				rows = &vitess.AliasedValues{
					Values: vals,
				}
			}
		}
	}
	return &vitess.Insert{
		Action:    vitess.InsertStr,
		Ignore:    ignore,
		Table:     tableName,
		Returning: returningExprs,
		With:      with,
		Columns:   columns,
		Rows:      rows,
		OnDup:     onDuplicate,
		Auth: vitess.AuthInformation{
			AuthType:    auth.AuthType_INSERT,
			TargetType:  auth.AuthTargetType_TableIdentifiers,
			TargetNames: []string{tableName.DbQualifier.String(), tableName.SchemaQualifier.String(), tableName.Name.String()},
		},
	}, nil
}

// isIgnore returns true if the ON CONFLICT clause provided is equivalent to INSERT IGNORE in GMS
func isIgnore(conflict *tree.OnConflict) bool {
	return conflict.Exprs == nil &&
		conflict.Where == nil &&
		conflict.DoNothing
}

// supportedOnConflictClause returns true if the ON CONFLICT clause given can be represented as
// an ON DUPLICATE KEY UPDATE clause in GMS. The clause's WHERE predicate is supported by
// rewriting each `col = expr` pair as `col = CASE WHEN <pred> THEN <expr> ELSE col END` so
// the ELSE branch keeps the existing row unchanged when the predicate is false.
// The arbiter predicate (`ON CONFLICT (col) WHERE arb_pred`) is preserved in
// the original query text; ValidateOnConflictArbiter reparses that text and
// matches it against metadata-backed partial unique indexes.
func supportedOnConflictClause(conflict *tree.OnConflict) bool {
	return true
}

// applyOnConflictUpdateWhere wraps each assignment expression with a CASE
// expression that preserves the existing column value when the ON CONFLICT
// DO UPDATE WHERE predicate evaluates to false. Returns the input unchanged
// when no predicate is provided.
func applyOnConflictUpdateWhere(updateExprs vitess.AssignmentExprs, whereExpr vitess.Expr) vitess.AssignmentExprs {
	if whereExpr == nil || len(updateExprs) == 0 {
		return updateExprs
	}
	wrapped := make(vitess.AssignmentExprs, len(updateExprs))
	for i, ae := range updateExprs {
		// The bare ColName references the existing row in
		// ON DUPLICATE KEY UPDATE context. When the predicate
		// is false the column is set to its prior value, which
		// GMS skips writing when oldRow == newRow.
		oldVal := &vitess.ColName{Name: ae.Name.Name}
		wrapped[i] = &vitess.AssignmentExpr{
			Name: ae.Name,
			Expr: &vitess.CaseExpr{
				Whens: []*vitess.When{
					{
						Cond: whereExpr,
						Val:  ae.Expr,
					},
				},
				Else: oldVal,
			},
		}
	}
	return wrapped
}
