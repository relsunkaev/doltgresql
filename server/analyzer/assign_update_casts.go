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

package analyzer

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"

	pgexprs "github.com/dolthub/doltgresql/server/expression"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// AssignUpdateCasts adds the appropriate assign casts for updates.
func AssignUpdateCasts(ctx *sql.Context, a *analyzer.Analyzer, node sql.Node, scope *plan.Scope, selector analyzer.RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	update, ok := node.(*plan.Update)
	if !ok {
		return node, transform.SameTree, nil
	}
	var newUpdate sql.Node
	switch child := update.Child.(type) {
	case *plan.UpdateSource:
		newUpdateSource, err := assignUpdateCastsHandleSource(ctx, a, child)
		if err != nil {
			return nil, transform.NewTree, err
		}
		newUpdate, err = update.WithChildren(ctx, newUpdateSource)
		if err != nil {
			return nil, transform.NewTree, err
		}
	case *plan.ForeignKeyHandler:
		updateSource, ok := child.OriginalNode.(*plan.UpdateSource)
		if !ok {
			return nil, transform.NewTree, errors.Errorf("UPDATE: assumption that Foreign Key child is always UpdateSource is incorrect: %T", child.OriginalNode)
		}
		newUpdateSource, err := assignUpdateCastsHandleSource(ctx, a, updateSource)
		if err != nil {
			return nil, transform.NewTree, err
		}
		newHandler, err := child.WithChildren(ctx, newUpdateSource)
		if err != nil {
			return nil, transform.NewTree, err
		}
		newUpdate, err = update.WithChildren(ctx, newHandler)
		if err != nil {
			return nil, transform.NewTree, err
		}
	case *plan.UpdateJoin:
		updateSource, ok := child.Child.(*plan.UpdateSource)
		if !ok {
			return nil, transform.NewTree, fmt.Errorf("UPDATE: unknown source type: %T", child.Child)
		}

		newUpdateSource, err := assignUpdateCastsHandleSource(ctx, a, updateSource)
		if err != nil {
			return nil, transform.NewTree, err
		}
		newHandler, err := child.WithChildren(ctx, newUpdateSource)
		if err != nil {
			return nil, transform.NewTree, err
		}
		newUpdate, err = update.WithChildren(ctx, newHandler)
		if err != nil {
			return nil, transform.NewTree, err
		}
	default:
		return nil, transform.NewTree, errors.Errorf("UPDATE: unknown source type: %T", child)
	}
	return newUpdate, transform.NewTree, nil
}

// assignUpdateCastsHandleSource handles the *plan.UpdateSource portion of AssignUpdateCasts.
func assignUpdateCastsHandleSource(ctx *sql.Context, a *analyzer.Analyzer, updateSource *plan.UpdateSource) (*plan.UpdateSource, error) {
	updateExprs := updateSource.UpdateExprs
	newUpdateExprs, err := assignUpdateFieldCasts(ctx, a, updateExprs.AllExpressions(), nil)
	if err != nil {
		return nil, err
	}
	numExplicitExprs := len(updateExprs.ExplicitUpdateExprs())
	if numExplicitExprs > 1 {
		newUpdateExprs = append(
			[]sql.Expression{pgexprs.NewSimultaneousUpdate(newUpdateExprs[:numExplicitExprs])},
			newUpdateExprs[numExplicitExprs:]...,
		)
		numExplicitExprs = 1
	}
	return plan.NewUpdateSource(
		updateSource.Child,
		updateSource.Ignore,
		plan.NewUpdateExprs(newUpdateExprs, numExplicitExprs),
	), nil
}

func assignUpdateFieldCasts(ctx *sql.Context, a *analyzer.Analyzer, updateExprs []sql.Expression, destinationSchema sql.Schema) ([]sql.Expression, error) {
	newUpdateExprs := make([]sql.Expression, len(updateExprs))
	for i, updateExpr := range updateExprs {
		setField, ok := updateExpr.(*expression.SetField)
		if !ok {
			return nil, errors.Errorf("UPDATE: assumption that expression is always SetField is incorrect: %T", updateExpr)
		}
		toType, ok := setField.LeftChild.Type(ctx).(*pgtypes.DoltgresType)
		if !ok {
			// Only non-Doltgres destination tables will have GMS types (such as system tables), so we don't error here
			toType = pgtypes.FromGmsType(setField.LeftChild.Type(ctx))
		}
		rightChild, same, err := transform.Expr(ctx, setField.RightChild, replaceArithmeticExpression)
		if err != nil {
			return nil, err
		}
		if !same {
			newSetField, err := setField.WithChildren(ctx, setField.LeftChild, rightChild)
			if err != nil {
				return nil, err
			}
			setField = newSetField.(*expression.SetField)
		}
		fromSqlType := setFieldRightChildType(ctx, setField.RightChild)
		if fromSqlType == nil {
			defaultExpr, err := updateDefaultExpression(ctx, a, setField, toType, destinationSchema)
			if err != nil {
				return nil, err
			}
			newSetField, err := setField.WithChildren(ctx, setField.LeftChild, defaultExpr)
			if err != nil {
				return nil, err
			}
			setField = newSetField.(*expression.SetField)
			fromSqlType = setField.RightChild.Type(ctx)
		}
		fromType, err := updateSourceType(fromSqlType)
		if err != nil {
			return nil, errors.Wrapf(err, "UPDATE: non-Doltgres type found in source: %s", setField.RightChild.String())
		}
		// We only assign the existing expression if the types perfectly match (same parameters), otherwise we'll cast
		if fromType.Equals(toType) {
			newUpdateExprs[i] = setField
		} else {
			newSetField, err := setField.WithChildren(ctx, setField.LeftChild, pgexprs.NewAssignmentCast(setField.RightChild, fromType, toType))
			if err != nil {
				return nil, err
			}
			newUpdateExprs[i] = newSetField
		}
	}
	return newUpdateExprs, nil
}

func updateSourceType(typ sql.Type) (*pgtypes.DoltgresType, error) {
	if typ == nil || typ == types.Null {
		return pgtypes.Unknown, nil
	}
	if doltgresType, ok := typ.(*pgtypes.DoltgresType); ok {
		return doltgresType, nil
	}
	doltgresType, err := pgtypes.FromGmsTypeToDoltgresType(typ)
	if err != nil {
		return nil, errors.Errorf("%s", typ.String())
	}
	return doltgresType, nil
}

func setFieldRightChildType(ctx *sql.Context, expr sql.Expression) sql.Type {
	if isDefaultColumnExpression(expr) {
		return nil
	}
	return expr.Type(ctx)
}

func isDefaultColumnExpression(expr sql.Expression) bool {
	switch expr := expr.(type) {
	case *expression.DefaultColumn:
		return true
	case *expression.Wrapper:
		_, ok := expr.Unwrap().(*expression.DefaultColumn)
		return ok
	default:
		return false
	}
}

func updateDefaultExpression(ctx *sql.Context, a *analyzer.Analyzer, setField *expression.SetField, toType *pgtypes.DoltgresType, destinationSchema sql.Schema) (sql.Expression, error) {
	if defaultValue := updateColumnDefaultExpression(setField, destinationSchema); defaultValue != nil {
		return defaultValue, nil
	}
	if toType.TypType == pgtypes.TypeType_Domain && toType.Default != "" {
		tableName := ""
		if getField, ok := setField.LeftChild.(*expression.GetField); ok {
			tableName = getField.Table()
		}
		defaultValue, err := getDomainDefault(ctx, a, toType.Default, tableName, toType, setField.LeftChild.IsNullable(ctx))
		if err != nil {
			return nil, err
		}
		if defaultValue != nil {
			return defaultValue, nil
		}
	}
	return expression.NewLiteral(nil, toType), nil
}

func updateColumnDefaultExpression(setField *expression.SetField, destinationSchema sql.Schema) *sql.ColumnDefaultValue {
	if destinationSchema == nil {
		return nil
	}
	getField, ok := setField.LeftChild.(*expression.GetField)
	if !ok {
		return nil
	}
	if index := destinationSchema.IndexOfColName(getField.Name()); index >= 0 {
		return destinationSchema[index].Default
	}
	if index := getField.Index(); index >= 0 && index < len(destinationSchema) {
		return destinationSchema[index].Default
	}
	return nil
}
