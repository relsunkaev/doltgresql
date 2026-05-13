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

package analyzer

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	gmsexpression "github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/server/ast"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// AssignInheritedAlterTable propagates inherited parent schema changes to
// descendant child tables, matching PostgreSQL inheritance DDL behavior.
func AssignInheritedAlterTable(ctx *sql.Context, a *analyzer.Analyzer, node sql.Node, _ *plan.Scope, _ analyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(ctx, node, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := node.(type) {
		case *plan.CreateCheck:
			logicalName := core.DecodePhysicalConstraintName(n.Check.Name)
			_, options := ast.DecodeCheckConstraintNameOptions(logicalName)
			if options.NoInherit {
				return node, transform.SameTree, nil
			}
			children, err := inheritedAlterDefaultChildren(ctx, n.Table)
			if err != nil || len(children) == 0 {
				return node, transform.SameTree, err
			}
			nodes := make([]*plan.CreateCheck, 0, len(children)+1)
			skipExistingValidation := make([]bool, 0, len(children)+1)
			parentNode, skipParentValidation := inheritedNormalizeCreateCheckNode(n)
			nodes = append(nodes, parentNode)
			skipExistingValidation = append(skipExistingValidation, skipParentValidation)
			for _, child := range children {
				childNode, skipChildValidation, err := inheritedCreateCheckNode(ctx, child, n)
				if err != nil {
					return nil, transform.SameTree, err
				}
				nodes = append(nodes, childNode)
				skipExistingValidation = append(skipExistingValidation, skipChildValidation)
			}
			return pgnodes.NewInheritedCreateCheck(nodes, a.Overrides, skipExistingValidation), transform.NewTree, nil
		case *plan.AddColumn:
			children, err := inheritedAlterDefaultChildren(ctx, n.Table)
			if err != nil || len(children) == 0 {
				return node, transform.SameTree, err
			}
			nodes := make([]sql.Node, 0, len(children)+1)
			nodes = append(nodes, n)
			for _, child := range children {
				childNode, err := inheritedAddColumnNode(ctx, child, n)
				if err != nil {
					return nil, transform.SameTree, err
				}
				nodes = append(nodes, childNode)
			}
			return pgnodes.NewInheritedAlterTable(nodes), transform.NewTree, nil
		case *plan.RenameColumn:
			children, err := inheritedAlterDefaultChildren(ctx, n.Table)
			if err != nil || len(children) == 0 {
				return node, transform.SameTree, err
			}
			nodes := make([]sql.Node, 0, len(children)+1)
			nodes = append(nodes, n)
			for _, child := range children {
				childNode, err := inheritedRenameColumnNode(ctx, child, n)
				if err != nil {
					return nil, transform.SameTree, err
				}
				nodes = append(nodes, childNode)
			}
			return pgnodes.NewInheritedAlterTable(nodes), transform.NewTree, nil
		case *plan.DropColumn:
			children, err := inheritedAlterDefaultChildren(ctx, n.Table)
			if err != nil || len(children) == 0 {
				return node, transform.SameTree, err
			}
			nodes := make([]sql.Node, 0, len(children)+1)
			nodes = append(nodes, n)
			for _, child := range children {
				childNode, err := inheritedDropColumnNode(ctx, child, n)
				if err != nil {
					return nil, transform.SameTree, err
				}
				nodes = append(nodes, childNode)
			}
			return pgnodes.NewInheritedAlterTable(nodes), transform.NewTree, nil
		case *plan.ModifyColumn:
			if modifyColumnOnlyChangesNullability(ctx, n) {
				return node, transform.SameTree, nil
			}
			children, err := inheritedAlterDefaultChildren(ctx, n.Table)
			if err != nil || len(children) == 0 {
				return node, transform.SameTree, err
			}
			nodes := make([]sql.Node, 0, len(children)+1)
			nodes = append(nodes, n)
			for _, child := range children {
				childNode, err := inheritedModifyColumnNode(ctx, child, n)
				if err != nil {
					return nil, transform.SameTree, err
				}
				nodes = append(nodes, childNode)
			}
			return pgnodes.NewInheritedAlterTable(nodes), transform.NewTree, nil
		default:
			return node, transform.SameTree, nil
		}
	})
}

func inheritedCreateCheckNode(ctx *sql.Context, child *plan.ResolvedTable, parentCreate *plan.CreateCheck) (*plan.CreateCheck, bool, error) {
	checkCopy := *parentCreate.Check
	expr, _, err := transform.Expr(ctx, checkCopy.Expr, func(ctx *sql.Context, expr sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		getField, ok := expr.(*gmsexpression.GetField)
		if !ok {
			return expr, transform.SameTree, nil
		}
		index, ok := inheritedCheckColumnIndex(child.Schema(ctx), getField.Name())
		if !ok {
			return nil, transform.SameTree, sql.ErrTableColumnNotFound.New(child.Name(), getField.Name())
		}
		if index == getField.Index() {
			return expr, transform.SameTree, nil
		}
		return getField.WithIndex(index), transform.NewTree, nil
	})
	if err != nil {
		return nil, false, err
	}
	checkCopy.Expr = expr
	createCheck := plan.NewAlterAddCheck(child, &checkCopy)
	normalized, skipExistingValidation := inheritedNormalizeCreateCheckNode(createCheck)
	return normalized, skipExistingValidation, nil
}

func inheritedCheckColumnIndex(schema sql.Schema, name string) (int, bool) {
	encodedName := core.EncodePhysicalColumnName(name)
	decodedName := core.DecodePhysicalColumnName(name)
	for idx, column := range schema {
		if column.Name == name || column.Name == encodedName || core.DecodePhysicalColumnName(column.Name) == decodedName {
			return idx, true
		}
	}
	return -1, false
}

func inheritedNormalizeCreateCheckNode(createCheck *plan.CreateCheck) (*plan.CreateCheck, bool) {
	logicalName := core.DecodePhysicalConstraintName(createCheck.Check.Name)
	cleanName, options := ast.DecodeCheckConstraintNameOptions(logicalName)
	if !options.NotValid && !options.NoInherit {
		return createCheck, false
	}
	checkCopy := *createCheck.Check
	checkCopy.Name = core.EncodePhysicalConstraintName(cleanName)
	return plan.NewAlterAddCheck(createCheck.Table, &checkCopy), options.NotValid
}

func inheritedAddColumnNode(ctx *sql.Context, child *plan.ResolvedTable, parentAdd *plan.AddColumn) (sql.Node, error) {
	columnCopy := *parentAdd.Column()
	childAdd := plan.NewAddColumnResolved(child, columnCopy, parentAdd.Order(ctx))
	return childAdd.WithTargetSchema(child.Schema(ctx))
}

func inheritedRenameColumnNode(ctx *sql.Context, child *plan.ResolvedTable, parentRename *plan.RenameColumn) (sql.Node, error) {
	childRename := plan.NewRenameColumnResolved(child, parentRename.ColumnName, parentRename.NewColumnName)
	return childRename.WithTargetSchema(child.Schema(ctx))
}

func inheritedDropColumnNode(ctx *sql.Context, child *plan.ResolvedTable, parentDrop *plan.DropColumn) (sql.Node, error) {
	childDrop := plan.NewDropColumnResolved(child, parentDrop.Column)
	return childDrop.WithTargetSchema(child.Schema(ctx))
}

func inheritedModifyColumnNode(ctx *sql.Context, child *plan.ResolvedTable, parentModify *plan.ModifyColumn) (sql.Node, error) {
	childColumn := inheritedNotNullColumn(child.Schema(ctx), parentModify.Column())
	if childColumn == nil {
		return nil, sql.ErrTableColumnNotFound.New(child.Name(), parentModify.Column())
	}
	columnCopy := *childColumn
	parentColumn := parentModify.NewColumn()
	columnCopy.Type = parentColumn.Type
	columnCopy.Nullable = parentColumn.Nullable
	columnCopy.Default = parentColumn.Default
	columnCopy.Generated = parentColumn.Generated
	columnCopy.OnUpdate = parentColumn.OnUpdate
	childModify := plan.NewModifyColumnResolved(child, childColumn.Name, columnCopy, parentModify.Order(ctx))
	return childModify.WithTargetSchema(child.Schema(ctx))
}
