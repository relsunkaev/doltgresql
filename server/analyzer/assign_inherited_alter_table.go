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
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// AssignInheritedAlterTable propagates inherited parent schema changes to
// descendant child tables, matching PostgreSQL inheritance DDL behavior.
func AssignInheritedAlterTable(ctx *sql.Context, _ *analyzer.Analyzer, node sql.Node, _ *plan.Scope, _ analyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(ctx, node, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := node.(type) {
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
