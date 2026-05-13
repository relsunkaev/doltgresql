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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// AssignInheritedNotNull propagates ALTER COLUMN SET/DROP NOT NULL from
// inherited parent tables to their inherited child tables.
func AssignInheritedNotNull(ctx *sql.Context, _ *analyzer.Analyzer, node sql.Node, _ *plan.Scope, _ analyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(ctx, node, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		modify, ok := node.(*plan.ModifyColumn)
		if !ok || !modifyColumnOnlyChangesNullability(ctx, modify) {
			return node, transform.SameTree, nil
		}
		children, err := inheritedAlterDefaultChildren(ctx, modify.Table)
		if err != nil || len(children) == 0 {
			return node, transform.SameTree, err
		}

		nodes := make([]sql.Node, 0, len(children)+1)
		if !modify.NewColumn().Nullable {
			for i := len(children) - 1; i >= 0; i-- {
				childModify, err := inheritedNotNullModifyNode(ctx, children[i], modify)
				if err != nil {
					return nil, transform.SameTree, err
				}
				nodes = append(nodes, childModify)
			}
			nodes = append(nodes, modify)
		} else {
			nodes = append(nodes, modify)
			for _, child := range children {
				childModify, err := inheritedNotNullModifyNode(ctx, child, modify)
				if err != nil {
					return nil, transform.SameTree, err
				}
				nodes = append(nodes, childModify)
			}
		}
		return pgnodes.NewInheritedAlterNotNull(nodes, modify.Column(), !modify.NewColumn().Nullable), transform.NewTree, nil
	})
}

func modifyColumnOnlyChangesNullability(ctx *sql.Context, modify *plan.ModifyColumn) bool {
	parent, ok := modify.Table.(*plan.ResolvedTable)
	if !ok || modify.Order(ctx) != nil {
		return false
	}
	oldColumn := inheritedNotNullColumn(parent.Schema(ctx), modify.Column())
	if oldColumn == nil {
		return false
	}
	newColumn := modify.NewColumn()
	return strings.EqualFold(oldColumn.Name, newColumn.Name) &&
		oldColumn.Nullable != newColumn.Nullable &&
		oldColumn.Type.String() == newColumn.Type.String()
}

func inheritedNotNullModifyNode(ctx *sql.Context, child *plan.ResolvedTable, parentModify *plan.ModifyColumn) (sql.Node, error) {
	childColumn := inheritedNotNullColumn(child.Schema(ctx), parentModify.Column())
	if childColumn == nil {
		return nil, sql.ErrTableColumnNotFound.New(child.Name(), parentModify.Column())
	}
	columnCopy := *childColumn
	columnCopy.Nullable = parentModify.NewColumn().Nullable
	childModify := plan.NewModifyColumnResolved(child, childColumn.Name, columnCopy, nil)
	return childModify.WithTargetSchema(child.Schema(ctx))
}

func inheritedNotNullColumn(schema sql.Schema, name string) *sql.Column {
	for _, column := range schema {
		if strings.EqualFold(column.Name, name) {
			return column
		}
	}
	return nil
}
