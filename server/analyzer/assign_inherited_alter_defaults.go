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

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/functions"
	pgnodes "github.com/dolthub/doltgresql/server/node"
	"github.com/dolthub/doltgresql/server/tablemetadata"
)

// AssignInheritedAlterDefaults propagates parent-column default changes to
// inherited child tables, matching PostgreSQL inheritance DDL behavior.
func AssignInheritedAlterDefaults(ctx *sql.Context, _ *analyzer.Analyzer, node sql.Node, _ *plan.Scope, _ analyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(ctx, node, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := node.(type) {
		case *plan.AlterDefaultSet:
			children, err := inheritedAlterDefaultChildren(ctx, n.Table)
			if err != nil || len(children) == 0 {
				return node, transform.SameTree, err
			}
			nodes := make([]sql.Node, 0, len(children)+1)
			nodes = append(nodes, n)
			for _, child := range children {
				nodes = append(nodes, plan.NewAlterDefaultSet(child.Database(), child, n.ColumnName, n.Default))
			}
			return pgnodes.NewInheritedAlterDefault(nodes), transform.NewTree, nil
		case *plan.AlterDefaultDrop:
			children, err := inheritedAlterDefaultChildren(ctx, n.Table)
			if err != nil || len(children) == 0 {
				return node, transform.SameTree, err
			}
			nodes := make([]sql.Node, 0, len(children)+1)
			nodes = append(nodes, n)
			for _, child := range children {
				nodes = append(nodes, plan.NewAlterDefaultDrop(child.Database(), child, n.ColumnName))
			}
			return pgnodes.NewInheritedAlterDefault(nodes), transform.NewTree, nil
		default:
			return node, transform.SameTree, nil
		}
	})
}

func inheritedAlterDefaultChildren(ctx *sql.Context, tableNode sql.Node) ([]*plan.ResolvedTable, error) {
	parent, ok := tableNode.(*plan.ResolvedTable)
	if !ok {
		return nil, nil
	}
	parentID, ok, err := id.GetFromTable(ctx, parent.Table)
	if err != nil || !ok {
		return nil, err
	}
	parentRef := tablemetadata.InheritedTable{Schema: parentID.SchemaName(), Name: parentID.TableName()}
	seen := map[string]struct{}{inheritedAlterDefaultKey(parentRef): {}}
	return inheritedAlterDefaultDescendants(ctx, parentRef, seen)
}

func inheritedAlterDefaultDescendants(ctx *sql.Context, parent tablemetadata.InheritedTable, seen map[string]struct{}) ([]*plan.ResolvedTable, error) {
	var children []*plan.ResolvedTable
	err := functions.IterateCurrentDatabase(ctx, functions.Callbacks{
		Table: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable) (cont bool, err error) {
			childRef := tablemetadata.InheritedTable{Schema: schema.Item.SchemaName(), Name: table.Item.Name()}
			childKey := inheritedAlterDefaultKey(childRef)
			if _, ok := seen[childKey]; ok {
				return true, nil
			}
			for _, inheritedParent := range tablemetadata.Inherits(inheritedAlterDefaultComment(table.Item)) {
				if inheritedParent.Schema == "" {
					inheritedParent.Schema = schema.Item.SchemaName()
				}
				if !inheritedAlterDefaultParentMatches(inheritedParent, parent) {
					continue
				}
				seen[childKey] = struct{}{}
				child := plan.NewResolvedTable(table.Item, schema.Item, nil)
				children = append(children, child)
				grandchildren, err := inheritedAlterDefaultDescendants(ctx, childRef, seen)
				if err != nil {
					return false, err
				}
				children = append(children, grandchildren...)
				break
			}
			return true, nil
		},
	})
	return children, err
}

func inheritedAlterDefaultComment(table sql.Table) string {
	for table != nil {
		if commented, ok := table.(sql.CommentedTable); ok {
			return commented.Comment()
		}
		wrapper, ok := table.(sql.TableWrapper)
		if !ok {
			return ""
		}
		table = wrapper.Underlying()
	}
	return ""
}

func inheritedAlterDefaultParentMatches(left tablemetadata.InheritedTable, right tablemetadata.InheritedTable) bool {
	return strings.EqualFold(left.Schema, right.Schema) && strings.EqualFold(left.Name, right.Name)
}

func inheritedAlterDefaultKey(table tablemetadata.InheritedTable) string {
	return strings.ToLower(table.Schema) + "." + strings.ToLower(table.Name)
}
