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

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	pgnodes "github.com/dolthub/doltgresql/server/node"
	pgtransform "github.com/dolthub/doltgresql/server/transform"
)

// AssignJsonbGinMaintainers wraps DML target tables with JSONB GIN posting
// maintenance when those targets have Doltgres-managed GIN metadata.
func AssignJsonbGinMaintainers(ctx *sql.Context, a *analyzer.Analyzer, node sql.Node, scope *plan.Scope, selector analyzer.RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return pgtransform.NodeWithOpaque(ctx, node, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch node := node.(type) {
		case *plan.InsertInto:
			destination, same, err := wrapJsonbGinMaintainedTables(ctx, node.Destination)
			if err != nil || same == transform.SameTree {
				return node, same, err
			}
			newNode, err := node.WithChildren(ctx, destination)
			if err != nil {
				return nil, transform.NewTree, err
			}
			return newNode, transform.NewTree, nil
		case *plan.Update:
			child, same, err := wrapJsonbGinMaintainedTables(ctx, node.Child)
			if err != nil || same == transform.SameTree {
				return node, same, err
			}
			newNode, err := node.WithChildren(ctx, child)
			if err != nil {
				return nil, transform.NewTree, err
			}
			return newNode, transform.NewTree, nil
		case *plan.DeleteFrom:
			child, sameChild, err := wrapJsonbGinMaintainedTables(ctx, node.Child)
			if err != nil {
				return nil, transform.NewTree, err
			}
			targets := node.GetDeleteTargets()
			newTargets := make([]sql.Node, len(targets))
			sameTargets := transform.SameTree
			for i, target := range targets {
				newTargets[i], sameTargets, err = wrapJsonbGinMaintainedTables(ctx, target)
				if err != nil {
					return nil, transform.NewTree, err
				}
				if sameTargets == transform.NewTree {
					sameChild = transform.NewTree
				}
			}
			if sameChild == transform.SameTree {
				return node, transform.SameTree, nil
			}
			newNode, err := node.WithChildren(ctx, child)
			if err != nil {
				return nil, transform.NewTree, err
			}
			return newNode.(*plan.DeleteFrom).WithTargets(newTargets), transform.NewTree, nil
		default:
			return node, transform.SameTree, nil
		}
	})
}

func wrapJsonbGinMaintainedTables(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
	return pgtransform.NodeWithOpaque(ctx, node, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		resolvedTable, ok := node.(*plan.ResolvedTable)
		if !ok {
			return node, transform.SameTree, nil
		}
		schemaName, err := schemaNameForTable(ctx, resolvedTable.Table)
		if err != nil {
			return nil, transform.NewTree, err
		}
		wrappedTable, wrapped, err := pgnodes.WrapJsonbGinMaintainedTable(ctx, schemaName, resolvedTable.Table)
		if err != nil || !wrapped {
			return node, transform.SameTree, err
		}
		newNode, err := resolvedTable.ReplaceTable(ctx, wrappedTable)
		if err != nil {
			return nil, transform.NewTree, err
		}
		return newNode.(sql.Node), transform.NewTree, nil
	})
}

func schemaNameForTable(ctx *sql.Context, table sql.Table) (string, error) {
	tableID, ok, err := id.GetFromTable(ctx, table)
	if err == nil && ok {
		return tableID.SchemaName(), nil
	}
	return core.GetSchemaName(ctx, nil, "")
}
