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
	pgtransform "github.com/dolthub/doltgresql/server/transform"
)

// AssignNullsNotDistinctUniqueChecks wraps DML target tables with PostgreSQL
// uniqueness checks that native Dolt indexes do not enforce directly.
func AssignNullsNotDistinctUniqueChecks(ctx *sql.Context, a *analyzer.Analyzer, node sql.Node, scope *plan.Scope, selector analyzer.RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return pgtransform.NodeWithOpaque(ctx, node, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch node := node.(type) {
		case *plan.InsertInto:
			destination, same, err := wrapPostgresUniqueTables(ctx, node.Destination)
			if err != nil || same == transform.SameTree {
				return node, same, err
			}
			newNode, err := node.WithChildren(ctx, destination)
			if err != nil {
				return nil, transform.NewTree, err
			}
			return newNode, transform.NewTree, nil
		case *plan.Update:
			child, same, err := wrapPostgresUniqueTables(ctx, node.Child)
			if err != nil || same == transform.SameTree {
				return node, same, err
			}
			newNode, err := node.WithChildren(ctx, child)
			if err != nil {
				return nil, transform.NewTree, err
			}
			return newNode, transform.NewTree, nil
		default:
			return node, transform.SameTree, nil
		}
	})
}

func wrapPostgresUniqueTables(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
	return pgtransform.NodeWithOpaque(ctx, node, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		resolvedTable, ok := node.(*plan.ResolvedTable)
		if !ok {
			return node, transform.SameTree, nil
		}
		table := resolvedTable.Table
		changed := false
		wrappedTable, wrapped, err := pgnodes.WrapPartialUniqueTable(ctx, table)
		if err != nil {
			return node, transform.SameTree, err
		}
		if wrapped {
			table = wrappedTable
			changed = true
		}
		wrappedTable, wrapped, err = pgnodes.WrapCitextUniqueTable(ctx, table)
		if err != nil {
			return node, transform.SameTree, err
		}
		if wrapped {
			table = wrappedTable
			changed = true
		}
		wrappedTable, wrapped, err = pgnodes.WrapNullsNotDistinctUniqueTable(ctx, table)
		if err != nil {
			return node, transform.SameTree, err
		}
		if wrapped {
			table = wrappedTable
			changed = true
		}
		wrappedTable, wrapped, err = pgnodes.WrapDeferrableUniqueTable(ctx, table)
		if err != nil {
			return node, transform.SameTree, err
		}
		if wrapped {
			table = wrappedTable
			changed = true
		}
		if !changed {
			return node, transform.SameTree, nil
		}
		newNode, err := resolvedTable.ReplaceTable(ctx, table)
		if err != nil {
			return nil, transform.NewTree, err
		}
		return newNode.(sql.Node), transform.NewTree, nil
	})
}
