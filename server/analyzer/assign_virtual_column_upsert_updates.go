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

// AssignVirtualColumnUpsertUpdates makes ON CONFLICT DO UPDATE maintain
// expression-index virtual columns when the duplicate-key old row came from
// storage rather than a VirtualColumnTable projection.
func AssignVirtualColumnUpsertUpdates(ctx *sql.Context, a *analyzer.Analyzer, node sql.Node, scope *plan.Scope, selector analyzer.RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return pgtransform.NodeWithOpaque(ctx, node, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		insert, ok := node.(*plan.InsertInto)
		if !ok || insert.OnDupExprs == nil || !insert.OnDupExprs.HasUpdates() {
			return node, transform.SameTree, nil
		}
		destination, same, err := wrapVirtualColumnUpsertTarget(ctx, insert.Destination)
		if err != nil || same == transform.SameTree {
			return node, same, err
		}
		newNode, err := insert.WithChildren(ctx, destination)
		if err != nil {
			return nil, transform.NewTree, err
		}
		return newNode, transform.NewTree, nil
	})
}

func wrapVirtualColumnUpsertTarget(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
	return pgtransform.NodeWithOpaque(ctx, node, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		resolvedTable, ok := node.(*plan.ResolvedTable)
		if !ok {
			return node, transform.SameTree, nil
		}
		wrapped, wasWrapped := pgnodes.WrapVirtualColumnUpdateTable(ctx, resolvedTable.Table)
		if !wasWrapped {
			return node, transform.SameTree, nil
		}
		newNode, err := resolvedTable.ReplaceTable(ctx, wrapped)
		if err != nil {
			return nil, transform.NewTree, err
		}
		return newNode.(sql.Node), transform.NewTree, nil
	})
}
