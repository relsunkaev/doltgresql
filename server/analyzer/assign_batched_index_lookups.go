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

// AssignBatchedIndexLookups wraps PostgreSQL btree lookup-join access with a
// small execution-time duplicate lookup cache. GMS still drives dynamic lookup
// joins one outer row at a time, so this removes repeated storage probes for
// duplicate keys without changing static index scan planning.
func AssignBatchedIndexLookups(ctx *sql.Context, a *analyzer.Analyzer, node sql.Node, scope *plan.Scope, selector analyzer.RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return pgtransform.NodeWithOpaque(ctx, node, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		indexedAccess, ok := node.(*plan.IndexedTableAccess)
		if !ok || indexedAccess.Typ != plan.ItaTypeLookup {
			return node, transform.SameTree, nil
		}
		wrappedTable, wrapped := pgnodes.WrapBatchedIndexLookupIndexedTable(indexedAccess.Table, indexedAccess.Index())
		if !wrapped {
			return node, transform.SameTree, nil
		}
		newNode, err := indexedAccess.WithTable(wrappedTable)
		if err != nil {
			return nil, transform.NewTree, err
		}
		return newNode, transform.NewTree, nil
	})
}
