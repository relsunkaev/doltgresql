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

	"github.com/dolthub/doltgresql/core/id"
	pgnodes "github.com/dolthub/doltgresql/server/node"
	pgtransform "github.com/dolthub/doltgresql/server/transform"
)

// AssignIndexStats wraps chosen index access paths so pg_stat_*_indexes reports
// live scan counters instead of static zeroes.
func AssignIndexStats(ctx *sql.Context, _ *analyzer.Analyzer, node sql.Node, _ *plan.Scope, _ analyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return pgtransform.NodeWithOpaque(ctx, node, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		indexedAccess, ok := node.(*plan.IndexedTableAccess)
		if !ok {
			return node, transform.SameTree, nil
		}
		index := indexedAccess.Index()
		if index == nil {
			return node, transform.SameTree, nil
		}
		table := indexedAccess.TableNode.UnderlyingTable()
		if table == nil {
			return node, transform.SameTree, nil
		}
		schemaName, err := schemaNameForTable(ctx, table)
		if err != nil {
			return nil, transform.NewTree, err
		}
		if schemaName == "" {
			return node, transform.SameTree, nil
		}
		indexOID := id.Cache().ToOID(id.NewIndex(schemaName, table.Name(), index.ID()).AsId())
		wrappedTable, wrapped := pgnodes.WrapIndexStatsTable(indexedAccess.Table, indexOID)
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
