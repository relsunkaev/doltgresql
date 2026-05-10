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
	gmsanalyzer "github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	pgtransform "github.com/dolthub/doltgresql/server/transform"
)

func PruneNotNullSortProbes(ctx *sql.Context, _ *gmsanalyzer.Analyzer, node sql.Node, _ *plan.Scope, _ gmsanalyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return pgtransform.NodeWithOpaque(ctx, node, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		sortNode, ok := node.(*plan.Sort)
		if !ok {
			return node, transform.SameTree, nil
		}
		fields := pruneNotNullSortProbeFields(ctx, sortNode.SortFields)
		if len(fields) == len(sortNode.SortFields) {
			return node, transform.SameTree, nil
		}
		if len(fields) == 0 {
			return sortNode.Child, transform.NewTree, nil
		}
		return plan.NewSort(fields, sortNode.Child), transform.NewTree, nil
	})
}

func pruneNotNullSortProbeFields(ctx *sql.Context, fields sql.SortFields) sql.SortFields {
	pruned := make(sql.SortFields, 0, len(fields))
	for _, field := range fields {
		if redundantNotNullSortProbe(ctx, field.Column) {
			continue
		}
		pruned = append(pruned, field)
	}
	return pruned
}

func redundantNotNullSortProbe(ctx *sql.Context, expr sql.Expression) bool {
	if _, ok := expr.(sql.IsNullExpression); !ok {
		return false
	}
	children := expr.Children()
	if len(children) != 1 {
		return false
	}
	return !children[0].IsNullable(ctx)
}
