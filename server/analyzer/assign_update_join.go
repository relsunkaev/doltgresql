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
	gmsanalyzer "github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// AssignUpdateJoin replaces GMS's update-join assignment rule. GMS runs this
// after join optimization, so PostgreSQL UPDATE ... FROM plans can already be
// optimized to MergeJoin or LookupJoin instead of the original JoinNode.
func AssignUpdateJoin(ctx *sql.Context, a *gmsanalyzer.Analyzer, node sql.Node, scope *plan.Scope, selector gmsanalyzer.RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	update, ok := node.(*plan.Update)
	if !ok {
		return node, transform.SameTree, nil
	}
	if _, ok = update.Child.(*plan.UpdateJoin); ok {
		return node, transform.SameTree, nil
	}
	updateSource, ok := update.Child.(*plan.UpdateSource)
	if !ok {
		return node, transform.SameTree, nil
	}
	if !hasUpdateJoinSource(ctx, updateSource.Child) {
		return node, transform.SameTree, nil
	}
	updateTargets, err := updateTargetsByTable(ctx, updateSource, updateSource.Child, update.IsJoin)
	if err != nil {
		return nil, transform.SameTree, err
	}
	updateJoin := plan.NewUpdateJoin(updateTargets, updateSource)
	ret, err := update.WithChildren(ctx, updateJoin)
	if err != nil {
		return nil, transform.SameTree, err
	}
	return ret, transform.NewTree, nil
}

func hasUpdateJoinSource(ctx *sql.Context, node sql.Node) bool {
	found := false
	transform.InspectWithOpaque(ctx, node, func(ctx *sql.Context, n sql.Node) bool {
		if found {
			return false
		}
		if _, ok := n.(*plan.SubqueryAlias); ok {
			return false
		}
		if len(n.Children()) > 1 {
			found = true
			return false
		}
		return true
	})
	return found
}

func updateTargetsByTable(ctx *sql.Context, node sql.Node, joinSource sql.Node, isJoin bool) (map[string]sql.Node, error) {
	namesOfTablesToBeUpdated := plan.GetTablesToBeUpdated(ctx, node)
	resolvedTables := resolvedTablesByName(ctx, joinSource)

	updateTargets := make(map[string]sql.Node)
	for tableToBeUpdated := range namesOfTablesToBeUpdated {
		resolvedTable, ok := resolvedTables[strings.ToLower(tableToBeUpdated)]
		if !ok {
			return nil, plan.ErrUpdateForTableNotSupported.New(tableToBeUpdated)
		}
		table := resolvedTable.UnderlyingTable()
		updatable, ok := table.(sql.UpdatableTable)
		if !ok || updatable == nil {
			return nil, plan.ErrUpdateForTableNotSupported.New(tableToBeUpdated)
		}
		if sql.IsKeyless(updatable.Schema(ctx)) && isJoin {
			return nil, sql.ErrUnsupportedFeature.New("error: keyless tables unsupported for UPDATE JOIN")
		}
		updateTargets[strings.ToLower(tableToBeUpdated)] = resolvedTable
	}

	return updateTargets, nil
}

func resolvedTablesByName(ctx *sql.Context, node sql.Node) map[string]*plan.ResolvedTable {
	ret := make(map[string]*plan.ResolvedTable)
	transform.Inspect(node, func(n sql.Node) bool {
		switch n := n.(type) {
		case *plan.ResolvedTable:
			ret[strings.ToLower(n.Table.Name())] = n
		case *plan.IndexedTableAccess:
			if rt, ok := n.TableNode.(*plan.ResolvedTable); ok {
				ret[strings.ToLower(rt.Name())] = rt
			}
		case *plan.TableAlias:
			if rt := firstResolvedTable(ctx, n); rt != nil {
				ret[strings.ToLower(n.Name())] = rt
			}
		}
		return true
	})
	return ret
}

func firstResolvedTable(ctx *sql.Context, node sql.Node) *plan.ResolvedTable {
	var table *plan.ResolvedTable
	transform.InspectWithOpaque(ctx, node, func(ctx *sql.Context, n sql.Node) bool {
		if table != nil {
			return false
		}
		switch n := n.(type) {
		case *plan.SubqueryAlias:
			return false
		case *plan.ResolvedTable:
			if !plan.IsDualTable(n) {
				table = n
				return false
			}
		case *plan.IndexedTableAccess:
			if rt, ok := n.TableNode.(*plan.ResolvedTable); ok {
				table = rt
				return false
			}
		}
		return true
	})
	return table
}
