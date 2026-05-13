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
	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	gmsanalyzer "github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// AssignFetchWithTies lowers the internal marker emitted for PostgreSQL
// FETCH FIRST ... WITH TIES into a Doltgres execution node. The parser-to-Vitess
// bridge uses Limit.CalcFoundRows as a private marker because the Vitess LIMIT
// AST has no WITH TIES bit.
func AssignFetchWithTies(ctx *sql.Context, _ *gmsanalyzer.Analyzer, node sql.Node, _ *plan.Scope, _ gmsanalyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(ctx, node, func(ctx *sql.Context, n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		limit, ok := n.(*plan.Limit)
		if !ok || !limit.CalcFoundRows {
			return n, transform.SameTree, nil
		}

		child := limit.Child
		var offsetExpr sql.Expression
		if offset, ok := child.(*plan.Offset); ok {
			offsetExpr = offset.Offset
			child = offset.Child
		}

		var project *plan.Project
		if p, ok := child.(*plan.Project); ok {
			project = p
			child = p.Child
		}

		sort, ok := child.(*plan.Sort)
		if !ok || len(sort.SortFields) == 0 {
			return nil, transform.SameTree, errors.Errorf("WITH TIES cannot be specified without ORDER BY")
		}

		withTies := pgnodes.NewFetchWithTies(limit.Limit, offsetExpr, sort.SortFields, sort)
		if project == nil {
			return withTies, transform.NewTree, nil
		}
		newProject, err := project.WithChildren(ctx, withTies)
		if err != nil {
			return nil, transform.SameTree, err
		}
		return newProject, transform.NewTree, nil
	})
}
