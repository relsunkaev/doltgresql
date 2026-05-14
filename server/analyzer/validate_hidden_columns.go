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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	gmsanalyzer "github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

func skipInsertDestinationHiddenSystemValidation(original gmsanalyzer.RuleFunc) gmsanalyzer.RuleFunc {
	return func(
		ctx *sql.Context,
		a *gmsanalyzer.Analyzer,
		node sql.Node,
		scope *plan.Scope,
		selector gmsanalyzer.RuleSelector,
		qFlags *sql.QueryFlags,
	) (sql.Node, transform.TreeIdentity, error) {
		return validateNoHiddenSystemColumnsExceptInsertDestination(ctx, a, node, scope, selector, qFlags)
	}
}

func validateNoHiddenSystemColumnsExceptInsertDestination(ctx *sql.Context, _ *gmsanalyzer.Analyzer, n sql.Node, _ *plan.Scope, _ gmsanalyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	var err error
	transform.Inspect(n, func(n sql.Node) bool {
		switch nn := n.(type) {
		case *plan.CreateTable:
			for _, col := range nn.TargetSchema() {
				if sql.IsHiddenSystemColumn(col.Name) {
					err = fmt.Errorf("invalid column name: %s", col.Name)
				}
			}
		case *plan.ModifyColumn:
			if sql.IsHiddenSystemColumn(nn.Column()) {
				err = sql.ErrColumnNotFound.New(nn.Column())
			}
		case *plan.DropColumn:
			if sql.IsHiddenSystemColumn(nn.Column) {
				err = sql.ErrColumnNotFound.New(nn.Column)
			}
		case *plan.RenameColumn:
			if sql.IsHiddenSystemColumn(nn.ColumnName) {
				err = sql.ErrColumnNotFound.New(nn.ColumnName)
			}
			if sql.IsHiddenSystemColumn(nn.NewColumnName) {
				err = fmt.Errorf("invalid column name: %s", nn.NewColumnName)
			}
		}

		switch n.(type) {
		case *plan.InsertInto, *plan.InsertDestination, *plan.Update, *plan.UpdateSource:
			// Insert plans can include internal generated/default projections
			// for hidden expression-index backing columns, including ON
			// DUPLICATE KEY UPDATE expressions synthesized by GMS for those
			// columns. User expressions are still resolved against visible
			// scopes before this validator runs.
		default:
			transform.InspectExpressions(ctx, n, func(ctx *sql.Context, e sql.Expression) bool {
				if gf, ok := e.(*expression.GetField); ok {
					if sql.IsHiddenSystemColumn(gf.Name()) {
						err = sql.ErrColumnNotFound.New(gf.Name())
					}
				}
				return true
			})
		}

		return err == nil
	})
	if err != nil {
		return nil, transform.SameTree, err
	}
	return n, transform.SameTree, nil
}
