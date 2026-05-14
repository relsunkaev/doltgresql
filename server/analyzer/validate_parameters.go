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
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	pgtransform "github.com/dolthub/doltgresql/server/transform"
)

// ValidateParameters rejects PostgreSQL configuration parameters when they are
// resolved as implicit bare system-variable expressions. They remain registered
// as system variables for SET, SHOW, and current_setting() compatibility.
func ValidateParameters(ctx *sql.Context, _ *gmsanalyzer.Analyzer, node sql.Node, _ *plan.Scope, _ gmsanalyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return pgtransform.NodeExprsWithNodeWithOpaque(ctx, node, func(ctx *sql.Context, n sql.Node, expr sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		sysVar, ok := expr.(*expression.SystemVar)
		if !ok {
			return expr, transform.SameTree, nil
		}
		if _, ok = n.(*plan.Set); ok {
			return expr, transform.SameTree, nil
		}
		if sysVar.SpecifiedScope == "" && strings.EqualFold(sysVar.Name, "default_with_oids") {
			return nil, transform.SameTree, pgerror.Newf(pgcode.UndefinedColumn, `column "%s" does not exist`, sysVar.Name)
		}
		return expr, transform.SameTree, nil
	})
}
