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

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	pgtransform "github.com/dolthub/doltgresql/server/transform"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// ValidateOrderBy rejects ORDER BY expressions for PostgreSQL types that do not
// define an ordering operator.
func ValidateOrderBy(ctx *sql.Context, _ *gmsanalyzer.Analyzer, node sql.Node, _ *plan.Scope, _ gmsanalyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return pgtransform.NodeWithOpaque(ctx, node, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := node.(type) {
		case *plan.Sort:
			return node, transform.SameTree, validateOrderBySortFields(ctx, n.SortFields)
		case *plan.TopN:
			return node, transform.SameTree, validateOrderBySortFields(ctx, n.Fields)
		default:
			return node, transform.SameTree, nil
		}
	})
}

func validateOrderBySortFields(ctx *sql.Context, fields sql.SortFields) error {
	for _, field := range fields {
		typ, ok := field.Column.Type(ctx).(*pgtypes.DoltgresType)
		if !ok {
			continue
		}
		if typ.ID == pgtypes.Xid.ID || typ.ID == pgtypes.Xid8.ID {
			return pgerror.Newf(pgcode.UndefinedFunction, "could not identify an ordering operator for type %s", typ.Name())
		}
	}
	return nil
}
