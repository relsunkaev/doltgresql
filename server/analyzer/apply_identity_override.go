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

	"github.com/dolthub/doltgresql/postgres/parser/parser"
	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/functions/framework"
)

func ApplyIdentityOverride(ctx *sql.Context, _ *gmsanalyzer.Analyzer, n sql.Node, _ *plan.Scope, _ gmsanalyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	insertAST, ok := identityOverrideInsert(ctx.Query())
	if !ok {
		return n, transform.SameTree, nil
	}

	return transform.Node(ctx, n, func(ctx *sql.Context, n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		insert, ok := n.(*plan.InsertInto)
		if !ok {
			return n, transform.SameTree, nil
		}

		columnNames := identityOverrideColumnNames(insertAST, insert.Destination.Schema(ctx))
		rewritten := *insert
		rewritten.ColumnNames = columnNames
		if insertAST.Override == tree.InsertOverrideUser {
			source, changed := replaceIdentityUserValues(ctx, insert.Source, insert.Destination.Schema(ctx), columnNames)
			if changed {
				rewritten.Source = source
				rewritten.LiteralValueSource = false
			}
		}
		return &rewritten, transform.NewTree, nil
	})
}

func identityOverrideInsert(query string) (*tree.Insert, bool) {
	if query == "" {
		return nil, false
	}
	statements, err := parser.Parse(query)
	if err != nil {
		return nil, false
	}
	for _, statement := range statements {
		insert, ok := statement.AST.(*tree.Insert)
		if ok && insert.Override != tree.InsertOverrideNone {
			return insert, true
		}
	}
	return nil, false
}

func identityOverrideColumnNames(insert *tree.Insert, schema sql.Schema) []string {
	if len(insert.Columns) > 0 {
		columnNames := make([]string, len(insert.Columns))
		for i, name := range insert.Columns {
			columnNames[i] = string(name)
		}
		return columnNames
	}

	columnNames := make([]string, 0, len(schema))
	for _, column := range schema {
		if !column.HiddenSystem {
			columnNames = append(columnNames, column.Name)
		}
	}
	return columnNames
}

func replaceIdentityUserValues(ctx *sql.Context, source sql.Node, schema sql.Schema, columnNames []string) (sql.Node, bool) {
	identityDefaults := identityOverrideDefaults(ctx, schema, columnNames)
	if len(identityDefaults) == 0 {
		return source, false
	}

	switch source := source.(type) {
	case *plan.Values:
		tuples := make([][]sql.Expression, len(source.ExpressionTuples))
		for i, tuple := range source.ExpressionTuples {
			tuples[i] = make([]sql.Expression, len(tuple))
			copy(tuples[i], tuple)
			for tupleIdx, defaultExpr := range identityDefaults {
				if tupleIdx < len(tuples[i]) {
					tuples[i][tupleIdx] = expression.WrapExpression(defaultExpr)
				}
			}
		}
		return plan.NewValuesWithAlias(source.AliasName, source.ColumnNames, tuples), true
	default:
		sourceSchema := source.Schema(ctx)
		projections := make([]sql.Expression, len(columnNames))
		for i, columnName := range columnNames {
			if defaultExpr, ok := identityDefaults[i]; ok {
				projections[i] = expression.WrapExpression(defaultExpr)
				continue
			}
			if i >= len(sourceSchema) {
				return source, false
			}
			column := sourceSchema[i]
			projections[i] = expression.NewGetField(i, column.Type, columnName, column.Nullable)
		}
		return plan.NewProject(projections, source), true
	}
}

func identityOverrideDefaults(ctx *sql.Context, schema sql.Schema, columnNames []string) map[int]*sql.ColumnDefaultValue {
	defaults := make(map[int]*sql.ColumnDefaultValue)
	for tupleIdx, columnName := range columnNames {
		columnIdx := schema.IndexOfColName(columnName)
		if columnIdx == -1 {
			continue
		}
		if defaultExpr := identityDefaultExpression(ctx, schema[columnIdx]); defaultExpr != nil {
			defaults[tupleIdx] = defaultExpr
		}
	}
	return defaults
}

func identityDefaultExpression(ctx *sql.Context, column *sql.Column) *sql.ColumnDefaultValue {
	if column.Generated != nil && containsNextval(ctx, column.Generated) {
		return column.Generated
	}
	if column.Default != nil && containsNextval(ctx, column.Default) {
		return column.Default
	}
	return nil
}

func containsNextval(ctx *sql.Context, defaultExpr *sql.ColumnDefaultValue) bool {
	if defaultExpr == nil {
		return false
	}
	seen := false
	transform.InspectExpr(ctx, defaultExpr, func(ctx *sql.Context, expr sql.Expression) bool {
		if fn, ok := expr.(*framework.CompiledFunction); ok && strings.EqualFold(fn.Name, "nextval") {
			seen = true
			return false
		}
		return true
	})
	return seen || strings.Contains(strings.ToLower(defaultExpr.String()), "nextval")
}
