// Copyright 2020-2021 Dolthub, Inc.
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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	pgexprs "github.com/dolthub/doltgresql/server/expression"
	pgnode "github.com/dolthub/doltgresql/server/node"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// ValidateColumnDefaults ensures that newly created column defaults from a DDL statement are legal for the type of
// column, various other business logic checks to match MySQL's logic.
func ValidateColumnDefaults(ctx *sql.Context, _ *analyzer.Analyzer, n sql.Node, _ *plan.Scope, _ analyzer.RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("validateColumnDefaults")
	defer span.End()

	return transform.Node(ctx, n, func(ctx *sql.Context, n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch node := n.(type) {
		case *plan.AlterDefaultSet:
			table := getResolvedTable(node)
			sch := table.Schema(ctx)
			index := sch.IndexOfColName(node.ColumnName)
			if index == -1 {
				return nil, transform.SameTree, sql.ErrColumnNotFound.New(node.ColumnName)
			}
			col := sch[index]
			allowColumnReferences, err := allowColumnReferencesForSetDefaultForeignKey(ctx, table, node.ColumnName)
			if err != nil {
				return node, transform.SameTree, err
			}
			err = validateColumnDefault(ctx, col, node.Default, allowColumnReferences)
			if err != nil {
				return node, transform.SameTree, err
			}

			return node, transform.SameTree, nil

		case sql.SchemaTarget:
			switch node.(type) {
			case *plan.AlterPK, *plan.AddColumn, *plan.ModifyColumn, *plan.AlterDefaultDrop, *plan.CreateTable, *plan.DropColumn, *pgnode.CreateTable:
				// DDL nodes must validate any new column defaults, continue to logic below
			default:
				// other node types are not altering the schema and therefore don't need validation of column defaults
				return n, transform.SameTree, nil
			}

			// There may be multiple DDL nodes in the plan (ALTER TABLE statements can have many clauses), and for each of them
			// we need to count the column indexes in the very hacky way outlined above.
			i := 0
			return transform.NodeExprs(ctx, n, func(ctx *sql.Context, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				eWrapper, ok := e.(*expression.Wrapper)
				if !ok {
					return e, transform.SameTree, nil
				}

				defer func() {
					i++
				}()

				eVal := eWrapper.Unwrap()
				if eVal == nil {
					return e, transform.SameTree, nil
				}
				colDefault, ok := eVal.(*sql.ColumnDefaultValue)
				if !ok {
					return e, transform.SameTree, nil
				}

				col, err := lookupColumnForTargetSchema(ctx, node, i)
				if err != nil {
					return nil, transform.SameTree, err
				}

				isGeneratedDefault := isGeneratedColumnDefault(col, colDefault)
				sanitizedDefault, sameDefault, err := sanitizeColumnDefaultExpressionAliases(ctx, colDefault)
				if err != nil {
					return nil, transform.SameTree, err
				}
				colDefault = sanitizedDefault

				if isGeneratedDefault {
					err = validateColumnDefault(ctx, col, colDefault, true)
					if err != nil {
						return nil, transform.SameTree, err
					}
					err = validateGeneratedColumnDefault(ctx, col, colDefault, node.TargetSchema())
					if err != nil {
						return nil, transform.SameTree, err
					}
				} else {
					err = validateColumnDefault(ctx, col, colDefault, false)
					if err != nil {
						return nil, transform.SameTree, err
					}
				}

				if isGeneratedDefault && sameDefault == transform.NewTree {
					replaceGeneratedColumnDefault(node, col, colDefault)
				}

				if sameDefault == transform.SameTree {
					return e, transform.SameTree, nil
				}
				return expression.WrapExpression(colDefault), transform.NewTree, nil
			})
		default:
			return node, transform.SameTree, nil
		}
	})
}

func sanitizeColumnDefaultExpressionAliases(ctx *sql.Context, colDefault *sql.ColumnDefaultValue) (*sql.ColumnDefaultValue, transform.TreeIdentity, error) {
	if colDefault == nil || colDefault.Expr == nil {
		return colDefault, transform.SameTree, nil
	}
	cleanExpr, same, err := transform.Expr(ctx, colDefault.Expr, func(ctx *sql.Context, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		if alias, ok := e.(*expression.Alias); ok {
			return alias.Child, transform.NewTree, nil
		}
		return e, transform.SameTree, nil
	})
	if err != nil || same {
		return colDefault, same, err
	}
	cleanDefault, err := sql.NewColumnDefaultValue(cleanExpr, colDefault.OutType, colDefault.Literal, colDefault.Parenthesized, colDefault.ReturnNil)
	if err != nil {
		return nil, transform.SameTree, err
	}
	return cleanDefault, transform.NewTree, nil
}

func allowColumnReferencesForSetDefaultForeignKey(ctx *sql.Context, table *plan.ResolvedTable, columnName string) (bool, error) {
	if table == nil {
		return false, nil
	}
	fkTable, ok := table.UnderlyingTable().(sql.ForeignKeyTable)
	if !ok {
		return false, nil
	}
	foreignKeys, err := fkTable.GetDeclaredForeignKeys(ctx)
	if err != nil {
		return false, err
	}
	for _, foreignKey := range foreignKeys {
		if foreignKey.OnUpdate != sql.ForeignKeyReferentialAction_SetDefault &&
			foreignKey.OnDelete != sql.ForeignKeyReferentialAction_SetDefault {
			continue
		}
		for _, fkColumn := range foreignKey.Columns {
			if strings.EqualFold(fkColumn, columnName) {
				return true, nil
			}
		}
	}
	return false, nil
}

func replaceGeneratedColumnDefault(node sql.SchemaTarget, col *sql.Column, colDefault *sql.ColumnDefaultValue) {
	if col != nil && col.Generated != nil {
		col.Generated = colDefault
	}
	if modifyColumn, ok := node.(*plan.ModifyColumn); ok {
		newColumn := modifyColumn.NewColumn()
		if newColumn != nil && newColumn.Generated != nil && col != nil && strings.EqualFold(newColumn.Name, col.Name) {
			newColumn.Generated = colDefault
		}
	}
}

// lookupColumnForTargetSchema looks at the target schema for the specified SchemaTarget node and returns
// the column based on the specified index. For most node types, this is simply indexing into the target
// schema but a few types require special handling.
func lookupColumnForTargetSchema(_ *sql.Context, node sql.SchemaTarget, colIndex int) (*sql.Column, error) {
	schema := node.TargetSchema()

	switch n := node.(type) {
	case *plan.ModifyColumn:
		if colIndex < len(schema) {
			return schema[colIndex], nil
		} else {
			return n.NewColumn(), nil
		}
	case *plan.AddColumn:
		if colIndex < len(schema) {
			return schema[colIndex], nil
		} else {
			return n.Column(), nil
		}
	case *plan.AlterDefaultSet:
		index := schema.IndexOfColName(n.ColumnName)
		if index == -1 {
			return nil, sql.ErrTableColumnNotFound.New(n.Table, n.ColumnName)
		}
		return schema[index], nil
	default:
		if colIndex < len(schema) {
			return schema[colIndex], nil
		} else {
			// TODO: sql.ErrColumnNotFound would be a better error here, but we need to add all the different node types to
			//  the switch to get it
			return nil, expression.ErrIndexOutOfBounds.New(colIndex, len(schema))
		}
	}
}

// validateColumnDefault validates that the column default expression is valid for the column type and returns an error
// if not
func validateColumnDefault(ctx *sql.Context, col *sql.Column, colDefault *sql.ColumnDefaultValue, allowColumnReferences bool) error {
	if colDefault == nil {
		return nil
	}
	if !allowColumnReferences {
		if err := validateColumnDefaultExpressionText(colDefault.Expr.String()); err != nil {
			return err
		}
	}

	var err error
	sql.Inspect(ctx, colDefault.Expr, func(ctx *sql.Context, e sql.Expression) bool {
		switch e.(type) {
		case sql.FunctionExpression, *expression.UnresolvedFunction:
			// TODO: functions must be deterministic to be used in column defaults
			return true
		case *plan.Subquery:
			err = sql.ErrColumnDefaultSubquery.New(col.Name)
			return false
		}
		if !allowColumnReferences {
			if _, ok := e.(sql.Aggregation); ok {
				err = defaultExpressionAggregateError()
				return false
			}
			if _, ok := e.(sql.WindowAggregation); ok {
				err = defaultExpressionWindowError()
				return false
			}
			if expr, ok := e.(sql.WindowAdaptableExpression); ok {
				if expr.Window() != nil {
					err = defaultExpressionWindowError()
					return false
				}
			}
			if expr, ok := e.(sql.RowIterExpression); ok {
				if expr.ReturnsRowIter() {
					err = defaultExpressionSetReturningError()
					return false
				}
			}
		}
		switch e.(type) {
		case *expression.GetField:
			if !allowColumnReferences {
				err = defaultExpressionColumnReferenceError()
				return false
			}
			if !colDefault.IsParenthesized() {
				err = sql.ErrInvalidColumnDefaultValue.New(col.Name)
				return false
			}
			return true
		default:
			return true
		}
	})

	if err != nil {
		return err
	}

	if !allowColumnReferences {
		if err = validateDefaultRegclassCasts(ctx, colDefault.Expr); err != nil {
			return err
		}
	}

	// validate type of default expression
	if err = colDefault.CheckType(ctx); err != nil {
		return err
	}

	return nil
}

func validateDefaultRegclassCasts(ctx *sql.Context, expr sql.Expression) error {
	var err error
	sql.Inspect(ctx, expr, func(ctx *sql.Context, e sql.Expression) bool {
		cast, ok := e.(*pgexprs.ExplicitCast)
		if !ok {
			return true
		}
		castType, ok := cast.Type(ctx).(*pgtypes.DoltgresType)
		if !ok || castType.ID != pgtypes.Regclass.ID || !defaultRegclassCastHasLiteralInput(cast) {
			return true
		}
		_, err = cast.Eval(ctx, nil)
		return err == nil
	})
	return err
}

func defaultRegclassCastHasLiteralInput(cast *pgexprs.ExplicitCast) bool {
	_, ok := cast.Child().(*expression.Literal)
	return ok
}

func validateColumnDefaultExpressionText(expr string) error {
	lower := strings.ToLower(expr)
	if strings.Contains(lower, " over (") {
		return defaultExpressionWindowError()
	}
	for _, name := range generatedColumnAggregateFunctions {
		if containsFunctionCall(lower, name) {
			return defaultExpressionAggregateError()
		}
	}
	for _, name := range generatedColumnWindowFunctions {
		if containsFunctionCall(lower, name) {
			return defaultExpressionWindowError()
		}
	}
	for _, name := range generatedColumnSetReturningFunctions {
		if containsFunctionCall(lower, name) {
			return defaultExpressionSetReturningError()
		}
	}
	return nil
}

func defaultExpressionColumnReferenceError() error {
	return pgerror.New(pgcode.FeatureNotSupported, "cannot use column reference in DEFAULT expression")
}

func defaultExpressionAggregateError() error {
	return pgerror.New(pgcode.Grouping, "aggregate functions are not allowed in DEFAULT expressions")
}

func defaultExpressionWindowError() error {
	return pgerror.New(pgcode.Windowing, "window functions are not allowed in DEFAULT expressions")
}

func defaultExpressionSetReturningError() error {
	return pgerror.New(pgcode.FeatureNotSupported, "set-returning functions are not allowed in DEFAULT expressions")
}

func isGeneratedColumnDefault(col *sql.Column, colDefault *sql.ColumnDefaultValue) bool {
	if col == nil || col.AutoIncrement || col.Generated == nil || col.Generated != colDefault {
		return false
	}
	if doltgresType, ok := col.Type.(*pgtypes.DoltgresType); ok && doltgresType.IsSerial {
		return false
	}
	return true
}

func validateGeneratedColumnDefault(ctx *sql.Context, col *sql.Column, colDefault *sql.ColumnDefaultValue, schema sql.Schema) error {
	if colDefault == nil {
		return nil
	}
	for _, schemaCol := range schema {
		if schemaCol.Generated == nil {
			continue
		}
		if plan.ColumnReferencedInDefaultValueExpression(ctx, colDefault, schemaCol.Name) {
			return fmt.Errorf("cannot use generated column %q in column generation expression", schemaCol.Name)
		}
	}

	var err error
	sql.Inspect(ctx, colDefault.Expr, func(ctx *sql.Context, e sql.Expression) bool {
		if e != nil {
			if textErr := validateGeneratedColumnExpressionText(e.String()); textErr != nil {
				err = textErr
				return false
			}
		}
		if _, ok := e.(sql.Aggregation); ok {
			err = fmt.Errorf("aggregate functions are not allowed in column generation expressions")
			return false
		}
		if _, ok := e.(sql.WindowAggregation); ok {
			err = fmt.Errorf("window functions are not allowed in column generation expressions")
			return false
		}
		if expr, ok := e.(sql.WindowAdaptableExpression); ok {
			if expr.Window() != nil {
				err = fmt.Errorf("window functions are not allowed in column generation expressions")
				return false
			}
		}
		if expr, ok := e.(sql.RowIterExpression); ok {
			if expr.ReturnsRowIter() {
				err = fmt.Errorf("set-returning functions are not allowed in column generation expressions")
				return false
			}
		}
		if expr, ok := e.(sql.NonDeterministicExpression); ok {
			if expr.IsNonDeterministic() {
				err = fmt.Errorf("generation expression is not immutable")
				return false
			}
		}
		if expr, ok := e.(sql.FunctionExpression); ok {
			if functionErr := validateGeneratedColumnFunctionName(expr.FunctionName()); functionErr != nil {
				err = functionErr
				return false
			}
		}
		if expr, ok := e.(*expression.GetField); ok {
			if strings.EqualFold(expr.Name(), col.Name) {
				err = fmt.Errorf("cannot use generated column %q in column generation expression", col.Name)
				return false
			}
		}
		return true
	})
	return err
}

func validateGeneratedColumnExpressionText(expr string) error {
	lower := strings.ToLower(expr)
	if strings.Contains(lower, " over (") {
		return fmt.Errorf("window functions are not allowed in column generation expressions")
	}
	for _, name := range generatedColumnVolatileFunctions {
		if containsFunctionCall(lower, name) {
			return fmt.Errorf("generation expression is not immutable")
		}
	}
	for _, name := range generatedColumnAggregateFunctions {
		if containsFunctionCall(lower, name) {
			return fmt.Errorf("aggregate functions are not allowed in column generation expressions")
		}
	}
	for _, name := range generatedColumnWindowFunctions {
		if containsFunctionCall(lower, name) {
			return fmt.Errorf("window functions are not allowed in column generation expressions")
		}
	}
	for _, name := range generatedColumnSetReturningFunctions {
		if containsFunctionCall(lower, name) {
			return fmt.Errorf("set-returning functions are not allowed in column generation expressions")
		}
	}
	return nil
}

func validateGeneratedColumnFunctionName(name string) error {
	lower := strings.ToLower(name)
	if stringInList(lower, generatedColumnVolatileFunctions) {
		return fmt.Errorf("generation expression is not immutable")
	}
	if stringInList(lower, generatedColumnAggregateFunctions) {
		return fmt.Errorf("aggregate functions are not allowed in column generation expressions")
	}
	if stringInList(lower, generatedColumnWindowFunctions) {
		return fmt.Errorf("window functions are not allowed in column generation expressions")
	}
	if stringInList(lower, generatedColumnSetReturningFunctions) {
		return fmt.Errorf("set-returning functions are not allowed in column generation expressions")
	}
	return nil
}

func stringInList(value string, list []string) bool {
	for _, candidate := range list {
		if value == candidate {
			return true
		}
	}
	return false
}

func containsFunctionCall(expr string, name string) bool {
	needle := name + "("
	start := 0
	for {
		idx := strings.Index(expr[start:], needle)
		if idx == -1 {
			return false
		}
		idx += start
		if idx == 0 || isFunctionNameBoundary(expr[idx-1]) {
			return true
		}
		start = idx + len(needle)
	}
}

func isFunctionNameBoundary(ch byte) bool {
	return !((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_')
}

var generatedColumnVolatileFunctions = []string{
	"random",
	"rand",
}

var generatedColumnAggregateFunctions = []string{
	"avg",
	"bit_and",
	"bit_or",
	"bit_xor",
	"bool_and",
	"bool_or",
	"count",
	"every",
	"json_agg",
	"json_object_agg",
	"max",
	"min",
	"sum",
}

var generatedColumnWindowFunctions = []string{
	"cume_dist",
	"dense_rank",
	"first_value",
	"lag",
	"last_value",
	"lead",
	"ntile",
	"percent_rank",
	"rank",
	"row_number",
}

var generatedColumnSetReturningFunctions = []string{
	"generate_series",
	"json_array_elements",
	"json_array_elements_text",
	"jsonb_array_elements",
	"jsonb_array_elements_text",
	"regexp_matches",
	"regexp_split_to_table",
	"string_to_table",
}

// Finds first ResolvedTable node that is a descendant of the node given
// This function will not look inside SubqueryAliases
func getResolvedTable(node sql.Node) *plan.ResolvedTable {
	var table *plan.ResolvedTable
	transform.Inspect(node, func(n sql.Node) bool {
		// Inspect is called on all children of a node even if an earlier child's call returns false.
		// We only want the first TableNode match.
		if table != nil {
			return false
		}
		switch nn := n.(type) {
		case *plan.SubqueryAlias:
			// We should not be matching with ResolvedTables inside SubqueryAliases
			return false
		case *plan.ResolvedTable:
			if !plan.IsDualTable(nn) {
				table = nn
				return false
			}
		case *plan.IndexedTableAccess:
			if rt, ok := nn.TableNode.(*plan.ResolvedTable); ok {
				table = rt
				return false
			}
		}
		return true
	})
	return table
}
