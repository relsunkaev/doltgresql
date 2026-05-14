// Copyright 2023 Dolthub, Inc.
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

package ast

import (
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"

	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/auth"
	pgexprs "github.com/dolthub/doltgresql/server/expression"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// nodeAliasedTableExpr handles *tree.AliasedTableExpr nodes.
func nodeAliasedTableExpr(ctx *Context, node *tree.AliasedTableExpr) (*vitess.AliasedTableExpr, error) {
	if node.Ordinality {
		return nodeAliasedTableExprWithOrdinality(ctx, node)
	}
	if node.IndexFlags != nil {
		return nil, errors.Errorf("index flags are not yet supported")
	}
	var aliasExpr vitess.SimpleTableExpr
	var authInfo vitess.AuthInformation

	switch expr := node.Expr.(type) {
	case *tree.TableName:
		tableName, err := nodeTableName(ctx, expr)
		if err != nil {
			return nil, err
		}
		aliasExpr = tableName
		authInfo = vitess.AuthInformation{
			AuthType:    ctx.Auth().PeekAuthType(),
			TargetType:  auth.AuthTargetType_TableIdentifiers,
			TargetNames: []string{tableName.DbQualifier.String(), tableName.SchemaQualifier.String(), tableName.Name.String()},
		}
	case *tree.Subquery:
		tableExpr, err := nodeTableExpr(ctx, expr)
		if err != nil {
			return nil, err
		}

		ate, ok := tableExpr.(*vitess.AliasedTableExpr)
		if !ok {
			return nil, errors.Errorf("expected *vitess.AliasedTableExpr, found %T", tableExpr)
		}

		var selectStmt vitess.SelectStatement
		switch ate.Expr.(type) {
		case *vitess.Subquery:
			selectStmt = ate.Expr.(*vitess.Subquery).Select
		default:
			return nil, errors.Errorf("unhandled subquery table expression: `%T`", tableExpr)
		}

		// If the subquery is a VALUES statement, it should be represented more directly
		innerSelect := selectStmt
		if parentSelect, ok := innerSelect.(*vitess.ParenSelect); ok {
			innerSelect = parentSelect.Select
		}
		if inSelect, ok := innerSelect.(*vitess.Select); ok {
			if isTrivialSelectStar(inSelect) {
				if aliasedTblExpr, ok := inSelect.From[0].(*vitess.AliasedTableExpr); ok {
					if valuesStmt, ok := aliasedTblExpr.Expr.(*vitess.ValuesStatement); ok {
						if len(node.As.Cols) > 0 {
							columns := make([]vitess.ColIdent, len(node.As.Cols))
							for i := range node.As.Cols {
								columns[i] = vitess.NewColIdent(string(node.As.Cols[i]))
							}
							valuesStmt.Columns = columns
						}
						aliasExpr = valuesStmt
						break
					}
				}
			}
		}

		subquery := &vitess.Subquery{
			Select: selectStmt,
		}

		if len(node.As.Cols) > 0 {
			columns := make([]vitess.ColIdent, len(node.As.Cols))
			for i := range node.As.Cols {
				columns[i] = vitess.NewColIdent(string(node.As.Cols[i]))
			}
			subquery.Columns = columns
		}
		aliasExpr = subquery
	case *tree.RowsFromExpr:
		if len(node.As.ColDefs) > 0 {
			aliasedExpr, ok, err := nodeJsonToRecordAliasedTableExpr(ctx, node, expr)
			if err != nil || ok {
				return aliasedExpr, err
			}
		}
		aliasedExpr, ok, err := nodeRowsFromAlias(ctx, node, expr)
		if err != nil || ok {
			return aliasedExpr, err
		}
		aliasedExpr, ok, err = nodeMultiColumnUnnestAlias(ctx, node, expr)
		if err != nil || ok {
			return aliasedExpr, err
		}
		aliasedExpr, ok, err = nodeFixedColumnTableFunctionAlias(ctx, node, expr)
		if err != nil || ok {
			return aliasedExpr, err
		}
		aliasedExpr, ok, err = nodeSingleColumnTableFunctionAlias(ctx, node, expr)
		if err != nil || ok {
			return aliasedExpr, err
		}

		tableExpr, err := nodeTableExpr(ctx, expr)
		if err != nil {
			return nil, err
		}

		// TODO: this should be represented as a table function more directly
		subquery := &vitess.Subquery{
			Select: &vitess.Select{
				From: vitess.TableExprs{tableExpr},
			},
		}

		if len(node.As.Cols) > 0 {
			columns := make([]vitess.ColIdent, len(node.As.Cols))
			for i := range node.As.Cols {
				columns[i] = vitess.NewColIdent(string(node.As.Cols[i]))
			}
			subquery.Columns = columns
		}
		aliasExpr = subquery
	case *tree.JSONTableExpr:
		return nodeJSONTableAliasedTableExpr(ctx, node, expr)
	default:
		return nil, errors.Errorf("unhandled table expression: `%T`", expr)
	}
	alias := string(node.As.Alias)

	var asOf *vitess.AsOf
	if node.AsOf != nil {
		asOfExpr, err := nodeExpr(ctx, node.AsOf.Expr)
		if err != nil {
			return nil, err
		}
		// TODO: other forms of AS OF (not just point in time)
		asOf = &vitess.AsOf{
			Time: asOfExpr,
		}
	}

	return &vitess.AliasedTableExpr{
		Expr:    aliasExpr,
		As:      vitess.NewTableIdent(alias),
		AsOf:    asOf,
		Lateral: node.Lateral,
		Auth:    authInfo,
	}, nil
}

func nodeSingleColumnTableFunctionAlias(ctx *Context, node *tree.AliasedTableExpr, rowsFromExpr *tree.RowsFromExpr) (*vitess.AliasedTableExpr, bool, error) {
	if len(node.As.Cols) > 1 || len(rowsFromExpr.Items) != 1 {
		return nil, false, nil
	}
	funcExpr, ok := rowsFromExpr.Items[0].(*tree.FuncExpr)
	if !ok {
		return nil, false, nil
	}
	funcName := singleColumnTableFunctionName(funcExpr)
	switch funcName {
	case "generate_series", "regexp_split_to_table", "skeys", "string_to_table", "svals", "unnest":
	default:
		return nil, false, nil
	}
	if len(node.As.Cols) == 0 && node.As.Alias == "" {
		return nil, false, nil
	}

	args, err := nodeExprs(ctx, funcExpr.Exprs)
	if err != nil {
		return nil, true, err
	}
	tableFuncArgs := make(vitess.SelectExprs, len(args))
	for i, arg := range args {
		tableFuncArgs[i] = &vitess.AliasedExpr{Expr: arg}
	}

	internalAlias := "__doltgres_" + strings.ReplaceAll(funcName, ".", "_")
	colName := string(node.As.Alias)
	if len(node.As.Cols) == 1 {
		colName = string(node.As.Cols[0])
	}
	return &vitess.AliasedTableExpr{
		Expr: &vitess.Subquery{
			Select: &vitess.Select{
				SelectExprs: vitess.SelectExprs{
					&vitess.AliasedExpr{
						Expr: tableFuncColumn(internalAlias, funcName),
						As:   vitess.NewColIdent(colName),
					},
				},
				From: vitess.TableExprs{
					&vitess.TableFuncExpr{
						Name:  funcName,
						Exprs: tableFuncArgs,
						Alias: vitess.NewTableIdent(internalAlias),
					},
				},
			},
		},
		As:      vitess.NewTableIdent(string(node.As.Alias)),
		Lateral: true,
	}, true, nil
}

func nodeRowsFromAlias(ctx *Context, node *tree.AliasedTableExpr, rowsFromExpr *tree.RowsFromExpr) (*vitess.AliasedTableExpr, bool, error) {
	if len(rowsFromExpr.Items) <= 1 {
		return nil, false, nil
	}
	if len(node.As.Cols) > len(rowsFromExpr.Items) {
		return nil, true, errors.Errorf("ROWS FROM only has %d columns", len(rowsFromExpr.Items))
	}

	tableFuncArgs := make(vitess.SelectExprs, 0, len(rowsFromExpr.Items)*4)
	selectExprs := make(vitess.SelectExprs, len(rowsFromExpr.Items))
	internalAlias := "__doltgres_rows_from"
	for i, item := range rowsFromExpr.Items {
		funcExpr, ok := item.(*tree.FuncExpr)
		if !ok {
			return nil, true, errors.Errorf("ROWS FROM only supports table functions")
		}
		funcName := singleColumnTableFunctionName(funcExpr)
		switch funcName {
		case "generate_series", "unnest":
		default:
			return nil, true, errors.Errorf("ROWS FROM does not yet support function %s", funcName)
		}

		args, err := nodeExprs(ctx, funcExpr.Exprs)
		if err != nil {
			return nil, true, err
		}
		tableFuncArgs = append(tableFuncArgs, tableFuncTextArg(funcName), tableFuncTextArg(strconv.Itoa(len(args))))
		for _, arg := range args {
			tableFuncArgs = append(tableFuncArgs, &vitess.AliasedExpr{Expr: arg})
		}

		sourceName := "value_" + strconv.Itoa(i+1)
		colName := funcName
		if i < len(node.As.Cols) {
			colName = string(node.As.Cols[i])
		}
		selectExprs[i] = &vitess.AliasedExpr{
			Expr: tableFuncColumn(internalAlias, sourceName),
			As:   vitess.NewColIdent(colName),
		}
	}

	return &vitess.AliasedTableExpr{
		Expr: &vitess.Subquery{
			Select: &vitess.Select{
				SelectExprs: selectExprs,
				From: vitess.TableExprs{
					&vitess.TableFuncExpr{
						Name:  "doltgres_rows_from",
						Exprs: tableFuncArgs,
						Alias: vitess.NewTableIdent(internalAlias),
					},
				},
			},
		},
		As:      vitess.NewTableIdent(string(node.As.Alias)),
		Lateral: true,
	}, true, nil
}

func nodeMultiColumnUnnestAlias(ctx *Context, node *tree.AliasedTableExpr, rowsFromExpr *tree.RowsFromExpr) (*vitess.AliasedTableExpr, bool, error) {
	if len(rowsFromExpr.Items) != 1 {
		return nil, false, nil
	}
	funcExpr, ok := rowsFromExpr.Items[0].(*tree.FuncExpr)
	if !ok || singleColumnTableFunctionName(funcExpr) != "unnest" || len(funcExpr.Exprs) <= 1 {
		return nil, false, nil
	}
	if len(node.As.Cols) > len(funcExpr.Exprs) {
		return nil, true, errors.Errorf("table function unnest only has %d columns", len(funcExpr.Exprs))
	}

	args, err := nodeExprs(ctx, funcExpr.Exprs)
	if err != nil {
		return nil, true, err
	}
	tableFuncArgs := make(vitess.SelectExprs, len(args))
	for i, arg := range args {
		tableFuncArgs[i] = &vitess.AliasedExpr{Expr: arg}
	}

	internalAlias := "__doltgres_unnest"
	selectExprs := make(vitess.SelectExprs, len(args))
	for i := range args {
		sourceName := "unnest_" + strconv.Itoa(i+1)
		colName := sourceName
		if i < len(node.As.Cols) {
			colName = string(node.As.Cols[i])
		}
		selectExprs[i] = &vitess.AliasedExpr{
			Expr: tableFuncColumn(internalAlias, sourceName),
			As:   vitess.NewColIdent(colName),
		}
	}

	return &vitess.AliasedTableExpr{
		Expr: &vitess.Subquery{
			Select: &vitess.Select{
				SelectExprs: selectExprs,
				From: vitess.TableExprs{
					&vitess.TableFuncExpr{
						Name:  "unnest",
						Exprs: tableFuncArgs,
						Alias: vitess.NewTableIdent(internalAlias),
					},
				},
			},
		},
		As:      vitess.NewTableIdent(string(node.As.Alias)),
		Lateral: true,
	}, true, nil
}

func nodeFixedColumnTableFunctionAlias(ctx *Context, node *tree.AliasedTableExpr, rowsFromExpr *tree.RowsFromExpr) (*vitess.AliasedTableExpr, bool, error) {
	if len(node.As.Cols) == 0 || len(rowsFromExpr.Items) != 1 {
		return nil, false, nil
	}
	funcExpr, ok := rowsFromExpr.Items[0].(*tree.FuncExpr)
	if !ok {
		return nil, false, nil
	}
	funcName := singleColumnTableFunctionName(funcExpr)
	sourceColumns, ok := fixedColumnTableFunctionColumns(funcName)
	if !ok {
		return nil, false, nil
	}
	if len(node.As.Cols) > len(sourceColumns) {
		return nil, true, errors.Errorf("table function %s only has %d columns", funcName, len(sourceColumns))
	}

	args, err := nodeExprs(ctx, funcExpr.Exprs)
	if err != nil {
		return nil, true, err
	}
	tableFuncArgs := make(vitess.SelectExprs, len(args))
	for i, arg := range args {
		tableFuncArgs[i] = &vitess.AliasedExpr{Expr: arg}
	}

	internalAlias := "__doltgres_" + strings.ReplaceAll(funcName, ".", "_")
	selectExprs := make(vitess.SelectExprs, len(sourceColumns))
	for i, sourceColumn := range sourceColumns {
		colName := sourceColumn
		if i < len(node.As.Cols) {
			colName = string(node.As.Cols[i])
		}
		selectExprs[i] = &vitess.AliasedExpr{
			Expr: tableFuncColumn(internalAlias, sourceColumn),
			As:   vitess.NewColIdent(colName),
		}
	}

	return &vitess.AliasedTableExpr{
		Expr: &vitess.Subquery{
			Select: &vitess.Select{
				SelectExprs: selectExprs,
				From: vitess.TableExprs{
					&vitess.TableFuncExpr{
						Name:  funcName,
						Exprs: tableFuncArgs,
						Alias: vitess.NewTableIdent(internalAlias),
					},
				},
			},
		},
		As:      vitess.NewTableIdent(string(node.As.Alias)),
		Lateral: true,
	}, true, nil
}

func fixedColumnTableFunctionColumns(funcName string) ([]string, bool) {
	switch funcName {
	case "each":
		return []string{"key", "value"}, true
	default:
		return nil, false
	}
}

func jsonArrayElementsTableFunction(funcName string) bool {
	switch funcName {
	case "json_array_elements", "json_array_elements_text", "jsonb_array_elements", "jsonb_array_elements_text":
		return true
	default:
		return false
	}
}

func singleColumnTableFunctionName(funcExpr *tree.FuncExpr) string {
	funcName := strings.ToLower(funcExpr.Func.String())
	if idx := strings.LastIndex(funcName, "."); idx >= 0 {
		funcName = funcName[idx+1:]
	}
	return strings.Trim(funcName, `"`)
}

func nodeJSONTableAliasedTableExpr(ctx *Context, node *tree.AliasedTableExpr, jsonTableExpr *tree.JSONTableExpr) (*vitess.AliasedTableExpr, error) {
	if len(jsonTableExpr.Columns) == 0 {
		return nil, errors.Errorf("JSON_TABLE requires at least one column")
	}
	sourceExpr, err := nodeExpr(ctx, jsonTableExpr.Expr)
	if err != nil {
		return nil, err
	}
	pathExpr, err := nodeExpr(ctx, jsonTableExpr.Path)
	if err != nil {
		return nil, err
	}

	internalAlias := "__doltgres_json_table"
	tableFuncArgs := make(vitess.SelectExprs, 0, 2+len(jsonTableExpr.Columns)*6)
	tableFuncArgs = append(
		tableFuncArgs,
		&vitess.AliasedExpr{Expr: sourceExpr},
		&vitess.AliasedExpr{Expr: pathExpr},
	)
	selectExprs := make(vitess.SelectExprs, len(jsonTableExpr.Columns))
	for i, col := range jsonTableExpr.Columns {
		colName := string(col.Name)
		var colType *pgtypes.DoltgresType
		if col.Ordinality {
			colType = pgtypes.Int32
		} else {
			_, colType, err = nodeResolvableTypeReference(ctx, col.Type, false)
			if err != nil {
				return nil, err
			}
			if colType == nil {
				return nil, errors.Errorf("JSON_TABLE column requires a type")
			}
		}
		colPath, err := nodeJSONTableColumnPath(ctx, col)
		if err != nil {
			return nil, err
		}
		tableFuncArgs = append(
			tableFuncArgs,
			tableFuncTextArg(colName),
			tableFuncTextArg(colType.ID.SchemaName()),
			tableFuncTextArg(colType.ID.TypeName()),
			tableFuncTextArg(strconv.FormatInt(int64(colType.GetAttTypMod()), 10)),
			colPath,
			tableFuncTextArg(strconv.FormatBool(col.Ordinality)),
		)
		selectExprs[i] = &vitess.AliasedExpr{
			Expr: tableFuncColumn(internalAlias, colName),
			As:   vitess.NewColIdent(colName),
		}
	}

	return &vitess.AliasedTableExpr{
		Expr: &vitess.Subquery{
			Select: &vitess.Select{
				SelectExprs: selectExprs,
				From: vitess.TableExprs{
					&vitess.TableFuncExpr{
						Name:  "doltgres_json_table",
						Exprs: tableFuncArgs,
						Alias: vitess.NewTableIdent(internalAlias),
					},
				},
			},
		},
		As:      vitess.NewTableIdent(string(node.As.Alias)),
		Lateral: node.Lateral,
	}, nil
}

func nodeJSONTableColumnPath(ctx *Context, col tree.JSONTableColumn) (*vitess.AliasedExpr, error) {
	if col.Ordinality || col.Path == nil {
		return tableFuncTextArg(""), nil
	}
	expr, err := nodeExpr(ctx, col.Path)
	if err != nil {
		return nil, err
	}
	return &vitess.AliasedExpr{Expr: expr}, nil
}

func nodeJsonToRecordAliasedTableExpr(ctx *Context, node *tree.AliasedTableExpr, rowsFromExpr *tree.RowsFromExpr) (*vitess.AliasedTableExpr, bool, error) {
	if len(rowsFromExpr.Items) != 1 {
		return nil, true, errors.Errorf("column definition list is only supported for a single table function")
	}
	funcExpr, ok := rowsFromExpr.Items[0].(*tree.FuncExpr)
	if !ok {
		return nil, true, errors.Errorf("column definition list is only supported for table functions")
	}

	convertedFuncExpr, err := nodeFuncExpr(ctx, funcExpr)
	if err != nil {
		return nil, true, err
	}
	vitessFuncExpr, ok := convertedFuncExpr.(*vitess.FuncExpr)
	if !ok {
		return nil, true, errors.Errorf("column definition list is only supported for table functions returning record")
	}

	var internalName string
	switch strings.ToLower(vitessFuncExpr.Name.String()) {
	case "json_to_record":
		internalName = "doltgres_json_to_record"
	case "json_to_recordset":
		internalName = "doltgres_json_to_recordset"
	case "jsonb_to_record":
		internalName = "doltgres_jsonb_to_record"
	case "jsonb_to_recordset":
		internalName = "doltgres_jsonb_to_recordset"
	default:
		return nil, false, nil
	}

	internalAlias := "__doltgres_json_to_record"
	tableFuncArgs := make(vitess.SelectExprs, 0, len(vitessFuncExpr.Exprs)+len(node.As.ColDefs)*4)
	tableFuncArgs = append(tableFuncArgs, vitessFuncExpr.Exprs...)
	selectExprs := make(vitess.SelectExprs, len(node.As.ColDefs))
	for i, colDef := range node.As.ColDefs {
		_, colType, err := nodeResolvableTypeReference(ctx, colDef.Type, false)
		if err != nil {
			return nil, true, err
		}
		if colType == nil {
			return nil, true, errors.Errorf("column definition requires a type")
		}
		colName := string(colDef.Name)
		tableFuncArgs = append(
			tableFuncArgs,
			tableFuncTextArg(colName),
			tableFuncTextArg(colType.ID.SchemaName()),
			tableFuncTextArg(colType.ID.TypeName()),
			tableFuncTextArg(strconv.FormatInt(int64(colType.GetAttTypMod()), 10)),
		)
		selectExprs[i] = &vitess.AliasedExpr{
			Expr: tableFuncColumn(internalAlias, colName),
			As:   vitess.NewColIdent(colName),
		}
	}

	return &vitess.AliasedTableExpr{
		Expr: &vitess.Subquery{
			Select: &vitess.Select{
				SelectExprs: selectExprs,
				From: vitess.TableExprs{
					&vitess.TableFuncExpr{
						Name:  internalName,
						Exprs: tableFuncArgs,
						Alias: vitess.NewTableIdent(internalAlias),
					},
				},
			},
		},
		As:      vitess.NewTableIdent(string(node.As.Alias)),
		Lateral: node.Lateral,
	}, true, nil
}

func tableFuncTextArg(value string) *vitess.AliasedExpr {
	return &vitess.AliasedExpr{
		Expr: vitess.InjectedExpr{
			Expression: pgexprs.NewTextLiteral(value),
		},
	}
}

func nodeAliasedTableExprWithOrdinality(ctx *Context, node *tree.AliasedTableExpr) (*vitess.AliasedTableExpr, error) {
	rowsFromExpr, ok := node.Expr.(*tree.RowsFromExpr)
	if !ok || len(rowsFromExpr.Items) != 1 {
		return nil, errors.Errorf("WITH ORDINALITY is only supported for a single table function")
	}
	funcExpr, ok := rowsFromExpr.Items[0].(*tree.FuncExpr)
	if !ok {
		return nil, errors.Errorf("WITH ORDINALITY is only supported for table functions")
	}
	funcName := singleColumnTableFunctionName(funcExpr)
	internalName := ""
	switch funcName {
	case "unnest":
		internalName = "doltgres_unnest_with_ordinality"
	case "generate_series":
		internalName = "doltgres_generate_series_with_ordinality"
	default:
		return nil, errors.Errorf("WITH ORDINALITY is only supported for unnest and generate_series")
	}

	args, err := nodeExprs(ctx, funcExpr.Exprs)
	if err != nil {
		return nil, err
	}
	tableFuncArgs := make(vitess.SelectExprs, len(args))
	for i, arg := range args {
		tableFuncArgs[i] = &vitess.AliasedExpr{Expr: arg}
	}

	internalAlias := "__" + internalName
	valueName := funcName
	ordinalityName := "ordinality"
	if len(node.As.Cols) > 0 {
		if len(node.As.Cols) != 2 {
			return nil, errors.Errorf("WITH ORDINALITY alias must provide value and ordinality column names")
		}
		valueName = string(node.As.Cols[0])
		ordinalityName = string(node.As.Cols[1])
	}

	aliasExpr := &vitess.Subquery{
		Select: &vitess.Select{
			SelectExprs: vitess.SelectExprs{
				&vitess.AliasedExpr{
					Expr: tableFuncColumn(internalAlias, "value"),
					As:   vitess.NewColIdent(valueName),
				},
				&vitess.AliasedExpr{
					Expr: tableFuncColumn(internalAlias, "ordinality"),
					As:   vitess.NewColIdent(ordinalityName),
				},
			},
			From: vitess.TableExprs{
				&vitess.TableFuncExpr{
					Name:  internalName,
					Exprs: tableFuncArgs,
					Alias: vitess.NewTableIdent(internalAlias),
				},
			},
		},
	}

	return &vitess.AliasedTableExpr{
		Expr:    aliasExpr,
		As:      vitess.NewTableIdent(string(node.As.Alias)),
		Lateral: node.Lateral,
	}, nil
}

func tableFuncColumn(table string, column string) *vitess.ColName {
	return &vitess.ColName{
		Name: vitess.NewColIdent(column),
		Qualifier: vitess.TableName{
			Name: vitess.NewTableIdent(table),
		},
	}
}

// isTrivialSelectStar returns true when the Select is just "SELECT * FROM <single table>"
// with no other clauses that would alter semantics (no WHERE, ORDER BY, LIMIT, GROUP BY,
// HAVING, DISTINCT, or WITH).
func isTrivialSelectStar(s *vitess.Select) bool {
	if len(s.From) != 1 ||
		s.QueryOpts.Distinct ||
		s.With != nil ||
		s.Limit != nil ||
		len(s.OrderBy) != 0 ||
		s.Where != nil ||
		len(s.GroupBy) != 0 ||
		s.Having != nil ||
		len(s.SelectExprs) != 1 {
		return false
	}
	starExpr, ok := s.SelectExprs[0].(*vitess.StarExpr)
	if !ok {
		return false
	}
	return starExpr.TableName.IsEmpty()
}
