// Copyright 2024 Dolthub, Inc.
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

package functions

import (
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/postgres/parser/parser"
	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initPgGetViewDef registers the functions to the catalog.
func initPgGetViewDef() {
	framework.RegisterFunction(pg_get_viewdef_oid)
	framework.RegisterFunction(pg_get_viewdef_oid_bool)
	framework.RegisterFunction(pg_get_viewdef_oid_int)
}

// pg_get_viewdef_oid represents the PostgreSQL system catalog information function taking 1 parameter.
var pg_get_viewdef_oid = framework.Function1{
	Name:               "pg_get_viewdef",
	Return:             pgtypes.Text,
	Parameters:         [1]*pgtypes.DoltgresType{pgtypes.Oid},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		oidVal := val.(id.Id)
		return getViewDef(ctx, oidVal)
	},
}

// pg_get_viewdef_oid_bool represents the PostgreSQL system catalog information function taking 2 parameters.
var pg_get_viewdef_oid_bool = framework.Function2{
	Name:               "pg_get_viewdef",
	Return:             pgtypes.Text,
	Parameters:         [2]*pgtypes.DoltgresType{pgtypes.Oid, pgtypes.Bool},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		oidVal := val1.(id.Id)
		// TODO: pretty printing is not yet supported
		return getViewDef(ctx, oidVal)
	},
}

// pg_get_viewdef_oid_int represents the PostgreSQL system catalog information function taking 2 parameters.
var pg_get_viewdef_oid_int = framework.Function2{
	Name:               "pg_get_viewdef",
	Return:             pgtypes.Text,
	Parameters:         [2]*pgtypes.DoltgresType{pgtypes.Oid, pgtypes.Int64},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		oidVal := val1.(id.Id)
		// PostgreSQL treats the integer argument as a preferred wrap column.
		// Doltgres does not pretty-wrap definitions yet, but the overload
		// should still return the canonical view definition.
		return getViewDef(ctx, oidVal)
	},
}

// getViewDef takes oid of view and returns the text definition of underlying SELECT statement.
func getViewDef(ctx *sql.Context, oidVal id.Id) (string, error) {
	var result string
	err := RunCallback(ctx, oidVal, Callbacks{
		View: func(ctx *sql.Context, sch ItemSchema, view ItemView) (cont bool, err error) {
			result, err = pgGetViewdefDefinitionForView(view.Item.Name, view.Item.CreateViewStatement, view.Item.TextDefinition, sch.Item.SchemaName())
			if err != nil {
				return false, err
			}
			if result == "" {
				result = selectDefinitionFromCreateViewStatement(view.Item.CreateViewStatement)
			}
			if result == "" {
				result = view.Item.TextDefinition
			}
			if result == "" {
				stmts, err := parser.Parse(view.Item.CreateViewStatement)
				if err != nil {
					return false, err
				}
				if len(stmts) == 0 {
					return false, errors.Errorf("expected CREATE VIEW statement, got none")
				}
				cv, ok := stmts[0].AST.(*tree.CreateView)
				if !ok {
					return false, errors.Errorf("expected CREATE VIEW statement, got %s", stmts[0].SQL)
				}
				result = cv.AsSource.String()
			}
			result = formatPgGetViewdefDefinition(result)
			result = ensureTrailingSemicolon(closeTrailingStringLiteral(result))
			return false, nil
		},
	})
	if err != nil {
		return "", err
	}
	return result, nil
}

func pgGetViewdefDefinitionForView(viewName string, createViewStatement string, textDefinition string, defaultSchema string) (string, error) {
	result, err := pgGetViewdefDefinition(createViewStatement, defaultSchema)
	if err != nil || result != "" {
		return result, err
	}
	if strings.TrimSpace(textDefinition) == "" {
		return "", nil
	}
	return pgGetViewdefDefinition("CREATE VIEW "+quoteIdentifierIfNeeded(viewName)+" AS "+textDefinition, defaultSchema)
}

// SchemaQualifiedViewDefinition returns the SELECT body from a CREATE VIEW
// statement with unqualified relation references bound to defaultSchema.
func SchemaQualifiedViewDefinition(createViewStatement string, defaultSchema string) (string, error) {
	if strings.TrimSpace(createViewStatement) == "" {
		return "", nil
	}
	stmts, err := parser.Parse(createViewStatement)
	if err != nil {
		return "", err
	}
	if len(stmts) == 0 {
		return "", errors.Errorf("expected CREATE VIEW statement, got none")
	}
	cv, ok := stmts[0].AST.(*tree.CreateView)
	if !ok {
		return "", errors.Errorf("expected CREATE VIEW statement, got %s", stmts[0].SQL)
	}
	qualifySelectTableNames(cv.AsSource, defaultSchema)
	return cv.AsSource.String(), nil
}

func pgGetViewdefDefinition(createViewStatement string, defaultSchema string) (string, error) {
	if strings.TrimSpace(createViewStatement) == "" {
		return "", nil
	}
	stmts, err := parser.Parse(createViewStatement)
	if err != nil {
		return "", err
	}
	if len(stmts) == 0 {
		return "", errors.Errorf("expected CREATE VIEW statement, got none")
	}
	cv, ok := stmts[0].AST.(*tree.CreateView)
	if !ok {
		return "", errors.Errorf("expected CREATE VIEW statement, got %s", stmts[0].SQL)
	}
	if shouldSchemaQualifyPgGetViewdef(cv.AsSource) {
		qualifySelectTableNames(cv.AsSource, defaultSchema)
	}
	return cv.AsSource.String(), nil
}

func shouldSchemaQualifyPgGetViewdef(sel *tree.Select) bool {
	if sel == nil {
		return false
	}
	selectClause, ok := sel.Select.(*tree.SelectClause)
	if !ok {
		return true
	}
	if len(selectClause.From.Tables) != 1 {
		return true
	}
	tableExpr := selectClause.From.Tables[0]
	for {
		aliased, ok := tableExpr.(*tree.AliasedTableExpr)
		if !ok {
			break
		}
		tableExpr = aliased.Expr
	}
	_, ok = tableExpr.(*tree.TableName)
	return !ok
}

func qualifySelectTableNames(sel *tree.Select, defaultSchema string) {
	if sel == nil {
		return
	}
	cteNames := make(map[string]struct{})
	if sel.With != nil {
		for _, cte := range sel.With.CTEList {
			cteNames[string(cte.Name.Alias)] = struct{}{}
			if cteSelect, ok := cte.Stmt.(*tree.Select); ok {
				qualifySelectTableNames(cteSelect, defaultSchema)
			}
		}
	}
	qualifySelectStatementTableNames(sel.Select, defaultSchema, cteNames)
}

func qualifySelectStatementTableNames(stmt tree.SelectStatement, defaultSchema string, cteNames map[string]struct{}) {
	switch s := stmt.(type) {
	case *tree.SelectClause:
		for _, tableExpr := range s.From.Tables {
			qualifyTableExprNames(tableExpr, defaultSchema, cteNames)
		}
		qualifySelectExprsSubqueries(s.Exprs, defaultSchema)
		if s.Where != nil {
			qualifyExprSubqueries(s.Where.Expr, defaultSchema)
		}
		if s.Having != nil {
			qualifyExprSubqueries(s.Having.Expr, defaultSchema)
		}
	case *tree.ParenSelect:
		qualifySelectTableNames(s.Select, defaultSchema)
	case *tree.UnionClause:
		qualifySelectTableNames(s.Left, defaultSchema)
		qualifySelectTableNames(s.Right, defaultSchema)
	}
}

func qualifyTableExprNames(expr tree.TableExpr, defaultSchema string, cteNames map[string]struct{}) {
	switch e := expr.(type) {
	case *tree.TableName:
		if _, ok := cteNames[e.Table()]; ok {
			return
		}
		if !e.ExplicitSchema {
			e.SchemaName = tree.Name(defaultSchema)
			e.ExplicitSchema = true
		}
	case *tree.AliasedTableExpr:
		qualifyTableExprNames(e.Expr, defaultSchema, cteNames)
	case *tree.JoinTableExpr:
		qualifyTableExprNames(e.Left, defaultSchema, cteNames)
		qualifyTableExprNames(e.Right, defaultSchema, cteNames)
		if onCond, ok := e.Cond.(*tree.OnJoinCond); ok {
			qualifyExprSubqueries(onCond.Expr, defaultSchema)
		}
	case *tree.ParenTableExpr:
		qualifyTableExprNames(e.Expr, defaultSchema, cteNames)
	case *tree.Subquery:
		qualifySelectStatementTableNames(e.Select, defaultSchema, cteNames)
	}
}

func qualifySelectExprsSubqueries(exprs tree.SelectExprs, defaultSchema string) {
	for _, expr := range exprs {
		qualifyExprSubqueries(expr.Expr, defaultSchema)
	}
}

func qualifyExprSubqueries(expr tree.Expr, defaultSchema string) {
	if expr == nil {
		return
	}
	_, _ = tree.SimpleVisit(expr, func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		if subquery, ok := expr.(*tree.Subquery); ok {
			qualifySelectStatementTableNames(subquery.Select, defaultSchema, map[string]struct{}{})
		}
		return true, expr, nil
	})
}

func selectDefinitionFromCreateViewStatement(createViewStatement string) string {
	query := strings.TrimSpace(createViewStatement)
	if query == "" {
		return ""
	}
	query = strings.TrimSuffix(query, ";")
	asIdx := findKeywordOutsideQuotes(query, "as")
	if asIdx < 0 {
		return ""
	}
	return strings.TrimSpace(query[asIdx+len("as"):])
}

func formatPgGetViewdefDefinition(query string) string {
	query = strings.TrimSpace(query)
	query = strings.TrimSuffix(query, ";")
	query = strings.TrimSpace(query)
	if query == "" {
		return ""
	}
	if fromIdx := findTopLevelKeywordOutsideQuotes(query, "from"); fromIdx >= 0 {
		query = strings.TrimRight(query[:fromIdx], " \t\r\n") + "\n   " + strings.TrimLeft(query[fromIdx:], " \t\r\n")
	}
	return " " + query
}

func closeTrailingStringLiteral(query string) string {
	inSingleQuote := false
	for i := 0; i < len(query); i++ {
		if query[i] != '\'' {
			continue
		}
		if inSingleQuote && i+1 < len(query) && query[i+1] == '\'' {
			i++
			continue
		}
		inSingleQuote = !inSingleQuote
	}
	if inSingleQuote {
		return query + "'"
	}
	return query
}

func ensureTrailingSemicolon(query string) string {
	trimmed := strings.TrimRight(query, " \t\r\n")
	if strings.HasSuffix(trimmed, ";") {
		return query
	}
	return query + ";"
}

func findKeywordOutsideQuotes(query string, keyword string) int {
	lowerKeyword := strings.ToLower(keyword)
	inSingleQuote := false
	inDoubleQuote := false
	for i := 0; i < len(query); i++ {
		switch query[i] {
		case '\'':
			if inDoubleQuote {
				continue
			}
			if inSingleQuote && i+1 < len(query) && query[i+1] == '\'' {
				i++
				continue
			}
			inSingleQuote = !inSingleQuote
		case '"':
			if !inSingleQuote {
				inDoubleQuote = !inDoubleQuote
			}
		default:
			if inSingleQuote || inDoubleQuote {
				continue
			}
			end := i + len(keyword)
			if end > len(query) || strings.ToLower(query[i:end]) != lowerKeyword {
				continue
			}
			if isIdentifierByte(query, i-1) || isIdentifierByte(query, end) {
				continue
			}
			return i
		}
	}
	return -1
}

func findTopLevelKeywordOutsideQuotes(query string, keyword string) int {
	lowerKeyword := strings.ToLower(keyword)
	inSingleQuote := false
	inDoubleQuote := false
	parenDepth := 0
	for i := 0; i < len(query); i++ {
		switch query[i] {
		case '\'':
			if inDoubleQuote {
				continue
			}
			if inSingleQuote && i+1 < len(query) && query[i+1] == '\'' {
				i++
				continue
			}
			inSingleQuote = !inSingleQuote
		case '"':
			if !inSingleQuote {
				inDoubleQuote = !inDoubleQuote
			}
		case '(':
			if !inSingleQuote && !inDoubleQuote {
				parenDepth++
			}
		case ')':
			if !inSingleQuote && !inDoubleQuote && parenDepth > 0 {
				parenDepth--
			}
		default:
			if inSingleQuote || inDoubleQuote || parenDepth > 0 {
				continue
			}
			end := i + len(keyword)
			if end > len(query) || strings.ToLower(query[i:end]) != lowerKeyword {
				continue
			}
			if isIdentifierByte(query, i-1) || isIdentifierByte(query, end) {
				continue
			}
			return i
		}
	}
	return -1
}

func isIdentifierByte(query string, idx int) bool {
	if idx < 0 || idx >= len(query) {
		return false
	}
	ch := query[idx]
	return ch == '_' ||
		(ch >= '0' && ch <= '9') ||
		(ch >= 'a' && ch <= 'z') ||
		(ch >= 'A' && ch <= 'Z')
}
