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
		// TODO: prettyprint is implied, not yet supported
		// TODO: lines with fields are wrapped to specified number of columns
		return "", errors.Errorf("not yet supported")
	},
}

// getViewDef takes oid of view and returns the text definition of underlying SELECT statement.
func getViewDef(ctx *sql.Context, oidVal id.Id) (string, error) {
	var result string
	err := RunCallback(ctx, oidVal, Callbacks{
		View: func(ctx *sql.Context, sch ItemSchema, view ItemView) (cont bool, err error) {
			result = selectDefinitionFromCreateViewStatement(view.Item.CreateViewStatement)
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
			result = ensureTrailingSemicolon(closeTrailingStringLiteral(result))
			return false, nil
		},
	})
	if err != nil {
		return "", err
	}
	return result, nil
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
