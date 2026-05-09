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
	"strings"

	"github.com/cockroachdb/errors"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
)

// nodeCreateView handles *tree.CreateView nodes.
func nodeCreateView(ctx *Context, node *tree.CreateView) (*vitess.DDL, error) {
	if node == nil {
		return nil, nil
	}
	if node.Persistence.IsTemporary() {
		return nil, errors.Errorf("CREATE TEMPORARY VIEW is not yet supported")
	}
	if node.IsRecursive {
		return nil, errors.Errorf("CREATE RECURSIVE VIEW is not yet supported")
	}
	var checkOption = tree.ViewCheckOptionUnspecified
	var sqlSecurity string
	if node.Options != nil {
		for _, opt := range node.Options {
			switch strings.ToLower(opt.Name) {
			case "check_option":
				switch strings.ToLower(opt.CheckOpt) {
				case "local":
					checkOption = tree.ViewCheckOptionLocal
				case "cascaded":
					checkOption = tree.ViewCheckOptionCascaded
				default:
					return nil, errors.Errorf(`"ERROR:  syntax error at or near "%s"`, opt.Name)
				}
			case "security_barrier":
				return nil, errors.Errorf("CREATE VIEW '%s' option is not yet supported", opt.Name)
			case "security_invoker":
				if opt.Security {
					sqlSecurity = "invoker"
				} else {
					sqlSecurity = "definer"
				}
			default:
				return nil, errors.Errorf(`"ERROR:  syntax error at or near "%s"`, opt.Name)
			}
		}
	}

	if checkOption != tree.ViewCheckOptionUnspecified && node.CheckOption != tree.ViewCheckOptionUnspecified {
		return nil, errors.Errorf(`ERROR:  parameter "check_option" specified more than once`)
	} else {
		checkOption = node.CheckOption
	}

	vCheckOpt := vitess.ViewCheckOptionUnspecified
	switch checkOption {
	case tree.ViewCheckOptionCascaded:
		vCheckOpt = vitess.ViewCheckOptionCascaded
	case tree.ViewCheckOptionLocal:
		vCheckOpt = vitess.ViewCheckOptionLocal
	default:
	}

	tableName, err := nodeTableName(ctx, &node.Name)
	if err != nil {
		return nil, err
	}
	selectStmt, err := nodeSelect(ctx, node.AsSource)
	if err != nil {
		return nil, err
	}
	var cols = make(vitess.Columns, len(node.ColumnNames))
	for i, col := range node.ColumnNames {
		cols[i] = vitess.NewColIdent(col.String())
	}

	stmt := &vitess.DDL{
		Action:    vitess.CreateStr,
		OrReplace: node.Replace,
		ViewSpec: &vitess.ViewSpec{
			ViewName:    tableName,
			ViewExpr:    selectStmt,
			Columns:     cols,
			Security:    sqlSecurity,
			CheckOption: vCheckOpt,
		},
		SubStatementStr: createViewSelectDefinition(ctx, node.AsSource.String()),
	}
	return stmt, nil
}

func createViewSelectDefinition(ctx *Context, fallback string) string {
	query := strings.TrimSpace(ctx.originalQuery)
	if query == "" {
		return fallback
	}
	query = strings.TrimSuffix(query, ";")
	asIdx := findKeywordOutsideQuotes(query, "as")
	if asIdx < 0 {
		return fallback
	}
	return strings.TrimSpace(query[asIdx+len("as"):])
}

func findKeywordOutsideQuotes(query, keyword string) int {
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
			if isIdentifierByteAround(query, i-1) || isIdentifierByteAround(query, end) {
				continue
			}
			return i
		}
	}
	return -1
}

func isIdentifierByteAround(query string, idx int) bool {
	if idx < 0 || idx >= len(query) {
		return false
	}
	ch := query[idx]
	return ch == '_' ||
		(ch >= '0' && ch <= '9') ||
		(ch >= 'a' && ch <= 'z') ||
		(ch >= 'A' && ch <= 'Z')
}
