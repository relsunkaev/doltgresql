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
	"encoding/hex"
	"strings"

	"github.com/cockroachdb/errors"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/auth"
	pgexprs "github.com/dolthub/doltgresql/server/expression"
)

const qualifiedFunctionNamePrefix = "__doltgres_qualified_function__"
const qualifiedFunctionNameSeparator = "\x1f"

// nodeFuncExpr handles *tree.FuncExpr nodes.
func nodeFuncExpr(ctx *Context, node *tree.FuncExpr) (vitess.Expr, error) {
	if node == nil {
		return nil, nil
	}
	if node.Filter != nil {
		// PostgreSQL aggregate FILTER (WHERE pred): rewrite each argument
		// expression to `CASE WHEN pred THEN arg ELSE NULL END`. Aggregates
		// that ignore NULLs (sum/avg/count/etc.) then naturally skip
		// non-matching rows. count(*) is special-cased below: the * is
		// replaced with a literal 1 so the rewritten form becomes
		// count(CASE WHEN pred THEN 1 END).
		filtered, err := rewriteAggregateFilter(node)
		if err != nil {
			return nil, err
		}
		node = filtered
	}
	if node.AggType == tree.OrderedSetAgg {
		return nil, errors.Errorf("WITHIN GROUP is not yet supported")
	}

	var qualifier vitess.TableIdent
	var name vitess.ColIdent
	switch funcRef := node.Func.FunctionReference.(type) {
	case *tree.FunctionDefinition:
		name = vitess.NewColIdent(funcRef.Name)
	case *tree.UnresolvedName:
		if funcRef.NumParts == 3 {
			qualifier = vitess.NewTableIdent(funcRef.Parts[1])
			name = vitess.NewColIdent(qualifiedFunctionName(funcRef.Parts[2], funcRef.Parts[1], funcRef.Parts[0]))
		} else if funcRef.NumParts == 2 {
			qualifier = vitess.NewTableIdent(funcRef.Parts[1])
			name = vitess.NewColIdent(qualifiedFunctionName("", funcRef.Parts[1], funcRef.Parts[0]))
		} else {
			colName, err := unresolvedNameToColName(funcRef)
			if err != nil {
				return nil, err
			}

			qualifier = colName.Qualifier.Name
			name = colName.Name
		}
	default:
		return nil, errors.Errorf("unknown function reference")
	}
	var distinct bool
	switch node.Type {
	case 0, tree.AllFuncType:
		distinct = false
	case tree.DistinctFuncType:
		distinct = true
	default:
		return nil, errors.Errorf("unknown function spec type %d", node.Type)
	}
	windowDef, err := nodeWindowDef(ctx, node.WindowDef)
	if err != nil {
		return nil, err
	}
	exprs, err := nodeExprsToSelectExprs(ctx, node.Exprs)
	if err != nil {
		return nil, err
	}

	switch strings.ToLower(name.String()) {
	// special case for string_agg, which maps to the mysql aggregate function group_concat
	case "string_agg":
		if len(node.Exprs) != 2 {
			return nil, errors.Errorf("string_agg requires two arguments")
		}

		sepString := ""
		if sep, ok := node.Exprs[1].(*tree.StrVal); ok {
			sepString = strings.Trim(sep.String(), "'")
		} else {
			// TODO: need to support this function in doltgres
			c, is := node.Exprs[1].(*tree.CastExpr)
			if !is && c.Type.SQLString() != "TEXT" {
				return nil, errors.Errorf("string_agg requires a string separator")
			}
			sepString = strings.Trim(c.Expr.String(), "'")
		}

		var orderBy vitess.OrderBy
		if len(node.OrderBy) > 0 {
			orderBy, err = nodeOrderBy(ctx, node.OrderBy, nil)
			if err != nil {
				return nil, err
			}
		}

		distinctStr := ""
		if distinct {
			distinctStr = vitess.DistinctStr
		}
		return &vitess.GroupConcatExpr{
			Distinct: distinctStr,
			Exprs:    exprs[:1],
			Separator: vitess.Separator{
				SeparatorString: sepString,
			},
			OrderBy: orderBy,
		}, nil
	case "array_agg":
		var orderBy vitess.OrderBy
		if len(node.OrderBy) > 0 {
			orderBy, err = nodeOrderBy(ctx, node.OrderBy, nil)
			if err != nil {
				return nil, err
			}
		}

		return &vitess.OrderedInjectedExpr{
			InjectedExpr: vitess.InjectedExpr{
				Expression:         pgexprs.NewArrayAgg(distinct),
				SelectExprChildren: exprs,
				Auth:               vitess.AuthInformation{},
			},
			OrderBy: orderBy,
		}, nil
	case "json_agg", "jsonb_agg", "json_object_agg", "jsonb_object_agg":
		fnName := strings.ToLower(name.String())
		isObjectAgg := strings.Contains(fnName, "object")
		if isObjectAgg && len(node.Exprs) != 2 {
			return nil, errors.Errorf("%s requires two arguments", fnName)
		}
		if !isObjectAgg && len(node.Exprs) != 1 {
			return nil, errors.Errorf("%s requires one argument", fnName)
		}
		var orderBy vitess.OrderBy
		if len(node.OrderBy) > 0 {
			orderBy, err = nodeOrderBy(ctx, node.OrderBy, nil)
			if err != nil {
				return nil, err
			}
		}
		return &vitess.OrderedInjectedExpr{
			InjectedExpr: vitess.InjectedExpr{
				Expression:         pgexprs.NewJsonAgg(fnName, isObjectAgg, strings.HasPrefix(fnName, "jsonb"), distinct),
				SelectExprChildren: exprs,
				Auth:               vitess.AuthInformation{},
			},
			OrderBy: orderBy,
		}, nil
	}

	if len(node.OrderBy) > 0 {
		return nil, errors.Errorf("function ORDER BY is not yet supported")
	}

	return &vitess.FuncExpr{
		Qualifier: qualifier,
		Name:      name,
		Distinct:  distinct,
		Exprs:     exprs,
		Over:      (*vitess.Over)(windowDef),
		Auth: vitess.AuthInformation{
			AuthType:    auth.AuthType_EXECUTE,
			TargetType:  auth.AuthTargetType_FunctionIdentifiers,
			TargetNames: []string{qualifier.String(), name.String()},
		},
	}, nil
}

func qualifiedFunctionName(database string, schema string, function string) string {
	return qualifiedFunctionNamePrefix +
		hex.EncodeToString([]byte(database)) + qualifiedFunctionNameSeparator +
		hex.EncodeToString([]byte(schema)) + qualifiedFunctionNameSeparator +
		hex.EncodeToString([]byte(function))
}

// rewriteAggregateFilter rewrites `func(args...) FILTER (WHERE pred)` to
// `func(CASE WHEN pred THEN arg ELSE NULL END, ...)`. Returns a copy of
// the node with the filter cleared and arguments wrapped. UnqualifiedStar
// arguments are replaced with a literal 1 so count(*) FILTER becomes
// count(CASE WHEN pred THEN 1 END).
func rewriteAggregateFilter(node *tree.FuncExpr) (*tree.FuncExpr, error) {
	pred := node.Filter
	rewritten := *node
	rewritten.Filter = nil

	rewrittenExprs := make(tree.Exprs, len(node.Exprs))
	for i, arg := range node.Exprs {
		var val tree.Expr
		switch arg.(type) {
		case tree.UnqualifiedStar:
			val = tree.NewDInt(1)
		default:
			val = arg
		}
		rewrittenExprs[i] = &tree.CaseExpr{
			Whens: []*tree.When{{Cond: pred, Val: val}},
			Else:  tree.DNull,
		}
	}
	rewritten.Exprs = rewrittenExprs
	return &rewritten, nil
}
