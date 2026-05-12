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
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/config"
)

// setLocalSelect rewrites `SET LOCAL name = value` into the equivalent
// `SELECT set_config('name', 'value', true)`, which routes through the
// transaction-scope variable tracker.
func setLocalSelect(ctx *Context, node *tree.SetVar) (vitess.Statement, error) {
	fullName := node.Name
	if node.Namespace != "" {
		fullName = fmt.Sprintf("%s.%s", node.Namespace, node.Name)
	}
	valueExpr, err := setLocalValue(node.Name, node.Values)
	if err != nil {
		return nil, err
	}
	// We dispatch through __doltgres_set_config_local(name, value)
	// rather than the public set_config(name, value, is_local=true)
	// to avoid materializing a boolean literal in vitess (which
	// resolves to an integer overload that does not exist for
	// set_config). The internal function shares its implementation
	// with set_config and does the same SET LOCAL bookkeeping.
	return &vitess.Select{
		SelectExprs: vitess.SelectExprs{
			&vitess.AliasedExpr{
				Expr: &vitess.FuncExpr{
					Name: vitess.NewColIdent("__doltgres_set_config_local"),
					Exprs: vitess.SelectExprs{
						&vitess.AliasedExpr{
							Expr: &vitess.SQLVal{Type: vitess.StrVal, Val: []byte(fullName)},
						},
						&vitess.AliasedExpr{Expr: valueExpr},
					},
				},
				As: vitess.NewColIdent("set_config"),
			},
		},
	}, nil
}

func setLocalValue(name string, values tree.Exprs) (vitess.Expr, error) {
	if len(values) == 0 {
		return nil, errors.Errorf(`ERROR: syntax error at or near ";"'`)
	}
	if len(values) > 1 {
		vals := make([]string, len(values))
		for i, value := range values {
			vals[i] = value.String()
		}
		return &vitess.SQLVal{Type: vitess.StrVal, Val: []byte(strings.Join(vals, ", "))}, nil
	}
	value := values[0]
	if strings.EqualFold(value.String(), "default") {
		defaultValue, ok := config.GetPostgresConfigParameterDefault(name)
		if !ok {
			return nil, errors.Errorf(`ERROR: unrecognized configuration parameter "%s"`, name)
		}
		return &vitess.SQLVal{Type: vitess.StrVal, Val: []byte(fmt.Sprint(defaultValue))}, nil
	}
	if str, ok := value.(*tree.StrVal); ok {
		return &vitess.SQLVal{Type: vitess.StrVal, Val: []byte(str.RawString())}, nil
	}
	return &vitess.SQLVal{Type: vitess.StrVal, Val: []byte(value.String())}, nil
}

// nodeSetVar handles *tree.SetVar nodes.
func nodeSetVar(ctx *Context, node *tree.SetVar) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}
	// USE statement alias
	if node.Name == "database" {
		// strip off all quotes from the database name
		dbName := strings.TrimPrefix(strings.TrimSuffix(node.Values[0].String(), "'"), "'")
		dbName = strings.TrimPrefix(strings.TrimSuffix(dbName, "\""), "\"")
		dbName = strings.TrimPrefix(strings.TrimSuffix(dbName, "`"), "`")
		return &vitess.Use{DBName: vitess.NewTableIdent(dbName)}, nil
	}
	if node.Namespace == "" && !config.IsValidPostgresConfigParameter(node.Name) && !config.IsValidDoltConfigParameter(node.Name) {
		return nil, errors.Errorf(`ERROR: unrecognized configuration parameter "%s"`, node.Name)
	}
	if node.IsLocal {
		// PostgreSQL's SET LOCAL writes the variable for the duration
		// of the surrounding transaction only. We rewrite it into a
		// SELECT against set_config(name, value, true): the function
		// records a snapshot of the pre-write value and the wire layer
		// restores it at transaction end. This deliberately leaves the
		// command tag as SELECT instead of SET — a tradeoff we accept
		// to keep the GUC machinery on a single code path.
		return setLocalSelect(ctx, node)
	}
	var expr vitess.Expr
	var err error
	if len(node.Values) == 0 {
		// sanity check
		return nil, errors.Errorf(`ERROR: syntax error at or near ";"'`)
	} else if len(node.Values) > 1 {
		vals := make([]string, len(node.Values))
		for i, val := range node.Values {
			vals[i] = val.String()
		}
		expr = &vitess.ColName{
			Name: vitess.NewColIdent(strings.Join(vals, ", ")),
		}
	} else if strings.EqualFold(node.Name, "timezone") {
		if interval, ok := node.Values[0].(*tree.DInterval); ok {
			expr = &vitess.SQLVal{Type: vitess.StrVal, Val: []byte(interval.Duration.String())}
		} else {
			expr, err = nodeExpr(ctx, node.Values[0])
			if err != nil {
				return nil, err
			}
		}
	} else {
		expr, err = nodeExpr(ctx, node.Values[0])
		if err != nil {
			return nil, err
		}
	}

	if node.Namespace == "" {
		return &vitess.Set{
			Exprs: vitess.SetVarExprs{&vitess.SetVarExpr{
				Scope: vitess.SetScope_Session,
				Name: &vitess.ColName{
					Name: vitess.NewColIdent(node.Name),
				},
				Expr: expr,
			}},
		}, nil
	} else {
		return &vitess.Set{
			Exprs: vitess.SetVarExprs{&vitess.SetVarExpr{
				Scope: vitess.SetScope_User,
				Name: &vitess.ColName{
					Name: vitess.NewColIdent(fmt.Sprintf("%s.%s", node.Namespace, node.Name)),
				},
				Expr: expr,
			}},
		}, nil
	}
}
