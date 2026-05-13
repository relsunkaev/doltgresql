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

package framework

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

var _ sql.TableFunction = (*postgresFunctionTable)(nil)
var _ sql.ExecSourceRel = (*postgresFunctionTable)(nil)

// FunctionTable returns a PostgreSQL function wrapped as a table function.
func FunctionTable(ctx *sql.Context, name string) (sql.TableFunction, bool) {
	fn, ok := (&FunctionProvider{}).Function(ctx, name)
	if !ok {
		return nil, false
	}
	return &postgresFunctionTable{underlyingFunc: fn}, true
}

type postgresFunctionTable struct {
	underlyingFunc sql.Function
	database       sql.Database
	funcExpr       sql.Expression
	args           []sql.Expression
}

func (t *postgresFunctionTable) NewInstance(ctx *sql.Context, db sql.Database, args []sql.Expression) (sql.Node, error) {
	nt := *t
	nt.database = db
	nt.args = args
	fn, err := nt.underlyingFunc.NewInstance(ctx, args)
	if err != nil {
		return nil, err
	}
	if !fn.Resolved() {
		return nil, fmt.Errorf("table function is unresolved")
	}
	nt.funcExpr = fn
	return &nt, nil
}

func (t *postgresFunctionTable) Children() []sql.Node {
	return nil
}

func (t *postgresFunctionTable) Database() sql.Database {
	return t.database
}

func (t *postgresFunctionTable) Expressions() []sql.Expression {
	if t.funcExpr == nil {
		return nil
	}
	return []sql.Expression{t.funcExpr}
}

func (t *postgresFunctionTable) IsReadOnly() bool {
	return true
}

func (t *postgresFunctionTable) Name() string {
	return t.underlyingFunc.FunctionName()
}

func (t *postgresFunctionTable) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	value, err := t.funcExpr.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	iter, ok := value.(sql.RowIter)
	if !ok {
		return sql.RowsToRowIter(sql.Row{value}), nil
	}
	if !functionTableShouldExpandComposite(t.funcExpr.Type(ctx)) {
		return iter, nil
	}
	return &compositeFunctionTableRowIter{child: iter}, nil
}

func (t *postgresFunctionTable) Resolved() bool {
	for _, expr := range t.args {
		if !expr.Resolved() {
			return false
		}
	}
	return true
}

func (t *postgresFunctionTable) Schema(ctx *sql.Context) sql.Schema {
	dbName := ""
	if t.database != nil {
		dbName = t.database.Name()
	}
	valueType := t.funcExpr.Type(ctx)
	if typ, ok := valueType.(*pgtypes.DoltgresType); ok && typ.TypCategory == pgtypes.TypeCategory_CompositeTypes && len(typ.CompositeAttrs) > 0 {
		schema := make(sql.Schema, len(typ.CompositeAttrs))
		typeCollection, _ := pgtypes.GetTypesCollectionFromContext(ctx)
		for i, attr := range typ.CompositeAttrs {
			attrType := pgtypes.Unknown
			if typeCollection != nil {
				if resolvedType, err := attr.ResolveType(ctx, typeCollection); err == nil && resolvedType != nil {
					attrType = resolvedType
				}
			}
			schema[i] = &sql.Column{
				DatabaseSource: dbName,
				Source:         t.Name(),
				Name:           attr.Name,
				Type:           attrType,
				Nullable:       true,
			}
		}
		return schema
	}
	return sql.Schema{&sql.Column{
		DatabaseSource: dbName,
		Source:         t.Name(),
		Name:           t.Name(),
		Type:           valueType,
		Nullable:       true,
	}}
}

func (t *postgresFunctionTable) String() string {
	var args []string
	for _, expr := range t.args {
		args = append(args, expr.String())
	}
	return fmt.Sprintf("%s(%s)", t.Name(), strings.Join(args, ", "))
}

func (t *postgresFunctionTable) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(children), 0)
	}
	return t, nil
}

func (t *postgresFunctionTable) WithDatabase(database sql.Database) (sql.Node, error) {
	nt := *t
	nt.database = database
	return &nt, nil
}

func (t *postgresFunctionTable) WithExpressions(ctx *sql.Context, exprs ...sql.Expression) (sql.Node, error) {
	if t.funcExpr == nil {
		if len(exprs) != 0 {
			return nil, sql.ErrInvalidChildrenNumber.New(t, len(exprs), 0)
		}
		return t, nil
	}
	if len(exprs) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(exprs), 1)
	}
	nt := *t
	nt.funcExpr = exprs[0]
	return &nt, nil
}

func functionTableShouldExpandComposite(valueType sql.Type) bool {
	typ, ok := valueType.(*pgtypes.DoltgresType)
	return ok && typ.TypCategory == pgtypes.TypeCategory_CompositeTypes && len(typ.CompositeAttrs) > 0
}

type compositeFunctionTableRowIter struct {
	child sql.RowIter
}

func (i *compositeFunctionTableRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	row, err := i.child.Next(ctx)
	if err != nil {
		return nil, err
	}
	if len(row) != 1 {
		return row, nil
	}
	record, ok := row[0].([]pgtypes.RecordValue)
	if !ok {
		return row, nil
	}
	expanded := make(sql.Row, len(record))
	for i, field := range record {
		expanded[i] = field.Value
	}
	return expanded, nil
}

func (i *compositeFunctionTableRowIter) Close(ctx *sql.Context) error {
	return i.child.Close(ctx)
}
