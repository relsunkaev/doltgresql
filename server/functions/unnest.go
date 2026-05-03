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
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtablefunctions"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initUnnest registers the functions to the catalog.
func initUnnest() {
	framework.RegisterFunction(unnest)
	dtablefunctions.DoltTableFunctions = append(dtablefunctions.DoltTableFunctions, &unnestWithOrdinalityTableFunction{})
}

// unnest represents the PostgreSQL function of the same name, taking the same parameters.
var unnest = framework.Function1{
	Name:       "unnest",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.AnyElement),
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.AnyArray},
	Strict:     true,
	SRF:        true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val1 any) (any, error) {
		valArr := val1.([]interface{})

		var i = 0
		return pgtypes.NewSetReturningFunctionRowIter(func(ctx *sql.Context) (sql.Row, error) {
			defer func() { i++ }()

			if i >= len(valArr) {
				return nil, io.EOF
			}
			return sql.Row{valArr[i]}, nil
		}), nil
	},
}

var _ sql.TableFunction = (*unnestWithOrdinalityTableFunction)(nil)
var _ sql.ExecSourceRel = (*unnestWithOrdinalityTableFunction)(nil)

type unnestWithOrdinalityTableFunction struct {
	db        sql.Database
	exprs     []sql.Expression
	valueType sql.Type
}

func (u *unnestWithOrdinalityTableFunction) NewInstance(ctx *sql.Context, db sql.Database, args []sql.Expression) (sql.Node, error) {
	if len(args) != 1 {
		return nil, sql.ErrInvalidArgumentNumber.New(u.Name(), 1, len(args))
	}
	valueType := sql.Type(pgtypes.AnyElement)
	if doltgresType, ok := args[0].Type(ctx).(*pgtypes.DoltgresType); ok && doltgresType.IsArrayType() {
		valueType = doltgresType.ArrayBaseType()
	}
	return &unnestWithOrdinalityTableFunction{
		db:        db,
		exprs:     args,
		valueType: valueType,
	}, nil
}

func (u *unnestWithOrdinalityTableFunction) Name() string {
	return "doltgres_unnest_with_ordinality"
}

func (u *unnestWithOrdinalityTableFunction) String() string {
	args := make([]string, len(u.exprs))
	for i, expr := range u.exprs {
		args[i] = expr.String()
	}
	return fmt.Sprintf("%s(%s)", u.Name(), strings.Join(args, ", "))
}

func (u *unnestWithOrdinalityTableFunction) Resolved() bool {
	for _, expr := range u.exprs {
		if !expr.Resolved() {
			return false
		}
	}
	return true
}

func (u *unnestWithOrdinalityTableFunction) Expressions() []sql.Expression {
	return u.exprs
}

func (u *unnestWithOrdinalityTableFunction) WithExpressions(ctx *sql.Context, exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(u, len(exprs), 1)
	}
	nu := *u
	nu.exprs = exprs
	if doltgresType, ok := exprs[0].Type(ctx).(*pgtypes.DoltgresType); ok && doltgresType.IsArrayType() {
		nu.valueType = doltgresType.ArrayBaseType()
	}
	return &nu, nil
}

func (u *unnestWithOrdinalityTableFunction) Database() sql.Database {
	return u.db
}

func (u *unnestWithOrdinalityTableFunction) WithDatabase(db sql.Database) (sql.Node, error) {
	nu := *u
	nu.db = db
	return &nu, nil
}

func (u *unnestWithOrdinalityTableFunction) IsReadOnly() bool {
	return true
}

func (u *unnestWithOrdinalityTableFunction) Schema(ctx *sql.Context) sql.Schema {
	var dbName string
	if u.db != nil {
		dbName = u.db.Name()
	}
	valueType := u.valueType
	if valueType == nil {
		valueType = pgtypes.AnyElement
	}
	return sql.Schema{
		&sql.Column{
			DatabaseSource: dbName,
			Source:         u.Name(),
			Name:           "value",
			Type:           valueType,
			Nullable:       true,
		},
		&sql.Column{
			DatabaseSource: dbName,
			Source:         u.Name(),
			Name:           "ordinality",
			Type:           pgtypes.Int64,
			Nullable:       false,
		},
	}
}

func (u *unnestWithOrdinalityTableFunction) Children() []sql.Node {
	return nil
}

func (u *unnestWithOrdinalityTableFunction) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(u, len(children), 0)
	}
	return u, nil
}

func (u *unnestWithOrdinalityTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	value, err := u.exprs[0].Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return sql.RowsToRowIter(), nil
	}
	values, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("%s expected array argument, got %T", u.Name(), value)
	}
	return &unnestWithOrdinalityRowIter{values: values}, nil
}

func (u *unnestWithOrdinalityTableFunction) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (u *unnestWithOrdinalityTableFunction) Collation() sql.CollationID {
	return sql.Collation_Default
}

type unnestWithOrdinalityRowIter struct {
	values []any
	idx    int
}

var _ sql.RowIter = (*unnestWithOrdinalityRowIter)(nil)

func (u *unnestWithOrdinalityRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if u.idx >= len(u.values) {
		return nil, io.EOF
	}
	value := u.values[u.idx]
	u.idx++
	return sql.Row{value, int64(u.idx)}, nil
}

func (u *unnestWithOrdinalityRowIter) Close(ctx *sql.Context) error {
	return nil
}
