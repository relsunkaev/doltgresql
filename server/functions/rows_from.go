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

package functions

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtablefunctions"
	"github.com/dolthub/go-mysql-server/sql"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func initRowsFrom() {
	dtablefunctions.DoltTableFunctions = append(dtablefunctions.DoltTableFunctions, &rowsFromTableFunction{})
}

var _ sql.TableFunction = (*rowsFromTableFunction)(nil)
var _ sql.ExecSourceRel = (*rowsFromTableFunction)(nil)

type rowsFromTableFunction struct {
	db    sql.Database
	exprs []sql.Expression
	items []rowsFromItem
}

type rowsFromItem struct {
	name  string
	exprs []sql.Expression
	typ   sql.Type
}

func (r *rowsFromTableFunction) NewInstance(ctx *sql.Context, db sql.Database, args []sql.Expression) (sql.Node, error) {
	items, err := rowsFromItemsFromArgs(ctx, args)
	if err != nil {
		return nil, err
	}
	return &rowsFromTableFunction{
		db:    db,
		exprs: args,
		items: items,
	}, nil
}

func (r *rowsFromTableFunction) Name() string {
	return "doltgres_rows_from"
}

func (r *rowsFromTableFunction) String() string {
	args := make([]string, len(r.exprs))
	for i, expr := range r.exprs {
		args[i] = expr.String()
	}
	return fmt.Sprintf("%s(%s)", r.Name(), strings.Join(args, ", "))
}

func (r *rowsFromTableFunction) Resolved() bool {
	for _, expr := range r.exprs {
		if !expr.Resolved() {
			return false
		}
	}
	return true
}

func (r *rowsFromTableFunction) Expressions() []sql.Expression {
	return r.exprs
}

func (r *rowsFromTableFunction) WithExpressions(ctx *sql.Context, exprs ...sql.Expression) (sql.Node, error) {
	items, err := rowsFromItemsFromArgs(ctx, exprs)
	if err != nil {
		return nil, err
	}
	nr := *r
	nr.exprs = exprs
	nr.items = items
	return &nr, nil
}

func (r *rowsFromTableFunction) Database() sql.Database {
	return r.db
}

func (r *rowsFromTableFunction) WithDatabase(db sql.Database) (sql.Node, error) {
	nr := *r
	nr.db = db
	return &nr, nil
}

func (r *rowsFromTableFunction) IsReadOnly() bool {
	return true
}

func (r *rowsFromTableFunction) Schema(ctx *sql.Context) sql.Schema {
	var dbName string
	if r.db != nil {
		dbName = r.db.Name()
	}
	schema := make(sql.Schema, len(r.items))
	for i, item := range r.items {
		valueType := item.typ
		if valueType == nil {
			valueType = pgtypes.AnyElement
		}
		schema[i] = &sql.Column{
			DatabaseSource: dbName,
			Source:         r.Name(),
			Name:           "value_" + strconv.Itoa(i+1),
			Type:           valueType,
			Nullable:       true,
		}
	}
	return schema
}

func (r *rowsFromTableFunction) Children() []sql.Node {
	return nil
}

func (r *rowsFromTableFunction) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 0)
	}
	return r, nil
}

func (r *rowsFromTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	iters := make([]sql.RowIter, len(r.items))
	for i, item := range r.items {
		iter, err := rowsFromItemRowIter(ctx, row, item)
		if err != nil {
			for _, iter := range iters[:i] {
				_ = iter.Close(ctx)
			}
			return nil, err
		}
		iters[i] = iter
	}
	return &rowsFromZipRowIter{iters: iters, done: make([]bool, len(iters))}, nil
}

func (r *rowsFromTableFunction) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (r *rowsFromTableFunction) Collation() sql.CollationID {
	return sql.Collation_Default
}

func rowsFromItemsFromArgs(ctx *sql.Context, args []sql.Expression) ([]rowsFromItem, error) {
	var items []rowsFromItem
	for i := 0; i < len(args); {
		if i+2 > len(args) {
			return nil, errors.Errorf("ROWS FROM metadata must include function name and argument count")
		}
		name, err := jsonRecordStringArg(ctx, args[i])
		if err != nil {
			return nil, err
		}
		i++
		argCountText, err := jsonRecordStringArg(ctx, args[i])
		if err != nil {
			return nil, err
		}
		i++
		argCount, err := strconv.Atoi(argCountText)
		if err != nil {
			return nil, err
		}
		if argCount < 0 || i+argCount > len(args) {
			return nil, errors.Errorf("ROWS FROM metadata has invalid argument count %d", argCount)
		}
		itemExprs := args[i : i+argCount]
		i += argCount

		item := rowsFromItem{name: name, exprs: itemExprs}
		switch name {
		case "generate_series":
			item.typ = generateSeriesValueType(ctx, itemExprs)
		case "unnest":
			if len(itemExprs) != 1 {
				return nil, errors.Errorf("ROWS FROM only supports single-array unnest items")
			}
			item.typ = unnestArrayBaseType(ctx, itemExprs[0], pgtypes.AnyElement)
		default:
			return nil, errors.Errorf("ROWS FROM does not yet support function %s", name)
		}
		items = append(items, item)
	}
	if len(items) == 0 {
		return nil, errors.Errorf("ROWS FROM requires at least one function")
	}
	return items, nil
}

func rowsFromItemRowIter(ctx *sql.Context, row sql.Row, item rowsFromItem) (sql.RowIter, error) {
	values := make([]any, len(item.exprs))
	for i, expr := range item.exprs {
		value, err := expr.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return sql.RowsToRowIter(), nil
		}
		values[i] = value
	}

	switch item.name {
	case "generate_series":
		return generateSeriesRowIter(values)
	case "unnest":
		array, ok := values[0].([]any)
		if !ok {
			return nil, errors.Errorf("unnest expected array argument, got %T", values[0])
		}
		return &unnestRowIter{values: array}, nil
	default:
		return nil, errors.Errorf("ROWS FROM does not yet support function %s", item.name)
	}
}

type rowsFromZipRowIter struct {
	iters []sql.RowIter
	done  []bool
}

var _ sql.RowIter = (*rowsFromZipRowIter)(nil)

func (r *rowsFromZipRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if rowsFromAllDone(r.done) {
		return nil, io.EOF
	}

	row := make(sql.Row, len(r.iters))
	emitted := false
	for i, iter := range r.iters {
		if r.done[i] {
			continue
		}
		next, err := iter.Next(ctx)
		if err == io.EOF {
			r.done[i] = true
			continue
		}
		if err != nil {
			return nil, err
		}
		emitted = true
		if len(next) > 0 {
			row[i] = next[0]
		}
	}
	if !emitted {
		return nil, io.EOF
	}
	return row, nil
}

func (r *rowsFromZipRowIter) Close(ctx *sql.Context) error {
	var closeErr error
	for _, iter := range r.iters {
		if err := iter.Close(ctx); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}

func rowsFromAllDone(done []bool) bool {
	for _, isDone := range done {
		if !isDone {
			return false
		}
	}
	return true
}
