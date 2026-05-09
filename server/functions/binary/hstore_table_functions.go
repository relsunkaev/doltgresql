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

package binary

import (
	"fmt"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtablefunctions"
	"github.com/dolthub/go-mysql-server/sql"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

var _ sql.TableFunction = (*hstoreTextTableFunction)(nil)
var _ sql.ExecSourceRel = (*hstoreTextTableFunction)(nil)
var _ sql.TableFunction = (*hstoreEachTableFunction)(nil)
var _ sql.ExecSourceRel = (*hstoreEachTableFunction)(nil)

func initHstoreTableFunctions() {
	dtablefunctions.DoltTableFunctions = append(dtablefunctions.DoltTableFunctions,
		newHstoreTextTableFunction("skeys", false),
		newHstoreTextTableFunction("svals", true),
		&hstoreEachTableFunction{},
	)
}

func newHstoreTextTableFunction(name string, values bool) *hstoreTextTableFunction {
	return &hstoreTextTableFunction{name: name, values: values}
}

type hstoreTextTableFunction struct {
	db     sql.Database
	name   string
	values bool
	exprs  []sql.Expression
}

func (h *hstoreTextTableFunction) NewInstance(ctx *sql.Context, db sql.Database, args []sql.Expression) (sql.Node, error) {
	if len(args) != 1 {
		return nil, sql.ErrInvalidArgumentNumber.New(h.Name(), 1, len(args))
	}
	nt := *h
	nt.db = db
	nt.exprs = args
	return &nt, nil
}

func (h *hstoreTextTableFunction) Name() string {
	return h.name
}

func (h *hstoreTextTableFunction) String() string {
	if len(h.exprs) == 0 {
		return h.Name() + "()"
	}
	args := make([]string, len(h.exprs))
	for i, expr := range h.exprs {
		args[i] = expr.String()
	}
	return fmt.Sprintf("%s(%s)", h.Name(), strings.Join(args, ", "))
}

func (h *hstoreTextTableFunction) Resolved() bool {
	for _, expr := range h.exprs {
		if !expr.Resolved() {
			return false
		}
	}
	return true
}

func (h *hstoreTextTableFunction) Expressions() []sql.Expression {
	return h.exprs
}

func (h *hstoreTextTableFunction) WithExpressions(ctx *sql.Context, exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(h, len(exprs), 1)
	}
	nt := *h
	nt.exprs = exprs
	return &nt, nil
}

func (h *hstoreTextTableFunction) Database() sql.Database {
	return h.db
}

func (h *hstoreTextTableFunction) WithDatabase(db sql.Database) (sql.Node, error) {
	nt := *h
	nt.db = db
	return &nt, nil
}

func (h *hstoreTextTableFunction) IsReadOnly() bool {
	return true
}

func (h *hstoreTextTableFunction) Schema(ctx *sql.Context) sql.Schema {
	var dbName string
	if h.db != nil {
		dbName = h.db.Name()
	}
	return sql.Schema{
		&sql.Column{
			DatabaseSource: dbName,
			Source:         h.Name(),
			Name:           h.Name(),
			Type:           pgtypes.Text,
			Nullable:       h.values,
		},
	}
}

func (h *hstoreTextTableFunction) Children() []sql.Node {
	return nil
}

func (h *hstoreTextTableFunction) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(h, len(children), 0)
	}
	return h, nil
}

func (h *hstoreTextTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	value, err := h.exprs[0].Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return sql.RowsToRowIter(), nil
	}
	pairs, err := parseHstore(value.(string))
	if err != nil {
		return nil, err
	}
	keys := hstoreSortedKeys(pairs)
	if !h.values {
		return hstoreKeysRowIter(keys), nil
	}
	values := make([]*string, len(keys))
	for i, key := range keys {
		values[i] = pairs[key]
	}
	return hstoreValuesRowIter(values), nil
}

func (h *hstoreTextTableFunction) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (h *hstoreTextTableFunction) Collation() sql.CollationID {
	return sql.Collation_Default
}

type hstoreEachTableFunction struct {
	db    sql.Database
	exprs []sql.Expression
}

func (h *hstoreEachTableFunction) NewInstance(ctx *sql.Context, db sql.Database, args []sql.Expression) (sql.Node, error) {
	if len(args) != 1 {
		return nil, sql.ErrInvalidArgumentNumber.New(h.Name(), 1, len(args))
	}
	nt := *h
	nt.db = db
	nt.exprs = args
	return &nt, nil
}

func (h *hstoreEachTableFunction) Name() string {
	return "each"
}

func (h *hstoreEachTableFunction) String() string {
	if len(h.exprs) == 0 {
		return h.Name() + "()"
	}
	args := make([]string, len(h.exprs))
	for i, expr := range h.exprs {
		args[i] = expr.String()
	}
	return fmt.Sprintf("%s(%s)", h.Name(), strings.Join(args, ", "))
}

func (h *hstoreEachTableFunction) Resolved() bool {
	for _, expr := range h.exprs {
		if !expr.Resolved() {
			return false
		}
	}
	return true
}

func (h *hstoreEachTableFunction) Expressions() []sql.Expression {
	return h.exprs
}

func (h *hstoreEachTableFunction) WithExpressions(ctx *sql.Context, exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(h, len(exprs), 1)
	}
	nt := *h
	nt.exprs = exprs
	return &nt, nil
}

func (h *hstoreEachTableFunction) Database() sql.Database {
	return h.db
}

func (h *hstoreEachTableFunction) WithDatabase(db sql.Database) (sql.Node, error) {
	nt := *h
	nt.db = db
	return &nt, nil
}

func (h *hstoreEachTableFunction) IsReadOnly() bool {
	return true
}

func (h *hstoreEachTableFunction) Schema(ctx *sql.Context) sql.Schema {
	var dbName string
	if h.db != nil {
		dbName = h.db.Name()
	}
	return sql.Schema{
		&sql.Column{
			DatabaseSource: dbName,
			Source:         h.Name(),
			Name:           "key",
			Type:           pgtypes.Text,
			Nullable:       false,
		},
		&sql.Column{
			DatabaseSource: dbName,
			Source:         h.Name(),
			Name:           "value",
			Type:           pgtypes.Text,
			Nullable:       true,
		},
	}
}

func (h *hstoreEachTableFunction) Children() []sql.Node {
	return nil
}

func (h *hstoreEachTableFunction) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(h, len(children), 0)
	}
	return h, nil
}

func (h *hstoreEachTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	value, err := h.exprs[0].Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return sql.RowsToRowIter(), nil
	}
	pairs, err := parseHstore(value.(string))
	if err != nil {
		return nil, err
	}
	return hstoreEachTableRowIter(pairs), nil
}

func (h *hstoreEachTableFunction) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (h *hstoreEachTableFunction) Collation() sql.CollationID {
	return sql.Collation_Default
}
