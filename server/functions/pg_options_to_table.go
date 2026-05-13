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
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtablefunctions"
	"github.com/dolthub/go-mysql-server/sql"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func initPgOptionsToTable() {
	dtablefunctions.DoltTableFunctions = append(dtablefunctions.DoltTableFunctions,
		newPgOptionsToTableFunction("pg_options_to_table"),
		newPgOptionsToTableFunction(qualifiedPgCatalogFunctionName("pg_options_to_table")),
	)
}

var _ sql.TableFunction = (*pgOptionsToTableFunction)(nil)
var _ sql.ExecSourceRel = (*pgOptionsToTableFunction)(nil)

type pgOptionsToTableFunction struct {
	db    sql.Database
	name  string
	expr  sql.Expression
	exprs []sql.Expression
}

func newPgOptionsToTableFunction(name string) *pgOptionsToTableFunction {
	return &pgOptionsToTableFunction{name: name}
}

func (p *pgOptionsToTableFunction) NewInstance(ctx *sql.Context, db sql.Database, args []sql.Expression) (sql.Node, error) {
	if len(args) != 1 {
		return nil, sql.ErrInvalidArgumentNumber.New(p.Name(), 1, len(args))
	}
	return &pgOptionsToTableFunction{db: db, name: p.name, expr: args[0], exprs: args}, nil
}

func (p *pgOptionsToTableFunction) Name() string {
	return p.name
}

func (p *pgOptionsToTableFunction) String() string {
	if p.expr == nil {
		return p.Name() + "()"
	}
	return fmt.Sprintf("%s(%s)", p.Name(), p.expr.String())
}

func (p *pgOptionsToTableFunction) Resolved() bool {
	return p.expr != nil && p.expr.Resolved()
}

func (p *pgOptionsToTableFunction) Expressions() []sql.Expression {
	return p.exprs
}

func (p *pgOptionsToTableFunction) WithExpressions(ctx *sql.Context, exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(exprs), 1)
	}
	np := *p
	np.expr = exprs[0]
	np.exprs = exprs
	return &np, nil
}

func (p *pgOptionsToTableFunction) Database() sql.Database {
	return p.db
}

func (p *pgOptionsToTableFunction) WithDatabase(db sql.Database) (sql.Node, error) {
	np := *p
	np.db = db
	return &np, nil
}

func (p *pgOptionsToTableFunction) IsReadOnly() bool {
	return true
}

func (p *pgOptionsToTableFunction) Schema(ctx *sql.Context) sql.Schema {
	var dbName string
	if p.db != nil {
		dbName = p.db.Name()
	}
	return sql.Schema{
		&sql.Column{
			DatabaseSource: dbName,
			Source:         p.Name(),
			Name:           "option_name",
			Type:           pgtypes.Text,
			Nullable:       false,
		},
		&sql.Column{
			DatabaseSource: dbName,
			Source:         p.Name(),
			Name:           "option_value",
			Type:           pgtypes.Text,
			Nullable:       true,
		},
	}
}

func (p *pgOptionsToTableFunction) Children() []sql.Node {
	return nil
}

func (p *pgOptionsToTableFunction) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 0)
	}
	return p, nil
}

func (p *pgOptionsToTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	value, err := p.expr.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return sql.RowsToRowIter(), nil
	}
	options, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("%s expected text[] argument, got %T", p.Name(), value)
	}
	return &pgOptionsToTableRowIter{options: options}, nil
}

func (p *pgOptionsToTableFunction) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (p *pgOptionsToTableFunction) Collation() sql.CollationID {
	return sql.Collation_Default
}

type pgOptionsToTableRowIter struct {
	options []any
	idx     int
}

var _ sql.RowIter = (*pgOptionsToTableRowIter)(nil)

func (p *pgOptionsToTableRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	for p.idx < len(p.options) {
		option := p.options[p.idx]
		p.idx++
		if option == nil {
			continue
		}
		parts := strings.SplitN(option.(string), "=", 2)
		if len(parts) == 1 {
			return sql.Row{parts[0], nil}, nil
		}
		return sql.Row{parts[0], parts[1]}, nil
	}
	return nil, io.EOF
}

func (p *pgOptionsToTableRowIter) Close(ctx *sql.Context) error {
	return nil
}
