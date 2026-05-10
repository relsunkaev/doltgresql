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

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtablefunctions"
	"github.com/dolthub/go-mysql-server/sql"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func initPgCryptoTableFunctions() {
	dtablefunctions.DoltTableFunctions = append(dtablefunctions.DoltTableFunctions, &pgcryptoArmorHeadersTableFunction{})
}

var _ sql.TableFunction = (*pgcryptoArmorHeadersTableFunction)(nil)
var _ sql.ExecSourceRel = (*pgcryptoArmorHeadersTableFunction)(nil)

type pgcryptoArmorHeadersTableFunction struct {
	db    sql.Database
	expr  sql.Expression
	exprs []sql.Expression
}

func (p *pgcryptoArmorHeadersTableFunction) NewInstance(ctx *sql.Context, db sql.Database, args []sql.Expression) (sql.Node, error) {
	if len(args) != 1 {
		return nil, sql.ErrInvalidArgumentNumber.New(p.Name(), 1, len(args))
	}
	return &pgcryptoArmorHeadersTableFunction{db: db, expr: args[0], exprs: args}, nil
}

func (p *pgcryptoArmorHeadersTableFunction) Name() string {
	return "pgp_armor_headers"
}

func (p *pgcryptoArmorHeadersTableFunction) String() string {
	if p.expr == nil {
		return p.Name() + "()"
	}
	return fmt.Sprintf("%s(%s)", p.Name(), p.expr.String())
}

func (p *pgcryptoArmorHeadersTableFunction) Resolved() bool {
	return p.expr != nil && p.expr.Resolved()
}

func (p *pgcryptoArmorHeadersTableFunction) Expressions() []sql.Expression {
	return p.exprs
}

func (p *pgcryptoArmorHeadersTableFunction) WithExpressions(ctx *sql.Context, exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(exprs), 1)
	}
	np := *p
	np.expr = exprs[0]
	np.exprs = exprs
	return &np, nil
}

func (p *pgcryptoArmorHeadersTableFunction) Database() sql.Database {
	return p.db
}

func (p *pgcryptoArmorHeadersTableFunction) WithDatabase(db sql.Database) (sql.Node, error) {
	np := *p
	np.db = db
	return &np, nil
}

func (p *pgcryptoArmorHeadersTableFunction) IsReadOnly() bool {
	return true
}

func (p *pgcryptoArmorHeadersTableFunction) Schema(ctx *sql.Context) sql.Schema {
	var dbName string
	if p.db != nil {
		dbName = p.db.Name()
	}
	return sql.Schema{
		&sql.Column{
			DatabaseSource: dbName,
			Source:         p.Name(),
			Name:           "key",
			Type:           pgtypes.Text,
			Nullable:       false,
		},
		&sql.Column{
			DatabaseSource: dbName,
			Source:         p.Name(),
			Name:           "value",
			Type:           pgtypes.Text,
			Nullable:       false,
		},
	}
}

func (p *pgcryptoArmorHeadersTableFunction) Children() []sql.Node {
	return nil
}

func (p *pgcryptoArmorHeadersTableFunction) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 0)
	}
	return p, nil
}

func (p *pgcryptoArmorHeadersTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	value, err := p.expr.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	value, err = sql.UnwrapAny(ctx, value)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return sql.RowsToRowIter(), nil
	}
	armored, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("%s expected text argument, got %T", p.Name(), value)
	}
	headers, err := pgcryptoArmorHeaders(armored)
	if err != nil {
		return nil, err
	}
	return &pgcryptoArmorHeadersTableRowIter{headers: headers}, nil
}

func (p *pgcryptoArmorHeadersTableFunction) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (p *pgcryptoArmorHeadersTableFunction) Collation() sql.CollationID {
	return sql.Collation_Default
}

type pgcryptoArmorHeadersTableRowIter struct {
	headers []pgcryptoArmorHeader
	idx     int
}

var _ sql.RowIter = (*pgcryptoArmorHeadersTableRowIter)(nil)

func (p *pgcryptoArmorHeadersTableRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if p.idx >= len(p.headers) {
		return nil, io.EOF
	}
	header := p.headers[p.idx]
	p.idx++
	return sql.Row{header.key, header.value}, nil
}

func (p *pgcryptoArmorHeadersTableRowIter) Close(ctx *sql.Context) error {
	return nil
}
