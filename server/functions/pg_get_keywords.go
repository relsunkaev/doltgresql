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

	"github.com/dolthub/doltgresql/postgres/parser/lex"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func initPgGetKeywords() {
	dtablefunctions.DoltTableFunctions = append(dtablefunctions.DoltTableFunctions,
		newPgGetKeywordsTableFunction("pg_get_keywords"),
		newPgGetKeywordsTableFunction(qualifiedPgCatalogFunctionName("pg_get_keywords")),
	)
}

type pgGetKeywordsTableFunction struct {
	db   sql.Database
	name string
}

func newPgGetKeywordsTableFunction(name string) *pgGetKeywordsTableFunction {
	return &pgGetKeywordsTableFunction{name: name}
}

var _ sql.TableFunction = (*pgGetKeywordsTableFunction)(nil)
var _ sql.ExecSourceRel = (*pgGetKeywordsTableFunction)(nil)

func (p *pgGetKeywordsTableFunction) NewInstance(ctx *sql.Context, db sql.Database, args []sql.Expression) (sql.Node, error) {
	if len(args) != 0 {
		return nil, sql.ErrInvalidArgumentNumber.New(p.Name(), 0, len(args))
	}
	return &pgGetKeywordsTableFunction{db: db, name: p.name}, nil
}

func (p *pgGetKeywordsTableFunction) Name() string {
	return p.name
}

func (p *pgGetKeywordsTableFunction) String() string {
	return fmt.Sprintf("%s()", p.Name())
}

func (p *pgGetKeywordsTableFunction) Resolved() bool {
	return true
}

func (p *pgGetKeywordsTableFunction) Expressions() []sql.Expression {
	return nil
}

func (p *pgGetKeywordsTableFunction) WithExpressions(ctx *sql.Context, exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(exprs), 0)
	}
	return p, nil
}

func (p *pgGetKeywordsTableFunction) Database() sql.Database {
	return p.db
}

func (p *pgGetKeywordsTableFunction) WithDatabase(db sql.Database) (sql.Node, error) {
	np := *p
	np.db = db
	return &np, nil
}

func (p *pgGetKeywordsTableFunction) IsReadOnly() bool {
	return true
}

func (p *pgGetKeywordsTableFunction) Schema(ctx *sql.Context) sql.Schema {
	var dbName string
	if p.db != nil {
		dbName = p.db.Name()
	}
	return sql.Schema{
		&sql.Column{DatabaseSource: dbName, Source: p.Name(), Name: "word", Type: pgtypes.Name, Nullable: false},
		&sql.Column{DatabaseSource: dbName, Source: p.Name(), Name: "catcode", Type: pgtypes.InternalChar, Nullable: false},
		&sql.Column{DatabaseSource: dbName, Source: p.Name(), Name: "barelabel", Type: pgtypes.Bool, Nullable: false},
		&sql.Column{DatabaseSource: dbName, Source: p.Name(), Name: "catdesc", Type: pgtypes.Text, Nullable: false},
	}
}

func (p *pgGetKeywordsTableFunction) Children() []sql.Node {
	return nil
}

func (p *pgGetKeywordsTableFunction) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 0)
	}
	return p, nil
}

func (p *pgGetKeywordsTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return &pgGetKeywordsRowIter{}, nil
}

func (p *pgGetKeywordsTableFunction) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (p *pgGetKeywordsTableFunction) Collation() sql.CollationID {
	return sql.Collation_Default
}

type pgGetKeywordsRowIter struct {
	idx int
}

var _ sql.RowIter = (*pgGetKeywordsRowIter)(nil)

func (p *pgGetKeywordsRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if p.idx >= len(lex.KeywordNames) {
		return nil, io.EOF
	}
	word := lex.KeywordNames[p.idx]
	p.idx++
	catcode := lex.KeywordsCategories[word]
	return sql.Row{word, catcode, keywordCanBeBareLabel(catcode), keywordCategoryDescription(catcode)}, nil
}

func (p *pgGetKeywordsRowIter) Close(ctx *sql.Context) error {
	return nil
}

func keywordCategoryDescription(catcode string) string {
	switch catcode {
	case "U":
		return "unreserved"
	case "C":
		return "unreserved (cannot be function or type name)"
	case "T":
		return "reserved (can be function or type name)"
	case "R":
		return "reserved"
	default:
		return ""
	}
}

func keywordCanBeBareLabel(catcode string) bool {
	return catcode != "R"
}
