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

package pgcatalog

import (
	"sort"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/postgres/parser/parser"
	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgDependName is a constant to the pg_depend name.
const PgDependName = "pg_depend"

// InitPgDepend handles registration of the pg_depend handler.
func InitPgDepend() {
	tables.AddHandler(PgCatalogName, PgDependName, PgDependHandler{})
}

// PgDependHandler is the handler for the pg_depend table.
type PgDependHandler struct{}

var _ tables.Handler = PgDependHandler{}

// Name implements the interface tables.Handler.
func (p PgDependHandler) Name() string {
	return PgDependName
}

// RowIter implements the interface tables.Handler.
func (p PgDependHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	rows, err := pgDependRows(ctx)
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(rows...), nil
}

func pgDependRows(ctx *sql.Context) ([]sql.Row, error) {
	classID := id.NewTable(PgCatalogName, PgClassName).AsId()
	relationOids := make(map[relationDependencyKey]id.Id)
	if err := functions.IterateCurrentDatabase(ctx, functions.Callbacks{
		Table: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable) (cont bool, err error) {
			relationOids[newRelationDependencyKey(schema.Item.SchemaName(), table.Item.Name())] = table.OID.AsId()
			return true, nil
		},
		View: func(ctx *sql.Context, schema functions.ItemSchema, view functions.ItemView) (cont bool, err error) {
			relationOids[newRelationDependencyKey(schema.Item.SchemaName(), view.Item.Name)] = view.OID.AsId()
			return true, nil
		},
		Sequence: func(ctx *sql.Context, schema functions.ItemSchema, sequence functions.ItemSequence) (cont bool, err error) {
			relationOids[newRelationDependencyKey(schema.Item.SchemaName(), sequence.Item.Id.SequenceName())] = sequence.OID.AsId()
			return true, nil
		},
	}); err != nil {
		return nil, err
	}

	var rows []sql.Row
	if err := functions.IterateCurrentDatabase(ctx, functions.Callbacks{
		View: func(ctx *sql.Context, schema functions.ItemSchema, view functions.ItemView) (cont bool, err error) {
			refs, err := viewRelationDependencies(view.Item, schema.Item.SchemaName(), relationOids)
			if err != nil {
				return false, err
			}
			for _, ref := range refs {
				rows = append(rows, sql.Row{
					classID,         // classid
					view.OID.AsId(), // objid
					int32(0),        // objsubid
					classID,         // refclassid
					ref,             // refobjid
					int32(0),        // refobjsubid
					"n",             // deptype
				})
			}
			return true, nil
		},
	}); err != nil {
		return nil, err
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i][0] != rows[j][0] {
			return id.Cache().ToOID(rows[i][0].(id.Id)) < id.Cache().ToOID(rows[j][0].(id.Id))
		}
		return id.Cache().ToOID(rows[i][1].(id.Id)) < id.Cache().ToOID(rows[j][1].(id.Id))
	})
	return rows, nil
}

type relationDependencyKey struct {
	schema string
	name   string
}

func newRelationDependencyKey(schema string, name string) relationDependencyKey {
	if schema == "" {
		schema = "public"
	}
	return relationDependencyKey{schema: schema, name: name}
}

func viewRelationDependencies(view sql.ViewDefinition, defaultSchema string, relationOids map[relationDependencyKey]id.Id) ([]id.Id, error) {
	createViewStatement := view.CreateViewStatement
	if createViewStatement == "" {
		createViewStatement = "CREATE VIEW " + view.Name + " AS " + view.TextDefinition
	}
	stmts, err := parser.Parse(createViewStatement)
	if err != nil || len(stmts) == 0 {
		return nil, err
	}
	createView, ok := stmts[0].AST.(*tree.CreateView)
	if !ok {
		return nil, nil
	}
	refs := make(map[id.Id]struct{})
	collectSelectRelationDependencies(createView.AsSource, defaultSchema, relationOids, refs)
	result := make([]id.Id, 0, len(refs))
	for ref := range refs {
		result = append(result, ref)
	}
	sort.Slice(result, func(i, j int) bool {
		return id.Cache().ToOID(result[i]) < id.Cache().ToOID(result[j])
	})
	return result, nil
}

func collectSelectRelationDependencies(sel *tree.Select, defaultSchema string, relationOids map[relationDependencyKey]id.Id, refs map[id.Id]struct{}) {
	if sel == nil {
		return
	}
	cteNames := make(map[string]struct{})
	if sel.With != nil {
		for _, cte := range sel.With.CTEList {
			cteNames[string(cte.Name.Alias)] = struct{}{}
			if cteSelect, ok := cte.Stmt.(*tree.Select); ok {
				collectSelectRelationDependencies(cteSelect, defaultSchema, relationOids, refs)
			}
		}
	}
	collectSelectStatementRelationDependencies(sel.Select, defaultSchema, relationOids, refs, cteNames)
}

func collectSelectStatementRelationDependencies(stmt tree.SelectStatement, defaultSchema string, relationOids map[relationDependencyKey]id.Id, refs map[id.Id]struct{}, cteNames map[string]struct{}) {
	switch s := stmt.(type) {
	case *tree.SelectClause:
		for _, tableExpr := range s.From.Tables {
			collectTableExprRelationDependencies(tableExpr, defaultSchema, relationOids, refs, cteNames)
		}
		collectExprsSubqueryDependencies(s.Exprs, defaultSchema, relationOids, refs)
		if s.Where != nil {
			collectExprSubqueryDependencies(s.Where.Expr, defaultSchema, relationOids, refs)
		}
		if s.Having != nil {
			collectExprSubqueryDependencies(s.Having.Expr, defaultSchema, relationOids, refs)
		}
	case *tree.ParenSelect:
		collectSelectRelationDependencies(s.Select, defaultSchema, relationOids, refs)
	case *tree.UnionClause:
		collectSelectRelationDependencies(s.Left, defaultSchema, relationOids, refs)
		collectSelectRelationDependencies(s.Right, defaultSchema, relationOids, refs)
	}
}

func collectTableExprRelationDependencies(expr tree.TableExpr, defaultSchema string, relationOids map[relationDependencyKey]id.Id, refs map[id.Id]struct{}, cteNames map[string]struct{}) {
	switch e := expr.(type) {
	case *tree.TableName:
		if _, ok := cteNames[e.Table()]; ok {
			return
		}
		schema := defaultSchema
		if e.ExplicitSchema {
			schema = e.Schema()
		}
		if oid, ok := relationOids[newRelationDependencyKey(schema, e.Table())]; ok {
			refs[oid] = struct{}{}
		}
	case *tree.AliasedTableExpr:
		collectTableExprRelationDependencies(e.Expr, defaultSchema, relationOids, refs, cteNames)
	case *tree.JoinTableExpr:
		collectTableExprRelationDependencies(e.Left, defaultSchema, relationOids, refs, cteNames)
		collectTableExprRelationDependencies(e.Right, defaultSchema, relationOids, refs, cteNames)
		if onCond, ok := e.Cond.(*tree.OnJoinCond); ok {
			collectExprSubqueryDependencies(onCond.Expr, defaultSchema, relationOids, refs)
		}
	case *tree.ParenTableExpr:
		collectTableExprRelationDependencies(e.Expr, defaultSchema, relationOids, refs, cteNames)
	case *tree.Subquery:
		collectSelectStatementRelationDependencies(e.Select, defaultSchema, relationOids, refs, cteNames)
	}
}

func collectExprsSubqueryDependencies(exprs tree.SelectExprs, defaultSchema string, relationOids map[relationDependencyKey]id.Id, refs map[id.Id]struct{}) {
	for _, expr := range exprs {
		collectExprSubqueryDependencies(expr.Expr, defaultSchema, relationOids, refs)
	}
}

func collectExprSubqueryDependencies(expr tree.Expr, defaultSchema string, relationOids map[relationDependencyKey]id.Id, refs map[id.Id]struct{}) {
	tree.SimpleVisit(expr, func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		if subquery, ok := expr.(*tree.Subquery); ok {
			collectSelectStatementRelationDependencies(subquery.Select, defaultSchema, relationOids, refs, map[string]struct{}{})
		}
		return true, expr, nil
	})
}

// Schema implements the interface tables.Handler.
func (p PgDependHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgDependSchema,
		PkOrdinals: nil,
	}
}

// pgDependSchema is the schema for pg_depend.
var pgDependSchema = sql.Schema{
	{Name: "classid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgDependName},
	{Name: "objid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgDependName},
	{Name: "objsubid", Type: pgtypes.Int32, Default: nil, Nullable: false, Source: PgDependName},
	{Name: "refclassid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgDependName},
	{Name: "refobjid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgDependName},
	{Name: "refobjsubid", Type: pgtypes.Int32, Default: nil, Nullable: false, Source: PgDependName},
	{Name: "deptype", Type: pgtypes.InternalChar, Default: nil, Nullable: false, Source: PgDependName},
}
