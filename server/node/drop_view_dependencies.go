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

package node

import (
	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/postgres/parser/parser"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/tablemetadata"
)

type cascadeDependentView struct {
	relation     doltdb.TableName
	db           sql.Database
	normalView   bool
	materialized bool
}

func dropCascadeDependentViews(ctx *sql.Context, relations []doltdb.TableName) ([]dropViewTarget, []dropTableTarget, error) {
	dependents, err := cascadeDependentViews(ctx, relations)
	if err != nil {
		return nil, nil, err
	}

	viewTargets := make([]dropViewTarget, 0)
	tableTargets := make([]dropTableTarget, 0)
	for _, dependent := range dependents {
		switch {
		case dependent.normalView:
			viewID := id.NewView(dependent.relation.Schema, dependent.relation.Name).AsId()
			if err = id.ValidateOperation(ctx, id.Section_View, id.Operation_Delete, dependent.db.Name(), viewID, id.Null); err != nil {
				return nil, nil, err
			}
			viewDB, ok := dependent.db.(sql.ViewDatabase)
			if !ok {
				return nil, nil, errors.Errorf("database `%s` does not support dropping views", dependent.db.Name())
			}
			if err = viewDB.DropView(ctx, dependent.relation.Name); err != nil {
				return nil, nil, err
			}
			viewTargets = append(viewTargets, dropViewTarget{
				dbName:   dependent.db.Name(),
				viewID:   viewID,
				relation: dependent.relation,
			})
		case dependent.materialized:
			tableID := id.NewTable(dependent.relation.Schema, dependent.relation.Name).AsId()
			if err = id.ValidateOperation(ctx, id.Section_Table, id.Operation_Delete, dependent.db.Name(), tableID, id.Null); err != nil {
				return nil, nil, err
			}
			dropper, ok := dependent.db.(sql.TableDropper)
			if !ok {
				return nil, nil, sql.ErrDropTableNotSupported.New(dependent.db.Name())
			}
			if err = dropper.DropTable(ctx, dependent.relation.Name); err != nil {
				return nil, nil, err
			}
			tableTargets = append(tableTargets, dropTableTarget{
				dbName:   dependent.db.Name(),
				tableID:  tableID,
				relation: dependent.relation,
			})
		}
	}
	return viewTargets, tableTargets, nil
}

func rejectDependentViews(ctx *sql.Context, relations []doltdb.TableName) error {
	dependents, err := cascadeDependentViews(ctx, relations)
	if err != nil {
		return err
	}
	if len(dependents) == 0 {
		return nil
	}
	return pgerror.Newf(pgcode.DependentObjectsStillExist, "cannot drop relation %s because other objects depend on it", relations[0].Name)
}

func cascadeDependentViews(ctx *sql.Context, relations []doltdb.TableName) ([]cascadeDependentView, error) {
	if len(relations) == 0 {
		return nil, nil
	}
	graph, err := dependentViewsByReferencedRelation(ctx)
	if err != nil {
		return nil, err
	}
	explicit := make(map[dropRelationKey]struct{}, len(relations))
	for _, relation := range relations {
		explicit[newDropRelationKey(relation.Schema, relation.Name)] = struct{}{}
	}

	var dependents []cascadeDependentView
	emitted := make(map[dropRelationKey]struct{})
	for _, relation := range relations {
		key := newDropRelationKey(relation.Schema, relation.Name)
		dependents = append(dependents, collectCascadeDependentViews(key, graph, explicit, emitted, map[dropRelationKey]struct{}{})...)
	}
	return dependents, nil
}

func collectCascadeDependentViews(
	relation dropRelationKey,
	graph map[dropRelationKey][]cascadeDependentView,
	explicit map[dropRelationKey]struct{},
	emitted map[dropRelationKey]struct{},
	visiting map[dropRelationKey]struct{},
) []cascadeDependentView {
	var ret []cascadeDependentView
	for _, dependent := range graph[relation] {
		dependentKey := newDropRelationKey(dependent.relation.Schema, dependent.relation.Name)
		if _, ok := visiting[dependentKey]; ok {
			continue
		}
		visiting[dependentKey] = struct{}{}
		ret = append(ret, collectCascadeDependentViews(dependentKey, graph, explicit, emitted, visiting)...)
		delete(visiting, dependentKey)
		if _, ok := explicit[dependentKey]; ok {
			continue
		}
		if _, ok := emitted[dependentKey]; ok {
			continue
		}
		emitted[dependentKey] = struct{}{}
		ret = append(ret, dependent)
	}
	return ret
}

func dependentViewsByReferencedRelation(ctx *sql.Context) (map[dropRelationKey][]cascadeDependentView, error) {
	graph := make(map[dropRelationKey][]cascadeDependentView)
	err := functions.IterateCurrentDatabase(ctx, functions.Callbacks{
		View: func(ctx *sql.Context, schema functions.ItemSchema, view functions.ItemView) (cont bool, err error) {
			relation := doltdb.TableName{Schema: schema.Item.SchemaName(), Name: view.Item.Name}
			dependent := cascadeDependentView{
				relation:   relation,
				db:         schema.Item,
				normalView: true,
			}
			refs, err := relationReferencesFromDefinition(viewDefinitionSQL(view.Item), relation.Schema)
			if err != nil {
				return false, err
			}
			for ref := range refs {
				graph[ref] = append(graph[ref], dependent)
			}
			return true, nil
		},
		Table: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable) (cont bool, err error) {
			if !tablemetadata.IsMaterializedView(tableComment(table.Item)) {
				return true, nil
			}
			relation := doltdb.TableName{Schema: schema.Item.SchemaName(), Name: table.Item.Name()}
			dependent := cascadeDependentView{
				relation:     relation,
				db:           schema.Item,
				materialized: true,
			}
			refs, err := relationReferencesFromDefinition(tablemetadata.MaterializedViewDefinition(tableComment(table.Item)), relation.Schema)
			if err != nil {
				return false, err
			}
			for ref := range refs {
				graph[ref] = append(graph[ref], dependent)
			}
			return true, nil
		},
	})
	return graph, err
}

func viewDefinitionSQL(view sql.ViewDefinition) string {
	if view.CreateViewStatement != "" {
		return view.CreateViewStatement
	}
	return view.TextDefinition
}

type dropRelationKey struct {
	schema string
	name   string
}

func newDropRelationKey(schema string, name string) dropRelationKey {
	if schema == "" {
		schema = "public"
	}
	return dropRelationKey{schema: schema, name: name}
}

func relationReferencesFromDefinition(definition string, defaultSchema string) (map[dropRelationKey]struct{}, error) {
	refs := make(map[dropRelationKey]struct{})
	if definition == "" {
		return refs, nil
	}
	statements, err := parser.Parse(definition)
	if err != nil || len(statements) == 0 {
		return refs, err
	}
	switch stmt := statements[0].AST.(type) {
	case *tree.CreateView:
		collectSelectRelationReferences(stmt.AsSource, defaultSchema, refs)
	case *tree.CreateMaterializedView:
		collectSelectRelationReferences(stmt.AsSource, defaultSchema, refs)
	case *tree.Select:
		collectSelectRelationReferences(stmt, defaultSchema, refs)
	}
	return refs, nil
}

func collectSelectRelationReferences(sel *tree.Select, defaultSchema string, refs map[dropRelationKey]struct{}) {
	if sel == nil {
		return
	}
	cteNames := make(map[string]struct{})
	if sel.With != nil {
		for _, cte := range sel.With.CTEList {
			cteNames[string(cte.Name.Alias)] = struct{}{}
			if cteSelect, ok := cte.Stmt.(*tree.Select); ok {
				collectSelectRelationReferences(cteSelect, defaultSchema, refs)
			}
		}
	}
	collectSelectStatementRelationReferences(sel.Select, defaultSchema, refs, cteNames)
}

func collectSelectStatementRelationReferences(stmt tree.SelectStatement, defaultSchema string, refs map[dropRelationKey]struct{}, cteNames map[string]struct{}) {
	switch s := stmt.(type) {
	case *tree.SelectClause:
		for _, tableExpr := range s.From.Tables {
			collectTableExprRelationReferences(tableExpr, defaultSchema, refs, cteNames)
		}
		collectExprsSubqueryReferences(s.Exprs, defaultSchema, refs)
		if s.Where != nil {
			collectExprSubqueryReferences(s.Where.Expr, defaultSchema, refs)
		}
		if s.Having != nil {
			collectExprSubqueryReferences(s.Having.Expr, defaultSchema, refs)
		}
	case *tree.ParenSelect:
		collectSelectRelationReferences(s.Select, defaultSchema, refs)
	case *tree.UnionClause:
		collectSelectRelationReferences(s.Left, defaultSchema, refs)
		collectSelectRelationReferences(s.Right, defaultSchema, refs)
	}
}

func collectTableExprRelationReferences(expr tree.TableExpr, defaultSchema string, refs map[dropRelationKey]struct{}, cteNames map[string]struct{}) {
	switch e := expr.(type) {
	case *tree.TableName:
		if _, ok := cteNames[e.Table()]; ok {
			return
		}
		schema := defaultSchema
		if e.ExplicitSchema {
			schema = e.Schema()
		}
		refs[newDropRelationKey(schema, e.Table())] = struct{}{}
	case *tree.AliasedTableExpr:
		collectTableExprRelationReferences(e.Expr, defaultSchema, refs, cteNames)
	case *tree.JoinTableExpr:
		collectTableExprRelationReferences(e.Left, defaultSchema, refs, cteNames)
		collectTableExprRelationReferences(e.Right, defaultSchema, refs, cteNames)
		if onCond, ok := e.Cond.(*tree.OnJoinCond); ok {
			collectExprSubqueryReferences(onCond.Expr, defaultSchema, refs)
		}
	case *tree.ParenTableExpr:
		collectTableExprRelationReferences(e.Expr, defaultSchema, refs, cteNames)
	case *tree.Subquery:
		collectSelectStatementRelationReferences(e.Select, defaultSchema, refs, cteNames)
	}
}

func collectExprsSubqueryReferences(exprs tree.SelectExprs, defaultSchema string, refs map[dropRelationKey]struct{}) {
	for _, expr := range exprs {
		collectExprSubqueryReferences(expr.Expr, defaultSchema, refs)
	}
}

func collectExprSubqueryReferences(expr tree.Expr, defaultSchema string, refs map[dropRelationKey]struct{}) {
	tree.SimpleVisit(expr, func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		if subquery, ok := expr.(*tree.Subquery); ok {
			collectSelectStatementRelationReferences(subquery.Select, defaultSchema, refs, map[string]struct{}{})
		}
		return true, expr, nil
	})
}
