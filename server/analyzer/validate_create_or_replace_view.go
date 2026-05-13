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

package analyzer

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	gmsanalyzer "github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/postgres/parser/parser"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	pgast "github.com/dolthub/doltgresql/server/ast"
)

// ValidateCreateOrReplaceView rejects PostgreSQL-incompatible view replacement
// shapes before the generic GMS CREATE VIEW executor drops the existing view.
func ValidateCreateOrReplaceView(ctx *sql.Context, a *gmsanalyzer.Analyzer, n sql.Node, _ *plan.Scope, _ gmsanalyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	createView, ok := n.(*plan.CreateView)
	if !ok || !createView.IsReplace {
		return n, transform.SameTree, nil
	}
	if err := validateCreateOrReplaceView(ctx, a, createView); err != nil {
		return nil, transform.SameTree, err
	}
	return n, transform.SameTree, nil
}

func validateCreateOrReplaceView(ctx *sql.Context, a *gmsanalyzer.Analyzer, createView *plan.CreateView) error {
	existingSchema, exists, err := existingViewTargetSchema(ctx, a, createView)
	if err != nil || !exists {
		return err
	}
	return validateViewReplacementSchema(existingSchema, createView.TargetSchema())
}

func existingViewTargetSchema(ctx *sql.Context, a *gmsanalyzer.Analyzer, createView *plan.CreateView) (sql.Schema, bool, error) {
	if viewDB, ok := createView.Database().(sql.ViewDatabase); ok {
		view, exists, err := viewDB.GetViewDefinition(ctx, createView.Name)
		if err != nil || !exists {
			return nil, exists, err
		}
		targetSchema, err := viewDefinitionTargetSchema(ctx, a, view)
		return targetSchema, true, err
	}

	view, exists := ctx.GetViewRegistry().View(createView.Database().Name(), createView.Name)
	if !exists {
		return nil, false, nil
	}
	return view.Definition().Schema(ctx), true, nil
}

func viewDefinitionTargetSchema(ctx *sql.Context, a *gmsanalyzer.Analyzer, view sql.ViewDefinition) (sql.Schema, error) {
	createViewStatement := strings.TrimSpace(view.CreateViewStatement)
	if createViewStatement == "" {
		createViewStatement = "CREATE VIEW " + view.Name + " AS " + view.TextDefinition
	}
	parsedStatements, err := parser.Parse(createViewStatement)
	if err != nil {
		return nil, err
	}
	if len(parsedStatements) == 0 {
		return nil, sql.ErrViewCreateStatementInvalid.New(createViewStatement)
	}
	convertedStatement, err := pgast.Convert(parsedStatements[0])
	if err != nil {
		return nil, err
	}
	if convertedStatement == nil {
		return nil, sql.ErrViewCreateStatementInvalid.New(createViewStatement)
	}
	builder := planbuilder.New(ctx, a.Catalog, nil)
	node, _, err := builder.BindOnly(convertedStatement, createViewStatement, nil)
	if err != nil {
		return nil, err
	}
	createView, ok := node.(*plan.CreateView)
	if !ok {
		return nil, sql.ErrViewCreateStatementInvalid.New(createViewStatement)
	}
	return createView.TargetSchema(), nil
}

func validateViewReplacementSchema(existingSchema sql.Schema, replacementSchema sql.Schema) error {
	if len(replacementSchema) < len(existingSchema) {
		return pgerror.New(pgcode.InvalidTableDefinition, "cannot drop columns from view")
	}
	for i, existingCol := range existingSchema {
		replacementCol := replacementSchema[i]
		existingName := comparableViewColumnName(existingCol.Name)
		replacementName := comparableViewColumnName(replacementCol.Name)
		if existingName != replacementName {
			return pgerror.Newf(
				pgcode.InvalidTableDefinition,
				`cannot change name of view column "%s" to "%s"`,
				existingName,
				replacementName,
			)
		}
		if !existingCol.Type.Equals(replacementCol.Type) {
			return pgerror.Newf(
				pgcode.InvalidTableDefinition,
				`cannot change data type of view column "%s" from %s to %s`,
				existingName,
				existingCol.Type.String(),
				replacementCol.Type.String(),
			)
		}
	}
	return nil
}

func comparableViewColumnName(name string) string {
	name = core.DecodePhysicalColumnName(name)
	if displayName, ok := pgast.AnonColumnAliasDisplayName(name); ok {
		return displayName
	}
	return name
}
