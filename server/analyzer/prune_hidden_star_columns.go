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
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	pgtransform "github.com/dolthub/doltgresql/server/transform"
)

func PruneHiddenStarColumns(ctx *sql.Context, _ *analyzer.Analyzer, node sql.Node, _ *plan.Scope, _ analyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return pgtransform.NodeWithOpaque(ctx, node, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		project, ok := node.(*plan.Project)
		if !ok {
			return node, transform.SameTree, nil
		}
		childSchema := project.Child.Schema(ctx)
		if !projectIncludesChildSchema(project.Projections, childSchema) {
			return node, transform.SameTree, nil
		}
		projections := make([]sql.Expression, 0, len(project.Projections))
		for _, projectedExpr := range project.Projections {
			if isHiddenStarProjection(projectedExpr, childSchema) {
				continue
			}
			projections = append(projections, projectedExpr)
		}
		if len(projections) == len(project.Projections) {
			return node, transform.SameTree, nil
		}
		newProject := plan.NewProject(projections, project.Child)
		newProject.AliasDeps = project.AliasDeps
		newProject.CanDefer = project.CanDefer
		newProject.IncludesNestedIters = project.IncludesNestedIters
		return newProject, transform.NewTree, nil
	})
}

func projectIncludesChildSchema(projections []sql.Expression, childSchema sql.Schema) bool {
	if len(childSchema) == 0 || len(projections) < len(childSchema) {
		return false
	}
	matched := make([]bool, len(childSchema))
	for _, projectedExpr := range projections {
		getField, ok := projectedExpr.(*expression.GetField)
		if !ok {
			continue
		}
		if idx, ok := schemaColumnIndex(childSchema, getField); ok {
			matched[idx] = true
		}
	}
	for _, ok := range matched {
		if !ok {
			return false
		}
	}
	return true
}

func isHiddenStarProjection(projectedExpr sql.Expression, childSchema sql.Schema) bool {
	getField, ok := projectedExpr.(*expression.GetField)
	if !ok {
		return false
	}
	idx, ok := schemaColumnIndex(childSchema, getField)
	return ok && hiddenSchemaColumnAt(childSchema, idx)
}

func schemaColumnIndex(schema sql.Schema, getField *expression.GetField) (int, bool) {
	if schemaColumnMatches(schema, getField.Index(), getField.Name()) {
		return getField.Index(), true
	}
	if schemaColumnMatches(schema, getField.Index()-1, getField.Name()) {
		return getField.Index() - 1, true
	}
	return -1, false
}

func schemaColumnMatches(schema sql.Schema, idx int, name string) bool {
	return idx >= 0 && idx < len(schema) && strings.EqualFold(schema[idx].Name, name)
}

func hiddenSchemaColumnAt(schema sql.Schema, idx int) bool {
	column := schema[idx]
	return column.Hidden || column.HiddenSystem
}
