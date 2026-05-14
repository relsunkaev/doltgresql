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
)

// RebindInsertGeneratedSourceRefs fixes generated/default projection field
// metadata after GMS wraps INSERT sources. ProjectRow evaluates non-literal
// defaults against the projected destination row, so their GetField children
// must use the destination projection index and column type.
func RebindInsertGeneratedSourceRefs(ctx *sql.Context, _ *analyzer.Analyzer, node sql.Node, _ *plan.Scope, _ analyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(ctx, node, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		return rebindInsertGeneratedSourceRefs(ctx, node)
	})
}

func rebindInsertGeneratedSourceRefs(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
	insertInto, ok := node.(*plan.InsertInto)
	if !ok {
		return node, transform.SameTree, nil
	}

	sourceProject, ok := insertInto.Source.(*plan.Project)
	if !ok {
		return node, transform.SameTree, nil
	}

	destinationSchema := insertInto.Destination.Schema(ctx)
	if len(sourceProject.Projections) != len(destinationSchema) {
		return node, transform.SameTree, nil
	}

	destinationColumns := make(map[string]insertProjectionColumn, len(destinationSchema))
	for i, column := range destinationSchema {
		destinationColumns[strings.ToLower(column.Name)] = insertProjectionColumn{
			index:  i,
			column: column,
		}
	}

	projections := make([]sql.Expression, len(sourceProject.Projections))
	changed := false
	for i, projection := range sourceProject.Projections {
		if !isGeneratedOrDefaultProjection(projection) {
			projections[i] = projection
			continue
		}

		nextProjection, same, err := rebindInsertProjectionGetFields(ctx, projection, destinationColumns)
		if err != nil {
			return node, transform.SameTree, err
		}
		if same == transform.NewTree {
			changed = true
		}
		projections[i] = nextProjection
	}
	if !changed {
		return node, transform.SameTree, nil
	}

	nextSource, err := sourceProject.WithExpressions(ctx, projections...)
	if err != nil {
		return node, transform.SameTree, err
	}
	return insertInto.WithSource(nextSource), transform.NewTree, nil
}

type insertProjectionColumn struct {
	index  int
	column *sql.Column
}

func isGeneratedOrDefaultProjection(projection sql.Expression) bool {
	if wrapper, ok := projection.(*expression.Wrapper); ok {
		projection = wrapper.Unwrap()
	}
	defaultValue, ok := projection.(*sql.ColumnDefaultValue)
	return ok && defaultValue != nil && !defaultValue.IsLiteral()
}

func rebindInsertProjectionGetFields(ctx *sql.Context, projection sql.Expression, destinationColumns map[string]insertProjectionColumn) (sql.Expression, transform.TreeIdentity, error) {
	return transform.Expr(ctx, projection, func(ctx *sql.Context, expr sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		getField, ok := expr.(*expression.GetField)
		if !ok {
			return expr, transform.SameTree, nil
		}
		destinationColumn, ok := destinationColumns[strings.ToLower(getField.Name())]
		if !ok {
			return expr, transform.SameTree, nil
		}
		if destinationColumn.index == getField.Index() && destinationColumn.column.Type.Equals(getField.Type(ctx)) {
			return expr, transform.SameTree, nil
		}
		return expression.NewGetFieldWithTable(
			destinationColumn.index,
			int(getField.TableID()),
			destinationColumn.column.Type,
			getField.Database(),
			getField.Table(),
			getField.Name(),
			destinationColumn.column.Nullable,
		), transform.NewTree, nil
	})
}
