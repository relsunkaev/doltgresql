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
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtablefunctions"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
)

var _ sql.TableFunction = (*doltgresDiffSummaryTableFunction)(nil)
var _ sql.ExecSourceRel = (*doltgresDiffSummaryTableFunction)(nil)
var _ sql.AuthorizationCheckerNode = (*doltgresDiffSummaryTableFunction)(nil)

type doltgresDiffSummaryTableFunction struct {
	inner                            sql.TableFunction
	suppressSyntheticViewRootObjects bool
}

func initDoltDiffSummaryTableFunction() {
	for i, tableFunction := range dtablefunctions.DoltTableFunctions {
		if strings.EqualFold(tableFunction.Name(), "dolt_diff_summary") {
			dtablefunctions.DoltTableFunctions[i] = &doltgresDiffSummaryTableFunction{inner: tableFunction}
			return
		}
	}
}

func (d *doltgresDiffSummaryTableFunction) NewInstance(ctx *sql.Context, db sql.Database, args []sql.Expression) (sql.Node, error) {
	node, err := d.inner.NewInstance(ctx, db, args)
	if err != nil {
		return nil, err
	}
	return wrapDoltgresDiffSummaryNode(node, !diffSummaryHasTableName(args))
}

func (d *doltgresDiffSummaryTableFunction) Name() string {
	return d.inner.Name()
}

func (d *doltgresDiffSummaryTableFunction) Resolved() bool {
	return d.inner.Resolved()
}

func (d *doltgresDiffSummaryTableFunction) IsReadOnly() bool {
	return d.inner.IsReadOnly()
}

func (d *doltgresDiffSummaryTableFunction) String() string {
	return d.inner.String()
}

func (d *doltgresDiffSummaryTableFunction) Schema(ctx *sql.Context) sql.Schema {
	return d.inner.Schema(ctx)
}

func (d *doltgresDiffSummaryTableFunction) Children() []sql.Node {
	return d.inner.Children()
}

func (d *doltgresDiffSummaryTableFunction) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	node, err := d.inner.WithChildren(ctx, children...)
	if err != nil {
		return nil, err
	}
	return wrapDoltgresDiffSummaryNode(node, d.suppressSyntheticViewRootObjects)
}

func (d *doltgresDiffSummaryTableFunction) Expressions() []sql.Expression {
	return d.inner.Expressions()
}

func (d *doltgresDiffSummaryTableFunction) WithExpressions(ctx *sql.Context, exprs ...sql.Expression) (sql.Node, error) {
	node, err := d.inner.WithExpressions(ctx, exprs...)
	if err != nil {
		return nil, err
	}
	return wrapDoltgresDiffSummaryNode(node, !diffSummaryHasTableName(exprs))
}

func (d *doltgresDiffSummaryTableFunction) Database() sql.Database {
	return d.inner.Database()
}

func (d *doltgresDiffSummaryTableFunction) WithDatabase(db sql.Database) (sql.Node, error) {
	node, err := d.inner.WithDatabase(db)
	if err != nil {
		return nil, err
	}
	return wrapDoltgresDiffSummaryNode(node, d.suppressSyntheticViewRootObjects)
}

func (d *doltgresDiffSummaryTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	rowIter, ok := d.inner.(sql.ExecSourceRel)
	if !ok {
		return nil, fmt.Errorf("%s does not implement row iteration", d.Name())
	}
	if d.suppressSyntheticViewRootObjects {
		ctx = core.WithoutSyntheticViewRootObjects(ctx)
	}
	return rowIter.RowIter(ctx, row)
}

func (d *doltgresDiffSummaryTableFunction) CheckAuth(ctx *sql.Context, checker sql.PrivilegedOperationChecker) bool {
	authNode, ok := d.inner.(sql.AuthorizationCheckerNode)
	return !ok || authNode.CheckAuth(ctx, checker)
}

func (d *doltgresDiffSummaryTableFunction) DataLength(ctx *sql.Context) (uint64, error) {
	statsTable, ok := d.inner.(interface {
		DataLength(*sql.Context) (uint64, error)
	})
	if !ok {
		return 0, nil
	}
	return statsTable.DataLength(ctx)
}

func (d *doltgresDiffSummaryTableFunction) RowCount(ctx *sql.Context) (uint64, bool, error) {
	statsTable, ok := d.inner.(interface {
		RowCount(*sql.Context) (uint64, bool, error)
	})
	if !ok {
		return 0, false, nil
	}
	return statsTable.RowCount(ctx)
}

func wrapDoltgresDiffSummaryNode(node sql.Node, suppressSyntheticViewRootObjects bool) (sql.Node, error) {
	tableFunction, ok := node.(sql.TableFunction)
	if !ok {
		return nil, fmt.Errorf("unexpected dolt_diff_summary node type: %T", node)
	}
	return &doltgresDiffSummaryTableFunction{
		inner:                            tableFunction,
		suppressSyntheticViewRootObjects: suppressSyntheticViewRootObjects,
	}, nil
}

func diffSummaryHasTableName(exprs []sql.Expression) bool {
	if len(exprs) == 0 {
		return false
	}
	if strings.Contains(exprs[0].String(), "..") {
		return len(exprs) == 2
	}
	return len(exprs) == 3
}
