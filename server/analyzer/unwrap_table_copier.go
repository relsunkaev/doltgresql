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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	pgnode "github.com/dolthub/doltgresql/server/node"
)

// UnwrapTableCopierCreateTable keeps CREATE TABLE AS SELECT destinations in
// the raw go-mysql-server shape that TableCopier's executor expects.
func UnwrapTableCopierCreateTable(ctx *sql.Context, a *analyzer.Analyzer, node sql.Node, scope *plan.Scope, selector analyzer.RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	tableCopier, ok := node.(*plan.TableCopier)
	if !ok {
		return node, transform.SameTree, nil
	}
	createTable, ok := unwrapCreateTableDestination(tableCopier.Destination)
	if !ok {
		return node, transform.SameTree, nil
	}
	copied := *tableCopier
	copied.Destination = createTable
	return &copied, transform.NewTree, nil
}

func unwrapCreateTableDestination(node sql.Node) (*plan.CreateTable, bool) {
	switch node := node.(type) {
	case *plan.CreateTable:
		return node, true
	case *pgnode.CreateTable:
		return node.GMSCreateTable(), true
	case *pgnode.ContextRootFinalizer:
		return unwrapCreateTableDestination(node.Child())
	default:
		return nil, false
	}
}
