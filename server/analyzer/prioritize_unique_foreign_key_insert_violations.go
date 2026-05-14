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

	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// PrioritizeUniqueForeignKeyInsertViolations makes FK-checked INSERTs report
// duplicate-key errors before foreign-key errors, matching PostgreSQL.
func PrioritizeUniqueForeignKeyInsertViolations(ctx *sql.Context, a *analyzer.Analyzer, node sql.Node, scope *plan.Scope, selector analyzer.RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	insert, ok := node.(*plan.InsertInto)
	if !ok {
		return node, transform.SameTree, nil
	}
	fkHandler, ok := insert.Destination.(*plan.ForeignKeyHandler)
	if !ok {
		return node, transform.SameTree, nil
	}
	wrapped, changed, err := pgnodes.WrapPostgresForeignKeyInsertHandler(ctx, fkHandler)
	if err != nil {
		return nil, transform.NewTree, err
	}
	if !changed {
		return node, transform.SameTree, nil
	}
	next, err := insert.WithChildren(ctx, wrapped)
	if err != nil {
		return nil, transform.NewTree, err
	}
	return next, transform.NewTree, nil
}
