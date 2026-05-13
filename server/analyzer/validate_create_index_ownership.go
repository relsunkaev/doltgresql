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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/doltgresql/core"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

func validateCreateIndexOwnership(ctx *sql.Context, _ *analyzer.Analyzer, n sql.Node, _ *plan.Scope, _ analyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(ctx, n, func(ctx *sql.Context, n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		alterIndex, ok := n.(*plan.AlterIndex)
		if !ok || alterIndex.Action != plan.IndexAction_Create {
			return n, transform.SameTree, nil
		}
		resolvedTable, ok := alterIndex.Table.(*plan.ResolvedTable)
		if !ok {
			return n, transform.SameTree, nil
		}
		schemaName, err := core.GetSchemaName(ctx, resolvedTable.Database(), "")
		if err != nil {
			return nil, transform.SameTree, err
		}
		return pgnodes.NewTableOwnershipCheck(n, doltdb.TableName{Schema: schemaName, Name: resolvedTable.Name()}), transform.NewTree, nil
	})
}
