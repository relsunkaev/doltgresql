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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/doltgresql/server/ast"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// resolveDropColumnIfExists rewrites the plan tree to implement PostgreSQL's
// ALTER TABLE ... DROP COLUMN IF EXISTS semantics. The AST translator marks
// such columns with ast.EncodeDropColumnIfExists; this rule strips the marker
// and either keeps the DropColumn (when the column exists in the resolved
// table's schema) or replaces it with a NoOp that emits the matching
// "column ... does not exist, skipping" NOTICE.
//
// Running first in the analyzer pipeline ensures that no later validator sees
// the marker or fires "column not found" on a missing IF EXISTS target.
func resolveDropColumnIfExists(ctx *sql.Context, _ *analyzer.Analyzer, n sql.Node, _ *plan.Scope, _ analyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(ctx, n, func(_ *sql.Context, n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		dc, ok := n.(*plan.DropColumn)
		if !ok {
			return n, transform.SameTree, nil
		}
		columnName, hadMarker := ast.DecodeDropColumnIfExists(dc.Column)
		if !hadMarker {
			return n, transform.SameTree, nil
		}

		rt, ok := dc.Table.(*plan.ResolvedTable)
		if !ok {
			// Table didn't resolve to *plan.ResolvedTable, which means a later
			// rule will surface a table-resolution error. Strip the marker so
			// the column name in that downstream error stays human-readable.
			stripped := *dc
			stripped.Column = columnName
			return &stripped, transform.NewTree, nil
		}

		if columnExistsInSchema(rt.Schema(ctx), columnName) {
			stripped := *dc
			stripped.Column = columnName
			return &stripped, transform.NewTree, nil
		}

		return pgnodes.NoOp{
			Severity: "NOTICE",
			Warnings: []string{
				fmt.Sprintf(`column "%s" of relation "%s" does not exist, skipping`, columnName, rt.Name()),
			},
		}, transform.NewTree, nil
	})
}

// columnExistsInSchema reports whether sch contains a column whose name matches
// |name| under PostgreSQL's case-insensitive comparison for unquoted
// identifiers. PostgreSQL lower-cases unquoted identifiers at parse time, so a
// case-insensitive match here is consistent with the rest of the engine.
func columnExistsInSchema(sch sql.Schema, name string) bool {
	for _, col := range sch {
		if strings.EqualFold(col.Name, name) {
			return true
		}
	}
	return false
}
