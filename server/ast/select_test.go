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

package ast

import (
	"testing"

	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/parser"
)

func TestSetOpBareColumnKeepsOutputAlias(t *testing.T) {
	stmt, err := parser.ParseOne(`
WITH RECURSIVE r AS (
	SELECT issue_id FROM seed
	UNION ALL
	SELECT r.issue_id FROM r
)
SELECT * FROM r;
`)
	if err != nil {
		t.Fatal(err)
	}
	converted, err := Convert(stmt)
	if err != nil {
		t.Fatal(err)
	}

	selectStmt := converted.(*vitess.Select)
	cte := selectStmt.With.Ctes[0]
	setOp := cte.AliasedTableExpr.Expr.(*vitess.Subquery).Select.(*vitess.SetOp)
	left := setOp.Left.(*vitess.Select)
	aliased := left.SelectExprs[0].(*vitess.AliasedExpr)
	if got := aliased.As.String(); got != "issue_id" {
		t.Fatalf("got alias %q, want issue_id", got)
	}
}
