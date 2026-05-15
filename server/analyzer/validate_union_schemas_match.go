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

	"github.com/dolthub/go-mysql-server/sql"
	gmsanalyzer "github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/analyzer/analyzererrors"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// ValidateUnionSchemasMatch is the Doltgres replacement for the GMS set-op
// validation rule. GMS requires each input column type to be exactly equal,
// but plan.SetOp.Schema already computes the common output type. Keep the
// arity check here and leave type reconciliation to the set-op schema.
func ValidateUnionSchemasMatch(ctx *sql.Context, _ *gmsanalyzer.Analyzer, node sql.Node, _ *plan.Scope, _ gmsanalyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("validate_union_schemas_match")
	defer span.End()

	var firstMismatch []string
	transform.InspectWithOpaque(ctx, node, func(ctx *sql.Context, node sql.Node) bool {
		setOp, ok := node.(*plan.SetOp)
		if !ok {
			return true
		}

		leftSchema := setOp.Left().Schema(ctx)
		rightSchema := setOp.Right().Schema(ctx)
		if len(leftSchema) != len(rightSchema) {
			firstMismatch = []string{
				fmt.Sprintf("%d columns", len(leftSchema)),
				fmt.Sprintf("%d columns", len(rightSchema)),
			}
			return false
		}
		return true
	})
	if firstMismatch != nil {
		return nil, transform.SameTree, analyzererrors.ErrUnionSchemasMatch.New(firstMismatch[0], firstMismatch[1])
	}
	return node, transform.SameTree, nil
}
