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

package node

import (
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func validateCheckConstraintExpression(ctx *sql.Context, check *sql.CheckConstraint) error {
	if check == nil || check.Expr == nil {
		return nil
	}
	if err := validateDomainCheckExpressionText(check.Expr.String()); err != nil {
		return err
	}
	var err error
	sql.Inspect(ctx, check.Expr, func(ctx *sql.Context, expr sql.Expression) bool {
		switch expr.(type) {
		case *plan.Subquery:
			err = errors.New("cannot use subquery in check constraint")
			return false
		case sql.Aggregation:
			err = errors.New("aggregate functions are not allowed in check constraints")
			return false
		case sql.WindowAggregation:
			err = errors.New("window functions are not allowed in check constraints")
			return false
		}
		if windowExpr, ok := expr.(sql.WindowAdaptableExpression); ok && windowExpr.Window() != nil {
			err = errors.New("window functions are not allowed in check constraints")
			return false
		}
		if rowIterExpr, ok := expr.(sql.RowIterExpression); ok && rowIterExpr.ReturnsRowIter() {
			err = errors.New("set-returning functions are not allowed in check constraints")
			return false
		}
		if fn, ok := expr.(sql.FunctionExpression); ok {
			if functionErr := validateDomainCheckFunctionName(fn.FunctionName()); functionErr != nil {
				err = functionErr
				return false
			}
		}
		return true
	})
	return err
}

func validateDomainCheckExpressionText(expr string) error {
	lower := strings.ToLower(expr)
	if strings.Contains(lower, " over (") {
		return errors.New("window functions are not allowed in check constraints")
	}
	for _, name := range domainCheckAggregateFunctions {
		if containsFunctionCall(lower, name) {
			return errors.New("aggregate functions are not allowed in check constraints")
		}
	}
	for _, name := range domainCheckWindowFunctions {
		if containsFunctionCall(lower, name) {
			return errors.New("window functions are not allowed in check constraints")
		}
	}
	for _, name := range domainCheckSetReturningFunctions {
		if containsFunctionCall(lower, name) {
			return errors.New("set-returning functions are not allowed in check constraints")
		}
	}
	return nil
}

func validateDomainCheckFunctionName(name string) error {
	lower := strings.ToLower(name)
	switch {
	case stringInSlice(lower, domainCheckAggregateFunctions):
		return errors.New("aggregate functions are not allowed in check constraints")
	case stringInSlice(lower, domainCheckWindowFunctions):
		return errors.New("window functions are not allowed in check constraints")
	case stringInSlice(lower, domainCheckSetReturningFunctions):
		return errors.New("set-returning functions are not allowed in check constraints")
	default:
		return nil
	}
}

func containsFunctionCall(expr string, name string) bool {
	needle := name + "("
	start := 0
	for {
		idx := strings.Index(expr[start:], needle)
		if idx == -1 {
			return false
		}
		idx += start
		if idx == 0 || isFunctionNameBoundary(expr[idx-1]) {
			return true
		}
		start = idx + len(needle)
	}
}

func isFunctionNameBoundary(ch byte) bool {
	return !((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_')
}

func stringInSlice(value string, candidates []string) bool {
	for _, candidate := range candidates {
		if value == candidate {
			return true
		}
	}
	return false
}

var domainCheckAggregateFunctions = []string{
	"avg",
	"bit_and",
	"bit_or",
	"bit_xor",
	"bool_and",
	"bool_or",
	"count",
	"every",
	"json_agg",
	"json_object_agg",
	"max",
	"min",
	"sum",
}

var domainCheckWindowFunctions = []string{
	"cume_dist",
	"dense_rank",
	"first_value",
	"lag",
	"last_value",
	"lead",
	"ntile",
	"percent_rank",
	"rank",
	"row_number",
}

var domainCheckSetReturningFunctions = []string{
	"generate_series",
	"json_array_elements",
	"json_array_elements_text",
	"jsonb_array_elements",
	"jsonb_array_elements_text",
	"regexp_matches",
	"regexp_split_to_table",
	"string_to_table",
}
