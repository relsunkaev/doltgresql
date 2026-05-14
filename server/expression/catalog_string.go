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

package expression

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
)

// CatalogExpressionString renders expression trees for catalog pg_node_tree text
// columns until Doltgres stores real pg_node_tree nodes.
func CatalogExpressionString(expr sql.Expression) string {
	if expr == nil {
		return ""
	}
	if binary, ok := expr.(*BinaryOperator); ok {
		children := binary.Children()
		if len(children) == 2 {
			return fmt.Sprintf(
				"(%s %s %s)",
				CatalogExpressionString(children[0]),
				binary.operator.String(),
				CatalogExpressionString(children[1]),
			)
		}
	}
	return expr.String()
}
