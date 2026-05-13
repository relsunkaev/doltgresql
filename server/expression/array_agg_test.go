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
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	gmsexpression "github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/doltgresql/server/types"
)

func TestArrayAggPreservesArrayInputsAsNestedElements(t *testing.T) {
	ctx := sql.NewEmptyContext()
	agg := &ArrayAgg{
		selectExprs: []sql.Expression{
			gmsexpression.NewGetField(0, types.Float64.ToArrayType(), "vals", false),
		},
	}
	buffer, err := agg.NewBuffer(ctx)
	require.NoError(t, err)

	require.NoError(t, buffer.Update(ctx, sql.Row{[]any{float64(1), float64(2)}}))
	require.NoError(t, buffer.Update(ctx, sql.Row{[]any{float64(3), float64(4)}}))

	got, err := buffer.Eval(ctx)
	require.NoError(t, err)
	require.Equal(t, types.ArrayValue{Elements: []any{
		[]any{float64(1), float64(2)},
		[]any{float64(3), float64(4)},
	}}, got)
}
