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

package expression_test

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	gmsexpression "github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"

	pgexpression "github.com/dolthub/doltgresql/server/expression"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func TestFunctionDoltgresTypeSumPromotesGMSIntegerInputs(t *testing.T) {
	ctx := sql.NewEmptyContext()
	tests := []struct {
		name string
		typ  sql.Type
		want *pgtypes.DoltgresType
	}{
		{name: "int32", typ: gmstypes.Int32, want: pgtypes.Int64},
		{name: "int64", typ: gmstypes.Int64, want: pgtypes.Numeric},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			field := gmsexpression.NewGetField(0, test.typ, "row_count", false)
			sum := aggregation.NewSum(field)

			got, ok := pgexpression.FunctionDoltgresType(ctx, sum)
			require.True(t, ok)
			require.Truef(t, got.Equals(test.want), "got %s, want %s", got.String(), test.want.String())
		})
	}
}

func TestAggregationGMSCastPromotesGMSIntegerSumResult(t *testing.T) {
	ctx := sql.NewEmptyContext()
	field := gmsexpression.NewGetField(0, gmstypes.Int32, "row_count", false)
	sum := pgexpression.NewAggregationGMSCast(aggregation.NewSum(field))
	buffer, err := sum.NewBuffer(ctx)
	require.NoError(t, err)

	require.NoError(t, buffer.Update(ctx, sql.NewRow(int32(10))))
	require.NoError(t, buffer.Update(ctx, sql.NewRow(int32(12))))
	got, err := buffer.Eval(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(22), got)
}
