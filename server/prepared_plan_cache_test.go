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

package server

import (
	"reflect"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	gmsexpression "github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/doltgresql/server/node"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func TestPreparedPlanCacheability(t *testing.T) {
	handler := &ConnectionHandler{}

	require.True(t, handler.preparedPlanCacheable(ConvertedQuery{AST: &sqlparser.Select{}}, nil))
	require.False(t, handler.preparedPlanCacheable(ConvertedQuery{AST: &sqlparser.Select{}}, []uint32{1}))
	require.False(t, handler.preparedPlanCacheable(ConvertedQuery{AST: &sqlparser.Insert{}}, nil))
	require.False(t, handler.preparedPlanCacheable(ConvertedQuery{}, nil))
}

func TestPreparedPlanCacheInvalidatingQuery(t *testing.T) {
	require.False(t, preparedPlanCacheInvalidatingQuery(ConvertedQuery{AST: &sqlparser.Select{}}))
	require.False(t, preparedPlanCacheInvalidatingQuery(ConvertedQuery{AST: sqlparser.InjectedStatement{
		Statement: node.ExecuteStatement{},
	}}))

	require.True(t, preparedPlanCacheInvalidatingQuery(ConvertedQuery{AST: &sqlparser.DDL{}}))
	require.True(t, preparedPlanCacheInvalidatingQuery(ConvertedQuery{AST: &sqlparser.Commit{}}))
	require.True(t, preparedPlanCacheInvalidatingQuery(ConvertedQuery{AST: &sqlparser.Insert{}}))
	require.True(t, preparedPlanCacheInvalidatingQuery(ConvertedQuery{AST: &sqlparser.Rollback{}}))
	require.True(t, preparedPlanCacheInvalidatingQuery(ConvertedQuery{AST: &sqlparser.RollbackSavepoint{}}))
	require.True(t, preparedPlanCacheInvalidatingQuery(ConvertedQuery{AST: &sqlparser.Set{}}))
	require.True(t, preparedPlanCacheInvalidatingQuery(ConvertedQuery{AST: &sqlparser.Select{
		SelectExprs: sqlparser.SelectExprs{
			&sqlparser.AliasedExpr{Expr: &sqlparser.FuncExpr{Name: sqlparser.NewColIdent("__doltgres_set_config_local")}},
		},
	}}))
	require.True(t, preparedPlanCacheInvalidatingQuery(ConvertedQuery{AST: sqlparser.InjectedStatement{
		Statement: node.DiscardStatement{},
	}}))
}

func TestPreparedPlanCacheCopiesAndInvalidatesByGeneration(t *testing.T) {
	ctx := sql.NewEmptyContext()
	handler := &ConnectionHandler{planCacheGeneration: 7}
	data := PreparedStatementData{
		Query: ConvertedQuery{
			String: "SELECT 1",
			AST:    &sqlparser.Select{},
		},
	}
	sourcePlan := plan.NewProject(
		[]sql.Expression{gmsexpression.NewLiteral(int32(1), pgtypes.Int32)},
		plan.NewResolvedDualTable(),
	)

	handler.cachePreparedPlan(ctx, &data, sourcePlan)
	require.NotNil(t, data.cachedPlan)
	require.Equal(t, uint64(7), data.cachedPlanGeneration)

	cachedPlan, ok := handler.cachedPreparedPlan(ctx, data)
	require.True(t, ok)
	require.NotEqual(t, reflect.ValueOf(data.cachedPlan).Pointer(), reflect.ValueOf(cachedPlan).Pointer())

	handler.planCacheGeneration++
	_, ok = handler.cachedPreparedPlan(ctx, data)
	require.False(t, ok)
}
