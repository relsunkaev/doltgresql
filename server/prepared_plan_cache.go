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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/node"
)

func (h *ConnectionHandler) preparedPlanCacheable(query ConvertedQuery, bindVarTypes []uint32) bool {
	if len(bindVarTypes) != 0 || query.AST == nil {
		return false
	}
	switch query.AST.(type) {
	case *sqlparser.Select, *sqlparser.SetOp:
		return true
	default:
		return false
	}
}

func (h *ConnectionHandler) cachePreparedPlan(ctx *sql.Context, data *PreparedStatementData, plan sql.Node) {
	if data == nil || plan == nil || !h.preparedPlanCacheable(data.Query, data.BindVarTypes) {
		return
	}
	cachedPlan, err := planbuilder.DeepCopyNode(ctx, plan)
	if err != nil {
		logrus.WithError(err).WithField("query", data.Query.String).Debug("skipping prepared plan cache")
		return
	}
	data.cachedPlan = cachedPlan
	data.cachedPlanGeneration = h.planCacheGeneration
}

func (h *ConnectionHandler) cachedPreparedPlan(ctx *sql.Context, data PreparedStatementData) (sql.Node, bool) {
	if data.cachedPlan == nil || data.cachedPlanGeneration != h.planCacheGeneration {
		return nil, false
	}
	planCopy, err := planbuilder.DeepCopyNode(ctx, data.cachedPlan)
	if err != nil {
		logrus.WithError(err).WithField("query", data.Query.String).Debug("skipping prepared plan cache hit")
		return nil, false
	}
	return planCopy, true
}

func validatePreparedStatementResultShape(query ConvertedQuery, expected, actual []pgproto3.FieldDescription) error {
	if !queryReturnsRows(query, expected) && !queryReturnsRows(query, actual) {
		return nil
	}
	if len(expected) != len(actual) {
		return preparedStatementResultShapeChangedError()
	}
	for i := range expected {
		if expected[i].DataTypeOID != actual[i].DataTypeOID ||
			expected[i].TypeModifier != actual[i].TypeModifier {
			return preparedStatementResultShapeChangedError()
		}
	}
	return nil
}

func preparedStatementResultShapeChangedError() error {
	return pgerror.New(pgcode.FeatureNotSupported, "cached plan must not change result type")
}

func (h *ConnectionHandler) invalidatePreparedPlanCacheIfNeeded(query ConvertedQuery) {
	if preparedPlanCacheInvalidatingQuery(query) {
		h.planCacheGeneration++
	}
}

func preparedPlanCacheInvalidatingQuery(query ConvertedQuery) bool {
	if query.AST == nil {
		return false
	}
	switch stmt := query.AST.(type) {
	case *sqlparser.AlterTable, *sqlparser.Analyze, *sqlparser.DBDDL, *sqlparser.DDL,
		*sqlparser.Commit, *sqlparser.Delete, *sqlparser.Insert, *sqlparser.Rollback, *sqlparser.RollbackSavepoint,
		*sqlparser.Set, *sqlparser.Update, *sqlparser.Use:
		return true
	case *sqlparser.Select:
		// A bare SELECT is not invalidating, but SELECT calls that mutate
		// the session's working set (DOLT_CHECKOUT, DOLT_REVERT, DOLT_MERGE,
		// DOLT_RESET, DOLT_BRANCH, DOLT_COMMIT) leave cached plans pointing
		// at the prior branch state. Walk the projection list looking for
		// these calls.
		return selectMutatesWorkingSet(stmt)
	case sqlparser.InjectedStatement:
		switch stmt.Statement.(type) {
		case node.PrepareStatement, node.ExecuteStatement:
			return false
		default:
			return true
		}
	default:
		return false
	}
}

// workingSetMutatingFuncs lists Dolt SQL functions whose call inside a SELECT
// changes the session's branch/working-set view. Subsequent queries must not
// reuse plans cached against the prior view.
var workingSetMutatingFuncs = map[string]struct{}{
	"dolt_checkout":    {},
	"dolt_reset":       {},
	"dolt_revert":      {},
	"dolt_merge":       {},
	"dolt_commit":      {},
	"dolt_branch":      {},
	"dolt_clone":       {},
	"dolt_fetch":       {},
	"dolt_pull":        {},
	"dolt_rebase":      {},
	"dolt_cherry_pick": {},
	"dolt_clean":       {},
	"set_config":       {},

	// SET LOCAL is rewritten into this helper so it can share the
	// transaction-local GUC tracking used by set_config(..., true).
	"__doltgres_set_config_local": {},
}

func selectMutatesWorkingSet(stmt *sqlparser.Select) bool {
	mutates := false
	_ = sqlparser.Walk(func(n sqlparser.SQLNode) (kontinue bool, err error) {
		fn, ok := n.(*sqlparser.FuncExpr)
		if !ok {
			return true, nil
		}
		name := fn.Name.Lowered()
		if _, ok := workingSetMutatingFuncs[name]; ok {
			mutates = true
			return false, nil
		}
		return true, nil
	}, stmt)
	return mutates
}
