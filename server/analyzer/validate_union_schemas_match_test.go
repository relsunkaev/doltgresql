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
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer/analyzererrors"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"
)

func TestValidateUnionSchemasMatchAllowsTypeGeneralization(t *testing.T) {
	ctx := sql.NewEmptyContext()
	node := plan.NewSetOp(
		plan.UnionType,
		setOpValidationProject(
			expression.NewLiteral("name", types.Text),
			expression.NewLiteral(int64(1), types.Int64),
		),
		setOpValidationProject(
			expression.NewLiteral(int64(2), types.Int64),
			expression.NewLiteral("model", types.Text),
		),
		false,
		nil,
		nil,
		nil,
	)

	_, _, err := ValidateUnionSchemasMatch(ctx, nil, node, nil, nil, nil)
	require.NoError(t, err)
}

func TestValidateUnionSchemasMatchRejectsArityMismatch(t *testing.T) {
	ctx := sql.NewEmptyContext()
	node := plan.NewSetOp(
		plan.UnionType,
		setOpValidationProject(
			expression.NewLiteral("name", types.Text),
			expression.NewLiteral(int64(1), types.Int64),
		),
		setOpValidationProject(
			expression.NewLiteral("name", types.Text),
		),
		false,
		nil,
		nil,
		nil,
	)

	_, _, err := ValidateUnionSchemasMatch(ctx, nil, node, nil, nil, nil)
	require.Error(t, err)
	require.True(t, analyzererrors.ErrUnionSchemasMatch.Is(err))
}

func setOpValidationProject(exprs ...sql.Expression) sql.Node {
	return plan.NewProject(exprs, plan.NewEmptyTableWithSchema(nil))
}
