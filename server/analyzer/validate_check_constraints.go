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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	pgnode "github.com/dolthub/doltgresql/server/node"
)

type checkConstraintColumn interface {
	sql.Expression
	sql.Nameable
	sql.Tableable
}

type checkConstraintColumnKey struct {
	table string
	name  string
}

// ValidateCheckConstraints validates CHECK expressions with PostgreSQL's rules.
//
// GMS rejects any sql.NonDeterministicExpression here. Doltgres SQL functions
// implement that interface before their overload is fully initialized, so valid
// PostgreSQL CHECK constraints that call user-defined functions are rejected too
// early. This rule keeps the expression-shape checks Doltgres needs and leaves
// function resolution to the later Doltgres function optimizer.
func ValidateCheckConstraints(ctx *sql.Context, _ *analyzer.Analyzer, n sql.Node, _ *plan.Scope, _ analyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	switch n := n.(type) {
	case *plan.CreateCheck:
		if err := validateCheckConstraint(ctx, n.Check, n.Table.Schema(ctx)); err != nil {
			return nil, transform.SameTree, err
		}
	case *plan.CreateTable:
		for _, check := range n.Checks() {
			if err := validateCheckConstraint(ctx, check, n.PkSchema().Schema); err != nil {
				return nil, transform.SameTree, err
			}
		}
	}
	return n, transform.SameTree, nil
}

func validateCheckConstraint(ctx *sql.Context, check *sql.CheckConstraint, schema sql.Schema) error {
	if check == nil || check.Expr == nil {
		return nil
	}
	if err := pgnode.ValidateCheckConstraintExpression(ctx, check); err != nil {
		return err
	}
	return validateCheckConstraintColumns(ctx, check.Expr, schema)
}

func validateCheckConstraintColumns(ctx *sql.Context, expr sql.Expression, schema sql.Schema) error {
	columns := make(map[checkConstraintColumnKey]struct{}, len(schema)*2)
	for _, col := range schema {
		if col == nil {
			continue
		}
		columns[newCheckConstraintColumnKey("", col.Name)] = struct{}{}
		columns[newCheckConstraintColumnKey(col.Source, col.Name)] = struct{}{}
	}

	var err error
	sql.Inspect(ctx, expr, func(ctx *sql.Context, expr sql.Expression) bool {
		col, ok := expr.(checkConstraintColumn)
		if !ok {
			return true
		}
		if _, ok = columns[newCheckConstraintColumnKey(col.Table(), col.Name())]; ok {
			return true
		}
		if _, ok = columns[newCheckConstraintColumnKey("", col.Name())]; ok {
			return true
		}
		err = sql.ErrTableColumnNotFound.New(col.Table(), col.Name())
		return false
	})
	return err
}

func newCheckConstraintColumnKey(table string, name string) checkConstraintColumnKey {
	return checkConstraintColumnKey{
		table: strings.ToLower(table),
		name:  strings.ToLower(name),
	}
}
