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
	"github.com/cockroachdb/errors"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// nodeAlterDomain handles ALTER DOMAIN nodes.
func nodeAlterDomain(ctx *Context, stmt *tree.AlterDomain) (vitess.Statement, error) {
	if stmt == nil {
		return nil, nil
	}

	domainName := stmt.Name.ToTableName()
	if owner, ok := stmt.Cmd.(*tree.AlterDomainOwner); ok {
		return vitess.InjectedStatement{
			Statement: pgnodes.NewAlterTypeOwner(
				domainName.Catalog(),
				domainName.Schema(),
				domainName.Object(),
				owner.Owner,
				true,
			),
		}, nil
	}
	if rename, ok := stmt.Cmd.(*tree.AlterDomainRename); ok {
		return vitess.InjectedStatement{
			Statement: pgnodes.NewAlterTypeRename(
				domainName.Catalog(),
				domainName.Schema(),
				domainName.Object(),
				rename.NewName,
				true,
			),
		}, nil
	}
	if setSchema, ok := stmt.Cmd.(*tree.AlterDomainSetSchema); ok {
		return vitess.InjectedStatement{
			Statement: pgnodes.NewAlterTypeSetSchema(
				domainName.Catalog(),
				domainName.Schema(),
				domainName.Object(),
				setSchema.Schema,
				true,
			),
		}, nil
	}
	if setDrop, ok := stmt.Cmd.(*tree.AlterDomainSetDrop); ok {
		if setDrop.NotNull {
			if setDrop.IsSet {
				return vitess.InjectedStatement{
					Statement: pgnodes.NewAlterDomainSetNotNull(
						domainName.Catalog(),
						domainName.Schema(),
						domainName.Object(),
					),
				}, nil
			}
			return vitess.InjectedStatement{
				Statement: pgnodes.NewAlterDomainDropNotNull(
					domainName.Catalog(),
					domainName.Schema(),
					domainName.Object(),
				),
			}, nil
		}
		if !setDrop.IsSet {
			return vitess.InjectedStatement{
				Statement: pgnodes.NewAlterDomainDropDefault(
					domainName.Catalog(),
					domainName.Schema(),
					domainName.Object(),
				),
			}, nil
		}
		defExpr, err := nodeExpr(ctx, setDrop.Default)
		if err != nil {
			return nil, err
		}
		if _, ok := defExpr.(*vitess.FuncExpr); ok {
			defExpr = &vitess.ParenExpr{Expr: defExpr}
		}
		return vitess.InjectedStatement{
			Statement: pgnodes.NewAlterDomainSetDefault(
				domainName.Catalog(),
				domainName.Schema(),
				domainName.Object(),
			),
			Children: []vitess.Expr{defExpr},
		}, nil
	}
	if constraint, ok := stmt.Cmd.(*tree.AlterDomainConstraint); ok {
		if constraint.Action != tree.AlterDomainAddConstraint {
			return NotYetSupportedError("ALTER DOMAIN constraint action is not yet supported")
		}
		if constraint.Constraint.Check == nil {
			return nil, errors.Errorf("ALTER DOMAIN ADD CONSTRAINT currently requires a CHECK expression")
		}
		check, err := verifyAndReplaceValue(stmt.Name, constraint.Constraint.Check)
		if err != nil {
			return nil, err
		}
		expr, err := nodeExpr(ctx, check)
		if err != nil {
			return nil, err
		}
		return vitess.InjectedStatement{
			Statement: pgnodes.NewAlterDomainAddConstraint(
				domainName.Catalog(),
				domainName.Schema(),
				domainName.Object(),
				string(constraint.Constraint.Constraint),
				constraint.NotValid,
			),
			Children: []vitess.Expr{expr},
		}, nil
	}

	return NotYetSupportedError("ALTER DOMAIN is not yet supported")
}
