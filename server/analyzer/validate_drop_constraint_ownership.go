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

	"github.com/dolthub/doltgresql/server/ast"
	"github.com/dolthub/doltgresql/server/indexmetadata"
)

// validateDropConstraintOwnership prevents standalone unique indexes from being
// treated as table constraints by GMS' generic DROP CONSTRAINT resolver.
func validateDropConstraintOwnership(ctx *sql.Context, _ *analyzer.Analyzer, n sql.Node, _ *plan.Scope, _ analyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(ctx, n, func(ctx *sql.Context, n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		dropConstraint, ok := n.(*plan.DropConstraint)
		if !ok || dropConstraint.IfExists {
			return n, transform.SameTree, nil
		}
		dropConstraintName, _ := ast.DecodeDropConstraintCascade(dropConstraint.Name)

		rt, ok := dropConstraint.Child.(*plan.ResolvedTable)
		if !ok {
			return n, transform.SameTree, nil
		}

		indexedTable, ok := rt.Table.(sql.IndexAddressable)
		if !ok {
			return n, transform.SameTree, nil
		}

		matchesConstraint, err := matchesForeignKeyOrCheckConstraint(ctx, rt.Table, dropConstraintName)
		if err != nil {
			return nil, transform.SameTree, err
		} else if matchesConstraint {
			return n, transform.SameTree, nil
		}

		indexes, err := indexedTable.GetIndexes(ctx)
		if err != nil {
			return nil, transform.SameTree, err
		}

		for _, index := range indexes {
			if index.IsUnique() &&
				strings.EqualFold(index.ID(), dropConstraintName) &&
				indexmetadata.IsStandaloneIndex(index.Comment()) {
				return nil, transform.SameTree, sql.ErrUnknownConstraint.New(dropConstraintName)
			}
		}

		return n, transform.SameTree, nil
	})
}

func matchesForeignKeyOrCheckConstraint(ctx *sql.Context, table sql.Table, name string) (bool, error) {
	if foreignKeyTable, ok := table.(sql.ForeignKeyTable); ok {
		declaredForeignKeys, err := foreignKeyTable.GetDeclaredForeignKeys(ctx)
		if err != nil {
			return false, err
		}
		referencedForeignKeys, err := foreignKeyTable.GetReferencedForeignKeys(ctx)
		if err != nil {
			return false, err
		}
		for _, foreignKey := range append(declaredForeignKeys, referencedForeignKeys...) {
			if strings.EqualFold(foreignKey.Name, name) {
				return true, nil
			}
		}
	}

	checkTable, ok := table.(sql.CheckTable)
	if !ok {
		return false, nil
	}
	checks, err := checkTable.GetChecks(ctx)
	if err != nil {
		return false, err
	}
	for _, check := range checks {
		if strings.EqualFold(check.Name, name) {
			return true, nil
		}
	}
	return false, nil
}
