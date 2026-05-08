// Copyright 2025 Dolthub, Inc.
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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/doltgresql/server/functions"
)

// generateForeignKeyName populates a generated foreign key name, in the Postgres default foreign key name format,
// when a foreign key is created without an explicit name specified.
func generateForeignKeyName(ctx *sql.Context, _ *analyzer.Analyzer, n sql.Node, _ *plan.Scope, _ analyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(ctx, n, func(ctx *sql.Context, n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := n.(type) {
		case *plan.CreateTable:
			// The doltgres AST emits a column-level REFERENCES clause as a table-level Constraint, and the GMS
			// planbuilder additionally builds an FK from the column's ForeignKeyDef. Drop those structural duplicates
			// before name generation so we don't try to register the same constraint twice.
			deduped := dedupCreateTableForeignKeys(n.ForeignKeys())
			copiedForeignKeys := make([]*sql.ForeignKeyConstraint, len(deduped))
			for i, fk := range deduped {
				copied := *fk
				copiedForeignKeys[i] = &copied
			}

			changedForeignKey := len(copiedForeignKeys) != len(n.ForeignKeys())
			generatedNames := make(map[string]struct{}, len(copiedForeignKeys))
			for _, fk := range copiedForeignKeys {
				if fk.Name == "" {
					generatedName, err := generateFkName(ctx, n.Name(), fk, generatedNames)
					if err != nil {
						return nil, transform.SameTree, err
					}
					changedForeignKey = true
					fk.Name = generatedName
				}
				generatedNames[fk.Name] = struct{}{}
			}
			if changedForeignKey {
				newCreateTable := plan.NewCreateTable(n.Db, n.Name(), n.IfNotExists(), n.Temporary(), &plan.TableSpec{
					Schema:    n.PkSchema(),
					FkDefs:    copiedForeignKeys,
					ChDefs:    n.Checks(),
					IdxDefs:   n.Indexes(),
					Collation: n.Collation,
					TableOpts: n.TableOpts,
				})
				return newCreateTable, transform.NewTree, nil
			} else {
				return n, transform.SameTree, nil
			}

		case *plan.CreateForeignKey:
			if n.FkDef.Name == "" {
				copiedFk := *n.FkDef
				generatedName, err := generateFkName(ctx, copiedFk.Table, &copiedFk, nil)
				if err != nil {
					return nil, transform.SameTree, err
				}
				copiedFk.Name = generatedName
				return &plan.CreateForeignKey{
					DbProvider: n.DbProvider,
					FkDef:      &copiedFk,
				}, transform.NewTree, nil
			} else {
				return n, transform.SameTree, nil
			}

		default:
			return n, transform.SameTree, nil
		}
	})
}

// generateFkName creates a default foreign key name, according to Postgres naming rules
// (i.e. "<tablename>_<col1name>_<col2name>_fkey"). If an existing foreign key is found with the default, generated
// name, the generated name will be suffixed with a number to ensure uniqueness. Names already chosen during this
// analysis pass should be passed via |sameStmtNames| so that we do not collide with FKs that have not yet been
// committed to the catalog.
func generateFkName(ctx *sql.Context, tableName string, newFk *sql.ForeignKeyConstraint, sameStmtNames map[string]struct{}) (string, error) {
	columnNames := strings.Join(newFk.Columns, "_")
	generatedBaseName := fmt.Sprintf("%s_%s_fkey", tableName, columnNames)

	for counter := 0; counter < 100; counter += 1 {
		generatedFkName := generatedBaseName
		if counter > 0 {
			generatedFkName = fmt.Sprintf("%s%d", generatedBaseName, counter)
		}

		if _, taken := sameStmtNames[generatedFkName]; taken {
			continue
		}

		duplicate := false
		err := functions.IterateCurrentDatabase(ctx, functions.Callbacks{
			ForeignKey: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable, foreignKey functions.ItemForeignKey) (cont bool, err error) {
				if foreignKey.Item.Name == generatedFkName {
					duplicate = true
					return false, nil
				}
				return true, nil
			},
		})
		if err != nil {
			return "", err
		}

		if !duplicate {
			return generatedFkName, nil
		}
	}

	return "", fmt.Errorf("unable to create unique foreign key %s: "+
		"a foreign key constraint already exists with this name", generatedBaseName)
}

// dedupCreateTableForeignKeys removes structurally identical foreign key definitions from the slice when at least one
// of the colliding entries has no explicit name. The doltgres AST emits a column-level REFERENCES clause as a
// table-level Constraint while the GMS planbuilder also builds an FK from the column's ForeignKeyDef, yielding two FK
// entries for the same REFERENCES clause; this drops the duplicate while leaving distinct, explicitly named FKs alone
// so that user-supplied naming conflicts surface from the underlying engine's checks.
func dedupCreateTableForeignKeys(fks []*sql.ForeignKeyConstraint) []*sql.ForeignKeyConstraint {
	if len(fks) < 2 {
		return fks
	}
	seen := make(map[string]int, len(fks))
	out := make([]*sql.ForeignKeyConstraint, 0, len(fks))
	for _, fk := range fks {
		key := fkSignature(fk)
		if existing, ok := seen[key]; ok {
			if out[existing].Name == "" || fk.Name == "" {
				if out[existing].Name == "" && fk.Name != "" {
					out[existing] = fk
				}
				continue
			}
		}
		seen[key] = len(out)
		out = append(out, fk)
	}
	return out
}

// fkSignature returns a structural key that ignores the constraint name, used to identify duplicate definitions.
func fkSignature(fk *sql.ForeignKeyConstraint) string {
	return strings.Join([]string{
		strings.ToLower(fk.Database),
		strings.ToLower(fk.SchemaName),
		strings.ToLower(fk.Table),
		strings.ToLower(strings.Join(fk.Columns, ",")),
		strings.ToLower(fk.ParentDatabase),
		strings.ToLower(fk.ParentSchema),
		strings.ToLower(fk.ParentTable),
		strings.ToLower(strings.Join(fk.ParentColumns, ",")),
		string(fk.OnUpdate),
		string(fk.OnDelete),
	}, "\x00")
}
